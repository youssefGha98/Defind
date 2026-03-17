from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

from fastapi import HTTPException
from fastapi.responses import StreamingResponse

from defind.api.ops.dataset.repository import DatasetRepository
from defind.api.ops.dataset.utils import dataset_from_route
from defind.api.ops.jobs.repository import JobsRepository
from defind.api.ops.logs.models import JobLogReadModel, JobLogsPageReadModel
from defind.api.ops.logs.repository import LogsRepository
from defind.api.ops.logs.utils import encode_sse, matches_level_filter
from defind.api.ops.shared.models import DatasetRef


class LogsController:
    def __init__(
        self,
        *,
        dataset_repository: DatasetRepository,
        jobs_repository: JobsRepository,
        logs_repository: LogsRepository,
    ) -> None:
        self._dataset_repository = dataset_repository
        self._jobs_repository = jobs_repository
        self._logs_repository = logs_repository

    async def get_job_logs(
        self,
        protocol: str,
        contract: str,
        job_id: str,
        *,
        page: int,
        limit: int,
        level: str,
    ) -> JobLogsPageReadModel:
        dataset = dataset_from_route(protocol, contract)
        self._dataset_repository.ensure_dataset_known_or_404(dataset)
        if self._jobs_repository.get_job_summary(dataset, job_id) is None:
            raise HTTPException(status_code=404, detail="job not found")
        rows = await self._logs_repository.list_job_events(
            dataset_id=dataset.dataset_id,
            job_id=job_id,
            limit=min(1000, max(1, page) * max(1, limit) * 5),
        )
        filtered = [
            JobLogReadModel.model_validate(row)
            for row in rows
            if matches_level_filter(dict(row.get("payload") or {}), level)
        ]
        start = (page - 1) * limit
        end = start + limit
        return JobLogsPageReadModel(page=page, limit=limit, items=filtered[start:end])

    async def stream_job_logs(self, protocol: str, contract: str, job_id: str) -> StreamingResponse:
        dataset = dataset_from_route(protocol, contract)
        self._dataset_repository.ensure_dataset_known_or_404(dataset)
        if self._jobs_repository.get_job_summary(dataset, job_id) is None:
            raise HTTPException(status_code=404, detail="job not found")
        return StreamingResponse(
            self._job_log_stream(dataset, job_id),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "Connection": "keep-alive"},
        )

    async def _job_log_stream(self, dataset: DatasetRef, job_id: str) -> AsyncIterator[bytes]:
        last_seen_id = 0
        while True:
            rows = await self._logs_repository.list_job_events(dataset_id=dataset.dataset_id, job_id=job_id, limit=200)
            new_rows = [row for row in reversed(rows) if int(row.get("id") or 0) > last_seen_id]
            for row in new_rows:
                last_seen_id = max(last_seen_id, int(row.get("id") or 0))
                yield encode_sse(row)

            job = self._jobs_repository.get_job_summary(dataset, job_id)
            if job is None:
                break
            if str(job.get("status") or "") in {"stopped", "failed", "completed"} and not new_rows:
                break
            await asyncio.sleep(1.0)
