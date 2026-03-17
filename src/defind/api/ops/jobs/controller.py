from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from fastapi import HTTPException

from defind.api.ops.dataset.repository import DatasetRepository
from defind.api.ops.dataset.utils import dataset_from_route
from defind.api.ops.jobs.models import (
    JobDeleteReadModel,
    JobGetReadModel,
    JobListReadModel,
    JobRestartPostModel,
    JobRunReadModel,
    JobStartPostModel,
)
from defind.api.ops.jobs.repository import JobsRepository
from defind.api.ops.jobs.utils import (
    PreparedDatasetRun,
    dataset_job_start_http_exception,
    prepare_dataset_job_run,
    validate_job_start_preflight,
)
from defind.api.ops.logs.repository import LogsRepository
from defind.api.ops.logs.utils import log_record_payload
from defind.api.ops.shared.models import DatasetRef, OpsApiConfig
from defind.api.ops.shared.utils import exception_detail
from defind.dataset_state import build_job_snapshot
from defind.observability import bind_log_context, get_logger

logger = get_logger(__name__)


class _JobRuntimeLogHandler(logging.Handler):
    def __init__(
        self,
        *,
        dataset_id: str,
        job_id: str,
        run_id: str,
        emit_event: Callable[[str, str | None, str | None, str | None, dict[str, Any] | None], Awaitable[None]],
    ) -> None:
        super().__init__(level=logging.INFO)
        self._dataset_id = dataset_id
        self._job_id = job_id
        self._run_id = run_id
        self._emit_event = emit_event

    def emit(self, record: logging.LogRecord) -> None:
        if not record.name.startswith("defind."):
            return
        if getattr(record, "dataset_id", None) != self._dataset_id:
            return
        message = str(record.getMessage() or "").strip()
        if not message:
            return
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return
        asyncio.create_task(
            self._emit_event(
                message,
                self._dataset_id,
                self._job_id,
                self._run_id,
                log_record_payload(record),
            )
        )


@dataclass
class _DatasetRuntimeJob:
    dataset: DatasetRef
    job_id: str
    mode: str
    task: asyncio.Task[None]


class DatasetJobRuntimeManager:
    def __init__(
        self,
        *,
        cfg: OpsApiConfig,
        dataset_repository: DatasetRepository,
        jobs_repository: JobsRepository,
        logs_repository: LogsRepository,
        fetch_data: Callable[..., Awaitable[Any]],
        fetch_rpc_chain_head: Callable[..., Awaitable[int]],
    ) -> None:
        self._cfg = cfg
        self._dataset_repository = dataset_repository
        self._jobs_repository = jobs_repository
        self._logs_repository = logs_repository
        self._fetch_data = fetch_data
        self._fetch_rpc_chain_head = fetch_rpc_chain_head
        self._tasks: dict[str, _DatasetRuntimeJob] = {}
        self._lock = asyncio.Lock()

    @staticmethod
    def _make_job_id() -> str:
        return str(time.time_ns())

    async def _record_event(
        self,
        event_type: str,
        dataset_id: str | None = None,
        job_id: str | None = None,
        run_id: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> None:
        try:
            await self._logs_repository.record_event(
                event_type=event_type,
                dataset_id=dataset_id,
                job_id=job_id,
                run_id=run_id,
                payload=payload,
            )
        except Exception as exc:
            logger.error(
                "event_store_persist_failed",
                extra={
                    "event_type": event_type,
                    "dataset_id": dataset_id,
                    "job_id": job_id,
                    "run_id": run_id,
                    "error": str(exc),
                },
                exc_info=True,
            )

    async def recover_stale_jobs(self) -> None:
        for dataset in self._dataset_repository.discover_datasets():
            running = self._jobs_repository.active_writer_job(dataset)
            if running is None:
                continue
            try:
                self._jobs_repository.mark_terminal(
                    dataset,
                    job_id=str(running.get("job_id") or ""),
                    status="failed",
                    error="api restarted while job was running",
                )
            except Exception:
                logger.warning("dataset_job_recovery_failed", extra={"dataset_id": dataset.dataset_id}, exc_info=True)

    async def start(
        self,
        *,
        dataset: DatasetRef,
        meta: dict[str, Any],
        mode: str,
        concurrency: int,
        origin: str,
        source_job_id: str | None = None,
    ) -> dict[str, Any]:
        blocking = self._jobs_repository.active_writer_job(dataset)
        if blocking is not None:
            blocking_job_id = str(blocking.get("job_id") or "")
            raise RuntimeError(f"writer_job_already_active:{blocking_job_id}")

        resume_from = int(meta.get("last_block") or meta.get("start_block") or 0)
        prepared = prepare_dataset_job_run(
            cfg=self._cfg,
            dataset=dataset,
            meta=meta,
            mode=mode,
            concurrency=concurrency,
            origin=origin,
            resume_from=resume_from,
        )
        observed_chain_head = await validate_job_start_preflight(
            meta,
            fetch_rpc_chain_head=self._fetch_rpc_chain_head,
            resolve_public_chain_head=self._dataset_repository.resolve_public_chain_head,
        )
        self._dataset_repository.record_observed_chain_head(dataset, observed_chain_head)
        job_id = self._make_job_id()
        job_row = build_job_snapshot(
            job_id=job_id,
            mode=mode,
            status="running",
            resume_from=resume_from,
            origin=origin,
            config_snapshot=prepared.public_config,
        )
        self._jobs_repository.append_job(dataset, job_row)

        task = asyncio.create_task(
            self._run_job(dataset=dataset, prepared=prepared, job_id=job_id),
            name=f"defind-dataset-job-{dataset.dataset_id}-{job_id}",
        )
        async with self._lock:
            self._tasks[job_id] = _DatasetRuntimeJob(dataset=dataset, job_id=job_id, mode=mode, task=task)

        await self._record_event(
            event_type="dataset_job_started",
            dataset_id=dataset.dataset_id,
            job_id=job_id,
            run_id=job_id,
            payload={"mode": mode, "origin": origin, "sourceJobId": source_job_id},
        )
        return self._jobs_repository.get_job(dataset, job_id) or job_row

    async def _run_job(
        self,
        *,
        dataset: DatasetRef,
        prepared: PreparedDatasetRun,
        job_id: str,
    ) -> None:
        runtime_log_handler = _JobRuntimeLogHandler(
            dataset_id=dataset.dataset_id,
            job_id=job_id,
            run_id=job_id,
            emit_event=self._record_event,
        )
        root_logger = logging.getLogger()
        root_logger.addHandler(runtime_log_handler)

        async def _on_chunk_written(_: int, confirmed_to_block: int) -> None:
            self._dataset_repository.update_meta(dataset, {"last_block": int(confirmed_to_block)})
            self._jobs_repository.increment_progress(dataset, job_id=job_id, confirmed_to_block=confirmed_to_block)

        try:
            with bind_log_context(
                job_id=job_id,
                protocol=dataset.protocol,
                contract=dataset.contract,
            ):
                await self._fetch_data(
                    config=prepared.config,
                    registry=prepared.registry,
                    on_chunk_written=_on_chunk_written,
                )
        except asyncio.CancelledError:
            row = self._jobs_repository.get_job(dataset, job_id)
            if str((row or {}).get("status") or "") != "stopped":
                with contextlib.suppress(Exception):
                    self._jobs_repository.mark_terminal(dataset, job_id=job_id, status="stopped", error=None)
                await self._record_event(
                    event_type="dataset_job_stopped",
                    dataset_id=dataset.dataset_id,
                    job_id=job_id,
                    run_id=job_id,
                    payload={},
                )
            raise
        except Exception as exc:
            detail = exception_detail(exc)
            with contextlib.suppress(Exception):
                self._jobs_repository.mark_terminal(dataset, job_id=job_id, status="failed", error=detail)
            await self._record_event(
                event_type="dataset_job_failed",
                dataset_id=dataset.dataset_id,
                job_id=job_id,
                run_id=job_id,
                payload={"error": detail},
            )
        else:
            with contextlib.suppress(Exception):
                self._jobs_repository.mark_terminal(dataset, job_id=job_id, status="completed", error=None)
            await self._record_event(
                event_type="dataset_job_completed",
                dataset_id=dataset.dataset_id,
                job_id=job_id,
                run_id=job_id,
                payload={},
            )
        finally:
            root_logger.removeHandler(runtime_log_handler)
            runtime_log_handler.close()
            async with self._lock:
                self._tasks.pop(job_id, None)

    async def stop(self, *, dataset: DatasetRef, job_id: str) -> dict[str, Any]:
        task: asyncio.Task[None] | None = None
        async with self._lock:
            active = self._tasks.get(job_id)
            if active is not None:
                task = active.task
        if task is None:
            row = self._jobs_repository.get_job(dataset, job_id)
            if row is None:
                raise KeyError(job_id)
            return row
        task.cancel()
        row = self._jobs_repository.get_job(dataset, job_id)
        if row is None:
            raise KeyError(job_id)
        if str(row.get("status") or "") == "running":
            row = self._jobs_repository.mark_terminal(dataset, job_id=job_id, status="stopped", error=None)
            await self._record_event(
                event_type="dataset_job_stopped",
                dataset_id=dataset.dataset_id,
                job_id=job_id,
                run_id=job_id,
                payload={"source": "api-stop"},
            )
        return row

    async def shutdown(self) -> None:
        async with self._lock:
            tasks = [item.task for item in self._tasks.values()]
        for task in tasks:
            task.cancel()
        if tasks:
            with contextlib.suppress(Exception):
                await asyncio.gather(*tasks, return_exceptions=True)


class JobsController:
    def __init__(
        self,
        *,
        dataset_repository: DatasetRepository,
        jobs_repository: JobsRepository,
        logs_repository: LogsRepository,
        runtime_manager: DatasetJobRuntimeManager,
    ) -> None:
        self._dataset_repository = dataset_repository
        self._jobs_repository = jobs_repository
        self._logs_repository = logs_repository
        self._runtime_manager = runtime_manager

    async def list_jobs(self, protocol: str, contract: str) -> list[JobListReadModel]:
        dataset = dataset_from_route(protocol, contract)
        self._dataset_repository.ensure_dataset_known_or_404(dataset)
        return [JobListReadModel.model_validate(row) for row in self._jobs_repository.list_jobs(dataset)]

    async def start_job(self, protocol: str, contract: str, payload: JobStartPostModel) -> JobRunReadModel:
        dataset, meta, _ = self._dataset_repository.get_context(protocol, contract)
        try:
            row = await self._runtime_manager.start(
                dataset=dataset,
                meta=meta,
                mode=payload.mode,
                concurrency=payload.concurrency,
                origin="ui",
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except RuntimeError as exc:
            raise dataset_job_start_http_exception(exc) from exc
        return JobRunReadModel.model_validate(row)

    async def get_job(self, protocol: str, contract: str, job_id: str) -> JobGetReadModel:
        dataset = dataset_from_route(protocol, contract)
        self._dataset_repository.ensure_dataset_known_or_404(dataset)
        row = self._jobs_repository.get_job_summary(dataset, job_id)
        if row is None:
            raise HTTPException(status_code=404, detail="job not found")
        return JobGetReadModel.model_validate(row)

    async def stop_job(self, protocol: str, contract: str, job_id: str) -> JobRunReadModel:
        dataset = dataset_from_route(protocol, contract)
        self._dataset_repository.ensure_dataset_known_or_404(dataset)
        try:
            row = await self._runtime_manager.stop(dataset=dataset, job_id=job_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="job not found") from exc
        return JobRunReadModel.model_validate(row)

    async def restart_job(
        self,
        protocol: str,
        contract: str,
        job_id: str,
        payload: JobRestartPostModel | None,
    ) -> JobRunReadModel:
        dataset, meta, _ = self._dataset_repository.get_context(protocol, contract)
        previous = self._jobs_repository.get_job(dataset, job_id)
        if previous is None:
            raise HTTPException(status_code=404, detail="job not found")
        snapshot = previous.get("config_snapshot")
        if not isinstance(snapshot, dict):
            raise HTTPException(status_code=400, detail="job has no config_snapshot")
        mode = str(
            (
                payload.mode if payload is not None and payload.mode is not None else snapshot.get("mode")
            )
            or previous.get("mode")
            or "backfill"
        )
        concurrency = int(
            (
                payload.concurrency
                if payload is not None and payload.concurrency is not None
                else snapshot.get("concurrency")
            )
            or 16
        )
        try:
            row = await self._runtime_manager.start(
                dataset=dataset,
                meta=meta,
                mode=mode,
                concurrency=concurrency,
                origin="ui",
                source_job_id=job_id,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except RuntimeError as exc:
            raise dataset_job_start_http_exception(exc) from exc
        return JobRunReadModel.model_validate(row)

    async def delete_job(self, protocol: str, contract: str, job_id: str) -> JobDeleteReadModel:
        dataset = dataset_from_route(protocol, contract)
        self._dataset_repository.ensure_dataset_known_or_404(dataset)
        current = self._jobs_repository.get_job(dataset, job_id)
        if current is None:
            raise HTTPException(status_code=404, detail="job not found")
        if str(current.get("status") or "") == "running":
            raise HTTPException(status_code=409, detail="running jobs must be stopped before deletion")
        removed = self._jobs_repository.delete_job(dataset, job_id)

        async def _purge_deleted_job_logs() -> None:
            try:
                await self._logs_repository.delete_job_logs(dataset_id=dataset.dataset_id, job_id=job_id)
            except Exception:
                logger.warning(
                    "dataset_job_log_purge_failed",
                    extra={"dataset_id": dataset.dataset_id, "job_id": job_id},
                    exc_info=True,
                )

        asyncio.create_task(_purge_deleted_job_logs())
        return JobDeleteReadModel(
            job=JobRunReadModel.model_validate(removed),
            deleted_log_events=0,
            log_purge_scheduled=True,
        )
