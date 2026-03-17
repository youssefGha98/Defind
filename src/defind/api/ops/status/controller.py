from __future__ import annotations

from defind.api.ops.status.models import HealthReadModel, ReadyReadModel, StatusReadModel
from defind.api.ops.status.repository import StatusRepository


class StatusController:
    def __init__(self, *, repository: StatusRepository) -> None:
        self._repository = repository

    async def get_status(self) -> StatusReadModel:
        rows = self._repository._dataset_repository.list_cached_dataset_rows()
        return StatusReadModel(
            chain_head=max((int(row.get("chain_head") or 0) for row in rows), default=0),
            lag=max((int(row.get("lag") or 0) for row in rows), default=0),
            active_jobs_count=sum(int(row.get("active_jobs_count") or 0) for row in rows),
            datasets_count=len(rows),
        )

    async def get_health(self) -> HealthReadModel:
        return HealthReadModel(status="ok")

    async def get_ready(self) -> ReadyReadModel:
        return ReadyReadModel(status="ready")
