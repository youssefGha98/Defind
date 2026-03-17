from __future__ import annotations

from typing import Any

from defind.api.ops.dataset.repository import DatasetRepository
from defind.api.ops.jobs.repository import JobsRepository
from defind.api.ops.shared.models import DatasetRef


class StatusRepository:
    def __init__(
        self,
        *,
        dataset_repository: DatasetRepository,
        jobs_repository: JobsRepository,
    ) -> None:
        self._dataset_repository = dataset_repository
        self._jobs_repository = jobs_repository

    def discover_datasets(self) -> list[DatasetRef]:
        return self._dataset_repository.discover_datasets()

    def read_metas(self, datasets: list[DatasetRef]) -> list[dict[str, Any]]:
        return [self._dataset_repository.read_meta_or_404(dataset) for dataset in datasets]

    def count_active_jobs(self, datasets: list[DatasetRef]) -> int:
        active_jobs_count = 0
        for dataset in datasets:
            if self._jobs_repository.active_writer_job(dataset) is not None:
                active_jobs_count += 1
        return active_jobs_count
