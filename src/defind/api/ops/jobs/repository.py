from __future__ import annotations

from typing import Any

from defind.api.ops.dataset.repository import DatasetRepository
from defind.api.ops.shared.models import DatasetRef
from defind.dataset_state import (
    active_writer_job,
    append_dataset_job,
    delete_dataset_job,
    get_dataset_job,
    get_dataset_job_summary,
    increment_job_progress,
    list_dataset_job_summaries,
    mark_job_terminal,
)


class JobsRepository:
    def __init__(self, *, dataset_repository: DatasetRepository) -> None:
        self._dataset_repository = dataset_repository

    def list_jobs(self, dataset: DatasetRef) -> list[dict[str, Any]]:
        storage, _ = self._dataset_repository.build_dataset_storage(dataset)
        return list_dataset_job_summaries(storage)

    def get_job(self, dataset: DatasetRef, job_id: str) -> dict[str, Any] | None:
        storage, _ = self._dataset_repository.build_dataset_storage(dataset)
        return get_dataset_job(storage, job_id)

    def get_job_summary(self, dataset: DatasetRef, job_id: str) -> dict[str, Any] | None:
        storage, _ = self._dataset_repository.build_dataset_storage(dataset)
        return get_dataset_job_summary(storage, job_id)

    def active_writer_job(self, dataset: DatasetRef) -> dict[str, Any] | None:
        storage, _ = self._dataset_repository.build_dataset_storage(dataset)
        return active_writer_job(storage)

    def append_job(self, dataset: DatasetRef, row: dict[str, Any]) -> dict[str, Any]:
        storage, _ = self._dataset_repository.build_dataset_storage(dataset)
        appended = append_dataset_job(storage, row)
        self._dataset_repository.record_job_started(dataset, appended)
        return appended

    def mark_terminal(self, dataset: DatasetRef, *, job_id: str, status: str, error: str | None) -> dict[str, Any]:
        storage, _ = self._dataset_repository.build_dataset_storage(dataset)
        row = mark_job_terminal(storage, job_id=job_id, status=status, error=error)
        self._dataset_repository.record_job_terminal(dataset, status)
        return row

    def increment_progress(self, dataset: DatasetRef, *, job_id: str, confirmed_to_block: int) -> dict[str, Any]:
        storage, _ = self._dataset_repository.build_dataset_storage(dataset)
        row = increment_job_progress(storage, job_id=job_id, confirmed_to_block=confirmed_to_block)
        self._dataset_repository.record_chunk_progress(dataset, confirmed_to_block)
        return row

    def delete_job(self, dataset: DatasetRef, job_id: str) -> dict[str, Any]:
        storage, _ = self._dataset_repository.build_dataset_storage(dataset)
        removed = delete_dataset_job(storage, job_id)
        self._dataset_repository.record_job_deleted(dataset)
        return removed
