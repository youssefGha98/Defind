from __future__ import annotations

import json
from typing import Any

from defind.core.dataset_state.constants import JOBS_KEY, JOBS_SUMMARY_KEY, RUNNING_STATUSES, TERMINAL_STATUSES
from defind.core.dataset_state.locks import with_jobs_lock
from defind.core.dataset_state.meta import now_iso
from defind.core.interfaces import IChunkStorage

JOBS_SUMMARY_VERSION = 1
JOB_SUMMARY_FIELDS = (
    "job_id",
    "mode",
    "status",
    "resume_from",
    "chunks_written",
    "origin",
    "started_at",
    "ended_at",
    "error",
)


def parse_jobs(raw: str | None) -> list[dict[str, Any]]:
    if raw is None:
        return []
    rows: list[dict[str, Any]] = []
    for line in raw.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        payload = json.loads(stripped)
        if isinstance(payload, dict):
            rows.append(dict(payload))
    return rows


def read_dataset_jobs(storage: IChunkStorage) -> list[dict[str, Any]]:
    return parse_jobs(storage.read_text(JOBS_KEY))


def _job_summary_row(row: dict[str, Any]) -> dict[str, Any]:
    return {field: row.get(field) for field in JOB_SUMMARY_FIELDS}


def _jobs_summary_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    summarized = [_job_summary_row(dict(row)) for row in rows]
    return sorted(summarized, key=lambda row: str(row.get("started_at") or ""), reverse=True)


def _write_jobs_summary(storage: IChunkStorage, rows: list[dict[str, Any]]) -> None:
    storage.write_json(
        JOBS_SUMMARY_KEY,
        {
            "version": JOBS_SUMMARY_VERSION,
            "jobs": _jobs_summary_rows(rows),
        },
    )


def list_dataset_job_summaries(storage: IChunkStorage) -> list[dict[str, Any]]:
    payload = storage.read_json(JOBS_SUMMARY_KEY)
    if isinstance(payload, dict) and int(payload.get("version") or 0) == JOBS_SUMMARY_VERSION:
        jobs = payload.get("jobs")
        if isinstance(jobs, list):
            rows: list[dict[str, Any]] = []
            for row in jobs:
                if isinstance(row, dict):
                    rows.append(dict(row))
            return rows

    rows = read_dataset_jobs(storage)
    _write_jobs_summary(storage, rows)
    return _jobs_summary_rows(rows)


def get_dataset_job_summary(storage: IChunkStorage, job_id: str) -> dict[str, Any] | None:
    rows = list_dataset_job_summaries(storage)
    for row in rows:
        current = dict(row)
        if str(current.get("job_id") or "") == job_id:
            return current
    return None


def write_jobs(storage: IChunkStorage, rows: list[dict[str, Any]]) -> None:
    serialized = "\n".join(json.dumps(row, separators=(",", ":"), sort_keys=True) for row in rows)
    if serialized:
        serialized += "\n"
    storage.write_text(JOBS_KEY, serialized)


def list_dataset_jobs(storage: IChunkStorage) -> list[dict[str, Any]]:
    rows = read_dataset_jobs(storage)
    return sorted(rows, key=lambda row: str(row.get("started_at") or ""), reverse=True)


def active_writer_job(storage: IChunkStorage) -> dict[str, Any] | None:
    rows = list_dataset_job_summaries(storage)
    running = [row for row in rows if str(row.get("status") or "") in RUNNING_STATUSES]
    if not running:
        return None
    running.sort(key=lambda row: str(row.get("started_at") or ""), reverse=True)
    return dict(running[0])


def append_dataset_job(storage: IChunkStorage, row: dict[str, Any]) -> dict[str, Any]:
    def append() -> dict[str, Any]:
        rows = read_dataset_jobs(storage)
        rows.append(dict(row))
        write_jobs(storage, rows)
        _write_jobs_summary(storage, rows)
        return dict(row)

    return with_jobs_lock(storage, append)


def update_last_dataset_job(
    storage: IChunkStorage,
    *,
    expected_job_id: str,
    update_fn: Any,
) -> dict[str, Any]:
    def update() -> dict[str, Any]:
        rows = read_dataset_jobs(storage)
        if not rows:
            raise KeyError(expected_job_id)
        current = dict(rows[-1])
        if str(current.get("job_id") or "") != expected_job_id:
            raise RuntimeError("last job does not match expected job id")
        updated = dict(update_fn(current))
        rows[-1] = updated
        write_jobs(storage, rows)
        _write_jobs_summary(storage, rows)
        return updated

    return with_jobs_lock(storage, update)


def get_dataset_job(storage: IChunkStorage, job_id: str) -> dict[str, Any] | None:
    rows = read_dataset_jobs(storage)
    for row in reversed(rows):
        if str(row.get("job_id") or "") == job_id:
            return dict(row)
    return None


def delete_dataset_job(storage: IChunkStorage, job_id: str) -> dict[str, Any]:
    def delete() -> dict[str, Any]:
        rows = read_dataset_jobs(storage)
        remaining: list[dict[str, Any]] = []
        removed: dict[str, Any] | None = None
        for row in rows:
            current = dict(row)
            if removed is None and str(current.get("job_id") or "") == job_id:
                removed = current
                continue
            remaining.append(current)
        if removed is None:
            raise KeyError(job_id)
        write_jobs(storage, remaining)
        _write_jobs_summary(storage, remaining)
        return removed

    return with_jobs_lock(storage, delete)


def build_job_snapshot(
    *,
    job_id: str,
    mode: str,
    status: str,
    resume_from: int,
    origin: str,
    config_snapshot: dict[str, Any],
) -> dict[str, Any]:
    started_at = now_iso()
    return {
        "job_id": job_id,
        "mode": mode,
        "status": status,
        "resume_from": int(resume_from),
        "chunks_written": 0,
        "origin": origin,
        "started_at": started_at,
        "ended_at": None,
        "error": None,
        "config_snapshot": dict(config_snapshot),
    }


def mark_job_terminal(
    storage: IChunkStorage,
    *,
    job_id: str,
    status: str,
    error: str | None,
) -> dict[str, Any]:
    if status not in TERMINAL_STATUSES:
        raise ValueError(f"unsupported terminal status: {status}")

    def update(current: dict[str, Any]) -> dict[str, Any]:
        current["status"] = status
        current["error"] = error
        current["ended_at"] = now_iso()
        return current

    return update_last_dataset_job(storage, expected_job_id=job_id, update_fn=update)


def increment_job_progress(
    storage: IChunkStorage,
    *,
    job_id: str,
    confirmed_to_block: int,
) -> dict[str, Any]:
    def update(current: dict[str, Any]) -> dict[str, Any]:
        current["chunks_written"] = int(current.get("chunks_written") or 0) + 1
        current["resume_from"] = int(confirmed_to_block)
        return current

    return update_last_dataset_job(storage, expected_job_id=job_id, update_fn=update)
