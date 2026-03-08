from __future__ import annotations

import json
import threading
import uuid
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import PurePosixPath
from typing import Any, cast

from defind.core.interfaces import IChunkStorage
from defind.decoding.specs import EventRegistry
from defind.indexer_request import deserialize_registry, serialize_registry
from defind.observability import get_logger
from defind.orchestration.orchestrator import (
    _acquire_writer_lock,
    _release_writer_lock,
)

logger = get_logger(__name__)

META_KEY = "_meta.json"
JOBS_KEY = "_jobs.jsonl"
JOBS_LOCK_KEY = "_jobs.lock.json"
META_VERSION = 1
_RUNNING_STATUSES = frozenset({"running"})
_TERMINAL_STATUSES = frozenset({"completed", "failed", "stopped"})
_IMMUTABLE_META_FIELDS = frozenset({"protocol", "contract", "start_block"})
_UNSUPPORTED_LOCK_MARKERS = (
    "requires boto3",
    "botocore client support",
    "unsupported",
    "if-none-match",
    "if-match",
    "conditional",
    "precondition",
    "not implemented",
    "501",
)
_PROCESS_LOCAL_LOCKS: dict[str, threading.Lock] = {}
_PROCESS_LOCAL_LOCKS_GUARD = threading.Lock()
_DEGRADATION_WARNINGS: set[str] = set()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _read_json_with_version(
    storage: IChunkStorage,
    key: str,
) -> tuple[dict[str, Any] | None, str | None]:
    read_with_version = getattr(storage, "read_json_with_version", None)
    if callable(read_with_version) and hasattr(type(storage), "read_json_with_version"):
        result = read_with_version(key)
        if isinstance(result, tuple) and len(result) == 2:
            data, version = result
            return cast(dict[str, Any] | None, data), cast(str | None, version)
    return storage.read_json(key), None


def _storage_lock_id(storage: IChunkStorage, key: str) -> str:
    root = getattr(storage, "root", None)
    bucket = getattr(storage, "bucket", None)
    prefix = getattr(storage, "prefix", None)
    return f"{type(storage).__name__}:{root!s}:{bucket!s}:{prefix!s}:{key}"


def _process_local_lock(storage: IChunkStorage, key: str) -> threading.Lock:
    lock_id = _storage_lock_id(storage, key)
    with _PROCESS_LOCAL_LOCKS_GUARD:
        lock = _PROCESS_LOCAL_LOCKS.get(lock_id)
        if lock is None:
            lock = threading.Lock()
            _PROCESS_LOCAL_LOCKS[lock_id] = lock
        return lock


def _looks_unsupported_lock_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return any(marker in message for marker in _UNSUPPORTED_LOCK_MARKERS)


def _warn_local_only_degradation(storage: IChunkStorage, key: str, *, reason: str) -> None:
    warning_id = _storage_lock_id(storage, key)
    if warning_id in _DEGRADATION_WARNINGS:
        return
    _DEGRADATION_WARNINGS.add(warning_id)
    logger.warning(
        "dataset_state_lock_degraded_local_only",
        extra={
            "key": key,
            "storage": type(storage).__name__,
            "reason": reason,
        },
    )


def _write_json_if_version(
    storage: IChunkStorage,
    key: str,
    payload: dict[str, Any],
    expected_version: str | None,
) -> bool:
    write_if_version = getattr(storage, "write_json_if_version", None)
    if callable(write_if_version) and hasattr(type(storage), "write_json_if_version"):
        return bool(write_if_version(key, payload, expected_version))
    storage.write_json(key, payload)
    return True


def discover_dataset_refs(storage: IChunkStorage) -> list[tuple[str, str]]:
    refs: set[tuple[str, str]] = set()
    for key in storage.list_keys(""):
        normalized = key.strip().lstrip("/")
        if not normalized.endswith(f"/{META_KEY}"):
            continue
        parts = PurePosixPath(normalized).parts
        if len(parts) != 3:
            continue
        protocol, contract, filename = parts
        if filename != META_KEY:
            continue
        if protocol and contract:
            refs.add((protocol, contract))
    return sorted(refs)


def read_dataset_meta(storage: IChunkStorage) -> dict[str, Any] | None:
    payload = storage.read_json(META_KEY)
    if not isinstance(payload, dict):
        return None
    return dict(payload)


def create_dataset_meta(storage: IChunkStorage, payload: dict[str, Any]) -> dict[str, Any]:
    if storage.exists(META_KEY):
        raise FileExistsError(META_KEY)
    normalized = dict(payload)
    normalized["version"] = META_VERSION
    normalized["updated_at"] = _now_iso()
    storage.write_json(META_KEY, normalized)
    return normalized


def update_dataset_meta(
    storage: IChunkStorage,
    update_fn: Callable[[dict[str, Any]], dict[str, Any]],
    *,
    max_attempts: int = 8,
) -> dict[str, Any]:
    def _local_only_update() -> dict[str, Any]:
        with _process_local_lock(storage, META_KEY):
            current = read_dataset_meta(storage)
            if not isinstance(current, dict):
                raise FileNotFoundError(META_KEY)
            updated = dict(update_fn(dict(current)))
            updated["version"] = META_VERSION
            updated["updated_at"] = _now_iso()
            storage.write_json(META_KEY, updated)
            return updated

    for _ in range(max(1, max_attempts)):
        try:
            current, version = _read_json_with_version(storage, META_KEY)
            if not isinstance(current, dict):
                raise FileNotFoundError(META_KEY)
            updated = dict(update_fn(dict(current)))
            updated["version"] = META_VERSION
            updated["updated_at"] = _now_iso()
            if _write_json_if_version(storage, META_KEY, updated, version):
                return updated
        except Exception as exc:
            if _looks_unsupported_lock_error(exc):
                _warn_local_only_degradation(storage, META_KEY, reason=str(exc))
                return _local_only_update()
            raise
    raise RuntimeError("dataset meta update conflict")


def validate_meta_patch(payload: dict[str, Any]) -> None:
    forbidden = sorted(field for field in payload if field in _IMMUTABLE_META_FIELDS)
    if forbidden:
        raise ValueError(f"immutable meta fields cannot be updated: {forbidden}")


def validate_meta_runtime_fields(meta: dict[str, Any]) -> EventRegistry:
    rpc_url = str(meta.get("rpc_url") or "").strip()
    if not rpc_url:
        raise ValueError("dataset meta is missing rpc_url")
    event_names = meta.get("event_names")
    if not isinstance(event_names, list) or not event_names or not all(isinstance(item, str) and item for item in event_names):
        raise ValueError("dataset meta is missing event_names")
    registry_json = meta.get("registry_json")
    if not isinstance(registry_json, dict):
        raise ValueError("dataset meta is missing registry_json")
    return deserialize_registry(registry_json)


def build_dataset_meta(
    *,
    protocol: str,
    contract: str,
    contract_address: str,
    chain_id: int,
    start_block: int,
    chunk_size: int,
    step: int,
    storage: str,
    rpc_url: str,
    event_names: list[str],
    registry: EventRegistry,
) -> dict[str, Any]:
    return {
        "protocol": protocol,
        "contract": contract,
        "contract_address": contract_address,
        "chain_id": int(chain_id),
        "start_block": int(start_block),
        "last_block": int(start_block),
        "chunk_size": int(chunk_size),
        "step": int(step),
        "storage": storage,
        "rpc_url": rpc_url,
        "event_names": list(event_names),
        "registry_json": serialize_registry(registry),
    }


def _parse_jobs(raw: str | None) -> list[dict[str, Any]]:
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
    return _parse_jobs(storage.read_text(JOBS_KEY))


def _write_jobs(storage: IChunkStorage, rows: list[dict[str, Any]]) -> None:
    serialized = "\n".join(json.dumps(row, separators=(",", ":"), sort_keys=True) for row in rows)
    if serialized:
        serialized += "\n"
    storage.write_text(JOBS_KEY, serialized)


def _with_jobs_lock[T](storage: IChunkStorage, fn: Callable[[], T]) -> T:
    try:
        lock = _acquire_writer_lock(
            storage=storage,
            key=JOBS_LOCK_KEY,
            owner_id=f"dataset-jobs:{uuid.uuid4().hex}",
            run_id=uuid.uuid4().hex,
            ttl_s=30,
        )
    except Exception as exc:
        if _looks_unsupported_lock_error(exc):
            _warn_local_only_degradation(storage, JOBS_LOCK_KEY, reason=str(exc))
            with _process_local_lock(storage, JOBS_LOCK_KEY):
                return fn()
        raise
    try:
        return fn()
    finally:
        _release_writer_lock(storage=storage, lock=lock)


def list_dataset_jobs(storage: IChunkStorage) -> list[dict[str, Any]]:
    rows = read_dataset_jobs(storage)
    return sorted(rows, key=lambda row: str(row.get("started_at") or ""), reverse=True)


def active_writer_job(storage: IChunkStorage) -> dict[str, Any] | None:
    rows = read_dataset_jobs(storage)
    running = [row for row in rows if str(row.get("status") or "") in _RUNNING_STATUSES]
    if not running:
        return None
    running.sort(key=lambda row: str(row.get("started_at") or ""), reverse=True)
    return dict(running[0])


def append_dataset_job(storage: IChunkStorage, row: dict[str, Any]) -> dict[str, Any]:
    def _append() -> dict[str, Any]:
        rows = read_dataset_jobs(storage)
        rows.append(dict(row))
        _write_jobs(storage, rows)
        return dict(row)

    return _with_jobs_lock(storage, _append)


def update_last_dataset_job(
    storage: IChunkStorage,
    *,
    expected_job_id: str,
    update_fn: Callable[[dict[str, Any]], dict[str, Any]],
) -> dict[str, Any]:
    def _update() -> dict[str, Any]:
        rows = read_dataset_jobs(storage)
        if not rows:
            raise KeyError(expected_job_id)
        current = dict(rows[-1])
        if str(current.get("job_id") or "") != expected_job_id:
            raise RuntimeError("last job does not match expected job id")
        updated = dict(update_fn(current))
        rows[-1] = updated
        _write_jobs(storage, rows)
        return updated

    return _with_jobs_lock(storage, _update)


def get_dataset_job(storage: IChunkStorage, job_id: str) -> dict[str, Any] | None:
    rows = read_dataset_jobs(storage)
    for row in reversed(rows):
        if str(row.get("job_id") or "") == job_id:
            return dict(row)
    return None


def build_job_snapshot(
    *,
    job_id: str,
    mode: str,
    status: str,
    resume_from: int,
    origin: str,
    config_snapshot: dict[str, Any],
) -> dict[str, Any]:
    started_at = _now_iso()
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
    if status not in _TERMINAL_STATUSES:
        raise ValueError(f"unsupported terminal status: {status}")

    def _update(current: dict[str, Any]) -> dict[str, Any]:
        current["status"] = status
        current["error"] = error
        current["ended_at"] = _now_iso()
        return current

    return update_last_dataset_job(storage, expected_job_id=job_id, update_fn=_update)


def increment_job_progress(
    storage: IChunkStorage,
    *,
    job_id: str,
    confirmed_to_block: int,
) -> dict[str, Any]:
    def _update(current: dict[str, Any]) -> dict[str, Any]:
        current["chunks_written"] = int(current.get("chunks_written") or 0) + 1
        current["resume_from"] = int(confirmed_to_block)
        return current

    return update_last_dataset_job(storage, expected_job_id=job_id, update_fn=_update)
