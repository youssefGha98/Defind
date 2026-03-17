from __future__ import annotations

import threading
import uuid
from collections.abc import Callable
from importlib import import_module
from typing import Any, TypeVar, cast

from defind.core.dataset_state.constants import JOBS_LOCK_KEY, UNSUPPORTED_LOCK_MARKERS
from defind.core.interfaces import IChunkStorage
from defind.core.observability import get_logger
from defind.orchestration.orchestrator import _acquire_writer_lock, _release_writer_lock

logger = get_logger(__name__)

T = TypeVar("T")

PROCESS_LOCAL_LOCKS: dict[str, threading.Lock] = {}
PROCESS_LOCAL_LOCKS_GUARD = threading.Lock()
DEGRADATION_WARNINGS: set[str] = set()


def read_json_with_version(
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


def write_json_if_version(
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


def storage_lock_id(storage: IChunkStorage, key: str) -> str:
    root = getattr(storage, "root", None)
    bucket = getattr(storage, "bucket", None)
    prefix = getattr(storage, "prefix", None)
    return f"{type(storage).__name__}:{root!s}:{bucket!s}:{prefix!s}:{key}"


def process_local_lock(storage: IChunkStorage, key: str) -> threading.Lock:
    lock_id = storage_lock_id(storage, key)
    with PROCESS_LOCAL_LOCKS_GUARD:
        lock = PROCESS_LOCAL_LOCKS.get(lock_id)
        if lock is None:
            lock = threading.Lock()
            PROCESS_LOCAL_LOCKS[lock_id] = lock
        return lock


def looks_unsupported_lock_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return any(marker in message for marker in UNSUPPORTED_LOCK_MARKERS)


def warn_local_only_degradation(storage: IChunkStorage, key: str, *, reason: str) -> None:
    warning_id = storage_lock_id(storage, key)
    if warning_id in DEGRADATION_WARNINGS:
        return
    DEGRADATION_WARNINGS.add(warning_id)
    logger.warning(
        "dataset_state_lock_degraded_local_only",
        extra={
            "key": key,
            "storage": type(storage).__name__,
            "reason": reason,
        },
    )


def storage_supports_atomic_locks(storage: IChunkStorage) -> bool:
    checker = getattr(storage, "supports_atomic_locks", None)
    if not callable(checker):
        return True
    try:
        return bool(checker())
    except Exception:
        return True


def with_jobs_lock(storage: IChunkStorage, fn: Callable[[], T]) -> T:
    compatibility_module = import_module("defind.dataset_state")
    acquire_writer_lock = getattr(compatibility_module, "_acquire_writer_lock", _acquire_writer_lock)
    release_writer_lock = getattr(compatibility_module, "_release_writer_lock", _release_writer_lock)

    if not storage_supports_atomic_locks(storage):
        with process_local_lock(storage, JOBS_LOCK_KEY):
            return fn()
    try:
        lock = acquire_writer_lock(
            storage=storage,
            key=JOBS_LOCK_KEY,
            owner_id=f"dataset-jobs:{uuid.uuid4().hex}",
            run_id=uuid.uuid4().hex,
            ttl_s=30,
        )
    except Exception as exc:
        if looks_unsupported_lock_error(exc):
            warn_local_only_degradation(storage, JOBS_LOCK_KEY, reason=str(exc))
            with process_local_lock(storage, JOBS_LOCK_KEY):
                return fn()
        raise
    try:
        return fn()
    finally:
        release_writer_lock(storage=storage, lock=lock)
