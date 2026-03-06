"""Streaming orchestrator: fetch → decode → write chunk files.

Provides:
1) `fetch_decode(config, registry)` — convenience wrapper that wires concrete
   implementations (RPC, LocalChunkStorage or S3ChunkStorage) and runs the
   full pipeline.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import socket
import time
import uuid
from dataclasses import dataclass
from typing import Any

from defind.clients.rpc import RPC
from defind.core.config import OrchestratorConfig
from defind.core.interfaces import IChunkStorage, IEvmLogsProvider
from defind.core.use_cases.fetch_decode import (
    FetchDecodeConfig,
    FetchDecodeService,
    ProcessStats,
    WorkSeed,
    build_work_seeds,
)
from defind.decoding.registry import EventRegistryProvider
from defind.decoding.specs import EventRegistry
from defind.observability import bind_log_context, configure_logging, get_logger
from defind.orchestration.utils import (
    load_done_chunks,
    load_done_chunks_from_index,
    merge_intervals,
    save_done_chunks_to_index,
    subtract_iv,
)
from defind.storage.chunks import chunk_key, parse_chunk_key
from defind.storage.local import LocalChunkStorage
from defind.storage.s3 import S3ChunkStorage

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Output DTO
# ---------------------------------------------------------------------------


@dataclass(kw_only=True)
class FetchDecodeOutput:
    """High-level output of the orchestrator."""

    stats: ProcessStats
    contract_dir: str  # root key/path for all chunks of this contract


# ---------------------------------------------------------------------------
# Heartbeat
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class _ProgressState:
    """Shared mutable progress state consumed by the heartbeat loop."""

    mode: str
    start_block: int
    target_end_block: int
    last_indexed_block: int
    chain_head_block: int | None


@dataclass(slots=True)
class _WriterLock:
    """Lease lock used to prevent concurrent writers on one dataset."""

    key: str
    owner_id: str
    run_id: str
    ttl_s: int
    acquired_at_s: int


def _last_indexed_block(done_chunks: list[tuple[int, int]], *, start: int) -> int:
    if not done_chunks:
        return start - 1
    return max(end for _, end in done_chunks)


async def _heartbeat_loop(
    *,
    storage: IChunkStorage,
    key: str,
    interval_s: float,
    lag_warn_threshold_blocks: int,
    progress: _ProgressState,
) -> None:
    while True:
        lag_blocks: int | None = None
        if progress.chain_head_block is not None:
            lag_blocks = max(0, progress.chain_head_block - progress.last_indexed_block)

        payload = {
            "ts_unix_s": int(time.time()),
            "mode": progress.mode,
            "start_block": progress.start_block,
            "target_end_block": progress.target_end_block,
            "last_indexed_block": progress.last_indexed_block,
            "chain_head_block": progress.chain_head_block,
            "lag_blocks": lag_blocks,
        }

        try:
            storage.write_json(key, payload)
        except Exception as exc:
            logger.warning(
                "heartbeat_write_failed",
                extra={
                    "heartbeat_key": key,
                    "error": str(exc),
                },
            )

        base_extra = {
            "heartbeat_key": key,
            "mode": progress.mode,
            "last_indexed_block": progress.last_indexed_block,
            "chain_head_block": progress.chain_head_block,
            "lag_blocks": lag_blocks,
        }
        if lag_blocks is not None and lag_warn_threshold_blocks > 0 and lag_blocks > lag_warn_threshold_blocks:
            logger.warning("heartbeat_lag_high", extra=base_extra)
        else:
            logger.info("heartbeat", extra=base_extra)

        await asyncio.sleep(interval_s)


def _read_writer_lock(storage: IChunkStorage, key: str) -> dict[str, Any] | None:
    exists = storage.exists(key)
    if not exists:
        return None
    data = storage.read_json(key)
    if not isinstance(data, dict):
        raise RuntimeError(f"writer lock exists but is unreadable key={key}")
    return data


def _lock_expiry(lock_payload: dict[str, Any]) -> int:
    try:
        return int(lock_payload.get("expires_at_s", 0))
    except Exception:
        return 0


def _try_create_lock_if_absent(
    *,
    storage: IChunkStorage,
    key: str,
    payload: dict[str, Any],
) -> bool:
    create_if_absent = getattr(storage, "create_json_if_absent", None)
    if callable(create_if_absent) and hasattr(type(storage), "create_json_if_absent"):
        return bool(create_if_absent(key, payload))

    # Legacy fallback for backends without atomic-create support.
    if storage.exists(key):
        return False
    storage.write_json(key, payload)
    return True


def _acquire_writer_lock(
    *,
    storage: IChunkStorage,
    key: str,
    owner_id: str,
    run_id: str,
    ttl_s: int,
) -> _WriterLock:
    now = int(time.time())
    payload = {
        "version": 1,
        "owner_id": owner_id,
        "run_id": run_id,
        "hostname": socket.gethostname(),
        "pid": os.getpid(),
        "acquired_at_s": now,
        "refreshed_at_s": now,
        "expires_at_s": now + ttl_s,
    }
    created = _try_create_lock_if_absent(storage=storage, key=key, payload=payload)
    if not created:
        current = _read_writer_lock(storage, key)
        current_owner = str((current or {}).get("owner_id") or "")
        expires_at = _lock_expiry(current or {})
        if current_owner and current_owner != owner_id and expires_at > now:
            raise RuntimeError(f"writer lock is already held key={key} owner={current_owner} expires_at_s={expires_at}")

        # Stale lock takeover path.
        storage.delete(key)
        created = _try_create_lock_if_absent(storage=storage, key=key, payload=payload)
        if not created:
            check_existing = _read_writer_lock(storage, key)
            observed = None if not isinstance(check_existing, dict) else check_existing.get("owner_id")
            raise RuntimeError(f"failed to acquire writer lock key={key}; observed_owner={observed}")

    check = _read_writer_lock(storage, key)
    if not isinstance(check, dict) or str(check.get("owner_id") or "") != owner_id:
        observed = None if not isinstance(check, dict) else check.get("owner_id")
        raise RuntimeError(f"failed to acquire writer lock key={key}; observed_owner={observed}")

    return _WriterLock(
        key=key,
        owner_id=owner_id,
        run_id=run_id,
        ttl_s=ttl_s,
        acquired_at_s=now,
    )


def _refresh_writer_lock(
    *,
    storage: IChunkStorage,
    lock: _WriterLock,
) -> None:
    now = int(time.time())
    current = _read_writer_lock(storage, lock.key)
    if not isinstance(current, dict):
        raise RuntimeError(f"writer lock disappeared key={lock.key}")

    current_owner = str(current.get("owner_id") or "")
    if current_owner != lock.owner_id:
        raise RuntimeError(f"writer lock lost key={lock.key}; owner={current_owner} expected={lock.owner_id}")

    payload = {
        "version": 1,
        "owner_id": lock.owner_id,
        "run_id": lock.run_id,
        "hostname": socket.gethostname(),
        "pid": os.getpid(),
        "acquired_at_s": int(current.get("acquired_at_s") or lock.acquired_at_s),
        "refreshed_at_s": now,
        "expires_at_s": now + lock.ttl_s,
    }
    storage.write_json(lock.key, payload)
    check = _read_writer_lock(storage, lock.key)
    if not isinstance(check, dict) or str(check.get("owner_id") or "") != lock.owner_id:
        observed = None if not isinstance(check, dict) else check.get("owner_id")
        raise RuntimeError(f"writer lock verification failed key={lock.key}; observed_owner={observed}")


def _release_writer_lock(
    *,
    storage: IChunkStorage,
    lock: _WriterLock,
) -> None:
    try:
        current = _read_writer_lock(storage, lock.key)
    except Exception as exc:
        logger.warning(
            "writer_lock_release_read_failed",
            extra={"writer_lock_key": lock.key, "error": str(exc)},
        )
        return

    if not isinstance(current, dict):
        return
    current_owner = str(current.get("owner_id") or "")
    if current_owner != lock.owner_id:
        return

    try:
        storage.delete(lock.key)
    except Exception as exc:
        logger.warning(
            "writer_lock_release_delete_failed",
            extra={"writer_lock_key": lock.key, "error": str(exc)},
        )


async def _writer_lock_refresh_loop(
    *,
    storage: IChunkStorage,
    lock: _WriterLock,
    refresh_interval_s: float,
    lock_errors: list[Exception],
    lock_lost_event: asyncio.Event,
) -> None:
    while True:
        try:
            _refresh_writer_lock(storage=storage, lock=lock)
            logger.debug(
                "writer_lock_refreshed",
                extra={"writer_lock_key": lock.key, "writer_owner_id": lock.owner_id},
            )
        except Exception as exc:
            lock_errors.append(exc)
            lock_lost_event.set()
            logger.error(
                "writer_lock_refresh_failed",
                extra={"writer_lock_key": lock.key, "writer_owner_id": lock.owner_id, "error": str(exc)},
                exc_info=True,
            )
            return
        await asyncio.sleep(refresh_interval_s)


def _raise_if_writer_lock_lost(lock_errors: list[Exception]) -> None:
    if not lock_errors:
        return
    raise RuntimeError("writer lock lost during run") from lock_errors[0]


def _validate_index_fallback_recent_presence(
    *,
    storage: IChunkStorage,
    event_names: list[str],
    done_chunks: list[tuple[int, int]],
    sample_size: int = 25,
    min_ratio: float = 0.8,
) -> tuple[int, int, float]:
    if not done_chunks:
        return 0, 0, 1.0
    if sample_size <= 0:
        return 0, 0, 1.0

    recent = sorted(done_chunks, key=lambda iv: iv[1])[-sample_size:]
    present = 0
    for a, b in recent:
        if all(storage.exists(chunk_key(ev, a, b)) for ev in event_names):
            present += 1

    checked = len(recent)
    ratio = float(present) / float(checked)
    if ratio < min_ratio:
        raise RuntimeError(
            "index fallback validation failed: "
            f"present={present} checked={checked} ratio={ratio:.2f} min_ratio={min_ratio:.2f}"
        )
    return present, checked, ratio


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _resolve_block_range(
    logs_provider: IEvmLogsProvider,
    start_block: int | str,
    end_block: int | str,
) -> tuple[int, int]:
    def _parse_bound(raw: int | str, *, label: str) -> int:
        try:
            if isinstance(raw, int):
                out = raw
            else:
                out = int(raw, 0)
        except Exception as exc:
            raise ValueError(f"{label} must be an integer-like value") from exc
        if out < 0:
            raise ValueError(f"{label} must be >= 0")
        return out

    if isinstance(start_block, str) and start_block.lower() in ("earliest", "genesis"):
        start = 0
    else:
        start = _parse_bound(start_block, label="start_block")

    if isinstance(end_block, str) and end_block.lower() == "latest":
        end = await logs_provider.latest_block()
    else:
        end = _parse_bound(end_block, label="end_block")

    if start > end:
        raise ValueError("start_block must be <= end_block")

    return start, end


def _build_storage(config: OrchestratorConfig) -> tuple[IChunkStorage, str]:
    """Build the appropriate storage backend from config.

    Returns (storage, contract_dir_description) where contract_dir_description
    is a human-readable string for logging/output.
    """
    _validate_slug(config.protocol_slug, label="protocol_slug")
    _validate_slug(config.contract_slug, label="contract_slug")
    contract_subpath = f"{config.protocol_slug}/{config.contract_slug}"

    storage: IChunkStorage
    if config.s3_bucket:
        prefix = f"{config.s3_prefix.rstrip('/')}/{contract_subpath}/" if config.s3_prefix else f"{contract_subpath}/"
        storage = S3ChunkStorage(
            bucket=config.s3_bucket,
            prefix=prefix,
            endpoint_url=config.s3_endpoint_url,
            access_key=config.s3_access_key,
            secret_key=config.s3_secret_key,
            region=config.s3_region,
            max_retries=config.s3_max_retries,
            retry_backoff_s=config.s3_retry_backoff_s,
        )
        contract_dir = f"s3://{config.s3_bucket}/{prefix}"
    else:
        root = config.out_root / config.protocol_slug / config.contract_slug
        storage = LocalChunkStorage(root)
        contract_dir = str(root)

    return storage, contract_dir


def _merge_stats(total: ProcessStats, delta: ProcessStats) -> None:
    total.processed_ok += delta.processed_ok
    total.processed_failed += delta.processed_failed
    total.executed_subranges += delta.executed_subranges
    total.total_logs += delta.total_logs
    total.partially_covered_split += delta.partially_covered_split
    total.chunks_written += delta.chunks_written


def _validate_slug(value: str, *, label: str) -> None:
    if not value or not value.strip():
        raise ValueError(f"{label} must not be empty")
    if value != value.strip():
        raise ValueError(f"{label} must not have leading/trailing spaces")
    if any(sep in value for sep in ("/", "\\")) or ".." in value:
        raise ValueError(f"{label} must not contain path separators or '..'")


def _validate_runtime_config(*, config: OrchestratorConfig, effective_chunk_size: int) -> None:
    if config.step <= 0:
        raise ValueError("step must be > 0")
    if effective_chunk_size <= 0:
        raise ValueError("chunk_size must be > 0")
    if config.concurrency <= 0:
        raise ValueError("concurrency must be > 0")
    if config.listen_poll_interval_s < 0:
        raise ValueError("listen_poll_interval_s must be >= 0")
    if config.reorg_lookback_blocks < 0:
        raise ValueError("reorg_lookback_blocks must be >= 0")
    if config.s3_max_retries < 0:
        raise ValueError("s3_max_retries must be >= 0")
    if config.s3_retry_backoff_s < 0:
        raise ValueError("s3_retry_backoff_s must be >= 0")
    if config.heartbeat_interval_s < 0:
        raise ValueError("heartbeat_interval_s must be >= 0")
    if config.lag_warn_threshold_blocks < 0:
        raise ValueError("lag_warn_threshold_blocks must be >= 0")
    if not config.heartbeat_key or not config.heartbeat_key.strip():
        raise ValueError("heartbeat_key must not be empty")
    if not config.writer_lock_key or not config.writer_lock_key.strip():
        raise ValueError("writer_lock_key must not be empty")
    if config.writer_lock_ttl_s <= 0:
        raise ValueError("writer_lock_ttl_s must be > 0")
    if config.writer_lock_refresh_s <= 0:
        raise ValueError("writer_lock_refresh_s must be > 0")
    if config.writer_lock_refresh_s >= config.writer_lock_ttl_s:
        raise ValueError("writer_lock_refresh_s must be < writer_lock_ttl_s")
    if not config.log_level or not config.log_level.strip():
        raise ValueError("log_level must not be empty")
    if not config.address or not config.address.strip():
        raise ValueError("address must not be empty")
    if not config.topic0s:
        raise ValueError("topic0s must not be empty")
    _validate_slug(config.protocol_slug, label="protocol_slug")
    _validate_slug(config.contract_slug, label="contract_slug")


def _plan_seeds_with_tail_extension(
    *,
    current_start: int,
    current_end: int,
    chunk_size: int,
    done_chunks: list[tuple[int, int]],
) -> list[WorkSeed]:
    """Plan work seeds and optionally extend the latest partial chunk."""
    if current_start > current_end:
        return []

    covered = merge_intervals(done_chunks)
    seed_start = current_start

    if done_chunks:
        tail = max(done_chunks, key=lambda iv: iv[1])
        ta, tb = tail
        tail_len = tb - ta + 1
        target_tail_end = min(ta + chunk_size - 1, current_end)
        # Never rewind before current_start unless the tail is exactly contiguous.
        contiguous_left = tb == current_start - 1
        no_rewind = ta >= current_start or contiguous_left
        # Do not extend tail if there are uncovered holes before it.
        has_gap_before_tail = False
        if current_start < ta:
            has_gap_before_tail = bool(subtract_iv((current_start, ta - 1), covered))

        if tail_len < chunk_size and target_tail_end > tb and no_rewind and (not has_gap_before_tail):
            seed_start = ta
            covered = merge_intervals([iv for iv in done_chunks if iv != tail])

    seeds = build_work_seeds(
        start=seed_start,
        end=current_end,
        chunk_size=chunk_size,
        covered=covered,
    )
    return seeds


def _select_reorg_rewrite_seeds(
    *,
    done_chunks: list[tuple[int, int]],
    window_start: int,
    window_end: int,
) -> list[WorkSeed]:
    """Select existing chunk intervals that intersect the reorg window."""
    selected: list[WorkSeed] = []
    for a, b in sorted(set(done_chunks)):
        if b < window_start or a > window_end:
            continue
        selected.append(WorkSeed(start=a, end=b))
    return selected


def _plan_startup_small_interval_compaction(
    *,
    anchor_start: int,
    chunk_size: int,
    done_chunks: list[tuple[int, int]],
) -> list[WorkSeed]:
    """Compact fragmented intervals per canonical chunk window (< chunk_size)."""
    if chunk_size <= 0:
        return []

    exact = set(done_chunks)
    by_window: dict[int, list[tuple[int, int]]] = {}
    for a, b in done_chunks:
        # Startup compaction is bounded to this run's start block.
        if a < anchor_start:
            continue
        idx = (a - anchor_start) // chunk_size
        by_window.setdefault(idx, []).append((a, b))

    out: list[WorkSeed] = []
    for idx, parts in by_window.items():
        ws = anchor_start + idx * chunk_size
        we = ws + chunk_size - 1
        merged = merge_intervals(parts)
        if len(merged) != 1:
            continue
        ma, mb = merged[0]
        span = mb - ma + 1
        if ma != ws or span >= chunk_size:
            continue
        if len(set(parts)) <= 1:
            continue
        if (ma, mb) in exact:
            continue
        if mb > we:
            continue
        out.append(WorkSeed(start=ma, end=mb))
    return out


def _cleanup_redundant_old_chunks(
    *,
    storage: IChunkStorage,
    event_names: list[str],
    old_done_chunks: list[tuple[int, int]],
    seeds: list[WorkSeed],
) -> list[tuple[int, int]]:
    """Delete chunk files fully covered by newly written seeds.

    - First, removes intervals known as complete (`old_done_chunks`) across all events.
    - Then, removes any per-event orphan partials found during directory scan.
    """
    if not seeds:
        return []

    written = {(s.start, s.end) for s in seeds}
    orphan_intervals: set[tuple[int, int]] = set()

    for oa, ob in old_done_chunks:
        if (oa, ob) in written:
            continue
        if any(wa <= oa and ob <= wb for wa, wb in written):
            orphan_intervals.add((oa, ob))

    for ev in event_names:
        try:
            keys = storage.list_keys(f"{ev}/")
        except Exception as exc:
            # Non-critical maintenance path: keep running if backend listing is transiently unavailable.
            logger.warning(
                "cleanup_redundant_scan_failed",
                extra={"event_name": ev, "error": str(exc)},
            )
            continue
        for key in keys:
            parsed = parse_chunk_key(key)
            if parsed is not None:
                oa, ob = parsed
                if (oa, ob) in written:
                    continue
                if any(wa <= oa and ob <= wb for wa, wb in written):
                    orphan_intervals.add((oa, ob))

    # Keep event directories aligned: purge orphan intervals for every event.
    for oa, ob in sorted(orphan_intervals):
        for ev in event_names:
            storage.delete(chunk_key(ev, oa, ob))

    return sorted(orphan_intervals)


def _cleanup_overlapping_intervals(
    *,
    storage: IChunkStorage,
    event_names: list[str],
) -> list[tuple[int, int]]:
    """Delete per-event overlapping intervals to keep one non-overlapping chain.

    For each overlap pair, keeps the interval that loses less block coverage.
    """
    deleted: set[tuple[int, int]] = set()
    for ev in event_names:
        candidates: set[tuple[int, int]] = set()
        try:
            keys = storage.list_keys(f"{ev}/")
        except Exception as exc:
            logger.warning(
                "cleanup_overlaps_scan_failed",
                extra={"event_name": ev, "error": str(exc)},
            )
            return []
        for key in keys:
            parsed = parse_chunk_key(key)
            if parsed is not None:
                candidates.add(parsed)

        intervals = sorted(candidates)
        keep: list[tuple[int, int]] = []
        to_delete: set[tuple[int, int]] = set()
        for a, b in intervals:
            if not keep:
                keep.append((a, b))
                continue

            ka, kb = keep[-1]
            if a > kb:
                keep.append((a, b))
                continue

            # Overlap: choose the option with smaller uncovered loss.
            loss_replace = max(0, a - ka)
            loss_keep = max(0, b - kb)
            if loss_replace < loss_keep:
                to_delete.add((ka, kb))
                keep[-1] = (a, b)
            elif loss_keep < loss_replace:
                to_delete.add((a, b))
            else:
                # Tie-breaker: prefer the wider interval.
                if (b - a) > (kb - ka):
                    to_delete.add((ka, kb))
                    keep[-1] = (a, b)
                else:
                    to_delete.add((a, b))

        for a, b in sorted(to_delete):
            storage.delete(chunk_key(ev, a, b))
            deleted.add((a, b))
    return sorted(deleted)


def _safe_save_done_chunks_index(
    storage: IChunkStorage,
    event_names: list[str],
    done_chunks: list[tuple[int, int]],
) -> None:
    try:
        save_done_chunks_to_index(storage, event_names, done_chunks)
    except Exception as exc:
        # Coverage index is a cache hint: never fail the pipeline on index write issues.
        logger.warning("coverage_index_write_failed", extra={"error": str(exc)})


def _load_done_chunks_with_index_fallback(
    storage: IChunkStorage,
    event_names: list[str],
) -> tuple[list[tuple[int, int]], str]:
    """Load done chunks from scan, with index fallback when scan backend fails."""
    try:
        scanned = load_done_chunks(storage, event_names)
        return scanned, "scan"
    except Exception as exc:
        indexed = load_done_chunks_from_index(storage, event_names)
        if indexed is None:
            logger.error("done_chunk_scan_failed_no_index", extra={"error": str(exc)}, exc_info=True)
            raise
        try:
            present, checked, ratio = _validate_index_fallback_recent_presence(
                storage=storage,
                event_names=event_names,
                done_chunks=indexed,
            )
        except Exception as validation_exc:
            logger.error(
                "done_chunk_scan_failed_index_rejected",
                extra={"error": str(exc), "validation_error": str(validation_exc)},
                exc_info=True,
            )
            raise
        logger.warning(
            "done_chunk_scan_failed_using_index",
            extra={
                "error": str(exc),
                "indexed_chunk_count": len(indexed),
                "indexed_recent_checked": checked,
                "indexed_recent_present": present,
                "indexed_recent_ratio": ratio,
            },
        )
        return indexed, "index"


# ---------------------------------------------------------------------------
# Main entrypoint
# ---------------------------------------------------------------------------


async def fetch_decode(
    *,
    config: OrchestratorConfig,
    registry: EventRegistry,
) -> FetchDecodeOutput:
    """Convenience wrapper: wires concrete implementations and runs the pipeline.

    - Resolves block range (handles "latest").
    - Builds storage backend (local or S3).
    - Loads coverage from existing chunk files.
    - Runs fetch → decode → write for uncovered ranges.
    - If `config.listen=True`, continues polling for new blocks after backfill.
    """
    if not registry:
        raise ValueError("registry must not be empty")
    if len(set(registry.keys())) != len(registry):
        raise ValueError("registry topic0 keys must be unique")
    for key, spec in registry.items():
        if key != key.lower():
            raise ValueError("registry topic0 keys must be lower-case")
        if key != spec.topic0.lower():
            raise ValueError("registry key must match spec.topic0")

    registry_provider = EventRegistryProvider(registry)
    event_names = [spec.name for spec in registry.values()]
    if any(not name or not str(name).strip() for name in event_names):
        raise ValueError("registry event names must not be empty")
    if any(any(sep in name for sep in ("/", "\\")) or ".." in name for name in event_names):
        raise ValueError("registry event names must not contain path separators or '..'")
    if len(set(event_names)) != len(event_names):
        raise ValueError("registry event names must be unique")

    configure_logging(level=config.log_level, json_logs=config.log_json)
    run_id = uuid.uuid4().hex
    dataset_id = f"{config.protocol_slug}/{config.contract_slug}"

    rpc = RPC(
        config.rpc_url,
        timeout_s=config.timeout_s,
        max_connections=max(32, 2 * config.concurrency),
        max_retries=config.rpc_max_retries,
        retry_backoff_s=config.rpc_retry_backoff_s,
    )

    heartbeat_task: asyncio.Task[None] | None = None
    writer_lock: _WriterLock | None = None
    writer_lock_task: asyncio.Task[None] | None = None
    writer_lock_errors: list[Exception] = []
    writer_lock_lost_event = asyncio.Event()
    storage: IChunkStorage | None = None
    total_stats = ProcessStats()
    contract_dir = ""

    with bind_log_context(run_id=run_id, dataset_id=dataset_id):
        try:
            storage, contract_dir = _build_storage(config)
            effective_chunk_size = config.chunk_size if config.chunk_size is not None else config.step
            _validate_runtime_config(config=config, effective_chunk_size=effective_chunk_size)

            if config.single_writer_guard:
                owner_id = f"{socket.gethostname()}:{os.getpid()}:{run_id}"
                writer_lock = _acquire_writer_lock(
                    storage=storage,
                    key=config.writer_lock_key,
                    owner_id=owner_id,
                    run_id=run_id,
                    ttl_s=config.writer_lock_ttl_s,
                )
                logger.info(
                    "writer_lock_acquired",
                    extra={
                        "writer_lock_key": writer_lock.key,
                        "writer_owner_id": writer_lock.owner_id,
                        "writer_ttl_s": writer_lock.ttl_s,
                    },
                )
                writer_lock_task = asyncio.create_task(
                    _writer_lock_refresh_loop(
                        storage=storage,
                        lock=writer_lock,
                        refresh_interval_s=config.writer_lock_refresh_s,
                        lock_errors=writer_lock_errors,
                        lock_lost_event=writer_lock_lost_event,
                    )
                )

            domain_config = FetchDecodeConfig(
                address=config.address,
                topic0s=config.topic0s,
                step=config.step,
                chunk_size=effective_chunk_size,
                concurrency=config.concurrency,
                codec=config.codec,
                print_chunk_writes=config.print_chunk_writes,
            )

            service = FetchDecodeService(
                logs_provider=rpc,
                registry_provider=registry_provider,
            )

            start, end = await _resolve_block_range(rpc, config.start_block, config.end_block)
            current_start = start
            current_end = end

            progress = _ProgressState(
                mode="backfill",
                start_block=start,
                target_end_block=end,
                last_indexed_block=start - 1,
                chain_head_block=(
                    end if isinstance(config.end_block, str) and config.end_block.lower() == "latest" else None
                ),
            )

            if config.heartbeat_interval_s > 0:
                heartbeat_task = asyncio.create_task(
                    _heartbeat_loop(
                        storage=storage,
                        key=config.heartbeat_key,
                        interval_s=config.heartbeat_interval_s,
                        lag_warn_threshold_blocks=config.lag_warn_threshold_blocks,
                        progress=progress,
                    )
                )

            logger.info(
                "indexer_started",
                extra={
                    "contract_dir": contract_dir,
                    "listen_mode": config.listen,
                    "start_block": start,
                    "end_block": end,
                    "chunk_size": effective_chunk_size,
                    "step": config.step,
                    "concurrency": config.concurrency,
                },
            )

            # Startup consistency check: files are the source of truth.
            scanned_done_chunks, startup_source = _load_done_chunks_with_index_fallback(storage, event_names)
            indexed_done_chunks = load_done_chunks_from_index(storage, event_names)
            if startup_source == "scan":
                if indexed_done_chunks is None or indexed_done_chunks != scanned_done_chunks:
                    done_chunks_state = scanned_done_chunks
                    _safe_save_done_chunks_index(storage, event_names, done_chunks_state)
                    logger.warning(
                        "coverage_index_stale_or_missing",
                        extra={
                            "indexed_chunk_count": 0 if indexed_done_chunks is None else len(indexed_done_chunks),
                            "scanned_chunk_count": len(scanned_done_chunks),
                        },
                    )
                else:
                    done_chunks_state = indexed_done_chunks
            else:
                done_chunks_state = scanned_done_chunks

            progress.last_indexed_block = _last_indexed_block(done_chunks_state, start=start)

            # One-shot compaction of legacy overlaps created by interrupted runs.
            deleted_contained = _cleanup_overlapping_intervals(
                storage=storage,
                event_names=event_names,
            )
            if deleted_contained:
                logger.warning(
                    "legacy_overlap_cleanup",
                    extra={"deleted_intervals": len(deleted_contained)},
                )
                done_chunks_state, _ = _load_done_chunks_with_index_fallback(storage, event_names)
                _safe_save_done_chunks_index(storage, event_names, done_chunks_state)
                progress.last_indexed_block = _last_indexed_block(done_chunks_state, start=start)

            # Startup compaction: if a merged covered range is < chunk_size but split
            # across multiple files, rewrite it as one interval before normal planning.
            startup_compaction_seeds = _plan_startup_small_interval_compaction(
                anchor_start=start,
                chunk_size=effective_chunk_size,
                done_chunks=done_chunks_state,
            )
            if startup_compaction_seeds:
                logger.info(
                    "startup_compaction_planned",
                    extra={"seed_count": len(startup_compaction_seeds)},
                )
                delta_stats = await service.run(
                    config=domain_config,
                    storage=storage,
                    seeds=startup_compaction_seeds,
                    force_reprocess=True,
                    stop_event=writer_lock_lost_event if config.single_writer_guard else None,
                )
                _merge_stats(total_stats, delta_stats)
                _cleanup_redundant_old_chunks(
                    storage=storage,
                    event_names=event_names,
                    old_done_chunks=done_chunks_state,
                    seeds=startup_compaction_seeds,
                )
                done_chunks_state, _ = _load_done_chunks_with_index_fallback(storage, event_names)
                _safe_save_done_chunks_index(storage, event_names, done_chunks_state)
                progress.last_indexed_block = _last_indexed_block(done_chunks_state, start=start)

            is_backfill_pass = True
            seen_batch_plans: set[tuple[tuple[int, int], ...]] = set()
            while True:
                _raise_if_writer_lock_lost(writer_lock_errors)
                done_chunks_state, _ = _load_done_chunks_with_index_fallback(storage, event_names)
                progress.last_indexed_block = _last_indexed_block(done_chunks_state, start=start)

                if config.listen and (not is_backfill_pass) and config.reorg_lookback_blocks > 0:
                    window_start = max(start, current_end - config.reorg_lookback_blocks + 1)
                    reorg_seeds = _select_reorg_rewrite_seeds(
                        done_chunks=done_chunks_state,
                        window_start=window_start,
                        window_end=current_end,
                    )
                    if reorg_seeds:
                        logger.warning(
                            "reorg_rewrite_planned",
                            extra={
                                "window_start": window_start,
                                "window_end": current_end,
                                "seed_count": len(reorg_seeds),
                            },
                        )
                        delta_stats = await service.run(
                            config=domain_config,
                            storage=storage,
                            seeds=reorg_seeds,
                            force_reprocess=True,
                            stop_event=writer_lock_lost_event if config.single_writer_guard else None,
                        )
                        _merge_stats(total_stats, delta_stats)

                        done_chunks_state, _ = _load_done_chunks_with_index_fallback(storage, event_names)
                        _safe_save_done_chunks_index(storage, event_names, done_chunks_state)
                        progress.last_indexed_block = _last_indexed_block(done_chunks_state, start=start)

                seeds = _plan_seeds_with_tail_extension(
                    current_start=current_start,
                    current_end=current_end,
                    chunk_size=effective_chunk_size,
                    done_chunks=done_chunks_state,
                )

                if not config.listen:
                    if not seeds:
                        break
                    plan_key = tuple((s.start, s.end) for s in seeds)
                    if plan_key in seen_batch_plans:
                        logger.warning("plan_cycle_guard_triggered", extra={"seed_count": len(seeds)})
                        break
                    seen_batch_plans.add(plan_key)

                if seeds:
                    logger.info(
                        "seed_batch_start",
                        extra={
                            "seed_count": len(seeds),
                            "first_seed_start": seeds[0].start,
                            "last_seed_end": seeds[-1].end,
                            "force_reprocess": False,
                        },
                    )
                    delta_stats = await service.run(
                        config=domain_config,
                        storage=storage,
                        seeds=seeds,
                        force_reprocess=False,
                        stop_event=writer_lock_lost_event if config.single_writer_guard else None,
                    )
                    _merge_stats(total_stats, delta_stats)
                    _cleanup_redundant_old_chunks(
                        storage=storage,
                        event_names=event_names,
                        old_done_chunks=done_chunks_state,
                        seeds=seeds,
                    )
                    # The file scan is the source of truth (covers split writes and concurrent writers).
                    done_chunks_state, _ = _load_done_chunks_with_index_fallback(storage, event_names)
                    _safe_save_done_chunks_index(storage, event_names, done_chunks_state)
                    progress.last_indexed_block = _last_indexed_block(done_chunks_state, start=start)

                if not config.listen:
                    # Keep iterating in batch mode until no more backfill/compaction seeds.
                    continue

                next_min_block = current_end + 1
                progress.mode = "listen_wait"
                while True:
                    _raise_if_writer_lock_lost(writer_lock_errors)
                    latest = await rpc.latest_block()
                    progress.chain_head_block = latest
                    if latest >= next_min_block:
                        current_end = latest
                        current_start = next_min_block
                        progress.target_end_block = current_end
                        progress.mode = "listen_backfill"
                        logger.info(
                            "new_head_detected",
                            extra={
                                "next_min_block": next_min_block,
                                "latest_block": latest,
                            },
                        )
                        break
                    await asyncio.sleep(config.listen_poll_interval_s)

                is_backfill_pass = False

        finally:
            if writer_lock_task is not None:
                writer_lock_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await writer_lock_task

            if writer_lock is not None and storage is not None:
                _release_writer_lock(storage=storage, lock=writer_lock)
                logger.info(
                    "writer_lock_released",
                    extra={
                        "writer_lock_key": writer_lock.key,
                        "writer_owner_id": writer_lock.owner_id,
                    },
                )

            if heartbeat_task is not None:
                heartbeat_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await heartbeat_task

            await rpc.aclose()
            logger.info(
                "indexer_stopped",
                extra={
                    "contract_dir": contract_dir,
                    "processed_ok": total_stats.processed_ok,
                    "processed_failed": total_stats.processed_failed,
                    "chunks_written": total_stats.chunks_written,
                    "total_logs": total_stats.total_logs,
                },
            )

    return FetchDecodeOutput(stats=total_stats, contract_dir=contract_dir)
