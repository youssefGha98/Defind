"""Streaming orchestrator: fetch → decode → write chunk files.

Provides:
1) `fetch_decode(config, registry)` — convenience wrapper that wires concrete
   implementations (RPC, LocalChunkStorage or S3ChunkStorage) and runs the
   full pipeline.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

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

# ---------------------------------------------------------------------------
# Output DTO
# ---------------------------------------------------------------------------


@dataclass(kw_only=True)
class FetchDecodeOutput:
    """High-level output of the orchestrator."""
    stats: ProcessStats
    contract_dir: str   # root key/path for all chunks of this contract


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
        contiguous_left = (tb == current_start - 1)
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
        for key in storage.list_keys(f"{ev}/"):
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
        for key in storage.list_keys(f"{ev}/"):
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
    except Exception:
        # Coverage index is a cache hint: never fail the pipeline on index write issues.
        pass


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

    rpc = RPC(
        config.rpc_url,
        timeout_s=config.timeout_s,
        max_connections=max(32, 2 * config.concurrency),
        max_retries=config.rpc_max_retries,
        retry_backoff_s=config.rpc_retry_backoff_s,
    )

    storage, contract_dir = _build_storage(config)

    try:
        effective_chunk_size = config.chunk_size if config.chunk_size is not None else config.step
        _validate_runtime_config(config=config, effective_chunk_size=effective_chunk_size)

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
        total_stats = ProcessStats()

        start, end = await _resolve_block_range(rpc, config.start_block, config.end_block)
        current_start = start
        current_end = end

        # Startup consistency check: files are the source of truth.
        scanned_done_chunks = load_done_chunks(storage, event_names)
        indexed_done_chunks = load_done_chunks_from_index(storage, event_names)
        if indexed_done_chunks is None or indexed_done_chunks != scanned_done_chunks:
            done_chunks_state = scanned_done_chunks
            _safe_save_done_chunks_index(storage, event_names, done_chunks_state)
        else:
            done_chunks_state = indexed_done_chunks

        # One-shot compaction of legacy overlaps created by interrupted runs.
        deleted_contained = _cleanup_overlapping_intervals(
            storage=storage,
            event_names=event_names,
        )
        if deleted_contained:
            done_chunks_state = load_done_chunks(storage, event_names)
            _safe_save_done_chunks_index(storage, event_names, done_chunks_state)

        # Startup compaction: if a merged covered range is < chunk_size but split
        # across multiple files, rewrite it as one interval before normal planning.
        startup_compaction_seeds = _plan_startup_small_interval_compaction(
            anchor_start=start,
            chunk_size=effective_chunk_size,
            done_chunks=done_chunks_state,
        )
        if startup_compaction_seeds:
            delta_stats = await service.run(
                config=domain_config,
                storage=storage,
                seeds=startup_compaction_seeds,
                force_reprocess=True,
            )
            _merge_stats(total_stats, delta_stats)
            _cleanup_redundant_old_chunks(
                storage=storage,
                event_names=event_names,
                old_done_chunks=done_chunks_state,
                seeds=startup_compaction_seeds,
            )
            done_chunks_state = load_done_chunks(storage, event_names)
            _safe_save_done_chunks_index(storage, event_names, done_chunks_state)

        is_backfill_pass = True
        seen_batch_plans: set[tuple[tuple[int, int], ...]] = set()
        while True:
            done_chunks_state = load_done_chunks(storage, event_names)

            if config.listen and (not is_backfill_pass) and config.reorg_lookback_blocks > 0:
                window_start = max(start, current_end - config.reorg_lookback_blocks + 1)
                reorg_seeds = _select_reorg_rewrite_seeds(
                    done_chunks=done_chunks_state,
                    window_start=window_start,
                    window_end=current_end,
                )
                if reorg_seeds:
                    delta_stats = await service.run(
                        config=domain_config,
                        storage=storage,
                        seeds=reorg_seeds,
                        force_reprocess=True,
                    )
                    _merge_stats(total_stats, delta_stats)

                    done_chunks_state = load_done_chunks(storage, event_names)
                    _safe_save_done_chunks_index(storage, event_names, done_chunks_state)

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
                    break
                seen_batch_plans.add(plan_key)

            if seeds:
                delta_stats = await service.run(
                    config=domain_config,
                    storage=storage,
                    seeds=seeds,
                    force_reprocess=False,
                )
                _merge_stats(total_stats, delta_stats)
                _cleanup_redundant_old_chunks(
                    storage=storage,
                    event_names=event_names,
                    old_done_chunks=done_chunks_state,
                    seeds=seeds,
                )
                # The file scan is the source of truth (covers split writes and concurrent writers).
                done_chunks_state = load_done_chunks(storage, event_names)
                _safe_save_done_chunks_index(storage, event_names, done_chunks_state)

            if not config.listen:
                # Keep iterating in batch mode until no more backfill/compaction seeds.
                continue

            next_min_block = current_end + 1
            while True:
                latest = await rpc.latest_block()
                if latest >= next_min_block:
                    current_end = latest
                    current_start = next_min_block
                    break
                await asyncio.sleep(config.listen_poll_interval_s)

            is_backfill_pass = False

    finally:
        await rpc.aclose()

    return FetchDecodeOutput(stats=total_stats, contract_dir=contract_dir)
