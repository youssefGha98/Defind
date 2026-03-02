"""Streaming orchestrator: fetch → decode → write chunk files.

Provides:
1) `fetch_decode(config, registry)` — convenience wrapper that wires concrete
   implementations (RPC, LocalChunkStorage or S3ChunkStorage) and runs the
   full pipeline.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from pathlib import Path

from defind.core.config import OrchestratorConfig
from defind.core.interfaces import IChunkStorage
from defind.core.use_cases.fetch_decode import (
    FetchDecodeConfig,
    FetchDecodeService,
    ProcessStats,
    WorkSeed,
    build_work_seeds,
)
from defind.decoding.registry import EventRegistryProvider
from defind.decoding.specs import EventRegistry
from defind.clients.rpc import RPC
from defind.orchestration.utils import load_done_chunks, merge_intervals
from defind.storage.chunks import chunk_key
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
    logs_provider,
    start_block: int | str,
    end_block: int | str,
) -> tuple[int, int]:
    if isinstance(start_block, str) and start_block.lower() in ("earliest", "genesis"):
        start = 0
    else:
        start = int(start_block)

    if isinstance(end_block, str) and end_block.lower() == "latest":
        end = await logs_provider.latest_block()
    else:
        end = int(end_block)

    if start > end:
        raise ValueError("start_block must be <= end_block")

    return start, end


def _build_storage(config: OrchestratorConfig) -> tuple[IChunkStorage, str]:
    """Build the appropriate storage backend from config.

    Returns (storage, contract_dir_description) where contract_dir_description
    is a human-readable string for logging/output.
    """
    contract_subpath = f"{config.protocol_slug}/{config.contract_slug}"

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


def _plan_seeds_with_tail_extension(
    *,
    current_start: int,
    current_end: int,
    chunk_size: int,
    done_chunks: list[tuple[int, int]],
) -> tuple[list[WorkSeed], tuple[int, int] | None]:
    """Plan work seeds and optionally extend the latest partial chunk."""
    if current_start > current_end:
        return [], None

    covered = merge_intervals(done_chunks)
    seed_start = current_start
    old_tail_to_replace: tuple[int, int] | None = None

    if done_chunks:
        tail = max(done_chunks, key=lambda iv: iv[1])
        ta, tb = tail
        tail_len = tb - ta + 1
        target_tail_end = min(ta + chunk_size - 1, current_end)
        if tail_len < chunk_size and target_tail_end > tb:
            old_tail_to_replace = tail
            seed_start = min(current_start, ta)
            covered = merge_intervals([iv for iv in done_chunks if iv != tail])

    seeds = build_work_seeds(
        start=seed_start,
        end=current_end,
        chunk_size=chunk_size,
        covered=covered,
    )
    return seeds, old_tail_to_replace


def _has_extended_tail_seed(
    seeds: list[WorkSeed],
    old_tail: tuple[int, int],
) -> bool:
    ta, tb = old_tail
    return any(s.start == ta and s.end > tb for s in seeds)


def _delete_old_chunk_interval(
    storage: IChunkStorage,
    event_names: list[str],
    interval: tuple[int, int],
) -> None:
    a, b = interval
    for ev in event_names:
        storage.delete(chunk_key(ev, a, b))


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
    registry_provider = EventRegistryProvider(registry)
    event_names = [spec.name for spec in registry.values()]

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

        while True:
            done_chunks = load_done_chunks(storage, event_names)
            seeds, old_tail_to_replace = _plan_seeds_with_tail_extension(
                current_start=current_start,
                current_end=current_end,
                chunk_size=effective_chunk_size,
                done_chunks=done_chunks,
            )
            delta_stats = await service.run(
                config=domain_config,
                storage=storage,
                seeds=seeds,
            )
            _merge_stats(total_stats, delta_stats)

            if (
                old_tail_to_replace is not None
                and _has_extended_tail_seed(seeds, old_tail_to_replace)
            ):
                _delete_old_chunk_interval(storage, event_names, old_tail_to_replace)

            if not config.listen:
                break

            current_start = current_end + 1
            while True:
                latest = await rpc.latest_block()
                if latest >= current_start:
                    current_end = latest
                    break
                await asyncio.sleep(config.listen_poll_interval_s)

    finally:
        await rpc.aclose()

    return FetchDecodeOutput(stats=total_stats, contract_dir=contract_dir)
