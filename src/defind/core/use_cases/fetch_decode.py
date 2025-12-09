from __future__ import annotations

import asyncio
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import List, Tuple, Dict

from defind.core.interfaces import (
    IEvmLogsProvider,
    IManifestRepository,
    IEventShardsRepository,
    IEventRegistryProvider,
)
from defind.core.models import ChunkRecord, ExtendedChunkRecord, Column, EventLog, Meta
from defind.decoding.decoder import decode_event
from defind.decoding.specs import EventRegistry
from defind.orchestration.utils import iter_chunks, subtract_iv


# ---------------------------------------------------------------------------
# Domain configuration
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FetchDecodeConfig:
    """
    Domain-level configuration for the fetch-decode use case.

    This config is intentionally free of infrastructure concerns
    (no RPC URL, no filesystem paths, etc.).
    """

    address: str
    topic0s: list[str]
    step: int
    concurrency: int
    batch_decode_rows: int
    protocol_slug: str | None = None
    contract_slug: str | None = None
    shards_layout: str = "legacy"  # "legacy" or "per_event"
    extended_manifest: bool = False


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------


@dataclass(kw_only=True)
class ProcessStats:
    """
    Aggregated counters for the fetch-decode pipeline.

    This object is mutated by workers to track:
    - how many subranges were executed
    - how many logs were fetched
    - how many ranges succeeded / failed
    - how many shards were written
    """

    processed_ok: int = 0
    processed_failed: int = 0
    executed_subranges: int = 0
    total_logs: int = 0
    partially_covered_split: int = 0
    shards_written: int = 0


# ---------------------------------------------------------------------------
# Processing context
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class ProcessContext:
    """
    Shared state for interval processing (keeps worker signatures small).

    This context is expressed in terms of domain interfaces:

    - `rpc` is an IEvmLogsProvider: the domain does not care if logs come
      from a live RPC node, an archive DB, or an in-memory provider.

    - `manifest` is an IManifestRepository: the domain only appends
      ChunkRecord entries; coverage aggregation is handled at the application level.

    - `aggregator` is an IEventShardsRepository: the domain emits decoded
      batches of events and only cares about how many shards were produced.

    Concurrency primitives (Semaphore, Lock) live here because they are part of
    the worker orchestration strategy, but they are not tied to any specific
    infrastructure technology.
    """

    rpc: IEvmLogsProvider
    address: str
    topic0s: list[str]
    registry: EventRegistry
    batch_decode_rows: int
    sem: asyncio.Semaphore
    agg_lock: asyncio.Lock
    manifest: IManifestRepository
    aggregator: IEventShardsRepository
    stats: ProcessStats
    shards_layout: str
    extended_manifest: bool

@dataclass(frozen=True)
class WorkSeed:
    """Inclusive block interval to process."""
    start: int
    end: int

    def split(self) -> tuple[WorkSeed, WorkSeed]:
        mid = (self.start + self.end) // 2
        return (
            WorkSeed(self.start, mid),
            WorkSeed(mid + 1, self.end)
        )

# ---------------------------------------------------------------------------
# Chunk record helpers
# ---------------------------------------------------------------------------


def _create_started_record(a: int, b: int) -> ChunkRecord:
    """Create a 'started' chunk record."""
    return ChunkRecord(
        from_block=a,
        to_block=b,
        status="started",
        attempts=0,
        error=None,
        logs=0,
        decoded=0,
        shards=0,
        updated_at=time.time(),
        filtered=0,
    )


def _create_done_record(
    a: int,
    b: int,
    logs: int,
    decoded: int,
    shards: int,
    filtered: int,
) -> ChunkRecord:
    """Create a 'done' chunk record."""
    return ChunkRecord(
        from_block=a,
        to_block=b,
        status="done",
        attempts=1,
        error=None,
        logs=logs,
        decoded=decoded,
        shards=shards,
        updated_at=time.time(),
        filtered=filtered,
    )


def _create_failed_record(a: int, b: int, error: str) -> ChunkRecord:
    """Create a 'failed' chunk record."""
    return ChunkRecord(
        from_block=a,
        to_block=b,
        status="failed",
        attempts=1,
        error=error,
        logs=0,
        decoded=0,
        shards=0,
        updated_at=time.time(),
        filtered=0,
    )


# ---------------------------------------------------------------------------
# Work seeds builder
# ---------------------------------------------------------------------------


def build_work_seeds(
    start: int,
    end: int,
    step: int,
    covered: List[Tuple[int, int]],
) -> List[WorkSeed]:
    """
    Build a list of uncovered block intervals to process.
    """
    seeds: list[WorkSeed] = []
    for uncovered_start, uncovered_end in subtract_iv((start, end), covered):
        for a, b in iter_chunks(uncovered_start, uncovered_end, step):
            seeds.append(WorkSeed(start=a, end=b))
    return seeds


# ---------------------------------------------------------------------------
# Core log processing
# ---------------------------------------------------------------------------


async def _process_logs_core(
    ctx: ProcessContext,
    logs: list[EventLog],
) -> tuple[int, int, int, dict[str, int], list[Path]]:
    """
    Core decoding logic shared by legacy and event strategies.
    
    Returns
    -------
    (decoded_rows, filtered_rows, shards_written_count, rows_by_event, all_written_paths)
    """
    buf = Column.empty()
    rows_in_batch = 0
    decoded_rows = 0
    filtered_rows = 0
    shards_written_count = 0
    rows_by_event: dict[str, int] = defaultdict(int)
    all_written_paths: list[Path] = []

    for ev in logs:
        if not ev.topics:
            filtered_rows += 1
            continue

        # Decode raw hex data payload
        data_hex = ev.data_hex[2:] if ev.data_hex.lower().startswith("0x") else ev.data_hex
        data_bytes = bytes.fromhex(data_hex) if data_hex else b""

        meta = Meta(
            block_number=ev.block_number,
            block_timestamp=ev.block_timestamp,
            tx_hash=ev.tx_hash,
            log_index=ev.log_index,
            address=ev.address,
        )

        pe = decode_event(
            topics=ev.topics,
            data=data_bytes,
            meta=meta,
            registry=ctx.registry,
        )
        if pe is None:
            filtered_rows += 1
            continue

        # Append all dynamic keys in one go
        buf.append_from_parsed(
            pe_name=pe.name,
            meta=meta,
            values=pe.values,
            contract_addr=pe.pool,
        )
        rows_in_batch += 1
        decoded_rows += 1
        rows_by_event[pe.name] += 1

        if rows_in_batch >= ctx.batch_decode_rows:
            async with ctx.agg_lock:
                written = ctx.aggregator.add(buf)
            shards_written_count += len(written)
            all_written_paths.extend(written)
            buf = Column.empty()
            rows_in_batch = 0

    # Flush last partial batch for this interval
    if rows_in_batch > 0:
        async with ctx.agg_lock:
            written = ctx.aggregator.add(buf)
        shards_written_count += len(written)
        all_written_paths.extend(written)

    return decoded_rows, filtered_rows, shards_written_count, dict(rows_by_event), all_written_paths


async def _process_logs_legacy(
    ctx: ProcessContext,
    logs: list[EventLog],
) -> tuple[int, int, int, dict[str, int], dict[str, int]]:
    """Legacy log processing: ignores per-event shard tracking."""
    decoded, filtered, shards_written, rows_by_event, _ = await _process_logs_core(ctx, logs)
    return decoded, filtered, shards_written, rows_by_event, {}


async def _process_logs_event(
    ctx: ProcessContext,
    logs: list[EventLog],
) -> tuple[int, int, int, dict[str, int], dict[str, int]]:
    """Event log processing: computes per-event shard stats."""
    decoded, filtered, shards_written, rows_by_event, written_paths = await _process_logs_core(ctx, logs)
    
    shards_by_event: dict[str, int] = defaultdict(int)
    for p in written_paths:
        # Assumes path structure .../EventName/shard_xxxxx.parquet
        ev_name = p.parent.name
        shards_by_event[ev_name] += 1
        
    return decoded, filtered, shards_written, rows_by_event, dict(shards_by_event)


async def _process_logs(
    ctx: ProcessContext,
    logs: list[EventLog],
) -> tuple[int, int, int, dict[str, int], dict[str, int]]:
    """
    Decode logs and write to the aggregator.
    
    Delegates to legacy or event strategy based on context layout.
    """
    if ctx.shards_layout == "per_event":
        return await _process_logs_event(ctx, logs)
    else:
        return await _process_logs_legacy(ctx, logs)


async def _process_interval_core(
    ctx: ProcessContext,
    seed: WorkSeed,
    create_done_record: callable,
) -> None:
    """Core interval processing loop with retries."""
    stack: list[WorkSeed] = [seed]

    while stack:
        current = stack.pop()
        start_block, end_block = current.start, current.end

        await ctx.manifest.append(_create_started_record(start_block, end_block))
        try:
            async with ctx.sem:
                logs = await ctx.rpc.get_logs(
                    address=ctx.address,
                    topic0s=ctx.topic0s,
                    from_block=start_block,
                    to_block=end_block,
                )

            ctx.stats.executed_subranges += 1
            ctx.stats.total_logs += len(logs)

            decoded_rows, filtered_rows, shards_here, rows_by_event, shards_by_event = await _process_logs(ctx, logs)

            ctx.stats.processed_ok += 1
            ctx.stats.shards_written += shards_here
            
            done_record = create_done_record(
                start_block,
                end_block,
                len(logs),
                decoded_rows,
                shards_here,
                filtered_rows,
                rows_by_event,
                shards_by_event
            )
            await ctx.manifest.append(done_record)

        except Exception as e:
            left, right = current.split()
            stack.extend([left, right])
            ctx.stats.partially_covered_split += 1
            await ctx.manifest.append(_create_failed_record(start_block, end_block, str(e)))


async def _process_interval_legacy(
    ctx: ProcessContext,
    seed: WorkSeed,
) -> None:
    """Legacy interval processing using standard ChunkRecord."""
    def _create(start, end, logs, decoded, shards, filtered, rows_map, shards_map):
        return _create_done_record(
            start, end, logs, decoded, shards, filtered
        )
    
    await _process_interval_core(ctx, seed, _create)


async def _process_interval_event(
    ctx: ProcessContext,
    seed: WorkSeed,
) -> None:
    """Event interval processing using ExtendedChunkRecord."""
    def _create(start, end, logs, decoded, shards, filtered, rows_map, shards_map):
        return ExtendedChunkRecord(
            from_block=start,
            to_block=end,
            status="done",
            attempts=1,
            error=None,
            logs=logs,
            decoded=decoded,
            shards=shards,
            updated_at=time.time(),
            filtered=filtered,
            shards_written=shards_map or None,
            rows_per_event=rows_map or None,
        )

    await _process_interval_core(ctx, seed, _create)


async def process_interval(
    ctx: ProcessContext,
    seed: WorkSeed,
) -> None:
    """
    Process one inclusive [seed.start, seed.end] range with retry splitting.
    """
    if ctx.extended_manifest:
        await _process_interval_event(ctx, seed)
    else:
        await _process_interval_legacy(ctx, seed)

# ---------------------------------------------------------------------------
# Domain service – FetchDecodeService
# ---------------------------------------------------------------------------


class FetchDecodeService:
    """
    Domain service for orchestrating the fetch-decode-write pipeline.

    It depends only on abstract providers & repositories (interfaces)
    and contains all the orchestration & decoding logic over a precomputed
    set of block intervals ("seeds").
    """

    def __init__(
        self,
        logs_provider: IEvmLogsProvider,
        registry_provider: IEventRegistryProvider,
    ) -> None:
        self._logs_provider = logs_provider
        self._registry_provider = registry_provider

    async def run(
        self,
        *,
        config: FetchDecodeConfig,
        manifest_repo: IManifestRepository,
        shards_repo: IEventShardsRepository,
        seeds: list[WorkSeed],
    ) -> ProcessStats:
        """
        Execute the decoding process over the given block seeds.

        Parameters
        ----------
        config : FetchDecodeConfig
            Domain configuration (address, topics, step, concurrency, batch size).
        manifest_repo : IManifestRepository
            Append-only manifest sink for chunk status records.
        shards_repo : IEventShardsRepository
            Sink for decoded event rows (parquet, DB, etc.).
        seeds : list[(int, int)]
            Inclusive block ranges [(from_block, to_block), ...] to process.

        Notes
        -----
        - Coverage / "what is already done" is computed by the application layer.
        - This service only orchestrates fetching, decoding, writing and stats.
        """
        stats = ProcessStats()

        if not seeds:
            return stats

        # 1) Resolve registry (ABI + decoding specs)
        registry = self._registry_provider.get_registry()

        # 2) Build shared context
        sem = asyncio.Semaphore(config.concurrency)
        agg_lock = asyncio.Lock()
        ctx = ProcessContext(
            rpc=self._logs_provider,
            address=config.address,
            topic0s=config.topic0s,
            registry=registry,
            batch_decode_rows=config.batch_decode_rows,
            sem=sem,
            agg_lock=agg_lock,
            manifest=manifest_repo,
            aggregator=shards_repo,
            stats=stats,
            shards_layout=config.shards_layout,
            extended_manifest=config.extended_manifest,
        )

        # 3) Run workers concurrently over each seed
        tasks = [
            asyncio.create_task(process_interval(ctx, seed))
            for seed in seeds
        ]

        await asyncio.gather(*tasks)

        # 4) Close aggregator (optional) and update stats
        async with agg_lock:
            last = shards_repo.close()
        if last:
            stats.shards_written += 1

        return stats
