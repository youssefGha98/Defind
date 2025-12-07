from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import List, Tuple

from defind.core.interfaces import (
    IEvmLogsProvider,
    IManifestRepository,
    IEventShardsRepository,
    IEventRegistryProvider,
)
from defind.core.models import ChunkRecord, Column, EventLog, Meta
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


async def _process_logs(
    ctx: ProcessContext,
    logs: list[EventLog],
) -> tuple[int, int, int]:
    """
    Decode logs and write to the aggregator.

    Returns
    -------
    (decoded_rows, filtered_rows, shards_written)
    """
    buf = Column.empty()
    rows_in_batch = 0
    decoded_rows = 0
    filtered_rows = 0
    shards_written = 0

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

        if rows_in_batch >= ctx.batch_decode_rows:
            async with ctx.agg_lock:
                written = ctx.aggregator.add(buf)
            shards_written += len(written)
            buf = Column.empty()
            rows_in_batch = 0

    # Flush last partial batch for this interval
    if rows_in_batch > 0:
        async with ctx.agg_lock:
            written = ctx.aggregator.add(buf)
        shards_written += len(written)

    return decoded_rows, filtered_rows, shards_written


async def process_interval(
    ctx: ProcessContext,
    seed: WorkSeed,
) -> None:
    """
    Process one inclusive [seed.start, seed.end] range with retry splitting.

    This function:
    - appends a 'started' record for each attemptsed interval,
    - fetches logs from the IEvmLogsProvider,
    - decodes and writes them via IEventShardsRepository,
    - appends 'done' or 'failed' records to the manifest,
    - splits the interval on failure and retries with smaller seeds.
    """
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

            decoded_rows, filtered_rows, shards_here = await _process_logs(ctx, logs)

            ctx.stats.processed_ok += 1
            ctx.stats.shards_written += shards_here
            await ctx.manifest.append(
                _create_done_record(
                    start_block,
                    end_block,
                    logs=len(logs),
                    decoded=decoded_rows,
                    shards=shards_here,
                    filtered=filtered_rows,
                )
            )

        except Exception as e:
            left, right = current.split()
            stack.extend([left, right])
            ctx.stats.partially_covered_split += 1
            await ctx.manifest.append(_create_failed_record(start_block, end_block, str(e)))

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
