from __future__ import annotations

import asyncio
from dataclasses import dataclass

import httpx

from defind.core.interfaces import IChunkStorage, IEventRegistryProvider, IEvmLogsProvider
from defind.core.models import EventLog, Meta
from defind.decoding.decoder import decode_event
from defind.decoding.specs import EventRegistry
from defind.observability import get_logger
from defind.orchestration.utils import iter_chunks, subtract_iv
from defind.storage.chunks import chunk_is_done, write_chunk

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Domain configuration
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FetchDecodeConfig:
    """
    Domain-level configuration for the fetch-decode use case.

    Free of infrastructure concerns (no RPC URL, no filesystem paths).

    step       — RPC fetch granularity (blocks per eth_getLogs call).
    chunk_size — Output Parquet granularity (blocks per output file).
                 Must be a multiple of step. Defaults to step when not set.
    codec      — Parquet compression codec ("lz4", "zstd", "snappy", "none").
    """

    address: str
    topic0s: list[str]
    step: int
    chunk_size: int
    concurrency: int
    codec: str = "lz4"
    print_chunk_writes: bool = False


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------


@dataclass(kw_only=True)
class ProcessStats:
    """Aggregated counters for the fetch-decode pipeline."""

    processed_ok: int = 0
    processed_failed: int = 0
    executed_subranges: int = 0
    total_logs: int = 0
    partially_covered_split: int = 0
    chunks_written: int = 0


# ---------------------------------------------------------------------------
# Processing context
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class ProcessContext:
    """Shared state for interval processing (keeps worker signatures small)."""

    rpc: IEvmLogsProvider
    address: str
    topic0s: list[str]
    registry: EventRegistry
    sem: asyncio.Semaphore
    storage: IChunkStorage
    event_names: list[str]
    step: int
    codec: str
    print_chunk_writes: bool
    force_reprocess: bool
    stats: ProcessStats
    stop_event: asyncio.Event | None = None


@dataclass(frozen=True)
class WorkSeed:
    """Inclusive block interval to process (one output Parquet per seed)."""
    start: int
    end: int

    def split(self) -> tuple[WorkSeed, WorkSeed] | None:
        if self.start >= self.end:
            return None
        mid = (self.start + self.end) // 2
        return (
            WorkSeed(self.start, mid),
            WorkSeed(mid + 1, self.end)
        )


class RPCFetchError(RuntimeError):
    """Raised when an RPC log fetch fails for a block interval."""

    def __init__(self, message: str, *, splitable: bool) -> None:
        super().__init__(message)
        self.splitable = splitable


_RANGE_ERROR_MARKERS = (
    "more than",
    "too many",
    "max results",
    "response size",
    "request entity too large",
    "block range",
    "query returned",
)


def _looks_range_limited_message(message: str) -> bool:
    msg = (message or "").lower()
    return any(marker in msg for marker in _RANGE_ERROR_MARKERS)


def _is_splitable_rpc_exception(exc: Exception) -> bool:
    # JSON-RPC structured errors from the RPC client.
    if isinstance(exc, RuntimeError) and str(exc).startswith("RPC error:"):
        msg = str(exc).lower()
        return ("-32005" in msg) or _looks_range_limited_message(msg)

    # Some providers return HTTP errors instead of JSON-RPC errors.
    if isinstance(exc, httpx.HTTPStatusError):
        status = exc.response.status_code
        if status == 413:
            return True
        if status in (400, 414):
            body = ""
            try:
                body = exc.response.text
            except Exception:
                pass
            return _looks_range_limited_message(body) or _looks_range_limited_message(str(exc))
        return False

    return False


# ---------------------------------------------------------------------------
# Work seeds builder
# ---------------------------------------------------------------------------


def build_work_seeds(
    start: int,
    end: int,
    chunk_size: int,
    covered: list[tuple[int, int]],
) -> list[WorkSeed]:
    """Build a list of uncovered block intervals to process.

    Each WorkSeed spans `chunk_size` blocks and produces one output Parquet.
    """
    seeds: list[WorkSeed] = []
    for uncovered_start, uncovered_end in subtract_iv((start, end), covered):
        for a, b in iter_chunks(uncovered_start, uncovered_end, chunk_size):
            seeds.append(WorkSeed(start=a, end=b))
    return seeds


# ---------------------------------------------------------------------------
# Core log decoding
# ---------------------------------------------------------------------------


def _decode_logs(
    logs: list[EventLog],
    registry: EventRegistry,
) -> dict[str, dict[str, list]]:
    """Decode all logs into per-event columnar buffers.

    Returns a dict mapping event_name → {field_name → list_of_values}.
    Each event type only tracks its own fields — no padding.
    Filtered logs are silently skipped.
    """
    buffers: dict[str, dict[str, list]] = {}

    for ev in logs:
        if not ev.topics:
            continue

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
            registry=registry,
        )
        if pe is None:
            continue

        ev_buf = buffers.get(pe.name)
        if ev_buf is None:
            ev_buf = {
                "block_number": [],
                "block_timestamp": [],
                "tx_hash": [],
                "log_index": [],
                "contract": [],
            }
            for out_key in pe.values:
                ev_buf[out_key] = []
            buffers[pe.name] = ev_buf

        ev_buf["block_number"].append(meta.block_number)
        ev_buf["block_timestamp"].append(int(meta.block_timestamp or 0))
        ev_buf["tx_hash"].append(meta.tx_hash)
        ev_buf["log_index"].append(meta.log_index)
        ev_buf["contract"].append(pe.pool)
        for out_key, v in pe.values.items():
            ev_buf[out_key].append(None if v is None else str(v))

    return buffers


# ---------------------------------------------------------------------------
# Internal: fetch all logs for a chunk_size range via step-sized sub-fetches
# ---------------------------------------------------------------------------


async def _fetch_chunk_logs(ctx: ProcessContext, a: int, b: int) -> list[EventLog]:
    """Fetch all logs for [a, b] using concurrent step-sized sub-range calls."""

    sub_ranges = list(iter_chunks(a, b, ctx.step))

    async def _fetch_sub(from_b: int, to_b: int) -> list[EventLog]:
        async with ctx.sem:
            return await ctx.rpc.get_logs(
                address=ctx.address,
                topic0s=ctx.topic0s,
                from_block=from_b,
                to_block=to_b,
            )

    try:
        results = await asyncio.gather(*[_fetch_sub(f, t) for f, t in sub_ranges])
    except Exception as e:
        splitable = _is_splitable_rpc_exception(e)
        raise RPCFetchError(
            f"RPC fetch failed for interval [{a}, {b}]",
            splitable=splitable,
        ) from e
    ctx.stats.executed_subranges += len(sub_ranges)
    return [log for batch in results for log in batch]


# ---------------------------------------------------------------------------
# Interval worker
# ---------------------------------------------------------------------------


async def process_interval(ctx: ProcessContext, seed: WorkSeed) -> None:
    """Process one inclusive [seed.start, seed.end] range with retry splitting.

    Each seed produces exactly one Parquet per event type.
    Internally, the range is fetched using step-sized concurrent sub-calls.
    On RPC fetch failures, the range splits in half and both halves retry.
    Decode/write failures are raised immediately.
    """
    if seed.start < 0 or seed.end < 0:
        raise ValueError("seed bounds must be >= 0")
    if seed.start > seed.end:
        raise ValueError("seed.start must be <= seed.end")
    if ctx.stop_event is not None and ctx.stop_event.is_set():
        raise RuntimeError("writer lock lost during run")

    stack: list[WorkSeed] = [seed]

    while stack:
        if ctx.stop_event is not None and ctx.stop_event.is_set():
            raise RuntimeError("writer lock lost during run")
        current = stack.pop()
        a, b = current.start, current.end

        # Skip if already written (handles both resume and retry-after-split)
        if (not ctx.force_reprocess) and chunk_is_done(ctx.storage, ctx.event_names, a, b):
            continue

        try:
            logs = await _fetch_chunk_logs(ctx, a, b)
        except RPCFetchError as e:
            if not e.splitable:
                ctx.stats.processed_failed += 1
                logger.error(
                    "chunk_fetch_failed",
                    extra={
                        "chunk_start": a,
                        "chunk_end": b,
                        "splitable": False,
                    },
                    exc_info=True,
                )
                raise
            children = current.split()
            if children is None:
                ctx.stats.processed_failed += 1
                logger.error(
                    "chunk_fetch_failed",
                    extra={
                        "chunk_start": a,
                        "chunk_end": b,
                        "splitable": True,
                        "reason": "single_block_unrecoverable",
                    },
                    exc_info=True,
                )
                raise
            left, right = children
            stack.extend([left, right])
            ctx.stats.partially_covered_split += 1
            ctx.stats.processed_failed += 1
            logger.warning(
                "chunk_split_retry",
                extra={
                    "chunk_start": a,
                    "chunk_end": b,
                    "left_start": left.start,
                    "left_end": left.end,
                    "right_start": right.start,
                    "right_end": right.end,
                },
            )
            continue

        buffers = _decode_logs(logs, ctx.registry)
        if ctx.stop_event is not None and ctx.stop_event.is_set():
            raise RuntimeError("writer lock lost during run")
        written = write_chunk(
            ctx.storage, ctx.registry, a, b, buffers, ctx.codec
        )

        if ctx.print_chunk_writes:
            logger.info(
                "chunk_written",
                extra={
                    "chunk_start": a,
                    "chunk_end": b,
                    "logs": len(logs),
                    "files": len(written),
                },
            )
            for key in written:
                logger.info(
                    "chunk_file_written",
                    extra={
                        "chunk_start": a,
                        "chunk_end": b,
                        "chunk_key": key,
                    },
                )

        ctx.stats.total_logs += len(logs)
        ctx.stats.processed_ok += 1
        ctx.stats.chunks_written += len(written)


# ---------------------------------------------------------------------------
# Domain service
# ---------------------------------------------------------------------------


class FetchDecodeService:
    """Domain service for orchestrating the fetch-decode-write pipeline."""

    def __init__(
        self,
        logs_provider: IEvmLogsProvider,
        registry_provider: IEventRegistryProvider,
    ) -> None:
        self._logs_provider = logs_provider
        self._registry_provider = registry_provider

    @staticmethod
    def _validate_inputs(config: FetchDecodeConfig, seeds: list[WorkSeed]) -> None:
        if config.step <= 0:
            raise ValueError("step must be > 0")
        if config.chunk_size <= 0:
            raise ValueError("chunk_size must be > 0")
        if config.concurrency <= 0:
            raise ValueError("concurrency must be > 0")
        if not config.address:
            raise ValueError("address must not be empty")
        if not config.topic0s:
            raise ValueError("topic0s must not be empty")
        for s in seeds:
            if s.start < 0 or s.end < 0:
                raise ValueError("seed bounds must be >= 0")
            if s.start > s.end:
                raise ValueError("seed.start must be <= seed.end")

    async def run(
        self,
        *,
        config: FetchDecodeConfig,
        storage: IChunkStorage,
        seeds: list[WorkSeed],
        force_reprocess: bool = False,
        stop_event: asyncio.Event | None = None,
    ) -> ProcessStats:
        """Execute the decoding process over the given block seeds."""
        self._validate_inputs(config, seeds)
        stats = ProcessStats()

        if not seeds:
            return stats

        registry = self._registry_provider.get_registry()
        event_names = [spec.name for spec in registry.values()]

        sem = asyncio.Semaphore(config.concurrency)
        ctx = ProcessContext(
            rpc=self._logs_provider,
            address=config.address,
            topic0s=config.topic0s,
            registry=registry,
            sem=sem,
            storage=storage,
            event_names=event_names,
            step=config.step,
            codec=config.codec,
            print_chunk_writes=config.print_chunk_writes,
            force_reprocess=force_reprocess,
            stats=stats,
            stop_event=stop_event,
        )

        worker_count = min(len(seeds), max(1, config.concurrency))
        seed_iter = iter(seeds)
        seed_lock = asyncio.Lock()

        async def _worker() -> None:
            while True:
                if stop_event is not None and stop_event.is_set():
                    raise RuntimeError("writer lock lost during run")
                async with seed_lock:
                    try:
                        seed = next(seed_iter)
                    except StopIteration:
                        return
                await process_interval(ctx, seed)

        tasks = [asyncio.create_task(_worker()) for _ in range(worker_count)]
        await asyncio.gather(*tasks)

        return stats
