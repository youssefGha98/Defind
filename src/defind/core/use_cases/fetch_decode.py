from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit

import httpx

from defind.clients.rpc import RPCError, is_hex_address, is_topic0
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
    on_chunk_written: Any = None


@dataclass(frozen=True)
class WorkSeed:
    """Inclusive block interval to process (one output Parquet per seed)."""

    start: int
    end: int

    def split(self) -> tuple[WorkSeed, WorkSeed] | None:
        if self.start >= self.end:
            return None
        mid = (self.start + self.end) // 2
        return (WorkSeed(self.start, mid), WorkSeed(mid + 1, self.end))


class RPCFetchError(RuntimeError):
    """Raised when an RPC log fetch fails for a block interval."""

    def __init__(self, message: str, *, splitable: bool, retryable: bool = False) -> None:
        super().__init__(message)
        self.splitable = splitable
        self.retryable = retryable


class _SubrangeFetchError(RuntimeError):
    def __init__(self, from_block: int, to_block: int) -> None:
        super().__init__(f"subrange fetch failed for [{from_block}, {to_block}]")
        self.from_block = from_block
        self.to_block = to_block


_RANGE_ERROR_MARKERS = (
    "more than",
    "too many",
    "max results",
    "response size",
    "request entity too large",
    "block range",
    "query returned",
)


def _looks_timeout_message(message: str) -> bool:
    msg = (message or "").lower()
    return "timed out" in msg or "timeout" in msg


def _looks_range_limited_message(message: str) -> bool:
    msg = (message or "").lower()
    return any(marker in msg for marker in _RANGE_ERROR_MARKERS)


def _is_splitable_rpc_exception(exc: Exception) -> bool:
    if isinstance(exc, RPCError):
        if exc.rpc_code == -32002:
            return True
        msg = " ".join(str(part or "") for part in (exc.rpc_message, exc))
        if exc.rpc_code == -32005:
            return True
        return _looks_range_limited_message(msg) or _looks_timeout_message(msg)

    # JSON-RPC structured errors from the RPC client.
    if isinstance(exc, RuntimeError) and str(exc).startswith("RPC error:"):
        msg = str(exc).lower()
        return ("-32005" in msg) or _looks_range_limited_message(msg) or _looks_timeout_message(msg)

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


def _is_retryable_rpc_exception(exc: Exception) -> bool:
    if isinstance(exc, RPCError):
        if exc.rpc_code == -32002:
            return True
        msg = " ".join(str(part or "") for part in (exc.rpc_message, exc))
        return _looks_timeout_message(msg)
    return isinstance(exc, (httpx.TransportError, httpx.TimeoutException, asyncio.TimeoutError))


_CHUNK_NETWORK_RETRY_ATTEMPTS = 2
_CHUNK_NETWORK_RETRY_BACKOFF_S = 1.0


def _jittered_backoff_s(delay_s: float) -> float:
    if delay_s <= 0:
        return 0.0
    return random.uniform(delay_s, min(delay_s * 1.25, 8.0))


def _redact_request_url(url: str) -> str:
    cleaned = (url or "").strip()
    if not cleaned:
        return ""
    parts = urlsplit(cleaned)
    if not parts.scheme or not parts.hostname:
        return ""
    port = f":{parts.port}" if parts.port is not None else ""
    return f"{parts.scheme}://{parts.hostname}{port}"


def _compact_error_text(value: str, *, limit: int = 240) -> str:
    cleaned = " ".join((value or "").split())
    if len(cleaned) <= limit:
        return cleaned
    return f"{cleaned[: limit - 3]}..."


def _rpc_error_log_context(exc: Exception) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "rpc_error_type": type(exc).__name__,
    }
    detail = _compact_error_text(str(exc).strip())
    if detail:
        payload["rpc_error"] = detail

    if isinstance(exc, RPCError):
        redacted_url = _redact_request_url(exc.url)
        if redacted_url:
            payload["rpc_url"] = redacted_url
        if exc.rpc_method:
            payload["rpc_method"] = exc.rpc_method
        if exc.rpc_code is not None:
            payload["rpc_code"] = exc.rpc_code
        if exc.rpc_message:
            payload["rpc_message"] = str(exc.rpc_message)
        if exc.rpc_data is not None:
            payload["rpc_data"] = exc.rpc_data
        return payload

    request = getattr(exc, "request", None)
    if request is None and isinstance(exc, httpx.HTTPStatusError):
        request = exc.response.request
    if request is not None:
        redacted_url = _redact_request_url(str(request.url))
        if redacted_url:
            payload["rpc_url"] = redacted_url
        method = str(getattr(request, "method", "") or "").upper()
        if method:
            payload["rpc_method"] = method

    if isinstance(exc, httpx.HTTPStatusError):
        payload["rpc_http_status"] = int(exc.response.status_code)
        try:
            response_body = _compact_error_text(exc.response.text)
        except Exception:
            response_body = ""
        if response_body:
            payload["rpc_response_body"] = response_body

    return payload


def _decoded_row_count(buffers: dict[str, dict[str, list[Any]]]) -> int:
    total = 0
    for ev_buf in buffers.values():
        total += len(ev_buf.get("block_number", []))
    return total


def _chunk_manifest_payload(
    *,
    from_block: int,
    to_block: int,
    status: str,
    attempts: int,
    error: str | None,
    logs: int,
    decoded: int,
    filtered: int,
    shards: int,
    files_written: int | None = None,
    step: int | None = None,
    subrange_count: int | None = None,
    duration_s: float | None = None,
    retryable: bool | None = None,
    splitable: bool | None = None,
    rpc_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "from_block": from_block,
        "to_block": to_block,
        "status": status,
        "attempts": attempts,
        "error": error,
        "logs": logs,
        "decoded": decoded,
        "filtered": filtered,
        "shards": shards,
        "updated_at": time.time(),
    }
    if files_written is not None:
        payload["files_written"] = files_written
    if step is not None:
        payload["step"] = step
    if subrange_count is not None:
        payload["subrange_count"] = subrange_count
    if duration_s is not None:
        payload["duration_s"] = duration_s
    if retryable is not None:
        payload["retryable"] = retryable
    if splitable is not None:
        payload["splitable"] = splitable
    if rpc_context:
        payload.update(rpc_context)
    return payload


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
) -> dict[str, dict[str, list[Any]]]:
    """Decode all logs into per-event columnar buffers.

    Returns a dict mapping event_name → {field_name → list_of_values}.
    Each event type only tracks its own fields — no padding.
    Filtered logs are silently skipped.
    """
    buffers: dict[str, dict[str, list[Any]]] = {}
    _decode_logs_into(logs, registry, buffers)
    return buffers


def _decode_logs_into(
    logs: list[EventLog],
    registry: EventRegistry,
    buffers: dict[str, dict[str, list[Any]]],
) -> None:
    """Decode logs into existing per-event columnar buffers."""

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


# ---------------------------------------------------------------------------
# Internal: fetch all logs for a chunk_size range via step-sized sub-fetches
# ---------------------------------------------------------------------------


async def _fetch_chunk_buffers(
    ctx: ProcessContext,
    a: int,
    b: int,
) -> tuple[dict[str, dict[str, list[Any]]], int, int, float]:
    """Fetch all logs for [a, b], decoding batches as they arrive."""

    sub_ranges = list(iter_chunks(a, b, ctx.step))
    started_at = asyncio.get_running_loop().time()

    logger.info(
        "chunk_fetch_start",
        extra={
            "chunk_start": a,
            "chunk_end": b,
            "subrange_count": len(sub_ranges),
            "step": ctx.step,
        },
    )

    buffers: dict[str, dict[str, list[Any]]] = {}
    total_log_count = 0
    executed_subranges = 0

    async def _fetch_subrange(from_b: int, to_b: int, *, attempt: int = 0) -> tuple[int, int]:
        try:
            async with ctx.sem:
                logs = await ctx.rpc.get_logs(
                    address=ctx.address,
                    topic0s=ctx.topic0s,
                    from_block=from_b,
                    to_block=to_b,
                )
        except Exception as exc:
            cause = exc
            splitable = _is_splitable_rpc_exception(cause)
            retryable = _is_retryable_rpc_exception(cause)
            rpc_context = _rpc_error_log_context(cause)

            if retryable and attempt < _CHUNK_NETWORK_RETRY_ATTEMPTS:
                delay_s = _jittered_backoff_s(_CHUNK_NETWORK_RETRY_BACKOFF_S * (2**attempt))
                logger.warning(
                    "chunk_fetch_retry",
                    extra={
                        "chunk_start": a,
                        "chunk_end": b,
                        "subrange_start": from_b,
                        "subrange_end": to_b,
                        "attempt": attempt + 1,
                        "max_attempts": _CHUNK_NETWORK_RETRY_ATTEMPTS + 1,
                        "retry_in_s": delay_s,
                        **rpc_context,
                    },
                )
                logger.warning(
                    "chunk_manifest",
                    extra=_chunk_manifest_payload(
                        from_block=a,
                        to_block=b,
                        status="retrying",
                        attempts=attempt + 1,
                        error=str(cause).strip() or type(cause).__name__,
                        logs=0,
                        decoded=0,
                        filtered=0,
                        shards=0,
                        step=ctx.step,
                        subrange_count=len(sub_ranges),
                        retryable=True,
                        splitable=splitable,
                        rpc_context={
                            **rpc_context,
                            "subrange_start": from_b,
                            "subrange_end": to_b,
                        },
                    ),
                )
                if delay_s > 0:
                    await asyncio.sleep(delay_s)
                return await _fetch_subrange(from_b, to_b, attempt=attempt + 1)

            if splitable and from_b < to_b:
                mid = (from_b + to_b) // 2
                ctx.stats.partially_covered_split += 1
                logger.warning(
                    "chunk_subrange_split_retry",
                    extra={
                        "chunk_start": a,
                        "chunk_end": b,
                        "subrange_start": from_b,
                        "subrange_end": to_b,
                        "left_start": from_b,
                        "left_end": mid,
                        "right_start": mid + 1,
                        "right_end": to_b,
                        **rpc_context,
                    },
                )
                left_result, right_result = await asyncio.gather(
                    _fetch_subrange(from_b, mid),
                    _fetch_subrange(mid + 1, to_b),
                )
                return (
                    left_result[0] + right_result[0],
                    left_result[1] + right_result[1],
                )

            detail = f"RPC fetch failed for interval [{a}, {b}] subrange [{from_b}, {to_b}]"
            raise RPCFetchError(
                detail,
                splitable=splitable,
                retryable=retryable,
            ) from cause

        _decode_logs_into(logs, ctx.registry, buffers)
        return len(logs), 1

    tasks = [asyncio.create_task(_fetch_subrange(f, t)) for f, t in sub_ranges]
    try:
        for task in asyncio.as_completed(tasks):
            log_count, successful_subranges = await task
            total_log_count += log_count
            executed_subranges += successful_subranges
    except Exception as exc:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if isinstance(exc, RPCFetchError):
            raise
        cause = exc.__cause__ if isinstance(exc, _SubrangeFetchError) and exc.__cause__ is not None else exc
        splitable = _is_splitable_rpc_exception(cause)
        retryable = _is_retryable_rpc_exception(cause)
        detail = f"RPC fetch failed for interval [{a}, {b}]"
        if isinstance(exc, _SubrangeFetchError):
            detail += f" subrange [{exc.from_block}, {exc.to_block}]"
        raise RPCFetchError(
            detail,
            splitable=splitable,
            retryable=retryable,
        ) from cause
    ctx.stats.executed_subranges += executed_subranges
    duration_s = round(asyncio.get_running_loop().time() - started_at, 3)
    logger.info(
        "chunk_fetch_complete",
        extra={
            "chunk_start": a,
            "chunk_end": b,
            "subrange_count": executed_subranges,
            "log_count": total_log_count,
            "duration_s": duration_s,
        },
    )
    return buffers, total_log_count, executed_subranges, duration_s


# ---------------------------------------------------------------------------
# Interval worker
# ---------------------------------------------------------------------------


async def process_interval(ctx: ProcessContext, seed: WorkSeed) -> None:
    """Process one inclusive [seed.start, seed.end] range.

    Each seed produces exactly one Parquet per event type.
    Internally, the range is fetched using step-sized concurrent sub-calls.
    On RPC fetch failures, retry/split happens at the RPC subrange level only.
    Decode/write failures are raised immediately.
    """
    if seed.start < 0 or seed.end < 0:
        raise ValueError("seed bounds must be >= 0")
    if seed.start > seed.end:
        raise ValueError("seed.start must be <= seed.end")
    if ctx.stop_event is not None and ctx.stop_event.is_set():
        raise RuntimeError("writer lock lost during run")

    a, b = seed.start, seed.end
    planned_subrange_count = len(list(iter_chunks(a, b, ctx.step)))

    if (not ctx.force_reprocess) and chunk_is_done(ctx.storage, ctx.event_names, a, b):
        return

    try:
        logger.info(
            "chunk_process_start",
            extra={
                "chunk_start": a,
                "chunk_end": b,
                "force_reprocess": ctx.force_reprocess,
            },
        )
        logger.info(
            "chunk_manifest",
            extra=_chunk_manifest_payload(
                from_block=a,
                to_block=b,
                status="started",
                attempts=0,
                error=None,
                logs=0,
                decoded=0,
                filtered=0,
                shards=0,
                step=ctx.step,
                subrange_count=planned_subrange_count,
            ),
        )
        buffers, log_count, subrange_count, fetch_duration_s = await _fetch_chunk_buffers(ctx, a, b)
    except RPCFetchError as e:
        ctx.stats.processed_failed += 1
        cause = e.__cause__ if isinstance(e.__cause__, Exception) else e
        rpc_context = _rpc_error_log_context(cause)
        manifest_error = str(cause).strip() or str(e).strip()
        logger.error(
            "chunk_fetch_failed",
            extra={
                "chunk_start": a,
                "chunk_end": b,
                "splitable": e.splitable,
                "retryable": e.retryable,
                **rpc_context,
            },
            exc_info=True,
        )
        logger.error(
            "chunk_manifest",
            extra=_chunk_manifest_payload(
                from_block=a,
                to_block=b,
                status="failed",
                attempts=_CHUNK_NETWORK_RETRY_ATTEMPTS + 1 if e.retryable else 1,
                error=manifest_error,
                logs=0,
                decoded=0,
                filtered=0,
                shards=0,
                step=ctx.step,
                subrange_count=planned_subrange_count,
                retryable=e.retryable,
                splitable=e.splitable,
                rpc_context=rpc_context,
            ),
            exc_info=True,
        )
        raise

    if ctx.stop_event is not None and ctx.stop_event.is_set():
        raise RuntimeError("writer lock lost during run")
    written = write_chunk(ctx.storage, ctx.registry, a, b, buffers, ctx.codec)
    decoded_count = _decoded_row_count(buffers)
    filtered_count = max(0, log_count - decoded_count)

    if ctx.print_chunk_writes:
        logger.info(
            "chunk_written",
            extra={
                "chunk_start": a,
                "chunk_end": b,
                "logs": log_count,
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

    logger.info(
        "chunk_manifest",
        extra=_chunk_manifest_payload(
            from_block=a,
            to_block=b,
            status="done",
            attempts=1,
            error=None,
            logs=log_count,
            decoded=decoded_count,
            filtered=filtered_count,
            shards=len(written),
            files_written=len(written),
            step=ctx.step,
            subrange_count=subrange_count,
            duration_s=fetch_duration_s,
        ),
    )

    ctx.stats.total_logs += log_count
    ctx.stats.processed_ok += 1
    ctx.stats.chunks_written += len(written)
    if ctx.on_chunk_written is not None:
        await ctx.on_chunk_written(a, b)
    logger.info(
        "chunk_process_complete",
        extra={
            "chunk_start": a,
            "chunk_end": b,
            "log_count": log_count,
            "files_written": len(written),
        },
    )


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
        if not is_hex_address(config.address):
            raise ValueError("address must be a 0x-prefixed 40-hex Ethereum address")
        if not config.topic0s:
            raise ValueError("topic0s must not be empty")
        invalid_topic0s = [topic for topic in config.topic0s if not is_topic0(topic)]
        if invalid_topic0s:
            raise ValueError("topic0s must contain only 0x-prefixed 64-hex topic signatures")
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
        on_chunk_written: Any = None,
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
            on_chunk_written=on_chunk_written,
        )

        worker_count = min(len(seeds), max(1, config.concurrency))
        seed_iter = iter(seeds)
        seed_lock = asyncio.Lock()

        async def _worker(worker_id: int) -> None:
            while True:
                if stop_event is not None and stop_event.is_set():
                    raise RuntimeError("writer lock lost during run")
                async with seed_lock:
                    try:
                        seed = next(seed_iter)
                    except StopIteration:
                        return
                try:
                    await process_interval(ctx, seed)
                except Exception as exc:
                    raise RuntimeError(
                        f"worker {worker_id} failed on seed [{seed.start}, {seed.end}]"
                    ) from exc

        tasks = [asyncio.create_task(_worker(idx)) for idx in range(worker_count)]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        errors = [result for result in results if isinstance(result, BaseException)]
        if errors:
            first = errors[0]
            if len(errors) == 1:
                raise first
            raise RuntimeError(f"{len(errors)} workers failed; first error: {first}") from first

        return stats
