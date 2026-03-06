from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from defind.core.use_cases.fetch_decode import (
    FetchDecodeConfig,
    FetchDecodeService,
    ProcessContext,
    ProcessStats,
    RPCFetchError,
    WorkSeed,
    process_interval,
)
from defind.decoding.specs import DataFieldSpec, EventRegistry, EventSpec, ProjectionRefs, TopicFieldSpec

ADDR = "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640"


def _make_registry() -> EventRegistry:
    spec = EventSpec(
        topic0="0xabc",
        name="TestEvent",
        topic_fields=[TopicFieldSpec("user", 1, "address")],
        data_fields=[DataFieldSpec("amount", 0, "uint256")],
        projection={
            "user": ProjectionRefs.TopicRef(name="user"),
            "amount": ProjectionRefs.DataRef(name="amount"),
        },
    )
    registry: EventRegistry = {}
    registry[spec.topic0] = spec
    return registry


def _ctx(
    *,
    rpc: Any,
    storage: Any,
    force_reprocess: bool = False,
    print_chunk_writes: bool = False,
    stop_event: asyncio.Event | None = None,
) -> ProcessContext:
    return ProcessContext(
        rpc=rpc,
        address=ADDR,
        topic0s=["0xabc"],
        registry=_make_registry(),
        sem=asyncio.Semaphore(1),
        storage=storage,
        event_names=["TestEvent"],
        step=10,
        codec="lz4",
        print_chunk_writes=print_chunk_writes,
        force_reprocess=force_reprocess,
        stats=ProcessStats(),
        stop_event=stop_event,
    )


@pytest.mark.parametrize(
    "config,seeds,error_match",
    [
        (
            FetchDecodeConfig(address=ADDR, topic0s=["0xabc"], step=0, chunk_size=1, concurrency=1),
            [WorkSeed(0, 0)],
            "step must be > 0",
        ),
        (
            FetchDecodeConfig(address=ADDR, topic0s=["0xabc"], step=1, chunk_size=0, concurrency=1),
            [WorkSeed(0, 0)],
            "chunk_size must be > 0",
        ),
        (
            FetchDecodeConfig(address=ADDR, topic0s=["0xabc"], step=1, chunk_size=1, concurrency=0),
            [WorkSeed(0, 0)],
            "concurrency must be > 0",
        ),
        (
            FetchDecodeConfig(address="", topic0s=["0xabc"], step=1, chunk_size=1, concurrency=1),
            [WorkSeed(0, 0)],
            "address must not be empty",
        ),
        (
            FetchDecodeConfig(address=ADDR, topic0s=[], step=1, chunk_size=1, concurrency=1),
            [WorkSeed(0, 0)],
            "topic0s must not be empty",
        ),
        (
            FetchDecodeConfig(address=ADDR, topic0s=["0xabc"], step=1, chunk_size=1, concurrency=1),
            [WorkSeed(-1, 0)],
            "seed bounds must be >= 0",
        ),
        (
            FetchDecodeConfig(address=ADDR, topic0s=["0xabc"], step=1, chunk_size=1, concurrency=1),
            [WorkSeed(2, 1)],
            "seed.start must be <= seed.end",
        ),
    ],
)
@pytest.mark.asyncio
async def test_fetch_decode_service_run_validates_inputs(
    config: FetchDecodeConfig,
    seeds: list[WorkSeed],
    error_match: str,
) -> None:
    registry_provider = MagicMock()
    registry_provider.get_registry.return_value = _make_registry()
    service = FetchDecodeService(logs_provider=AsyncMock(), registry_provider=registry_provider)

    with pytest.raises(ValueError, match=error_match):
        await service.run(config=config, storage=MagicMock(), seeds=seeds)


@pytest.mark.asyncio
async def test_process_interval_guardrails_on_seed_bounds() -> None:
    rpc = AsyncMock()
    storage = MagicMock()
    storage.exists.return_value = False
    context = _ctx(rpc=rpc, storage=storage)

    with pytest.raises(ValueError, match="seed bounds must be >= 0"):
        await process_interval(context, WorkSeed(-1, 0))

    with pytest.raises(ValueError, match="seed.start must be <= seed.end"):
        await process_interval(context, WorkSeed(3, 2))


@pytest.mark.asyncio
async def test_process_interval_stops_when_lock_lost_event_is_set() -> None:
    rpc = AsyncMock()
    storage = MagicMock()
    storage.exists.return_value = False
    stop_event = asyncio.Event()
    stop_event.set()
    context = _ctx(rpc=rpc, storage=storage, stop_event=stop_event)

    with pytest.raises(RuntimeError, match="writer lock lost during run"):
        await process_interval(context, WorkSeed(0, 10))

    rpc.get_logs.assert_not_awaited()


@pytest.mark.asyncio
async def test_process_interval_skips_when_chunk_already_done() -> None:
    rpc = AsyncMock()
    storage = MagicMock()
    storage.exists.return_value = True
    context = _ctx(rpc=rpc, storage=storage, force_reprocess=False)

    await process_interval(context, WorkSeed(0, 0))

    rpc.get_logs.assert_not_awaited()


@pytest.mark.asyncio
async def test_process_interval_force_reprocess_ignores_existing_chunk() -> None:
    rpc = AsyncMock()
    rpc.get_logs = AsyncMock(return_value=[])
    storage = MagicMock()
    storage.exists.return_value = True
    storage.write_table.return_value = None
    context = _ctx(rpc=rpc, storage=storage, force_reprocess=True)

    await process_interval(context, WorkSeed(0, 0))

    assert rpc.get_logs.await_count == 1


@pytest.mark.asyncio
async def test_process_interval_single_block_splitable_error_increments_failed() -> None:
    rpc = AsyncMock()
    rpc.get_logs = AsyncMock(side_effect=RuntimeError("RPC error: -32005 too many results"))
    storage = MagicMock()
    storage.exists.return_value = False
    context = _ctx(rpc=rpc, storage=storage)

    with pytest.raises(RPCFetchError):
        await process_interval(context, WorkSeed(7, 7))

    assert context.stats.processed_failed == 1


@pytest.mark.asyncio
async def test_process_interval_splits_on_http_400_range_limited_body() -> None:
    req = httpx.Request("POST", "http://localhost:8545")
    resp = httpx.Response(400, request=req, text="query returned too many results")
    err = httpx.HTTPStatusError("400 Bad Request", request=req, response=resp)

    rpc = AsyncMock()
    rpc.get_logs = AsyncMock(side_effect=[err, [], []])
    storage = MagicMock()
    storage.exists.return_value = False
    storage.write_table.return_value = None
    context = _ctx(rpc=rpc, storage=storage)

    await process_interval(context, WorkSeed(0, 1))

    assert context.stats.partially_covered_split == 1
    assert rpc.get_logs.await_count == 3


@pytest.mark.asyncio
async def test_process_interval_does_not_split_on_http_414_without_range_markers() -> None:
    req = httpx.Request("POST", "http://localhost:8545")
    resp = httpx.Response(414, request=req, text="uri too long")
    err = httpx.HTTPStatusError("414 URI Too Long", request=req, response=resp)

    rpc = AsyncMock()
    rpc.get_logs = AsyncMock(side_effect=err)
    storage = MagicMock()
    storage.exists.return_value = False
    context = _ctx(rpc=rpc, storage=storage)

    with pytest.raises(RPCFetchError):
        await process_interval(context, WorkSeed(0, 1))

    assert context.stats.partially_covered_split == 0
    assert context.stats.processed_failed == 1


@pytest.mark.asyncio
async def test_process_interval_prints_chunk_writes_when_enabled() -> None:
    rpc = AsyncMock()
    rpc.get_logs = AsyncMock(return_value=[])
    storage = MagicMock()
    storage.exists.return_value = False
    context = _ctx(rpc=rpc, storage=storage, print_chunk_writes=True)

    with (
        patch("defind.core.use_cases.fetch_decode.write_chunk", return_value=["TestEvent/chunk_0_0.parquet"]),
        patch("defind.core.use_cases.fetch_decode.logger.info") as log_mock,
    ):
        await process_interval(context, WorkSeed(0, 0))

    assert log_mock.call_count >= 2


def test_fetch_decode_service_run_empty_seeds_returns_zero_stats() -> None:
    registry_provider = MagicMock()
    registry_provider.get_registry.return_value = _make_registry()
    service = FetchDecodeService(logs_provider=AsyncMock(), registry_provider=registry_provider)
    config = FetchDecodeConfig(
        address=ADDR,
        topic0s=["0xabc"],
        step=1,
        chunk_size=1,
        concurrency=1,
    )

    stats = asyncio.run(service.run(config=config, storage=MagicMock(), seeds=[]))
    assert stats == ProcessStats()
