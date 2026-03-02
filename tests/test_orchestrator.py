import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from defind.core.config import OrchestratorConfig
from defind.core.use_cases.fetch_decode import (
    ProcessContext,
    ProcessStats,
    RPCFetchError,
    WorkSeed,
    process_interval,
)
from defind.decoding.specs import EventRegistry, EventSpec, ProjectionRefs, TopicFieldSpec, DataFieldSpec
from defind.orchestration.orchestrator import fetch_decode


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
    registry = EventRegistry()
    registry[spec.topic0] = spec
    return registry


ADDR = "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640"  # valid checksum address


def _base_config(**kwargs) -> OrchestratorConfig:
    defaults = dict(
        rpc_url="http://localhost:8545",
        address=ADDR,
        topic0s=["0xabc"],
        start_block=0,
        end_block=100,
        protocol_slug="test",
        contract_slug="pool",
    )
    defaults.update(kwargs)
    return OrchestratorConfig(**defaults)


@pytest.mark.asyncio
async def test_fetch_decode_empty_seeds(mock_rpc: Any) -> None:
    registry = _make_registry()
    mock_storage = MagicMock()
    mock_storage.exists.return_value = True   # all chunks "done" → no work

    with (
        patch("defind.orchestration.orchestrator.RPC", return_value=mock_rpc),
        patch("defind.orchestration.orchestrator._build_storage", return_value=(mock_storage, "/tmp/test/pool")),
        patch("defind.orchestration.orchestrator.load_done_coverage", return_value=[]),
        patch("defind.orchestration.orchestrator.build_work_seeds", return_value=[]),
    ):
        output = await fetch_decode(config=_base_config(), registry=registry)

    assert output.stats.processed_ok == 0
    assert output.stats.total_logs == 0


@pytest.mark.asyncio
async def test_fetch_decode_with_work(mock_rpc: Any) -> None:
    mock_log = MagicMock()
    mock_log.topics = ["0xabc", "0x" + "0" * 24 + "1234567890123456789012345678901234567890"]
    mock_log.data_hex = "0x" + "00" * 31 + "64"  # amount = 100
    mock_log.block_number = 10
    mock_log.block_timestamp = 1000
    mock_log.tx_hash = "0xtx"
    mock_log.log_index = 0
    mock_log.address = ADDR
    mock_rpc.get_logs.return_value = [mock_log]

    registry = _make_registry()
    mock_storage = MagicMock()
    mock_storage.exists.return_value = False   # nothing done yet
    mock_storage.write_table.return_value = None
    mock_storage.list_keys.return_value = []

    with (
        patch("defind.orchestration.orchestrator.RPC", return_value=mock_rpc),
        patch("defind.orchestration.orchestrator._build_storage", return_value=(mock_storage, "/tmp/test/pool")),
        patch("defind.orchestration.orchestrator.load_done_coverage", return_value=[]),
        patch("defind.orchestration.orchestrator.build_work_seeds", return_value=[WorkSeed(0, 100)]),
    ):
        output = await fetch_decode(config=_base_config(), registry=registry)

    assert output.stats.executed_subranges == 1
    assert output.stats.total_logs == 1
    assert output.stats.processed_ok == 1


@pytest.mark.asyncio
async def test_process_interval_single_block_rpc_error_raises_without_infinite_split() -> None:
    rpc = AsyncMock()
    rpc.get_logs = AsyncMock(side_effect=RuntimeError("rpc unavailable"))

    registry = _make_registry()
    storage = MagicMock()
    storage.exists.return_value = False

    ctx = ProcessContext(
        rpc=rpc,
        address=ADDR,
        topic0s=["0xabc"],
        registry=registry,
        sem=asyncio.Semaphore(1),
        storage=storage,
        event_names=["TestEvent"],
        step=10,
        codec="lz4",
        stats=ProcessStats(),
    )

    with pytest.raises(RPCFetchError):
        await process_interval(ctx, WorkSeed(42, 42))

    assert rpc.get_logs.await_count == 1


@pytest.mark.asyncio
async def test_process_interval_does_not_split_on_decode_write_error() -> None:
    rpc = AsyncMock()
    rpc.get_logs = AsyncMock(return_value=[])

    registry = _make_registry()
    storage = MagicMock()
    storage.exists.return_value = False

    ctx = ProcessContext(
        rpc=rpc,
        address=ADDR,
        topic0s=["0xabc"],
        registry=registry,
        sem=asyncio.Semaphore(1),
        storage=storage,
        event_names=["TestEvent"],
        step=1000,
        codec="lz4",
        stats=ProcessStats(),
    )

    with patch("defind.core.use_cases.fetch_decode.write_chunk", side_effect=ValueError("write failed")):
        with pytest.raises(ValueError):
            await process_interval(ctx, WorkSeed(10, 20))

    assert rpc.get_logs.await_count == 1


@pytest.mark.asyncio
async def test_fetch_decode_listen_continues_after_backfill(mock_rpc: Any) -> None:
    registry = _make_registry()
    mock_storage = MagicMock()
    mock_storage.exists.return_value = False
    mock_storage.list_keys.return_value = []

    mock_rpc.latest_block = AsyncMock(side_effect=[100, 102, 102])

    service = MagicMock()
    service.run = AsyncMock(
        side_effect=[
            ProcessStats(processed_ok=1, total_logs=2),
            ProcessStats(processed_ok=1, total_logs=1),
        ]
    )

    with (
        patch("defind.orchestration.orchestrator.RPC", return_value=mock_rpc),
        patch("defind.orchestration.orchestrator._build_storage", return_value=(mock_storage, "/tmp/test/pool")),
        patch("defind.orchestration.orchestrator.load_done_coverage", return_value=[]),
        patch(
            "defind.orchestration.orchestrator.build_work_seeds",
            side_effect=[[WorkSeed(0, 100)], [WorkSeed(101, 102)]],
        ) as build_seeds_mock,
        patch("defind.orchestration.orchestrator.FetchDecodeService", return_value=service),
        patch(
            "defind.orchestration.orchestrator.asyncio.sleep",
            new=AsyncMock(side_effect=asyncio.CancelledError),
        ),
    ):
        with pytest.raises(asyncio.CancelledError):
            await fetch_decode(
                config=_base_config(
                    end_block="latest",
                    listen=True,
                    listen_poll_interval_s=0.0,
                ),
                registry=registry,
            )

    assert service.run.await_count == 2
    assert build_seeds_mock.call_args_list[0].kwargs["start"] == 0
    assert build_seeds_mock.call_args_list[0].kwargs["end"] == 100
    assert build_seeds_mock.call_args_list[1].kwargs["start"] == 101
    assert build_seeds_mock.call_args_list[1].kwargs["end"] == 102
