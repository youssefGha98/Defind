import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from defind.core.config import OrchestratorConfig
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
from defind.orchestration.orchestrator import (
    _cleanup_overlapping_intervals,
    _cleanup_redundant_old_chunks,
    _plan_seeds_with_tail_extension,
    _plan_startup_small_interval_compaction,
    fetch_decode,
)


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


def _base_config(**kwargs: Any) -> OrchestratorConfig:
    defaults: dict[str, Any] = dict(
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
    mock_storage.exists.return_value = True  # all chunks "done" → no work

    with (
        patch("defind.orchestration.orchestrator.RPC", return_value=mock_rpc),
        patch("defind.orchestration.orchestrator._build_storage", return_value=(mock_storage, "/tmp/test/pool")),
        patch("defind.orchestration.orchestrator.load_done_chunks", return_value=[]),
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
    mock_storage.exists.return_value = False  # nothing done yet
    mock_storage.write_table.return_value = None
    mock_storage.list_keys.return_value = []

    with (
        patch("defind.orchestration.orchestrator.RPC", return_value=mock_rpc),
        patch("defind.orchestration.orchestrator._build_storage", return_value=(mock_storage, "/tmp/test/pool")),
        patch("defind.orchestration.orchestrator.load_done_chunks", return_value=[]),
        patch("defind.orchestration.orchestrator.build_work_seeds", return_value=[WorkSeed(0, 100)]),
    ):
        output = await fetch_decode(config=_base_config(), registry=registry)

    assert output.stats.executed_subranges == 1
    assert output.stats.total_logs == 1
    assert output.stats.processed_ok == 1


@pytest.mark.asyncio
async def test_process_interval_single_block_rpc_error_raises_without_infinite_split() -> None:
    rpc = AsyncMock()
    rpc.get_logs = AsyncMock(side_effect=ConnectionError("rpc unavailable"))

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
        print_chunk_writes=False,
        force_reprocess=False,
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
        print_chunk_writes=False,
        force_reprocess=False,
        stats=ProcessStats(),
    )

    with patch("defind.core.use_cases.fetch_decode.write_chunk", side_effect=ValueError("write failed")):
        with pytest.raises(ValueError):
            await process_interval(ctx, WorkSeed(10, 20))

    assert rpc.get_logs.await_count == 1


@pytest.mark.asyncio
async def test_process_interval_splits_on_rpc_error_code_then_recovers() -> None:
    rpc = AsyncMock()
    rpc.get_logs = AsyncMock(
        side_effect=[
            RuntimeError("RPC error: -32005 query returned more than 10000 results"),
            [],
            [],
        ]
    )

    registry = _make_registry()
    storage = MagicMock()
    storage.exists.return_value = False
    storage.write_table.return_value = None

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
        print_chunk_writes=False,
        force_reprocess=False,
        stats=ProcessStats(),
    )

    await process_interval(ctx, WorkSeed(0, 1))

    assert rpc.get_logs.await_count == 3
    assert ctx.stats.partially_covered_split == 1


@pytest.mark.asyncio
async def test_process_interval_splits_on_http_413_then_recovers() -> None:
    req = httpx.Request("POST", "http://localhost:8545")
    resp = httpx.Response(413, request=req, text="request entity too large")
    http_err = httpx.HTTPStatusError("413 Payload Too Large", request=req, response=resp)

    rpc = AsyncMock()
    rpc.get_logs = AsyncMock(side_effect=[http_err, [], []])

    registry = _make_registry()
    storage = MagicMock()
    storage.exists.return_value = False
    storage.write_table.return_value = None

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
        print_chunk_writes=False,
        force_reprocess=False,
        stats=ProcessStats(),
    )

    await process_interval(ctx, WorkSeed(0, 1))

    assert rpc.get_logs.await_count == 3
    assert ctx.stats.partially_covered_split == 1


@pytest.mark.asyncio
async def test_fetch_decode_service_limits_seed_worker_parallelism() -> None:
    registry = _make_registry()
    registry_provider = MagicMock()
    registry_provider.get_registry.return_value = registry
    logs_provider = AsyncMock()
    storage = MagicMock()

    service = FetchDecodeService(
        logs_provider=logs_provider,
        registry_provider=registry_provider,
    )

    config = FetchDecodeConfig(
        address=ADDR,
        topic0s=["0xabc"],
        step=1,
        chunk_size=1,
        concurrency=3,
    )
    seeds = [WorkSeed(i, i) for i in range(20)]

    active = 0
    max_active = 0

    async def _fake_process_interval(ctx: ProcessContext, seed: WorkSeed) -> None:
        nonlocal active, max_active
        active += 1
        max_active = max(max_active, active)
        await asyncio.sleep(0.01)
        active -= 1

    with patch("defind.core.use_cases.fetch_decode.process_interval", new=_fake_process_interval):
        await service.run(config=config, storage=storage, seeds=seeds)

    assert max_active <= 3


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
        patch("defind.orchestration.orchestrator.load_done_chunks", return_value=[]),
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
                    chunk_size=101,
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


@pytest.mark.asyncio
async def test_fetch_decode_extends_last_partial_chunk_and_deletes_old_one(mock_rpc: Any) -> None:
    registry = _make_registry()
    mock_storage = MagicMock()
    mock_storage.exists.return_value = False
    mock_storage.list_keys.return_value = []
    mock_storage.delete.return_value = None

    service = MagicMock()
    service.run = AsyncMock(return_value=ProcessStats(processed_ok=1, total_logs=3))

    with (
        patch("defind.orchestration.orchestrator.RPC", return_value=mock_rpc),
        patch("defind.orchestration.orchestrator._build_storage", return_value=(mock_storage, "/tmp/test/pool")),
        patch(
            "defind.orchestration.orchestrator.load_done_chunks",
            return_value=[(0, 99)],
        ),
        patch("defind.orchestration.orchestrator.FetchDecodeService", return_value=service),
    ):
        await fetch_decode(
            config=_base_config(end_block=120, chunk_size=200),
            registry=registry,
        )

    seeds = service.run.await_args.kwargs["seeds"]
    assert seeds == [WorkSeed(0, 120)]
    mock_storage.delete.assert_called_once_with("TestEvent/chunk_0000000000_0000000099.parquet")


@pytest.mark.asyncio
async def test_fetch_decode_ignores_stale_coverage_index_and_uses_file_scan(mock_rpc: Any) -> None:
    registry = _make_registry()
    mock_storage = MagicMock()
    mock_storage.exists.return_value = False
    mock_storage.list_keys.return_value = []
    mock_storage.delete.return_value = None

    service = MagicMock()
    service.run = AsyncMock(return_value=ProcessStats(processed_ok=1, total_logs=3))

    with (
        patch("defind.orchestration.orchestrator.RPC", return_value=mock_rpc),
        patch("defind.orchestration.orchestrator._build_storage", return_value=(mock_storage, "/tmp/test/pool")),
        patch(
            "defind.orchestration.orchestrator.load_done_chunks_from_index",
            return_value=[(0, 199)],  # stale index says full chunk already exists
        ),
        patch(
            "defind.orchestration.orchestrator.load_done_chunks",
            side_effect=[[(0, 99)], [(0, 99)], [(0, 120)], [(0, 120)]],  # real files show tail is partial
        ),
        patch("defind.orchestration.orchestrator.FetchDecodeService", return_value=service),
    ):
        await fetch_decode(
            config=_base_config(end_block=120, chunk_size=200),
            registry=registry,
        )

    seeds = service.run.await_args.kwargs["seeds"]
    assert seeds == [WorkSeed(0, 120)]
    mock_storage.delete.assert_called_once_with("TestEvent/chunk_0000000000_0000000099.parquet")


@pytest.mark.asyncio
async def test_fetch_decode_batch_mode_runs_until_no_more_compaction_work(mock_rpc: Any) -> None:
    registry = _make_registry()
    mock_storage = MagicMock()
    mock_storage.exists.return_value = False
    mock_storage.list_keys.return_value = []
    mock_storage.delete.return_value = None

    service = MagicMock()
    service.run = AsyncMock(
        side_effect=[
            ProcessStats(processed_ok=1, total_logs=3),  # compaction write
        ]
    )

    with (
        patch("defind.orchestration.orchestrator.RPC", return_value=mock_rpc),
        patch("defind.orchestration.orchestrator._build_storage", return_value=(mock_storage, "/tmp/test/pool")),
        patch("defind.orchestration.orchestrator.load_done_chunks_from_index", return_value=[(0, 49), (50, 99)]),
        patch(
            "defind.orchestration.orchestrator.load_done_chunks",
            side_effect=[[(0, 49), (50, 99)], [(0, 99)], [(0, 99)]],
        ),
        patch("defind.orchestration.orchestrator.FetchDecodeService", return_value=service),
    ):
        await fetch_decode(
            config=_base_config(end_block=99, chunk_size=200),
            registry=registry,
        )

    assert service.run.await_count == 1
    assert service.run.await_args.kwargs["seeds"] == [WorkSeed(0, 99)]


def test_plan_tail_extension_does_not_rewind_when_tail_is_not_contiguous() -> None:
    seeds = _plan_seeds_with_tail_extension(
        current_start=200,
        current_end=300,
        chunk_size=200,
        done_chunks=[(0, 99)],
    )
    assert seeds == [WorkSeed(200, 300)]


def test_plan_tail_extension_does_not_skip_hole_before_tail() -> None:
    seeds = _plan_seeds_with_tail_extension(
        current_start=0,
        current_end=260,
        chunk_size=200,
        done_chunks=[(0, 99), (150, 249)],
    )
    assert seeds == [WorkSeed(100, 149), WorkSeed(250, 260)]


def test_plan_startup_small_interval_compaction_rewrites_fragmented_subchunk() -> None:
    seeds = _plan_startup_small_interval_compaction(
        anchor_start=0,
        chunk_size=200,
        done_chunks=[(0, 49), (50, 99), (200, 399)],
    )
    assert seeds == [WorkSeed(0, 99)]


def test_plan_startup_small_interval_compaction_works_when_global_coverage_is_huge() -> None:
    seeds = _plan_startup_small_interval_compaction(
        anchor_start=0,
        chunk_size=200,
        done_chunks=[(0, 199), (200, 250), (251, 260)],
    )
    assert seeds == [WorkSeed(200, 260)]


def test_cleanup_redundant_old_chunks_deletes_orphan_partials() -> None:
    storage = MagicMock()
    storage.delete.return_value = None
    storage.list_keys.side_effect = lambda prefix: {
        "E1/": [
            "E1/chunk_0000000000_0000000099.parquet",  # orphan partial
            "E1/chunk_0000000000_0000000199.parquet",  # new full chunk
        ],
        "E2/": [
            "E2/chunk_0000000000_0000000199.parquet",
        ],
    }.get(prefix, [])

    deleted = _cleanup_redundant_old_chunks(
        storage=storage,
        event_names=["E1", "E2"],
        old_done_chunks=[(0, 199)],
        seeds=[WorkSeed(0, 199)],
    )

    assert deleted == [(0, 99)]
    storage.delete.assert_any_call("E1/chunk_0000000000_0000000099.parquet")
    storage.delete.assert_any_call("E2/chunk_0000000000_0000000099.parquet")


def test_cleanup_overlapping_intervals_deletes_legacy_overlaps() -> None:
    storage = MagicMock()
    storage.delete.return_value = None
    storage.list_keys.side_effect = lambda prefix: {
        "E1/": [
            "E1/chunk_0000000000_0000000099.parquet",
            "E1/chunk_0000000000_0000000199.parquet",
        ],
        "E2/": [
            "E2/chunk_0000000000_0000000099.parquet",
            "E2/chunk_0000000000_0000000199.parquet",
        ],
    }.get(prefix, [])

    deleted = _cleanup_overlapping_intervals(
        storage=storage,
        event_names=["E1", "E2"],
    )

    assert deleted == [(0, 99)]
    storage.delete.assert_any_call("E1/chunk_0000000000_0000000099.parquet")
    storage.delete.assert_any_call("E2/chunk_0000000000_0000000099.parquet")


def test_cleanup_overlapping_intervals_handles_partial_overlap() -> None:
    storage = MagicMock()
    storage.delete.return_value = None
    storage.list_keys.side_effect = lambda prefix: {
        "E1/": [
            "E1/chunk_0000000100_0000000200.parquet",
            "E1/chunk_0000000200_0000000900.parquet",
            "E1/chunk_0000000201_0000000901.parquet",
        ],
    }.get(prefix, [])

    deleted = _cleanup_overlapping_intervals(
        storage=storage,
        event_names=["E1"],
    )

    assert deleted == [(100, 200), (201, 901)]
    storage.delete.assert_any_call("E1/chunk_0000000100_0000000200.parquet")
    storage.delete.assert_any_call("E1/chunk_0000000201_0000000901.parquet")


@pytest.mark.asyncio
async def test_fetch_decode_listen_reorg_lookback_forces_reprocess(mock_rpc: Any) -> None:
    registry = _make_registry()
    mock_storage = MagicMock()
    mock_storage.exists.return_value = False
    mock_storage.list_keys.return_value = []

    # initial "latest" for end_block resolution, then listen ticks
    mock_rpc.latest_block = AsyncMock(side_effect=[100, 102, 102])

    service = MagicMock()
    service.run = AsyncMock(
        side_effect=[
            ProcessStats(processed_ok=1, total_logs=2),
            ProcessStats(processed_ok=1, total_logs=1),
            ProcessStats(processed_ok=1, total_logs=1),
        ]
    )

    with (
        patch("defind.orchestration.orchestrator.RPC", return_value=mock_rpc),
        patch("defind.orchestration.orchestrator._build_storage", return_value=(mock_storage, "/tmp/test/pool")),
        patch(
            "defind.orchestration.orchestrator.load_done_chunks",
            side_effect=[[(0, 100)], [(0, 100)], [(0, 100)], [(0, 100)], [(0, 100)], [(0, 102)]],
        ),
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
                    chunk_size=200,
                    listen=True,
                    listen_poll_interval_s=0.0,
                    reorg_lookback_blocks=20,
                ),
                registry=registry,
            )

    assert service.run.await_count == 2
    kwargs_list = [call.kwargs for call in service.run.await_args_list]
    assert any(kw["force_reprocess"] is True and kw["seeds"] == [WorkSeed(0, 100)] for kw in kwargs_list)
    assert any(kw["force_reprocess"] is False for kw in kwargs_list)
