from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from defind.core.config import OrchestratorConfig
from defind.core.use_cases.fetch_decode import WorkSeed
from defind.decoding.specs import DataFieldSpec, EventRegistry, EventSpec, ProjectionRefs, TopicFieldSpec
from defind.orchestration.orchestrator import (
    _build_storage,
    _cleanup_overlapping_intervals,
    _cleanup_redundant_old_chunks,
    _plan_startup_small_interval_compaction,
    _resolve_block_range,
    _safe_save_done_chunks_index,
    _select_reorg_rewrite_seeds,
    _validate_runtime_config,
    fetch_decode,
)

ADDR = "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640"


def _base_config(**kwargs: Any) -> OrchestratorConfig:
    defaults = dict(
        rpc_url="http://localhost:8545",
        address=ADDR,
        topic0s=["0xabc"],
        start_block=0,
        end_block=100,
        protocol_slug="test",
        contract_slug="pool",
        out_root=Path("/tmp/defind-tests"),
    )
    defaults.update(kwargs)
    return OrchestratorConfig(**defaults)


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


@pytest.mark.asyncio
async def test_resolve_block_range_supports_keywords_and_hex() -> None:
    provider = AsyncMock()
    provider.latest_block = AsyncMock(return_value=123)

    start, end = await _resolve_block_range(provider, "earliest", "latest")
    assert (start, end) == (0, 123)

    start2, end2 = await _resolve_block_range(provider, "0x10", "0x20")
    assert (start2, end2) == (16, 32)


@pytest.mark.asyncio
async def test_resolve_block_range_guardrails() -> None:
    provider = AsyncMock()
    provider.latest_block = AsyncMock(return_value=10)

    with pytest.raises(ValueError, match="start_block must be an integer-like value"):
        await _resolve_block_range(provider, "nope", 10)

    with pytest.raises(ValueError, match="end_block must be >= 0"):
        await _resolve_block_range(provider, 0, -1)

    with pytest.raises(ValueError, match="start_block must be <= end_block"):
        await _resolve_block_range(provider, 11, 10)


def test_validate_runtime_config_guardrails() -> None:
    cfg = _base_config(address="")
    with pytest.raises(ValueError, match="address must not be empty"):
        _validate_runtime_config(config=cfg, effective_chunk_size=cfg.chunk_size or cfg.step)

    cfg = _base_config(topic0s=[])
    with pytest.raises(ValueError, match="topic0s must not be empty"):
        _validate_runtime_config(config=cfg, effective_chunk_size=cfg.chunk_size or cfg.step)

    cfg = _base_config(protocol_slug="../oops")
    with pytest.raises(ValueError, match="protocol_slug must not contain"):
        _validate_runtime_config(config=cfg, effective_chunk_size=cfg.chunk_size or cfg.step)

    cfg = _base_config(contract_slug=" with-space ")
    with pytest.raises(ValueError, match="contract_slug must not have leading/trailing spaces"):
        _validate_runtime_config(config=cfg, effective_chunk_size=cfg.chunk_size or cfg.step)


def test_build_storage_local_and_s3() -> None:
    local_cfg = _base_config(out_root=Path("/tmp/local-root"))
    with patch("defind.orchestration.orchestrator.LocalChunkStorage") as local_cls:
        storage, contract_dir = _build_storage(local_cfg)
        local_cls.assert_called_once_with(Path("/tmp/local-root/test/pool"))
        assert storage is local_cls.return_value
        assert contract_dir == "/tmp/local-root/test/pool"

    s3_cfg = _base_config(
        s3_bucket="bucket",
        s3_prefix="prefix-root/",
    )
    with patch("defind.orchestration.orchestrator.S3ChunkStorage") as s3_cls:
        storage, contract_dir = _build_storage(s3_cfg)
        s3_cls.assert_called_once()
        assert s3_cls.call_args.kwargs["prefix"] == "prefix-root/test/pool/"
        assert storage is s3_cls.return_value
        assert contract_dir == "s3://bucket/prefix-root/test/pool/"


def test_build_storage_rejects_unsafe_slugs() -> None:
    with pytest.raises(ValueError, match="protocol_slug must not contain"):
        _build_storage(_base_config(protocol_slug="../unsafe"))


def test_select_reorg_rewrite_seeds_intersection_only() -> None:
    seeds = _select_reorg_rewrite_seeds(
        done_chunks=[(0, 99), (100, 199), (200, 299)],
        window_start=120,
        window_end=250,
    )
    assert seeds == [WorkSeed(100, 199), WorkSeed(200, 299)]


def test_safe_save_done_chunks_index_swallows_storage_errors() -> None:
    storage = MagicMock()
    storage.write_json.side_effect = RuntimeError("disk full")
    _safe_save_done_chunks_index(storage, ["Event"], [(0, 1)])


def test_cleanup_redundant_old_chunks_no_seeds_is_noop() -> None:
    storage = MagicMock()
    deleted = _cleanup_redundant_old_chunks(
        storage=storage,
        event_names=["E1", "E2"],
        old_done_chunks=[(0, 99)],
        seeds=[],
    )
    assert deleted == []
    storage.delete.assert_not_called()


def test_cleanup_overlapping_intervals_no_overlap_is_noop() -> None:
    storage = MagicMock()
    storage.list_keys.side_effect = lambda prefix: {
        "E1/": [
            "E1/chunk_0000000000_0000000099.parquet",
            "E1/chunk_0000000100_0000000199.parquet",
        ],
    }.get(prefix, [])
    assert _cleanup_overlapping_intervals(storage=storage, event_names=["E1"]) == []
    storage.delete.assert_not_called()


def test_plan_startup_small_interval_compaction_with_non_zero_anchor() -> None:
    seeds = _plan_startup_small_interval_compaction(
        anchor_start=24176729,
        chunk_size=200_000,
        done_chunks=[
            (24576729, 24577031),
            (24577032, 24577033),
            (24577034, 24577034),
            (24577035, 24577192),
        ],
    )
    assert seeds == [WorkSeed(24576729, 24577192)]


def test_plan_startup_small_interval_compaction_ignores_intervals_before_anchor() -> None:
    seeds = _plan_startup_small_interval_compaction(
        anchor_start=1000,
        chunk_size=200,
        done_chunks=[(0, 50), (51, 70), (1000, 1049), (1050, 1099)],
    )
    assert seeds == [WorkSeed(1000, 1099)]


@pytest.mark.asyncio
async def test_fetch_decode_invalid_runtime_config_still_closes_rpc(mock_rpc: Any) -> None:
    registry = _make_registry()
    storage = MagicMock()
    storage.list_keys.return_value = []

    with (
        patch("defind.orchestration.orchestrator.RPC", return_value=mock_rpc),
        patch("defind.orchestration.orchestrator._build_storage", return_value=(storage, "/tmp/test/pool")),
    ):
        with pytest.raises(ValueError, match="topic0s must not be empty"):
            await fetch_decode(
                config=_base_config(topic0s=[]),
                registry=registry,
            )

    mock_rpc.aclose.assert_awaited_once()


@pytest.mark.asyncio
async def test_fetch_decode_rejects_empty_registry() -> None:
    with pytest.raises(ValueError, match="registry must not be empty"):
        await fetch_decode(
            config=_base_config(),
            registry={},
        )


@pytest.mark.asyncio
async def test_fetch_decode_rejects_duplicate_event_names() -> None:
    spec1 = EventSpec(
        topic0="0xaaa",
        name="SameName",
        topic_fields=[],
        data_fields=[],
        projection={},
    )
    spec2 = EventSpec(
        topic0="0xbbb",
        name="SameName",
        topic_fields=[],
        data_fields=[],
        projection={},
    )
    registry: EventRegistry = {
        spec1.topic0: spec1,
        spec2.topic0: spec2,
    }

    with pytest.raises(ValueError, match="event names must be unique"):
        await fetch_decode(
            config=_base_config(topic0s=["0xaaa", "0xbbb"]),
            registry=registry,
        )


@pytest.mark.asyncio
async def test_fetch_decode_rejects_uppercase_registry_keys() -> None:
    spec = EventSpec(
        topic0="0xabc",
        name="Event",
        topic_fields=[],
        data_fields=[],
        projection={},
    )
    with pytest.raises(ValueError, match="topic0 keys must be lower-case"):
        await fetch_decode(
            config=_base_config(topic0s=["0xABC"]),
            registry={"0xABC": spec},
        )


@pytest.mark.asyncio
async def test_fetch_decode_rejects_registry_key_spec_mismatch() -> None:
    spec = EventSpec(
        topic0="0xbbb",
        name="Event",
        topic_fields=[],
        data_fields=[],
        projection={},
    )
    with pytest.raises(ValueError, match="registry key must match spec.topic0"):
        await fetch_decode(
            config=_base_config(topic0s=["0xaaa"]),
            registry={"0xaaa": spec},
        )


@pytest.mark.asyncio
async def test_fetch_decode_rejects_unsafe_event_names() -> None:
    spec = EventSpec(
        topic0="0xabc",
        name="Bad/Event",
        topic_fields=[],
        data_fields=[],
        projection={},
    )
    with pytest.raises(ValueError, match="must not contain path separators"):
        await fetch_decode(
            config=_base_config(topic0s=["0xabc"]),
            registry={"0xabc": spec},
        )


@pytest.mark.asyncio
async def test_fetch_decode_listen_with_no_seeds_does_not_call_service(mock_rpc: Any) -> None:
    registry = _make_registry()
    storage = MagicMock()
    storage.list_keys.return_value = []

    mock_rpc.latest_block = AsyncMock(side_effect=[100, 101, 101])
    service = MagicMock()
    service.run = AsyncMock()

    with (
        patch("defind.orchestration.orchestrator.RPC", return_value=mock_rpc),
        patch("defind.orchestration.orchestrator._build_storage", return_value=(storage, "/tmp/test/pool")),
        patch("defind.orchestration.orchestrator.FetchDecodeService", return_value=service),
        patch("defind.orchestration.orchestrator.load_done_chunks", return_value=[]),
        patch("defind.orchestration.orchestrator.load_done_chunks_from_index", return_value=[]),
        patch("defind.orchestration.orchestrator._plan_seeds_with_tail_extension", return_value=[]),
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

    service.run.assert_not_awaited()
