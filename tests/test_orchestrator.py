from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from defind.core.config import OrchestratorConfig
from defind.core.use_cases.fetch_decode import WorkSeed
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
