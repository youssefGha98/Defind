from __future__ import annotations

from click.testing import CliRunner
import pytest

import defind.cli as cli_module


def test_parse_end_block_accepts_latest_and_hex_values() -> None:
    assert cli_module._parse_end_block("latest") == "latest"
    assert cli_module._parse_end_block("  LATEST  ") == "latest"
    assert cli_module._parse_end_block("0x10") == 16
    assert cli_module._parse_end_block("42") == 42


def test_parse_end_block_rejects_invalid_values() -> None:
    with pytest.raises(Exception):
        cli_module._parse_end_block("nope")

    with pytest.raises(Exception):
        cli_module._parse_end_block("-1")


def test_parse_start_block_accepts_auto_and_numbers() -> None:
    assert cli_module._parse_start_block("auto") == "auto"
    assert cli_module._parse_start_block("creation") == "auto"
    assert cli_module._parse_start_block("earliest") == "earliest"
    assert cli_module._parse_start_block("0x10") == 16
    assert cli_module._parse_start_block("42") == 42


def test_parse_start_block_rejects_invalid_values() -> None:
    with pytest.raises(Exception):
        cli_module._parse_start_block("nope")

    with pytest.raises(Exception):
        cli_module._parse_start_block("-1")


def test_storage_target_label_uses_protocol_and_contract_for_local_and_s3() -> None:
    local_target = cli_module._storage_target_label(
        storage="local",
        out_root=cli_module.Path("/tmp/defind"),
        s3_bucket=None,
        s3_prefix="",
        protocol_slug="uniswap",
        contract_slug="nfpm",
    )
    assert local_target == "/tmp/defind/uniswap/nfpm"

    s3_target = cli_module._storage_target_label(
        storage="s3",
        out_root=cli_module.Path("/tmp/defind"),
        s3_bucket="bucket",
        s3_prefix="datasets/raw",
        protocol_slug="uniswap",
        contract_slug="nfpm",
    )
    assert s3_target == "s3://bucket/datasets/raw/uniswap/nfpm/"


def test_run_command_uses_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    async def fake_run_indexer(**kwargs: object) -> None:
        captured.update(kwargs)

    monkeypatch.setattr(cli_module, "_run_indexer", fake_run_indexer)

    runner = CliRunner()
    result = runner.invoke(
        cli_module.cli,
        [
            "run",
            "--rpc-url",
            "https://rpc.example",
            "--address",
            "0x1234567890123456789012345678901234567890",
            "--start-block",
            "100",
            "--protocol-slug",
            "uniswap",
            "--contract-slug",
            "nfpm",
        ],
    )

    assert result.exit_code == 0
    assert captured == {
        "rpc_url": "https://rpc.example",
        "address": "0x1234567890123456789012345678901234567890",
        "abi_path": None,
        "start_block": 100,
        "end_block": "latest",
        "protocol_slug": "uniswap",
        "contract_slug": "nfpm",
        "step": 10_000,
        "chunk_size": 1_000_000,
        "concurrency": 15,
        "mode": "backfill",
        "storage": "local",
    }


def test_run_command_accepts_local_abi_path(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    captured: dict[str, object] = {}
    abi_path = tmp_path / "pool.json"
    abi_path.write_text("[]", encoding="utf-8")

    async def fake_run_indexer(**kwargs: object) -> None:
        captured.update(kwargs)

    monkeypatch.setattr(cli_module, "_run_indexer", fake_run_indexer)

    runner = CliRunner()
    result = runner.invoke(
        cli_module.cli,
        [
            "run",
            "--rpc-url",
            "https://rpc.example",
            "--address",
            "0x1234567890123456789012345678901234567890",
            "--abi-path",
            str(abi_path),
            "--start-block",
            "100",
            "--protocol-slug",
            "uniswap",
            "--contract-slug",
            "pool",
        ],
    )

    assert result.exit_code == 0
    assert captured["abi_path"] == abi_path


def test_run_command_accepts_auto_start_block(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    async def fake_run_indexer(**kwargs: object) -> None:
        captured.update(kwargs)

    monkeypatch.setattr(cli_module, "_run_indexer", fake_run_indexer)

    runner = CliRunner()
    result = runner.invoke(
        cli_module.cli,
        [
            "run",
            "--rpc-url",
            "https://rpc.example",
            "--address",
            "0x1234567890123456789012345678901234567890",
            "--start-block",
            "auto",
            "--protocol-slug",
            "uniswap",
            "--contract-slug",
            "pool",
        ],
    )

    assert result.exit_code == 0
    assert captured["start_block"] == "auto"


def test_run_command_rejects_invalid_block_window(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_run_indexer(**_: object) -> None:
        raise AssertionError("run indexer should not be called")

    monkeypatch.setattr(cli_module, "_run_indexer", fake_run_indexer)

    runner = CliRunner()
    result = runner.invoke(
        cli_module.cli,
        [
            "run",
            "--rpc-url",
            "https://rpc.example",
            "--address",
            "0x1234567890123456789012345678901234567890",
            "--start-block",
            "100",
            "--end-block",
            "99",
            "--protocol-slug",
            "uniswap",
            "--contract-slug",
            "nfpm",
        ],
    )

    assert result.exit_code == 1
    assert "start-block must be <= end-block" in result.output
