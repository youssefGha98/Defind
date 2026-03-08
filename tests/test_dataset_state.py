from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from defind.abi_events import make_event_registry_from_abi
from defind.api.ops_api import DatasetRef, OpsApiConfig, _prepare_dataset_job_run
from defind.dataset_state import (
    append_dataset_job,
    build_job_snapshot,
    increment_job_progress,
    list_dataset_jobs,
    mark_job_terminal,
    update_dataset_meta,
)
from defind.indexer_request import serialize_registry
from defind.storage.local import LocalChunkStorage


class _NoConditionalLocalChunkStorage(LocalChunkStorage):
    def read_json_with_version(self, key: str) -> tuple[dict[str, Any] | None, str | None]:
        raise RuntimeError(
            "Atomic S3 lock requires boto3/botocore client support; "
            "install boto3 or disable single_writer_guard for this backend"
        )

    def write_json_if_version(self, key: str, payload: dict[str, Any], expected_version: str | None) -> bool:
        raise RuntimeError(
            "Atomic S3 lock requires boto3/botocore client support; "
            "install boto3 or disable single_writer_guard for this backend"
        )


def _write_minimal_abi(path: Path) -> None:
    abi = [
        {
            "type": "event",
            "name": "Swap",
            "anonymous": False,
            "inputs": [
                {
                    "indexed": True,
                    "internalType": "address",
                    "name": "sender",
                    "type": "address",
                },
                {
                    "indexed": False,
                    "internalType": "uint256",
                    "name": "amount0",
                    "type": "uint256",
                },
            ],
        }
    ]
    path.write_text(json.dumps(abi), encoding="utf-8")


def test_update_dataset_meta_degrades_to_process_local_lock(tmp_path: Path) -> None:
    storage = _NoConditionalLocalChunkStorage(tmp_path / "dataset")
    storage.write_json(
        "_meta.json",
        {
            "protocol": "uniswap",
            "contract": "usdc_weth",
            "start_block": 100,
            "last_block": 110,
        },
    )

    updated = update_dataset_meta(storage, lambda current: {**current, "last_block": 120})

    assert updated["last_block"] == 120
    assert storage.read_json("_meta.json")["last_block"] == 120


def test_dataset_jobs_degrade_to_process_local_lock(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    storage = LocalChunkStorage(tmp_path / "dataset")
    storage.write_text("_jobs.jsonl", "")

    def _unsupported_lock(*, storage: Any, key: str, owner_id: str, run_id: str, ttl_s: int) -> Any:
        raise RuntimeError(
            "Atomic S3 lock requires boto3/botocore client support; "
            "install boto3 or disable single_writer_guard for this backend"
        )

    monkeypatch.setattr("defind.dataset_state._acquire_writer_lock", _unsupported_lock)

    snapshot = build_job_snapshot(
        job_id="job-1",
        mode="backfill",
        status="running",
        resume_from=100,
        origin="ui",
        config_snapshot={"mode": "backfill", "concurrency": 4},
    )
    append_dataset_job(storage, snapshot)
    increment_job_progress(storage, job_id="job-1", confirmed_to_block=120)
    mark_job_terminal(storage, job_id="job-1", status="completed", error=None)

    rows = list_dataset_jobs(storage)
    assert len(rows) == 1
    assert rows[0]["status"] == "completed"
    assert rows[0]["resume_from"] == 120
    assert rows[0]["chunks_written"] == 1


def test_prepare_dataset_job_run_disables_engine_single_writer_guard(tmp_path: Path) -> None:
    abi_path = tmp_path / "swap.json"
    _write_minimal_abi(abi_path)
    registry = make_event_registry_from_abi(abi_path)
    meta = {
        "protocol": "uniswap",
        "contract": "usdc_weth",
        "contract_address": "0x0000000000000000000000000000000000000001",
        "chain_id": 1,
        "start_block": 100,
        "last_block": 110,
        "chunk_size": 10,
        "step": 5,
        "storage": "s3",
        "rpc_url": "https://rpc.example.org",
        "event_names": ["Swap"],
        "registry_json": serialize_registry(registry),
    }

    prepared = _prepare_dataset_job_run(
        cfg=OpsApiConfig(out_root=tmp_path),
        dataset=DatasetRef(protocol="uniswap", contract="usdc_weth"),
        meta=meta,
        mode="backfill",
        concurrency=4,
        origin="ui",
        resume_from=110,
    )

    assert prepared.config.single_writer_guard is False
    assert prepared.config.start_block == 100
    assert prepared.public_config["resume_from"] == 110
