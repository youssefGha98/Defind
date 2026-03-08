from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path
from typing import Any

import httpx
import pytest

pytest.importorskip("fastapi")

from defind.api.ops_api import OpsApiConfig, create_app, load_ops_api_config_from_env
from defind.core.use_cases.fetch_decode import ProcessStats
from defind.orchestration.orchestrator import FetchDecodeOutput


def _minimal_abi_json() -> list[dict[str, Any]]:
    return [
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


def _write_minimal_abi(path: Path) -> None:
    path.write_text(json.dumps(_minimal_abi_json()), encoding="utf-8")


def _write_chunk_marker(root: Path, *, protocol: str, contract: str, event: str, start: int, end: int) -> None:
    path = root / protocol / contract / event / f"chunk_{start:010d}_{end:010d}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"")


def _make_client(app: Any) -> httpx.AsyncClient:
    transport = httpx.ASGITransport(app=app, raise_app_exceptions=True)
    return httpx.AsyncClient(transport=transport, base_url="http://test")


async def _create_dataset(
    client: httpx.AsyncClient,
    *,
    abi_path: Path,
    protocol: str = "uniswap",
    contract: str = "usdc_weth",
    start_block: int = 100,
    chunk_size: int = 10,
    step: int = 5,
) -> httpx.Response:
    return await client.post(
        "/datasets",
        json={
            "protocol": protocol,
            "contract": contract,
            "contract_address": "0x0000000000000000000000000000000000000001",
            "chain_id": 1,
            "start_block": start_block,
            "chunk_size": chunk_size,
            "step": step,
            "storage": "s3",
            "rpc_url": "https://rpc.example.org",
            "abi_path": str(abi_path),
        },
    )


def test_load_ops_api_config_from_env_loads_etherscan_api_key_from_dotenv(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("ETHERSCAN_API_KEY", raising=False)
    monkeypatch.delenv("DEFIND_API_ETHERSCAN_API_KEY", raising=False)
    (tmp_path / ".env").write_text("ETHERSCAN_API_KEY=test-etherscan-key\n", encoding="utf-8")

    cfg = load_ops_api_config_from_env()

    assert cfg.etherscan_api_key == "test-etherscan-key"


@pytest.mark.asyncio
async def test_root_dataset_api_create_list_get_patch_and_filter(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from defind.api import ops_api

    abi_path = tmp_path / "pool_abi.json"
    _write_minimal_abi(abi_path)

    async def _fake_chain_head(*, endpoint_url: str, chain_id: int, api_key: str | None) -> int:
        _ = (endpoint_url, chain_id, api_key)
        return 150

    monkeypatch.setattr(ops_api, "_fetch_etherscan_chain_head", _fake_chain_head)

    cfg = OpsApiConfig(out_root=tmp_path, etherscan_api_key="etherscan-key")
    async with _make_client(create_app(cfg)) as client:
        created = await _create_dataset(client, abi_path=abi_path)
        assert created.status_code == 200
        assert created.json()["event_names"] == ["Swap"]

        second = await _create_dataset(client, abi_path=abi_path, protocol="aerodrome", contract="eth_usdc")
        assert second.status_code == 200

        duplicate = await _create_dataset(client, abi_path=abi_path)
        assert duplicate.status_code == 409

        listed = await client.get("/datasets")
        assert listed.status_code == 200
        rows = listed.json()
        assert len(rows) == 2

        filtered = await client.get("/datasets?protocol_slug=uniswap&contract_slug=usdc_weth")
        assert filtered.status_code == 200
        assert [row["id"] for row in filtered.json()] == ["uniswap/usdc_weth"]

        single = await client.get("/datasets/uniswap/usdc_weth")
        assert single.status_code == 200
        row = single.json()
        assert row["id"] == "uniswap/usdc_weth"
        assert row["lag"] == 50
        assert row["chunks_total"] == 0
        assert row["status"] == "idle"

        immutable = await client.patch("/datasets/uniswap/usdc_weth", json={"start_block": 999})
        assert immutable.status_code == 400

        bad_storage = await client.patch("/datasets/uniswap/usdc_weth", json={"storage": "local"})
        assert bad_storage.status_code == 400

        patched = await client.patch("/datasets/uniswap/usdc_weth", json={"chunk_size": 12345})
        assert patched.status_code == 200
        assert patched.json()["chunk_size"] == 12345


@pytest.mark.asyncio
async def test_root_dataset_create_rejects_invalid_abi_json(tmp_path: Path) -> None:
    cfg = OpsApiConfig(out_root=tmp_path)
    async with _make_client(create_app(cfg)) as client:
        res = await client.post(
            "/datasets",
            json={
                "protocol": "uniswap",
                "contract": "usdc_weth",
                "contract_address": "0x0000000000000000000000000000000000000001",
                "chain_id": 1,
                "start_block": 100,
                "chunk_size": 10,
                "step": 5,
                "storage": "s3",
                "rpc_url": "https://rpc.example.org",
                "abi_json": [{"additionalProp1": {}}],
            },
        )
        assert res.status_code == 400
        assert "invalid abi_json" in res.json()["detail"]


@pytest.mark.asyncio
async def test_root_dataset_job_start_updates_meta_logs_and_blocks_second_writer(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from defind.api import ops_api

    abi_path = tmp_path / "pool_abi.json"
    _write_minimal_abi(abi_path)
    release = asyncio.Event()
    seen_start_blocks: list[int | str] = []

    async def _slow_fetch_data(*, config: Any, registry: Any, on_chunk_written: Any = None) -> FetchDecodeOutput:
        _ = registry
        dataset_id = f"{config.protocol_slug}/{config.contract_slug}"
        seen_start_blocks.append(config.start_block)
        logging.getLogger("defind.test").warning("worker_warning", extra={"dataset_id": dataset_id})
        if on_chunk_written is not None:
            await on_chunk_written(int(config.start_block), 109)
        await release.wait()
        return FetchDecodeOutput(stats=ProcessStats(chunks_written=1, total_logs=1), contract_dir="local://fake")

    monkeypatch.setattr(ops_api, "fetch_data", _slow_fetch_data)

    cfg = OpsApiConfig(out_root=tmp_path)
    async with _make_client(create_app(cfg)) as client:
        created = await _create_dataset(client, abi_path=abi_path)
        assert created.status_code == 200

        first = await client.post("/datasets/uniswap/usdc_weth/jobs", json={"mode": "backfill", "concurrency": 4})
        assert first.status_code == 200
        first_job_id = first.json()["job_id"]

        duplicate = await client.post("/datasets/uniswap/usdc_weth/jobs", json={"mode": "backfill", "concurrency": 4})
        assert duplicate.status_code == 409
        assert duplicate.json()["detail"]["message"] == "A writer job is already active on this dataset"
        assert duplicate.json()["detail"]["blocking_job_id"] == first_job_id

        dataset_payload: dict[str, Any] = {}
        running_job_payload: dict[str, Any] = {}
        warn_logs: list[dict[str, Any]] = []
        for _ in range(50):
            dataset = await client.get("/datasets/uniswap/usdc_weth")
            assert dataset.status_code == 200
            dataset_payload = dataset.json()
            running_job = await client.get(f"/datasets/uniswap/usdc_weth/jobs/{first_job_id}")
            assert running_job.status_code == 200
            running_job_payload = running_job.json()
            logs = await client.get(
                f"/datasets/uniswap/usdc_weth/jobs/{first_job_id}/logs?page=1&limit=20&level=WARN"
            )
            assert logs.status_code == 200
            warn_logs = logs.json()["items"]
            if dataset_payload["last_block"] == 109 and running_job_payload["chunks_written"] == 1 and warn_logs:
                break
            await asyncio.sleep(0.02)

        status = await client.get("/status")
        assert status.status_code == 200
        assert status.json()["active_jobs_count"] == 1
        assert status.json()["datasets_count"] == 1

        assert dataset_payload["last_block"] == 109
        assert dataset_payload["active_jobs_count"] == 1
        assert running_job_payload["chunks_written"] == 1
        assert running_job_payload["resume_from"] == 109
        assert warn_logs[0]["payload"]["level"] == "WARNING"

        jobs = await client.get("/datasets/uniswap/usdc_weth/jobs")
        assert jobs.status_code == 200
        assert jobs.json()[0]["job_id"] == first_job_id

        release.set()
        for _ in range(50):
            done = await client.get(f"/datasets/uniswap/usdc_weth/jobs/{first_job_id}")
            assert done.status_code == 200
            if done.json()["status"] != "running":
                break
            await asyncio.sleep(0.02)

        assert done.json()["status"] == "completed"

        status_done = await client.get("/status")
        assert status_done.status_code == 200
        assert status_done.json()["active_jobs_count"] == 0

    assert seen_start_blocks == [100]


@pytest.mark.asyncio
async def test_root_dataset_job_restart_uses_dataset_anchor_start_block(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from defind.api import ops_api

    abi_path = tmp_path / "pool_abi.json"
    _write_minimal_abi(abi_path)
    seen_start_blocks: list[int | str] = []
    final_blocks = [109, 129]

    async def _fake_fetch_data(*, config: Any, registry: Any, on_chunk_written: Any = None) -> FetchDecodeOutput:
        _ = registry
        seen_start_blocks.append(config.start_block)
        if on_chunk_written is not None:
            final_block = final_blocks[len(seen_start_blocks) - 1]
            await on_chunk_written(int(config.start_block), final_block)
        return FetchDecodeOutput(stats=ProcessStats(chunks_written=1, total_logs=1), contract_dir="local://fake")

    monkeypatch.setattr(ops_api, "fetch_data", _fake_fetch_data)

    cfg = OpsApiConfig(out_root=tmp_path)
    async with _make_client(create_app(cfg)) as client:
        created = await _create_dataset(client, abi_path=abi_path)
        assert created.status_code == 200

        first = await client.post("/datasets/uniswap/usdc_weth/jobs", json={"mode": "backfill", "concurrency": 4})
        assert first.status_code == 200
        first_job_id = first.json()["job_id"]

        for _ in range(50):
            status = await client.get(f"/datasets/uniswap/usdc_weth/jobs/{first_job_id}")
            assert status.status_code == 200
            if status.json()["status"] != "running":
                break
            await asyncio.sleep(0.02)

        restarted = await client.post(f"/datasets/uniswap/usdc_weth/jobs/{first_job_id}/restart")
        assert restarted.status_code == 200
        restarted_job_id = restarted.json()["job_id"]
        assert restarted_job_id != first_job_id

        for _ in range(50):
            status = await client.get(f"/datasets/uniswap/usdc_weth/jobs/{restarted_job_id}")
            assert status.status_code == 200
            if status.json()["status"] != "running":
                break
            await asyncio.sleep(0.02)

        dataset = await client.get("/datasets/uniswap/usdc_weth")
        assert dataset.status_code == 200
        assert dataset.json()["last_block"] == 129

    assert seen_start_blocks == [100, 100]


@pytest.mark.asyncio
async def test_root_dataset_job_stop_marks_job_stopped(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from defind.api import ops_api

    abi_path = tmp_path / "pool_abi.json"
    _write_minimal_abi(abi_path)
    release = asyncio.Event()

    async def _slow_fetch_data(*, config: Any, registry: Any, on_chunk_written: Any = None) -> FetchDecodeOutput:
        _ = (config, registry)
        if on_chunk_written is not None:
            await on_chunk_written(100, 109)
        await release.wait()
        return FetchDecodeOutput(stats=ProcessStats(chunks_written=1, total_logs=1), contract_dir="local://fake")

    monkeypatch.setattr(ops_api, "fetch_data", _slow_fetch_data)

    cfg = OpsApiConfig(out_root=tmp_path)
    async with _make_client(create_app(cfg)) as client:
        created = await _create_dataset(client, abi_path=abi_path)
        assert created.status_code == 200

        started = await client.post("/datasets/uniswap/usdc_weth/jobs", json={"mode": "listen", "concurrency": 4})
        assert started.status_code == 200
        job_id = started.json()["job_id"]

        stopped = await client.post(f"/datasets/uniswap/usdc_weth/jobs/{job_id}/stop")
        assert stopped.status_code == 200
        assert stopped.json()["status"] == "stopped"


@pytest.mark.asyncio
async def test_root_dataset_job_stop_returns_immediately_when_cancellation_cleanup_lags(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from defind.api import ops_api

    abi_path = tmp_path / "pool_abi.json"
    _write_minimal_abi(abi_path)
    release = asyncio.Event()

    async def _slow_cancel_fetch_data(*, config: Any, registry: Any, on_chunk_written: Any = None) -> FetchDecodeOutput:
        _ = (config, registry)
        if on_chunk_written is not None:
            await on_chunk_written(100, 109)
        try:
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            await release.wait()
            raise

    monkeypatch.setattr(ops_api, "fetch_data", _slow_cancel_fetch_data)

    cfg = OpsApiConfig(out_root=tmp_path)
    async with _make_client(create_app(cfg)) as client:
        created = await _create_dataset(client, abi_path=abi_path)
        assert created.status_code == 200

        started = await client.post("/datasets/uniswap/usdc_weth/jobs", json={"mode": "listen", "concurrency": 4})
        assert started.status_code == 200
        job_id = started.json()["job_id"]

        stopped = await asyncio.wait_for(
            client.post(f"/datasets/uniswap/usdc_weth/jobs/{job_id}/stop"),
            timeout=0.2,
        )
        assert stopped.status_code == 200
        assert stopped.json()["status"] == "stopped"

        release.set()


@pytest.mark.asyncio
async def test_root_dataset_coverage_reports_gaps(tmp_path: Path) -> None:
    abi_path = tmp_path / "pool_abi.json"
    _write_minimal_abi(abi_path)

    cfg = OpsApiConfig(out_root=tmp_path)
    async with _make_client(create_app(cfg)) as client:
        created = await _create_dataset(client, abi_path=abi_path, chunk_size=10, step=10)
        assert created.status_code == 200

        _write_chunk_marker(tmp_path, protocol="uniswap", contract="usdc_weth", event="Swap", start=100, end=109)
        _write_chunk_marker(tmp_path, protocol="uniswap", contract="usdc_weth", event="Swap", start=120, end=129)

        patched = await client.patch("/datasets/uniswap/usdc_weth", json={"last_block": 129})
        assert patched.status_code == 200

        coverage = await client.get("/datasets/uniswap/usdc_weth/coverage")
        assert coverage.status_code == 200
        body = coverage.json()
        assert body["complete"] is False
        assert body["gaps"] == [
            {
                "range_start": 110,
                "range_end": 119,
                "missing_blocks": 10,
                "detected_at": body["gaps"][0]["detected_at"],
            }
        ]


@pytest.mark.asyncio
async def test_root_indexer_abi_etherscan_endpoint(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    from defind.api import ops_api

    async def _fake_fetch_abi(
        *,
        endpoint_url: str,
        address: str,
        chain_id: int | None,
        api_key: str | None,
    ) -> list[dict[str, Any]]:
        assert endpoint_url == "https://api.etherscan.io/v2/api"
        assert address == "0x0000000000000000000000000000000000000001"
        assert chain_id == 1
        assert api_key == "etherscan-key"
        return _minimal_abi_json()

    monkeypatch.setattr(ops_api, "_fetch_etherscan_abi", _fake_fetch_abi)

    cfg = OpsApiConfig(out_root=tmp_path, etherscan_api_key="etherscan-key")
    async with _make_client(create_app(cfg)) as client:
        response = await client.post(
            "/indexer/abi/etherscan",
            json={
                "address": "0x0000000000000000000000000000000000000001",
            },
        )
        assert response.status_code == 200
        body = response.json()
        assert body["source"] == "etherscan"
        assert body["chainId"] == 1
        assert body["events"][0]["name"] == "Swap"
