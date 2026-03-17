from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path
from typing import Any

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

pytest.importorskip("fastapi")

from defind.api.ops_api import (
    OpsApiConfig,
    _EventStore,
    create_app,
    load_ops_api_config_from_env,
)
from defind.core.use_cases.fetch_decode import ProcessStats
from defind.dataset_state import META_KEY, append_dataset_job, build_job_snapshot, mark_job_terminal
from defind.orchestration.orchestrator import FetchDecodeOutput
from defind.storage.local import LocalChunkStorage


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
    pq.write_table(pa.table({"block_number": pa.array([], type=pa.uint64())}), path)


def _make_client(app: Any) -> httpx.AsyncClient:
    transport = httpx.ASGITransport(app=app, raise_app_exceptions=True)
    return httpx.AsyncClient(transport=transport, base_url="http://test")


@pytest.fixture(autouse=True)
def _stub_default_rpc_chain_head(monkeypatch: pytest.MonkeyPatch) -> None:
    from defind.api import ops_api

    async def _fake_rpc_chain_head(*, rpc_url: str) -> int:
        _ = rpc_url
        return 150

    monkeypatch.setattr(ops_api, "_fetch_rpc_chain_head", _fake_rpc_chain_head)


async def _create_dataset(
    client: httpx.AsyncClient,
    *,
    abi_path: Path,
    protocol: str = "uniswap",
    contract: str = "usdc_weth",
    chain_id: int = 1,
    start_block: int = 100,
    chunk_size: int = 10,
    step: int = 5,
    rpc_url: str = "https://rpc.example.org",
) -> httpx.Response:
    return await client.post(
        "/datasets",
        json={
            "protocol": protocol,
            "contract": contract,
            "contract_address": "0x0000000000000000000000000000000000000001",
            "chain_id": chain_id,
            "start_block": start_block,
            "chunk_size": chunk_size,
            "step": step,
            "storage": "s3",
            "rpc_url": rpc_url,
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
    monkeypatch.setenv("DEFIND_API_LOG_JSON", "false")
    monkeypatch.setenv("DEFIND_API_JSON_LOG_FILE", "./runtime/logs/defind-api.jsonl")
    monkeypatch.setenv("DEFIND_API_LOGS_BACKEND", "loki")
    monkeypatch.setenv("DEFIND_API_LOKI_URL", "http://127.0.0.1:3100")
    (tmp_path / ".env").write_text("ETHERSCAN_API_KEY=test-etherscan-key\n", encoding="utf-8")

    cfg = load_ops_api_config_from_env()

    assert cfg.etherscan_api_key == "test-etherscan-key"
    assert "http://localhost:3000" in cfg.cors_origins
    assert cfg.log_json is False
    assert cfg.json_log_file_path == Path("./runtime/logs/defind-api.jsonl")
    assert cfg.logs_backend == "loki"
    assert cfg.loki_url == "http://127.0.0.1:3100"


@pytest.mark.asyncio
async def test_health_and_ready_endpoints(tmp_path: Path) -> None:
    cfg = OpsApiConfig(out_root=tmp_path)
    async with _make_client(create_app(cfg)) as client:
        health = await client.get("/health")
        ready = await client.get("/ready")

    assert health.status_code == 200
    assert health.json() == {"status": "ok"}
    assert ready.status_code == 200
    assert ready.json() == {"status": "ready"}


@pytest.mark.asyncio
async def test_dataset_routes_return_503_when_storage_backend_is_unavailable(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from defind.api import ops_api

    cfg = OpsApiConfig(out_root=tmp_path)
    failing_storage = object()

    monkeypatch.setattr(
        ops_api,
        "_build_dataset_storage",
        lambda cfg, dataset: (failing_storage, "s3://defind/uniswap/usdc_weth/"),
    )
    monkeypatch.setattr(ops_api, "read_dataset_meta", lambda storage: (_ for _ in ()).throw(OSError("dns failed")))

    async with _make_client(create_app(cfg)) as client:
        res = await client.get("/datasets/uniswap/usdc_weth")

    assert res.status_code == 503
    assert res.json()["detail"]["message"] == "storage backend unavailable"


@pytest.mark.asyncio
async def test_root_dataset_api_create_list_get_patch_and_filter(tmp_path: Path) -> None:
    abi_path = tmp_path / "pool_abi.json"
    _write_minimal_abi(abi_path)

    cfg = OpsApiConfig(out_root=tmp_path, etherscan_api_key="etherscan-key")
    async with _make_client(create_app(cfg)) as client:
        created = await _create_dataset(client, abi_path=abi_path)
        assert created.status_code == 200
        assert created.json()["chain_head"] == 99
        assert created.json()["lag"] == 0

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
        assert row["chain_head"] == 99
        assert row["last_block"] == 99
        assert row["lag"] == 0
        assert row["chunks_total"] == 0
        assert row["status"] == "idle"

        immutable = await client.patch("/datasets/uniswap/usdc_weth", json={"start_block": 999})
        assert immutable.status_code == 400

        bad_storage = await client.patch("/datasets/uniswap/usdc_weth", json={"storage": "local"})
        assert bad_storage.status_code == 400

        patched = await client.patch("/datasets/uniswap/usdc_weth", json={"chunk_size": 12345})
        assert patched.status_code == 200
        assert patched.json()["id"] == "uniswap/usdc_weth"


@pytest.mark.asyncio
async def test_dataset_get_uses_cached_summary_instead_of_storage_scan(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from defind.api.ops.dataset import repository as dataset_repository

    abi_path = tmp_path / "pool_abi.json"
    _write_minimal_abi(abi_path)

    cfg = OpsApiConfig(out_root=tmp_path)
    async with _make_client(create_app(cfg)) as client:
        created = await _create_dataset(client, abi_path=abi_path)
        assert created.status_code == 200

        monkeypatch.setattr(
            dataset_repository,
            "list_dataset_jobs",
            lambda storage: (_ for _ in ()).throw(AssertionError("dataset read must not rescan jobs")),
        )
        monkeypatch.setattr(
            dataset_repository,
            "load_done_chunks",
            lambda storage, event_names: (_ for _ in ()).throw(AssertionError("dataset read must not rescan chunks")),
        )

        single = await client.get("/datasets/uniswap/usdc_weth")
        assert single.status_code == 200
        assert single.json()["id"] == "uniswap/usdc_weth"


@pytest.mark.asyncio
async def test_datasets_list_uses_cached_index_instead_of_root_scan(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from defind.api.ops.dataset import repository as dataset_repository

    abi_path = tmp_path / "pool_abi.json"
    _write_minimal_abi(abi_path)

    cfg = OpsApiConfig(out_root=tmp_path)
    async with _make_client(create_app(cfg)) as client:
        created = await _create_dataset(client, abi_path=abi_path)
        assert created.status_code == 200

        monkeypatch.setattr(
            dataset_repository,
            "discover_dataset_refs",
            lambda storage: (_ for _ in ()).throw(AssertionError("datasets list must not rescan root storage")),
        )

        listed = await client.get("/datasets")
        assert listed.status_code == 200
        assert [row["id"] for row in listed.json()] == ["uniswap/usdc_weth"]


@pytest.mark.asyncio
async def test_dataset_reads_use_root_summary_without_meta_reads(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from defind.api import ops_api

    abi_path = tmp_path / "pool_abi.json"
    _write_minimal_abi(abi_path)
    cfg = OpsApiConfig(out_root=tmp_path)

    async with _make_client(create_app(cfg)) as client:
        created = await _create_dataset(client, abi_path=abi_path)
        assert created.status_code == 200

    monkeypatch.setattr(
        ops_api,
        "read_dataset_meta",
        lambda storage: (_ for _ in ()).throw(AssertionError("read endpoints must use root summary")),
    )

    async with _make_client(create_app(cfg)) as client:
        listed = await client.get("/datasets")
        assert listed.status_code == 200
        assert [row["id"] for row in listed.json()] == ["uniswap/usdc_weth"]

        single = await client.get("/datasets/uniswap/usdc_weth")
        assert single.status_code == 200
        assert single.json()["id"] == "uniswap/usdc_weth"

        status = await client.get("/status")
        assert status.status_code == 200
        assert status.json()["datasets_count"] == 1


@pytest.mark.asyncio
async def test_job_and_log_reads_use_dataset_cache_without_meta_reads(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from defind.api import ops_api
    from defind.api.ops.jobs import repository as jobs_repository_module

    abi_path = tmp_path / "pool_abi.json"
    _write_minimal_abi(abi_path)
    cfg = OpsApiConfig(out_root=tmp_path)
    job_id = "job-1"

    async with _make_client(create_app(cfg)) as client:
        created = await _create_dataset(client, abi_path=abi_path)
        assert created.status_code == 200

    dataset_storage = LocalChunkStorage(tmp_path / "uniswap" / "usdc_weth")
    append_dataset_job(
        dataset_storage,
        build_job_snapshot(
            job_id=job_id,
            mode="both",
            status="completed",
            resume_from=100,
            origin="ui",
            config_snapshot={"mode": "both", "concurrency": 4},
        ),
    )

    monkeypatch.setattr(
        ops_api,
        "read_dataset_meta",
        lambda storage: (_ for _ in ()).throw(AssertionError("job read endpoints must use dataset cache")),
    )
    monkeypatch.setattr(
        jobs_repository_module,
        "get_dataset_job",
        lambda storage, job_id: (_ for _ in ()).throw(AssertionError("job read endpoints must use jobs summary")),
    )

    async with _make_client(create_app(cfg)) as client:
        jobs = await client.get("/datasets/uniswap/usdc_weth/jobs")
        assert jobs.status_code == 200
        assert jobs.json()[0]["job_id"] == job_id
        assert "config_snapshot" not in jobs.json()[0]

        job = await client.get(f"/datasets/uniswap/usdc_weth/jobs/{job_id}")
        assert job.status_code == 200
        assert job.json()["job_id"] == job_id
        assert "config_snapshot" not in job.json()

        logs = await client.get(f"/datasets/uniswap/usdc_weth/jobs/{job_id}/logs?page=1&limit=20")
        assert logs.status_code == 200
        assert logs.json()["items"] == []


@pytest.mark.asyncio
async def test_dataset_reads_accept_legacy_meta_shape(tmp_path: Path) -> None:
    legacy_dir = tmp_path / "legacy" / "pool"
    legacy_dir.mkdir(parents=True, exist_ok=True)
    (legacy_dir / META_KEY).write_text(
        json.dumps(
            {
                "protocol": "legacy",
                "contract": "pool",
                "contract_address": "0x0000000000000000000000000000000000000001",
                "chain_id": 1,
                "start_block": 100,
                "last_block": 150,
                "chunk_size": 10,
                "step": 5,
                "storage": "s3",
                "rpc_url": "https://rpc.example.org",
                "event_names": ["Swap"],
                "migration_status": "incomplete",
                "missing_fields": ["registry_json"],
            }
        ),
        encoding="utf-8",
    )

    cfg = OpsApiConfig(out_root=tmp_path)
    async with _make_client(create_app(cfg)) as client:
        listed = await client.get("/datasets")
        assert listed.status_code == 200
        rows = listed.json()
        assert len(rows) == 1
        assert rows[0]["id"] == "legacy/pool"
        assert rows[0]["last_block"] == 150

        single = await client.get("/datasets/legacy/pool")
        assert single.status_code == 200
        assert single.json()["last_block"] == 150


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
        assert status.json()["chain_head"] == 150

        assert dataset_payload["last_block"] == 109
        assert dataset_payload["chain_head"] == 150
        assert dataset_payload["active_jobs_count"] == 1
        assert running_job_payload["chunks_written"] == 1
        assert running_job_payload["resume_from"] == 109
        assert warn_logs[0]["payload"]["level"] == "WARNING"
        event_keys = [
            str(path.relative_to(tmp_path))
            for path in (tmp_path / "uniswap" / "usdc_weth" / "_meta" / "event_history").rglob("*.json")
        ]
        assert any("job/" in key for key in event_keys)

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
async def test_root_dataset_job_start_rejects_start_block_ahead_of_rpc_head(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from defind.api import ops_api

    abi_path = tmp_path / "pool_abi.json"
    _write_minimal_abi(abi_path)

    async def _unexpected_fetch_data(*, config: Any, registry: Any, on_chunk_written: Any = None) -> FetchDecodeOutput:
        _ = (config, registry, on_chunk_written)
        raise AssertionError("fetch_data should not run when the start_block preflight fails")

    monkeypatch.setattr(ops_api, "fetch_data", _unexpected_fetch_data)

    cfg = OpsApiConfig(out_root=tmp_path)
    async with _make_client(create_app(cfg)) as client:
        created = await _create_dataset(client, abi_path=abi_path, start_block=200)
        assert created.status_code == 200

        started = await client.post("/datasets/uniswap/usdc_weth/jobs", json={"mode": "both", "concurrency": 4})
        assert started.status_code == 400
        assert "start_block 200 is ahead of current RPC chain head 150" in started.json()["detail"]

        jobs = await client.get("/datasets/uniswap/usdc_weth/jobs")
        assert jobs.status_code == 200
        assert jobs.json() == []


@pytest.mark.asyncio
async def test_dataset_and_status_reads_do_not_fetch_live_chain_head(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from defind.api import ops_api

    abi_path = tmp_path / "pool_abi.json"
    _write_minimal_abi(abi_path)

    async def _unexpected_etherscan_head(*, endpoint_url: str, chain_id: int, api_key: str | None) -> int:
        _ = (endpoint_url, chain_id, api_key)
        raise AssertionError("read endpoints must not call etherscan for chain head")

    async def _unexpected_rpc_chain_head(*, rpc_url: str) -> int:
        _ = rpc_url
        raise AssertionError("read endpoints must not call rpc for chain head")

    monkeypatch.setattr(ops_api, "_fetch_etherscan_chain_head", _unexpected_etherscan_head)
    monkeypatch.setattr(ops_api, "_fetch_rpc_chain_head", _unexpected_rpc_chain_head)

    cfg = OpsApiConfig(out_root=tmp_path, etherscan_api_key="etherscan-key")
    async with _make_client(create_app(cfg)) as client:
        created = await _create_dataset(
            client,
            abi_path=abi_path,
            protocol="bean",
            contract="token",
            chain_id=8453,
            rpc_url="https://base.example.org",
        )
        assert created.status_code == 200

        listed = await client.get("/datasets")
        assert listed.status_code == 200
        assert listed.json()[0]["chain_head"] == 99

        dataset = await client.get("/datasets/bean/token")
        assert dataset.status_code == 200
        assert dataset.json()["chain_head"] == 99
        assert dataset.json()["last_block"] == 99
        assert dataset.json()["lag"] == 0

        status = await client.get("/status")
        assert status.status_code == 200
        assert status.json()["chain_head"] == 99
        assert status.json()["lag"] == 0


@pytest.mark.asyncio
async def test_root_dataset_job_start_reports_stale_base_rpc_against_public_head(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from defind.api import ops_api

    abi_path = tmp_path / "pool_abi.json"
    _write_minimal_abi(abi_path)

    async def _unexpected_fetch_data(*, config: Any, registry: Any, on_chunk_written: Any = None) -> FetchDecodeOutput:
        _ = (config, registry, on_chunk_written)
        raise AssertionError("fetch_data should not run when the rpc provider is stale")

    async def _rpc_chain_head(*, rpc_url: str) -> int:
        if rpc_url == "https://base.example.org":
            return 120
        if rpc_url in {"https://mainnet.base.org", "https://base-rpc.publicnode.com"}:
            return 140
        raise AssertionError(f"unexpected rpc url: {rpc_url}")

    monkeypatch.setattr(ops_api, "fetch_data", _unexpected_fetch_data)
    monkeypatch.setattr(ops_api, "_fetch_rpc_chain_head", _rpc_chain_head)

    cfg = OpsApiConfig(out_root=tmp_path)
    async with _make_client(create_app(cfg)) as client:
        created = await _create_dataset(
            client,
            abi_path=abi_path,
            protocol="bean",
            contract="token",
            chain_id=8453,
            start_block=130,
            rpc_url="https://base.example.org",
        )
        assert created.status_code == 200

        started = await client.post("/datasets/bean/token/jobs", json={"mode": "both", "concurrency": 4})
        assert started.status_code == 400
        assert "dataset rpc_url appears stale" in started.json()["detail"]
        assert "rpc head 120 is behind public chain head 140" in started.json()["detail"]


@pytest.mark.asyncio
async def test_root_dataset_job_delete_removes_terminal_job_and_logs(tmp_path: Path) -> None:
    abi_path = tmp_path / "pool_abi.json"
    _write_minimal_abi(abi_path)

    cfg = OpsApiConfig(out_root=tmp_path)
    async with _make_client(create_app(cfg)) as client:
        created = await _create_dataset(client, abi_path=abi_path)
        assert created.status_code == 200

        dataset_storage = LocalChunkStorage(tmp_path / "uniswap" / "usdc_weth")
        control_storage = LocalChunkStorage(tmp_path)
        job_id = "job-1"

        snapshot = build_job_snapshot(
            job_id=job_id,
            mode="both",
            status="running",
            resume_from=100,
            origin="ui",
            config_snapshot={"mode": "both", "concurrency": 4},
        )
        append_dataset_job(dataset_storage, snapshot)
        mark_job_terminal(dataset_storage, job_id=job_id, status="failed", error="boom")

        dataset_events = _EventStore(storage=dataset_storage, prefix="_meta/event_history/", max_events=100)
        control_events = _EventStore(storage=control_storage, prefix="_meta/event_history/", max_events=100)
        await dataset_events.append(
            event_type="chunk_fetch_failed",
            dataset_id="uniswap/usdc_weth",
            job_id=job_id,
            run_id=job_id,
            payload={"level": "ERROR", "rpc_error": "RPC error: -32002 request timed out"},
        )
        await control_events.append(
            event_type="chunk_fetch_failed",
            dataset_id="uniswap/usdc_weth",
            job_id=job_id,
            run_id=job_id,
            payload={"level": "ERROR", "rpc_error": "RPC error: -32002 request timed out"},
        )

        deleted = await client.delete(f"/datasets/uniswap/usdc_weth/jobs/{job_id}")
        assert deleted.status_code == 200
        body = deleted.json()
        assert body["job"]["job_id"] == job_id
        assert body["deleted_log_events"] == 0
        assert body["log_purge_scheduled"] is True

        jobs = await client.get("/datasets/uniswap/usdc_weth/jobs")
        assert jobs.status_code == 200
        assert jobs.json() == []

        for _ in range(50):
            if (
                not dataset_storage.list_keys("_meta/event_history/")
                and not control_storage.list_keys("_meta/event_history/")
            ):
                break
            await asyncio.sleep(0.02)
        assert not dataset_storage.list_keys("_meta/event_history/")
        assert not control_storage.list_keys("_meta/event_history/")


@pytest.mark.asyncio
async def test_root_dataset_job_delete_rejects_running_job(tmp_path: Path) -> None:
    abi_path = tmp_path / "pool_abi.json"
    _write_minimal_abi(abi_path)

    cfg = OpsApiConfig(out_root=tmp_path)
    async with _make_client(create_app(cfg)) as client:
        created = await _create_dataset(client, abi_path=abi_path)
        assert created.status_code == 200

        dataset_storage = LocalChunkStorage(tmp_path / "uniswap" / "usdc_weth")
        job_id = "job-1"
        append_dataset_job(
            dataset_storage,
            build_job_snapshot(
                job_id=job_id,
                mode="both",
                status="running",
                resume_from=100,
                origin="ui",
                config_snapshot={"mode": "both", "concurrency": 4},
            ),
        )

        deleted = await client.delete(f"/datasets/uniswap/usdc_weth/jobs/{job_id}")
        assert deleted.status_code == 409
        assert deleted.json()["detail"] == "running jobs must be stopped before deletion"

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
async def test_root_dataset_job_restart_uses_updated_runtime_config_and_payload_overrides(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from defind.api import ops_api

    abi_path = tmp_path / "pool_abi.json"
    _write_minimal_abi(abi_path)
    seen_configs: list[dict[str, Any]] = []

    async def _fake_fetch_data(*, config: Any, registry: Any, on_chunk_written: Any = None) -> FetchDecodeOutput:
        _ = registry
        seen_configs.append(
            {
                "rpc_url": config.rpc_url,
                "concurrency": config.concurrency,
                "step": config.step,
                "timeout_s": config.timeout_s,
                "rpc_max_retries": config.rpc_max_retries,
                "rpc_retry_backoff_s": config.rpc_retry_backoff_s,
                "listen": config.listen,
                "start_block": config.start_block,
            }
        )
        if on_chunk_written is not None:
            await on_chunk_written(int(config.start_block), 109)
        return FetchDecodeOutput(stats=ProcessStats(chunks_written=1, total_logs=1), contract_dir="local://fake")

    monkeypatch.setattr(ops_api, "fetch_data", _fake_fetch_data)

    cfg = OpsApiConfig(out_root=tmp_path)
    async with _make_client(create_app(cfg)) as client:
        created = await _create_dataset(client, abi_path=abi_path)
        assert created.status_code == 200

        patched = await client.patch(
            "/datasets/uniswap/usdc_weth",
            json={
                "rpc_url": "https://rpc.updated.example.org",
                "step": 7,
                "timeout_s": 45,
                "rpc_max_retries": 9,
                "rpc_retry_backoff_s": 1.25,
            },
        )
        assert patched.status_code == 200
        assert patched.json()["id"] == "uniswap/usdc_weth"

        dataset_storage = LocalChunkStorage(tmp_path / "uniswap" / "usdc_weth")
        append_dataset_job(
            dataset_storage,
            build_job_snapshot(
                job_id="job-1",
                mode="both",
                status="failed",
                resume_from=109,
                origin="ui",
                config_snapshot={
                    "mode": "both",
                    "concurrency": 4,
                    "step": 5,
                    "rpc_url": "https://rpc.example.org",
                    "timeout_s": 20,
                    "rpc_max_retries": 3,
                    "rpc_retry_backoff_s": 0.5,
                },
            ),
        )

        restarted = await client.post(
            "/datasets/uniswap/usdc_weth/jobs/job-1/restart",
            json={"mode": "listen", "concurrency": 7},
        )
        assert restarted.status_code == 200
        body = restarted.json()
        assert body["config_snapshot"]["mode"] == "listen"
        assert body["config_snapshot"]["concurrency"] == 7
        assert body["config_snapshot"]["step"] == 7
        assert body["config_snapshot"]["rpc_url"] == "https://rpc.updated.example.org"
        assert body["config_snapshot"]["timeout_s"] == 45
        assert body["config_snapshot"]["rpc_max_retries"] == 9
        assert body["config_snapshot"]["rpc_retry_backoff_s"] == 1.25

        for _ in range(50):
            status = await client.get(f"/datasets/uniswap/usdc_weth/jobs/{body['job_id']}")
            assert status.status_code == 200
            if status.json()["status"] != "running":
                break
            await asyncio.sleep(0.02)

    assert seen_configs == [
        {
            "rpc_url": "https://rpc.updated.example.org",
            "concurrency": 7,
            "step": 7,
            "timeout_s": 45,
            "rpc_max_retries": 9,
            "rpc_retry_backoff_s": 1.25,
            "listen": True,
            "start_block": 100,
        }
    ]


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
            },
        ]


@pytest.mark.asyncio
async def test_event_store_partitions_keys_by_dataset_and_job(tmp_path: Path) -> None:
    storage = LocalChunkStorage(tmp_path)
    store = _EventStore(storage=storage, prefix="_meta/event_history/", max_events=100)

    await store.append(
        event_type="runtime_log",
        dataset_id="bean/token",
        job_id="123",
        run_id="123",
        payload={"level": "INFO"},
    )
    await store.append(
        event_type="runtime_log",
        dataset_id="other/dataset",
        job_id="999",
        run_id="999",
        payload={"level": "INFO"},
    )

    keys = storage.list_keys("_meta/event_history/")
    assert any("dataset/bean/token/job/123/run/123/type/runtime_log/" in key for key in keys)
    assert any("dataset/other/dataset/job/999/run/999/type/runtime_log/" in key for key in keys)

    rows = await store.list_events(limit=20, dataset_id="bean/token", job_id="123")
    assert len(rows) == 1
    assert rows[0]["datasetId"] == "bean/token"
    assert rows[0]["jobId"] == "123"


@pytest.mark.asyncio
async def test_event_store_reads_legacy_flat_keys_with_dataset_job_filters(tmp_path: Path) -> None:
    storage = LocalChunkStorage(tmp_path)
    store = _EventStore(storage=storage, prefix="_meta/event_history/", max_events=100)
    storage.write_json(
        "_meta/event_history/00000000000000000001_legacy.json",
        {
            "id": 1,
            "tsUnixS": 1,
            "ts": "1970-01-01T00:00:01Z",
            "eventType": "chunk_fetch_failed",
            "datasetId": "Bean/token",
            "jobId": "job-1",
            "runId": "job-1",
            "payload": {"level": "ERROR", "logger": "defind.core.use_cases.fetch_decode"},
        },
    )

    rows = await store.list_events(limit=20, dataset_id="Bean/token", job_id="job-1")
    assert len(rows) == 1
    assert rows[0]["eventType"] == "chunk_fetch_failed"
    assert rows[0]["payload"]["level"] == "ERROR"


@pytest.mark.asyncio
async def test_event_store_assigns_monotonic_unique_row_ids(tmp_path: Path) -> None:
    storage = LocalChunkStorage(tmp_path)
    store = _EventStore(storage=storage, prefix="_meta/event_history/", max_events=100)

    await store.append(
        event_type="runtime_log",
        dataset_id="bean/token",
        job_id="job-1",
        run_id="job-1",
        payload={"level": "INFO", "message": "first"},
    )
    await store.append(
        event_type="runtime_log",
        dataset_id="bean/token",
        job_id="job-1",
        run_id="job-1",
        payload={"level": "INFO", "message": "second"},
    )

    rows = await store.list_events(limit=20, dataset_id="bean/token", job_id="job-1")
    ids = [int(row["id"]) for row in rows]

    assert len(ids) == 2
    assert len(set(ids)) == 2


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
