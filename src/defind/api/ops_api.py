from __future__ import annotations

# mypy: disable-error-code=untyped-decorator
import asyncio
import contextlib
import json
import logging
import os
import re
import time
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal, cast

import httpx
from dotenv import find_dotenv, load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from defind.abi_events import (
    AbiEvent,
    get_event_signature,
    get_event_topic0,
    get_events_from_abi,
    make_event_registry_from_events,
)
from defind.api.fetch_data import fetch_data
from defind.core.config import OrchestratorConfig
from defind.core.interfaces import IChunkStorage
from defind.dataset_state import (
    JOBS_KEY,
    META_KEY,
    active_writer_job,
    append_dataset_job,
    build_dataset_meta,
    build_job_snapshot,
    create_dataset_meta,
    discover_dataset_refs,
    get_dataset_job,
    increment_job_progress,
    list_dataset_jobs,
    mark_job_terminal,
    read_dataset_meta,
    update_dataset_meta,
    validate_meta_patch,
    validate_meta_runtime_fields,
)
from defind.decoding.specs import EventRegistry
from defind.indexer_request import deserialize_registry
from defind.observability import get_logger
from defind.orchestration.utils import load_done_chunks
from defind.orchestration.validator import validate_coverage
from defind.storage.local import LocalChunkStorage
from defind.storage.s3 import S3ChunkStorage

_ETHERSCAN_API_URL = "https://api.etherscan.io/v2/api"
_ADDRESS_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")
logger = get_logger(__name__)


@dataclass(frozen=True)
class DatasetRef:
    protocol: str
    contract: str

    @property
    def dataset_id(self) -> str:
        return f"{self.protocol}/{self.contract}"


@dataclass(frozen=True)
class OpsApiConfig:
    out_root: Path = Path("./data")
    default_chunk_size: int = 200_000

    s3_bucket: str | None = None
    s3_prefix: str = ""
    s3_endpoint_url: str | None = None
    s3_access_key: str | None = None
    s3_secret_key: str | None = None
    s3_region: str = "auto"
    s3_max_retries: int = 3
    s3_retry_backoff_s: float = 0.5

    host: str = "0.0.0.0"
    port: int = 8000
    cors_origins: tuple[str, ...] = ("*",)
    etherscan_api_url: str = _ETHERSCAN_API_URL
    etherscan_api_key: str | None = None
    etherscan_chain_id: int = 1
    event_history_prefix: str = "_meta/event_history/"
    event_history_limit: int = 10_000


class DatasetCreateRequest(BaseModel):
    protocol: str
    contract: str
    contract_address: str
    chain_id: int = Field(ge=1)
    start_block: int = Field(ge=0)
    chunk_size: int = Field(ge=1)
    step: int = Field(ge=1)
    storage: Literal["s3"] = "s3"
    rpc_url: str
    abi_path: str | None = None
    abi_json: list[dict[str, Any]] | None = None
    registry_json: dict[str, Any] | None = None
    event_names: list[str] | None = None


class DatasetJobStartRequest(BaseModel):
    mode: Literal["backfill", "listen", "both"]
    concurrency: int = Field(default=16, ge=1)


class EtherscanAbiRequest(BaseModel):
    address: str
    chain_id: int | None = Field(default=None, ge=1)
    api_key: str | None = None
    endpoint_url: str | None = None


@dataclass(frozen=True)
class _PreparedDatasetRun:
    config: OrchestratorConfig
    registry: EventRegistry
    dataset_id: str
    mode: Literal["backfill", "listen"]
    public_config: dict[str, Any]


_RESERVED_LOG_RECORD_KEYS = frozenset(logging.makeLogRecord({}).__dict__.keys())


def _parse_int(value: str | None, *, default: int, min_value: int = 0) -> int:
    if value is None or value == "":
        return default
    return max(min_value, int(value))


def _parse_float(value: str | None, *, default: float, min_value: float = 0.0) -> float:
    if value is None or value == "":
        return default
    return max(min_value, float(value))


def _parse_cors_origins(raw: str | None) -> tuple[str, ...]:
    if raw is None or raw.strip() == "":
        return ("*",)
    out = tuple(item.strip() for item in raw.split(",") if item.strip())
    return out or ("*",)


def _clean_optional_str(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    return stripped if stripped else None


def _to_iso_z(ts_unix_s: int) -> str:
    return datetime.fromtimestamp(ts_unix_s, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _normalize_collection_prefix(raw: str | None, *, fallback: str) -> str:
    cleaned = (raw or fallback).strip().strip("/")
    if not cleaned:
        cleaned = fallback.strip("/")
    if cleaned.endswith(".json"):
        cleaned = cleaned.removesuffix(".json")
    return f"{cleaned.rstrip('/')}/"


def _normalize_etherscan_endpoint(endpoint_url: str) -> str:
    cleaned = endpoint_url.strip().rstrip("/")
    if cleaned.endswith("/v2/api"):
        return cleaned
    if cleaned.endswith("/api"):
        return f"{cleaned[:-4]}/v2/api"
    if "/v2/" in cleaned:
        return cleaned
    return f"{cleaned}/v2/api"


def _is_hex_address(value: str) -> bool:
    return bool(_ADDRESS_RE.match(value))


def _log_record_payload(record: logging.LogRecord) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "level": record.levelname,
        "logger": record.name,
    }
    for key, value in record.__dict__.items():
        if key in _RESERVED_LOG_RECORD_KEYS or key.startswith("_"):
            continue
        payload[key] = value
    return payload


def load_ops_api_config_from_env() -> OpsApiConfig:
    load_dotenv(find_dotenv(usecwd=True))
    return OpsApiConfig(
        out_root=Path(os.getenv("DEFIND_API_OUT_ROOT", "./data")),
        default_chunk_size=_parse_int(os.getenv("DEFIND_API_DEFAULT_CHUNK_SIZE"), default=200_000, min_value=1),
        s3_bucket=os.getenv("S3_BUCKET") or os.getenv("DEFIND_API_S3_BUCKET"),
        s3_prefix=(os.getenv("S3_PREFIX") or os.getenv("DEFIND_API_S3_PREFIX", "") or ""),
        s3_endpoint_url=os.getenv("S3_ENDPOINT_URL") or os.getenv("DEFIND_API_S3_ENDPOINT_URL"),
        s3_access_key=os.getenv("S3_ACCESS_KEY") or os.getenv("DEFIND_API_S3_ACCESS_KEY"),
        s3_secret_key=os.getenv("S3_SECRET_KEY") or os.getenv("DEFIND_API_S3_SECRET_KEY"),
        s3_region=(os.getenv("S3_REGION") or os.getenv("DEFIND_API_S3_REGION", "auto") or "auto"),
        s3_max_retries=_parse_int(os.getenv("DEFIND_API_S3_MAX_RETRIES"), default=3),
        s3_retry_backoff_s=_parse_float(os.getenv("DEFIND_API_S3_RETRY_BACKOFF_S"), default=0.5),
        host=os.getenv("DEFIND_API_HOST", "0.0.0.0"),
        port=_parse_int(os.getenv("DEFIND_API_PORT"), default=8000, min_value=1),
        cors_origins=_parse_cors_origins(os.getenv("DEFIND_API_CORS_ORIGINS")),
        etherscan_api_url=os.getenv("DEFIND_API_ETHERSCAN_API_URL", _ETHERSCAN_API_URL),
        etherscan_api_key=os.getenv("ETHERSCAN_API_KEY") or os.getenv("DEFIND_API_ETHERSCAN_API_KEY"),
        etherscan_chain_id=_parse_int(os.getenv("DEFIND_API_ETHERSCAN_CHAIN_ID"), default=1, min_value=1),
        event_history_prefix=(
            os.getenv("DEFIND_API_EVENT_HISTORY_PREFIX")
            or os.getenv("DEFIND_API_HISTORY_KEY")
            or "_meta/event_history/"
        ),
        event_history_limit=_parse_int(
            os.getenv("DEFIND_API_EVENT_HISTORY_LIMIT") or os.getenv("DEFIND_API_HISTORY_MAX_EVENTS"),
            default=10_000,
            min_value=100,
        ),
    )


def _build_dataset_storage(cfg: OpsApiConfig, dataset: DatasetRef) -> tuple[IChunkStorage, str]:
    if cfg.s3_bucket:
        subpath = f"{dataset.protocol}/{dataset.contract}"
        prefix = f"{cfg.s3_prefix.rstrip('/')}/{subpath}/" if cfg.s3_prefix else f"{subpath}/"
        storage = S3ChunkStorage(
            bucket=cfg.s3_bucket,
            prefix=prefix,
            endpoint_url=cfg.s3_endpoint_url,
            access_key=cfg.s3_access_key,
            secret_key=cfg.s3_secret_key,
            region=cfg.s3_region,
            max_retries=cfg.s3_max_retries,
            retry_backoff_s=cfg.s3_retry_backoff_s,
        )
        return storage, f"s3://{cfg.s3_bucket}/{prefix}"

    root = cfg.out_root / dataset.protocol / dataset.contract
    return LocalChunkStorage(root), str(root)


def _build_control_storage(cfg: OpsApiConfig) -> IChunkStorage:
    if cfg.s3_bucket:
        prefix = cfg.s3_prefix.rstrip("/") if cfg.s3_prefix else ""
        control_prefix = f"{prefix}/" if prefix else ""
        return S3ChunkStorage(
            bucket=cfg.s3_bucket,
            prefix=control_prefix,
            endpoint_url=cfg.s3_endpoint_url,
            access_key=cfg.s3_access_key,
            secret_key=cfg.s3_secret_key,
            region=cfg.s3_region,
            max_retries=cfg.s3_max_retries,
            retry_backoff_s=cfg.s3_retry_backoff_s,
        )
    return LocalChunkStorage(cfg.out_root)


def _discover_meta_datasets(cfg: OpsApiConfig) -> list[DatasetRef]:
    root_storage = _build_control_storage(cfg)
    return [
        DatasetRef(protocol=protocol, contract=contract)
        for protocol, contract in discover_dataset_refs(root_storage)
    ]


def _read_dataset_meta_or_404(cfg: OpsApiConfig, dataset: DatasetRef) -> dict[str, Any]:
    storage, _ = _build_dataset_storage(cfg, dataset)
    meta = read_dataset_meta(storage)
    if not isinstance(meta, dict):
        raise HTTPException(status_code=404, detail="dataset not found")
    return meta


async def _fetch_etherscan_chain_head(
    *,
    endpoint_url: str,
    chain_id: int,
    api_key: str | None,
) -> int:
    params: dict[str, Any] = {
        "chainid": chain_id,
        "module": "proxy",
        "action": "eth_blockNumber",
    }
    if api_key:
        params["apikey"] = api_key

    async with httpx.AsyncClient(timeout=20.0) as client:
        response = await client.get(endpoint_url, params=params)
        response.raise_for_status()
        payload = response.json()

    if not isinstance(payload, dict):
        raise ValueError("etherscan chain head response is not a JSON object")
    result = payload.get("result")
    if not isinstance(result, str):
        raise ValueError("etherscan chain head response is missing result")
    try:
        return int(result, 16)
    except ValueError as exc:
        raise ValueError(f"invalid etherscan chain head result: {result}") from exc


async def _resolve_meta_chain_head(cfg: OpsApiConfig, metas: list[dict[str, Any]]) -> int:
    if not metas:
        return 0
    fallback = max(int(meta.get("last_block") or 0) for meta in metas)
    api_key = _clean_optional_str(cfg.etherscan_api_key)
    if api_key is None:
        return fallback
    try:
        return int(
            await _fetch_etherscan_chain_head(
                endpoint_url=_normalize_etherscan_endpoint(cfg.etherscan_api_url),
                chain_id=cfg.etherscan_chain_id,
                api_key=api_key,
            )
        )
    except Exception:
        return fallback


def _events_from_inputs(
    *,
    abi_path: str | None,
    abi_json: list[dict[str, Any]] | None,
) -> dict[str, AbiEvent]:
    if abi_json is not None:
        try:
            events = get_events_from_abi(abi_json)
        except Exception as exc:
            raise ValueError(f"invalid abi_json: {exc}") from exc
        if events:
            return events
        raise ValueError("abi_json produced an empty event list")

    normalized_path = (abi_path or "").strip()
    if not normalized_path:
        raise ValueError("abi_path, abi_json or registry_json is required")

    path = Path(normalized_path).expanduser()
    if not path.exists():
        raise ValueError(f"abi_path does not exist: {path}")
    if not path.is_file():
        raise ValueError(f"abi_path must be a file: {path}")

    try:
        events = get_events_from_abi(path)
    except Exception as exc:
        raise ValueError(f"invalid abi_path: {exc}") from exc
    if events:
        return events
    raise ValueError("abi_path produced an empty event list")


def _selected_events(events: dict[str, AbiEvent], selected_names: list[str] | None) -> list[AbiEvent]:
    if not selected_names:
        return list(events.values())

    unique: list[str] = []
    seen: set[str] = set()
    for name in selected_names:
        normalized = str(name).strip()
        if not normalized or normalized in seen:
            continue
        unique.append(normalized)
        seen.add(normalized)

    missing = [name for name in unique if name not in events]
    if missing:
        available = ", ".join(sorted(events.keys()))
        raise ValueError(f"unknown event_names: {missing}; available: [{available}]")
    return [events[name] for name in unique]


def _selected_registry(
    registry: EventRegistry,
    selected_names: list[str] | None,
) -> tuple[EventRegistry, list[str]]:
    if not selected_names:
        names = [spec.name for spec in registry.values()]
        return registry, names

    unique: list[str] = []
    seen: set[str] = set()
    for name in selected_names:
        normalized = str(name).strip()
        if not normalized or normalized in seen:
            continue
        unique.append(normalized)
        seen.add(normalized)

    available = {spec.name for spec in registry.values()}
    missing = [name for name in unique if name not in available]
    if missing:
        available_names = ", ".join(sorted(available))
        raise ValueError(f"unknown event_names: {missing}; available: [{available_names}]")

    selected = {topic0: spec for topic0, spec in registry.items() if spec.name in seen}
    if not selected:
        raise ValueError("selected events produced an empty registry")
    return selected, unique


def _build_registry_from_inputs(
    *,
    abi_path: str | None,
    abi_json: list[dict[str, Any]] | None,
    registry_json: dict[str, Any] | None,
    event_names: list[str] | None,
) -> tuple[EventRegistry, list[str]]:
    if registry_json is not None:
        registry = deserialize_registry(registry_json)
        return _selected_registry(registry, event_names)

    events = _events_from_inputs(abi_path=abi_path, abi_json=abi_json)
    selected = _selected_events(events, event_names)
    registry = make_event_registry_from_events(selected)
    if not registry:
        raise ValueError("selected events produced an empty registry")
    return registry, [event.name for event in selected]


def _event_descriptors(events: dict[str, AbiEvent]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for name in sorted(events.keys()):
        ev = events[name]
        out.append(
            {
                "name": ev.name,
                "signature": get_event_signature(ev),
                "topic0": get_event_topic0(ev),
                "indexedInputs": sum(1 for item in ev.inputs if item.indexed),
                "nonIndexedInputs": sum(1 for item in ev.inputs if not item.indexed),
                "inputs": [
                    {
                        "name": item.name,
                        "type": item.type,
                        "indexed": item.indexed,
                    }
                    for item in ev.inputs
                ],
            }
        )
    return out


async def _fetch_etherscan_abi(
    *,
    endpoint_url: str,
    address: str,
    chain_id: int | None,
    api_key: str | None,
) -> list[dict[str, Any]]:
    params: dict[str, Any] = {
        "module": "contract",
        "action": "getabi",
        "address": address,
    }
    if chain_id is not None:
        params["chainid"] = chain_id
    if api_key:
        params["apikey"] = api_key

    async with httpx.AsyncClient(timeout=20.0) as client:
        response = await client.get(endpoint_url, params=params)
        response.raise_for_status()
        payload = response.json()

    if not isinstance(payload, dict):
        raise ValueError("etherscan response is not a JSON object")

    status = payload.get("status")
    message = str(payload.get("message") or "").strip()
    result = payload.get("result")

    if isinstance(result, list):
        return [row for row in result if isinstance(row, dict)]

    if isinstance(result, str):
        stripped = result.strip()
        if "deprecated v1 endpoint" in stripped.lower():
            raise ValueError("deprecated etherscan v1 endpoint; use https://api.etherscan.io/v2/api")
        if stripped.startswith("[") and stripped.endswith("]"):
            decoded = json.loads(stripped)
            if not isinstance(decoded, list):
                raise ValueError("etherscan ABI result is not a JSON array")
            return [row for row in decoded if isinstance(row, dict)]
        if str(status) != "1":
            raise ValueError(stripped or f"etherscan error: {message or 'unknown'}")

    if str(status) != "1":
        raise ValueError(str(result) if result is not None else (message or "etherscan error"))

    raise ValueError("etherscan ABI payload is empty or invalid")


def _dataset_from_route(protocol: str, contract: str) -> DatasetRef:
    normalized_protocol = protocol.strip()
    normalized_contract = contract.strip()
    if not normalized_protocol or not normalized_contract:
        raise HTTPException(status_code=400, detail="protocol and contract are required")
    return DatasetRef(protocol=normalized_protocol, contract=normalized_contract)


def _dataset_context(protocol: str, contract: str, cfg: OpsApiConfig) -> tuple[DatasetRef, dict[str, Any], IChunkStorage]:
    dataset = _dataset_from_route(protocol, contract)
    meta = _read_dataset_meta_or_404(cfg, dataset)
    storage, _ = _build_dataset_storage(cfg, dataset)
    return dataset, meta, storage


def _meta_dataset_row(cfg: OpsApiConfig, dataset: DatasetRef, meta: dict[str, Any], *, chain_head: int) -> dict[str, Any]:
    storage, location = _build_dataset_storage(cfg, dataset)
    jobs = list_dataset_jobs(storage)
    latest = jobs[0] if jobs else None
    last_block = int(meta.get("last_block") or meta.get("start_block") or 0)
    return {
        **meta,
        "id": dataset.dataset_id,
        "location": location,
        "lag": max(0, chain_head - last_block),
        "active_jobs_count": sum(1 for row in jobs if str(row.get("status") or "") == "running"),
        "status": str((latest or {}).get("status") or "idle"),
    }


def _dataset_chunks_total(dataset: DatasetRef, meta: dict[str, Any], cfg: OpsApiConfig) -> int:
    storage, _ = _build_dataset_storage(cfg, dataset)
    event_names = meta.get("event_names")
    if not isinstance(event_names, list) or not event_names:
        return 0
    return len(load_done_chunks(storage, [str(item) for item in event_names]))


def _normalize_dataset_patch(payload: dict[str, Any]) -> dict[str, Any]:
    try:
        validate_meta_patch(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    normalized = dict(payload)
    if "storage" in normalized and str(normalized.get("storage") or "") != "s3":
        raise HTTPException(status_code=400, detail="storage must remain 's3'")
    if "contract_address" in normalized and not _is_hex_address(str(normalized.get("contract_address") or "").strip()):
        raise HTTPException(status_code=400, detail="invalid contract address")
    if "registry_json" in normalized:
        registry_json = normalized.get("registry_json")
        if not isinstance(registry_json, dict):
            raise HTTPException(status_code=400, detail="registry_json must be an object")
        registry = deserialize_registry(registry_json)
        if "event_names" not in normalized:
            normalized["event_names"] = [spec.name for spec in registry.values()]
    return normalized


def _prepare_dataset_job_run(
    *,
    cfg: OpsApiConfig,
    dataset: DatasetRef,
    meta: dict[str, Any],
    mode: str,
    concurrency: int,
    origin: str,
    resume_from: int,
) -> _PreparedDatasetRun:
    registry = validate_meta_runtime_fields(meta)
    listen = mode in {"listen", "both"}
    public_config: dict[str, Any] = {
        "protocol": dataset.protocol,
        "contract": dataset.contract,
        "contract_address": str(meta.get("contract_address") or ""),
        "chain_id": int(meta.get("chain_id") or 0),
        "rpc_url": str(meta.get("rpc_url") or ""),
        "event_names": list(cast(list[str], meta.get("event_names") or [])),
        "registry_json": cast(dict[str, Any], meta.get("registry_json") or {}),
        "start_block": int(meta.get("start_block") or 0),
        "resume_from": int(resume_from),
        "chunk_size": int(meta.get("chunk_size") or cfg.default_chunk_size),
        "step": int(meta.get("step") or 1),
        "mode": mode,
        "concurrency": int(concurrency),
        "origin": origin,
        "storage": str(meta.get("storage") or "s3"),
    }
    orchestrator_cfg = OrchestratorConfig(
        rpc_url=str(meta.get("rpc_url") or ""),
        address=str(meta.get("contract_address") or ""),
        topic0s=list(registry.keys()),
        # Keep the immutable dataset anchor so restart can rewrite the
        # partial tail chunk instead of creating an adjacent duplicate.
        start_block=int(meta.get("start_block") or resume_from),
        end_block="latest",
        protocol_slug=dataset.protocol,
        contract_slug=dataset.contract,
        step=int(meta.get("step") or 1),
        chunk_size=int(meta.get("chunk_size") or cfg.default_chunk_size),
        concurrency=int(concurrency),
        listen=listen,
        heartbeat_interval_s=0.0,
        lag_warn_threshold_blocks=0,
        single_writer_guard=False,
        out_root=cfg.out_root,
        s3_bucket=cfg.s3_bucket,
        s3_prefix=cfg.s3_prefix,
        s3_endpoint_url=cfg.s3_endpoint_url,
        s3_access_key=cfg.s3_access_key,
        s3_secret_key=cfg.s3_secret_key,
        s3_region=cfg.s3_region,
        s3_max_retries=cfg.s3_max_retries,
        s3_retry_backoff_s=cfg.s3_retry_backoff_s,
    )
    return _PreparedDatasetRun(
        config=orchestrator_cfg,
        registry=registry,
        dataset_id=dataset.dataset_id,
        mode="listen" if listen else "backfill",
        public_config=public_config,
    )


class _JobRuntimeLogHandler(logging.Handler):
    def __init__(
        self,
        *,
        dataset_id: str,
        job_id: str,
        run_id: str,
        emit_event: Callable[[str, str | None, str | None, str | None, dict[str, Any] | None], Awaitable[None]],
    ) -> None:
        super().__init__(level=logging.INFO)
        self._dataset_id = dataset_id
        self._job_id = job_id
        self._run_id = run_id
        self._emit_event = emit_event

    def emit(self, record: logging.LogRecord) -> None:
        if not record.name.startswith("defind."):
            return
        if getattr(record, "dataset_id", None) != self._dataset_id:
            return
        message = str(record.getMessage() or "").strip()
        if not message:
            return
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return
        asyncio.create_task(
            self._emit_event(
                message,
                self._dataset_id,
                self._job_id,
                self._run_id,
                _log_record_payload(record),
            )
        )


class _EventStore:
    def __init__(self, *, storage: IChunkStorage, prefix: str, max_events: int) -> None:
        self._storage = storage
        self._prefix = _normalize_collection_prefix(prefix, fallback="_meta/event_history")
        self._max_events = max(100, int(max_events))
        self._write_lock = asyncio.Lock()

    @staticmethod
    def _decode_payload(payload: Any) -> dict[str, Any]:
        if isinstance(payload, dict):
            return cast(dict[str, Any], payload)
        return {}

    def _event_key(self, event_id: int) -> str:
        return f"{self._prefix}{event_id:020d}_{uuid.uuid4().hex}.json"

    def _normalize_event(self, payload: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": int(payload.get("id", 0) or 0),
            "tsUnixS": int(payload.get("tsUnixS", 0) or 0),
            "ts": str(payload.get("ts") or ""),
            "eventType": str(payload.get("eventType") or ""),
            "datasetId": payload.get("datasetId"),
            "jobId": payload.get("jobId"),
            "runId": payload.get("runId"),
            "payload": self._decode_payload(payload.get("payload")),
        }

    def _list_event_keys(self) -> list[str]:
        return sorted(
            key
            for key in self._storage.list_keys(self._prefix)
            if key.startswith(self._prefix) and key.endswith(".json")
        )

    def _load_event_key(self, key: str) -> dict[str, Any] | None:
        payload = self._storage.read_json(key)
        if not isinstance(payload, dict):
            return None
        return self._normalize_event(payload)

    def _trim_old_events_locked(self) -> None:
        keys = self._list_event_keys()
        if len(keys) <= self._max_events:
            return
        for key in keys[: len(keys) - self._max_events]:
            self._storage.delete(key)

    async def append(
        self,
        *,
        event_type: str,
        dataset_id: str | None = None,
        job_id: str | None = None,
        run_id: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> None:
        event_id = time.time_ns()
        ts_unix_s = int(time.time())
        row: dict[str, Any] = {
            "id": int(time.time() * 1000),
            "tsUnixS": ts_unix_s,
            "ts": _to_iso_z(ts_unix_s),
            "eventType": event_type,
            "datasetId": dataset_id,
            "jobId": job_id,
            "runId": run_id,
            "payload": payload or {},
        }
        async with self._write_lock:
            self._storage.write_json(self._event_key(event_id), row)
            self._trim_old_events_locked()

    async def list_events(
        self,
        *,
        limit: int,
        dataset_id: str | None = None,
        job_id: str | None = None,
        run_id: str | None = None,
        event_type: str | None = None,
    ) -> list[dict[str, Any]]:
        capped_limit = max(1, min(1000, int(limit)))
        async with self._write_lock:
            keys = self._list_event_keys()
            events = [row for row in (self._load_event_key(key) for key in keys) if row is not None]
        filtered: list[dict[str, Any]] = []
        for row in events:
            if dataset_id is not None and row.get("datasetId") != dataset_id:
                continue
            if job_id is not None and row.get("jobId") != job_id:
                continue
            if run_id is not None and row.get("runId") != run_id:
                continue
            if event_type is not None and row.get("eventType") != event_type:
                continue
            filtered.append(self._normalize_event(row))
        filtered.sort(key=lambda item: (cast(int, item["tsUnixS"]), cast(int, item["id"])), reverse=True)
        return filtered[:capped_limit]


@dataclass
class _DatasetRuntimeJob:
    dataset: DatasetRef
    job_id: str
    mode: str
    task: asyncio.Task[None]


class _DatasetJobRuntimeManager:
    def __init__(
        self,
        *,
        cfg: OpsApiConfig,
        emit_runtime_event: Callable[[str, str | None, str | None, str | None, dict[str, Any] | None], Awaitable[None]],
        emit_history_event: Callable[[str, str | None, str | None, str | None, dict[str, Any] | None], Awaitable[None]],
    ) -> None:
        self._cfg = cfg
        self._emit_runtime_event = emit_runtime_event
        self._emit_history_event = emit_history_event
        self._tasks: dict[str, _DatasetRuntimeJob] = {}
        self._lock = asyncio.Lock()

    @staticmethod
    def _make_job_id() -> str:
        return str(time.time_ns())

    async def recover_stale_jobs(self) -> None:
        for dataset in _discover_meta_datasets(self._cfg):
            storage, _ = _build_dataset_storage(self._cfg, dataset)
            running = active_writer_job(storage)
            if running is None:
                continue
            try:
                mark_job_terminal(
                    storage,
                    job_id=str(running.get("job_id") or ""),
                    status="failed",
                    error="api restarted while job was running",
                )
            except Exception:
                logger.warning("dataset_job_recovery_failed", extra={"dataset_id": dataset.dataset_id}, exc_info=True)

    async def start(
        self,
        *,
        dataset: DatasetRef,
        meta: dict[str, Any],
        mode: str,
        concurrency: int,
        origin: str,
        source_job_id: str | None = None,
    ) -> dict[str, Any]:
        storage, _ = _build_dataset_storage(self._cfg, dataset)
        blocking = active_writer_job(storage)
        if blocking is not None:
            blocking_job_id = str(blocking.get("job_id") or "")
            raise RuntimeError(f"writer_job_already_active:{blocking_job_id}")

        resume_from = int(meta.get("last_block") or meta.get("start_block") or 0)
        prepared = _prepare_dataset_job_run(
            cfg=self._cfg,
            dataset=dataset,
            meta=meta,
            mode=mode,
            concurrency=concurrency,
            origin=origin,
            resume_from=resume_from,
        )
        job_id = self._make_job_id()
        job_row = build_job_snapshot(
            job_id=job_id,
            mode=mode,
            status="running",
            resume_from=resume_from,
            origin=origin,
            config_snapshot=prepared.public_config,
        )
        append_dataset_job(storage, job_row)

        task = asyncio.create_task(
            self._run_job(dataset=dataset, prepared=prepared, job_id=job_id),
            name=f"defind-dataset-job-{dataset.dataset_id}-{job_id}",
        )
        async with self._lock:
            self._tasks[job_id] = _DatasetRuntimeJob(dataset=dataset, job_id=job_id, mode=mode, task=task)

        await self._emit_history_event(
            event_type="dataset_job_started",
            dataset_id=dataset.dataset_id,
            job_id=job_id,
            run_id=job_id,
            payload={"mode": mode, "origin": origin, "sourceJobId": source_job_id},
        )
        return get_dataset_job(storage, job_id) or job_row

    async def _run_job(
        self,
        *,
        dataset: DatasetRef,
        prepared: _PreparedDatasetRun,
        job_id: str,
    ) -> None:
        storage, _ = _build_dataset_storage(self._cfg, dataset)
        runtime_log_handler = _JobRuntimeLogHandler(
            dataset_id=dataset.dataset_id,
            job_id=job_id,
            run_id=job_id,
            emit_event=self._emit_runtime_event,
        )
        root_logger = logging.getLogger()
        root_logger.addHandler(runtime_log_handler)

        async def _on_chunk_written(_: int, confirmed_to_block: int) -> None:
            update_dataset_meta(
                storage,
                lambda current: {
                    **current,
                    "last_block": max(int(current.get("last_block") or 0), int(confirmed_to_block)),
                },
            )
            increment_job_progress(storage, job_id=job_id, confirmed_to_block=confirmed_to_block)

        try:
            await fetch_data(config=prepared.config, registry=prepared.registry, on_chunk_written=_on_chunk_written)
        except asyncio.CancelledError:
            row = get_dataset_job(storage, job_id)
            if str((row or {}).get("status") or "") != "stopped":
                with contextlib.suppress(Exception):
                    mark_job_terminal(storage, job_id=job_id, status="stopped", error=None)
                await self._emit_history_event(
                    event_type="dataset_job_stopped",
                    dataset_id=dataset.dataset_id,
                    job_id=job_id,
                    run_id=job_id,
                    payload={},
                )
            raise
        except Exception as exc:
            detail = _exception_detail(exc)
            with contextlib.suppress(Exception):
                mark_job_terminal(storage, job_id=job_id, status="failed", error=detail)
            await self._emit_history_event(
                event_type="dataset_job_failed",
                dataset_id=dataset.dataset_id,
                job_id=job_id,
                run_id=job_id,
                payload={"error": detail},
            )
        else:
            with contextlib.suppress(Exception):
                mark_job_terminal(storage, job_id=job_id, status="completed", error=None)
            await self._emit_history_event(
                event_type="dataset_job_completed",
                dataset_id=dataset.dataset_id,
                job_id=job_id,
                run_id=job_id,
                payload={},
            )
        finally:
            root_logger.removeHandler(runtime_log_handler)
            runtime_log_handler.close()
            async with self._lock:
                self._tasks.pop(job_id, None)

    async def stop(self, *, dataset: DatasetRef, job_id: str) -> dict[str, Any]:
        storage, _ = _build_dataset_storage(self._cfg, dataset)
        task: asyncio.Task[None] | None = None
        async with self._lock:
            active = self._tasks.get(job_id)
            if active is not None:
                task = active.task
        if task is None:
            row = get_dataset_job(storage, job_id)
            if row is None:
                raise KeyError(job_id)
            return row
        task.cancel()
        row = get_dataset_job(storage, job_id)
        if row is None:
            raise KeyError(job_id)
        if str(row.get("status") or "") == "running":
            row = mark_job_terminal(storage, job_id=job_id, status="stopped", error=None)
            await self._emit_history_event(
                event_type="dataset_job_stopped",
                dataset_id=dataset.dataset_id,
                job_id=job_id,
                run_id=job_id,
                payload={"source": "api-stop"},
            )
        return row

    async def shutdown(self) -> None:
        async with self._lock:
            tasks = [item.task for item in self._tasks.values()]
        for task in tasks:
            task.cancel()
        if tasks:
            with contextlib.suppress(Exception):
                await asyncio.gather(*tasks, return_exceptions=True)


def _exception_detail(exc: BaseException) -> str:
    seen: set[int] = set()
    parts: list[str] = []
    current: BaseException | None = exc
    depth = 0
    while current is not None and depth < 4 and id(current) not in seen:
        seen.add(id(current))
        message = str(current).strip()
        label = type(current).__name__
        parts.append(f"{label}: {message}" if message else label)
        current = current.__cause__ or current.__context__
        depth += 1
    return " <- ".join(parts) if parts else type(exc).__name__


def create_app(config: OpsApiConfig | None = None) -> FastAPI:
    cfg = config or load_ops_api_config_from_env()
    control_storage = _build_control_storage(cfg)
    event_store = _EventStore(
        storage=control_storage,
        prefix=cfg.event_history_prefix,
        max_events=cfg.event_history_limit,
    )

    async def _record_event(
        event_type: str,
        dataset_id: str | None = None,
        job_id: str | None = None,
        run_id: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> None:
        try:
            await event_store.append(
                event_type=event_type,
                dataset_id=dataset_id,
                job_id=job_id,
                run_id=run_id,
                payload=payload,
            )
        except Exception as exc:
            logger.error(
                "event_store_persist_failed",
                extra={
                    "event_type": event_type,
                    "dataset_id": dataset_id,
                    "job_id": job_id,
                    "run_id": run_id,
                    "error": str(exc),
                },
                exc_info=True,
            )

    dataset_job_manager = _DatasetJobRuntimeManager(
        cfg=cfg,
        emit_runtime_event=_record_event,
        emit_history_event=_record_event,
    )

    @contextlib.asynccontextmanager
    async def _lifespan(_: FastAPI) -> AsyncIterator[None]:
        try:
            await dataset_job_manager.recover_stale_jobs()
            yield
        finally:
            await dataset_job_manager.shutdown()

    app = FastAPI(title="Defind Ops API", version="0.2.0", lifespan=_lifespan)
    app.state.ops_cfg = cfg
    app.state.dataset_job_manager = dataset_job_manager

    app.add_middleware(
        CORSMiddleware,
        allow_origins=list(cfg.cors_origins),
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    def _dataset_job_start_exception(exc: RuntimeError) -> HTTPException:
        message = str(exc)
        if message.startswith("writer_job_already_active:"):
            blocking_job_id = message.split(":", 1)[1]
            return HTTPException(
                status_code=409,
                detail={
                    "message": "A writer job is already active on this dataset",
                    "blocking_job_id": blocking_job_id,
                },
            )
        return HTTPException(status_code=400, detail=message)

    async def _start_dataset_job(
        *,
        dataset: DatasetRef,
        meta: dict[str, Any],
        mode: str,
        concurrency: int,
        source_job_id: str | None = None,
    ) -> dict[str, Any]:
        try:
            return await dataset_job_manager.start(
                dataset=dataset,
                meta=meta,
                mode=mode,
                concurrency=concurrency,
                origin="ui",
                source_job_id=source_job_id,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except RuntimeError as exc:
            raise _dataset_job_start_exception(exc) from exc

    async def _list_job_logs(
        *,
        dataset: DatasetRef,
        job_id: str,
        page: int,
        limit: int,
        level: str,
    ) -> dict[str, Any]:
        events = await event_store.list_events(
            limit=min(1000, max(1, page) * max(1, limit) * 5),
            dataset_id=dataset.dataset_id,
            job_id=job_id,
        )
        normalized_level = level.upper().strip() or "ALL"
        filtered: list[dict[str, Any]] = []
        for row in events:
            payload = row.get("payload")
            payload_level = str((payload or {}).get("level") or "").upper()
            if normalized_level == "ERROR" and payload_level != "ERROR":
                continue
            if normalized_level == "WARN" and payload_level not in {"WARNING", "WARN", "ERROR"}:
                continue
            filtered.append(row)
        start = (page - 1) * limit
        end = start + limit
        return {
            "page": page,
            "limit": limit,
            "items": filtered[start:end],
        }

    async def _job_log_stream(dataset: DatasetRef, job_id: str) -> AsyncIterator[bytes]:
        last_seen_id = 0
        while True:
            events = await event_store.list_events(limit=200, dataset_id=dataset.dataset_id, job_id=job_id)
            new_rows = [row for row in reversed(events) if int(row.get("id") or 0) > last_seen_id]
            for row in new_rows:
                last_seen_id = max(last_seen_id, int(row.get("id") or 0))
                yield f"data: {json.dumps(row, separators=(',', ':'))}\n\n".encode("utf-8")

            storage, _ = _build_dataset_storage(cfg, dataset)
            job = get_dataset_job(storage, job_id)
            if job is None:
                break
            if str(job.get("status") or "") in {"stopped", "failed", "completed"} and not new_rows:
                break
            await asyncio.sleep(1.0)

    @app.get("/status")
    async def get_status() -> dict[str, Any]:
        datasets = _discover_meta_datasets(cfg)
        metas = [_read_dataset_meta_or_404(cfg, dataset) for dataset in datasets]
        chain_head = await _resolve_meta_chain_head(cfg, metas)
        lag = max((max(0, chain_head - int(meta.get("last_block") or 0)) for meta in metas), default=0)
        active_jobs_count = 0
        for dataset in datasets:
            storage, _ = _build_dataset_storage(cfg, dataset)
            if active_writer_job(storage) is not None:
                active_jobs_count += 1
        return {
            "chain_head": chain_head,
            "lag": lag,
            "active_jobs_count": active_jobs_count,
            "datasets_count": len(datasets),
        }

    @app.get("/datasets")
    async def list_datasets(
        protocol_slug: str | None = None,
        contract_slug: str | None = None,
    ) -> list[dict[str, Any]]:
        datasets = _discover_meta_datasets(cfg)
        if protocol_slug is not None:
            datasets = [dataset for dataset in datasets if dataset.protocol == protocol_slug.strip()]
        if contract_slug is not None:
            datasets = [dataset for dataset in datasets if dataset.contract == contract_slug.strip()]
        metas = [_read_dataset_meta_or_404(cfg, dataset) for dataset in datasets]
        chain_head = await _resolve_meta_chain_head(cfg, metas)
        return [_meta_dataset_row(cfg, dataset, meta, chain_head=chain_head) for dataset, meta in zip(datasets, metas, strict=False)]

    @app.post("/datasets")
    async def create_dataset(payload: DatasetCreateRequest) -> dict[str, Any]:
        protocol = payload.protocol.strip()
        contract = payload.contract.strip()
        address = payload.contract_address.strip()
        if not protocol or not contract:
            raise HTTPException(status_code=400, detail="protocol and contract are required")
        if not _is_hex_address(address):
            raise HTTPException(status_code=400, detail="invalid contract address")

        dataset = DatasetRef(protocol=protocol, contract=contract)
        storage, location = _build_dataset_storage(cfg, dataset)
        if storage.exists(META_KEY):
            raise HTTPException(status_code=409, detail="dataset already exists")

        try:
            registry, selected_names = _build_registry_from_inputs(
                abi_path=payload.abi_path,
                abi_json=payload.abi_json,
                registry_json=payload.registry_json,
                event_names=payload.event_names,
            )
            meta = build_dataset_meta(
                protocol=dataset.protocol,
                contract=dataset.contract,
                contract_address=address,
                chain_id=payload.chain_id,
                start_block=payload.start_block,
                chunk_size=payload.chunk_size,
                step=payload.step,
                storage=payload.storage,
                rpc_url=payload.rpc_url,
                event_names=selected_names,
                registry=registry,
            )
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        try:
            created = create_dataset_meta(storage, meta)
            storage.write_text(JOBS_KEY, "")
        except FileExistsError as exc:
            raise HTTPException(status_code=409, detail="dataset already exists") from exc

        return {
            **created,
            "id": dataset.dataset_id,
            "location": location,
            "active_jobs_count": 0,
            "status": "idle",
            "lag": 0,
        }

    @app.get("/datasets/{protocol}/{contract}")
    async def get_dataset(protocol: str, contract: str) -> dict[str, Any]:
        dataset, meta, _ = _dataset_context(protocol, contract, cfg)
        chain_head = await _resolve_meta_chain_head(cfg, [meta])
        row = _meta_dataset_row(cfg, dataset, meta, chain_head=chain_head)
        row["chunks_total"] = _dataset_chunks_total(dataset, meta, cfg)
        return row

    @app.patch("/datasets/{protocol}/{contract}")
    async def patch_dataset(protocol: str, contract: str, payload: dict[str, Any]) -> dict[str, Any]:
        dataset, _, storage = _dataset_context(protocol, contract, cfg)
        normalized_payload = _normalize_dataset_patch(payload)
        updated = update_dataset_meta(storage, lambda meta: {**meta, **normalized_payload})
        chain_head = await _resolve_meta_chain_head(cfg, [updated])
        row = _meta_dataset_row(cfg, dataset, updated, chain_head=chain_head)
        row["chunks_total"] = _dataset_chunks_total(dataset, updated, cfg)
        return row

    @app.get("/datasets/{protocol}/{contract}/jobs")
    async def list_dataset_jobs_route(protocol: str, contract: str) -> list[dict[str, Any]]:
        _, _, storage = _dataset_context(protocol, contract, cfg)
        return list_dataset_jobs(storage)

    @app.post("/datasets/{protocol}/{contract}/jobs")
    async def start_dataset_job(protocol: str, contract: str, payload: DatasetJobStartRequest) -> dict[str, Any]:
        dataset, meta, _ = _dataset_context(protocol, contract, cfg)
        return await _start_dataset_job(
            dataset=dataset,
            meta=meta,
            mode=payload.mode,
            concurrency=payload.concurrency,
        )

    @app.get("/datasets/{protocol}/{contract}/jobs/{jid}")
    async def get_dataset_job_route(protocol: str, contract: str, jid: str) -> dict[str, Any]:
        _, _, storage = _dataset_context(protocol, contract, cfg)
        row = get_dataset_job(storage, jid)
        if row is None:
            raise HTTPException(status_code=404, detail="job not found")
        return row

    @app.post("/datasets/{protocol}/{contract}/jobs/{jid}/stop")
    async def stop_dataset_job(protocol: str, contract: str, jid: str) -> dict[str, Any]:
        dataset, _, _ = _dataset_context(protocol, contract, cfg)
        try:
            return await dataset_job_manager.stop(dataset=dataset, job_id=jid)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="job not found") from exc

    @app.post("/datasets/{protocol}/{contract}/jobs/{jid}/restart")
    async def restart_dataset_job(protocol: str, contract: str, jid: str) -> dict[str, Any]:
        dataset, meta, storage = _dataset_context(protocol, contract, cfg)
        previous = get_dataset_job(storage, jid)
        if previous is None:
            raise HTTPException(status_code=404, detail="job not found")
        snapshot = previous.get("config_snapshot")
        if not isinstance(snapshot, dict):
            raise HTTPException(status_code=400, detail="job has no config_snapshot")
        mode = str(snapshot.get("mode") or previous.get("mode") or "backfill")
        concurrency = int(snapshot.get("concurrency") or 16)
        return await _start_dataset_job(
            dataset=dataset,
            meta=meta,
            mode=mode,
            concurrency=concurrency,
            source_job_id=jid,
        )

    @app.get("/datasets/{protocol}/{contract}/jobs/{jid}/logs")
    async def get_dataset_job_logs(
        protocol: str,
        contract: str,
        jid: str,
        page: int = Query(default=1, ge=1),
        limit: int = Query(default=50, ge=1, le=200),
        level: str = Query(default="ALL"),
    ) -> dict[str, Any]:
        dataset, _, storage = _dataset_context(protocol, contract, cfg)
        if get_dataset_job(storage, jid) is None:
            raise HTTPException(status_code=404, detail="job not found")
        return await _list_job_logs(dataset=dataset, job_id=jid, page=page, limit=limit, level=level)

    @app.get("/datasets/{protocol}/{contract}/jobs/{jid}/logs/stream")
    async def stream_dataset_job_logs(protocol: str, contract: str, jid: str) -> StreamingResponse:
        dataset, _, storage = _dataset_context(protocol, contract, cfg)
        if get_dataset_job(storage, jid) is None:
            raise HTTPException(status_code=404, detail="job not found")
        return StreamingResponse(
            _job_log_stream(dataset, jid),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "Connection": "keep-alive"},
        )

    @app.get("/datasets/{protocol}/{contract}/coverage")
    async def get_dataset_coverage(protocol: str, contract: str) -> dict[str, Any]:
        dataset, meta, storage = _dataset_context(protocol, contract, cfg)
        event_names = meta.get("event_names")
        if not isinstance(event_names, list) or not event_names:
            raise HTTPException(status_code=400, detail="dataset meta is missing event_names")
        start_block = int(meta.get("start_block") or 0)
        end_block = int(meta.get("last_block") or start_block)
        report = validate_coverage(
            storage=storage,
            event_names=[str(item) for item in event_names],
            start_block=start_block,
            end_block=end_block,
        )
        detected_at = _to_iso_z(int(time.time()))
        gaps = [
            {
                "range_start": gap_start,
                "range_end": gap_end,
                "missing_blocks": max(0, gap_end - gap_start + 1),
                "detected_at": detected_at,
            }
            for gap_start, gap_end in report.missing_in_range
        ]
        return {
            "complete": len(gaps) == 0,
            "gaps": gaps,
        }

    @app.post("/indexer/abi/etherscan")
    async def post_indexer_abi_etherscan(payload: EtherscanAbiRequest) -> dict[str, Any]:
        address = payload.address.strip()
        if not _is_hex_address(address):
            raise HTTPException(status_code=400, detail="invalid contract address")

        endpoint_url = _normalize_etherscan_endpoint(
            _clean_optional_str(payload.endpoint_url) or cfg.etherscan_api_url
        )
        api_key = _clean_optional_str(payload.api_key) or _clean_optional_str(cfg.etherscan_api_key)
        chain_id = payload.chain_id if payload.chain_id is not None else cfg.etherscan_chain_id

        try:
            abi_json = await _fetch_etherscan_abi(
                endpoint_url=endpoint_url,
                address=address,
                chain_id=chain_id,
                api_key=api_key,
            )
            events = get_events_from_abi(abi_json)
        except httpx.HTTPStatusError as exc:
            raise HTTPException(status_code=502, detail=f"etherscan http error: {exc.response.status_code}") from exc
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        if not events:
            raise HTTPException(status_code=400, detail="abi has no events")

        await _record_event(
            event_type="abi_fetched",
            payload={
                "address": address,
                "chainId": chain_id,
                "source": "etherscan",
                "eventCount": len(events),
            },
        )
        return {
            "address": address,
            "source": "etherscan",
            "chainId": chain_id,
            "abiJson": abi_json,
            "events": _event_descriptors(events),
        }

    return app
