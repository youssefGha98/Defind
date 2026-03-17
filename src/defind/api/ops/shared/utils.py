from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

from dotenv import find_dotenv, load_dotenv

from defind.core.config import OrchestratorConfig

if TYPE_CHECKING:
    from defind.api.ops.shared.models import OpsApiConfig

ETHERSCAN_API_URL = "https://api.etherscan.io/v2/api"
ADDRESS_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")
PUBLIC_CHAIN_HEAD_RPCS: dict[int, tuple[str, ...]] = {
    8453: (
        "https://mainnet.base.org",
        "https://base-rpc.publicnode.com",
    ),
}
DEFAULT_CORS_ORIGINS = (
    "http://localhost:3000",
    "http://127.0.0.1:3000",
    "http://localhost:3001",
    "http://127.0.0.1:3001",
    "http://localhost:5173",
    "http://127.0.0.1:5173",
)
ORCHESTRATOR_TIMEOUT_DEFAULT_S = int(OrchestratorConfig.__dataclass_fields__["timeout_s"].default)
ORCHESTRATOR_RPC_MAX_RETRIES_DEFAULT = int(
    OrchestratorConfig.__dataclass_fields__["rpc_max_retries"].default
)
ORCHESTRATOR_RPC_RETRY_BACKOFF_DEFAULT_S = float(
    OrchestratorConfig.__dataclass_fields__["rpc_retry_backoff_s"].default
)


def parse_int(value: str | None, *, default: int, min_value: int = 0) -> int:
    if value is None or value == "":
        return default
    return max(min_value, int(value))


def parse_float(value: str | None, *, default: float, min_value: float = 0.0) -> float:
    if value is None or value == "":
        return default
    return max(min_value, float(value))


def parse_bool(value: str | None, *, default: bool) -> bool:
    if value is None or value == "":
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def parse_cors_origins(raw: str | None) -> tuple[str, ...]:
    if raw is None or raw.strip() == "":
        return DEFAULT_CORS_ORIGINS
    if raw.strip() == "*":
        return ("*",)
    out = tuple(item.strip() for item in raw.split(",") if item.strip())
    return out or DEFAULT_CORS_ORIGINS


def clean_optional_str(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    return stripped if stripped else None


def meta_int_value(meta: dict[str, Any], key: str, *, default: int, min_value: int = 0) -> int:
    raw = meta.get(key)
    if raw is None or raw == "":
        return default
    return max(min_value, int(raw))


def meta_float_value(meta: dict[str, Any], key: str, *, default: float, min_value: float = 0.0) -> float:
    raw = meta.get(key)
    if raw is None or raw == "":
        return default
    return max(min_value, float(raw))


def to_iso_z(ts_unix_s: int) -> str:
    return datetime.fromtimestamp(ts_unix_s, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def normalize_collection_prefix(raw: str | None, *, fallback: str) -> str:
    cleaned = (raw or fallback).strip().strip("/")
    if not cleaned:
        cleaned = fallback.strip("/")
    if cleaned.endswith(".json"):
        cleaned = cleaned.removesuffix(".json")
    return f"{cleaned.rstrip('/')}/"


def normalize_etherscan_endpoint(endpoint_url: str) -> str:
    cleaned = endpoint_url.strip().rstrip("/")
    if cleaned.endswith("/v2/api"):
        return cleaned
    if cleaned.endswith("/api"):
        return f"{cleaned[:-4]}/v2/api"
    if "/v2/" in cleaned:
        return cleaned
    return f"{cleaned}/v2/api"


def is_hex_address(value: str) -> bool:
    return bool(ADDRESS_RE.match(value))


def service_unavailable_detail(exc: BaseException) -> dict[str, Any]:
    return {
        "message": "storage backend unavailable",
        "error": str(exc),
    }


def exception_detail(exc: BaseException) -> str:
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


def dumps_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, separators=(",", ":"), sort_keys=True, default=str)


def load_ops_api_config_from_env() -> OpsApiConfig:
    from defind.api.ops.shared.models import OpsApiConfig

    load_dotenv(find_dotenv(usecwd=True))
    json_log_file = clean_optional_str(os.getenv("DEFIND_API_JSON_LOG_FILE"))
    return OpsApiConfig(
        out_root=Path(os.getenv("DEFIND_API_OUT_ROOT", "./data")),
        default_chunk_size=parse_int(
            os.getenv("DEFIND_API_DEFAULT_CHUNK_SIZE"),
            default=200_000,
            min_value=1,
        ),
        log_level=os.getenv("DEFIND_API_LOG_LEVEL", "INFO"),
        log_json=parse_bool(os.getenv("DEFIND_API_LOG_JSON"), default=True),
        json_log_file_path=Path(json_log_file) if json_log_file is not None else None,
        log_service_name=os.getenv("DEFIND_API_LOG_SERVICE_NAME", "defind-api"),
        s3_bucket=os.getenv("S3_BUCKET") or os.getenv("DEFIND_API_S3_BUCKET"),
        s3_prefix=(os.getenv("S3_PREFIX") or os.getenv("DEFIND_API_S3_PREFIX", "") or ""),
        s3_endpoint_url=os.getenv("S3_ENDPOINT_URL") or os.getenv("DEFIND_API_S3_ENDPOINT_URL"),
        s3_access_key=os.getenv("S3_ACCESS_KEY") or os.getenv("DEFIND_API_S3_ACCESS_KEY"),
        s3_secret_key=os.getenv("S3_SECRET_KEY") or os.getenv("DEFIND_API_S3_SECRET_KEY"),
        s3_region=(os.getenv("S3_REGION") or os.getenv("DEFIND_API_S3_REGION", "auto") or "auto"),
        s3_max_retries=parse_int(os.getenv("DEFIND_API_S3_MAX_RETRIES"), default=3),
        s3_retry_backoff_s=parse_float(os.getenv("DEFIND_API_S3_RETRY_BACKOFF_S"), default=0.5),
        host=os.getenv("DEFIND_API_HOST", "0.0.0.0"),
        port=parse_int(os.getenv("DEFIND_API_PORT"), default=8000, min_value=1),
        cors_origins=parse_cors_origins(os.getenv("DEFIND_API_CORS_ORIGINS")),
        etherscan_api_url=os.getenv("DEFIND_API_ETHERSCAN_API_URL", ETHERSCAN_API_URL),
        etherscan_api_key=os.getenv("ETHERSCAN_API_KEY") or os.getenv("DEFIND_API_ETHERSCAN_API_KEY"),
        etherscan_chain_id=parse_int(
            os.getenv("DEFIND_API_ETHERSCAN_CHAIN_ID"),
            default=1,
            min_value=1,
        ),
        event_history_prefix=(
            os.getenv("DEFIND_API_EVENT_HISTORY_PREFIX")
            or os.getenv("DEFIND_API_HISTORY_KEY")
            or "_meta/event_history/"
        ),
        event_history_limit=parse_int(
            os.getenv("DEFIND_API_EVENT_HISTORY_LIMIT") or os.getenv("DEFIND_API_HISTORY_MAX_EVENTS"),
            default=10_000,
            min_value=100,
        ),
        logs_backend=(clean_optional_str(os.getenv("DEFIND_API_LOGS_BACKEND")) or "event_store").lower(),
        loki_url=clean_optional_str(os.getenv("DEFIND_API_LOKI_URL")),
        loki_timeout_s=parse_float(os.getenv("DEFIND_API_LOKI_TIMEOUT_S"), default=5.0, min_value=0.1),
        loki_lookback_s=parse_int(
            os.getenv("DEFIND_API_LOKI_LOOKBACK_S"),
            default=2_592_000,
            min_value=60,
        ),
    )
