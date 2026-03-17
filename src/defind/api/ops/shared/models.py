from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from defind.api.ops.shared.utils import DEFAULT_CORS_ORIGINS, ETHERSCAN_API_URL


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
    log_level: str = "INFO"
    log_json: bool = True
    json_log_file_path: Path | None = None
    log_service_name: str = "defind-api"

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
    cors_origins: tuple[str, ...] = DEFAULT_CORS_ORIGINS
    etherscan_api_url: str = ETHERSCAN_API_URL
    etherscan_api_key: str | None = None
    etherscan_chain_id: int = 1
    event_history_prefix: str = "_meta/event_history/"
    event_history_limit: int = 10_000
    logs_backend: str = "event_store"
    loki_url: str | None = None
    loki_timeout_s: float = 5.0
    loki_lookback_s: int = 2_592_000
