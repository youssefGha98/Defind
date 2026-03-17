from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class OrchestratorConfig:
    """Configuration for the streaming orchestrator."""

    # Required
    rpc_url: str
    address: str
    topic0s: list[str]
    start_block: int | str
    end_block: int | str
    protocol_slug: str
    contract_slug: str

    # Fetch tuning
    step: int = 5_000  # taille des appels RPC (blocs par requête)
    chunk_size: int | None = None  # taille du fichier de sortie (blocs par Parquet)
    # None → même valeur que step (1 fichier par step)
    concurrency: int = 16
    timeout_s: int = 90
    rpc_max_retries: int = 5
    rpc_retry_backoff_s: float = 1
    codec: str = "lz4"  # compression Parquet ("lz4", "zstd", "snappy", "none")
    listen: bool = False  # continue polling after backfill
    listen_poll_interval_s: float = 2.0
    reorg_lookback_blocks: int = 0  # in listen mode, reprocess the last N blocks when a new block arrives
    print_chunk_writes: bool = False  # print each written chunk key/interval
    heartbeat_interval_s: float = 0.0  # 0 disables heartbeat writer/logs
    lag_warn_threshold_blocks: int = 0  # 0 disables lag warning threshold
    heartbeat_key: str = "_meta/heartbeat.json"
    single_writer_guard: bool = False
    writer_lock_key: str = "_meta/writer.lock.json"
    writer_lock_ttl_s: int = 120
    writer_lock_refresh_s: float = 30.0
    log_level: str = "INFO"
    log_json: bool = True

    # Local storage (used when s3_bucket is None)
    out_root: Path = Path("./data")

    # S3 storage (optional — overrides local if s3_bucket is set)
    s3_bucket: str | None = None
    s3_prefix: str = ""
    s3_endpoint_url: str | None = None
    s3_access_key: str | None = None
    s3_secret_key: str | None = None
    s3_region: str = "auto"
    s3_max_retries: int = 3
    s3_retry_backoff_s: float = 0.5
