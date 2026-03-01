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
    step: int = 5_000              # taille des appels RPC (blocs par requête)
    chunk_size: int | None = None  # taille du fichier de sortie (blocs par Parquet)
                                   # None → même valeur que step (1 fichier par step)
    concurrency: int = 16
    timeout_s: int = 20

    # Local storage (used when s3_bucket is None)
    out_root: Path = Path("./data")

    # S3 storage (optional — overrides local if s3_bucket is set)
    s3_bucket: str | None = None
    s3_prefix: str = ""
    s3_endpoint_url: str | None = None
    s3_access_key: str | None = None
    s3_secret_key: str | None = None
    s3_region: str = "auto"


@dataclass(frozen=True)
class RawFetchConfig:
    """Configuration for the raw log fetcher (CLI)."""

    rpc_url: str
    contract: str
    events: list[str]
    from_block: int
    to_block: int
    step: int = 1_000
    concurrency: int = 16
    jsonl_out: str = ""
    manifest_path: str = ""
    rerun_failed: bool = False
