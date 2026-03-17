from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class DatasetCreatePostModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

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


class DatasetUpdatePatchModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    protocol: str | None = None
    contract: str | None = None
    start_block: int | None = Field(default=None, ge=0)
    contract_address: str | None = None
    chunk_size: int | None = Field(default=None, ge=1)
    step: int | None = Field(default=None, ge=1)
    storage: str | None = None
    rpc_url: str | None = None
    event_names: list[str] | None = None
    registry_json: dict[str, Any] | None = None
    last_block: int | None = Field(default=None, ge=0)
    timeout_s: int | None = Field(default=None, ge=1)
    rpc_max_retries: int | None = Field(default=None, ge=0)
    rpc_retry_backoff_s: float | None = Field(default=None, ge=0.0)


class DatasetReadModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    protocol: str
    contract: str
    contract_address: str
    chain_id: int
    start_block: int
    last_block: int
    chain_head: int
    chunks_total: int
    lag: int
    active_jobs_count: int
    status: str


class DatasetCoverageGapReadModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    range_start: int
    range_end: int
    missing_blocks: int
    detected_at: str


class DatasetCoverageReadModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    complete: bool
    gaps: list[DatasetCoverageGapReadModel]
    invalid_chunks: dict[str, list[str]]
