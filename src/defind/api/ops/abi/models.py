from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class EtherscanAbiFetchPostModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    address: str
    chain_id: int | None = Field(default=None, ge=1)
    api_key: str | None = None
    endpoint_url: str | None = None


class AbiEventInputReadModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    type: str
    indexed: bool


class AbiEventReadModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    signature: str
    topic0: str
    indexedInputs: int
    nonIndexedInputs: int
    inputs: list[AbiEventInputReadModel]


class EtherscanAbiReadModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    address: str
    source: str
    chainId: int
    abiJson: list[dict[str, Any]]
    events: list[AbiEventReadModel]
