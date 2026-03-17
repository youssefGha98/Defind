from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class StatusReadModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    chain_head: int
    lag: int
    active_jobs_count: int
    datasets_count: int


class HealthReadModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: str


class ReadyReadModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: str
