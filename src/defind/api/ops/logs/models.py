from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class JobLogReadModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: int
    tsUnixS: int
    ts: str
    eventType: str
    datasetId: str | None = None
    jobId: str | None = None
    runId: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)


class JobLogsPageReadModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    page: int
    limit: int
    items: list[JobLogReadModel]
