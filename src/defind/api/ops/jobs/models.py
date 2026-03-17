from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class JobStartPostModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mode: Literal["backfill", "listen", "both"]
    concurrency: int = Field(default=16, ge=1)


class JobRestartPostModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mode: Literal["backfill", "listen", "both"] | None = None
    concurrency: int | None = Field(default=None, ge=1)


class JobListReadModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    job_id: str
    mode: str
    status: str
    resume_from: int
    chunks_written: int
    origin: str
    started_at: str
    ended_at: str | None = None
    error: str | None = None


class JobGetReadModel(JobListReadModel):
    model_config = ConfigDict(extra="forbid")


class JobRunReadModel(JobListReadModel):
    model_config = ConfigDict(extra="forbid")

    config_snapshot: dict[str, Any]


class JobDeleteReadModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    job: JobRunReadModel
    deleted_log_events: int
    log_purge_scheduled: bool
