from __future__ import annotations

from fastapi import APIRouter

from defind.api.ops.jobs.models import (
    JobDeleteReadModel,
    JobGetReadModel,
    JobListReadModel,
    JobRestartPostModel,
    JobRunReadModel,
    JobStartPostModel,
)
from defind.api.ops.shared.context import ServicesDep

router = APIRouter()


@router.get("/datasets/{protocol}/{contract}/jobs", response_model=list[JobListReadModel])
async def list_dataset_jobs(
    protocol: str,
    contract: str,
    services: ServicesDep,
) -> list[JobListReadModel]:
    return await services.jobs_controller.list_jobs(protocol, contract)


@router.post("/datasets/{protocol}/{contract}/jobs", response_model=JobRunReadModel)
async def start_dataset_job(
    protocol: str,
    contract: str,
    payload: JobStartPostModel,
    services: ServicesDep,
) -> JobRunReadModel:
    return await services.jobs_controller.start_job(protocol, contract, payload)


@router.get("/datasets/{protocol}/{contract}/jobs/{jid}", response_model=JobGetReadModel)
async def get_dataset_job(
    protocol: str,
    contract: str,
    jid: str,
    services: ServicesDep,
) -> JobGetReadModel:
    return await services.jobs_controller.get_job(protocol, contract, jid)


@router.post("/datasets/{protocol}/{contract}/jobs/{jid}/stop", response_model=JobRunReadModel)
async def stop_dataset_job(
    protocol: str,
    contract: str,
    jid: str,
    services: ServicesDep,
) -> JobRunReadModel:
    return await services.jobs_controller.stop_job(protocol, contract, jid)


@router.post("/datasets/{protocol}/{contract}/jobs/{jid}/restart", response_model=JobRunReadModel)
async def restart_dataset_job(
    protocol: str,
    contract: str,
    jid: str,
    services: ServicesDep,
    payload: JobRestartPostModel | None = None,
) -> JobRunReadModel:
    return await services.jobs_controller.restart_job(protocol, contract, jid, payload)


@router.delete("/datasets/{protocol}/{contract}/jobs/{jid}", response_model=JobDeleteReadModel)
async def delete_dataset_job(
    protocol: str,
    contract: str,
    jid: str,
    services: ServicesDep,
) -> JobDeleteReadModel:
    return await services.jobs_controller.delete_job(protocol, contract, jid)
