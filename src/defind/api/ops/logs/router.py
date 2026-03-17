from __future__ import annotations

from fastapi import APIRouter, Query
from fastapi.responses import StreamingResponse

from defind.api.ops.logs.models import JobLogsPageReadModel
from defind.api.ops.shared.context import ServicesDep

router = APIRouter()


@router.get("/datasets/{protocol}/{contract}/jobs/{jid}/logs", response_model=JobLogsPageReadModel)
async def get_dataset_job_logs(
    protocol: str,
    contract: str,
    jid: str,
    services: ServicesDep,
    page: int = Query(default=1, ge=1),
    limit: int = Query(default=50, ge=1, le=200),
    level: str = Query(default="ALL"),
) -> JobLogsPageReadModel:
    return await services.logs_controller.get_job_logs(
        protocol,
        contract,
        jid,
        page=page,
        limit=limit,
        level=level,
    )


@router.get("/datasets/{protocol}/{contract}/jobs/{jid}/logs/stream")
async def stream_dataset_job_logs(
    protocol: str,
    contract: str,
    jid: str,
    services: ServicesDep,
) -> StreamingResponse:
    return await services.logs_controller.stream_job_logs(protocol, contract, jid)
