from __future__ import annotations

from fastapi import APIRouter

from defind.api.ops.shared.context import ServicesDep
from defind.api.ops.status.models import HealthReadModel, ReadyReadModel, StatusReadModel

router = APIRouter()


@router.get("/status", response_model=StatusReadModel)
async def get_status(services: ServicesDep) -> StatusReadModel:
    return await services.status_controller.get_status()


@router.get("/health", response_model=HealthReadModel)
async def get_health(services: ServicesDep) -> HealthReadModel:
    return await services.status_controller.get_health()


@router.get("/ready", response_model=ReadyReadModel)
async def get_ready(services: ServicesDep) -> ReadyReadModel:
    return await services.status_controller.get_ready()
