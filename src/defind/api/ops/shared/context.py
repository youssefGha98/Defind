from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated, Any, cast

from fastapi import Depends, Request

from defind.api.ops.shared.models import OpsApiConfig


@dataclass(slots=True)
class OpsApiServices:
    cfg: OpsApiConfig
    dataset_controller: Any
    jobs_controller: Any
    logs_controller: Any
    abi_controller: Any
    status_controller: Any


async def get_services(request: Request) -> OpsApiServices:
    return cast(OpsApiServices, request.app.state.ops_services)


ServicesDep = Annotated[OpsApiServices, Depends(get_services)]
