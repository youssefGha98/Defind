from __future__ import annotations

from fastapi import APIRouter

from defind.api.ops.dataset.models import (
    DatasetCoverageReadModel,
    DatasetCreatePostModel,
    DatasetReadModel,
    DatasetUpdatePatchModel,
)
from defind.api.ops.shared.context import ServicesDep

router = APIRouter()


@router.get("/datasets", response_model=list[DatasetReadModel])
async def list_datasets(
    services: ServicesDep,
    protocol_slug: str | None = None,
    contract_slug: str | None = None,
) -> list[DatasetReadModel]:
    return await services.dataset_controller.list_datasets(
        protocol_slug=protocol_slug,
        contract_slug=contract_slug,
    )


@router.post("/datasets", response_model=DatasetReadModel)
async def create_dataset(
    payload: DatasetCreatePostModel,
    services: ServicesDep,
) -> DatasetReadModel:
    return await services.dataset_controller.create_dataset(payload)


@router.get("/datasets/{protocol}/{contract}", response_model=DatasetReadModel)
async def get_dataset(
    protocol: str,
    contract: str,
    services: ServicesDep,
) -> DatasetReadModel:
    return await services.dataset_controller.get_dataset(protocol, contract)


@router.patch("/datasets/{protocol}/{contract}", response_model=DatasetReadModel)
async def patch_dataset(
    protocol: str,
    contract: str,
    payload: DatasetUpdatePatchModel,
    services: ServicesDep,
) -> DatasetReadModel:
    return await services.dataset_controller.patch_dataset(protocol, contract, payload)


@router.get("/datasets/{protocol}/{contract}/coverage", response_model=DatasetCoverageReadModel)
async def get_dataset_coverage(
    protocol: str,
    contract: str,
    services: ServicesDep,
) -> DatasetCoverageReadModel:
    return await services.dataset_controller.get_coverage(protocol, contract)
