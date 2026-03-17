from __future__ import annotations

from fastapi import APIRouter

from defind.api.ops.abi.models import EtherscanAbiFetchPostModel, EtherscanAbiReadModel
from defind.api.ops.shared.context import ServicesDep

router = APIRouter()


@router.post("/indexer/abi/etherscan", response_model=EtherscanAbiReadModel)
async def post_indexer_abi_etherscan(
    payload: EtherscanAbiFetchPostModel,
    services: ServicesDep,
) -> EtherscanAbiReadModel:
    return await services.abi_controller.fetch_etherscan_abi(payload)
