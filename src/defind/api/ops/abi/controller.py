from __future__ import annotations

import httpx
from fastapi import HTTPException

from defind.abi_events import get_events_from_abi
from defind.api.ops.abi.models import EtherscanAbiFetchPostModel, EtherscanAbiReadModel
from defind.api.ops.abi.repository import AbiRepository
from defind.api.ops.abi.utils import event_descriptors
from defind.api.ops.logs.repository import LogsRepository
from defind.api.ops.shared.models import OpsApiConfig
from defind.api.ops.shared.utils import (
    clean_optional_str,
    is_hex_address,
    normalize_etherscan_endpoint,
)
from defind.observability import get_logger

logger = get_logger(__name__)


class AbiController:
    def __init__(
        self,
        *,
        cfg: OpsApiConfig,
        repository: AbiRepository,
        logs_repository: LogsRepository,
    ) -> None:
        self._cfg = cfg
        self._repository = repository
        self._logs_repository = logs_repository

    async def fetch_etherscan_abi(self, payload: EtherscanAbiFetchPostModel) -> EtherscanAbiReadModel:
        address = payload.address.strip()
        if not is_hex_address(address):
            raise HTTPException(status_code=400, detail="invalid contract address")

        endpoint_url = normalize_etherscan_endpoint(
            clean_optional_str(payload.endpoint_url) or self._cfg.etherscan_api_url
        )
        api_key = clean_optional_str(payload.api_key) or clean_optional_str(self._cfg.etherscan_api_key)
        chain_id = payload.chain_id if payload.chain_id is not None else self._cfg.etherscan_chain_id

        try:
            abi_json = await self._repository.fetch_etherscan_abi(
                endpoint_url=endpoint_url,
                address=address,
                chain_id=chain_id,
                api_key=api_key,
            )
            events = get_events_from_abi(abi_json)
        except httpx.HTTPStatusError as exc:
            raise HTTPException(status_code=502, detail=f"etherscan http error: {exc.response.status_code}") from exc
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        if not events:
            raise HTTPException(status_code=400, detail="abi has no events")

        try:
            await self._logs_repository.record_event(
                event_type="abi_fetched",
                payload={
                    "address": address,
                    "chainId": chain_id,
                    "source": "etherscan",
                    "eventCount": len(events),
                },
            )
        except Exception:
            logger.warning(
                "abi_fetch_event_persist_failed",
                extra={"address": address, "chain_id": chain_id},
                exc_info=True,
            )

        return EtherscanAbiReadModel(
            address=address,
            source="etherscan",
            chainId=chain_id,
            abiJson=abi_json,
            events=event_descriptors(events),
        )
