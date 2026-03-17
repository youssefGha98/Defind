from __future__ import annotations

from typing import Any

from defind.api.ops.shared.dependencies import OpsApiDependencies


class AbiRepository:
    def __init__(self, *, deps: OpsApiDependencies) -> None:
        self._deps = deps

    async def fetch_etherscan_abi(
        self,
        *,
        endpoint_url: str,
        address: str,
        chain_id: int | None,
        api_key: str | None,
    ) -> list[dict[str, Any]]:
        return await self._deps.fetch_etherscan_abi(
            endpoint_url=endpoint_url,
            address=address,
            chain_id=chain_id,
            api_key=api_key,
        )
