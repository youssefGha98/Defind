from __future__ import annotations

import json
from typing import Any

import httpx

from defind.clients.rpc import RPC


async def fetch_etherscan_chain_head(
    *,
    endpoint_url: str,
    chain_id: int,
    api_key: str | None,
) -> int:
    params: dict[str, Any] = {
        "chainid": chain_id,
        "module": "proxy",
        "action": "eth_blockNumber",
    }
    if api_key:
        params["apikey"] = api_key

    async with httpx.AsyncClient(timeout=20.0) as client:
        response = await client.get(endpoint_url, params=params)
        response.raise_for_status()
        payload = response.json()

    if not isinstance(payload, dict):
        raise ValueError("etherscan chain head response is not a JSON object")
    result = payload.get("result")
    if not isinstance(result, str):
        raise ValueError("etherscan chain head response is missing result")
    try:
        return int(result, 16)
    except ValueError as exc:
        raise ValueError(f"invalid etherscan chain head result: {result}") from exc


async def fetch_rpc_chain_head(*, rpc_url: str) -> int:
    rpc = RPC(rpc_url)
    try:
        return int(await rpc.latest_block())
    finally:
        await rpc.aclose()


async def fetch_etherscan_abi(
    *,
    endpoint_url: str,
    address: str,
    chain_id: int | None,
    api_key: str | None,
) -> list[dict[str, Any]]:
    params: dict[str, Any] = {
        "module": "contract",
        "action": "getabi",
        "address": address,
    }
    if chain_id is not None:
        params["chainid"] = chain_id
    if api_key:
        params["apikey"] = api_key

    async with httpx.AsyncClient(timeout=20.0) as client:
        response = await client.get(endpoint_url, params=params)
        response.raise_for_status()
        payload = response.json()

    if not isinstance(payload, dict):
        raise ValueError("etherscan response is not a JSON object")

    status = payload.get("status")
    message = str(payload.get("message") or "").strip()
    result = payload.get("result")

    if isinstance(result, list):
        return [row for row in result if isinstance(row, dict)]

    if isinstance(result, str):
        stripped = result.strip()
        if "deprecated v1 endpoint" in stripped.lower():
            raise ValueError("deprecated etherscan v1 endpoint; use https://api.etherscan.io/v2/api")
        if stripped.startswith("[") and stripped.endswith("]"):
            decoded = json.loads(stripped)
            if not isinstance(decoded, list):
                raise ValueError("etherscan ABI result is not a JSON array")
            return [row for row in decoded if isinstance(row, dict)]
        if str(status) != "1":
            raise ValueError(stripped or f"etherscan error: {message or 'unknown'}")

    if str(status) != "1":
        raise ValueError(str(result) if result is not None else (message or "etherscan error"))

    raise ValueError("etherscan ABI payload is empty or invalid")
