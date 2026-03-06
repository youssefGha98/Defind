"""Lightweight JSON-RPC client for Ethereum-compatible nodes.

This module provides:
- `RPC`: an async client with sane timeouts/connection limits
- Helper utilities to format block numbers and topics

It returns `EventLog` records ready for downstream decoding.
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import Any, cast

import httpx

from defind.core.interfaces import IEvmLogsProvider
from defind.core.models import EventLog

JsonDict = dict[str, Any]


def to_hex_block(x: int) -> str:
    """Return a 0x-prefixed hex block number."""
    return hex(x)


def topics_param(topic0s: Sequence[str]) -> list[list[str]]:
    """Format topic0 signatures for eth_getLogs RPC call."""
    return [[t.lower() for t in topic0s]]


class RPC(IEvmLogsProvider):
    """Minimal async RPC client.

    Parameters
    ----------
    url : str
        RPC endpoint URL.
    timeout_s : int
        Per-operation timeout in seconds (connect/read/write).
    max_connections : int
        Maximum concurrent connections to keep in the pool.
    """

    def __init__(
        self,
        url: str,
        *,
        timeout_s: int = 20,
        max_connections: int = 64,
        max_retries: int = 3,
        retry_backoff_s: float = 0.5,
    ) -> None:
        self.url = url
        self.max_retries = max(0, int(max_retries))
        self.retry_backoff_s = max(0.0, float(retry_backoff_s))
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(
                connect=timeout_s,
                read=timeout_s,
                write=timeout_s,
                pool=max(30, timeout_s * 3),
            ),
            limits=httpx.Limits(
                max_connections=max_connections,
                max_keepalive_connections=max(1, max_connections // 2),
            ),
            http2=True,
        )

    async def _post_json(self, payload: JsonDict) -> JsonDict:
        delay = self.retry_backoff_s
        attempt = 0
        while True:
            try:
                r = await self.client.post(self.url, json=payload)
                r.raise_for_status()
                return cast(JsonDict, r.json())
            except httpx.HTTPStatusError as e:
                status = e.response.status_code
                retryable = status in (408, 425, 429, 500, 502, 503, 504)
                if (not retryable) or attempt >= self.max_retries:
                    raise
            except (httpx.TransportError, httpx.TimeoutException):
                if attempt >= self.max_retries:
                    raise

            if delay > 0:
                await asyncio.sleep(delay)
                delay = min(delay * 2, 8.0)
            attempt += 1

    async def latest_block(self) -> int:
        """Return the latest block number as an int."""
        payload = {"jsonrpc": "2.0", "id": 1, "method": "eth_blockNumber", "params": []}
        data = await self._post_json(payload)
        if "error" in data:
            e = data["error"]
            raise RuntimeError(f"RPC error: {e.get('code')} {e.get('message')}")
        return int(data["result"], 16)

    async def get_logs(
        self,
        *,
        address: str,
        topic0s: Sequence[str],
        from_block: int,
        to_block: int,
    ) -> list[EventLog]:
        """Fetch logs for an address and a set of topic0 signatures within a block range."""
        params = [
            {
                "address": address.lower(),
                "fromBlock": to_hex_block(from_block),
                "toBlock": to_hex_block(to_block),
                "topics": topics_param(topic0s),
            }
        ]
        data = await self._post_json(
            {"jsonrpc": "2.0", "id": 1, "method": "eth_getLogs", "params": params},
        )
        if "error" in data:
            e = data["error"]
            raise RuntimeError(f"RPC error: {e.get('code')} {e.get('message')}")

        out: list[EventLog] = []
        for rl in data.get("result", []):
            topics = tuple((t if isinstance(t, str) else t.decode()).lower() for t in rl.get("topics", []))
            ts = rl.get("blockTimestamp")
            ts_i = (
                int(ts, 16)
                if isinstance(ts, str) and ts.startswith("0x")
                else (int(ts) if isinstance(ts, int) else None)
            )
            out.append(
                EventLog(
                    address=rl["address"].lower(),
                    topics=topics,
                    data_hex=str(rl.get("data") or "0x"),
                    block_number=int(rl["blockNumber"], 16),
                    tx_hash=(rl.get("transactionHash") or rl.get("transaction_hash") or "").lower(),
                    log_index=int(rl["logIndex"], 16),
                    block_timestamp=ts_i,
                )
            )
        return out

    async def aclose(self) -> None:
        """Close the underlying HTTP client."""
        await self.client.aclose()
