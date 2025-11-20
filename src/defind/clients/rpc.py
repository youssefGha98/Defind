"""Lightweight JSON-RPC client for Ethereum-compatible nodes.

This module provides:
- `RPC`: an async client with sane timeouts/connection limits
- Helper utilities to format block numbers and topics

It returns `EventLog` records ready for downstream decoding.
"""

from __future__ import annotations

from collections.abc import Sequence

import httpx

from defind.core.models import EventLog


def to_hex_block(x: int) -> str:
    """Return a 0x-prefixed hex block number."""
    return hex(x)


def topics_param(topic0s: Sequence[str]) -> list[list[str]]:
    """Format topic0 signatures for eth_getLogs RPC call."""
    return [[t.lower() for t in topic0s]]


class RPC:
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

    def __init__(self, url: str, *, timeout_s: int = 20, max_connections: int = 64) -> None:
        self.url = url
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

    async def latest_block(self) -> int:
        """Return the latest block number as an int."""
        payload = {"jsonrpc": "2.0", "id": 1, "method": "eth_blockNumber", "params": []}
        r = await self.client.post(self.url, json=payload)
        r.raise_for_status()
        return int(r.json()["result"], 16)

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
        r = await self.client.post(
            self.url,
            json={"jsonrpc": "2.0", "id": 1, "method": "eth_getLogs", "params": params},
        )
        r.raise_for_status()
        data = r.json()
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
