"""Lightweight JSON-RPC client for Ethereum-compatible nodes.

This module provides:
- `RPC`: an async client with sane timeouts/connection limits
- Helper utilities to format block numbers and topics

It returns `EventLog` records ready for downstream decoding.
"""

from __future__ import annotations

import asyncio
from typing import Sequence
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
        self._max_retries = 5  # Default retry count
        self._base_delay = 1.0  # Default base delay
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

    async def _make_request_with_retry(self, payload: dict) -> dict:
        """Make RPC request with retry + provider-specified backoff for 429."""
        for attempt in range(self._max_retries + 1):
            try:
                r = await self.client.post(self.url, json=payload)
            except (httpx.ConnectError, httpx.TimeoutException) as e:
                # Network issues: exponential backoff
                if attempt == self._max_retries:
                    raise

                delay = self._base_delay * (2 ** attempt)
                print(
                    f"⚠️  Connection error {e!r}, retrying in {delay}s... "
                    f"(attempt {attempt + 1}/{self._max_retries + 1})"
                )
                await asyncio.sleep(delay)
                continue

            # We got an HTTP response here
            # Try to parse JSON (JSON-RPC)
            try:
                data = r.json()
            except ValueError:
                # Not JSON? Just raise if it's an error, otherwise return raw text
                try:
                    r.raise_for_status()
                except httpx.HTTPStatusError:
                    if attempt == self._max_retries:
                        raise
                    delay = self._base_delay * (2 ** attempt)
                    print(
                        f"⚠️  HTTP error {r.status_code}, retrying in {delay}s... "
                        f"(attempt {attempt + 1}/{self._max_retries + 1})"
                    )
                    await asyncio.sleep(delay)
                    continue
                return r.text

            # Check for JSON-RPC error block
            rpc_error = data.get("error")

            # Detect rate limiting:
            #  - HTTP 429, OR
            #  - JSON-RPC error.code == 429 (some providers use HTTP 200 + JSON error)
            is_rate_limited = (
                r.status_code == 429
                or (rpc_error is not None and rpc_error.get("code") == 429)
            )

            if is_rate_limited:
                if attempt == self._max_retries:
                    raise RuntimeError(f"Rate limited and max retries exceeded: {data}")

                # Try to respect provider's backoff_seconds
                backoff = 5.0
                if rpc_error:
                    rpc_data = rpc_error.get("data", {})
                    try:
                        backoff = float(rpc_data.get("backoff_seconds", backoff))
                    except (TypeError, ValueError):
                        pass

                # Optional: enforce a minimum to avoid hammering anyway
                backoff = max(backoff, 30.0)

                print(
                    f"⚠️  Rate limited (429), retrying in {backoff}s... "
                    f"(attempt {attempt + 1}/{self._max_retries + 1})"
                )
                await asyncio.sleep(backoff)
                continue

            # Handle other HTTP errors
            try:
                r.raise_for_status()
            except httpx.HTTPStatusError as e:
                if attempt == self._max_retries:
                    raise

                delay = self._base_delay * (2 ** attempt)
                print(
                    f"⚠️  HTTP error {e.response.status_code}, retrying in {delay}s... "
                    f"(attempt {attempt + 1}/{self._max_retries + 1})"
                )
                await asyncio.sleep(delay)
                continue

            # If we reach this point, HTTP is OK.
            # But JSON-RPC could still have *non-429* errors: up to you how to handle.
            if rpc_error:
                # Personally I'd raise to notice it:
                raise RuntimeError(f"JSON-RPC error: {rpc_error}")

            return data

        # Should be unreachable
        raise RuntimeError(f"Max retries ({self._max_retries}) exceeded")


    async def latest_block(self) -> int:
        """Return the latest block number as an int."""
        payload = {"jsonrpc": "2.0", "id": 1, "method": "eth_blockNumber", "params": []}
        data = await self._make_request_with_retry(payload)
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
        payload = {"jsonrpc": "2.0", "id": 1, "method": "eth_getLogs", "params": params}
        data = await self._make_request_with_retry(payload)
        
        if "error" in data:
            e = data["error"]
            raise RuntimeError(f"RPC error: {e.get('code')} {e.get('message')}")

        out: list[EventLog] = []
        for rl in data.get("result", []):
            topics = tuple(
                (t if isinstance(t, str) else t.decode()).lower()
                for t in rl.get("topics", [])
            )
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
