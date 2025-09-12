from __future__ import annotations

from typing import Sequence
import httpx

from defind.core.models import EventLog


def _to_hex_block(x: int) -> str: return hex(x)
def _topics_param(topic0s: Sequence[str]) -> list[list[str]]: return [[t.lower() for t in topic0s]]

class RPC:
    def __init__(self, url: str, *, timeout_s: int = 20, max_connections: int = 64) -> None:
        self.url = url
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(connect=timeout_s, read=timeout_s, write=timeout_s, pool=max(30, timeout_s*3)),
            limits=httpx.Limits(max_connections=max_connections, max_keepalive_connections=max(1, max_connections//2)),
            http2=True,
        )

    async def latest_block(self) -> int:
        payload = {"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}
        r = await self.client.post(self.url, json=payload); r.raise_for_status()
        return int(r.json()["result"], 16)

    async def get_logs(self, *, address: str, topic0s: Sequence[str], from_block: int, to_block: int) -> list[EventLog]:
        params = [{"address":address.lower(),"fromBlock":_to_hex_block(from_block),
                   "toBlock":_to_hex_block(to_block),"topics":_topics_param(topic0s)}]
        r = await self.client.post(self.url, json={"jsonrpc":"2.0","id":1,"method":"eth_getLogs","params":params})
        r.raise_for_status()
        data = r.json()
        if "error" in data:
            e = data["error"]; raise RuntimeError(f"RPC error: {e.get('code')} {e.get('message')}")
        out: list[EventLog] = []
        for rl in data.get("result", []):
            topics = tuple((t if isinstance(t,str) else t.decode()).lower() for t in rl.get("topics", []))
            ts = rl.get("blockTimestamp")
            ts_i = int(ts,16) if isinstance(ts,str) and ts.startswith("0x") else (int(ts) if isinstance(ts,int) else None)
            out.append(EventLog(
                address=rl["address"].lower(),
                topics=topics,
                data_hex=str(rl.get("data") or "0x"),
                block_number=int(rl["blockNumber"],16),
                tx_hash=(rl.get("transactionHash") or rl.get("transaction_hash") or "").lower(),
                log_index=int(rl["logIndex"],16),
                block_timestamp=ts_i,
            ))
        return out

    async def aclose(self) -> None:
        await self.client.aclose()
