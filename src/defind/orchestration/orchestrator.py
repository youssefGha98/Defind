# orchestrator.py
from __future__ import annotations
import os, time, asyncio
from typing import List, Optional, Tuple

from defind.clients.rpc import RPC
from defind.decoding.registry import make_default_registry
from defind.decoding.decoder import Meta, decode_to_parsed_event
from defind.storage.manifest import LiveManifest
from defind.storage.shards import ShardAggregatorUniversal
from defind.orchestration.coverage import load_done_coverage, subtract_iv
from defind.core.models import ChunkRecord, UniversalDynColumns

def _topics_fingerprint(t0s: List[str]) -> str:
    uniq = sorted({(t or "").lower() for t in t0s if t})
    return "x".join(x[:10] for x in uniq) if uniq else "none"

def _iter_chunks(a:int, b:int, step:int):
    x = a
    while x <= b:
        y = min(b, x + step - 1)
        yield (x, y)
        x = y + 1

async def fetch_decode_streaming_universal(
    *,
    rpc_url: str,
    address: str,
    topic0s: List[str],
    start_block: int | str,
    end_block: int | str,
    step: int = 5_000,
    concurrency: int = 16,
    out_root: str = "./data",
    rows_per_shard: int = 250_000,
    batch_decode_rows: int = 10_000,
    timeout_s: int = 20,
    min_split_span: int = 2_000,
    write_final_partial: bool = True,
    registry=None,
) -> dict[str, int]:
    addr_slug = address.lower()
    topics_fp = _topics_fingerprint(topic0s)
    key_dir = os.path.join(out_root, f"{addr_slug}__topics-{topics_fp}__universal")
    man_dir = os.path.join(key_dir, "manifests")
    os.makedirs(man_dir, exist_ok=True)

    reg = registry or make_default_registry()
    rpc = RPC(rpc_url, timeout_s=timeout_s, max_connections=max(32, 2*concurrency))
    try:
        s_block = 0 if (isinstance(start_block, str) and start_block.lower() in ("earliest","genesis")) else int(start_block)
        e_block = await rpc.latest_block() if (isinstance(end_block, str) and end_block.lower() == "latest") else int(end_block)
        if s_block > e_block:
            raise ValueError("start_block must be <= end_block")

        run_basename = f"run_{time.strftime('%Y%m%d_%H%M%S', time.gmtime())}_{addr_slug}_{topics_fp}_{s_block}_{e_block}.jsonl"
        manifest = LiveManifest(os.path.join(man_dir, run_basename))
        covered = load_done_coverage(man_dir, exclude_basename=run_basename)

        seeds: List[Tuple[int,int]] = []
        for us, ue in subtract_iv((s_block, e_block), covered):
            for a,b in _iter_chunks(us, ue, step):
                seeds.append((a,b))
        if not seeds:
            return {"processed_ok":0,"processed_failed":0,"executed_subranges":0,
                    "total_logs":0,"partially_covered_split":0,"shards_written":0}

        agg = ShardAggregatorUniversal(out_root, addr_slug, topics_fp,
                                       rows_per_shard=rows_per_shard,
                                       write_final_partial=write_final_partial)
        agg_lock = asyncio.Lock()
        sem = asyncio.Semaphore(concurrency)

        stats = {"processed_ok":0,"processed_failed":0,"executed_subranges":0,
                 "total_logs":0,"partially_covered_split":0,"shards_written":0}

        async def process_interval(fb:int, tb:int) -> None:
            stack: list[tuple[int,int]] = [(fb, tb)]
            while stack:
                a, b = stack.pop()
                await manifest.append(ChunkRecord(a,b,"started",0,None,0,0,0,time.time(),0))
                try:
                    async with sem:
                        logs = await rpc.get_logs(address=address, topic0s=topic0s, from_block=a, to_block=b)
                    stats["executed_subranges"] += 1
                    stats["total_logs"] += len(logs)

                    buf = UniversalDynColumns.empty()
                    batch_count = 0
                    decoded_rows = 0
                    filtered_rows = 0
                    shards_here = 0

                    for ev in logs:
                        if not ev.topics:
                            filtered_rows += 1; continue
                        data_hex = ev.data_hex[2:] if ev.data_hex.lower().startswith("0x") else ev.data_hex
                        data_bytes = bytes.fromhex(data_hex) if data_hex else b""
                        meta = Meta(ev.block_number, ev.block_timestamp, ev.tx_hash, ev.log_index, ev.address)

                        pe = decode_to_parsed_event(topics=ev.topics, data=data_bytes, meta=meta, registry=reg)
                        if pe is None:
                            filtered_rows += 1; continue

                        # Single call: append everything dynamically
                        buf.append_from_parsed(pe_name=pe.name, meta=meta, values=pe.values, contract_addr=pe.pool)
                        batch_count += 1
                        decoded_rows += 1

                        if batch_count >= batch_decode_rows:
                            async with agg_lock:
                                written = agg.add(buf)
                            shards_here += len(written)
                            buf = UniversalDynColumns.empty()
                            batch_count = 0

                    if batch_count > 0:
                        async with agg_lock:
                            written = agg.add(buf)
                        shards_here += len(written)

                    stats["processed_ok"] += 1
                    stats["shards_written"] += shards_here
                    await manifest.append(ChunkRecord(a,b,"done",1,None,len(logs),decoded_rows,shards_here,time.time(),filtered_rows))

                except Exception as e:
                    span = b - a + 1
                    if span > min_split_span:
                        mid = (a + b)//2
                        stack.extend([(a, mid), (mid+1, b)])
                        stats["partially_covered_split"] += 1
                        await manifest.append(ChunkRecord(a,b,"failed",1,str(e),0,0,0,time.time(),0))
                    else:
                        stats["processed_failed"] += 1
                        await manifest.append(ChunkRecord(a,b,"failed",1,str(e),0,0,0,time.time(),0))

        await asyncio.gather(*(asyncio.create_task(process_interval(fb, tb)) for fb, tb in seeds))

        async with agg_lock:
            last = agg.close()
        if last: stats["shards_written"] += 1

        return stats
    finally:
        await rpc.aclose()
