from __future__ import annotations

import os, time, asyncio
from typing import List, Tuple, Sequence

from .rpc import RPC
from .constants import MINT_T0, BURN_T0, COLLECT_T0, COLLECTFEES_T0  # (re-export convenience)
from .models import DecodedColumns, ChunkRecord
from .decoder import Meta, decode_event
from .manifest import LiveManifest
from .coverage import load_done_coverage, subtract_iv
from .shards import ShardAggregator

def _now_ts() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

def _topics_fingerprint(topic0s: Sequence[str]) -> str:
    base = ",".join(sorted([t.lower() for t in topic0s]))
    import hashlib as h
    return h.sha1(base.encode()).hexdigest()[:10]

async def fetch_decode_streaming_to_shards(
    *,
    rpc_url: str,
    address: str,
    topic0s: List[str],
    start_block: int | str,          # int | "earliest"/"genesis"
    end_block: int | str,            # int | "latest"
    step: int,
    concurrency: int,
    out_root: str,                   # base dir (manifests + shards)
    rows_per_shard: int = 250_000,
    batch_decode_rows: int = 10_000,
    timeout_s: int = 20,
    min_split_span: int = 2_000,
    write_final_partial: bool = True,  # set False to suppress final partial file
) -> dict[str, int]:
    """
    Streaming pipeline:
      RPC logs → decode on the fly → aggregate → write shard every `rows_per_shard`.
    Coverage is computed from previous manifests in the same key_dir.
    """
    addr_slug = address.lower()
    topics_fp = _topics_fingerprint(topic0s)

    key_dir = os.path.join(out_root, f"{addr_slug}__topics-{topics_fp}")
    man_dir = os.path.join(key_dir, "manifests")
    os.makedirs(man_dir, exist_ok=True)

    rpc = RPC(rpc_url, timeout_s=timeout_s, max_connections=max(32, 2*concurrency))
    try:
        s_block = 0 if (isinstance(start_block, str) and start_block.lower() in ("earliest", "genesis")) else int(start_block)
        e_block = await rpc.latest_block() if (isinstance(end_block, str) and end_block.lower() == "latest") else int(end_block)
        if s_block > e_block:
            raise ValueError(f"start_block ({s_block}) must be <= end_block ({e_block})")

        run_basename = f"run_{_now_ts()}_{addr_slug}_{topics_fp}_{s_block}_{e_block}.jsonl"
        manifest = LiveManifest(os.path.join(man_dir, run_basename))
        covered = load_done_coverage(man_dir, exclude_basename=run_basename)

        # plan seeds
        seeds: list[tuple[int,int]] = []
        for us, ue in subtract_iv((s_block, e_block), covered):
            b = us
            while b <= ue:
                fb, tb = b, min(ue, b + step - 1)
                seeds.append((fb, tb)); b = tb + 1
        if not seeds:
            return {"processed_ok":0,"processed_failed":0,"executed_subranges":0,
                    "total_logs":0,"partially_covered_split":0,"shards_written":0}

        agg = ShardAggregator(
            out_root, addr_slug, topics_fp,
            rows_per_shard=rows_per_shard,
            write_final_partial=write_final_partial,
        )
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

                    cols = DecodedColumns.empty()
                    batch_count = 0
                    decoded_rows = 0
                    filtered_rows = 0
                    shards_here = 0

                    for ev in logs:
                        topics = ev.topics
                        if not topics:
                            filtered_rows += 1
                            continue
                        data_hex = ev.data_hex[2:] if ev.data_hex.lower().startswith("0x") else ev.data_hex
                        data_bytes = bytes.fromhex(data_hex) if data_hex else b""
                        meta = Meta(ev.block_number, ev.block_timestamp, ev.tx_hash, ev.log_index, ev.address)

                        row = decode_event(topics=topics, data=data_bytes, meta=meta)
                        if row is None:
                            filtered_rows += 1
                            continue

                        cols.append(row)
                        batch_count += 1
                        decoded_rows += 1

                        if batch_count >= batch_decode_rows:
                            async with agg_lock:
                                written = agg.add(cols)   # flushes only full shards (250k good rows)
                            shards_here += len(written)
                            cols = DecodedColumns.empty()
                            batch_count = 0

                    if batch_count > 0:
                        async with agg_lock:
                            written = agg.add(cols)
                        shards_here += len(written)

                    stats["processed_ok"] += 1
                    stats["shards_written"] += shards_here
                    await manifest.append(ChunkRecord(a,b,"done",1,None,len(logs),decoded_rows,shards_here,time.time(),filtered_rows))

                except Exception as e:
                    span = b - a + 1
                    if span > min_split_span:
                        mid = (a + b)//2
                        left, right = (a, mid), (mid+1, b)
                        stats["partially_covered_split"] += 1
                        if left[0] <= left[1]:  stack.append(left)
                        if right[0] <= right[1]: stack.append(right)
                    else:
                        stats["processed_failed"] += 1
                        await manifest.append(ChunkRecord(a,b,"failed",1,str(e),0,0,0,time.time(),0))

        await asyncio.gather(*(asyncio.create_task(process_interval(fb, tb)) for fb, tb in seeds))

        async with agg_lock:
            last = agg.close()  # writes final partial iff write_final_partial=True
        if last: stats["shards_written"] += 1

        return stats

    finally:
        await rpc.aclose()
