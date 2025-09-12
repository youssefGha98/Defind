from __future__ import annotations

import os, time, asyncio
from typing import List, Tuple, Sequence, Optional

from defind.adapters.models import ModelAdapter
from defind.storage.shards import ShardAggregator, ShardAggregatorGeneric

from defind.clients.rpc import RPC
from defind.core.models import DecodedColumns, ChunkRecord, GaugeColumns, GaugeRow
from defind.decoding.decoder import Meta, decode_event_with_registry, decode_to_parsed_event
from defind.storage.manifest import LiveManifest
from .coverage import load_done_coverage, subtract_iv
from defind.decoding.specs import EventRegistry
from defind.decoding.registry import make_default_registry

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
    write_final_partial: bool = True,
    registry: Optional[EventRegistry] = None,   # ← NEW: plug in your own specs
) -> dict[str, int]:
    """
    Streaming pipeline:
      RPC logs → generic decode via registry → aggregate → write shard every `rows_per_shard`.
    Coverage is computed from previous manifests in the same key_dir.
    """
    addr_slug = address.lower()
    topics_fp = _topics_fingerprint(topic0s)

    key_dir = os.path.join(out_root, f"{addr_slug}__topics-{topics_fp}")
    man_dir = os.path.join(key_dir, "manifests")
    os.makedirs(man_dir, exist_ok=True)

    reg = registry or make_default_registry()  # default 4 events

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
                        if not ev.topics:
                            filtered_rows += 1
                            continue
                        data_hex = ev.data_hex[2:] if ev.data_hex.lower().startswith("0x") else ev.data_hex
                        data_bytes = bytes.fromhex(data_hex) if data_hex else b""
                        meta = Meta(ev.block_number, ev.block_timestamp, ev.tx_hash, ev.log_index, ev.address)

                        row = decode_event_with_registry(topics=ev.topics, data=data_bytes, meta=meta, registry=reg)
                        if row is None:
                            filtered_rows += 1
                            continue

                        cols.append(row)
                        batch_count += 1
                        decoded_rows += 1

                        if batch_count >= batch_decode_rows:
                            async with agg_lock:
                                written = agg.add(cols)
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
            last = agg.close()
        if last: stats["shards_written"] += 1

        return stats

    finally:
        await rpc.aclose()



async def fetch_decode_streaming_generic(
    *,
    rpc_url: str,
    address: str,
    topic0s: List[str],
    start_block: int | str,
    end_block: int | str,
    step: int,
    concurrency: int,
    out_root: str,
    rows_per_shard: int = 250_000,
    batch_decode_rows: int = 10_000,
    timeout_s: int = 20,
    min_split_span: int = 2_000,
    write_final_partial: bool = True,
    registry: Optional[EventRegistry] = None,
    adapter: ModelAdapter,
) -> dict[str, int]:
    """
    Generic streaming pipeline (adapter-driven):
      RPC getLogs → decode_to_parsed_event (registry) → adapter.row_from_parsed → buffer → shard write.

    - Works for any contract/event family as long as `registry` contains specs with the right `model_tag`,
      and `adapter` implements the typed Columns buffer + row mapping for that tag.
    - Coverage is computed from previous manifests under out_root/{addr_slug}__topics-{fp}__{adapter.tag}/manifests.
    """
    # --- tiny helpers (scoped) ---
    def _now_ts() -> str:
        # human-ish timestamp for manifest filenames
        return time.strftime("%Y%m%d_%H%M%S", time.gmtime())

    def _topics_fingerprint(t0s: List[str]) -> str:
        # stable, short directory key for the set of topic0s
        uniq = sorted({(t or "").lower() for t in t0s if t})
        return "x".join(x[:10] for x in uniq) if uniq else "none"

    # --- dirs, registry, client ---
    addr_slug = address.lower()
    topics_fp = _topics_fingerprint(topic0s)

    # separate key space per adapter tag so different models never mix shards
    key_dir = os.path.join(out_root, f"{addr_slug}__topics-{topics_fp}__{adapter.tag}")
    man_dir = os.path.join(key_dir, "manifests")
    os.makedirs(man_dir, exist_ok=True)

    reg = registry or make_default_registry()

    rpc = RPC(rpc_url, timeout_s=timeout_s, max_connections=max(32, 2 * concurrency))
    try:
        # --- resolve start/end ---
        s_block = 0 if (isinstance(start_block, str) and start_block.lower() in ("earliest", "genesis")) else int(start_block)
        e_block = await rpc.latest_block() if (isinstance(end_block, str) and end_block.lower() == "latest") else int(end_block)
        if s_block > e_block:
            raise ValueError("start_block must be <= end_block")

        # --- coverage planning ---
        run_basename = f"run_{_now_ts()}_{addr_slug}_{topics_fp}_{s_block}_{e_block}.jsonl"
        manifest = LiveManifest(os.path.join(man_dir, run_basename))
        covered = load_done_coverage(man_dir, exclude_basename=run_basename)

        seeds: list[tuple[int, int]] = []
        for us, ue in subtract_iv((s_block, e_block), covered):
            b = us
            while b <= ue:
                fb, tb = b, min(ue, b + step - 1)
                seeds.append((fb, tb))
                b = tb + 1

        # quick exit if everything is already covered
        if not seeds:
            return {
                "processed_ok": 0,
                "processed_failed": 0,
                "executed_subranges": 0,
                "total_logs": 0,
                "partially_covered_split": 0,
                "shards_written": 0,
            }

        # --- shard aggregator (generic) ---
        agg = ShardAggregatorGeneric(
            out_root=out_root,
            addr_slug=addr_slug,
            topics_fp=topics_fp,
            adapter=adapter,
            rows_per_shard=rows_per_shard,
            write_final_partial=write_final_partial,
        )
        agg_lock = asyncio.Lock()
        sem = asyncio.Semaphore(concurrency)

        stats = {
            "processed_ok": 0,
            "processed_failed": 0,
            "executed_subranges": 0,
            "total_logs": 0,
            "partially_covered_split": 0,
            "shards_written": 0,
        }

        async def process_interval(fb: int, tb: int) -> None:
            stack: list[tuple[int, int]] = [(fb, tb)]
            while stack:
                a, b = stack.pop()
                await manifest.append(ChunkRecord(a, b, "started", 0, None, 0, 0, 0, time.time(), 0))
                try:
                    # fetch
                    async with sem:
                        logs = await rpc.get_logs(address=address, topic0s=topic0s, from_block=a, to_block=b)
                    stats["executed_subranges"] += 1
                    stats["total_logs"] += len(logs)

                    # batch buffer for this sub-range
                    cols = adapter.new_buffer()
                    batch_count = 0
                    decoded_rows = 0
                    filtered_rows = 0
                    shards_here = 0

                    for ev in logs:
                        # sanity
                        if not ev.topics:
                            filtered_rows += 1
                            continue

                        # decode
                        data_hex = ev.data_hex[2:] if ev.data_hex.lower().startswith("0x") else ev.data_hex
                        data_bytes = bytes.fromhex(data_hex) if data_hex else b""
                        meta = Meta(ev.block_number, ev.block_timestamp, ev.tx_hash, ev.log_index, ev.address)

                        pe = decode_to_parsed_event(topics=ev.topics, data=data_bytes, meta=meta, registry=reg)
                        if pe is None or pe.model_tag != adapter.tag:
                            filtered_rows += 1
                            continue

                        row = adapter.row_from_parsed(pe)
                        if row is None:
                            filtered_rows += 1
                            continue

                        # buffer
                        adapter.append_row(cols, row)
                        batch_count += 1
                        decoded_rows += 1

                        # periodic flush
                        if batch_count >= batch_decode_rows:
                            async with agg_lock:
                                written = agg.add(cols)
                            shards_here += len(written)
                            cols = adapter.new_buffer()
                            batch_count = 0

                    # final flush for the chunk
                    if batch_count > 0:
                        async with agg_lock:
                            written = agg.add(cols)
                        shards_here += len(written)

                    stats["processed_ok"] += 1
                    stats["shards_written"] += shards_here
                    await manifest.append(ChunkRecord(a, b, "done", 1, None, len(logs), decoded_rows, shards_here, time.time(), filtered_rows))

                except Exception as e:
                    # split range when large enough; otherwise mark failed
                    span = b - a + 1
                    if span > min_split_span:
                        mid = (a + b) // 2
                        stack.extend([(a, mid), (mid + 1, b)])
                        stats["partially_covered_split"] += 1
                        # mark partial failure for visibility
                        await manifest.append(ChunkRecord(a, b, "failed", 1, str(e), 0, 0, 0, time.time(), 0))
                    else:
                        stats["processed_failed"] += 1
                        await manifest.append(ChunkRecord(a, b, "failed", 1, str(e), 0, 0, 0, time.time(), 0))

        # fan out
        await asyncio.gather(*(asyncio.create_task(process_interval(fb, tb)) for fb, tb in seeds))

        # finalize shards
        async with agg_lock:
            last = agg.close()
        if last:
            stats["shards_written"] += 1

        return stats

    finally:
        await rpc.aclose()