"""Streaming orchestrator: fetch → decode → write universal shards.

Features
--------
- Resumable via *live manifest* + historical coverage merging.
- Dynamic projections: any registry projection key becomes a column.
- Bounded memory via `rows_per_shard` and `batch_decode_rows`.
- Automatic interval splitting on failures (`min_split_span`).

Public API
----------
`fetch_decode(...)` → dict[str, int] summary.
"""

from __future__ import annotations

import asyncio
import os
import time
from dataclasses import dataclass
from typing import Any

from defind.clients.rpc import RPC
from defind.decoding.decoder import Meta, decode_event
from defind.decoding.specs import EventRegistry
from defind.storage.manifest import LiveManifest
from defind.storage.shards import ShardWriter
from defind.orchestration.utils import (
    iter_chunks,
    load_done_coverage,
    subtract_iv,
    topics_fingerprint,
)
from defind.core.models import ChunkRecord, Column


# -------------------------
# Context & worker
# -------------------------

@dataclass(slots=True)
class ProcessContext:
    """Shared state for interval processing (keeps worker signature small)."""
    rpc: RPC
    address: str
    topic0s: list[str]
    registry: EventRegistry
    batch_decode_rows: int
    min_split_span: int
    sem: asyncio.Semaphore
    agg_lock: asyncio.Lock
    manifest: LiveManifest
    aggregator: ShardWriter
    stats: dict[str, int]


async def process_interval(ctx: ProcessContext, start_block: int, end_block: int) -> None:
    """Process one inclusive [start_block, end_block] range with retry splitting."""
    stack: list[tuple[int, int]] = [(start_block, end_block)]
    while stack:
        a, b = stack.pop()
        await ctx.manifest.append(ChunkRecord(a, b, "started", 0, None, 0, 0, 0, time.time(), 0))
        try:
            async with ctx.sem:
                logs = await ctx.rpc.get_logs(address=ctx.address, topic0s=ctx.topic0s, from_block=a, to_block=b)

            ctx.stats["executed_subranges"] += 1
            ctx.stats["total_logs"] += len(logs)

            buf = Column.empty()
            rows_in_batch = 0
            decoded_rows = 0
            filtered_rows = 0
            shards_here = 0

            for i, ev in enumerate(logs):
         
                if not ev.topics:
                    filtered_rows += 1
                    continue

                # Decode raw hex data payload
                data_hex = ev.data_hex[2:] if ev.data_hex.lower().startswith("0x") else ev.data_hex
                data_bytes = bytes.fromhex(data_hex) if data_hex else b""

                meta = Meta(ev.block_number, ev.block_timestamp, ev.tx_hash, ev.log_index, ev.address)

                pe = decode_event(topics=ev.topics, data=data_bytes, meta=meta, registry=ctx.registry)
                
                if pe is None:
                    filtered_rows += 1
                    continue

                # Append all dynamic keys in one go
                buf.append_from_parsed(pe_name=pe.name, meta=meta, values=pe.values, contract_addr=pe.pool)
                rows_in_batch += 1
                decoded_rows += 1

                if rows_in_batch >= ctx.batch_decode_rows:
                    async with ctx.agg_lock:
                        written_by_event = ctx.aggregator.add(buf)
                    # Count total shards across all events
                    for paths in written_by_event.values():
                        shards_here += len(paths)
                    buf = Column.empty()
                    rows_in_batch = 0

            # Flush last partial batch for this interval
            if rows_in_batch > 0:
                async with ctx.agg_lock:
                    written_by_event = ctx.aggregator.add(buf)
                # Count total shards across all events
                for paths in written_by_event.values():
                    shards_here += len(paths)

            ctx.stats["processed_ok"] += 1
            ctx.stats["shards_written"] += shards_here
            await ctx.manifest.append(
                ChunkRecord(a, b, "done", 1, None, len(logs), decoded_rows, shards_here, time.time(), filtered_rows)
            )

        except Exception as e:
            span = b - a + 1
            if span > ctx.min_split_span:
                mid = (a + b) // 2
                stack.extend([(a, mid), (mid + 1, b)])
                ctx.stats["partially_covered_split"] += 1
                await ctx.manifest.append(ChunkRecord(a, b, "failed", 1, str(e), 0, 0, 0, time.time(), 0))
            else:
                ctx.stats["processed_failed"] += 1
                await ctx.manifest.append(ChunkRecord(a, b, "failed", 1, str(e), 0, 0, 0, time.time(), 0))


# -------------------------
# Orchestrator
# -------------------------

def _setup_directories(out_root: str) -> str:
    """Setup output directories for the fetch operation.
    
    Returns:
        Root directory path for manifests
    """
    manifests_dir = os.path.join(out_root, "_manifests")
    os.makedirs(manifests_dir, exist_ok=True)
    return manifests_dir


async def _resolve_block_range(rpc: RPC, start_block: int | str, end_block: int | str) -> tuple[int, int]:
    """Resolve start and end blocks, handling special values like 'latest'.
    
    Returns:
        Tuple of (start, end) as integers
    """
    start = 0 if (isinstance(start_block, str) and start_block.lower() in ("earliest", "genesis")) else int(start_block)
    end = await rpc.latest_block() if (isinstance(end_block, str) and end_block.lower() == "latest") else int(end_block)
    
    if start > end:
        raise ValueError("start_block must be <= end_block")
    
    return start, end


def _build_work_seeds(start: int, end: int, step: int, covered: list[tuple[int, int]]) -> list[tuple[int, int]]:
    """Build list of uncovered block ranges to process.
    
    Returns:
        List of (start_block, end_block) tuples to process
    """
    seeds: list[tuple[int, int]] = []
    uncovered_ranges = list(subtract_iv((start, end), covered))
    
    for uncovered_start, uncovered_end in uncovered_ranges:
        chunks = list(iter_chunks(uncovered_start, uncovered_end, step))
        for a, b in chunks:
            seeds.append((a, b))
    
    return seeds


async def fetch_decode(
    *,
    rpc_url: str,
    address: str,
    topic0s: list[str],
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
    registry: EventRegistry | None = None,
) -> dict[str, int]:
    """Stream logs from RPC, decode with registry, and write Parquet shards.

    Returns
    -------
    dict[str, int]
        Summary counters for the run.
    """
    if not registry:
        raise ValueError("Registry is empty. Compose a registry (e.g., CL pool + Gauge) and pass it explicitly.")

    # Setup directories and RPC client
    manifests_dir = _setup_directories(out_root)
    rpc = RPC(rpc_url, timeout_s=timeout_s, max_connections=max(32, concurrency))
    
    try:
        # Resolve block range
        start, end = await _resolve_block_range(rpc, start_block, end_block)
        
        # Setup manifest and coverage
        address_lc = address.lower()
        topics_fp = topics_fingerprint(topic0s)
        run_basename = f"run_{time.strftime('%Y%m%d_%H%M%S', time.gmtime())}_{address_lc}_{topics_fp}_{start}_{end}.jsonl"
        manifest = LiveManifest(os.path.join(manifests_dir, run_basename))
        
        covered = load_done_coverage(manifests_dir, exclude_basename=run_basename)
        
        # Build work list
        seeds = _build_work_seeds(start, end, step, covered)



        if not seeds:
            return {
                "processed_ok": 0,
                "processed_failed": 0,
                "executed_subranges": 0,
                "total_logs": 0,
                "partially_covered_split": 0,
                "shards_written": 0,
            }
        aggregator = ShardWriter(
            out_root,
            address_lc,
            topics_fp,
            rows_per_shard=rows_per_shard,
            write_final_partial=write_final_partial,
        )
        agg_lock = asyncio.Lock()
        sem = asyncio.Semaphore(concurrency)

        stats: dict[str, int] = {
            "processed_ok": 0,
            "processed_failed": 0,
            "executed_subranges": 0,
            "total_logs": 0,
            "partially_covered_split": 0,
            "shards_written": 0,
        }

        ctx = ProcessContext(
            rpc=rpc,
            address=address,
            topic0s=topic0s,
            registry=registry,
            batch_decode_rows=batch_decode_rows,
            min_split_span=min_split_span,
            sem=sem,
            agg_lock=agg_lock,
            manifest=manifest,
            aggregator=aggregator,
            stats=stats,
        )

        # Dispatch workers
        tasks = [asyncio.create_task(process_interval(ctx, a, b)) for a, b in seeds]
        await asyncio.gather(*tasks)
        # Close aggregator and possibly write trailing partial shards
        async with agg_lock:
            last_by_event = aggregator.close()
        # Count final shards written
        for last_path in last_by_event.values():
            if last_path:
                stats["shards_written"] += 1
        return stats
    finally:
        await rpc.aclose()
