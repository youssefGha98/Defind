from __future__ import annotations

import os, glob
from typing import List, Optional
import pyarrow.parquet as pq

from .models import DecodedColumns

class ShardAggregator:
    """
    Strict 250k 'good rows' per shard; optional final partial.
    """
    def __init__(
        self,
        out_root: str,
        addr_slug: str,
        topics_fp: str,
        *,
        rows_per_shard: int = 250_000,
        codec: str = "zstd",
        write_final_partial: bool = True,  # set False to suppress final partial write
    ) -> None:
        self.rows_per_shard = rows_per_shard
        self.codec = codec
        self.write_final_partial = write_final_partial
        self.key_dir = os.path.join(out_root, f"{addr_slug}__topics-{topics_fp}")
        self.shards_dir = os.path.join(self.key_dir, "shards")
        os.makedirs(self.shards_dir, exist_ok=True)
        self.buf = DecodedColumns.empty()
        self.buffered = 0
        self.shard_idx = self._next_shard_index()

    def _next_shard_index(self) -> int:
        existing = sorted(glob.glob(os.path.join(self.shards_dir, "shard_*.parquet")))
        return 1 if not existing else int(os.path.basename(existing[-1]).split("_")[1].split(".")[0]) + 1

    def _write_table(self, cols: DecodedColumns, shard_idx: int) -> str:
        table = cols.to_arrow_table()
        out_path = os.path.join(self.shards_dir, f"shard_{shard_idx:05d}.parquet")
        pq.write_table(table, out_path, compression=self.codec)
        print(f"💾 wrote shard {shard_idx:05d} → {out_path}  (rows={len(table)})")
        return out_path

    def add(self, cols: DecodedColumns) -> List[str]:
        n = cols.size()
        if n == 0:
            return []
        # append
        for i in range(n):
            self.buf.block_number.append(cols.block_number[i])
            self.buf.block_timestamp.append(cols.block_timestamp[i])
            self.buf.tx_hash.append(cols.tx_hash[i])
            self.buf.log_index.append(cols.log_index[i])
            self.buf.pool.append(cols.pool[i])
            self.buf.event.append(cols.event[i])
            self.buf.owner.append(cols.owner[i])
            self.buf.sender.append(cols.sender[i])
            self.buf.recipient.append(cols.recipient[i])
            self.buf.tick_lower.append(cols.tick_lower[i])
            self.buf.tick_upper.append(cols.tick_upper[i])
            self.buf.liquidity.append(cols.liquidity[i])
            self.buf.amount0.append(cols.amount0[i])
            self.buf.amount1.append(cols.amount1[i])
        self.buffered += n

        written: List[str] = []
        while self.buffered >= self.rows_per_shard:
            slice_cols = self.buf.take_first(self.rows_per_shard)  # exactly rows_per_shard rows
            out_path = self._write_table(slice_cols, self.shard_idx)
            written.append(out_path)
            self.shard_idx += 1
            self.buffered -= self.rows_per_shard
        return written

    def close(self) -> Optional[str]:
        if self.buffered == 0:
            return None
        if not self.write_final_partial and self.buffered < self.rows_per_shard:
            # drop the tail if caller insists on strict full shards only
            self.buf = DecodedColumns.empty()
            self.buffered = 0
            return None
        remaining = self.buf.take_first(self.buffered)
        out_path = self._write_table(remaining, self.shard_idx)
        self.shard_idx += 1
        self.buffered = 0
        return out_path
