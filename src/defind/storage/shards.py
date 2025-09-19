# shards.py
from __future__ import annotations
import os, glob
from typing import List, Optional
import pyarrow.parquet as pq

# ... keep any existing imports/classes ...

class ShardAggregatorUniversal:
    """
    Universal shard writer using dynamic columns:
    any projected key becomes a new column on the fly.
    """
    def __init__(self, out_root: str, addr_slug: str, topics_fp: str, *,
                 rows_per_shard: int = 250_000, codec: str = "zstd", write_final_partial: bool = True) -> None:
        self.rows_per_shard = rows_per_shard
        self.codec = codec
        self.write_final_partial = write_final_partial
        key_name = f"{addr_slug}__topics-{topics_fp}__universal"
        self.key_dir = os.path.join(out_root, key_name)
        self.shards_dir = os.path.join(self.key_dir, "shards")
        os.makedirs(self.shards_dir, exist_ok=True)

        from defind.core.models import UniversalDynColumns
        self.buf = UniversalDynColumns.empty()
        self.shard_idx = self._next_shard_index()

    def _next_shard_index(self) -> int:
        existing = sorted(glob.glob(os.path.join(self.shards_dir, "shard_*.parquet")))
        return 1 if not existing else int(os.path.basename(existing[-1]).split("_")[1].split(".")[0]) + 1

    def _write_table(self, cols, shard_idx: int) -> str:
        table = cols.to_arrow_table()
        if len(table) == 0:
            return ""
        out_path = os.path.join(self.shards_dir, f"shard_{shard_idx:05d}.parquet")
        pq.write_table(table, out_path, compression=self.codec)
        print(f"💾 wrote shard {shard_idx:05d} → {out_path}  (rows={len(table)}, cols={len(table.schema)})")
        return out_path

    def add(self, cols) -> List[str]:
        appended = self.buf.extend(cols)
        if appended == 0:
            return []
        written: List[str] = []
        while self.buf.size() >= self.rows_per_shard:
            slice_cols = self.buf.take_first(self.rows_per_shard)
            out_path = self._write_table(slice_cols, self.shard_idx)
            if out_path:
                written.append(out_path)
                self.shard_idx += 1
        return written

    def close(self) -> Optional[str]:
        size = self.buf.size()
        if size == 0:
            return None
        if not self.write_final_partial and size < self.rows_per_shard:
            from defind.core.models import UniversalDynColumns
            self.buf = UniversalDynColumns.empty()
            return None
        slice_cols = self.buf.take_first(size)
        out_path = self._write_table(slice_cols, self.shard_idx)
        if out_path:
            self.shard_idx += 1
            return out_path
        return None
