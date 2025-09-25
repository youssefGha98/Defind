# shards.py
from __future__ import annotations

import glob
import os
from typing import List, Optional

import pyarrow as pa
import pyarrow.parquet as pq

from defind.core.models import UniversalDynColumns


class ShardAggregatorUniversal:
    """
    Universal shard writer using dynamic columns:
    any projected key becomes a new column on the fly.

    New behavior:
    - If the last existing shard is not full, we *append to it* (by rewriting) until it
      reaches `rows_per_shard`, even across brand-new runs. Only then do we advance to the
      next shard index.
    """

    def __init__(
        self,
        out_root: str,
        addr_slug: str,
        topics_fp: str,
        *,
        rows_per_shard: int = 250_000,
        codec: str = "zstd",
        write_final_partial: bool = True,
    ) -> None:
        self.rows_per_shard = rows_per_shard
        self.codec = codec
        self.write_final_partial = write_final_partial

        key_name = f"{addr_slug}__topics-{topics_fp}__universal"
        self.key_dir = os.path.join(out_root, key_name)
        self.shards_dir = os.path.join(self.key_dir, "shards")
        os.makedirs(self.shards_dir, exist_ok=True)

        # in-memory buffer for incoming rows
        self.buf = UniversalDynColumns.empty()

        # Partial-open shard state (if last shard < rows_per_shard)
        self._open_partial_idx: Optional[int] = None
        self._open_partial_tbl: Optional[pa.Table] = None
        self._open_partial_remaining: int = 0

        # Initialize writer state from existing shards
        self.shard_idx = self._init_from_existing()

    # ---------- init & helpers ----------

    def _list_shards(self) -> list[str]:
        return sorted(glob.glob(os.path.join(self.shards_dir, "shard_*.parquet")))

    def _init_from_existing(self) -> int:
        """Load last shard (if any). If it's partial, keep it open for topping up."""
        existing = self._list_shards()
        if not existing:
            return 1

        last_path = existing[-1]
        last_idx = int(os.path.basename(last_path).split("_")[1].split(".")[0])

        # Read only the metadata (cheap) to get row count.
        pf = pq.ParquetFile(last_path)
        last_rows = pf.metadata.num_rows

        if last_rows < self.rows_per_shard and last_rows > 0:
            # Keep the partial shard "open": read it once so we can rewrite atomically.
            self._open_partial_idx = last_idx
            self._open_partial_tbl = pq.read_table(last_path)
            self._open_partial_remaining = self.rows_per_shard - last_rows
            return last_idx  # do NOT advance yet; we will when we fill it
        else:
            # No partial shard to fill; next write will use the next index.
            return last_idx + 1

    def _atomic_write(self, out_path: str, table: pa.Table) -> str:
        """Write Parquet atomically (tmp + replace)."""
        if len(table) == 0:
            return ""
        tmp = out_path + ".tmp"
        pq.write_table(table, tmp, compression=self.codec)
        os.replace(tmp, out_path)
        print(f"💾 wrote → {out_path}  (rows={len(table)}, cols={len(table.schema)})")
        return out_path

    def _shard_path(self, idx: int) -> str:
        return os.path.join(self.shards_dir, f"shard_{idx:05d}.parquet")

    # ---------- core API ----------

    def add(self, cols: UniversalDynColumns) -> List[str]:
        """Merge `cols` into the aggregator; write shards as they become full.

        Returns a list of shard paths that were written/rewritten.
        """
        written: List[str] = []

        # Extend in-memory buffer
        appended = self.buf.extend(cols)
        if appended == 0:
            return written

        # 1) If we have a partial last shard open, top it up first
        if self._open_partial_idx is not None and self._open_partial_remaining > 0:
            if self.buf.size() > 0:
                take_n = min(self._open_partial_remaining, self.buf.size())
                slice_cols = self.buf.take_first(take_n)
                new_tbl = slice_cols.to_arrow_table()

                # Concatenate with existing shard (promote schema if needed)
                assert self._open_partial_tbl is not None
                updated_tbl = pa.concat_tables([self._open_partial_tbl, new_tbl], promote=True)

                # Rewrite the same shard file atomically
                out_path = self._atomic_write(self._shard_path(self._open_partial_idx), updated_tbl)
                if out_path:
                    written.append(out_path)

                self._open_partial_remaining -= take_n
                self._open_partial_tbl = updated_tbl if self._open_partial_remaining > 0 else None

                # If we just filled it, advance shard index and clear partial state
                if self._open_partial_remaining == 0:
                    self.shard_idx = (self._open_partial_idx or 0) + 1
                    self._open_partial_idx = None
                    self._open_partial_tbl = None

        # 2) Write any full shards from the buffer into *new* shard files
        while self.buf.size() >= self.rows_per_shard:
            slice_cols = self.buf.take_first(self.rows_per_shard)
            tbl = slice_cols.to_arrow_table()
            out_path = self._atomic_write(self._shard_path(self.shard_idx), tbl)
            if out_path:
                written.append(out_path)
                self.shard_idx += 1

        # 3) Leave any remaining (< rows_per_shard) rows in memory buffer
        return written

    def close(self) -> Optional[str]:
        """Flush remaining rows.

        Rules:
        - If a partial shard is currently open, we *can* add remaining rows to it,
          regardless of `write_final_partial`, because this doesn't create a new shard.
        - If no partial shard is open:
            * If `write_final_partial` is False and in-memory rows < rows_per_shard, drop them.
            * Else, write them as a final (possibly short) shard.
        Returns the last shard path written (if any).
        """
        size = self.buf.size()
        if size == 0:
            return None

        # If we still have a partial shard open, try topping it up one last time.
        last_path: Optional[str] = None
        if self._open_partial_idx is not None and self._open_partial_remaining > 0 and size > 0:
            take_n = min(self._open_partial_remaining, size)
            slice_cols = self.buf.take_first(take_n)
            new_tbl = slice_cols.to_arrow_table()

            assert self._open_partial_tbl is not None
            updated_tbl = pa.concat_tables([self._open_partial_tbl, new_tbl], promote=True)
            last_path = self._atomic_write(self._shard_path(self._open_partial_idx), updated_tbl)

            self._open_partial_remaining -= take_n
            self._open_partial_tbl = updated_tbl if self._open_partial_remaining > 0 else None

            if self._open_partial_remaining == 0:
                self.shard_idx = (self._open_partial_idx or 0) + 1
                self._open_partial_idx = None
                self._open_partial_tbl = None

        # Now decide whether to write a *new* final partial shard
        remaining = self.buf.size()
        if remaining == 0:
            return last_path

        if not self.write_final_partial and remaining < self.rows_per_shard:
            # drop trailing in-memory rows (no file created)
            self.buf = UniversalDynColumns.empty()
            return last_path

        slice_cols = self.buf.take_first(remaining)
        tbl = slice_cols.to_arrow_table()
        out_path = self._atomic_write(self._shard_path(self.shard_idx), tbl)
        if out_path:
            self.shard_idx += 1
            return out_path
        return last_path
