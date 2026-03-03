from __future__ import annotations

import json
import os
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from defind.core.interfaces import IChunkStorage


class LocalChunkStorage(IChunkStorage):
    """Local filesystem implementation of IChunkStorage.

    Keys are relative paths under `root`, e.g.:
        "Mint/chunk_0012376729_0012381729.parquet"
        → {root}/Mint/chunk_0012376729_0012381729.parquet

    Uses atomic tmp → rename writes to ensure crash-safety.
    Empty tables (0 rows) are written as valid Parquet files so that
    file presence can serve as an unambiguous "done" marker.
    """

    def __init__(self, root: Path) -> None:
        self.root = root
        self.root.mkdir(exist_ok=True, parents=True)

    def _full_path(self, key: str) -> Path:
        return self.root / key

    def write_table(self, key: str, table: pa.Table, codec: str) -> None:
        out_path = self._full_path(key)
        out_path.parent.mkdir(exist_ok=True, parents=True)
        tmp_path = out_path.with_suffix(".tmp")
        pq.write_table(table, tmp_path, compression=codec)
        os.replace(tmp_path, out_path)

    def exists(self, key: str) -> bool:
        return self._full_path(key).exists()

    def list_keys(self, prefix: str) -> list[str]:
        """List all .parquet keys under `root/prefix`."""
        base = self.root / prefix
        if not base.exists():
            return []
        return [
            str(p.relative_to(self.root))
            for p in base.rglob("*.parquet")
        ]

    def delete(self, key: str) -> None:
        path = self._full_path(key)
        try:
            path.unlink()
        except FileNotFoundError:
            pass

    def write_json(self, key: str, payload: dict) -> None:
        out_path = self._full_path(key)
        out_path.parent.mkdir(exist_ok=True, parents=True)
        tmp_path = out_path.with_suffix(".tmp")
        tmp_path.write_text(json.dumps(payload, separators=(",", ":"), sort_keys=True), encoding="utf-8")
        os.replace(tmp_path, out_path)

    def create_json_if_absent(self, key: str, payload: dict) -> bool:
        out_path = self._full_path(key)
        out_path.parent.mkdir(exist_ok=True, parents=True)
        raw = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
        flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
        try:
            fd = os.open(out_path, flags, 0o644)
        except FileExistsError:
            return False
        try:
            with os.fdopen(fd, "wb") as out:
                out.write(raw)
        except Exception:
            # Best effort cleanup if write fails after create.
            try:
                out_path.unlink()
            except Exception:
                pass
            raise
        return True

    def read_json(self, key: str) -> dict | None:
        path = self._full_path(key)
        if not path.exists():
            return None
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            return data if isinstance(data, dict) else None
        except Exception:
            return None
