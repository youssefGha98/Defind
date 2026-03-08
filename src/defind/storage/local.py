# mypy: disable-error-code="import-untyped"
from __future__ import annotations

import contextlib
import fcntl
import hashlib
import json
import os
from pathlib import Path
from typing import Any

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

    def supports_atomic_locks(self) -> bool:
        return True

    def write_table(self, key: str, table: pa.Table, codec: str) -> None:
        out_path = self._full_path(key)
        out_path.parent.mkdir(exist_ok=True, parents=True)
        tmp_path = out_path.with_suffix(".tmp")
        pq.write_table(table, tmp_path, compression=codec)
        os.replace(tmp_path, out_path)

    def exists(self, key: str) -> bool:
        return self._full_path(key).exists()

    def list_keys(self, prefix: str) -> list[str]:
        """List all file keys under `root/prefix`."""
        base = self.root / prefix
        if not base.exists():
            return []
        return [str(p.relative_to(self.root)) for p in base.rglob("*") if p.is_file()]

    def delete(self, key: str) -> None:
        path = self._full_path(key)
        try:
            path.unlink()
        except FileNotFoundError:
            pass

    def write_json(self, key: str, payload: dict[str, Any]) -> None:
        out_path = self._full_path(key)
        out_path.parent.mkdir(exist_ok=True, parents=True)
        tmp_path = out_path.with_suffix(".tmp")
        tmp_path.write_text(json.dumps(payload, separators=(",", ":"), sort_keys=True), encoding="utf-8")
        os.replace(tmp_path, out_path)

    def write_text(self, key: str, payload: str) -> None:
        out_path = self._full_path(key)
        out_path.parent.mkdir(exist_ok=True, parents=True)
        tmp_path = out_path.with_suffix(".tmp")
        tmp_path.write_text(payload, encoding="utf-8")
        os.replace(tmp_path, out_path)

    @staticmethod
    def _json_version(raw: bytes) -> str:
        return hashlib.sha256(raw).hexdigest()

    def _json_lock_path(self, key: str) -> Path:
        target = self._full_path(key)
        return target.with_suffix(f"{target.suffix}.lock")

    def create_json_if_absent(self, key: str, payload: dict[str, Any]) -> bool:
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

    def read_json(self, key: str) -> dict[str, Any] | None:
        path = self._full_path(key)
        if not path.exists():
            return None
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            return data if isinstance(data, dict) else None
        except Exception:
            return None

    def read_text(self, key: str) -> str | None:
        path = self._full_path(key)
        if not path.exists():
            return None
        try:
            return path.read_text(encoding="utf-8")
        except Exception:
            return None

    def read_json_with_version(self, key: str) -> tuple[dict[str, Any] | None, str | None]:
        path = self._full_path(key)
        if not path.exists():
            return None, None
        try:
            raw = path.read_bytes()
        except FileNotFoundError:
            return None, None
        version = self._json_version(raw)
        try:
            data = json.loads(raw.decode("utf-8"))
        except Exception:
            return None, version
        return (data if isinstance(data, dict) else None), version

    def write_json_if_version(self, key: str, payload: dict[str, Any], expected_version: str | None) -> bool:
        out_path = self._full_path(key)
        out_path.parent.mkdir(exist_ok=True, parents=True)
        lock_path = self._json_lock_path(key)
        lock_path.parent.mkdir(exist_ok=True, parents=True)
        raw = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
        tmp_path = out_path.with_suffix(".tmp")

        with lock_path.open("a+b") as lock_file:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            try:
                try:
                    current_raw = out_path.read_bytes()
                    current_version = self._json_version(current_raw)
                except FileNotFoundError:
                    current_version = None
                if current_version != expected_version:
                    return False
                tmp_path.write_bytes(raw)
                os.replace(tmp_path, out_path)
                return True
            finally:
                with contextlib.suppress(FileNotFoundError):
                    tmp_path.unlink()
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
