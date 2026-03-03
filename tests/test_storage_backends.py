from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest
from pyarrow import fs as pa_fs

from defind.storage.local import LocalChunkStorage
from defind.storage.s3 import S3ChunkStorage


@dataclass
class _FakeFileInfo:
    type: Any
    path: str


class _FakeOutputStream:
    def __init__(self, store: dict[str, bytes], path: str) -> None:
        self._store = store
        self._path = path
        self._buf = bytearray()

    def __enter__(self) -> _FakeOutputStream:
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        if exc_type is None:
            self._store[self._path] = bytes(self._buf)
        return False

    def write(self, data: bytes) -> None:
        self._buf.extend(data)


class _FakeInputFile:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    def __enter__(self) -> _FakeInputFile:
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        return False

    def read(self) -> bytes:
        return self._payload


class _FakeS3FS:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.raise_on_get = False
        self.raise_on_delete = False
        self.fail_get_remaining = 0
        self.fail_delete_remaining = 0
        self.fail_open_output_remaining = 0
        self.fail_open_input_remaining = 0

    def get_file_info(self, target: Any) -> Any:
        if self.raise_on_get:
            raise RuntimeError("boom")
        if self.fail_get_remaining > 0:
            self.fail_get_remaining -= 1
            raise RuntimeError("503 Service Unavailable")

        if hasattr(target, "base_dir"):
            base = target.base_dir
            out: list[_FakeFileInfo] = []
            for path in sorted(self.objects.keys()):
                if path.startswith(base):
                    out.append(_FakeFileInfo(type=pa_fs.FileType.File, path=path))
            return out

        path = str(target)
        if path in self.objects:
            return _FakeFileInfo(type=pa_fs.FileType.File, path=path)
        return _FakeFileInfo(type=pa_fs.FileType.NotFound, path=path)

    def delete_file(self, path: str) -> None:
        if self.raise_on_delete:
            raise RuntimeError("delete failed")
        if self.fail_delete_remaining > 0:
            self.fail_delete_remaining -= 1
            raise RuntimeError("503 Service Unavailable")
        self.objects.pop(path, None)

    def open_output_stream(self, path: str) -> _FakeOutputStream:
        if self.fail_open_output_remaining > 0:
            self.fail_open_output_remaining -= 1
            raise RuntimeError("503 Service Unavailable")
        return _FakeOutputStream(self.objects, path)

    def open_input_file(self, path: str) -> _FakeInputFile:
        if self.fail_open_input_remaining > 0:
            self.fail_open_input_remaining -= 1
            raise RuntimeError("503 Service Unavailable")
        if path not in self.objects:
            raise FileNotFoundError(path)
        return _FakeInputFile(self.objects[path])


def test_local_chunk_storage_write_list_exists_delete(tmp_path: Path) -> None:
    storage = LocalChunkStorage(tmp_path / "chunks")
    key = "Mint/chunk_0000000000_0000000001.parquet"
    table = pa.table({"a": [1]})

    storage.write_table(key, table, "lz4")

    assert storage.exists(key) is True
    assert key in storage.list_keys("Mint/")

    storage.delete(key)
    assert storage.exists(key) is False

    # idempotent
    storage.delete(key)
    assert storage.exists(key) is False


def test_local_chunk_storage_write_and_read_json(tmp_path: Path) -> None:
    storage = LocalChunkStorage(tmp_path / "chunks")
    key = "_meta/coverage_index.json"
    payload = {"version": 1, "done_chunks": [[0, 9]]}

    storage.write_json(key, payload)
    assert storage.read_json(key) == payload


def test_local_chunk_storage_create_json_if_absent(tmp_path: Path) -> None:
    storage = LocalChunkStorage(tmp_path / "chunks")
    key = "_meta/writer.lock.json"
    payload = {"owner_id": "a"}

    assert storage.create_json_if_absent(key, payload) is True
    assert storage.create_json_if_absent(key, payload) is False
    assert storage.read_json(key) == payload


def test_local_chunk_storage_read_json_invalid_returns_none(tmp_path: Path) -> None:
    storage = LocalChunkStorage(tmp_path / "chunks")
    p = (tmp_path / "chunks" / "_meta" / "coverage_index.json")
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text("{broken", encoding="utf-8")

    assert storage.read_json("_meta/coverage_index.json") is None


def test_s3_chunk_storage_write_table_delegates_to_pyarrow() -> None:
    fake_fs = _FakeS3FS()
    with patch("defind.storage.s3.pa_fs.S3FileSystem", return_value=fake_fs), patch(
        "defind.storage.s3.pq.write_table"
    ) as write_table_mock:
        storage = S3ChunkStorage(bucket="bucket", prefix="proto/contract", max_retries=0, retry_backoff_s=0.0)
        table = pa.table({"a": [1]})
        storage.write_table("Mint/chunk_0000000000_0000000001.parquet", table, "lz4")

    args, kwargs = write_table_mock.call_args
    assert args[1] == "bucket/proto/contract/Mint/chunk_0000000000_0000000001.parquet"
    assert kwargs["compression"] == "lz4"
    assert kwargs["filesystem"] is fake_fs


def test_s3_chunk_storage_exists_and_list_keys() -> None:
    fake_fs = _FakeS3FS()
    fake_fs.objects["bucket/proto/contract/Mint/chunk_0000000000_0000000001.parquet"] = b"x"
    fake_fs.objects["bucket/proto/contract/Mint/notes.txt"] = b"x"
    with patch("defind.storage.s3.pa_fs.S3FileSystem", return_value=fake_fs):
        storage = S3ChunkStorage(bucket="bucket", prefix="proto/contract", max_retries=0, retry_backoff_s=0.0)

    assert storage.exists("Mint/chunk_0000000000_0000000001.parquet") is True
    assert storage.exists("Mint/chunk_9999999999_9999999999.parquet") is False
    assert storage.list_keys("Mint/") == ["Mint/chunk_0000000000_0000000001.parquet"]


def test_s3_chunk_storage_exists_and_list_fail_loud_on_fs_error() -> None:
    fake_fs = _FakeS3FS()
    fake_fs.raise_on_get = True
    with patch("defind.storage.s3.pa_fs.S3FileSystem", return_value=fake_fs):
        storage = S3ChunkStorage(bucket="bucket", prefix="proto/contract", max_retries=0, retry_backoff_s=0.0)

    with patch("defind.storage.s3.logger.error") as err_log:
        with pytest.raises(RuntimeError):
            storage.exists("Mint/chunk_0000000000_0000000001.parquet")
        with pytest.raises(RuntimeError):
            storage.list_keys("Mint/")
    assert err_log.call_count >= 2


def test_s3_chunk_storage_delete_is_idempotent() -> None:
    fake_fs = _FakeS3FS()
    fake_fs.objects["bucket/proto/contract/Mint/chunk_0000000000_0000000001.parquet"] = b"x"
    with patch("defind.storage.s3.pa_fs.S3FileSystem", return_value=fake_fs):
        storage = S3ChunkStorage(bucket="bucket", prefix="proto/contract", max_retries=0, retry_backoff_s=0.0)

    storage.delete("Mint/chunk_0000000000_0000000001.parquet")
    storage.delete("Mint/chunk_0000000000_0000000001.parquet")
    assert storage.exists("Mint/chunk_0000000000_0000000001.parquet") is False

    fake_fs.raise_on_delete = True
    storage.delete("Mint/chunk_0000000000_0000000001.parquet")


def test_s3_chunk_storage_write_and_read_json() -> None:
    fake_fs = _FakeS3FS()
    with patch("defind.storage.s3.pa_fs.S3FileSystem", return_value=fake_fs):
        storage = S3ChunkStorage(bucket="bucket", prefix="proto/contract", max_retries=0, retry_backoff_s=0.0)

    payload = {"version": 1, "event_names": ["Mint"]}
    storage.write_json("_meta/coverage_index.json", payload)
    assert storage.read_json("_meta/coverage_index.json") == payload

    fake_fs.objects["bucket/proto/contract/_meta/bad.json"] = b"{broken"
    assert storage.read_json("_meta/bad.json") is None

    fake_fs.objects["bucket/proto/contract/_meta/list.json"] = json.dumps([1, 2]).encode("utf-8")
    assert storage.read_json("_meta/list.json") is None


def test_s3_chunk_storage_create_json_if_absent_uses_conditional_put() -> None:
    fake_fs = _FakeS3FS()
    with patch("defind.storage.s3.pa_fs.S3FileSystem", return_value=fake_fs):
        storage = S3ChunkStorage(bucket="bucket", prefix="proto/contract", max_retries=0, retry_backoff_s=0.0)
    storage._s3_client = MagicMock()
    storage._s3_client.put_object.return_value = None

    assert storage.create_json_if_absent("_meta/writer.lock.json", {"owner_id": "a"}) is True
    storage._s3_client.put_object.assert_called_once()


def test_s3_chunk_storage_create_json_if_absent_returns_false_on_precondition_failed() -> None:
    fake_fs = _FakeS3FS()
    with patch("defind.storage.s3.pa_fs.S3FileSystem", return_value=fake_fs):
        storage = S3ChunkStorage(bucket="bucket", prefix="proto/contract", max_retries=0, retry_backoff_s=0.0)

    storage._s3_client = MagicMock()
    storage._s3_client.put_object.side_effect = RuntimeError("PreconditionFailed: 412")
    assert storage.create_json_if_absent("_meta/writer.lock.json", {"owner_id": "a"}) is False


def test_s3_chunk_storage_create_json_if_absent_raises_when_client_unavailable() -> None:
    fake_fs = _FakeS3FS()
    with patch("defind.storage.s3.pa_fs.S3FileSystem", return_value=fake_fs):
        storage = S3ChunkStorage(bucket="bucket", prefix="proto/contract", max_retries=0, retry_backoff_s=0.0)
    storage._s3_client = None

    with pytest.raises(RuntimeError, match="Atomic S3 lock requires boto3"):
        storage.create_json_if_absent("_meta/writer.lock.json", {"owner_id": "a"})


def test_s3_chunk_storage_retries_write_table_then_succeeds() -> None:
    fake_fs = _FakeS3FS()
    with (
        patch("defind.storage.s3.pa_fs.S3FileSystem", return_value=fake_fs),
        patch(
            "defind.storage.s3.pq.write_table",
            side_effect=[RuntimeError("503 Service Unavailable"), None],
        ) as write_table_mock,
        patch("defind.storage.s3.time.sleep") as sleep_mock,
    ):
        storage = S3ChunkStorage(bucket="bucket", prefix="proto/contract", max_retries=1, retry_backoff_s=0.01)
        storage.write_table("Mint/chunk_0000000000_0000000001.parquet", pa.table({"a": [1]}), "lz4")

    assert write_table_mock.call_count == 2
    sleep_mock.assert_called_once()


def test_s3_chunk_storage_retries_exists_then_succeeds() -> None:
    fake_fs = _FakeS3FS()
    fake_fs.fail_get_remaining = 1
    fake_fs.objects["bucket/proto/contract/Mint/chunk_0000000000_0000000001.parquet"] = b"x"
    with (
        patch("defind.storage.s3.pa_fs.S3FileSystem", return_value=fake_fs),
        patch("defind.storage.s3.time.sleep") as sleep_mock,
    ):
        storage = S3ChunkStorage(bucket="bucket", prefix="proto/contract", max_retries=1, retry_backoff_s=0.01)
        assert storage.exists("Mint/chunk_0000000000_0000000001.parquet") is True
    sleep_mock.assert_called_once()
