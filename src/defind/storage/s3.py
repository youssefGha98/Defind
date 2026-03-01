from __future__ import annotations

from typing import Optional

import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import fs as pa_fs

from defind.core.interfaces import IChunkStorage


class S3ChunkStorage(IChunkStorage):
    """S3-compatible implementation of IChunkStorage.

    Works with AWS S3, Cloudflare R2, Backblaze B2, MinIO, and any
    S3-compatible service. Uses PyArrow's native S3 filesystem for
    efficient Parquet I/O without loading data through Python.

    Keys are relative paths under the configured prefix, e.g.:
        "Mint/chunk_0012376729_0012381729.parquet"
        → s3://{bucket}/{prefix}Mint/chunk_0012376729_0012381729.parquet

    Note: S3 does not support atomic rename. Each write_table call is a
    direct PUT operation. The chunk done-check (all event files present)
    provides the necessary consistency guarantee.
    """

    def __init__(
        self,
        *,
        bucket: str,
        prefix: str = "",
        endpoint_url: Optional[str] = None,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        region: str = "auto",
    ) -> None:
        """
        Parameters
        ----------
        bucket : str
            S3 bucket name.
        prefix : str, optional
            Key prefix for all chunk files (e.g., 'uniswap/usdc_weth/').
            Normalized to end with '/' if not empty.
        endpoint_url : str, optional
            S3 endpoint URL. Required for R2, B2, MinIO.
            Example: 'https://<account_id>.r2.cloudflarestorage.com'
        access_key : str, optional
            AWS access key ID or equivalent.
        secret_key : str, optional
            AWS secret access key or equivalent.
        region : str, optional
            AWS region. Use 'auto' for R2/MinIO.
        """
        self.bucket = bucket
        self.prefix = prefix.rstrip("/") + "/" if prefix else ""

        fs_kwargs: dict = {"region": region}
        if endpoint_url:
            fs_kwargs["endpoint_override"] = endpoint_url
        if access_key and secret_key:
            fs_kwargs["access_key"] = access_key
            fs_kwargs["secret_key"] = secret_key

        self._fs = pa_fs.S3FileSystem(**fs_kwargs)

    def _s3_path(self, key: str) -> str:
        return f"{self.bucket}/{self.prefix}{key}"

    def write_table(self, key: str, table: pa.Table, codec: str) -> None:
        s3_path = self._s3_path(key)
        pq.write_table(table, s3_path, compression=codec, filesystem=self._fs)
        print(f"wrote s3://{s3_path}  (rows={len(table)})")

    def exists(self, key: str) -> bool:
        s3_path = self._s3_path(key)
        try:
            info = self._fs.get_file_info(s3_path)
            return info.type == pa_fs.FileType.File
        except Exception:
            return False

    def list_keys(self, prefix: str) -> list[str]:
        """List all .parquet keys under `self.prefix + prefix`."""
        s3_prefix_path = f"{self.bucket}/{self.prefix}{prefix}"
        selector = pa_fs.FileSelector(s3_prefix_path, recursive=True)
        try:
            file_infos = self._fs.get_file_info(selector)
        except Exception:
            return []

        keys = []
        full_prefix = f"{self.bucket}/{self.prefix}"
        for info in file_infos:
            if info.type == pa_fs.FileType.File and info.path.endswith(".parquet"):
                # Strip bucket/prefix to get the relative key
                rel = info.path.removeprefix(full_prefix)
                keys.append(rel)
        return keys
