# mypy: disable-error-code="import-untyped,import-not-found"
from __future__ import annotations

import json
import random
import time
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from typing import Any, TypeVar, cast

import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import fs as pa_fs

from defind.core.interfaces import IChunkStorage
from defind.observability import get_logger

try:
    import boto3
    from botocore.exceptions import ClientError
except Exception:  # pragma: no cover - optional runtime dependency
    boto3 = None
    ClientError = Exception

logger = get_logger(__name__)
T = TypeVar("T")
_MAX_MULTIPART_PAGES = 1_000
_RETRYABLE_MARKERS = (
    "timeout",
    "timed out",
    "connection reset",
    "connection aborted",
    "network is unreachable",
    "temporarily unavailable",
    "service unavailable",
    "slow down",
    "throttl",
    "too many requests",
    "internalerror",
    "503",
    "500",
    "429",
)
_NOT_FOUND_MARKERS = (
    "not found",
    "no such file",
    "does not exist",
    "404",
)
_NO_SUCH_UPLOAD_MARKERS = (
    "nosuchupload",
    "no such upload",
    "upload not found",
)


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
        endpoint_url: str | None = None,
        access_key: str | None = None,
        secret_key: str | None = None,
        region: str = "auto",
        max_retries: int = 3,
        retry_backoff_s: float = 0.5,
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
        self._max_retries = max(0, int(max_retries))
        self._retry_backoff_s = max(0.0, float(retry_backoff_s))
        self._s3_client: Any | None = None

        fs_kwargs: dict[str, Any] = {"region": region}
        if endpoint_url:
            fs_kwargs["endpoint_override"] = endpoint_url
        if access_key and secret_key:
            fs_kwargs["access_key"] = access_key
            fs_kwargs["secret_key"] = secret_key

        self._fs = pa_fs.S3FileSystem(**fs_kwargs)
        if boto3 is not None:
            client_kwargs: dict[str, Any] = {}
            if endpoint_url:
                client_kwargs["endpoint_url"] = endpoint_url
            if access_key and secret_key:
                client_kwargs["aws_access_key_id"] = access_key
                client_kwargs["aws_secret_access_key"] = secret_key
            if region and region != "auto":
                client_kwargs["region_name"] = region
            try:
                self._s3_client = boto3.client("s3", **client_kwargs)
            except Exception:
                self._s3_client = None

    def _is_retryable_exception(self, exc: Exception) -> bool:
        if self._is_not_found_exception(exc):
            return False
        msg = str(exc).lower()
        if any(marker in msg for marker in _RETRYABLE_MARKERS):
            return True
        if isinstance(exc, (TimeoutError, OSError)):
            return True
        arrow_io_error = getattr(pa, "ArrowIOError", None)
        if arrow_io_error is not None and isinstance(exc, arrow_io_error):
            return True
        return False

    @staticmethod
    def _is_not_found_exception(exc: Exception) -> bool:
        msg = str(exc).lower()
        return any(marker in msg for marker in _NOT_FOUND_MARKERS)

    @staticmethod
    def _is_precondition_failed_exception(exc: Exception) -> bool:
        if isinstance(exc, ClientError) and hasattr(exc, "response"):
            code = str(exc.response.get("Error", {}).get("Code", ""))
            status = int(exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0))
            if code in ("PreconditionFailed", "412") or status == 412:
                return True
        msg = str(exc).lower()
        return ("precondition" in msg and "failed" in msg) or "412" in msg

    def _with_retries(self, op_name: str, fn: Callable[[], T]) -> T:
        attempt = 0
        delay = self._retry_backoff_s
        while True:
            try:
                return fn()
            except Exception as exc:
                if (attempt >= self._max_retries) or (not self._is_retryable_exception(exc)):
                    raise
                logger.warning(
                    "s3_retry op=%s attempt=%d/%d err=%s retry_in_s=%.2f",
                    op_name,
                    attempt + 1,
                    self._max_retries + 1,
                    exc,
                    delay,
                    extra={
                        "op": op_name,
                        "attempt": attempt + 1,
                        "max_attempts": self._max_retries + 1,
                        "error": str(exc),
                        "retry_in_s": delay,
                    },
                )
                if delay > 0:
                    sleep_for = random.uniform(delay, min(delay * 1.25, 8.0))
                    time.sleep(sleep_for)
                    delay = min(delay * 2, 8.0)
                attempt += 1

    def _s3_path(self, key: str) -> str:
        return f"{self.bucket}/{self.prefix}{key}"

    def _require_s3_client(self) -> Any:
        if self._s3_client is None:
            raise RuntimeError("S3 operation requires boto3/botocore client support; install boto3 for this backend")
        return self._s3_client

    def supports_atomic_locks(self) -> bool:
        return self._s3_client is not None

    def _normalize_relative_key(self, key: str) -> str:
        normalized = key.lstrip("/")
        if self.prefix and normalized.startswith(self.prefix):
            return normalized.removeprefix(self.prefix)
        return normalized

    @staticmethod
    def _is_no_such_upload_exception(exc: Exception) -> bool:
        if isinstance(exc, ClientError) and hasattr(exc, "response"):
            code = str(exc.response.get("Error", {}).get("Code", ""))
            status = int(exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0))
            if code in ("NoSuchUpload", "404") or status == 404:
                return True
        msg = str(exc).lower()
        return any(marker in msg for marker in _NO_SUCH_UPLOAD_MARKERS)

    @staticmethod
    def _to_utc(dt: datetime) -> datetime:
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    @staticmethod
    def _dt_to_rfc3339(dt: datetime) -> str:
        return S3ChunkStorage._to_utc(dt).isoformat().replace("+00:00", "Z")

    def write_table(self, key: str, table: pa.Table, codec: str) -> None:
        s3_path = self._s3_path(key)
        self._with_retries(
            "write_table",
            lambda: pq.write_table(table, s3_path, compression=codec, filesystem=self._fs),
        )
        logger.debug(
            "s3_chunk_written path=s3://%s rows=%d",
            s3_path,
            len(table),
            extra={"path": f"s3://{s3_path}", "rows": len(table)},
        )

    def exists(self, key: str) -> bool:
        s3_path = self._s3_path(key)
        try:
            info = self._with_retries("exists", lambda: self._fs.get_file_info(s3_path))
            return bool(info.type == pa_fs.FileType.File)
        except Exception as exc:
            if self._is_not_found_exception(exc):
                return False
            logger.error(
                "s3_exists_failed path=s3://%s err=%s",
                s3_path,
                exc,
                extra={"path": f"s3://{s3_path}", "error": str(exc)},
                exc_info=True,
            )
            raise

    def list_keys(self, prefix: str) -> list[str]:
        """List all file keys under `self.prefix + prefix`."""
        s3_prefix_path = f"{self.bucket}/{self.prefix}{prefix}"
        selector = pa_fs.FileSelector(s3_prefix_path, recursive=True)
        try:
            file_infos = self._with_retries("list_keys", lambda: self._fs.get_file_info(selector))
        except Exception as exc:
            if self._is_not_found_exception(exc):
                return []
            logger.error(
                "s3_list_keys_failed path=s3://%s err=%s",
                s3_prefix_path,
                exc,
                extra={"path": f"s3://{s3_prefix_path}", "error": str(exc)},
                exc_info=True,
            )
            raise

        keys = []
        full_prefix = f"{self.bucket}/{self.prefix}"
        for info in file_infos:
            if info.type == pa_fs.FileType.File:
                # Strip bucket/prefix to get the relative key
                rel = info.path.removeprefix(full_prefix)
                keys.append(rel)
        return keys

    def delete(self, key: str) -> None:
        s3_path = self._s3_path(key)
        try:
            self._with_retries("delete", lambda: self._fs.delete_file(s3_path))
        except Exception as exc:
            logger.warning(
                "s3_delete_failed path=s3://%s err=%s",
                s3_path,
                exc,
                extra={"path": f"s3://{s3_path}", "error": str(exc)},
            )

    def write_json(self, key: str, payload: dict[str, Any]) -> None:
        s3_path = self._s3_path(key)
        raw = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")

        def _do_write() -> None:
            with self._fs.open_output_stream(s3_path) as out:
                out.write(raw)

        self._with_retries("write_json", _do_write)

    def write_text(self, key: str, payload: str) -> None:
        s3_path = self._s3_path(key)
        raw = payload.encode("utf-8")

        def _do_write() -> None:
            with self._fs.open_output_stream(s3_path) as out:
                out.write(raw)

        self._with_retries("write_text", _do_write)

    def create_json_if_absent(self, key: str, payload: dict[str, Any]) -> bool:
        client = self._s3_client
        if client is None:
            raise RuntimeError(
                "Atomic S3 lock requires boto3/botocore client support; "
                "install boto3 or disable single_writer_guard for this backend"
            )

        raw = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
        s3_key = f"{self.prefix}{key}"

        def _put_conditional() -> None:
            client.put_object(
                Bucket=self.bucket,
                Key=s3_key,
                Body=raw,
                ContentType="application/json",
                IfNoneMatch="*",
            )

        try:
            self._with_retries("create_json_if_absent", _put_conditional)
            return True
        except Exception as exc:
            if self._is_precondition_failed_exception(exc):
                return False
            raise

    def read_json_with_version(self, key: str) -> tuple[dict[str, Any] | None, str | None]:
        client = self._require_s3_client()
        s3_key = f"{self.prefix}{key}"

        def _get_object() -> dict[str, Any]:
            return cast(dict[str, Any], client.get_object(Bucket=self.bucket, Key=s3_key))

        try:
            response = self._with_retries("read_json_with_version", _get_object)
        except Exception as exc:
            if self._is_not_found_exception(exc):
                return None, None
            logger.warning(
                "s3_read_json_with_version_failed path=s3://%s err=%s",
                f"{self.bucket}/{s3_key}",
                exc,
                extra={"path": f"s3://{self.bucket}/{s3_key}", "error": str(exc)},
            )
            raise

        body = response.get("Body")
        etag = cast(str | None, response.get("ETag"))
        if body is None:
            return None, etag
        raw = cast(bytes, body.read())
        try:
            data = json.loads(raw.decode("utf-8"))
        except Exception:
            return None, etag
        return (cast(dict[str, Any], data) if isinstance(data, dict) else None), etag

    def write_json_if_version(self, key: str, payload: dict[str, Any], expected_version: str | None) -> bool:
        client = self._require_s3_client()
        raw = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
        s3_key = f"{self.prefix}{key}"

        def _put_conditional() -> None:
            kwargs: dict[str, Any] = {
                "Bucket": self.bucket,
                "Key": s3_key,
                "Body": raw,
                "ContentType": "application/json",
            }
            if expected_version is None:
                kwargs["IfNoneMatch"] = "*"
            else:
                kwargs["IfMatch"] = expected_version
            client.put_object(**kwargs)

        try:
            self._with_retries("write_json_if_version", _put_conditional)
            return True
        except Exception as exc:
            if self._is_precondition_failed_exception(exc):
                return False
            raise

    def list_incomplete_multipart_uploads(
        self,
        *,
        key_prefix: str = "",
        older_than_s: int = 0,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """List ongoing multipart uploads for this storage prefix.

        Parameters
        ----------
        key_prefix:
            Optional relative prefix under this dataset (ex: "CollectProtocol/").
        older_than_s:
            If > 0, only keep uploads initiated at least this many seconds ago.
        limit:
            Optional max number of uploads returned.
        """
        if older_than_s < 0:
            raise ValueError("older_than_s must be >= 0")
        if limit is not None and limit <= 0:
            raise ValueError("limit must be > 0 when provided")

        client = self._require_s3_client()
        rel_prefix = key_prefix.lstrip("/")
        s3_prefix = f"{self.prefix}{rel_prefix}"
        cutoff_dt = datetime.now(timezone.utc) - timedelta(seconds=older_than_s)

        uploads: list[dict[str, Any]] = []
        list_kwargs: dict[str, Any] = {"Bucket": self.bucket, "Prefix": s3_prefix}
        seen_markers: set[tuple[str, str]] = set()
        page_count = 0

        while True:
            page_count += 1
            if page_count > _MAX_MULTIPART_PAGES:
                raise RuntimeError(f"multipart upload pagination exceeded max pages ({_MAX_MULTIPART_PAGES})")
            marker = (
                str(list_kwargs.get("KeyMarker") or ""),
                str(list_kwargs.get("UploadIdMarker") or ""),
            )
            if marker in seen_markers:
                raise RuntimeError("multipart upload pagination entered a marker loop")
            seen_markers.add(marker)
            request_kwargs = list_kwargs.copy()

            def _list_page(kwargs: dict[str, Any] = request_kwargs) -> dict[str, Any]:
                return cast(dict[str, Any], client.list_multipart_uploads(**kwargs))

            page = self._with_retries(
                "list_multipart_uploads",
                _list_page,
            )
            for upload in page.get("Uploads", []) or []:
                key_raw = str(upload.get("Key", "")).strip()
                upload_id = str(upload.get("UploadId", "")).strip()
                initiated_raw = upload.get("Initiated")
                initiated_dt = initiated_raw if isinstance(initiated_raw, datetime) else None
                if initiated_dt is not None:
                    initiated_dt = self._to_utc(initiated_dt)
                if older_than_s > 0 and initiated_dt is not None and initiated_dt > cutoff_dt:
                    continue
                if not key_raw or not upload_id:
                    continue
                rel_key = self._normalize_relative_key(key_raw)
                item: dict[str, Any] = {
                    "key": rel_key,
                    "upload_id": upload_id,
                }
                if initiated_dt is not None:
                    item["initiated"] = self._dt_to_rfc3339(initiated_dt)
                    item["age_s"] = max(
                        0,
                        int((datetime.now(timezone.utc) - initiated_dt).total_seconds()),
                    )
                uploads.append(item)
                if limit is not None and len(uploads) >= limit:
                    return uploads

            if not page.get("IsTruncated"):
                break
            next_key_marker = page.get("NextKeyMarker")
            next_upload_marker = page.get("NextUploadIdMarker")
            if not next_key_marker:
                break
            list_kwargs["KeyMarker"] = next_key_marker
            if next_upload_marker:
                list_kwargs["UploadIdMarker"] = next_upload_marker
            else:
                list_kwargs.pop("UploadIdMarker", None)

        return uploads

    def abort_incomplete_multipart_upload(self, *, key: str, upload_id: str) -> bool:
        """Abort one multipart upload.

        Returns False when the upload is already gone (idempotent behavior).
        """
        client = self._require_s3_client()
        rel_key = self._normalize_relative_key(key)
        s3_key = f"{self.prefix}{rel_key}"

        def _do_abort() -> None:
            client.abort_multipart_upload(
                Bucket=self.bucket,
                Key=s3_key,
                UploadId=upload_id,
            )

        try:
            self._with_retries("abort_multipart_upload", _do_abort)
            logger.info(
                "s3_multipart_upload_aborted",
                extra={"bucket": self.bucket, "key": rel_key, "upload_id": upload_id},
            )
            return True
        except Exception as exc:
            if self._is_no_such_upload_exception(exc):
                return False
            raise

    def cleanup_incomplete_multipart_uploads(
        self,
        *,
        key_prefix: str = "",
        older_than_s: int = 0,
        dry_run: bool = True,
        limit: int | None = None,
    ) -> dict[str, Any]:
        """List and optionally abort incomplete multipart uploads."""
        uploads = self.list_incomplete_multipart_uploads(
            key_prefix=key_prefix,
            older_than_s=older_than_s,
            limit=limit,
        )
        aborted = 0
        failed = 0
        if not dry_run:
            for item in uploads:
                try:
                    if self.abort_incomplete_multipart_upload(key=item["key"], upload_id=item["upload_id"]):
                        aborted += 1
                except Exception as exc:
                    failed += 1
                    logger.warning(
                        "s3_multipart_upload_abort_failed",
                        extra={
                            "bucket": self.bucket,
                            "key": item.get("key"),
                            "upload_id": item.get("upload_id"),
                            "error": str(exc),
                        },
                    )
        summary = {
            "dry_run": dry_run,
            "found": len(uploads),
            "aborted": aborted,
            "failed": failed,
            "uploads": uploads,
        }
        logger.info(
            "s3_multipart_cleanup_summary",
            extra={
                "dry_run": dry_run,
                "found": len(uploads),
                "aborted": aborted,
                "failed": failed,
                "key_prefix": key_prefix,
            },
        )
        return summary

    def read_json(self, key: str) -> dict[str, Any] | None:
        s3_path = self._s3_path(key)
        try:
            info = self._with_retries("read_json_info", lambda: self._fs.get_file_info(s3_path))
        except Exception as exc:
            if self._is_not_found_exception(exc):
                return None
            logger.warning(
                "s3_read_json_info_failed path=s3://%s err=%s",
                s3_path,
                exc,
                extra={"path": f"s3://{s3_path}", "error": str(exc)},
            )
            raise
        if info.type != pa_fs.FileType.File:
            return None

        def _read_raw() -> bytes:
            with self._fs.open_input_file(s3_path) as inp:
                return cast(bytes, inp.read())

        try:
            raw = self._with_retries("read_json", _read_raw)
        except Exception as exc:
            if self._is_not_found_exception(exc):
                return None
            logger.warning(
                "s3_read_json_failed path=s3://%s err=%s",
                s3_path,
                exc,
                extra={"path": f"s3://{s3_path}", "error": str(exc)},
            )
            raise
        try:
            data = json.loads(raw.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            logger.warning(
                "s3_read_json_invalid path=s3://%s",
                s3_path,
                extra={"path": f"s3://{s3_path}"},
            )
            return None
        return cast(dict[str, Any], data) if isinstance(data, dict) else None

    def read_text(self, key: str) -> str | None:
        s3_path = self._s3_path(key)
        try:
            info = self._with_retries("read_text_info", lambda: self._fs.get_file_info(s3_path))
        except Exception as exc:
            if self._is_not_found_exception(exc):
                return None
            logger.warning(
                "s3_read_text_info_failed path=s3://%s err=%s",
                s3_path,
                exc,
                extra={"path": f"s3://{s3_path}", "error": str(exc)},
            )
            raise
        if info.type != pa_fs.FileType.File:
            return None

        def _read_raw() -> bytes:
            with self._fs.open_input_file(s3_path) as inp:
                return cast(bytes, inp.read())

        try:
            raw = self._with_retries("read_text", _read_raw)
        except Exception as exc:
            if self._is_not_found_exception(exc):
                return None
            logger.warning(
                "s3_read_text_failed path=s3://%s err=%s",
                s3_path,
                exc,
                extra={"path": f"s3://{s3_path}", "error": str(exc)},
            )
            raise
        try:
            return raw.decode("utf-8")
        except UnicodeDecodeError:
            logger.warning(
                "s3_read_text_invalid_encoding path=s3://%s",
                s3_path,
                extra={"path": f"s3://{s3_path}"},
            )
            return None

    def is_valid_parquet(self, key: str) -> bool:
        s3_path = self._s3_path(key)
        try:
            info = self._with_retries("parquet_info", lambda: self._fs.get_file_info(s3_path))
            if info.type != pa_fs.FileType.File:
                return False
            pq.read_metadata(s3_path, filesystem=self._fs)
            return True
        except Exception as exc:
            if self._is_not_found_exception(exc):
                return False
            logger.warning(
                "s3_parquet_validation_failed path=s3://%s err=%s",
                s3_path,
                exc,
                extra={"path": f"s3://{s3_path}", "error": str(exc)},
            )
            return False
