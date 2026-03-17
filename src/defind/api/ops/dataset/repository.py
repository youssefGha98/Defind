from __future__ import annotations

from collections.abc import Callable
from typing import Any

from fastapi import HTTPException

from defind.api.ops.dataset.utils import (
    normalize_str_list,
    optional_meta_int,
    public_chain_head_rpc_urls,
    stored_chain_head,
)
from defind.api.ops.shared.dependencies import OpsApiDependencies
from defind.api.ops.shared.models import DatasetRef, OpsApiConfig
from defind.api.ops.shared.utils import clean_optional_str
from defind.core.dataset_state.locks import (
    looks_unsupported_lock_error,
    process_local_lock,
    read_json_with_version,
    warn_local_only_degradation,
    write_json_if_version,
)
from defind.core.interfaces import IChunkStorage
from defind.dataset_state import (
    JOBS_KEY,
    create_dataset_meta,
    discover_dataset_refs,
    list_dataset_jobs,
    update_dataset_meta,
)
from defind.orchestration.utils import load_done_chunks, load_done_chunks_from_index
from defind.storage.local import LocalChunkStorage
from defind.storage.s3 import S3ChunkStorage

DATASET_SUMMARY_KEY = "_meta/ops_summary.json"
DATASET_SUMMARY_VERSION = 1
DATASETS_INDEX_KEY = "_meta/datasets_index.json"
DATASETS_INDEX_VERSION = 1
DATASETS_ROOT_SUMMARY_KEY = "_meta/datasets_summary.json"
DATASETS_ROOT_SUMMARY_VERSION = 2
DATASET_DISPLAY_FIELDS = (
    "id",
    "protocol",
    "contract",
    "contract_address",
    "chain_id",
    "start_block",
    "last_block",
    "chain_head",
    "chunks_total",
    "lag",
    "active_jobs_count",
    "status",
)


class DatasetRepository:
    def __init__(self, *, cfg: OpsApiConfig, deps: OpsApiDependencies) -> None:
        self._cfg = cfg
        self._deps = deps

    def build_dataset_storage(self, dataset: DatasetRef) -> tuple[IChunkStorage, str]:
        return self._deps.build_dataset_storage(self._cfg, dataset)

    def build_control_storage(self) -> IChunkStorage:
        return self._deps.build_control_storage(self._cfg)

    def discover_datasets(self) -> list[DatasetRef]:
        root_storage = self.build_control_storage()
        indexed = self._read_dataset_index(root_storage)
        if indexed is not None:
            return indexed
        datasets = [
            DatasetRef(protocol=protocol, contract=contract)
            for protocol, contract in discover_dataset_refs(root_storage)
        ]
        self._write_dataset_index(root_storage, datasets)
        return datasets

    def read_meta_or_404(self, dataset: DatasetRef) -> dict[str, Any]:
        storage, _ = self.build_dataset_storage(dataset)
        meta = self._deps.read_dataset_meta(storage)
        if not isinstance(meta, dict):
            raise HTTPException(status_code=404, detail="dataset not found")
        return meta

    def get_context(self, protocol: str, contract: str) -> tuple[DatasetRef, dict[str, Any], IChunkStorage]:
        from defind.api.ops.dataset.utils import dataset_from_route

        dataset = dataset_from_route(protocol, contract)
        meta = self.read_meta_or_404(dataset)
        storage, _ = self.build_dataset_storage(dataset)
        return dataset, meta, storage

    async def resolve_public_chain_head(self, chain_id: int) -> int | None:
        for rpc_url in public_chain_head_rpc_urls(chain_id):
            try:
                return int(await self._deps.fetch_rpc_chain_head(rpc_url=rpc_url))
            except Exception:
                continue
        return None

    @staticmethod
    def _base_last_block(meta: dict[str, Any]) -> int:
        start_block = int(meta.get("start_block") or 0)
        stored_last_block = int(meta.get("last_block") or start_block)
        if stored_last_block > start_block:
            return stored_last_block
        return max(0, start_block - 1)

    @staticmethod
    def _dataset_index_payload(datasets: list[DatasetRef]) -> dict[str, Any]:
        return {
            "version": DATASETS_INDEX_VERSION,
            "datasets": sorted({dataset.dataset_id for dataset in datasets}),
        }

    def _default_dataset_summary(self, meta: dict[str, Any]) -> dict[str, Any]:
        return {
            "version": DATASET_SUMMARY_VERSION,
            "last_block": self._base_last_block(meta),
            "chunks_total": 0,
            "active_jobs_count": 0,
            "status": "idle",
            "observed_chain_head": stored_chain_head(meta),
        }

    @staticmethod
    def _default_root_summary_payload() -> dict[str, Any]:
        return {
            "version": DATASETS_ROOT_SUMMARY_VERSION,
            "datasets": {},
        }

    def _normalize_dataset_summary(self, meta: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
        base = self._default_dataset_summary(meta)
        last_block = optional_meta_int(payload, "last_block")
        chunks_total = optional_meta_int(payload, "chunks_total")
        active_jobs_count = optional_meta_int(payload, "active_jobs_count")
        observed_chain_head = optional_meta_int(payload, "observed_chain_head")
        status = clean_optional_str(str(payload.get("status") or ""))
        if last_block is not None:
            base["last_block"] = last_block
        if chunks_total is not None:
            base["chunks_total"] = max(0, chunks_total)
        if active_jobs_count is not None:
            base["active_jobs_count"] = max(0, active_jobs_count)
        if observed_chain_head is not None:
            base["observed_chain_head"] = observed_chain_head
        if status is not None:
            base["status"] = status
        return base

    def _root_summary_rows_from_payload(self, payload: dict[str, Any]) -> dict[str, dict[str, Any]] | None:
        if int(payload.get("version") or 0) != DATASETS_ROOT_SUMMARY_VERSION:
            return None
        raw_datasets = payload.get("datasets")
        if not isinstance(raw_datasets, dict):
            return None
        rows: dict[str, dict[str, Any]] = {}
        for dataset_id, row in raw_datasets.items():
            normalized_id = clean_optional_str(str(dataset_id))
            if normalized_id is None or "/" not in normalized_id or not isinstance(row, dict):
                continue
            rows[normalized_id] = self._display_row_payload(
                {
                    **dict(row),
                    "id": normalized_id,
                }
            )
        return rows

    def _display_row_payload(self, row: dict[str, Any]) -> dict[str, Any]:
        return {key: row[key] for key in DATASET_DISPLAY_FIELDS if key in row}

    def _root_summary_payload(self, rows: dict[str, dict[str, Any]]) -> dict[str, Any]:
        normalized_rows: dict[str, dict[str, Any]] = {}
        for dataset_id in sorted(rows):
            row = rows[dataset_id]
            if not isinstance(row, dict):
                continue
            normalized_rows[dataset_id] = self._display_row_payload(
                {
                    **dict(row),
                    "id": dataset_id,
                }
            )
        return {
            "version": DATASETS_ROOT_SUMMARY_VERSION,
            "datasets": normalized_rows,
        }

    def _update_json_document(
        self,
        storage: IChunkStorage,
        key: str,
        *,
        default_payload: dict[str, Any],
        update_fn: Callable[[dict[str, Any]], dict[str, Any]],
        max_attempts: int = 8,
    ) -> dict[str, Any]:
        def local_only_update() -> dict[str, Any]:
            with process_local_lock(storage, key):
                current = storage.read_json(key)
                base = dict(current) if isinstance(current, dict) else dict(default_payload)
                updated = dict(update_fn(base))
                storage.write_json(key, updated)
                return updated

        for _ in range(max(1, max_attempts)):
            try:
                current, version = read_json_with_version(storage, key)
                base = dict(current) if isinstance(current, dict) else dict(default_payload)
                updated = dict(update_fn(base))
                if write_json_if_version(storage, key, updated, version):
                    return updated
            except Exception as exc:
                if looks_unsupported_lock_error(exc):
                    warn_local_only_degradation(storage, key, reason=str(exc))
                    return local_only_update()
                raise
        raise RuntimeError(f"json update conflict for {key}")

    def _read_dataset_index(self, storage: IChunkStorage) -> list[DatasetRef] | None:
        payload = storage.read_json(DATASETS_INDEX_KEY)
        if not isinstance(payload, dict):
            return None
        return self._dataset_refs_from_index_payload(payload)

    def _read_root_summary_rows(self, storage: IChunkStorage | None = None) -> dict[str, dict[str, Any]] | None:
        resolved_storage = storage or self.build_control_storage()
        payload = resolved_storage.read_json(DATASETS_ROOT_SUMMARY_KEY)
        if not isinstance(payload, dict):
            return None
        return self._root_summary_rows_from_payload(payload)

    def _dataset_refs_from_index_payload(self, payload: dict[str, Any]) -> list[DatasetRef] | None:
        if int(payload.get("version") or 0) != DATASETS_INDEX_VERSION:
            return None
        raw_datasets = payload.get("datasets")
        if not isinstance(raw_datasets, list):
            return None
        refs: list[DatasetRef] = []
        seen: set[str] = set()
        for item in raw_datasets:
            dataset_id = clean_optional_str(str(item))
            if dataset_id is None or "/" not in dataset_id:
                continue
            protocol, contract = dataset_id.split("/", 1)
            ref = DatasetRef(protocol=protocol, contract=contract)
            if ref.dataset_id in seen:
                continue
            refs.append(ref)
            seen.add(ref.dataset_id)
        return sorted(refs, key=lambda ref: ref.dataset_id)

    def _write_dataset_index(self, storage: IChunkStorage, datasets: list[DatasetRef]) -> dict[str, Any]:
        payload = self._dataset_index_payload(datasets)
        storage.write_json(DATASETS_INDEX_KEY, payload)
        return payload

    def _write_root_summary_rows(
        self,
        storage: IChunkStorage,
        rows: dict[str, dict[str, Any]],
    ) -> dict[str, Any]:
        payload = self._root_summary_payload(rows)
        storage.write_json(DATASETS_ROOT_SUMMARY_KEY, payload)
        return payload

    def _update_root_summary(
        self,
        update_fn: Callable[[dict[str, dict[str, Any]]], dict[str, dict[str, Any]]],
    ) -> dict[str, Any]:
        storage = self.build_control_storage()
        return self._update_json_document(
            storage,
            DATASETS_ROOT_SUMMARY_KEY,
            default_payload=self._default_root_summary_payload(),
            update_fn=lambda current: self._root_summary_payload(
                update_fn(self._root_summary_rows_from_payload(current) or {})
            ),
        )

    def _upsert_root_summary_row(self, row: dict[str, Any]) -> dict[str, Any]:
        dataset_id = clean_optional_str(str(row.get("id") or ""))
        if dataset_id is None:
            raise ValueError("dataset row is missing id")
        row_payload = {
            **dict(row),
            "id": dataset_id,
        }
        return self._update_root_summary(
            lambda current: {
                **current,
                dataset_id: row_payload,
            }
        )

    def _bootstrap_root_summary_rows(self) -> dict[str, dict[str, Any]]:
        rows: dict[str, dict[str, Any]] = {}
        for dataset in self.discover_datasets():
            meta = self.read_meta_or_404(dataset)
            row = self.build_dataset_row(dataset, meta)
            rows[dataset.dataset_id] = row
        storage = self.build_control_storage()
        self._write_root_summary_rows(storage, rows)
        return rows

    def _register_dataset(self, dataset: DatasetRef) -> dict[str, Any]:
        storage = self.build_control_storage()
        return self._update_json_document(
            storage,
            DATASETS_INDEX_KEY,
            default_payload=self._dataset_index_payload([]),
            update_fn=lambda current: self._dataset_index_payload(
                [
                    *(self._dataset_refs_from_index_payload(current) or []),
                    dataset,
                ]
            ),
        )

    def list_cached_dataset_rows(
        self,
        *,
        protocol_slug: str | None = None,
        contract_slug: str | None = None,
    ) -> list[dict[str, Any]]:
        rows_by_id = self._read_root_summary_rows()
        if rows_by_id is None:
            rows_by_id = self._bootstrap_root_summary_rows()
        rows = [dict(row) for _, row in sorted(rows_by_id.items())]
        if protocol_slug is not None:
            rows = [row for row in rows if str(row.get("protocol") or "") == protocol_slug.strip()]
        if contract_slug is not None:
            rows = [row for row in rows if str(row.get("contract") or "") == contract_slug.strip()]
        return rows

    def get_cached_dataset_row(self, dataset: DatasetRef) -> dict[str, Any] | None:
        rows_by_id = self._read_root_summary_rows()
        if rows_by_id is None:
            return None
        row = rows_by_id.get(dataset.dataset_id)
        if not isinstance(row, dict):
            return None
        return dict(row)

    def ensure_dataset_known_or_404(self, dataset: DatasetRef) -> None:
        if self.get_cached_dataset_row(dataset) is not None:
            return
        indexed = self._read_dataset_index(self.build_control_storage())
        if indexed is None:
            indexed = self.discover_datasets()
        if any(ref.dataset_id == dataset.dataset_id for ref in indexed):
            return
        raise HTTPException(status_code=404, detail="dataset not found")

    def refresh_cached_dataset_row(
        self,
        dataset: DatasetRef,
        *,
        meta: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        resolved_meta = meta if meta is not None else self.read_meta_or_404(dataset)
        row = self.build_dataset_row(dataset, resolved_meta)
        self._upsert_root_summary_row(row)
        return row

    def _read_dataset_summary(self, storage: IChunkStorage, meta: dict[str, Any]) -> dict[str, Any] | None:
        payload = storage.read_json(DATASET_SUMMARY_KEY)
        if not isinstance(payload, dict):
            return None
        if int(payload.get("version") or 0) != DATASET_SUMMARY_VERSION:
            return None
        return self._normalize_dataset_summary(meta, payload)

    def _bootstrap_dataset_summary(
        self,
        dataset: DatasetRef,
        meta: dict[str, Any],
        storage: IChunkStorage,
    ) -> dict[str, Any]:
        summary = self._default_dataset_summary(meta)
        event_names = normalize_str_list(meta.get("event_names"))
        done_chunks: list[tuple[int, int]] | None = None
        if event_names:
            done_chunks = load_done_chunks_from_index(storage, event_names)
            if done_chunks is None:
                done_chunks = load_done_chunks(storage, event_names)
        if done_chunks:
            summary["chunks_total"] = len(done_chunks)
            summary["last_block"] = max(summary["last_block"], max(end for _, end in done_chunks))
        jobs = list_dataset_jobs(storage)
        if jobs:
            latest = jobs[0]
            summary["status"] = clean_optional_str(str(latest.get("status") or "")) or "idle"
            summary["active_jobs_count"] = sum(1 for row in jobs if str(row.get("status") or "") == "running")
        return self._update_dataset_summary(dataset, meta=meta, update_fn=lambda _: summary)

    def _update_dataset_summary(
        self,
        dataset: DatasetRef,
        *,
        meta: dict[str, Any] | None = None,
        update_fn: Callable[[dict[str, Any]], dict[str, Any]],
    ) -> dict[str, Any]:
        storage, _ = self.build_dataset_storage(dataset)
        resolved_meta = meta if meta is not None else self.read_meta_or_404(dataset)
        default_summary = self._default_dataset_summary(resolved_meta)
        return self._update_json_document(
            storage,
            DATASET_SUMMARY_KEY,
            default_payload=default_summary,
            update_fn=lambda current: self._normalize_dataset_summary(
                resolved_meta,
                update_fn(self._normalize_dataset_summary(resolved_meta, current)),
            ),
        )

    def _ensure_dataset_summary(
        self,
        dataset: DatasetRef,
        meta: dict[str, Any],
        storage: IChunkStorage,
    ) -> dict[str, Any]:
        summary = self._read_dataset_summary(storage, meta)
        if summary is not None:
            return summary
        return self._bootstrap_dataset_summary(dataset, meta, storage)

    def sync_summary_with_meta(self, dataset: DatasetRef, meta: dict[str, Any]) -> dict[str, Any]:
        base_last_block = self._base_last_block(meta)
        observed_chain_head = stored_chain_head(meta)

        def _update(current: dict[str, Any]) -> dict[str, Any]:
            updated = dict(current)
            updated["last_block"] = base_last_block
            if observed_chain_head is not None:
                updated["observed_chain_head"] = observed_chain_head
            return updated

        summary = self._update_dataset_summary(dataset, meta=meta, update_fn=_update)
        self.refresh_cached_dataset_row(dataset, meta=meta)
        return summary

    def record_observed_chain_head(self, dataset: DatasetRef, chain_head: int) -> dict[str, Any]:
        storage, _ = self.build_dataset_storage(dataset)
        updated = update_dataset_meta(
            storage,
            lambda meta: {
                **meta,
                "observed_chain_head": max(
                    int(chain_head),
                    stored_chain_head(meta) or 0,
                ),
            },
        )
        self.sync_summary_with_meta(dataset, updated)
        return updated

    def record_job_started(self, dataset: DatasetRef, row: dict[str, Any]) -> dict[str, Any]:
        status = clean_optional_str(str(row.get("status") or "")) or "running"
        summary = self._update_dataset_summary(
            dataset,
            update_fn=lambda current: {
                **current,
                "status": status,
                "active_jobs_count": 1,
            },
        )
        self.refresh_cached_dataset_row(dataset)
        return summary

    def record_job_terminal(self, dataset: DatasetRef, status: str) -> dict[str, Any]:
        normalized_status = clean_optional_str(status) or "idle"
        summary = self._update_dataset_summary(
            dataset,
            update_fn=lambda current: {
                **current,
                "status": normalized_status,
                "active_jobs_count": 0,
            },
        )
        self.refresh_cached_dataset_row(dataset)
        return summary

    def record_job_deleted(self, dataset: DatasetRef) -> dict[str, Any]:
        storage, _ = self.build_dataset_storage(dataset)
        jobs = list_dataset_jobs(storage)
        latest = jobs[0] if jobs else None
        status = clean_optional_str(str((latest or {}).get("status") or "")) or "idle"
        active_jobs_count = sum(1 for row in jobs if str(row.get("status") or "") == "running")
        summary = self._update_dataset_summary(
            dataset,
            update_fn=lambda current: {
                **current,
                "status": status,
                "active_jobs_count": active_jobs_count,
            },
        )
        self.refresh_cached_dataset_row(dataset)
        return summary

    def record_chunk_progress(self, dataset: DatasetRef, confirmed_to_block: int) -> dict[str, Any]:
        summary = self._update_dataset_summary(
            dataset,
            update_fn=lambda current: {
                **current,
                "last_block": max(int(confirmed_to_block), optional_meta_int(current, "last_block") or 0),
                "chunks_total": max(0, (optional_meta_int(current, "chunks_total") or 0) + 1),
                "active_jobs_count": max(1, optional_meta_int(current, "active_jobs_count") or 0),
                "status": clean_optional_str(str(current.get("status") or "")) or "running",
            },
        )
        self.refresh_cached_dataset_row(dataset)
        return summary

    def build_dataset_row(
        self,
        dataset: DatasetRef,
        meta: dict[str, Any],
        *,
        chain_head: int | None = None,
    ) -> dict[str, Any]:
        storage, _ = self.build_dataset_storage(dataset)
        summary = self._ensure_dataset_summary(dataset, meta, storage)
        start_block = int(meta.get("start_block") or 0)
        last_block = max(
            self._base_last_block(meta),
            optional_meta_int(summary, "last_block") or self._base_last_block(meta),
        )
        observed_chain_head = optional_meta_int(summary, "observed_chain_head")
        if observed_chain_head is None:
            observed_chain_head = stored_chain_head(meta)
        resolved_chain_head = max(last_block, observed_chain_head or last_block)
        if chain_head is not None:
            resolved_chain_head = max(last_block, int(chain_head))
        chunks_total = max(0, optional_meta_int(summary, "chunks_total") or 0)
        active_jobs_count = max(0, optional_meta_int(summary, "active_jobs_count") or 0)
        status = clean_optional_str(str(summary.get("status") or "")) or "idle"
        return {
            "id": dataset.dataset_id,
            "protocol": clean_optional_str(str(meta.get("protocol") or "")) or dataset.protocol,
            "contract": clean_optional_str(str(meta.get("contract") or "")) or dataset.contract,
            "contract_address": clean_optional_str(str(meta.get("contract_address") or "")) or "",
            "chain_id": int(meta.get("chain_id") or self._cfg.etherscan_chain_id),
            "start_block": start_block,
            "last_block": last_block,
            "chain_head": resolved_chain_head,
            "chunks_total": chunks_total,
            "lag": max(0, resolved_chain_head - last_block),
            "active_jobs_count": active_jobs_count,
            "status": status,
        }

    def create_dataset(self, dataset: DatasetRef, meta: dict[str, Any]) -> tuple[dict[str, Any], str]:
        storage, location = self.build_dataset_storage(dataset)
        created = create_dataset_meta(storage, meta)
        storage.write_text(JOBS_KEY, "")
        storage.write_json(DATASET_SUMMARY_KEY, self._default_dataset_summary(created))
        self._register_dataset(dataset)
        self.refresh_cached_dataset_row(dataset, meta=created)
        return created, location

    def update_meta(self, dataset: DatasetRef, payload: dict[str, Any]) -> dict[str, Any]:
        storage, _ = self.build_dataset_storage(dataset)
        updated = update_dataset_meta(storage, lambda meta: {**meta, **payload})
        self.sync_summary_with_meta(dataset, updated)
        return updated


def build_dataset_storage(cfg: OpsApiConfig, dataset: DatasetRef) -> tuple[IChunkStorage, str]:
    if cfg.s3_bucket:
        subpath = f"{dataset.protocol}/{dataset.contract}"
        prefix = f"{cfg.s3_prefix.rstrip('/')}/{subpath}/" if cfg.s3_prefix else f"{subpath}/"
        storage = S3ChunkStorage(
            bucket=cfg.s3_bucket,
            prefix=prefix,
            endpoint_url=cfg.s3_endpoint_url,
            access_key=cfg.s3_access_key,
            secret_key=cfg.s3_secret_key,
            region=cfg.s3_region,
            max_retries=cfg.s3_max_retries,
            retry_backoff_s=cfg.s3_retry_backoff_s,
        )
        return storage, f"s3://{cfg.s3_bucket}/{prefix}"

    root = cfg.out_root / dataset.protocol / dataset.contract
    return LocalChunkStorage(root), str(root)


def build_control_storage(cfg: OpsApiConfig) -> IChunkStorage:
    if cfg.s3_bucket:
        prefix = cfg.s3_prefix.rstrip("/") if cfg.s3_prefix else ""
        control_prefix = f"{prefix}/" if prefix else ""
        return S3ChunkStorage(
            bucket=cfg.s3_bucket,
            prefix=control_prefix,
            endpoint_url=cfg.s3_endpoint_url,
            access_key=cfg.s3_access_key,
            secret_key=cfg.s3_secret_key,
            region=cfg.s3_region,
            max_retries=cfg.s3_max_retries,
            retry_backoff_s=cfg.s3_retry_backoff_s,
        )
    return LocalChunkStorage(cfg.out_root)
