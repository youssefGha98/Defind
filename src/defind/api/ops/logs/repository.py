from __future__ import annotations

import asyncio
import logging
import time
import uuid
from typing import Any, cast

from defind.api.ops.dataset.utils import dataset_ref_from_dataset_id
from defind.api.ops.logs.loki import LokiClient
from defind.api.ops.shared.dependencies import OpsApiDependencies
from defind.api.ops.shared.models import OpsApiConfig
from defind.api.ops.shared.utils import normalize_collection_prefix, to_iso_z
from defind.core.interfaces import IChunkStorage

ops_event_logger = logging.getLogger("ops_events")


class EventStore:
    def __init__(self, *, storage: IChunkStorage, prefix: str, max_events: int) -> None:
        self._storage = storage
        self._prefix = normalize_collection_prefix(prefix, fallback="_meta/event_history")
        self._max_events = max(100, int(max_events))
        self._write_lock = asyncio.Lock()
        self._last_event_row_id = 0

    @staticmethod
    def _decode_payload(payload: Any) -> dict[str, Any]:
        if isinstance(payload, dict):
            return cast(dict[str, Any], payload)
        return {}

    @staticmethod
    def _clean_segment(value: str | None) -> str | None:
        cleaned = str(value or "").strip().strip("/")
        return cleaned or None

    def _scoped_prefix(
        self,
        *,
        dataset_id: str | None = None,
        job_id: str | None = None,
        run_id: str | None = None,
        event_type: str | None = None,
    ) -> str:
        prefix = self._prefix
        clean_dataset_id = self._clean_segment(dataset_id)
        clean_job_id = self._clean_segment(job_id)
        clean_run_id = self._clean_segment(run_id)
        clean_event_type = self._clean_segment(event_type)

        if clean_dataset_id is not None:
            prefix = f"{prefix}dataset/{clean_dataset_id}/"
        if clean_job_id is not None:
            prefix = f"{prefix}job/{clean_job_id}/"
        if clean_run_id is not None:
            prefix = f"{prefix}run/{clean_run_id}/"
        if clean_event_type is not None:
            prefix = f"{prefix}type/{clean_event_type}/"
        return prefix

    def _event_key(
        self,
        event_id: int,
        *,
        dataset_id: str | None = None,
        job_id: str | None = None,
        run_id: str | None = None,
        event_type: str | None = None,
    ) -> str:
        base_prefix = self._scoped_prefix(
            dataset_id=dataset_id,
            job_id=job_id,
            run_id=run_id,
            event_type=event_type,
        )
        return f"{base_prefix}{event_id:020d}_{uuid.uuid4().hex}.json"

    def _normalize_event(self, payload: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": int(payload.get("id", 0) or 0),
            "tsUnixS": int(payload.get("tsUnixS", 0) or 0),
            "ts": str(payload.get("ts") or ""),
            "eventType": str(payload.get("eventType") or ""),
            "datasetId": payload.get("datasetId"),
            "jobId": payload.get("jobId"),
            "runId": payload.get("runId"),
            "payload": self._decode_payload(payload.get("payload")),
        }

    def _list_event_keys(self, prefix: str | None = None) -> list[str]:
        normalized_prefix = prefix or self._prefix
        return sorted(
            key
            for key in self._storage.list_keys(normalized_prefix)
            if key.startswith(normalized_prefix) and key.endswith(".json")
        )

    def _load_event_key(self, key: str) -> dict[str, Any] | None:
        payload = self._storage.read_json(key)
        if not isinstance(payload, dict):
            return None
        return self._normalize_event(payload)

    @staticmethod
    def _matches_filters(
        row: dict[str, Any],
        *,
        dataset_id: str | None = None,
        job_id: str | None = None,
        run_id: str | None = None,
        event_type: str | None = None,
    ) -> bool:
        if dataset_id is not None and row.get("datasetId") != dataset_id:
            return False
        if job_id is not None and row.get("jobId") != job_id:
            return False
        if run_id is not None and row.get("runId") != run_id:
            return False
        if event_type is not None and row.get("eventType") != event_type:
            return False
        return True

    def _trim_old_events_locked(self) -> None:
        keys = self._list_event_keys()
        if len(keys) <= self._max_events:
            return
        for key in keys[: len(keys) - self._max_events]:
            self._storage.delete(key)

    def _candidate_event_keys(
        self,
        *,
        dataset_id: str | None = None,
        job_id: str | None = None,
        run_id: str | None = None,
        event_type: str | None = None,
        include_legacy_fallback: bool,
    ) -> list[str]:
        scoped_prefix = self._scoped_prefix(
            dataset_id=dataset_id,
            job_id=job_id,
            run_id=run_id,
            event_type=event_type,
        )
        keys = self._list_event_keys(scoped_prefix)
        if include_legacy_fallback and not keys and scoped_prefix != self._prefix:
            keys = self._list_event_keys(self._prefix)
        return keys

    def _list_events_locked(
        self,
        *,
        limit: int,
        dataset_id: str | None = None,
        job_id: str | None = None,
        run_id: str | None = None,
        event_type: str | None = None,
    ) -> list[dict[str, Any]]:
        keys = self._candidate_event_keys(
            dataset_id=dataset_id,
            job_id=job_id,
            run_id=run_id,
            event_type=event_type,
            include_legacy_fallback=True,
        )
        filtered: list[dict[str, Any]] = []
        for key in reversed(keys):
            row = self._load_event_key(key)
            if row is None:
                continue
            if not self._matches_filters(
                row,
                dataset_id=dataset_id,
                job_id=job_id,
                run_id=run_id,
                event_type=event_type,
            ):
                continue
            filtered.append(self._normalize_event(row))
            if len(filtered) >= limit:
                break
        filtered.sort(key=lambda item: (cast(int, item["tsUnixS"]), cast(int, item["id"])), reverse=True)
        return filtered

    def _delete_events_locked(
        self,
        *,
        dataset_id: str | None = None,
        job_id: str | None = None,
        run_id: str | None = None,
        event_type: str | None = None,
        include_legacy_fallback: bool,
    ) -> int:
        keys = self._candidate_event_keys(
            dataset_id=dataset_id,
            job_id=job_id,
            run_id=run_id,
            event_type=event_type,
            include_legacy_fallback=include_legacy_fallback,
        )
        deleted = 0
        for key in keys:
            row = self._load_event_key(key)
            if row is None:
                continue
            if not self._matches_filters(
                row,
                dataset_id=dataset_id,
                job_id=job_id,
                run_id=run_id,
                event_type=event_type,
            ):
                continue
            self._storage.delete(key)
            deleted += 1
        return deleted

    async def append(
        self,
        *,
        event_type: str,
        dataset_id: str | None = None,
        job_id: str | None = None,
        run_id: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> None:
        event_id = time.time_ns()
        ts_unix_s = int(time.time())
        row_id = int(event_id // 1000)
        row: dict[str, Any] = {
            "id": 0,
            "tsUnixS": ts_unix_s,
            "ts": to_iso_z(ts_unix_s),
            "eventType": event_type,
            "datasetId": dataset_id,
            "jobId": job_id,
            "runId": run_id,
            "payload": payload or {},
        }
        async with self._write_lock:
            if row_id <= self._last_event_row_id:
                row_id = self._last_event_row_id + 1
            self._last_event_row_id = row_id
            row["id"] = row_id
            self._storage.write_json(
                self._event_key(
                    event_id,
                    dataset_id=dataset_id,
                    job_id=job_id,
                    run_id=run_id,
                    event_type=event_type,
                ),
                row,
            )
            self._trim_old_events_locked()

    async def list_events(
        self,
        *,
        limit: int,
        dataset_id: str | None = None,
        job_id: str | None = None,
        run_id: str | None = None,
        event_type: str | None = None,
    ) -> list[dict[str, Any]]:
        capped_limit = max(1, min(1000, int(limit)))
        async with self._write_lock:
            return self._list_events_locked(
                limit=capped_limit,
                dataset_id=dataset_id,
                job_id=job_id,
                run_id=run_id,
                event_type=event_type,
            )

    async def delete_events(
        self,
        *,
        dataset_id: str | None = None,
        job_id: str | None = None,
        run_id: str | None = None,
        event_type: str | None = None,
        include_legacy_fallback: bool = False,
    ) -> int:
        async with self._write_lock:
            return self._delete_events_locked(
                dataset_id=dataset_id,
                job_id=job_id,
                run_id=run_id,
                event_type=event_type,
                include_legacy_fallback=include_legacy_fallback,
            )


class DatasetEventStoreFactory:
    def __init__(self, *, cfg: OpsApiConfig, deps: OpsApiDependencies) -> None:
        self._cfg = cfg
        self._deps = deps
        self._control_event_store = EventStore(
            storage=deps.build_control_storage(cfg),
            prefix=cfg.event_history_prefix,
            max_events=cfg.event_history_limit,
        )
        self._dataset_event_stores: dict[str, EventStore] = {}

    @property
    def control_event_store(self) -> EventStore:
        return self._control_event_store

    def event_store_for_dataset_id(self, dataset_id: str | None) -> EventStore:
        dataset = dataset_ref_from_dataset_id(dataset_id)
        if dataset is None:
            return self._control_event_store
        cached = self._dataset_event_stores.get(dataset.dataset_id)
        if cached is not None:
            return cached
        storage, _ = self._deps.build_dataset_storage(self._cfg, dataset)
        store = EventStore(
            storage=storage,
            prefix=self._cfg.event_history_prefix,
            max_events=self._cfg.event_history_limit,
        )
        self._dataset_event_stores[dataset.dataset_id] = store
        return store


class LogsRepository:
    def __init__(
        self,
        *,
        event_stores: DatasetEventStoreFactory,
        loki_client: LokiClient | None = None,
        read_backend: str = "event_store",
    ) -> None:
        self._event_stores = event_stores
        self._loki_client = loki_client
        self._read_backend = read_backend.strip().lower()

    @staticmethod
    def _should_mirror_event(payload: dict[str, Any] | None) -> bool:
        if not isinstance(payload, dict):
            return True
        return not ("logger" in payload and "level" in payload)

    @staticmethod
    def _mirror_event_to_application_log(
        *,
        event_type: str,
        dataset_id: str | None,
        job_id: str | None,
        run_id: str | None,
        payload: dict[str, Any] | None,
    ) -> None:
        extra = dict(payload or {})
        extra.update(
            {
                "dataset_id": dataset_id,
                "job_id": job_id,
                "run_id": run_id,
                "event_source": "ops_api",
            }
        )
        log_method = ops_event_logger.error if "failed" in event_type.lower() else ops_event_logger.info
        log_method(event_type, extra=extra)

    async def record_event(
        self,
        *,
        event_type: str,
        dataset_id: str | None = None,
        job_id: str | None = None,
        run_id: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> None:
        await self._event_stores.event_store_for_dataset_id(dataset_id).append(
            event_type=event_type,
            dataset_id=dataset_id,
            job_id=job_id,
            run_id=run_id,
            payload=payload,
        )
        if self._should_mirror_event(payload):
            self._mirror_event_to_application_log(
                event_type=event_type,
                dataset_id=dataset_id,
                job_id=job_id,
                run_id=run_id,
                payload=payload,
            )

    async def list_job_events(self, *, dataset_id: str, job_id: str, limit: int) -> list[dict[str, Any]]:
        if self._loki_client is not None and self._read_backend == "loki":
            try:
                loki_events = await self._loki_client.list_job_events(
                    dataset_id=dataset_id,
                    job_id=job_id,
                    limit=limit,
                )
            except Exception:
                logging.getLogger(__name__).warning(
                    "loki_query_failed",
                    extra={"dataset_id": dataset_id, "job_id": job_id},
                    exc_info=True,
                )
            else:
                if loki_events:
                    return loki_events

        dataset_events = await self._event_stores.event_store_for_dataset_id(dataset_id).list_events(
            limit=limit,
            dataset_id=dataset_id,
            job_id=job_id,
        )
        if dataset_events:
            return dataset_events
        return await self._event_stores.control_event_store.list_events(
            limit=limit,
            dataset_id=dataset_id,
            job_id=job_id,
        )

    async def delete_job_logs(self, *, dataset_id: str, job_id: str) -> int:
        deleted = await self._event_stores.event_store_for_dataset_id(dataset_id).delete_events(
            dataset_id=dataset_id,
            job_id=job_id,
        )
        deleted += await self._event_stores.control_event_store.delete_events(
            dataset_id=dataset_id,
            job_id=job_id,
        )
        return deleted
