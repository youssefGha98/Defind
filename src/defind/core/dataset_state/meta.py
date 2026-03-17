from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import PurePosixPath
from typing import Any

from defind.core.dataset_state.constants import IMMUTABLE_META_FIELDS, META_KEY, META_VERSION
from defind.core.dataset_state.locks import (
    looks_unsupported_lock_error,
    process_local_lock,
    read_json_with_version,
    warn_local_only_degradation,
    write_json_if_version,
)
from defind.core.indexer_request import deserialize_registry, serialize_registry
from defind.core.interfaces import IChunkStorage
from defind.decoding.specs import EventRegistry


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def discover_dataset_refs(storage: IChunkStorage) -> list[tuple[str, str]]:
    refs: set[tuple[str, str]] = set()
    for key in storage.list_keys(""):
        normalized = key.strip().lstrip("/")
        if not normalized.endswith(f"/{META_KEY}"):
            continue
        parts = PurePosixPath(normalized).parts
        if len(parts) != 3:
            continue
        protocol, contract, filename = parts
        if filename != META_KEY:
            continue
        if protocol and contract:
            refs.add((protocol, contract))
    return sorted(refs)


def read_dataset_meta(storage: IChunkStorage) -> dict[str, Any] | None:
    payload = storage.read_json(META_KEY)
    if not isinstance(payload, dict):
        return None
    return dict(payload)


def create_dataset_meta(storage: IChunkStorage, payload: dict[str, Any]) -> dict[str, Any]:
    if storage.exists(META_KEY):
        raise FileExistsError(META_KEY)
    normalized = dict(payload)
    normalized["version"] = META_VERSION
    normalized["updated_at"] = now_iso()
    storage.write_json(META_KEY, normalized)
    return normalized


def update_dataset_meta(
    storage: IChunkStorage,
    update_fn: Callable[[dict[str, Any]], dict[str, Any]],
    *,
    max_attempts: int = 8,
) -> dict[str, Any]:
    def local_only_update() -> dict[str, Any]:
        with process_local_lock(storage, META_KEY):
            current = read_dataset_meta(storage)
            if not isinstance(current, dict):
                raise FileNotFoundError(META_KEY)
            updated = dict(update_fn(dict(current)))
            updated["version"] = META_VERSION
            updated["updated_at"] = now_iso()
            storage.write_json(META_KEY, updated)
            return updated

    for _ in range(max(1, max_attempts)):
        try:
            current, version = read_json_with_version(storage, META_KEY)
            if not isinstance(current, dict):
                raise FileNotFoundError(META_KEY)
            updated = dict(update_fn(dict(current)))
            updated["version"] = META_VERSION
            updated["updated_at"] = now_iso()
            if write_json_if_version(storage, META_KEY, updated, version):
                return updated
        except Exception as exc:
            if looks_unsupported_lock_error(exc):
                warn_local_only_degradation(storage, META_KEY, reason=str(exc))
                return local_only_update()
            raise
    raise RuntimeError("dataset meta update conflict")


def validate_meta_patch(payload: dict[str, Any]) -> None:
    forbidden = sorted(field for field in payload if field in IMMUTABLE_META_FIELDS)
    if forbidden:
        raise ValueError(f"immutable meta fields cannot be updated: {forbidden}")


def validate_meta_runtime_fields(meta: dict[str, Any]) -> EventRegistry:
    rpc_url = str(meta.get("rpc_url") or "").strip()
    if not rpc_url:
        raise ValueError("dataset meta is missing rpc_url")
    event_names = meta.get("event_names")
    if (
        not isinstance(event_names, list)
        or not event_names
        or not all(isinstance(item, str) and item for item in event_names)
    ):
        raise ValueError("dataset meta is missing event_names")
    registry_json = meta.get("registry_json")
    if not isinstance(registry_json, dict):
        raise ValueError("dataset meta is missing registry_json")
    return deserialize_registry(registry_json)


def build_dataset_meta(
    *,
    protocol: str,
    contract: str,
    contract_address: str,
    chain_id: int,
    start_block: int,
    chunk_size: int,
    step: int,
    storage: str,
    rpc_url: str,
    event_names: list[str],
    registry: EventRegistry,
) -> dict[str, Any]:
    return {
        "protocol": protocol,
        "contract": contract,
        "contract_address": contract_address,
        "chain_id": int(chain_id),
        "start_block": int(start_block),
        "last_block": int(start_block),
        "chunk_size": int(chunk_size),
        "step": int(step),
        "storage": storage,
        "rpc_url": rpc_url,
        "event_names": list(event_names),
        "registry_json": serialize_registry(registry),
    }
