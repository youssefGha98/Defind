from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import HTTPException

from defind.abi_events import AbiEvent, get_events_from_abi, make_event_registry_from_events
from defind.api.ops.shared.models import DatasetRef
from defind.api.ops.shared.utils import PUBLIC_CHAIN_HEAD_RPCS, clean_optional_str, is_hex_address
from defind.dataset_state import validate_meta_patch
from defind.decoding.specs import EventRegistry
from defind.indexer_request import deserialize_registry


def dataset_from_route(protocol: str, contract: str) -> DatasetRef:
    normalized_protocol = protocol.strip()
    normalized_contract = contract.strip()
    if not normalized_protocol or not normalized_contract:
        raise HTTPException(status_code=400, detail="protocol and contract are required")
    return DatasetRef(protocol=normalized_protocol, contract=normalized_contract)


def dataset_ref_from_dataset_id(dataset_id: str | None) -> DatasetRef | None:
    raw = str(dataset_id or "").strip().strip("/")
    if not raw or "/" not in raw:
        return None
    protocol, contract = raw.split("/", 1)
    if not protocol or not contract:
        return None
    return DatasetRef(protocol=protocol, contract=contract)


def public_chain_head_rpc_urls(chain_id: int) -> tuple[str, ...]:
    return PUBLIC_CHAIN_HEAD_RPCS.get(int(chain_id), ())


def stored_chain_head(meta: dict[str, Any]) -> int | None:
    candidates: list[int] = []
    for key in ("observed_chain_head", "chain_head", "chain_head_block"):
        raw = meta.get(key)
        if raw is None or raw == "":
            continue
        try:
            candidates.append(int(raw))
        except (TypeError, ValueError):
            continue
    return max(candidates) if candidates else None


def normalize_str_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    normalized: list[str] = []
    for item in value:
        cleaned = clean_optional_str(str(item))
        if cleaned is not None:
            normalized.append(cleaned)
    return normalized


def optional_meta_int(meta: dict[str, Any], key: str) -> int | None:
    raw = meta.get(key)
    if raw is None or raw == "":
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def optional_meta_float(meta: dict[str, Any], key: str) -> float | None:
    raw = meta.get(key)
    if raw is None or raw == "":
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def meta_chain_id(default_chain_id: int, meta: dict[str, Any]) -> int:
    chain_id = int(meta.get("chain_id") or 0)
    return chain_id if chain_id > 0 else int(default_chain_id)


def _events_from_inputs(
    *,
    abi_path: str | None,
    abi_json: list[dict[str, Any]] | None,
) -> dict[str, AbiEvent]:
    if abi_json is not None:
        try:
            events = get_events_from_abi(abi_json)
        except Exception as exc:
            raise ValueError(f"invalid abi_json: {exc}") from exc
        if events:
            return events
        raise ValueError("abi_json produced an empty event list")

    normalized_path = (abi_path or "").strip()
    if not normalized_path:
        raise ValueError("abi_path, abi_json or registry_json is required")

    path = Path(normalized_path).expanduser()
    if not path.exists():
        raise ValueError(f"abi_path does not exist: {path}")
    if not path.is_file():
        raise ValueError(f"abi_path must be a file: {path}")

    try:
        events = get_events_from_abi(path)
    except Exception as exc:
        raise ValueError(f"invalid abi_path: {exc}") from exc
    if events:
        return events
    raise ValueError("abi_path produced an empty event list")


def _selected_events(events: dict[str, AbiEvent], selected_names: list[str] | None) -> list[AbiEvent]:
    if not selected_names:
        return list(events.values())

    unique: list[str] = []
    seen: set[str] = set()
    for name in selected_names:
        normalized = str(name).strip()
        if not normalized or normalized in seen:
            continue
        unique.append(normalized)
        seen.add(normalized)

    missing = [name for name in unique if name not in events]
    if missing:
        available = ", ".join(sorted(events.keys()))
        raise ValueError(f"unknown event_names: {missing}; available: [{available}]")
    return [events[name] for name in unique]


def _selected_registry(
    registry: EventRegistry,
    selected_names: list[str] | None,
) -> tuple[EventRegistry, list[str]]:
    if not selected_names:
        names = [spec.name for spec in registry.values()]
        return registry, names

    unique: list[str] = []
    seen: set[str] = set()
    for name in selected_names:
        normalized = str(name).strip()
        if not normalized or normalized in seen:
            continue
        unique.append(normalized)
        seen.add(normalized)

    available = {spec.name for spec in registry.values()}
    missing = [name for name in unique if name not in available]
    if missing:
        available_names = ", ".join(sorted(available))
        raise ValueError(f"unknown event_names: {missing}; available: [{available_names}]")

    selected = {topic0: spec for topic0, spec in registry.items() if spec.name in seen}
    if not selected:
        raise ValueError("selected events produced an empty registry")
    return selected, unique


def build_registry_from_inputs(
    *,
    abi_path: str | None,
    abi_json: list[dict[str, Any]] | None,
    registry_json: dict[str, Any] | None,
    event_names: list[str] | None,
) -> tuple[EventRegistry, list[str]]:
    if registry_json is not None:
        registry = deserialize_registry(registry_json)
        return _selected_registry(registry, event_names)

    events = _events_from_inputs(abi_path=abi_path, abi_json=abi_json)
    selected = _selected_events(events, event_names)
    registry = make_event_registry_from_events(selected)
    if not registry:
        raise ValueError("selected events produced an empty registry")
    return registry, [event.name for event in selected]


def normalize_dataset_patch(payload: dict[str, Any]) -> dict[str, Any]:
    try:
        validate_meta_patch(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    normalized = dict(payload)
    if "storage" in normalized and str(normalized.get("storage") or "") != "s3":
        raise HTTPException(status_code=400, detail="storage must remain 's3'")
    if "contract_address" in normalized and not is_hex_address(str(normalized.get("contract_address") or "").strip()):
        raise HTTPException(status_code=400, detail="invalid contract address")
    if "registry_json" in normalized:
        registry_json = normalized.get("registry_json")
        if not isinstance(registry_json, dict):
            raise HTTPException(status_code=400, detail="registry_json must be an object")
        registry = deserialize_registry(registry_json)
        if "event_names" not in normalized:
            normalized["event_names"] = [spec.name for spec in registry.values()]
    return normalized
