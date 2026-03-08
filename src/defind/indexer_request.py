from __future__ import annotations

import time
from typing import Any, cast

from defind.core.config import OrchestratorConfig
from defind.core.interfaces import IChunkStorage
from defind.decoding.registry import add_event_spec
from defind.decoding.specs import (
    DataFieldSpec,
    EventRegistry,
    EventSpec,
    ProjectionRef,
    ProjectionRefs,
    TopicFieldSpec,
)

INDEXER_REQUEST_KEY = "_meta/indexer_request.json"
_S3_SECRET_KEYS = frozenset({"s3_access_key", "s3_secret_key"})


def _serialize_projection_ref(ref: ProjectionRef) -> dict[str, Any] | None:
    if ref is None:
        return None
    match ref:
        case ProjectionRefs.TopicRef():
            return {"kind": "topic", "name": ref.name}
        case ProjectionRefs.DataRef():
            return {"kind": "data", "name": ref.name}
        case ProjectionRefs.Constant():
            return {"kind": "constant", "value": ref.value}
    raise ValueError("unsupported projection ref")


def _deserialize_projection_ref(raw: Any) -> ProjectionRef:
    if raw is None:
        return None
    if not isinstance(raw, dict):
        raise ValueError("projection ref must be an object or null")

    kind = str(raw.get("kind") or "").strip()
    if kind == "topic":
        name = str(raw.get("name") or "").strip()
        if not name:
            raise ValueError("topic projection ref must have a name")
        return ProjectionRefs.TopicRef(name=name)
    if kind == "data":
        name = str(raw.get("name") or "").strip()
        if not name:
            raise ValueError("data projection ref must have a name")
        return ProjectionRefs.DataRef(name=name)
    if kind == "constant":
        return ProjectionRefs.Constant(value=str(raw.get("value") or ""))
    raise ValueError(f"unsupported projection ref kind: {kind!r}")


def serialize_registry(registry: EventRegistry) -> dict[str, Any]:
    events = sorted(registry.values(), key=lambda spec: (spec.name, spec.topic0))
    return {
        "version": 1,
        "events": [
            {
                "topic0": spec.topic0,
                "name": spec.name,
                "topic_fields": [
                    {"name": field.name, "index": field.index, "type": field.type}
                    for field in spec.topic_fields
                ],
                "data_fields": [
                    {"name": field.name, "word_index": field.word_index, "type": field.type}
                    for field in spec.data_fields
                ],
                "projection": {
                    key: _serialize_projection_ref(ref)
                    for key, ref in spec.projection.items()
                },
                "fast_zero_words": list(spec.fast_zero_words),
                "drop_if_all_zero_fields": list(spec.drop_if_all_zero_fields),
            }
            for spec in events
        ],
    }


def deserialize_registry(payload: dict[str, Any]) -> EventRegistry:
    if not isinstance(payload, dict):
        raise ValueError("registry_json must be an object")
    version = int(payload.get("version") or 0)
    if version != 1:
        raise ValueError(f"unsupported registry_json version: {version}")

    events = payload.get("events")
    if not isinstance(events, list) or not events:
        raise ValueError("registry_json.events must be a non-empty array")

    registry: EventRegistry = {}
    for idx, raw_event in enumerate(events):
        if not isinstance(raw_event, dict):
            raise ValueError(f"registry_json.events[{idx}] must be an object")
        try:
            topic0 = str(raw_event.get("topic0") or "").strip()
            name = str(raw_event.get("name") or "").strip()
            if not topic0 or not name:
                raise ValueError("event must define topic0 and name")

            raw_topic_fields = raw_event.get("topic_fields")
            raw_data_fields = raw_event.get("data_fields")
            raw_projection = raw_event.get("projection")
            if not isinstance(raw_topic_fields, list) or not isinstance(raw_data_fields, list):
                raise ValueError("event fields must be arrays")
            if not isinstance(raw_projection, dict):
                raise ValueError("event projection must be an object")

            topic_fields = [
                TopicFieldSpec(
                    name=str(field.get("name") or "").strip(),
                    index=int(field.get("index") or 0),
                    type=str(field.get("type") or "").strip(),
                )
                for field in raw_topic_fields
                if isinstance(field, dict)
            ]
            if len(topic_fields) != len(raw_topic_fields):
                raise ValueError("topic_fields entries must be objects")

            data_fields = [
                DataFieldSpec(
                    name=str(field.get("name") or "").strip(),
                    word_index=int(field.get("word_index") or 0),
                    type=str(field.get("type") or "").strip(),
                )
                for field in raw_data_fields
                if isinstance(field, dict)
            ]
            if len(data_fields) != len(raw_data_fields):
                raise ValueError("data_fields entries must be objects")

            projection = {
                str(key): _deserialize_projection_ref(ref)
                for key, ref in raw_projection.items()
            }
            spec = EventSpec(
                topic0=topic0,
                name=name,
                topic_fields=topic_fields,
                data_fields=data_fields,
                projection=projection,
                fast_zero_words=tuple(int(value) for value in cast(list[Any], raw_event.get("fast_zero_words") or [])),
                drop_if_all_zero_fields=tuple(
                    str(value) for value in cast(list[Any], raw_event.get("drop_if_all_zero_fields") or [])
                ),
            )
        except Exception as exc:
            raise ValueError(f"invalid registry_json.events[{idx}]: {exc}") from exc
        add_event_spec(registry, spec)

    if not registry:
        raise ValueError("registry_json produced an empty registry")
    return registry


def sanitize_request_payload(payload: dict[str, Any]) -> dict[str, Any]:
    sanitized = dict(payload)
    for key in _S3_SECRET_KEYS:
        if key in sanitized:
            sanitized[key] = None
    return sanitized


def build_request_payload_from_config(
    *,
    config: OrchestratorConfig,
    registry: EventRegistry,
    job_origin: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "rpc_url": config.rpc_url,
        "address": config.address,
        "abi_path": None,
        "abi_json": None,
        "registry_json": serialize_registry(registry),
        "event_names": [spec.name for spec in registry.values()],
        "start_block": config.start_block,
        "end_block": config.end_block,
        "protocol_slug": config.protocol_slug,
        "contract_slug": config.contract_slug,
        "step": config.step,
        "chunk_size": config.chunk_size,
        "concurrency": config.concurrency,
        "timeout_s": config.timeout_s,
        "rpc_max_retries": config.rpc_max_retries,
        "rpc_retry_backoff_s": config.rpc_retry_backoff_s,
        "codec": config.codec,
        "listen": config.listen,
        "listen_poll_interval_s": config.listen_poll_interval_s,
        "reorg_lookback_blocks": config.reorg_lookback_blocks,
        "print_chunk_writes": config.print_chunk_writes,
        "heartbeat_interval_s": config.heartbeat_interval_s,
        "lag_warn_threshold_blocks": config.lag_warn_threshold_blocks,
        "heartbeat_key": config.heartbeat_key,
        "single_writer_guard": False,
        "writer_lock_key": config.writer_lock_key,
        "writer_lock_ttl_s": config.writer_lock_ttl_s,
        "writer_lock_refresh_s": config.writer_lock_refresh_s,
        "log_level": config.log_level,
        "log_json": config.log_json,
        "storage": "s3" if config.s3_bucket else "local",
        "out_root": None if config.s3_bucket else str(config.out_root),
        "s3_bucket": config.s3_bucket,
        "s3_prefix": config.s3_prefix,
        "s3_endpoint_url": config.s3_endpoint_url,
        "s3_access_key": config.s3_access_key,
        "s3_secret_key": config.s3_secret_key,
        "s3_region": config.s3_region,
        "s3_max_retries": config.s3_max_retries,
        "s3_retry_backoff_s": config.s3_retry_backoff_s,
    }
    if job_origin:
        payload["job_origin"] = job_origin
    return sanitize_request_payload(payload)


def write_indexer_request(
    *,
    storage: IChunkStorage,
    request_payload: dict[str, Any],
    source: str,
) -> None:
    dataset_id = f"{request_payload.get('protocol_slug')}/{request_payload.get('contract_slug')}"
    storage.write_json(
        INDEXER_REQUEST_KEY,
        {
            "version": 1,
            "savedAtS": int(time.time()),
            "source": source,
            "datasetId": dataset_id,
            "request": sanitize_request_payload(request_payload),
        },
    )


def read_indexer_request(storage: IChunkStorage) -> dict[str, Any] | None:
    payload = storage.read_json(INDEXER_REQUEST_KEY)
    if not isinstance(payload, dict):
        return None
    request_payload = payload.get("request")
    if not isinstance(request_payload, dict):
        return None
    return sanitize_request_payload(request_payload)
