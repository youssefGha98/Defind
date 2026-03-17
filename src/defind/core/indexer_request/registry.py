from __future__ import annotations

from typing import Any

from defind.decoding.registry import add_event_spec
from defind.decoding.specs import (
    DataFieldSpec,
    EventRegistry,
    EventSpec,
    ProjectionRef,
    ProjectionRefs,
    TopicFieldSpec,
)


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


def _deserialize_int_tuple(raw: Any, *, field_name: str) -> tuple[int, ...]:
    if raw is None:
        return ()
    if not isinstance(raw, list):
        raise ValueError(f"{field_name} must be an array")
    out: list[int] = []
    for idx, value in enumerate(raw):
        if isinstance(value, bool):
            raise ValueError(f"{field_name}[{idx}] must be an integer")
        try:
            out.append(int(value))
        except Exception as exc:
            raise ValueError(f"{field_name}[{idx}] must be an integer") from exc
    return tuple(out)


def _deserialize_str_tuple(raw: Any, *, field_name: str) -> tuple[str, ...]:
    if raw is None:
        return ()
    if not isinstance(raw, list):
        raise ValueError(f"{field_name} must be an array")
    out: list[str] = []
    for idx, value in enumerate(raw):
        if not isinstance(value, str):
            raise ValueError(f"{field_name}[{idx}] must be a string")
        out.append(value)
    return tuple(out)


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
                fast_zero_words=_deserialize_int_tuple(
                    raw_event.get("fast_zero_words"),
                    field_name="fast_zero_words",
                ),
                drop_if_all_zero_fields=_deserialize_str_tuple(
                    raw_event.get("drop_if_all_zero_fields"),
                    field_name="drop_if_all_zero_fields",
                ),
            )
        except Exception as exc:
            raise ValueError(f"invalid registry_json.events[{idx}]: {exc}") from exc
        add_event_spec(registry, spec)

    if not registry:
        raise ValueError("registry_json produced an empty registry")
    return registry
