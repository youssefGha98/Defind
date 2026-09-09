import json
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Any, Literal

from eth_utils.abi import event_signature_to_log_topic
from pydantic import BaseModel, ValidationError

from defind.decoding.registry import add_event_spec
from defind.decoding.specs import DataFieldSpec, EventRegistry, EventSpec, ProjectionRefs, TopicFieldSpec


class AbiInput(BaseModel):
    indexed: bool
    internalType: str
    name: str
    type: str


class AbiEvent(BaseModel):
    anonymous: bool
    inputs: Sequence[AbiInput]
    name: str
    type: Literal["event"]


def get_event_signature(event: AbiEvent) -> str:
    return f"{event.name}({','.join(event_input.type for event_input in event.inputs)})"


def get_event_topic0(event: AbiEvent) -> str:
    return "0x" + event_signature_to_log_topic(get_event_signature(event)).hex()


def _event_input_name(*, raw_name: str, is_indexed: bool, field_index: int) -> str:
    cleaned = raw_name.strip()
    if cleaned:
        return cleaned
    prefix = "topic" if is_indexed else "data"
    return f"{prefix}_{field_index}"


def _iter_event_inputs(
    event: AbiEvent,
) -> list[tuple[str, AbiInput, bool, int]]:
    topic_field_offset = 1  # topic[0] is the event signature hash
    data_field_offset = 0
    out: list[tuple[str, AbiInput, bool, int]] = []

    for event_input in event.inputs:
        is_indexed = bool(event_input.indexed)
        field_index = topic_field_offset if is_indexed else data_field_offset
        input_name = _event_input_name(
            raw_name=event_input.name,
            is_indexed=is_indexed,
            field_index=field_index,
        )
        out.append((input_name, event_input, is_indexed, field_index))
        if is_indexed:
            topic_field_offset += 1
        else:
            data_field_offset += 1

    return out


def get_event_topic_field_specs(event: AbiEvent) -> list[TopicFieldSpec]:
    return [
        TopicFieldSpec(input_name, field_index, event_input.type)
        for input_name, event_input, is_indexed, field_index in _iter_event_inputs(event)
        if is_indexed
    ]


def get_event_data_field_specs(event: AbiEvent) -> list[DataFieldSpec]:
    return [
        DataFieldSpec(input_name, field_index, event_input.type)
        for input_name, event_input, is_indexed, field_index in _iter_event_inputs(event)
        if not is_indexed
    ]


def get_event_projection_ref(
    *,
    input_name: str,
    is_indexed: bool,
) -> ProjectionRefs.TopicRef | ProjectionRefs.DataRef:
    if is_indexed:
        return ProjectionRefs.TopicRef(name=input_name)
    return ProjectionRefs.DataRef(name=input_name)


def get_event_spec(event: AbiEvent) -> EventSpec:
    event_inputs = _iter_event_inputs(event)
    topic_fields = [
        TopicFieldSpec(input_name, field_index, event_input.type)
        for input_name, event_input, is_indexed, field_index in event_inputs
        if is_indexed
    ]
    data_fields = [
        DataFieldSpec(input_name, field_index, event_input.type)
        for input_name, event_input, is_indexed, field_index in event_inputs
        if not is_indexed
    ]
    projection = {
        input_name: get_event_projection_ref(
            input_name=input_name,
            is_indexed=is_indexed,
        )
        for input_name, _event_input, is_indexed, _field_index in event_inputs
    }
    return EventSpec(
        topic0=get_event_topic0(event),
        name=event.name,
        topic_fields=topic_fields,
        data_fields=data_fields,
        projection=projection,
        # fast_zero_words=(1, 2, 3),
        # drop_if_all_zero_fields=("liquidity", "amount0", "amount1"),
    )


AbiJson = list[dict[str, Any]]
AbiSpec = AbiJson | Path


def _load_abi(abi: AbiSpec) -> AbiJson:
    if isinstance(abi, Path):
        loaded = json.loads(abi.read_text(encoding="utf-8"))
        if not isinstance(loaded, list):
            raise ValueError("ABI file must contain a JSON array")
        return loaded
    return abi


def get_events_from_abi(abi: AbiSpec) -> dict[str, AbiEvent]:
    abi = _load_abi(abi)
    events: dict[str, AbiEvent] = {}
    for idx, entry in enumerate(abi):
        if not isinstance(entry, dict):
            raise ValueError(f"ABI entry at index {idx} must be a JSON object")
        if "type" not in entry:
            raise ValueError(f"ABI entry at index {idx} is missing 'type'")
        if entry.get("type") != "event":
            continue
        try:
            event = AbiEvent.model_validate(entry)
        except ValidationError as exc:
            raise ValueError(f"invalid event ABI entry at index {idx}: {exc}") from exc
        events[event.name] = event
    return events


def make_event_registry_from_events(events: Iterable[AbiEvent]) -> EventRegistry:
    reg: EventRegistry = {}

    for event in events:
        add_event_spec(
            reg,
            get_event_spec(event),
        )

    return reg


def make_event_registry_from_abi(abi: AbiSpec) -> EventRegistry:
    return make_event_registry_from_events(get_events_from_abi(abi).values())
