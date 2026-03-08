from pathlib import Path

import pytest

from defind.abi_events import get_event_topic0, get_events_from_abi, make_event_registry_from_abi
from defind.decoding.specs import DataFieldSpec, EventSpec, ProjectionRefs, TopicFieldSpec
from defind.decoding.registries import make_clpool_registry, make_gauge_registry, make_nfpm_registry, make_vfat_registry
from defind.indexer_request import deserialize_registry, serialize_registry


def test_make_clpool_registry() -> None:
    make_clpool_registry()


def test_make_gauge_registry() -> None:
    make_gauge_registry()


def test_make_vfat_registry() -> None:
    make_vfat_registry()


def test_make_nfpm_registry() -> None:
    make_nfpm_registry()


def test_make_event_registry_from_abi() -> None:
    abi = Path(__file__).parent / "abi" / "aerodrome_clpool_abi.json"
    assert abi.is_file()
    registry = make_event_registry_from_abi(abi)
    events = get_events_from_abi(abi)
    assert len(registry) == 9  # ABI defines 9 events
    assert len(registry) == len(events)
    assert set(registry.keys()) == set([get_event_topic0(event) for event in events.values()])


def test_get_events_from_abi_rejects_entry_missing_type() -> None:
    with pytest.raises(ValueError, match="missing 'type'"):
        get_events_from_abi([{"name": "Swap"}])


def test_registry_json_roundtrip() -> None:
    spec = EventSpec(
        topic0="0xabc",
        name="Swap",
        topic_fields=[TopicFieldSpec("sender", 1, "address")],
        data_fields=[DataFieldSpec("amount0", 0, "uint256")],
        projection={
            "sender": ProjectionRefs.TopicRef(name="sender"),
            "amount0": ProjectionRefs.DataRef(name="amount0"),
            "source": ProjectionRefs.Constant(value="manual"),
        },
        fast_zero_words=(0,),
        drop_if_all_zero_fields=("amount0",),
    )
    registry = {spec.topic0: spec}

    payload = serialize_registry(registry)
    roundtrip = deserialize_registry(payload)

    assert roundtrip == registry
