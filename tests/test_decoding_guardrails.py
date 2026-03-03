from __future__ import annotations

import pytest

from defind.core.models import Meta
from defind.decoding.decoder import decode_event
from defind.decoding.specs import (
    DataFieldSpec,
    EventRegistry,
    EventSpec,
    ProjectionRefs,
    TopicFieldSpec,
    resolve_projection_ref,
)
from defind.decoding.utils import parse_data_word, parse_topic_field, word_at


def _base_meta() -> Meta:
    return Meta(1, 1000, "0xtx", 0, "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045")


def test_decode_event_skips_fast_zero_words() -> None:
    spec = EventSpec(
        topic0="0xabc",
        name="Swap",
        topic_fields=[TopicFieldSpec("user", 1, "address")],
        data_fields=[DataFieldSpec("amount", 0, "uint256")],
        projection={"amount": ProjectionRefs.DataRef(name="amount")},
        fast_zero_words=(0,),
    )
    registry: EventRegistry = {spec.topic0: spec}

    out = decode_event(
        topics=["0xabc", "0x" + "0" * 24 + "1234567890123456789012345678901234567890"],
        data=bytes(32),
        meta=_base_meta(),
        registry=registry,
    )
    assert out is None


def test_decode_event_skips_drop_if_all_zero_fields() -> None:
    spec = EventSpec(
        topic0="0xabc",
        name="Swap",
        topic_fields=[],
        data_fields=[DataFieldSpec("amount0", 0, "uint256"), DataFieldSpec("amount1", 1, "uint256")],
        projection={"amount0": ProjectionRefs.DataRef(name="amount0")},
        drop_if_all_zero_fields=("amount0", "amount1"),
    )
    registry: EventRegistry = {spec.topic0: spec}

    out = decode_event(
        topics=["0xabc"],
        data=bytes(64),
        meta=_base_meta(),
        registry=registry,
    )
    assert out is None


def test_decode_event_returns_none_when_topic_index_missing() -> None:
    spec = EventSpec(
        topic0="0xabc",
        name="Swap",
        topic_fields=[TopicFieldSpec("owner", 2, "address")],
        data_fields=[],
        projection={"owner": ProjectionRefs.TopicRef(name="owner")},
    )
    registry: EventRegistry = {spec.topic0: spec}

    out = decode_event(
        topics=["0xabc", "0x" + "0" * 24 + "1234567890123456789012345678901234567890"],
        data=b"",
        meta=_base_meta(),
        registry=registry,
    )
    assert out is None


def test_decode_event_returns_none_when_data_too_short() -> None:
    spec = EventSpec(
        topic0="0xabc",
        name="Swap",
        topic_fields=[],
        data_fields=[DataFieldSpec("amount", 1, "uint256")],
        projection={"amount": ProjectionRefs.DataRef(name="amount")},
    )
    registry: EventRegistry = {spec.topic0: spec}

    out = decode_event(
        topics=["0xabc"],
        data=bytes(32),  # requires at least 2 words
        meta=_base_meta(),
        registry=registry,
    )
    assert out is None


def test_parse_data_word_signed_negative_int() -> None:
    # int8(-1) encoded in 256-bit two's complement
    word = bytes.fromhex("ff" * 32)
    assert parse_data_word(word, "int8") == -1
    assert parse_data_word(word, "int") == -1


def test_parse_topic_field_unknown_type_returns_raw_lowercase() -> None:
    spec = TopicFieldSpec("x", 1, "bytes32")
    assert parse_topic_field("0xABCD", spec) == "0xabcd"


def test_word_at_out_of_range_returns_zero_padded_word() -> None:
    assert word_at(b"\x01\x02", 99) == b"\x00" * 32


def test_resolve_projection_ref_all_variants_and_unsupported() -> None:
    assert resolve_projection_ref(None, {}, {}) is None
    assert resolve_projection_ref(ProjectionRefs.TopicRef(name="x"), {"x": 1}, {}) == 1
    assert resolve_projection_ref(ProjectionRefs.DataRef(name="y"), {}, {"y": 2}) == 2
    assert resolve_projection_ref(ProjectionRefs.Constant(value="c"), {}, {}) == "c"
    with pytest.raises(RuntimeError, match="Unsupported ProjectionEntry type"):
        resolve_projection_ref(object(), {}, {})  # type: ignore[arg-type]


def test_event_spec_guardrails_on_invalid_projections() -> None:
    with pytest.raises(ValueError, match="projection is not a ProjectionRef instance"):
        EventSpec(
            topic0="0xabc",
            name="X",
            topic_fields=[],
            data_fields=[],
            projection={"x": 1},  # type: ignore[dict-item]
        )

    with pytest.raises(ValueError, match="non-existant topic field"):
        EventSpec(
            topic0="0xabc",
            name="X",
            topic_fields=[],
            data_fields=[],
            projection={"x": ProjectionRefs.TopicRef(name="missing")},
        )

    with pytest.raises(ValueError, match="non-existant data field"):
        EventSpec(
            topic0="0xabc",
            name="X",
            topic_fields=[],
            data_fields=[],
            projection={"x": ProjectionRefs.DataRef(name="missing")},
        )
