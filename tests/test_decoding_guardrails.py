from __future__ import annotations

from eth_abi import encode as abi_encode
import pyarrow as pa
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
from defind.decoding.utils import parse_data_fields, parse_data_word, parse_topic_field, word_at
from defind.storage.chunks import _build_table


def _base_meta() -> Meta:
    return Meta(1, 1000, "0xtx", 0, "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045")


def _encode_topic_int(value: int) -> str:
    encoded = value if value >= 0 else (1 << 256) + value
    return f"0x{encoded:064x}"


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


def test_parse_data_fields_uses_eth_abi_for_contiguous_dynamic_fields() -> None:
    payload = abi_encode(["bytes", "bool"], [b"\x12\x34", True])
    data_fields = [
        DataFieldSpec("blob", 0, "bytes"),
        DataFieldSpec("enabled", 1, "bool"),
    ]

    assert parse_data_fields(payload, data_fields) == {
        "blob": "0x1234",
        "enabled": True,
    }


def test_parse_data_fields_sparse_specs_fall_back_to_word_parsing() -> None:
    payload = bytes.fromhex(
        "00" * 31
        + "01"
        + "00" * 31
        + "2a"
    )
    data_fields = [DataFieldSpec("amount", 1, "uint256")]

    assert parse_data_fields(payload, data_fields) == {"amount": 42}


@pytest.mark.parametrize(
    ("value", "typ"),
    [
        (-8_388_608, "int24"),
        (-887_272, "int24"),
        (-1, "int24"),
        (0, "int24"),
        (8_388_607, "int24"),
    ],
)
def test_parse_topic_field_signed_int24(value: int, typ: str) -> None:
    spec = TopicFieldSpec("tick", 1, typ)
    assert parse_topic_field(_encode_topic_int(value), spec) == value


def test_decode_event_preserves_negative_int24_topic_projection() -> None:
    spec = EventSpec(
        topic0="0xabc",
        name="Mint",
        topic_fields=[
            TopicFieldSpec("tickLower", 1, "int24"),
            TopicFieldSpec("tickUpper", 2, "int24"),
        ],
        data_fields=[],
        projection={
            "tickLower": ProjectionRefs.TopicRef(name="tickLower"),
            "tickUpper": ProjectionRefs.TopicRef(name="tickUpper"),
        },
    )
    registry: EventRegistry = {spec.topic0: spec}

    out = decode_event(
        topics=["0xabc", _encode_topic_int(-887_272), _encode_topic_int(887_272)],
        data=b"",
        meta=_base_meta(),
        registry=registry,
    )

    assert out is not None
    assert out.values["tickLower"] == -887_272
    assert out.values["tickUpper"] == 887_272


def test_decode_event_decodes_dynamic_data_via_eth_abi() -> None:
    spec = EventSpec(
        topic0="0xabc",
        name="Message",
        topic_fields=[],
        data_fields=[
            DataFieldSpec("blob", 0, "bytes"),
            DataFieldSpec("enabled", 1, "bool"),
        ],
        projection={
            "blob": ProjectionRefs.DataRef(name="blob"),
            "enabled": ProjectionRefs.DataRef(name="enabled"),
        },
    )
    registry: EventRegistry = {spec.topic0: spec}

    out = decode_event(
        topics=["0xabc"],
        data=abi_encode(["bytes", "bool"], [b"\x12\x34", True]),
        meta=_base_meta(),
        registry=registry,
    )

    assert out is not None
    assert out.values["blob"] == "0x1234"
    assert out.values["enabled"] is True


def test_build_table_uses_native_int_type_for_int24_projection() -> None:
    spec = EventSpec(
        topic0="0xabc",
        name="Mint",
        topic_fields=[
            TopicFieldSpec("tickLower", 1, "int24"),
            TopicFieldSpec("owner", 2, "address"),
        ],
        data_fields=[DataFieldSpec("amount0", 0, "uint256")],
        projection={
            "tickLower": ProjectionRefs.TopicRef(name="tickLower"),
            "owner": ProjectionRefs.TopicRef(name="owner"),
            "amount0": ProjectionRefs.DataRef(name="amount0"),
        },
    )
    table = _build_table(
        {
            "block_number": [1],
            "block_timestamp": [1_700_000_000],
            "tx_hash": ["0xtx"],
            "log_index": [0],
            "contract": ["0x123"],
            "tickLower": [-887_272],
            "owner": ["0x1234567890123456789012345678901234567890"],
            "amount0": [2**200],
        },
        spec,
    )

    assert table.schema.field("tickLower").type == pa.int32()
    assert table.column("tickLower").to_pylist() == [-887_272]
    assert table.schema.field("amount0").type == pa.string()
    assert table.column("amount0").to_pylist() == [str(2**200)]


def test_parse_topic_field_bytes32_decodes_to_hex_string() -> None:
    spec = TopicFieldSpec("x", 1, "bytes32")
    assert parse_topic_field("0xABCD", spec) == ("0x" + ("0" * 60) + "abcd")


def test_parse_topic_field_dynamic_indexed_type_returns_raw_hash() -> None:
    spec = TopicFieldSpec("label", 1, "string")
    topic = "0x" + "ab" * 32
    assert parse_topic_field(topic, spec) == topic


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
