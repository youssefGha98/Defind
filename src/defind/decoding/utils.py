"""Decoding utilities: ABI word access, typed parsers, and projection resolution."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from eth_abi import decode as abi_decode

from .specs import DataFieldSpec
from .specs import TopicFieldSpec


def _parse_abi_int(raw: int, typ: str) -> int:
    """Parse an ABI integer value with the declared bit width."""
    signed = typ.startswith("int")
    prefix_len = 3 if signed else 4
    bits = int(typ[prefix_len:]) if typ not in ("int", "uint") else 256
    if bits <= 0 or bits > 256:
        raise ValueError(f"invalid {'signed' if signed else 'unsigned'} integer width: {typ}")
    if bits < 256:
        raw &= (1 << bits) - 1
    if signed and raw >= 2 ** (bits - 1):
        raw -= 2**bits
    return raw


def _normalize_abi_value(value: Any) -> Any:
    """Convert decoded ABI values to repo-friendly Python representations."""
    if isinstance(value, bytes):
        return "0x" + value.hex()
    if isinstance(value, tuple):
        return tuple(_normalize_abi_value(item) for item in value)
    if isinstance(value, list):
        return [_normalize_abi_value(item) for item in value]
    return value


def _topic_hex_to_word(topic_hex: str) -> bytes:
    """Normalize a topic hex string into a 32-byte ABI word."""
    cleaned = topic_hex[2:] if topic_hex.lower().startswith("0x") else topic_hex
    if len(cleaned) % 2:
        cleaned = "0" + cleaned
    cleaned = cleaned[-64:].rjust(64, "0")
    return bytes.fromhex(cleaned)


def _is_dynamic_abi_type(typ: str) -> bool:
    """Return True when the ABI type uses dynamic encoding."""
    cleaned = typ.strip()
    if cleaned in {"bytes", "string"}:
        return True
    if cleaned.startswith("tuple") or cleaned.startswith("("):
        return True
    return "[" in cleaned


def word_at(data: bytes, i: int) -> bytes:
    """Return the i-th 32-byte ABI word (zero-padded if out-of-range)."""
    start = 32 * i
    end = start + 32
    return data[start:end] if start < len(data) else b"\x00" * 32


def parse_topic_field(topic_hex: str, spec: TopicFieldSpec) -> Any:
    """Parse one indexed topic according to the declared type."""
    t = spec.type
    h = topic_hex.lower()
    if _is_dynamic_abi_type(t):
        # Indexed dynamic values are stored as a Keccak hash, not the original value.
        return h
    try:
        return _normalize_abi_value(abi_decode([t], _topic_hex_to_word(h))[0])
    except Exception:
        if t == "address":
            return "0x" + h[-40:]
        if t.startswith("uint") or t.startswith("int"):
            return _parse_abi_int(int(h, 16), t)
    # Unknown type: return raw hex string
    return h


def parse_data_word(word: bytes, typ: str) -> Any:
    """Parse one ABI word from data according to the declared type."""
    if _is_dynamic_abi_type(typ):
        return "0x" + word.hex()
    try:
        return _normalize_abi_value(abi_decode([typ], word)[0])
    except Exception:
        if typ == "address":
            return "0x" + word[-20:].hex()
        if typ.startswith("uint") or typ.startswith("int"):
            return _parse_abi_int(int.from_bytes(word, "big", signed=False), typ)
    return "0x" + word.hex()


def parse_data_fields(data: bytes, data_fields: Sequence[DataFieldSpec]) -> dict[str, Any] | None:
    """Decode non-indexed event data.

    When the declared fields cover the full non-indexed ABI argument list in
    order, decode everything with `eth_abi`. Sparse/manual specs fall back to
    one-word parsing for backward compatibility.
    """
    if not data_fields:
        return {}

    ordered = sorted(data_fields, key=lambda field: field.word_index)
    positions = [field.word_index for field in ordered]

    if positions == list(range(len(ordered))):
        try:
            decoded = abi_decode([field.type for field in ordered], data)
        except Exception:
            return None
        return {
            field.name: _normalize_abi_value(value)
            for field, value in zip(ordered, decoded)
        }

    need_words = max(positions) + 1
    if len(data) < 32 * need_words:
        return None

    return {
        field.name: parse_data_word(word_at(data, field.word_index), field.type)
        for field in ordered
    }
