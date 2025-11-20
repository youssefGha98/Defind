"""Decoding utilities: ABI word access, typed parsers, and projection resolution."""

from __future__ import annotations

from typing import Any

from .specs import TopicFieldSpec


def word_at(data: bytes, i: int) -> bytes:
    """Return the i-th 32-byte ABI word (zero-padded if out-of-range)."""
    start = 32 * i
    end = start + 32
    return data[start:end] if start < len(data) else b"\x00" * 32


def parse_topic_field(topic_hex: str, spec: TopicFieldSpec) -> Any:
    """Parse one indexed topic according to the declared type."""
    t = spec.type
    h = topic_hex.lower()
    if t == "address":
        return "0x" + h[-40:]
    if t.startswith("uint") or t.startswith("int"):
        return int(h, 16)
    # Unknown type: return raw hex string
    return h


def parse_data_word(word: bytes, typ: str) -> Any:
    """Parse one ABI word from data according to the declared type."""
    if typ == "address":
        return "0x" + word[-20:].hex()
    if typ.startswith("uint"):
        return int.from_bytes(word, "big", signed=False)
    if typ.startswith("int"):
        # Handle signed integers using two's complement conversion
        v = int.from_bytes(word, "big", signed=False)
        bits = int(typ[3:]) if typ != "int" else 256
        # Convert to signed if value exceeds positive range
        if v >= 2 ** (bits - 1):
            v -= 2**bits
        return v
    return "0x" + word.hex()


def resolve_ref(
    ref: str | None,
    topic_vals: dict[str, Any],
    data_vals: dict[str, Any],
) -> Any:
    """Resolve a projection reference like 'topic.foo', 'data.bar', or a literal."""
    if ref is None:
        return None
    if ref.startswith("topic."):
        return topic_vals.get(ref.split(".", 1)[1])
    if ref.startswith("data."):
        return data_vals.get(ref.split(".", 1)[1])
    # Return literal value (not a reference)
    return ref
