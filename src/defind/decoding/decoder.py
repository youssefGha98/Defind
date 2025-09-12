from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence, Mapping, Any

from eth_utils import to_checksum_address

from defind.core.models import DecodedRow
from defind.decoding.specs import EventRegistry, TopicFieldSpec

@dataclass(slots=True, frozen=True)
class Meta:
    block_number: int
    block_timestamp: Optional[int]
    tx_hash: str
    log_index: int
    pool: str

# ---------- low-level readers ----------
def _word(b: bytes, i: int) -> bytes:
    o = i * 32
    return b[o:o+32]

def _addr_from_word(w: bytes) -> str:
    # last 20 bytes
    return to_checksum_address("0x" + w[-20:].hex())

def _int24_from_topic_hex(t: str) -> int:
    h = t[2:] if t[:2].lower() == "0x" else t
    b = bytes.fromhex(h[-6:])
    v = int.from_bytes(b, "big")
    return v - (1 << 24) if (v & (1 << 23)) else v

def _addr_from_topic_hex(t: str) -> str:
    h = t[2:] if t[:2].lower() == "0x" else t
    # topic is 32-byte hex; take last 20 bytes
    return to_checksum_address("0x" + h[-40:])

def _normalize_topic_hex(t: str) -> str:
    s = t.lower()
    return s if s.startswith("0x") else "0x" + s

def _uint_from_topic_hex(t: str) -> int:
    h = t[2:] if t[:2].lower() == "0x" else t
    return int(h, 16)

# ---------- field parsing according to spec ----------
def _parse_topic_field(topic_hex: str, spec: TopicFieldSpec) -> Any:
    if spec.type == "address":
        return _addr_from_topic_hex(topic_hex)
    if spec.type == "int24":
        return _int24_from_topic_hex(topic_hex)
    if spec.type == "hex":
        return _normalize_topic_hex(topic_hex)
    if spec.type == "bytes32":
        return _normalize_topic_hex(topic_hex)
    if spec.type in ("uint256", "uint128"):  # ← handle both
        return _uint_from_topic_hex(topic_hex)
    raise ValueError(f"Unsupported topic field type: {spec.type}")

def _parse_data_word(word: bytes, typ: str) -> Any:
    if typ == "address":
        return _addr_from_word(word)
    if typ == "uint128":
        return int.from_bytes(word[-16:], "big")
    if typ == "uint256":
        return int.from_bytes(word, "big")
    if typ == "int128":
        raw = int.from_bytes(word[-16:], "big")
        # signed 128
        if raw & (1 << 127):
            raw -= (1 << 128)
        return raw
    if typ == "bytes32":
        return "0x" + word.hex()
    raise ValueError(f"Unsupported data field type: {typ}")

def _resolve_ref(ref: Optional[str], topic_vals: Mapping[str, Any], data_vals: Mapping[str, Any]) -> Any:
    if not ref:
        return None
    if ref.startswith("topic."):
        return topic_vals.get(ref.split(".", 1)[1])
    if ref.startswith("data."):
        return data_vals.get(ref.split(".", 1)[1])
    # allow literal fallback (rare)
    return ref

# ---------- main generic decoder ----------
def decode_event_with_registry(
    *,
    topics: Sequence[str],
    data: bytes,
    meta: Meta,
    registry: EventRegistry,
) -> Optional[DecodedRow]:
    if not topics:
        return None

    topic0 = topics[0].lower()
    spec = registry.get(topic0)
    if spec is None:
        return None

    # Early fast zero check on specified data words
    if spec.fast_zero_words:
        for wi in spec.fast_zero_words:
            w = _word(data, wi)
            if any(w):
                break
        else:
            return None  # all designated words were zero

    # Parse topic fields
    topic_vals: dict[str, Any] = {}
    for tf in spec.topic_fields:
        if tf.index >= len(topics):
            return None
        topic_vals[tf.name] = _parse_topic_field(topics[tf.index], tf)

    # Parse data words (ensure data size)
    if spec.data_fields:
        need_words = max(df.word_index for df in spec.data_fields) + 1
        if len(data) < 32 * need_words:
            return None

    data_vals: dict[str, Any] = {}
    for df in spec.data_fields:
        data_vals[df.name] = _parse_data_word(_word(data, df.word_index), df.type)

    # Drop if all specified fields are zero (post-parse)
    if spec.drop_if_all_zero_fields:
        if all(int(data_vals.get(name, 0) or 0) == 0 for name in spec.drop_if_all_zero_fields):
            return None

    # Build DecodedRow via projection
    owner      = _resolve_ref(spec.projection.get("owner"),      topic_vals, data_vals)
    sender     = _resolve_ref(spec.projection.get("sender"),     topic_vals, data_vals)
    recipient  = _resolve_ref(spec.projection.get("recipient"),  topic_vals, data_vals)
    tick_lower = _resolve_ref(spec.projection.get("tick_lower"), topic_vals, data_vals) or 0
    tick_upper = _resolve_ref(spec.projection.get("tick_upper"), topic_vals, data_vals) or 0
    liquidity  = _resolve_ref(spec.projection.get("liquidity"),  topic_vals, data_vals)
    amount0    = _resolve_ref(spec.projection.get("amount0"),    topic_vals, data_vals)
    amount1    = _resolve_ref(spec.projection.get("amount1"),    topic_vals, data_vals)

    # Convert big ints to strings for Arrow safety
    def _to_str_or_none(v: Any) -> Optional[str]:
        if v is None:
            return None
        if isinstance(v, int):
            return str(v)
        return str(v)

    return DecodedRow(
        block_number   = meta.block_number,
        block_timestamp= meta.block_timestamp or 0,
        tx_hash        = meta.tx_hash,
        log_index      = meta.log_index,
        pool           = to_checksum_address(meta.pool),
        event          = spec.name,
        owner          = owner,
        sender         = sender,
        recipient      = recipient,
        tick_lower     = int(tick_lower) if tick_lower is not None else 0,
        tick_upper     = int(tick_upper) if tick_upper is not None else 0,
        liquidity      = _to_str_or_none(liquidity),
        amount0        = _to_str_or_none(amount0),
        amount1        = _to_str_or_none(amount1),
    )

@dataclass(slots=True, frozen=True)
class ParsedEvent:
    topic0: str                 # the event signature topic (lowercased)
    name: str                   # spec.name
    model_tag: str              # spec.model_tag (defaults to "clpool")
    pool: str                   # checksum address of emitter
    meta: Meta                  # the raw meta you passed in
    values: Mapping[str, Any]   # canonical keys (owner/sender/recipient/ticks/liquidity/amount0/amount1)

def decode_to_parsed_event(
    *,
    topics: Sequence[str],
    data: bytes,
    meta: Meta,
    registry: EventRegistry
) -> Optional[ParsedEvent]:
    """Parse topics+data using the EventRegistry and return a model-agnostic ParsedEvent."""
    if not topics:
        return None

    topic0 = topics[0].lower()
    spec = registry.get(topic0)
    if spec is None:
        return None

    # -------- Early short-circuit on zero data (fast_zero_words) --------
    if spec.fast_zero_words:
        # Ensure we have enough data to check these words; if not, treat as no-op
        need_words_for_fast = 1 + max(spec.fast_zero_words)
        if len(data) >= 32 * need_words_for_fast:
            all_zero = True
            for wi in spec.fast_zero_words:
                if any(x != 0 for x in _word(data, wi)):
                    all_zero = False
                    break
            if all_zero:
                return None

    # -------- Parse indexed (topic) fields --------
    topic_vals: dict[str, Any] = {}
    for tf in spec.topic_fields:
        # Guard against malformed logs with fewer topics than expected
        if tf.index >= len(topics):
            return None
        topic_vals[tf.name] = _parse_topic_field(topics[tf.index], tf)

    # -------- Parse data fields (ensure payload has enough 32-byte words) --------
    data_vals: dict[str, Any] = {}
    if spec.data_fields:
        need_words = max(df.word_index for df in spec.data_fields) + 1
        if len(data) < 32 * need_words:
            return None
        for df in spec.data_fields:
            data_vals[df.name] = _parse_data_word(_word(data, df.word_index), df.type)

    # -------- Drop if all specified fields are zero (post-parse) --------
    if spec.drop_if_all_zero_fields:
        if all(int(data_vals.get(name, 0) or 0) == 0 for name in spec.drop_if_all_zero_fields):
            return None

    # -------- Build canonical values via projection --------
    owner      = _resolve_ref(spec.projection.get("owner"),      topic_vals, data_vals)
    sender     = _resolve_ref(spec.projection.get("sender"),     topic_vals, data_vals)
    recipient  = _resolve_ref(spec.projection.get("recipient"),  topic_vals, data_vals)
    tick_lower = _resolve_ref(spec.projection.get("tick_lower"), topic_vals, data_vals) or 0
    tick_upper = _resolve_ref(spec.projection.get("tick_upper"), topic_vals, data_vals) or 0
    liquidity  = _resolve_ref(spec.projection.get("liquidity"),  topic_vals, data_vals)
    amount0    = _resolve_ref(spec.projection.get("amount0"),    topic_vals, data_vals)
    amount1    = _resolve_ref(spec.projection.get("amount1"),    topic_vals, data_vals)

    def _s(v: Any) -> Optional[str]:
        if v is None:
            return None
        # normalize ints to str; keep strings as-is
        return str(v)

    vals = {
        "owner": owner,
        "sender": sender,
        "recipient": recipient,
        "tick_lower": int(tick_lower) if tick_lower is not None else 0,
        "tick_upper": int(tick_upper) if tick_upper is not None else 0,
        "liquidity": _s(liquidity),
        "amount0":   _s(amount0),
        "amount1":   _s(amount1),
    }

    return ParsedEvent(
        topic0=topic0,
        name=spec.name,
        model_tag=getattr(spec, "model_tag", "clpool"),
        pool=to_checksum_address(meta.pool),
        meta=meta,
        values=vals,
    )