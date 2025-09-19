# decoder.py
from __future__ import annotations
from typing import Any, Dict, Optional, Sequence
from eth_utils import to_checksum_address

from .specs import EventRegistry, EventSpec, TopicFieldSpec, DataFieldSpec

# ---------- meta & parsed event ----------
class Meta:
    __slots__ = ("block_number","block_timestamp","tx_hash","log_index","pool")
    def __init__(self, block_number:int, block_timestamp:int, tx_hash:str, log_index:int, pool:str):
        self.block_number = block_number
        self.block_timestamp = block_timestamp
        self.tx_hash = tx_hash
        self.log_index = log_index
        self.pool = pool

class ParsedEvent:
    __slots__ = ("name","pool","meta","values","model_tag")
    def __init__(self, name:str, pool:str, meta:Meta, values:Dict[str, Any], model_tag:Optional[str]=None):
        self.name = name
        self.pool = pool
        self.meta = meta
        self.values = values
        self.model_tag = model_tag

# ---------- helpers ----------
def _word(data: bytes, i: int) -> bytes:
    start = 32 * i
    end = start + 32
    return data[start:end] if start < len(data) else b"\x00" * 32

def _parse_topic_field(topic_hex: str, spec: TopicFieldSpec) -> Any:
    t = spec.type
    h = topic_hex.lower()
    if t == "address":
        return "0x" + h[-40:]
    # keep other types as hex-string or cast int for signed widths if desired
    if t.startswith("uint") or t.startswith("int"):
        return int(h, 16)
    return h

def _parse_data_word(word: bytes, typ: str) -> Any:
    if typ == "address":
        return "0x" + word[-20:].hex()
    if typ.startswith("uint"):
        return int.from_bytes(word, "big", signed=False)
    if typ.startswith("int"):
        # two's complement
        v = int.from_bytes(word, "big", signed=False)
        bits = int(typ[3:]) if typ != "int" else 256
        if v >= 2**(bits-1):
            v -= 2**bits
        return v
    return "0x" + word.hex()

def _resolve_ref(ref: Optional[str], topic_vals: Dict[str, Any], data_vals: Dict[str, Any]) -> Any:
    if ref is None:
        return None
    if ref.startswith("topic."):
        return topic_vals.get(ref.split(".", 1)[1])
    if ref.startswith("data."):
        return data_vals.get(ref.split(".", 1)[1])
    # literal string or raw value
    return ref

# ---------- main generic decoder (dynamic) ----------
def decode_to_parsed_event(
    *,
    topics: Sequence[str],
    data: bytes,
    meta: Meta,
    registry: EventRegistry,
) -> Optional[ParsedEvent]:
    if not topics:
        return None
    topic0 = topics[0].lower()
    spec = registry.get(topic0)
    if spec is None:
        return None

    # Early fast-zero check on specified data words
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

    # Dynamically resolve ALL projection keys
    resolved: Dict[str, Any] = {}
    for out_key, ref in spec.projection.items():
        v = _resolve_ref(ref, topic_vals, data_vals)
        if isinstance(v, int):
            v = str(v)  # Arrow safety for big ints
        resolved[out_key] = v

    return ParsedEvent(
        name=spec.name,
        pool=to_checksum_address(meta.pool),
        meta=meta,
        values=resolved,           # <- open-ended dict
        model_tag=spec.model_tag or "universal",
    )
