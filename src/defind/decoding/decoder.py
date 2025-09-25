"""Generic event decoder using dynamic projections.

This module translates raw logs into `ParsedEvent` using an `EventRegistry`
defined by `EventSpec` + (topic|data) field specs. Projections are dynamic:
any key defined in the registry's `projection` mapping becomes a column in
the universal buffer.
"""

from __future__ import annotations

from typing import Any, Sequence, Optional, Dict

from eth_utils import to_checksum_address

from .specs import EventRegistry
from .utils import word_at, parse_topic_field, parse_data_word, resolve_projection_ref


# ---------- meta & parsed event ----------

class Meta:
    """Lightweight metadata for a single log used during decoding."""
    __slots__ = ("block_number", "block_timestamp", "tx_hash", "log_index", "pool")

    def __init__(self, block_number: int, block_timestamp: int, tx_hash: str, log_index: int, pool: str):
        self.block_number = block_number
        self.block_timestamp = block_timestamp
        self.tx_hash = tx_hash
        self.log_index = log_index
        self.pool = pool


class ParsedEvent:
    """Decoded event with open-ended `values` for dynamic projections."""
    __slots__ = ("name", "pool", "meta", "values")

    def __init__(self, name: str, pool: str, meta: Meta, values: Dict[str, Any]):
        self.name = name
        self.pool = pool
        self.meta = meta
        self.values = values


# ---------- main generic decoder (dynamic) ----------

def decode_to_parsed_event(
    *,
    topics: Sequence[str],
    data: bytes,
    meta: Meta,
    registry: EventRegistry,
) -> Optional[ParsedEvent]:
    """Decode raw log (topics + data) into a `ParsedEvent` or return None if filtered.

    Filtering rules (optional per `EventSpec`):
    - `fast_zero_words`: if all designated data words are zero → drop early.
    - `drop_if_all_zero_fields`: after typed parse, if all listed fields are zero → drop.
    """
    if not topics:
        return None
    topic0 = topics[0].lower()
    spec = registry.get(topic0)
    if spec is None:
        return None

    # Early fast-zero check on specified data words
    if spec.fast_zero_words:
        for wi in spec.fast_zero_words:
            w = word_at(data, wi)
            if any(w):
                break
        else:
            return None  # all designated words were zero

    # Parse topic fields
    topic_vals: dict[str, Any] = {}
    for tf in spec.topic_fields:
        if tf.index >= len(topics):
            return None
        topic_vals[tf.name] = parse_topic_field(topics[tf.index], tf)

    # Parse data words (ensure data size)
    if spec.data_fields:
        need_words = max(df.word_index for df in spec.data_fields) + 1
        if len(data) < 32 * need_words:
            return None

    data_vals: dict[str, Any] = {}
    for df in spec.data_fields:
        data_vals[df.name] = parse_data_word(word_at(data, df.word_index), df.type)

    # Drop if all specified fields are zero (post-parse)
    if spec.drop_if_all_zero_fields:
        if all(int(data_vals.get(name, 0) or 0) == 0 for name in spec.drop_if_all_zero_fields):
            return None

    # Dynamically resolve ALL projection keys
    resolved: Dict[str, Any] = {}
    for out_key, ref in spec.projection.items():
        v = resolve_projection_ref(ref, topic_vals, data_vals)
        if isinstance(v, int):
            v = str(v)  # Arrow safety for big ints
        resolved[out_key] = v

    return ParsedEvent(
        name=spec.name,
        pool=to_checksum_address(meta.pool),
        meta=meta,
        values=resolved,           # <- open-ended dict
    )
