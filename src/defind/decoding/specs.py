from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, List, Mapping, Optional, Sequence, TypedDict, Literal

# ---------- Types for specs ----------
TopicFieldType = Literal["address", "int24", "hex", "bytes32", "uint256", "uint128"]

DataFieldType  = Literal["address", "uint128", "uint256", "int128", "bytes32"]

@dataclass(slots=True, frozen=True)
class TopicFieldSpec:
    name: str                 # e.g. "owner", "tick_lower"
    index: int                # topic index (1..N); 0 is topic0 (signature)
    type: TopicFieldType

@dataclass(slots=True, frozen=True)
class DataFieldSpec:
    name: str                 # e.g. "sender", "liquidity", "amount0"
    word_index: int           # 0-based word index within data payload
    type: DataFieldType

class Projection(TypedDict, total=False):
    # map DecodedRow fields -> "topic.<name>" or "data.<name>"
    owner: str
    sender: str
    recipient: str
    tick_lower: str
    tick_upper: str
    liquidity: str
    amount0: str
    amount1: str

@dataclass(slots=True, frozen=True)
class EventSpec:
    topic0: str
    name: str
    topic_fields: Sequence[TopicFieldSpec]
    data_fields: Sequence[DataFieldSpec]
    projection: Projection                       # how to build DecodedRow from parsed fields
    fast_zero_words: Sequence[int] = ()          # indices of data words for early zero check
    drop_if_all_zero_fields: Sequence[str] = ()  # names in data_fields, all == 0 -> drop
    model_tag: str = "clpool"

EventRegistry = Dict[str, EventSpec]  # key: topic0 (lowercase, 0x...)
