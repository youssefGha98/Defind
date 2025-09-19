# specs.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, List, Optional, Mapping

# ---- Dynamic projection mapping ----
# Any projection key is allowed; values are "topic.foo" | "data.bar" | literal
Projection = Mapping[str, str]

@dataclass(frozen=True)
class TopicFieldSpec:
    name: str
    index: int
    type: str  # e.g., "address", "uint256", "int24", "bytes32"

@dataclass(frozen=True)
class DataFieldSpec:
    name: str
    word_index: int
    type: str  # e.g., "address", "uint256", "uint128"

@dataclass(frozen=True)
class EventSpec:
    topic0: str
    name: str
    topic_fields: List[TopicFieldSpec]
    data_fields: List[DataFieldSpec]
    projection: Projection
    fast_zero_words: tuple[int, ...] = ()
    drop_if_all_zero_fields: tuple[str, ...] = ()
    model_tag: Optional[str] = None

EventRegistry = Dict[str, EventSpec]
