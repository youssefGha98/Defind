"""Event specification primitives and registry typing.

Defines lightweight dataclasses to describe how to decode events:
- `TopicFieldSpec` / `DataFieldSpec`: typed sources for indexed topics / data words
- `EventSpec`: one event rule (topic0, fields, dynamic projection, filters)
- `EventRegistry`: mapping from topic0 → EventSpec

Projection values accept:
  - "topic.<name>"  → take from parsed indexed topic fields
  - "data.<name>"   → take from parsed data words
  - any other string is treated as a literal
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

# ---- Dynamic projection mapping ----
# Any projection key is allowed; values are "topic.foo" | "data.bar" | literal
Projection = Mapping[str, str]


@dataclass(frozen=True)
class TopicFieldSpec:
    """Describe one indexed topic field (by 0-based topic index and ABI type)."""
    name: str
    index: int
    type: str  # e.g., "address", "uint256", "int24", "bytes32"


@dataclass(frozen=True)
class DataFieldSpec:
    """Describe one 32-byte ABI word in the data section (0-based word index)."""
    name: str
    word_index: int
    type: str  # e.g., "address", "uint256", "uint128"


@dataclass(frozen=True)
class EventSpec:
    """One event decoding rule + dynamic projection."""
    topic0: str
    name: str
    topic_fields: list[TopicFieldSpec]
    data_fields: list[DataFieldSpec]
    projection: Projection
    fast_zero_words: tuple[int, ...] = ()
    drop_if_all_zero_fields: tuple[str, ...] = ()


# The full registry keyed by topic0 (lowercased 0x-hex).
EventRegistry = dict[str, EventSpec]
