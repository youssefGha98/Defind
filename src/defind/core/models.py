"""Core data models.

- `EventLog`: minimal RPC log record used by the decoder.
- `Meta`: lightweight per-log metadata used during decoding.
- `BASE_FIELDS`: Arrow schema for the fixed base columns present in every chunk.
"""

from __future__ import annotations

from dataclasses import dataclass

import pyarrow as pa

# === Base schema (Arrow) ===

BASE_FIELDS: list[tuple[str, pa.DataType]] = [
    ("block_number", pa.uint64()),
    ("block_timestamp", pa.uint64()),
    ("tx_hash", pa.string()),
    ("log_index", pa.uint64()),
    ("contract", pa.string()),
    ("event", pa.string()),
]


# === RPC record ===


@dataclass(slots=True, frozen=True)
class EventLog:
    """Raw log as fetched from RPC, minimally normalized."""

    address: str  # lowercased 0x...
    topics: tuple[str, ...]  # lowercased 0x...
    data_hex: str  # "0x..."
    block_number: int
    tx_hash: str  # lowercased 0x...
    log_index: int
    block_timestamp: int | None = None


@dataclass(slots=True)
class Meta:
    """Lightweight per-log metadata used during decoding."""

    block_number: int
    block_timestamp: int | None
    tx_hash: str
    log_index: int
    address: str
