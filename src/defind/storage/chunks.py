"""Chunk-based Parquet writer aligned to block ranges.

Each processed block interval produces exactly one Parquet file per event type:
    {EventName}/chunk_{from_block:010d}_{to_block:010d}.parquet

Empty files (0 rows) are written for event types with no events in a given
range. This makes every written chunk file a reliable "done" marker, enabling
unambiguous resume logic based solely on file presence.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from defind.core.interfaces import IChunkStorage
from defind.core.models import BASE_FIELDS, Column
from defind.decoding.specs import EventRegistry, EventSpec


# ---------------------------------------------------------------------------
# Key helpers
# ---------------------------------------------------------------------------


def chunk_key(event_name: str, from_block: int, to_block: int) -> str:
    """Build the storage key for a chunk file.

    Example: chunk_key("Mint", 12376729, 12381729)
             → "Mint/chunk_0012376729_0012381729.parquet"
    """
    return f"{event_name}/chunk_{from_block:010d}_{to_block:010d}.parquet"


def parse_chunk_key(key: str) -> tuple[int, int] | None:
    """Parse (from_block, to_block) from a chunk key.

    Returns None if the key does not match the expected format.

    Example: parse_chunk_key("Mint/chunk_0012376729_0012381729.parquet")
             → (12376729, 12381729)
    """
    try:
        filename = Path(key).name  # "chunk_0012376729_0012381729.parquet"
        stem = filename.removesuffix(".parquet")  # "chunk_0012376729_0012381729"
        parts = stem.split("_")  # ["chunk", "0012376729", "0012381729"]
        if len(parts) != 3 or parts[0] != "chunk":
            return None
        return int(parts[1]), int(parts[2])
    except (ValueError, IndexError):
        return None


# ---------------------------------------------------------------------------
# Empty table builder
# ---------------------------------------------------------------------------


def empty_table_for_spec(spec: EventSpec) -> pa.Table:
    """Build an empty Arrow table with the correct schema for an event spec.

    Includes base columns (block_number, tx_hash, ...) plus all dynamic
    projection columns defined by the spec, all with 0 rows.
    """
    fields = [pa.field(name, dtype) for name, dtype in BASE_FIELDS]
    arrays: dict[str, pa.Array] = {
        name: pa.array([], type=dtype) for name, dtype in BASE_FIELDS
    }
    for col_name in sorted(spec.projection.keys()):
        fields.append(pa.field(col_name, pa.string()))
        arrays[col_name] = pa.array([], type=pa.string())
    schema = pa.schema(fields)
    return pa.Table.from_pydict(arrays, schema=schema)


# ---------------------------------------------------------------------------
# Chunk done check
# ---------------------------------------------------------------------------


def chunk_is_done(
    storage: IChunkStorage,
    event_names: list[str],
    from_block: int,
    to_block: int,
) -> bool:
    """Return True iff chunk files exist for ALL event types in storage.

    A chunk is considered done only when every event type has its file,
    guaranteeing that a partial crash (some events written, some not) is
    detected and the chunk is reprocessed.
    """
    return all(
        storage.exists(chunk_key(ev, from_block, to_block))
        for ev in event_names
    )


# ---------------------------------------------------------------------------
# Main write function
# ---------------------------------------------------------------------------


def write_chunk(
    storage: IChunkStorage,
    registry: EventRegistry,
    from_block: int,
    to_block: int,
    col: Column,
    codec: str = "zstd",
) -> list[str]:
    """Write one Parquet file per event type for this block range.

    For each event in the registry:
    - Extracts rows for this event type from `col`.
    - If 0 rows: writes an empty Parquet with the correct schema.
    - Writes to storage under key: `{EventName}/chunk_{from:010d}_{to:010d}.parquet`

    Returns the list of written keys.
    """
    written: list[str] = []

    # Build a fast index: event_name → row indices
    idx_by_event: dict[str, list[int]] = {}
    for i, ev in enumerate(col.event):
        idx_by_event.setdefault(ev, []).append(i)

    for spec in registry.values():
        ev_name = spec.name
        key = chunk_key(ev_name, from_block, to_block)

        indices = idx_by_event.get(ev_name)
        if indices:
            table = col.take_indices(indices).to_arrow_table()
        else:
            table = empty_table_for_spec(spec)

        storage.write_table(key, table, codec)
        written.append(key)

    return written
