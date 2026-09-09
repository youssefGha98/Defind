"""Chunk-based Parquet writer aligned to block ranges.

Each processed block interval produces exactly one Parquet file per event type:
    {EventName}/chunk_{from_block:010d}_{to_block:010d}.parquet

Empty files (0 rows) are written for event types with no events in a given
range. This makes every written chunk file a reliable "done" marker, enabling
unambiguous resume logic based solely on file presence.

Data format
-----------
`write_chunk` accepts per-event columnar buffers:
    buffers: dict[event_name → {field_name → list_of_values}]

Each event buffer contains the base fields (block_number, block_timestamp,
tx_hash, log_index, contract) plus the event's own projection keys.
No padding — each event buffer only tracks its own fields.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pyarrow as pa

from defind.core.interfaces import IChunkStorage
from defind.core.models import BASE_FIELDS
from defind.decoding.specs import EventRegistry, EventSpec, ProjectionRefs

# Sort order applied to every written table
_SORT_KEYS = [
    ("block_number", "ascending"),
    ("tx_hash", "ascending"),
    ("log_index", "ascending"),
]


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
# Arrow table builders
# ---------------------------------------------------------------------------


def _abi_int_width(typ: str) -> int:
    if typ == "int":
        return 256
    if typ == "uint":
        return 256
    if typ.startswith("int"):
        return int(typ[3:])
    if typ.startswith("uint"):
        return int(typ[4:])
    raise ValueError(f"unsupported integer ABI type: {typ}")


def _projection_arrow_type(spec: EventSpec, out_key: str) -> pa.DataType:
    ref = spec.projection[out_key]
    if isinstance(ref, ProjectionRefs.Constant) or ref is None:
        return pa.string()

    field_types = {
        field.name: field.type
        for field in [*spec.topic_fields, *spec.data_fields]
    }
    typ = field_types.get(ref.name)
    if typ is None:
        return pa.string()
    if typ == "address":
        return pa.string()
    if typ == "bool":
        return pa.bool_()
    if typ.startswith("int"):
        bits = _abi_int_width(typ)
        if bits <= 8:
            return pa.int8()
        if bits <= 16:
            return pa.int16()
        if bits <= 32:
            return pa.int32()
        if bits <= 64:
            return pa.int64()
        return pa.string()
    if typ.startswith("uint"):
        bits = _abi_int_width(typ)
        if bits <= 8:
            return pa.uint8()
        if bits <= 16:
            return pa.uint16()
        if bits <= 32:
            return pa.uint32()
        if bits <= 64:
            return pa.uint64()
        return pa.string()
    return pa.string()


def _coerce_projection_values(values: list[Any], dtype: pa.DataType) -> list[Any]:
    if pa.types.is_string(dtype):
        return [None if value is None else str(value) for value in values]
    return values


def empty_table_for_spec(spec: EventSpec) -> pa.Table:
    """Build an empty Arrow table with the correct schema for an event spec.

    Includes base columns plus all projection columns, all with 0 rows.
    """
    fields = [pa.field(name, dtype) for name, dtype in BASE_FIELDS]
    arrays: dict[str, pa.Array] = {name: pa.array([], type=dtype) for name, dtype in BASE_FIELDS}
    for col_name in sorted(spec.projection.keys()):
        dtype = _projection_arrow_type(spec, col_name)
        fields.append(pa.field(col_name, dtype))
        arrays[col_name] = pa.array([], type=dtype)
    schema = pa.schema(fields)
    return pa.Table.from_pydict(arrays, schema=schema)


def _build_table(ev_buf: dict[str, list[Any]], spec: EventSpec) -> pa.Table:
    """Build a sorted Arrow table directly from a per-event columnar buffer.

    ev_buf contains base fields + projection keys as plain Python lists.
    No intermediate Column object — Arrow arrays are built in one pass.
    """
    n = len(ev_buf["block_number"])
    fields = [pa.field(name, dtype) for name, dtype in BASE_FIELDS]
    arrays: dict[str, pa.Array] = {
        "block_number": pa.array(ev_buf["block_number"], type=pa.uint64()),
        "block_timestamp": pa.array(ev_buf["block_timestamp"], type=pa.uint64()),
        "tx_hash": pa.array(ev_buf["tx_hash"], type=pa.string()),
        "log_index": pa.array(ev_buf["log_index"], type=pa.uint64()),
        "contract": pa.array(ev_buf["contract"], type=pa.string()),
        "event": pa.array([spec.name] * n, type=pa.string()),
    }
    for out_key in sorted(spec.projection.keys()):
        dtype = _projection_arrow_type(spec, out_key)
        values = _coerce_projection_values(ev_buf.get(out_key, [None] * n), dtype)
        fields.append(pa.field(out_key, dtype))
        arrays[out_key] = pa.array(values, type=dtype)
    schema = pa.schema(fields)
    return pa.Table.from_pydict(arrays, schema=schema).sort_by(_SORT_KEYS)


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
    return all(storage.exists(chunk_key(ev, from_block, to_block)) for ev in event_names)


# ---------------------------------------------------------------------------
# Main write function
# ---------------------------------------------------------------------------


def write_chunk(
    storage: IChunkStorage,
    registry: EventRegistry,
    from_block: int,
    to_block: int,
    buffers: dict[str, dict[str, list[Any]]],
    codec: str = "lz4",
) -> list[str]:
    """Write one Parquet file per event type for this block range.

    `buffers` maps event_name → {field_name → list_of_values}.
    Each buffer contains only the fields for that event (no padding).

    For each event in the registry:
    - If rows exist: builds an Arrow table directly and writes it.
    - If 0 rows: writes an empty Parquet with the correct schema.

    Returns the list of written storage keys.
    """
    written: list[str] = []

    for spec in registry.values():
        key = chunk_key(spec.name, from_block, to_block)
        ev_buf = buffers.get(spec.name)
        table = _build_table(ev_buf, spec) if ev_buf else empty_table_for_spec(spec)
        storage.write_table(key, table, codec)
        written.append(key)

    return written
