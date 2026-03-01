"""Block-range coverage utilities for resumable indexing.

Functions
---------
- merge_intervals: merge overlapping/adjacent [start, end] integer ranges.
- subtract_iv: subtract a set of covered intervals from a target interval.
- load_done_coverage: scan chunk files in storage and collect 'done' ranges.

All intervals are inclusive on both ends: [start, end].
"""

from __future__ import annotations

from collections.abc import Generator
from pathlib import Path

from defind.core.interfaces import IChunkStorage
from defind.storage.chunks import parse_chunk_key


def topics_fingerprint(t0s: list[str]) -> str:
    """Compact fingerprint for a set of topic0 signatures (order-insensitive)."""
    uniq = sorted({(t or "").lower() for t in t0s if t})
    return "x".join(x[:10] for x in uniq) if uniq else "none"


def iter_chunks(a: int, b: int, step: int) -> Generator[tuple[int, int], None, None]:
    """Yield inclusive [start, end] block ranges of size at most `step`."""
    x = a
    while x <= b:
        y = min(b, x + step - 1)
        yield (x, y)
        x = y + 1


def merge_intervals(intervals: list[tuple[int, int]]) -> list[tuple[int, int]]:
    """Merge overlapping/adjacent inclusive intervals."""
    if not intervals:
        return []
    intervals_sorted = sorted(intervals)
    out: list[list[int]] = [[intervals_sorted[0][0], intervals_sorted[0][1]]]
    for s, e in intervals_sorted[1:]:
        ms, me = out[-1]
        if s <= me + 1:
            out[-1][1] = max(me, e)
        else:
            out.append([s, e])
    return [(s, e) for s, e in out]


def subtract_iv(iv: tuple[int, int], covered: list[tuple[int, int]]) -> list[tuple[int, int]]:
    """Subtract covered inclusive intervals from a target inclusive interval."""
    s, e = iv
    if s > e:
        return []
    if not covered:
        return [iv]
    res: list[tuple[int, int]] = []
    cur = s
    for cs, ce in covered:
        if ce < cur:
            continue
        if cs > e:
            break
        if cs > cur:
            res.append((cur, min(e, cs - 1)))
        cur = max(cur, ce + 1)
        if cur > e:
            break
    if cur <= e:
        res.append((cur, e))
    return res


def load_done_coverage(
    storage: IChunkStorage,
    event_names: list[str],
) -> list[tuple[int, int]]:
    """Load done block ranges by scanning chunk files in storage.

    A block range [from, to] is considered 'done' iff a chunk file exists
    for ALL event types in `event_names`. This guarantees that partial
    writes (crash mid-chunk) are detected and retried.

    Parameters
    ----------
    storage : IChunkStorage
        The storage backend to scan (local or S3).
    event_names : list[str]
        Names of all event types in the registry.

    Returns
    -------
    list[tuple[int, int]]
        Merged list of completed inclusive block ranges.
    """
    if not event_names:
        return []

    per_event: list[set[tuple[int, int]]] = []
    for ev in event_names:
        chunks: set[tuple[int, int]] = set()
        for key in storage.list_keys(f"{ev}/"):
            parsed = parse_chunk_key(key)
            if parsed is not None:
                chunks.add(parsed)
        per_event.append(chunks)

    # A chunk is done only if it appears in every event's set
    done = per_event[0].intersection(*per_event[1:])
    return merge_intervals(sorted(done))


def to_hex_block(block: int | str) -> str:
    """Convert block number to hex string if integer, else return as is."""
    if isinstance(block, int):
        return hex(block)
    return block
