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
from dataclasses import dataclass

from defind.core.interfaces import IChunkStorage
from defind.storage.chunks import parse_chunk_key

_COVERAGE_INDEX_KEY = "_meta/coverage_index.json"
_COVERAGE_INDEX_VERSION = 1


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


def intersect_intervals(
    left: list[tuple[int, int]],
    right: list[tuple[int, int]],
) -> list[tuple[int, int]]:
    """Intersect two sorted inclusive interval lists."""
    if not left or not right:
        return []

    out: list[tuple[int, int]] = []
    i, j = 0, 0
    while i < len(left) and j < len(right):
        ls, le = left[i]
        rs, re = right[j]
        s = max(ls, rs)
        e = min(le, re)
        if s <= e:
            out.append((s, e))
        if le < re:
            i += 1
        else:
            j += 1
    return merge_intervals(out)


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

    per_event: list[list[tuple[int, int]]] = []
    for ev in event_names:
        chunks: list[tuple[int, int]] = []
        for key in storage.list_keys(f"{ev}/"):
            parsed = parse_chunk_key(key)
            if parsed is not None:
                chunks.append(parsed)
        per_event.append(merge_intervals(chunks))

    # A block is done only if it is covered by every event family.
    done = per_event[0]
    for cov in per_event[1:]:
        done = intersect_intervals(done, cov)
        if not done:
            break
    return done


def load_done_chunks(
    storage: IChunkStorage,
    event_names: list[str],
) -> list[tuple[int, int]]:
    """Load exact done chunk intervals present for ALL event names."""
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

    done = per_event[0].intersection(*per_event[1:])
    return sorted(done)


def load_done_chunks_from_index(
    storage: IChunkStorage,
    event_names: list[str],
) -> list[tuple[int, int]] | None:
    """Load done chunks from persisted coverage index if compatible."""
    data = storage.read_json(_COVERAGE_INDEX_KEY)
    if not isinstance(data, dict):
        return None

    if data.get("version") != _COVERAGE_INDEX_VERSION:
        return None

    expected_events = sorted(event_names)
    indexed_events = data.get("event_names")
    if not isinstance(indexed_events, list) or sorted(indexed_events) != expected_events:
        return None

    raw_chunks = data.get("done_chunks")
    if not isinstance(raw_chunks, list):
        return None

    parsed: list[tuple[int, int]] = []
    try:
        for iv in raw_chunks:
            if not (isinstance(iv, list) and len(iv) == 2):
                return None
            a, b = int(iv[0]), int(iv[1])
            if a > b:
                return None
            parsed.append((a, b))
    except Exception:
        return None

    return sorted(set(parsed))


def save_done_chunks_to_index(
    storage: IChunkStorage,
    event_names: list[str],
    done_chunks: list[tuple[int, int]],
) -> None:
    """Persist done chunks as a compact JSON index."""
    payload = {
        "version": _COVERAGE_INDEX_VERSION,
        "event_names": sorted(event_names),
        "done_chunks": [[a, b] for a, b in sorted(set(done_chunks))],
    }
    storage.write_json(_COVERAGE_INDEX_KEY, payload)


@dataclass(frozen=True)
class RangeCompletion:
    """Completion report for a requested inclusive block range."""

    start: int
    end: int
    covered: list[tuple[int, int]]
    missing: list[tuple[int, int]]

    @property
    def is_complete(self) -> bool:
        return len(self.missing) == 0


def check_range_completion(
    *,
    storage: IChunkStorage,
    event_names: list[str],
    start: int,
    end: int,
) -> RangeCompletion:
    """Return whether [start, end] is fully covered by existing chunks."""
    covered = load_done_coverage(storage, event_names)
    missing = subtract_iv((start, end), covered)
    return RangeCompletion(
        start=start,
        end=end,
        covered=covered,
        missing=missing,
    )
