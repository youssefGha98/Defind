from __future__ import annotations

from dataclasses import dataclass

from defind.core.interfaces import IChunkStorage
from defind.orchestration.utils import (
    load_done_chunks,
    load_done_chunks_from_index,
    merge_intervals,
    subtract_iv,
)
from defind.storage.chunks import parse_chunk_key


@dataclass(frozen=True)
class CoverageValidationReport:
    event_names: list[str]
    scanned_done_chunks: list[tuple[int, int]]
    indexed_done_chunks: list[tuple[int, int]] | None
    index_matches_scan: bool
    missing_in_range: list[tuple[int, int]]
    overlaps_by_event: dict[str, list[tuple[int, int]]]
    event_mismatch_by_event: dict[str, list[tuple[int, int]]]

    @property
    def is_valid(self) -> bool:
        if not self.index_matches_scan:
            return False
        if self.missing_in_range:
            return False
        if any(self.overlaps_by_event.values()):
            return False
        if any(self.event_mismatch_by_event.values()):
            return False
        return True


def _load_event_intervals(storage: IChunkStorage, event_name: str) -> list[tuple[int, int]]:
    intervals: set[tuple[int, int]] = set()
    for key in storage.list_keys(f"{event_name}/"):
        parsed = parse_chunk_key(key)
        if parsed is not None:
            intervals.add(parsed)
    return sorted(intervals)


def _find_overlaps(intervals: list[tuple[int, int]]) -> list[tuple[int, int]]:
    if len(intervals) < 2:
        return []

    overlaps: set[tuple[int, int]] = set()
    prev_start, prev_end = intervals[0]
    for start, end in intervals[1:]:
        if start <= prev_end:
            overlaps.add((start, end))
            overlaps.add((prev_start, prev_end))
            prev_end = max(prev_end, end)
        else:
            prev_start, prev_end = start, end
    return sorted(overlaps)


def validate_coverage(
    *,
    storage: IChunkStorage,
    event_names: list[str],
    start_block: int | None = None,
    end_block: int | None = None,
) -> CoverageValidationReport:
    """Validate chunk coverage consistency independently from orchestrator flow."""
    if not event_names:
        raise ValueError("event_names must not be empty")

    if (start_block is None) ^ (end_block is None):
        raise ValueError("start_block and end_block must be both set or both None")
    if start_block is not None and end_block is not None and start_block > end_block:
        raise ValueError("start_block must be <= end_block")

    per_event = {event: _load_event_intervals(storage, event) for event in event_names}
    overlaps_by_event = {event: _find_overlaps(intervals) for event, intervals in per_event.items()}

    union_chunks: set[tuple[int, int]] = set()
    for chunks in per_event.values():
        union_chunks.update(chunks)
    event_mismatch_by_event = {event: sorted(union_chunks - set(chunks)) for event, chunks in per_event.items()}

    scanned_done_chunks = load_done_chunks(storage, event_names)
    indexed_done_chunks = load_done_chunks_from_index(storage, event_names)
    index_matches_scan = indexed_done_chunks == scanned_done_chunks

    missing_in_range: list[tuple[int, int]] = []
    if start_block is not None and end_block is not None:
        covered = merge_intervals(scanned_done_chunks)
        missing_in_range = subtract_iv((start_block, end_block), covered)

    return CoverageValidationReport(
        event_names=list(event_names),
        scanned_done_chunks=scanned_done_chunks,
        indexed_done_chunks=indexed_done_chunks,
        index_matches_scan=index_matches_scan,
        missing_in_range=missing_in_range,
        overlaps_by_event=overlaps_by_event,
        event_mismatch_by_event=event_mismatch_by_event,
    )
