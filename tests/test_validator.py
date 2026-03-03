from __future__ import annotations

from unittest.mock import MagicMock

from defind.orchestration.validator import validate_coverage


def _storage(keys_by_prefix: dict[str, list[str]], index_payload: dict | None) -> MagicMock:
    storage = MagicMock()
    storage.list_keys.side_effect = lambda prefix: keys_by_prefix.get(prefix, [])
    storage.read_json.return_value = index_payload
    return storage


def test_validate_coverage_happy_path() -> None:
    storage = _storage(
        keys_by_prefix={
            "Mint/": [
                "Mint/chunk_0000000000_0000000099.parquet",
                "Mint/chunk_0000000100_0000000199.parquet",
            ],
            "Burn/": [
                "Burn/chunk_0000000000_0000000099.parquet",
                "Burn/chunk_0000000100_0000000199.parquet",
            ],
        },
        index_payload={
            "version": 1,
            "event_names": ["Burn", "Mint"],
            "done_chunks": [[0, 99], [100, 199]],
        },
    )

    report = validate_coverage(
        storage=storage,
        event_names=["Mint", "Burn"],
        start_block=0,
        end_block=199,
    )

    assert report.is_valid is True
    assert report.index_matches_scan is True
    assert report.missing_in_range == []
    assert report.overlaps_by_event == {"Mint": [], "Burn": []}
    assert report.event_mismatch_by_event == {"Mint": [], "Burn": []}


def test_validate_coverage_detects_overlap_mismatch_and_holes() -> None:
    storage = _storage(
        keys_by_prefix={
            "Mint/": [
                "Mint/chunk_0000000000_0000000099.parquet",
                "Mint/chunk_0000000100_0000000199.parquet",
            ],
            "Burn/": [
                "Burn/chunk_0000000000_0000000099.parquet",
                "Burn/chunk_0000000100_0000000200.parquet",
                "Burn/chunk_0000000150_0000000249.parquet",
            ],
        },
        index_payload=None,
    )

    report = validate_coverage(
        storage=storage,
        event_names=["Mint", "Burn"],
        start_block=0,
        end_block=249,
    )

    assert report.is_valid is False
    assert report.index_matches_scan is False
    assert report.missing_in_range == [(100, 249)]
    assert report.overlaps_by_event["Burn"] == [(100, 200), (150, 249)]
    assert report.event_mismatch_by_event["Mint"] == [(100, 200), (150, 249)]
    assert report.event_mismatch_by_event["Burn"] == [(100, 199)]
