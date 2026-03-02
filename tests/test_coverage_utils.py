from unittest.mock import MagicMock

from defind.orchestration.utils import check_range_completion, load_done_coverage


def _mock_storage(keys_by_prefix: dict[str, list[str]]) -> MagicMock:
    storage = MagicMock()
    storage.list_keys.side_effect = lambda prefix: keys_by_prefix.get(prefix, [])
    return storage


def test_load_done_coverage_handles_misaligned_intervals() -> None:
    storage = _mock_storage(
        {
            "Mint/": [
                "Mint/chunk_0000000000_0000000049.parquet",
                "Mint/chunk_0000000050_0000000099.parquet",
            ],
            "Burn/": [
                "Burn/chunk_0000000000_0000000099.parquet",
            ],
        }
    )

    done = load_done_coverage(storage, ["Mint", "Burn"])
    assert done == [(0, 99)]


def test_check_range_completion_reports_holes() -> None:
    storage = _mock_storage(
        {
            "Mint/": [
                "Mint/chunk_0000000010_0000000020.parquet",
                "Mint/chunk_0000000030_0000000040.parquet",
            ],
            "Burn/": [
                "Burn/chunk_0000000010_0000000020.parquet",
                "Burn/chunk_0000000035_0000000040.parquet",
            ],
        }
    )

    report = check_range_completion(
        storage=storage,
        event_names=["Mint", "Burn"],
        start=10,
        end=40,
    )

    assert report.is_complete is False
    assert report.covered == [(10, 20), (35, 40)]
    assert report.missing == [(21, 34)]
