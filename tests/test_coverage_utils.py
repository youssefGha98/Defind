from unittest.mock import MagicMock

from defind.orchestration.utils import (
    check_range_completion,
    load_done_chunks_from_index,
    load_done_coverage,
    save_done_chunks_to_index,
)


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


def test_load_done_chunks_from_index_compatible() -> None:
    storage = MagicMock()
    storage.read_json.return_value = {
        "version": 1,
        "event_names": ["Burn", "Mint"],
        "done_chunks": [[0, 99], [100, 199]],
    }

    out = load_done_chunks_from_index(storage, ["Mint", "Burn"])
    assert out == [(0, 99), (100, 199)]


def test_load_done_chunks_from_index_incompatible_events() -> None:
    storage = MagicMock()
    storage.read_json.return_value = {
        "version": 1,
        "event_names": ["Swap"],
        "done_chunks": [[0, 99]],
    }

    out = load_done_chunks_from_index(storage, ["Mint", "Burn"])
    assert out is None


def test_load_done_chunks_from_index_invalid_version() -> None:
    storage = MagicMock()
    storage.read_json.return_value = {
        "version": 999,
        "event_names": ["Burn", "Mint"],
        "done_chunks": [[0, 99]],
    }
    assert load_done_chunks_from_index(storage, ["Mint", "Burn"]) is None


def test_load_done_chunks_from_index_invalid_shapes() -> None:
    storage = MagicMock()
    storage.read_json.return_value = {
        "version": 1,
        "event_names": ["Burn", "Mint"],
        "done_chunks": [0, 99],  # not list[list[int, int]]
    }
    assert load_done_chunks_from_index(storage, ["Mint", "Burn"]) is None

    storage.read_json.return_value = {
        "version": 1,
        "event_names": ["Burn", "Mint"],
        "done_chunks": [["a", 99]],  # invalid int conversion
    }
    assert load_done_chunks_from_index(storage, ["Mint", "Burn"]) is None

    storage.read_json.return_value = {
        "version": 1,
        "event_names": ["Burn", "Mint"],
        "done_chunks": [[100, 99]],  # a > b
    }
    assert load_done_chunks_from_index(storage, ["Mint", "Burn"]) is None


def test_save_done_chunks_to_index_writes_expected_payload() -> None:
    storage = MagicMock()
    save_done_chunks_to_index(storage, ["Mint", "Burn"], [(100, 199), (0, 99)])
    payload = storage.write_json.call_args.args[1]

    assert storage.write_json.call_args.args[0] == "_meta/coverage_index.json"
    assert payload["version"] == 1
    assert payload["event_names"] == ["Burn", "Mint"]
    assert payload["done_chunks"] == [[0, 99], [100, 199]]
