from __future__ import annotations

from pathlib import Path

from ownership_batch.run import (
    filter_registry_for_ownership_events,
    load_contract_rows,
)


def test_load_contract_rows_parses_latest_end_block(tmp_path: Path) -> None:
    csv_path = tmp_path / "contracts.csv"
    csv_path.write_text(
        "\n".join(
            (
                "address,protocol_slug,contract_slug,start_block,end_block",
                "0x820FB8127a689327C863de8433278d6181123982,arrakis,meta_vault_factory,20540819,latest",
            )
        ),
        encoding="utf-8",
    )

    rows = load_contract_rows(csv_path)

    assert len(rows) == 1
    assert rows[0].address == "0x820fb8127a689327c863de8433278d6181123982"
    assert rows[0].protocol_slug == "arrakis"
    assert rows[0].contract_slug == "meta_vault_factory"
    assert rows[0].start_block == 20540819
    assert rows[0].end_block == "latest"


def test_filter_registry_for_ownership_events_keeps_only_allowed_names() -> None:
    registry = {
        "0x01": type("Spec", (), {"name": "OwnershipTransferred"})(),
        "0x02": type("Spec", (), {"name": "Transfer"})(),
        "0x03": type("Spec", (), {"name": "RoleGranted"})(),
    }

    filtered = filter_registry_for_ownership_events(
        registry,
        allowed_event_names={"OwnershipTransferred", "RoleGranted"},
    )

    assert sorted(filtered.keys()) == ["0x01", "0x03"]
