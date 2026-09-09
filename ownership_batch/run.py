from __future__ import annotations

import asyncio
import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import click
from rich.console import Console
from rich.panel import Panel
from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn, TimeElapsedColumn
from rich.table import Table

from defind.abi_events import make_event_registry_from_abi
from defind.api.ops.shared.network import fetch_etherscan_abi
from defind.api.ops.shared.utils import exception_detail, load_ops_api_config_from_env, normalize_etherscan_endpoint
from defind.clients.rpc import RPC, is_hex_address
from defind.core.config import OrchestratorConfig
from defind.decoding.specs import EventRegistry
from defind.orchestration.orchestrator import fetch_decode

console = Console()
DEFAULT_OUTPUT_ROOT = Path("ownership_batch/data")
DEFAULT_STEP = 7_000
DEFAULT_CHUNK_SIZE = 200_000
DEFAULT_CONCURRENCY = 8
DEFAULT_EVENTS = (
    "OwnershipTransferred",
    "OwnershipTransferStarted",
    "AdminChanged",
    "RoleAdminChanged",
    "RoleGranted",
    "RoleRevoked",
    "DefaultAdminTransferScheduled",
    "DefaultAdminTransferCanceled",
    "DefaultAdminDelayChangeScheduled",
    "DefaultAdminDelayChangeCanceled",
)


@dataclass(frozen=True)
class ContractRow:
    address: str
    protocol_slug: str
    contract_slug: str
    start_block: int
    end_block: int | str


def _clean_text(value: Any, *, label: str) -> str:
    cleaned = str(value or "").strip()
    if not cleaned:
        raise click.ClickException(f"{label} must not be empty")
    return cleaned


def _parse_block_value(value: Any, *, label: str) -> int | str:
    cleaned = _clean_text(value, label=label)
    lowered = cleaned.lower()
    if lowered == "latest":
        return "latest"
    try:
        parsed = int(cleaned, 0)
    except Exception as exc:
        raise click.ClickException(f"{label} must be an integer-like value or 'latest'") from exc
    if parsed < 0:
        raise click.ClickException(f"{label} must be >= 0")
    return parsed


def _parse_contract_row(raw_row: dict[str, str], *, row_number: int) -> ContractRow:
    address = _clean_text(raw_row.get("address"), label=f"row {row_number} address").lower()
    if not is_hex_address(address):
        raise click.ClickException(f"row {row_number} address must be a 0x-prefixed 40-hex Ethereum address")

    protocol_slug = _clean_text(raw_row.get("protocol_slug"), label=f"row {row_number} protocol_slug")
    contract_slug = _clean_text(raw_row.get("contract_slug"), label=f"row {row_number} contract_slug")
    start_block = _parse_block_value(raw_row.get("start_block"), label=f"row {row_number} start_block")
    if not isinstance(start_block, int):
        raise click.ClickException(f"row {row_number} start_block must be an integer")

    raw_end_block = raw_row.get("end_block")
    end_block = "latest" if raw_end_block in {None, ""} else _parse_block_value(raw_end_block, label=f"row {row_number} end_block")
    if isinstance(end_block, int) and start_block > end_block:
        raise click.ClickException(f"row {row_number} start_block must be <= end_block")

    return ContractRow(
        address=address,
        protocol_slug=protocol_slug,
        contract_slug=contract_slug,
        start_block=start_block,
        end_block=end_block,
    )


def load_contract_rows(csv_path: Path) -> list[ContractRow]:
    try:
        with csv_path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            if reader.fieldnames is None:
                raise click.ClickException("CSV file is missing a header row")
            required = {"address", "protocol_slug", "contract_slug", "start_block"}
            missing = sorted(required.difference({name.strip() for name in reader.fieldnames if name}))
            if missing:
                raise click.ClickException(f"CSV file is missing required columns: {', '.join(missing)}")
            rows = [_parse_contract_row(row, row_number=index) for index, row in enumerate(reader, start=2)]
    except click.ClickException:
        raise
    except FileNotFoundError as exc:
        raise click.ClickException(f"CSV file not found: {csv_path}") from exc
    except OSError as exc:
        raise click.ClickException(f"Unable to read CSV file {csv_path}: {exc}") from exc

    if not rows:
        raise click.ClickException("CSV file contains no contracts")
    return rows


def filter_registry_for_ownership_events(
    registry: EventRegistry,
    *,
    allowed_event_names: set[str],
) -> EventRegistry:
    return {
        topic0: spec
        for topic0, spec in registry.items()
        if spec.name in allowed_event_names
    }


def _storage_target_label(
    *,
    storage: str,
    out_root: Path,
    s3_bucket: str | None,
    s3_prefix: str,
    protocol_slug: str,
    contract_slug: str,
) -> str:
    contract_subpath = f"{protocol_slug}/{contract_slug}"
    if storage == "s3":
        if not s3_bucket:
            raise click.ClickException(
                "storage=s3 requires S3 bucket configuration in .env "
                "(S3_BUCKET or DEFIND_API_S3_BUCKET)"
            )
        prefix = f"{s3_prefix.rstrip('/')}/{contract_subpath}/" if s3_prefix else f"{contract_subpath}/"
        return f"s3://{s3_bucket}/{prefix}"
    return str(out_root / protocol_slug / contract_slug)


def _render_summary(
    *,
    csv_path: Path,
    rpc_url: str,
    chain_id: int,
    storage: str,
    target_root: str,
    row_count: int,
    event_names: list[str],
) -> Panel:
    table = Table(show_header=False)
    table.add_column("Key", style="bold cyan")
    table.add_column("Value")
    table.add_row("CSV", str(csv_path))
    table.add_row("RPC", rpc_url)
    table.add_row("Chain ID", str(chain_id))
    table.add_row("Storage", storage)
    table.add_row("Target Root", target_root)
    table.add_row("Contracts", str(row_count))
    table.add_row("Events", ", ".join(event_names))
    return Panel.fit(table, title="Ownership Batch", border_style="blue")


async def _fetch_filtered_registry(
    *,
    address: str,
    chain_id: int,
    allowed_event_names: set[str],
) -> tuple[EventRegistry, list[str]]:
    env_cfg = load_ops_api_config_from_env()
    endpoint_url = normalize_etherscan_endpoint(env_cfg.etherscan_api_url)
    abi_json = await fetch_etherscan_abi(
        endpoint_url=endpoint_url,
        address=address,
        chain_id=chain_id,
        api_key=(env_cfg.etherscan_api_key or "").strip() or None,
    )
    full_registry = make_event_registry_from_abi(abi_json)
    filtered_registry = filter_registry_for_ownership_events(
        full_registry,
        allowed_event_names=allowed_event_names,
    )
    event_names = sorted({spec.name for spec in filtered_registry.values()})
    return filtered_registry, event_names


async def _run_batch(
    *,
    csv_path: Path,
    rpc_url: str,
    storage: str,
    out_root: Path,
    step: int,
    chunk_size: int,
    concurrency: int,
    allowed_event_names: set[str],
    dry_run: bool,
) -> None:
    rows = load_contract_rows(csv_path)
    env_cfg = load_ops_api_config_from_env()

    async with RPC(rpc_url, timeout_s=90, max_connections=8, max_retries=5, retry_backoff_s=1.0) as rpc:
        chain_id = await rpc.chain_id()

    target_root = _storage_target_label(
        storage=storage,
        out_root=out_root,
        s3_bucket=env_cfg.s3_bucket if storage == "s3" else None,
        s3_prefix=env_cfg.s3_prefix if storage == "s3" else "",
        protocol_slug="*",
        contract_slug="*",
    ).removesuffix("*/*")
    console.print(
        _render_summary(
            csv_path=csv_path,
            rpc_url=rpc_url,
            chain_id=chain_id,
            storage=storage,
            target_root=target_root,
            row_count=len(rows),
            event_names=sorted(allowed_event_names),
        )
    )

    report_rows: list[dict[str, Any]] = []
    progress = Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
        TimeElapsedColumn(),
        console=console,
        transient=False,
    )

    with progress:
        task_id = progress.add_task("Batch indexing", total=len(rows))
        for row in rows:
            status_text = f"{row.protocol_slug}/{row.contract_slug}"
            progress.update(task_id, description=f"Processing {status_text}")
            try:
                registry, event_names = await _fetch_filtered_registry(
                    address=row.address,
                    chain_id=chain_id,
                    allowed_event_names=allowed_event_names,
                )
                target = _storage_target_label(
                    storage=storage,
                    out_root=out_root,
                    s3_bucket=env_cfg.s3_bucket if storage == "s3" else None,
                    s3_prefix=env_cfg.s3_prefix if storage == "s3" else "",
                    protocol_slug=row.protocol_slug,
                    contract_slug=row.contract_slug,
                )

                if not registry:
                    report_rows.append(
                        {
                            "address": row.address,
                            "protocol_slug": row.protocol_slug,
                            "contract_slug": row.contract_slug,
                            "start_block": row.start_block,
                            "end_block": row.end_block,
                            "status": "skipped",
                            "reason": "no ownership/admin events found in ABI",
                            "event_names": [],
                            "target": target,
                        }
                    )
                    progress.advance(task_id)
                    continue

                if dry_run:
                    report_rows.append(
                        {
                            "address": row.address,
                            "protocol_slug": row.protocol_slug,
                            "contract_slug": row.contract_slug,
                            "start_block": row.start_block,
                            "end_block": row.end_block,
                            "status": "dry_run",
                            "reason": "",
                            "event_names": event_names,
                            "target": target,
                        }
                    )
                    progress.advance(task_id)
                    continue

                config = OrchestratorConfig(
                    rpc_url=rpc_url,
                    address=row.address,
                    topic0s=list(registry.keys()),
                    start_block=row.start_block,
                    end_block=row.end_block,
                    protocol_slug=row.protocol_slug,
                    contract_slug=row.contract_slug,
                    step=step,
                    chunk_size=chunk_size,
                    concurrency=concurrency,
                    log_level="WARNING",
                    log_json=False,
                    out_root=out_root,
                    s3_bucket=env_cfg.s3_bucket if storage == "s3" else None,
                    s3_prefix=env_cfg.s3_prefix if storage == "s3" else "",
                    s3_endpoint_url=env_cfg.s3_endpoint_url if storage == "s3" else None,
                    s3_access_key=env_cfg.s3_access_key if storage == "s3" else None,
                    s3_secret_key=env_cfg.s3_secret_key if storage == "s3" else None,
                    s3_region=env_cfg.s3_region if storage == "s3" else "auto",
                    s3_max_retries=env_cfg.s3_max_retries,
                    s3_retry_backoff_s=env_cfg.s3_retry_backoff_s,
                )
                output = await fetch_decode(config=config, registry=registry)
                report_rows.append(
                    {
                        "address": row.address,
                        "protocol_slug": row.protocol_slug,
                        "contract_slug": row.contract_slug,
                        "start_block": row.start_block,
                        "end_block": row.end_block,
                        "status": "indexed",
                        "reason": "",
                        "event_names": event_names,
                        "target": output.contract_dir,
                        "chunks_written": output.stats.chunks_written,
                        "total_logs": output.stats.total_logs,
                    }
                )
            except Exception as exc:
                report_rows.append(
                    {
                        "address": row.address,
                        "protocol_slug": row.protocol_slug,
                        "contract_slug": row.contract_slug,
                        "start_block": row.start_block,
                        "end_block": row.end_block,
                        "status": "error",
                        "reason": exception_detail(exc),
                        "event_names": [],
                        "target": "",
                    }
                )
            progress.advance(task_id)
        progress.update(task_id, description="[green]Batch complete[/green]")

    report = {
        "csv_path": str(csv_path),
        "rpc_url": rpc_url,
        "storage": storage,
        "out_root": str(out_root),
        "allowed_event_names": sorted(allowed_event_names),
        "contracts": report_rows,
    }

    report_path = out_root / "_meta" / "ownership_batch_report.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")

    status_table = Table(show_header=True)
    status_table.add_column("Status", style="bold")
    status_table.add_column("Contract")
    status_table.add_column("Events")
    status_table.add_column("Reason")
    for row in report_rows:
        status_table.add_row(
            str(row.get("status") or ""),
            f"{row.get('protocol_slug')}/{row.get('contract_slug')}",
            ", ".join(row.get("event_names") or []),
            str(row.get("reason") or ""),
        )
    console.print(status_table)
    console.print(Panel.fit(str(report_path), title="Batch Report", border_style="green"))


@click.command(help="Batch index ownership/admin events for contracts listed in a CSV file.")
@click.option(
    "--csv-path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=True,
    help="Input CSV file with one contract per row.",
)
@click.option("--rpc-url", required=True, help="RPC endpoint URL.")
@click.option(
    "--storage",
    type=click.Choice(("local", "s3"), case_sensitive=False),
    default="local",
    show_default=True,
    help="Storage backend.",
)
@click.option(
    "--out-root",
    type=click.Path(file_okay=False, path_type=Path),
    default=DEFAULT_OUTPUT_ROOT,
    show_default=True,
    help="Local output root for ownership datasets and report.",
)
@click.option("--step", type=click.IntRange(min=1), default=DEFAULT_STEP, show_default=True)
@click.option("--chunk-size", type=click.IntRange(min=1), default=DEFAULT_CHUNK_SIZE, show_default=True)
@click.option("--concurrency", type=click.IntRange(min=1), default=DEFAULT_CONCURRENCY, show_default=True)
@click.option(
    "--event-name",
    "event_names",
    multiple=True,
    help="Optional override. Repeat to keep only selected event names.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Resolve ABI and selected events without indexing logs.",
)
def cli(
    *,
    csv_path: Path,
    rpc_url: str,
    storage: str,
    out_root: Path,
    step: int,
    chunk_size: int,
    concurrency: int,
    event_names: tuple[str, ...],
    dry_run: bool,
) -> None:
    allowed_event_names = {name.strip() for name in (event_names or DEFAULT_EVENTS) if name.strip()}
    try:
        asyncio.run(
            _run_batch(
                csv_path=csv_path,
                rpc_url=rpc_url.strip(),
                storage=storage.lower(),
                out_root=out_root,
                step=int(step),
                chunk_size=int(chunk_size),
                concurrency=int(concurrency),
                allowed_event_names=allowed_event_names,
                dry_run=bool(dry_run),
            )
        )
    except click.ClickException as exc:
        console.print(Panel.fit(str(exc), title="Ownership Batch Failed", border_style="red"))
        raise SystemExit(1) from exc
    except KeyboardInterrupt:
        console.print(Panel.fit("Interrupted by user.", title="Ownership Batch Stopped", border_style="yellow"))
        raise SystemExit(130)


if __name__ == "__main__":
    cli()
