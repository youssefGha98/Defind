from __future__ import annotations

import asyncio
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from urllib.parse import urlsplit

import click
from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn
from rich.table import Table

from defind.abi_events import make_event_registry_from_abi
from defind.api.ops.shared.network import fetch_etherscan_abi
from defind.api.ops.shared.utils import exception_detail, load_ops_api_config_from_env, normalize_etherscan_endpoint
from defind.clients.rpc import RPC, is_hex_address
from defind.core.config import OrchestratorConfig
from defind.decoding.specs import EventRegistry
from defind.orchestration.orchestrator import FetchDecodeOutput, fetch_decode

console = Console()
_DEFAULT_STEP = 10_000
_DEFAULT_CHUNK_SIZE = 1_000_000
_DEFAULT_CONCURRENCY = 15
_DEFAULT_TIMEOUT_S = int(OrchestratorConfig.__dataclass_fields__["timeout_s"].default)
_DEFAULT_RPC_MAX_RETRIES = int(OrchestratorConfig.__dataclass_fields__["rpc_max_retries"].default)
_DEFAULT_RPC_RETRY_BACKOFF_S = float(OrchestratorConfig.__dataclass_fields__["rpc_retry_backoff_s"].default)
_DEFAULT_S3_REGION = str(OrchestratorConfig.__dataclass_fields__["s3_region"].default)
_DEFAULT_S3_MAX_RETRIES = int(OrchestratorConfig.__dataclass_fields__["s3_max_retries"].default)
_DEFAULT_S3_RETRY_BACKOFF_S = float(OrchestratorConfig.__dataclass_fields__["s3_retry_backoff_s"].default)


def _clean_optional(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = value.strip()
    return cleaned if cleaned else None


def _redact_url(url: str) -> str:
    cleaned = (url or "").strip()
    if not cleaned:
        return ""
    parts = urlsplit(cleaned)
    if not parts.scheme or not parts.hostname:
        return cleaned
    port = f":{parts.port}" if parts.port is not None else ""
    return f"{parts.scheme}://{parts.hostname}{port}"


def _parse_end_block(raw: str) -> int | str:
    cleaned = raw.strip()
    if not cleaned:
        return "latest"
    if cleaned.lower() == "latest":
        return "latest"
    try:
        parsed = int(cleaned, 0)
    except Exception as exc:
        raise click.ClickException("end-block must be an integer-like value or 'latest'") from exc
    if parsed < 0:
        raise click.ClickException("end-block must be >= 0")
    return parsed


def _normalize_required_text(value: str, *, label: str) -> str:
    cleaned = value.strip()
    if not cleaned:
        raise click.ClickException(f"{label} must not be empty")
    return cleaned


def _normalize_contract_address(address: str) -> str:
    cleaned = _normalize_required_text(address, label="address")
    if not is_hex_address(cleaned):
        raise click.ClickException("address must be a 0x-prefixed 40-hex Ethereum address")
    return cleaned.lower()


def _validate_block_window(*, start_block: int | str, end_block: int | str) -> None:
    if isinstance(start_block, int) and isinstance(end_block, int) and start_block > end_block:
        raise click.ClickException("start-block must be <= end-block")


def _parse_start_block(raw: str) -> int | str:
    cleaned = raw.strip()
    if not cleaned:
        raise click.ClickException("start-block must not be empty")
    lowered = cleaned.lower()
    if lowered in {"auto", "creation"}:
        return "auto"
    if lowered in {"earliest", "genesis"}:
        return "earliest"
    try:
        parsed = int(cleaned, 0)
    except Exception as exc:
        raise click.ClickException("start-block must be an integer-like value or 'auto'") from exc
    if parsed < 0:
        raise click.ClickException("start-block must be >= 0")
    return parsed


def _has_code(value: str) -> bool:
    cleaned = (value or "").strip().lower()
    return cleaned not in {"", "0x", "0x0"}


async def _resolve_contract_creation_block(
    *,
    rpc: RPC,
    address: str,
    latest_block: int,
) -> int:
    code_latest = await rpc.get_code(address=address, block=latest_block)
    if not _has_code(code_latest):
        raise click.ClickException("contract bytecode not found at latest block; check address or chain")

    lo = 0
    hi = latest_block
    while lo < hi:
        mid = (lo + hi) // 2
        code_mid = await rpc.get_code(address=address, block=mid)
        if _has_code(code_mid):
            hi = mid
        else:
            lo = mid + 1
    return lo


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


def _render_run_summary(
    *,
    rpc_url: str,
    address: str,
    chain_id: int,
    start_block: int,
    end_block: int | str,
    mode: str,
    storage: str,
    target: str,
    step: int,
    chunk_size: int,
    concurrency: int,
    protocol_slug: str,
    contract_slug: str,
    event_names: list[str],
) -> Panel:
    table = Table(box=box.SIMPLE_HEAVY, show_header=False, expand=False)
    table.add_column("Key", style="bold cyan")
    table.add_column("Value", style="white")
    table.add_row("Dataset", f"{protocol_slug}/{contract_slug}")
    table.add_row("Address", address)
    table.add_row("RPC", _redact_url(rpc_url))
    table.add_row("Chain ID", str(chain_id))
    table.add_row("Blocks", f"{start_block:,} -> {end_block if isinstance(end_block, str) else f'{end_block:,}'}")
    table.add_row("Mode", mode)
    table.add_row("Storage", storage)
    table.add_row("Target", target)
    table.add_row("Step", f"{step:,}")
    table.add_row("Chunk Size", f"{chunk_size:,}")
    table.add_row("Concurrency", str(concurrency))
    table.add_row("Events", f"{len(event_names)} loaded")
    if event_names:
        preview = ", ".join(event_names[:6])
        if len(event_names) > 6:
            preview = f"{preview}, +{len(event_names) - 6} more"
        table.add_row("Event Names", preview)
    return Panel.fit(table, title="Defind Run", border_style="blue")


def _render_success_summary(output: FetchDecodeOutput) -> Panel:
    stats = output.stats
    table = Table(box=box.SIMPLE_HEAVY, show_header=False, expand=False)
    table.add_column("Key", style="bold green")
    table.add_column("Value", style="white")
    table.add_row("Output", output.contract_dir)
    table.add_row("Chunks Written", f"{stats.chunks_written:,}")
    table.add_row("Logs Fetched", f"{stats.total_logs:,}")
    table.add_row("Seeds OK", f"{stats.processed_ok:,}")
    table.add_row("Seeds Failed", f"{stats.processed_failed:,}")
    table.add_row("Subranges", f"{stats.executed_subranges:,}")
    table.add_row("Split Retries", f"{stats.partially_covered_split:,}")
    return Panel.fit(table, title="Run Complete", border_style="green")


def _render_error(message: str, *, title: str = "Run Failed", style: str = "red") -> None:
    console.print(Panel.fit(message, title=title, border_style=style))


async def _prepare_registry_and_config(
    *,
    rpc_url: str,
    address: str,
    abi_path: Path | None,
    start_block: int | str,
    end_block: int | str,
    protocol_slug: str,
    contract_slug: str,
    step: int,
    chunk_size: int,
    concurrency: int,
    mode: str,
    storage: str,
) -> tuple[OrchestratorConfig, EventRegistry, list[str], int, int | str]:
    env_cfg = load_ops_api_config_from_env()
    resolved_end: int | str = end_block
    resolved_start: int | str = start_block

    async with RPC(
        rpc_url,
        timeout_s=_DEFAULT_TIMEOUT_S,
        max_connections=8,
        max_retries=_DEFAULT_RPC_MAX_RETRIES,
        retry_backoff_s=_DEFAULT_RPC_RETRY_BACKOFF_S,
    ) as rpc:
        chain_id = await rpc.chain_id()
        if end_block == "latest":
            resolved_end = await rpc.latest_block()
        if start_block == "auto":
            if not isinstance(resolved_end, int):
                resolved_end = await rpc.latest_block()
            resolved_start = await _resolve_contract_creation_block(
                rpc=rpc,
                address=address,
                latest_block=resolved_end,
            )

    try:
        if abi_path is not None:
            registry = make_event_registry_from_abi(abi_path)
        else:
            endpoint_url = normalize_etherscan_endpoint(env_cfg.etherscan_api_url)
            abi_json = await fetch_etherscan_abi(
                endpoint_url=endpoint_url,
                address=address,
                chain_id=chain_id,
                api_key=_clean_optional(env_cfg.etherscan_api_key),
            )
            registry = make_event_registry_from_abi(abi_json)
    except click.ClickException:
        raise
    except Exception as exc:
        source_label = f"local ABI file {abi_path}" if abi_path is not None else "Etherscan ABI"
        raise click.ClickException(f"unable to load {source_label}: {exception_detail(exc)}") from exc

    if not registry:
        if abi_path is not None:
            raise click.ClickException(f"ABI file contains no decodable events: {abi_path}")
        raise click.ClickException("Etherscan ABI contains no decodable events")

    s3_bucket = env_cfg.s3_bucket if storage == "s3" else None
    s3_prefix = env_cfg.s3_prefix if storage == "s3" else ""
    s3_endpoint_url = env_cfg.s3_endpoint_url if storage == "s3" else None
    s3_access_key = env_cfg.s3_access_key if storage == "s3" else None
    s3_secret_key = env_cfg.s3_secret_key if storage == "s3" else None
    s3_region = env_cfg.s3_region if storage == "s3" else _DEFAULT_S3_REGION
    _storage_target_label(
        storage=storage,
        out_root=env_cfg.out_root,
        s3_bucket=s3_bucket,
        s3_prefix=s3_prefix,
        protocol_slug=protocol_slug,
        contract_slug=contract_slug,
    )

    config = OrchestratorConfig(
        rpc_url=rpc_url,
        address=address,
        topic0s=list(registry.keys()),
        start_block=resolved_start,
        end_block=end_block,
        protocol_slug=protocol_slug,
        contract_slug=contract_slug,
        step=step,
        chunk_size=chunk_size,
        concurrency=concurrency,
        listen=mode in {"listen", "both"},
        log_level="WARNING",
        log_json=False,
        out_root=env_cfg.out_root,
        s3_bucket=s3_bucket,
        s3_prefix=s3_prefix,
        s3_endpoint_url=s3_endpoint_url,
        s3_access_key=s3_access_key,
        s3_secret_key=s3_secret_key,
        s3_region=s3_region,
        s3_max_retries=env_cfg.s3_max_retries if storage == "s3" else _DEFAULT_S3_MAX_RETRIES,
        s3_retry_backoff_s=env_cfg.s3_retry_backoff_s if storage == "s3" else _DEFAULT_S3_RETRY_BACKOFF_S,
    )
    return config, registry, sorted(spec.name for spec in registry.values()), chain_id, resolved_end


async def _run_indexer(
    *,
    rpc_url: str,
    address: str,
    abi_path: Path | None,
    start_block: int | str,
    end_block: int | str,
    protocol_slug: str,
    contract_slug: str,
    step: int,
    chunk_size: int,
    concurrency: int,
    mode: str,
    storage: str,
) -> None:
    abi_status = "loading local ABI file" if abi_path is not None else "fetching ABI from Etherscan"
    with console.status(f"[bold cyan]Resolving RPC and {abi_status}...[/]"):
        config, registry, event_names, chain_id, resolved_end = await _prepare_registry_and_config(
            rpc_url=rpc_url,
            address=address,
            abi_path=abi_path,
            start_block=start_block,
            end_block=end_block,
            protocol_slug=protocol_slug,
            contract_slug=contract_slug,
            step=step,
            chunk_size=chunk_size,
            concurrency=concurrency,
            mode=mode,
            storage=storage,
        )

    target = _storage_target_label(
        storage=storage,
        out_root=config.out_root,
        s3_bucket=config.s3_bucket,
        s3_prefix=config.s3_prefix,
        protocol_slug=protocol_slug,
        contract_slug=contract_slug,
    )
    console.print(
        _render_run_summary(
            rpc_url=rpc_url,
            address=address,
            chain_id=chain_id,
            start_block=int(config.start_block),
            end_block=resolved_end,
            mode=mode,
            storage=storage,
            target=target,
            step=step,
            chunk_size=chunk_size,
            concurrency=concurrency,
            protocol_slug=protocol_slug,
            contract_slug=contract_slug,
            event_names=event_names,
        )
    )

    progress = Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        TextColumn("chunks [bold]{task.fields[chunks]}[/]"),
        TextColumn("last [bold]{task.fields[last_range]}[/]"),
        TimeElapsedColumn(),
        console=console,
        transient=False,
    )
    chunks_written = 0
    last_range = "-"

    async def _on_chunk_written(chunk_start: int, chunk_end: int) -> None:
        nonlocal chunks_written, last_range
        chunks_written += 1
        last_range = f"{chunk_start:,} -> {chunk_end:,}"
        progress.update(task_id, chunks=chunks_written, last_range=last_range)

    with progress:
        task_id = progress.add_task(
            "Indexing",
            total=None,
            chunks=0,
            last_range=last_range,
        )
        output = await fetch_decode(
            config=config,
            registry=registry,
            on_chunk_written=_on_chunk_written,
        )
        progress.update(task_id, description="[green]Completed[/green]", chunks=chunks_written, last_range=last_range)

    console.print(_render_success_summary(output))


@click.group(help="Defind command line interface.")
def cli() -> None:
    """Root CLI group."""


@cli.command("version", help="Print installed Defind version.")
def version_cmd() -> None:
    """Display package version."""
    try:
        console.print(version("defind"))
    except PackageNotFoundError:
        console.print("defind (editable install)")


@cli.command("run", help="Run the EVM log indexer with auto Etherscan ABI fetch or a local ABI file.")
@click.option("--rpc-url", required=True, help="RPC endpoint URL.")
@click.option("--address", required=True, help="Contract address to index.")
@click.option(
    "--abi-path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    default=None,
    help="Optional local ABI JSON file. Skips Etherscan when provided.",
)
@click.option("--start-block", required=True, help="First block to index, or 'auto'.")
@click.option(
    "--end-block",
    default="latest",
    show_default=True,
    help="Last block to index, or 'latest'.",
)
@click.option("--protocol-slug", required=True, help="Protocol slug for output layout.")
@click.option("--contract-slug", required=True, help="Contract slug for output layout.")
@click.option(
    "--step",
    type=click.IntRange(min=1),
    default=_DEFAULT_STEP,
    show_default=True,
    help="RPC fetch window size in blocks.",
)
@click.option(
    "--chunk-size",
    type=click.IntRange(min=1),
    default=_DEFAULT_CHUNK_SIZE,
    show_default=True,
    help="Output parquet chunk size in blocks.",
)
@click.option(
    "--concurrency",
    type=click.IntRange(min=1),
    default=_DEFAULT_CONCURRENCY,
    show_default=True,
    help="Maximum concurrent RPC subrequests.",
)
@click.option(
    "--mode",
    type=click.Choice(("backfill", "listen", "both"), case_sensitive=False),
    default="backfill",
    show_default=True,
    help="Run mode.",
)
@click.option(
    "--storage",
    type=click.Choice(("local", "s3"), case_sensitive=False),
    default="local",
    show_default=True,
    help="Storage backend.",
)
def run_cmd(
    *,
    rpc_url: str,
    address: str,
    abi_path: Path | None,
    start_block: str,
    end_block: str,
    protocol_slug: str,
    contract_slug: str,
    step: int,
    chunk_size: int,
    concurrency: int,
    mode: str,
    storage: str,
) -> None:
    """Run the indexer."""
    try:
        normalized_end_block = _parse_end_block(end_block)
        normalized_start_block = _parse_start_block(start_block)
        normalized_rpc_url = _normalize_required_text(rpc_url, label="rpc-url")
        normalized_address = _normalize_contract_address(address)
        normalized_protocol_slug = _normalize_required_text(protocol_slug, label="protocol-slug")
        normalized_contract_slug = _normalize_required_text(contract_slug, label="contract-slug")
        _validate_block_window(start_block=normalized_start_block, end_block=normalized_end_block)

        asyncio.run(
            _run_indexer(
                rpc_url=normalized_rpc_url,
                address=normalized_address,
                abi_path=abi_path,
                start_block=normalized_start_block,
                end_block=normalized_end_block,
                protocol_slug=normalized_protocol_slug,
                contract_slug=normalized_contract_slug,
                step=int(step),
                chunk_size=int(chunk_size),
                concurrency=int(concurrency),
                mode=mode.lower(),
                storage=storage.lower(),
            )
        )
    except click.ClickException as exc:
        _render_error(str(exc))
        raise SystemExit(1) from exc
    except KeyboardInterrupt:
        _render_error("Run interrupted by user.", title="Interrupted", style="yellow")
        raise SystemExit(130)
    except Exception as exc:
        _render_error(exception_detail(exc))
        raise SystemExit(1) from exc


if __name__ == "__main__":
    cli()
