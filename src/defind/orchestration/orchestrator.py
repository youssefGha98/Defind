"""Streaming orchestrator: fetch → decode → write chunk files.

Provides:
1) `fetch_decode(config, registry)` — convenience wrapper that wires concrete
   implementations (RPC, LocalChunkStorage or S3ChunkStorage) and runs the
   full pipeline.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from defind.core.config import OrchestratorConfig
from defind.core.interfaces import IChunkStorage
from defind.core.use_cases.fetch_decode import (
    FetchDecodeConfig,
    FetchDecodeService,
    ProcessStats,
    build_work_seeds,
)
from defind.decoding.registry import EventRegistryProvider
from defind.decoding.specs import EventRegistry
from defind.clients.rpc import RPC
from defind.orchestration.utils import load_done_coverage
from defind.storage.local import LocalChunkStorage
from defind.storage.s3 import S3ChunkStorage


# ---------------------------------------------------------------------------
# Output DTO
# ---------------------------------------------------------------------------


@dataclass(kw_only=True)
class FetchDecodeOutput:
    """High-level output of the orchestrator."""
    stats: ProcessStats
    contract_dir: str   # root key/path for all chunks of this contract


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _resolve_block_range(
    logs_provider,
    start_block: int | str,
    end_block: int | str,
) -> tuple[int, int]:
    if isinstance(start_block, str) and start_block.lower() in ("earliest", "genesis"):
        start = 0
    else:
        start = int(start_block)

    if isinstance(end_block, str) and end_block.lower() == "latest":
        end = await logs_provider.latest_block()
    else:
        end = int(end_block)

    if start > end:
        raise ValueError("start_block must be <= end_block")

    return start, end


def _build_storage(config: OrchestratorConfig) -> tuple[IChunkStorage, str]:
    """Build the appropriate storage backend from config.

    Returns (storage, contract_dir_description) where contract_dir_description
    is a human-readable string for logging/output.
    """
    contract_subpath = f"{config.protocol_slug}/{config.contract_slug}"

    if config.s3_bucket:
        prefix = f"{config.s3_prefix.rstrip('/')}/{contract_subpath}/" if config.s3_prefix else f"{contract_subpath}/"
        storage = S3ChunkStorage(
            bucket=config.s3_bucket,
            prefix=prefix,
            endpoint_url=config.s3_endpoint_url,
            access_key=config.s3_access_key,
            secret_key=config.s3_secret_key,
            region=config.s3_region,
        )
        contract_dir = f"s3://{config.s3_bucket}/{prefix}"
    else:
        root = config.out_root / config.protocol_slug / config.contract_slug
        storage = LocalChunkStorage(root)
        contract_dir = str(root)

    return storage, contract_dir


# ---------------------------------------------------------------------------
# Main entrypoint
# ---------------------------------------------------------------------------


async def fetch_decode(
    *,
    config: OrchestratorConfig,
    registry: EventRegistry,
) -> FetchDecodeOutput:
    """Convenience wrapper: wires concrete implementations and runs the pipeline.

    - Resolves block range (handles "latest").
    - Builds storage backend (local or S3).
    - Loads coverage from existing chunk files.
    - Runs fetch → decode → write for uncovered ranges.
    """
    registry_provider = EventRegistryProvider(registry)
    event_names = [spec.name for spec in registry.values()]

    rpc = RPC(
        config.rpc_url,
        timeout_s=config.timeout_s,
        max_connections=max(32, 2 * config.concurrency),
    )

    storage, contract_dir = _build_storage(config)

    try:
        start, end = await _resolve_block_range(rpc, config.start_block, config.end_block)

        covered = load_done_coverage(storage, event_names)

        effective_chunk_size = config.chunk_size if config.chunk_size is not None else config.step

        domain_config = FetchDecodeConfig(
            address=config.address,
            topic0s=config.topic0s,
            step=config.step,
            chunk_size=effective_chunk_size,
            concurrency=config.concurrency,
        )

        seeds = build_work_seeds(
            start=start,
            end=end,
            chunk_size=effective_chunk_size,
            covered=covered,
        )

        service = FetchDecodeService(
            logs_provider=rpc,
            registry_provider=registry_provider,
        )

        stats = await service.run(
            config=domain_config,
            storage=storage,
            seeds=seeds,
        )

    finally:
        await rpc.aclose()

    return FetchDecodeOutput(stats=stats, contract_dir=contract_dir)
