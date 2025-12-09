"""Streaming orchestrator: fetch → decode → write universal shards.

This module provides two layers:

1) `run_fetch_decode_use_case(...)`:
   - Pure application-layer use case.
   - Depends ONLY on interfaces (IEvmLogsProvider, IManifestRepository,
     IEventShardsRepository, IEventRegistryProvider).
   - Does NOT instantiate RPC, LiveManifest, ShardWriter, etc.
   - Does NOT manage lifecycle (e.g., closing RPC).

2) `fetch_decode(...)` (optional convenience wrapper):
   - Wires concrete implementations (RPC, LiveManifest, ShardWriter) for
     typical CLI / script usage.
   - Calls `run_fetch_decode_use_case(...)` under the hood.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path

from defind.core.config import OrchestratorConfig
from defind.core.interfaces import (
    IEvmLogsProvider,
    IManifestRepository,
    IEventShardsRepository,
    IEventRegistryProvider,
)
from defind.core.use_cases.fetch_decode import (
    FetchDecodeConfig,
    FetchDecodeService,
    ProcessStats,
    build_work_seeds,
)
from defind.orchestration.utils import (
    load_done_coverage,
    topics_fingerprint,
)
from defind.storage.manifest import LiveManifest
from defind.storage.shards import ShardsDir, ShardWriter


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MIN_RPC_CONNECTIONS = 32



async def _resolve_block_range(
    logs_provider: IEvmLogsProvider,
    start_block: int | str,
    end_block: int | str,
) -> tuple[int, int]:
    """Resolve start and end blocks, handling special values like 'latest'."""
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


# ---------------------------------------------------------------------------
# Output DTO
# ---------------------------------------------------------------------------


@dataclass(kw_only=True)
class FetchDecodeOutput:
    """High-level output of the orchestrator."""
    stats: ProcessStats
    key_dir: Path
    manifests_dir: Path
    shards_dir: Path


# ---------------------------------------------------------------------------
# 1) Pure application use case (no concrete instantiation)
# ---------------------------------------------------------------------------


async def fetch_decode(
    *,
    config: OrchestratorConfig,
    registry_provider: IEventRegistryProvider,
    logs_provider: IEvmLogsProvider,
    manifest_repo: IManifestRepository,
    shards_repo: IEventShardsRepository,
    key_dir: Path,
    manifests_dir: Path,
    run_id: str,
) -> FetchDecodeOutput:
    """Pure application-layer orchestrator.

    This function:
    - Resolves the block range using the injected logs provider.
    - Computes coverage using the filesystem-based manifest directory.
    - Builds work seeds using domain helper `build_work_seeds`.
    - Invokes the domain use case `FetchDecodeService`.

    It does NOT:
    - Instantiate RPC / LiveManifest / ShardWriter.
    - Manage lifecycle (e.g., closing RPC clients).
    """
    # 1) Resolve block range
    start, end = await _resolve_block_range(
        logs_provider=logs_provider,
        start_block=config.start_block,
        end_block=config.end_block,
    )

    # 2) Historical coverage (application concern, uses filesystem utils)
    covered = load_done_coverage(
        manifests_dir=manifests_dir,
        exclude_basename=run_id,
    )

    # 3) Domain-level configuration
    domain_config = FetchDecodeConfig(
        address=config.address,
        topic0s=config.topic0s,
        step=config.step,
        concurrency=config.concurrency,
        batch_decode_rows=config.batch_decode_rows,
    )

    seeds = build_work_seeds(
        start=start,
        end=end,
        step=domain_config.step,
        covered=covered,
    )

    # 5) Domain service invocation
    service = FetchDecodeService(
        logs_provider=logs_provider,
        registry_provider=registry_provider,
    )

    stats = await service.run(
        config=domain_config,
        manifest_repo=manifest_repo,
        shards_repo=shards_repo,
        seeds=seeds,
    )

    # 6) Output DTO
    shards_dir_path = key_dir / "shards"

    return FetchDecodeOutput(
        stats=stats,
        key_dir=key_dir,
        manifests_dir=manifests_dir,
        shards_dir=shards_dir_path,
    )

