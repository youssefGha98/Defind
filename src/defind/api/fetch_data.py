from __future__ import annotations

import time

from defind.core.models import SetupDirectoriesResult
from defind.storage.directories import get_directory_setup
from defind.core.config import OrchestratorConfig
from defind.decoding.specs import EventRegistry
from defind.orchestration.orchestrator import (
    fetch_decode,
    FetchDecodeOutput,
)
from defind.orchestration.utils import topics_fingerprint
from defind.storage.manifest import LiveManifest
from defind.storage.shards import ShardsDir, ShardWriter, MultiEventShardWriter
from defind.decoding.registry import EventRegistryProvider
from defind.clients.rpc import RPC

def _setup(config: OrchestratorConfig, registry: EventRegistry)->tuple[EventRegistryProvider,RPC,SetupDirectoriesResult]:
    """Common setup for both legacy and event-based fetch strategies."""
    registry_provider = EventRegistryProvider(registry)

    # Prepare filesystem directories
    dir_strategy = get_directory_setup(config)
    setup = dir_strategy.setup(config)

    # Instantiate RPC
    rpc = RPC(
        config.rpc_url,
        timeout_s=config.timeout_s,
        max_connections=max(32, 2 * config.concurrency),
    )

    return registry_provider, rpc, setup


async def _fetch_data_legacy(
    config: OrchestratorConfig,
    registry_provider: EventRegistryProvider,
    rpc: RPC,
    setup: SetupDirectoriesResult,
) -> FetchDecodeOutput:
    """Legacy fetch strategy: shards by address+topics fingerprint."""
    address_lc = config.address.lower()
    topics_fp = topics_fingerprint(config.topic0s)
    
    run_basename = (
        f"run_{time.strftime('%Y%m%d_%H%M%S', time.gmtime())}_"
        f"{address_lc}_{topics_fp}_{config.start_block}_{config.end_block}.jsonl"
    )
    manifest_path = setup.manifests_dir / run_basename
    manifest_repo = LiveManifest(manifest_path)

    shards_dir_obj = ShardsDir(
        out_root=config.out_root,
        addr_slug=address_lc,
        topics_fp=topics_fp,
    )
    shards_repo = ShardWriter(
        shards_dir_obj,
        rows_per_shard=config.rows_per_shard,
        write_final_partial=config.write_final_partial,
    )

    return await fetch_decode(
        config=config,
        registry_provider=registry_provider,
        logs_provider=rpc,
        manifest_repo=manifest_repo,
        shards_repo=shards_repo,
        key_dir=setup.key_dir,
        manifests_dir=setup.manifests_dir,
    )


async def _fetch_data_event(
    config: OrchestratorConfig,
    registry_provider: EventRegistryProvider,
    rpc: RPC,
    setup: SetupDirectoriesResult,
) -> FetchDecodeOutput:
    """Event-based fetch strategy: shards by protocol/pool/event."""
    run_basename = (
        f"run_{time.strftime('%Y%m%d_%H%M%S', time.gmtime())}_"
        f"proto-{config.protocol_slug}_pool-{config.contract_slug}_{config.start_block}_{config.end_block}.jsonl"
    )
    print(run_basename)
    manifest_path = setup.manifests_dir / run_basename
    manifest_repo = LiveManifest(manifest_path)

    shards_root = setup.key_dir / "shards"
    shards_repo = MultiEventShardWriter(
        root=shards_root,
        rows_per_shard=config.rows_per_shard,
        codec="zstd",
        write_final_partial=config.write_final_partial,
    )

    return await fetch_decode(
        config=config,
        registry_provider=registry_provider,
        logs_provider=rpc,
        manifest_repo=manifest_repo,
        shards_repo=shards_repo,
        key_dir=setup.key_dir,
        manifests_dir=setup.manifests_dir,
    )


async def fetch_data(
    *,
    config: OrchestratorConfig,
    registry: EventRegistry,
) -> FetchDecodeOutput:
    """
    High-level convenience API for notebooks / scripts.
    Delegates to legacy or event-based strategy depending on config.
    """
    registry_provider, rpc, setup = _setup(config, registry)

    try:
        shards_layout = config.shards_layout
        protocol_slug = config.protocol_slug
        contract_slug = config.contract_slug

        is_event_layout = (
            shards_layout == "per_event"
            and protocol_slug is not None
            and contract_slug is not None
        )

        if is_event_layout:
            return await _fetch_data_event(config, registry_provider, rpc, setup)
        else:
            return await _fetch_data_legacy(config, registry_provider, rpc, setup)
    finally:
        await rpc.aclose()
