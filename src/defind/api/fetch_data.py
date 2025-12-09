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

def _setup(config: OrchestratorConfig, registry: EventRegistry)->tuple[EventRegistryProvider,RPC,SetupDirectoriesResult,str]:
    """Common setup for both legacy and event-based fetch strategies."""
    registry_provider = EventRegistryProvider(registry)

    # Prepare filesystem directories
    dir_strategy = get_directory_setup(config)
    setup = dir_strategy.setup(config)
    run_basename = dir_strategy.get_run_basename(config)

    # Instantiate RPC
    rpc = RPC(
        config.rpc_url,
        timeout_s=config.timeout_s,
        max_connections=max(32, 2 * config.concurrency),
    )

    return registry_provider, rpc, setup, run_basename


async def _fetch_data_legacy(
    config: OrchestratorConfig,
    registry_provider: EventRegistryProvider,
    rpc: RPC,
    setup: SetupDirectoriesResult,
    run_basename: str,
) -> FetchDecodeOutput:
    """Legacy fetch strategy: shards by address+topics fingerprint."""
    address_lc = config.address.lower()
    topics_fp = topics_fingerprint(config.topic0s)
    
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
        run_id=run_basename,
    )


async def _fetch_data_event(
    config: OrchestratorConfig,
    registry_provider: EventRegistryProvider,
    rpc: RPC,
    setup: SetupDirectoriesResult,
    run_basename: str,
) -> FetchDecodeOutput:
    """Event-based fetch strategy: shards by protocol/pool/event."""
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
        run_id=run_basename,
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
    registry_provider, rpc, setup, run_basename = _setup(config, registry)

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
            return await _fetch_data_event(config, registry_provider, rpc, setup, run_basename)
        else:
            return await _fetch_data_legacy(config, registry_provider, rpc, setup, run_basename)
    finally:
        await rpc.aclose()
