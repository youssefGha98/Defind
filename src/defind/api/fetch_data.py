from __future__ import annotations

import time
from pathlib import Path

from defind.clients.rpc import RPC
from defind.core.config import OrchestratorConfig
from defind.decoding.specs import EventRegistry
from defind.orchestration.orchestrator import (
    fetch_decode,
    FetchDecodeOutput,
    _setup_directories,
)
from defind.orchestration.utils import topics_fingerprint
from defind.storage.manifest import LiveManifest
from defind.storage.shards import ShardsDir, ShardWriter
from defind.decoding.registry import EventRegistryProvider



async def fetch_data(
    *,
    config: OrchestratorConfig,
    registry: EventRegistry,
) -> FetchDecodeOutput:
    """
    High-level convenience API for notebooks / scripts.

    This function:
    - Instantiates concrete infrastructure dependencies:
        * RPC (as IEvmLogsProvider)
        * LiveManifest (as IManifestRepository)
        * ShardWriter (as IEventShardsRepository)
        * EventRegistry (as IEventRegistryProvider)
    - Prepares filesystem directories (key_dir, manifests_dir, shards_dir).
    - Delegates the core orchestration to the strict `core_fetch_decode(...)`.

    Parameters
    ----------
    config : OrchestratorConfig
        Orchestrator-level configuration (RPC URL, out_root, address, etc.).
    registry : EventRegistry
        Decoding registry (events, projections, etc.).

    Returns
    -------
    FetchDecodeOutput
        Stats and output directories for downstream processing.
    """
  
    registry_provider = EventRegistryProvider(registry)

    # 2) Prepare filesystem directories
    setup = _setup_directories(
        out_root=config.out_root,
        address=config.address,
        topic0s=config.topic0s,
    )
    key_dir = setup.key_dir
    manifests_dir = setup.manifests_dir

    # 3) Instantiate concrete logs provider (RPC)
    rpc = RPC(
        config.rpc_url,
        timeout_s=getattr(config, "timeout_s", 30),
        max_connections=max(32, 2 * getattr(config, "concurrency", 4)),
    )

    # 4) Instantiate manifest repository for THIS run
    address_lc = config.address.lower()
    topics_fp = topics_fingerprint(config.topic0s)
    run_basename = (
        f"run_{time.strftime('%Y%m%d_%H%M%S', time.gmtime())}_"
        f"{address_lc}_{topics_fp}_{config.start_block}_{config.end_block}.jsonl"
    )
    manifest_path = manifests_dir / run_basename
    manifest_repo = LiveManifest(manifest_path)

    # 5) Instantiate shards repository (Parquet writer)
    shards_dir_obj = ShardsDir(
        out_root=config.out_root,
        addr_slug=address_lc,
        topics_fp=topics_fp,
    )
    shards_repo = ShardWriter(
        shards_dir_obj,
        rows_per_shard=getattr(config, "rows_per_shard", 250_000),
        write_final_partial=getattr(config, "write_final_partial", True),
    )

    try:
        # 6) Delegate to the strict orchestrator
        result = await fetch_decode(
            config=config,
            registry_provider=registry_provider,
            logs_provider=rpc,
            manifest_repo=manifest_repo,
            shards_repo=shards_repo,
            key_dir=key_dir,
            manifests_dir=manifests_dir,
        )
        return result

    finally:
        # 7) Clean up the RPC client
        await rpc.aclose()
