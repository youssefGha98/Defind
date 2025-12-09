from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol
from defind.core.interfaces import IDirectorySetup
from defind.core.models import SetupDirectoriesResult
from defind.core.config import OrchestratorConfig
from defind.orchestration.utils import topics_fingerprint


class BulkDirectorySetup:
    """
    Standard setup: shards by address+topics fingerprint.
    
    Layout: <out_root>/<address>__topics-<fp>/
              manifests/
              shards/ (managed by ShardsDir)
    """
    def setup(self, config: OrchestratorConfig) -> SetupDirectoriesResult:
        address_lc = config.address.lower()
        topics_fp = topics_fingerprint(config.topic0s)
        
        key_dir = config.out_root / f"{address_lc}__topics-{topics_fp}"
        manifests_dir = key_dir / "manifests"
        manifests_dir.mkdir(exist_ok=True, parents=True)
        
        return SetupDirectoriesResult(
            key_dir=key_dir,
            manifests_dir=manifests_dir,
        )

    def get_run_basename(self, config: OrchestratorConfig) -> str:
        address_lc = config.address.lower()
        topics_fp = topics_fingerprint(config.topic0s)
        timestamp = time.strftime('%Y%m%d_%H%M%S', time.gmtime())
        return f"run_{timestamp}_{address_lc}_{topics_fp}_{config.start_block}_{config.end_block}.jsonl"


class PerEventDirectorySetup:
    """
    Event-based setup: shards by protocol/contract/event.
    
    Layout: <out_root>/<slug>/<slug>/
              manifests/
              shards/ (managed by MultiEventShardWriter)
    """
    def setup(self, config: OrchestratorConfig) -> SetupDirectoriesResult:
        if not (config.protocol_slug and config.contract_slug):
            raise ValueError("protocol_slug and contract_slug required for per_event layout")
            
        key_dir = config.out_root / f"{config.protocol_slug}" / f"{config.contract_slug}"
        manifests_dir = key_dir / "manifests"
        manifests_dir.mkdir(exist_ok=True, parents=True)
        
        return SetupDirectoriesResult(
            key_dir=key_dir,
            manifests_dir=manifests_dir,
        )

    def get_run_basename(self, config: OrchestratorConfig) -> str:
        timestamp = time.strftime('%Y%m%d_%H%M%S', time.gmtime())
        return f"run_{timestamp}_{config.protocol_slug}_{config.contract_slug}_{config.start_block}_{config.end_block}.jsonl"


def get_directory_setup(config: OrchestratorConfig) -> IDirectorySetup:
    """Factory for directory setup strategy."""
    is_event_layout = (
        config.shards_layout == "per_event"
        and config.protocol_slug is not None
        and config.contract_slug is not None
    )
    
    if is_event_layout:
        return PerEventDirectorySetup()
    return BulkDirectorySetup()
