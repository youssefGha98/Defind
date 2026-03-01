from __future__ import annotations

from defind.core.config import OrchestratorConfig
from defind.decoding.specs import EventRegistry
from defind.orchestration.orchestrator import (
    fetch_decode,
    FetchDecodeOutput,
)


async def fetch_data(
    *,
    config: OrchestratorConfig,
    registry: EventRegistry,
) -> FetchDecodeOutput:
    """
    High-level convenience API for notebooks / scripts.

    Delegates directly to `fetch_decode` which handles both local and S3
    storage depending on whether `config.s3_bucket` is set.
    """
    return await fetch_decode(config=config, registry=registry)
