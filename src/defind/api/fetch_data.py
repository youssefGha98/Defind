from __future__ import annotations

from collections.abc import Awaitable, Callable

from defind.core.config import OrchestratorConfig
from defind.decoding.specs import EventRegistry
from defind.orchestration.orchestrator import (
    FetchDecodeOutput,
    fetch_decode,
)


async def fetch_data(
    *,
    config: OrchestratorConfig,
    registry: EventRegistry,
    on_chunk_written: Callable[[int, int], Awaitable[None]] | None = None,
) -> FetchDecodeOutput:
    """
    High-level convenience API for notebooks / scripts.

    Delegates directly to `fetch_decode` which handles both local and S3
    storage depending on whether `config.s3_bucket` is set.
    """
    return await fetch_decode(config=config, registry=registry, on_chunk_written=on_chunk_written)
