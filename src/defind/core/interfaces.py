from __future__ import annotations

from pathlib import Path
from typing import List, Protocol, runtime_checkable

from defind.core.models import EventLog, ChunkRecord, Column
from defind.decoding.specs import EventRegistry


# ---------------------------------------------------------------------------
# IEvmLogsProvider
# ---------------------------------------------------------------------------

@runtime_checkable
class IEvmLogsProvider(Protocol):
    """
    Abstract provider for fetching EVM logs.

    Domain expectations:
    - It returns EventLog objects already mapped into internal domain models.
    - It hides the underlying RPC / DB / archive technology.
    """

    async def get_logs(
        self,
        *,
        address: str,
        topic0s: list[str],
        from_block: int,
        to_block: int,
    ) -> List[EventLog]:
        """
        Return all logs for (address, topic0s) over the inclusive block range.

        Implementations:
        - RPC-based (current `RPC` class)
        - Log archive reader
        - In-memory or synthetic provider for testing
        """
        ...

    async def latest_block(self) -> int:
        """
        Return the current chain head height.

        Implementations may:
            - Query an RPC node
            - Query a database
            - Return a static value for testing
        """
        ...


# ---------------------------------------------------------------------------
# IManifestRepository
# ---------------------------------------------------------------------------

@runtime_checkable
class IManifestRepository(Protocol):
    """
    Append-only manifest repository for chunk status tracking.

    Domain expectations:
    - The manifest acts as an append-only journal.
    - Reading / aggregating coverage is an application-level responsibility.
    """

    async def append(self, record: ChunkRecord) -> None:
        """
        Append a new ChunkRecord (started/done/failed).

        Implementations:
        - LiveManifest (JSONL file writer)
        - Database-backed manifest
        - Remote logging system
        """
        ...


# ---------------------------------------------------------------------------
# IEventShardsRepository
# ---------------------------------------------------------------------------

@runtime_checkable
class IEventShardsRepository(Protocol):
    """
    Abstract sink for decoded event rows.

    Domain expectations:
    - It accepts batches of Column objects.
    - It may produce one or more completed "shards" when thresholds are reached.
    - The domain only cares about how many shards were produced.
    """

    def add(self, column_batch: Column) -> List[Path]:
        """
        Add a batch of decoded rows.

        Returns
        -------
        List[Path]
            Identifiers (e.g. file paths) of shards that were written or rewritten.
            The domain never inspects the Path objects, only their count.
        """
        ...

    def close(self) -> Path | None:
        """
        Flush and finalize any remaining buffered rows.

        Returns
        -------
        Path | None
            Identifier of the last written shard, or None if nothing was written.
        """
        ...


# ---------------------------------------------------------------------------
# IEventRegistryProvider
# ---------------------------------------------------------------------------

@runtime_checkable
class IEventRegistryProvider(Protocol):
    """
    Abstract provider of EventRegistry objects used for decoding events.

    Domain expectations:
    - It provides a fully initialized EventRegistry.
    - How this registry is built (from ABIs, filesystem, Etherscan, DB...) is an
      infrastructure concern.
    """

    def get_registry(self) -> EventRegistry:
        """
        Return a fully configured EventRegistry instance.

        Implementations:
        - Static provider wrapping an already-created registry
        - ABI loader
        - Etherscan fetcher
        - Database-backed registry
        """
        ...
