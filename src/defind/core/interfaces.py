from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

import pyarrow as pa

from defind.core.models import EventLog

if TYPE_CHECKING:
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
    ) -> list[EventLog]: ...

    async def latest_block(self) -> int: ...


# ---------------------------------------------------------------------------
# IEventRegistryProvider
# ---------------------------------------------------------------------------


@runtime_checkable
class IEventRegistryProvider(Protocol):
    """
    Abstract provider of EventRegistry objects used for decoding events.
    """

    def get_registry(self) -> EventRegistry: ...


# ---------------------------------------------------------------------------
# IChunkStorage
# ---------------------------------------------------------------------------


@runtime_checkable
class IChunkStorage(Protocol):
    """
    Abstract storage backend for chunk files (local or S3).

    A "chunk" is a Parquet file keyed by `{EventName}/chunk_{from:010d}_{to:010d}.parquet`.

    Domain expectations:
    - Hides the underlying storage technology (local filesystem, S3, R2, etc.).
    - write_table always writes, even for empty tables (0 rows are valid markers).
    """

    def write_table(self, key: str, table: pa.Table, codec: str) -> None:
        """
        Write parquet table at the given key.

        Writes even for 0-row tables (empty file = marker that this chunk was processed).
        """
        ...

    def exists(self, key: str) -> bool:
        """Check if a key exists in storage."""
        ...

    def list_keys(self, prefix: str) -> list[str]:
        """
        List all keys that start with the given prefix.

        Returns relative keys (not full paths).
        Example: list_keys("Mint/") → ["Mint/chunk_0000000100_0000005099.parquet", ...]
        """
        ...

    def delete(self, key: str) -> None:
        """Delete a key if present (best effort for idempotent maintenance)."""
        ...

    def write_json(self, key: str, payload: dict[str, Any]) -> None:
        """Write a JSON payload to a relative key."""
        ...

    def read_json(self, key: str) -> dict[str, Any] | None:
        """Read a JSON payload from a relative key, or None if missing/invalid."""
        ...
