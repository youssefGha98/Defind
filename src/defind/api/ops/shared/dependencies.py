from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from defind.api.ops.shared.models import DatasetRef, OpsApiConfig
from defind.core.interfaces import IChunkStorage


@dataclass(frozen=True)
class OpsApiDependencies:
    fetch_data: Callable[..., Awaitable[Any]]
    read_dataset_meta: Callable[[IChunkStorage], dict[str, Any] | None]
    build_dataset_storage: Callable[[OpsApiConfig, DatasetRef], tuple[IChunkStorage, str]]
    build_control_storage: Callable[[OpsApiConfig], IChunkStorage]
    fetch_rpc_chain_head: Callable[..., Awaitable[int]]
    fetch_etherscan_chain_head: Callable[..., Awaitable[int]]
    fetch_etherscan_abi: Callable[..., Awaitable[list[dict[str, Any]]]]
