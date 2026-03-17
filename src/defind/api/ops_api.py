from __future__ import annotations

from fastapi import FastAPI

from defind.api.fetch_data import fetch_data
from defind.api.ops.app import create_ops_app
from defind.api.ops.dataset.repository import build_control_storage as _build_control_storage
from defind.api.ops.dataset.repository import build_dataset_storage as _build_dataset_storage
from defind.api.ops.jobs.utils import prepare_dataset_job_run as _prepare_dataset_job_run
from defind.api.ops.logs.repository import EventStore as _EventStore
from defind.api.ops.shared.dependencies import OpsApiDependencies
from defind.api.ops.shared.models import DatasetRef, OpsApiConfig
from defind.api.ops.shared.network import (
    fetch_etherscan_abi as _fetch_etherscan_abi,
)
from defind.api.ops.shared.network import (
    fetch_etherscan_chain_head as _fetch_etherscan_chain_head,
)
from defind.api.ops.shared.network import fetch_rpc_chain_head as _fetch_rpc_chain_head
from defind.api.ops.shared.utils import load_ops_api_config_from_env
from defind.dataset_state import read_dataset_meta


def create_app(config: OpsApiConfig | None = None) -> FastAPI:
    deps = OpsApiDependencies(
        fetch_data=fetch_data,
        read_dataset_meta=read_dataset_meta,
        build_dataset_storage=_build_dataset_storage,
        build_control_storage=_build_control_storage,
        fetch_rpc_chain_head=_fetch_rpc_chain_head,
        fetch_etherscan_chain_head=_fetch_etherscan_chain_head,
        fetch_etherscan_abi=_fetch_etherscan_abi,
    )
    return create_ops_app(config, deps=deps)


__all__ = [
    "DatasetRef",
    "OpsApiConfig",
    "_EventStore",
    "_build_control_storage",
    "_build_dataset_storage",
    "_fetch_etherscan_abi",
    "_fetch_etherscan_chain_head",
    "_fetch_rpc_chain_head",
    "_prepare_dataset_job_run",
    "create_app",
    "fetch_data",
    "load_ops_api_config_from_env",
    "read_dataset_meta",
]
