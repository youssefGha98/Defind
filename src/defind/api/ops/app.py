from __future__ import annotations

import contextlib
from collections.abc import AsyncIterator
from typing import Any

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from defind.api.fetch_data import fetch_data
from defind.api.ops.abi.controller import AbiController
from defind.api.ops.abi.repository import AbiRepository
from defind.api.ops.abi.router import router as abi_router
from defind.api.ops.dataset.controller import DatasetController
from defind.api.ops.dataset.repository import DatasetRepository, build_control_storage, build_dataset_storage
from defind.api.ops.dataset.router import router as dataset_router
from defind.api.ops.jobs.controller import DatasetJobRuntimeManager, JobsController
from defind.api.ops.jobs.repository import JobsRepository
from defind.api.ops.jobs.router import router as jobs_router
from defind.api.ops.logs.controller import LogsController
from defind.api.ops.logs.loki import LokiClient
from defind.api.ops.logs.repository import DatasetEventStoreFactory, LogsRepository
from defind.api.ops.logs.router import router as logs_router
from defind.api.ops.shared.context import OpsApiServices
from defind.api.ops.shared.dependencies import OpsApiDependencies
from defind.api.ops.shared.models import OpsApiConfig
from defind.api.ops.shared.network import fetch_etherscan_abi, fetch_etherscan_chain_head, fetch_rpc_chain_head
from defind.api.ops.shared.utils import load_ops_api_config_from_env, service_unavailable_detail
from defind.api.ops.status.controller import StatusController
from defind.api.ops.status.repository import StatusRepository
from defind.api.ops.status.router import router as status_router
from defind.dataset_state import read_dataset_meta
from defind.observability import get_logger

logger = get_logger(__name__)


def default_dependencies() -> OpsApiDependencies:
    return OpsApiDependencies(
        fetch_data=fetch_data,
        read_dataset_meta=read_dataset_meta,
        build_dataset_storage=build_dataset_storage,
        build_control_storage=build_control_storage,
        fetch_rpc_chain_head=fetch_rpc_chain_head,
        fetch_etherscan_chain_head=fetch_etherscan_chain_head,
        fetch_etherscan_abi=fetch_etherscan_abi,
    )


def create_ops_app(
    config: OpsApiConfig | None = None,
    *,
    deps: OpsApiDependencies | None = None,
) -> FastAPI:
    cfg = config or load_ops_api_config_from_env()
    resolved_deps = deps or default_dependencies()

    dataset_repository = DatasetRepository(cfg=cfg, deps=resolved_deps)
    jobs_repository = JobsRepository(dataset_repository=dataset_repository)
    event_store_factory = DatasetEventStoreFactory(cfg=cfg, deps=resolved_deps)
    loki_client = None
    if cfg.logs_backend == "loki" and cfg.loki_url:
        loki_client = LokiClient(
            base_url=cfg.loki_url,
            service_name=cfg.log_service_name,
            timeout_s=cfg.loki_timeout_s,
            lookback_s=cfg.loki_lookback_s,
        )
    elif cfg.logs_backend == "loki":
        logger.warning("loki_logs_backend_requested_without_loki_url")
    logs_repository = LogsRepository(
        event_stores=event_store_factory,
        loki_client=loki_client,
        read_backend=cfg.logs_backend,
    )
    runtime_manager = DatasetJobRuntimeManager(
        cfg=cfg,
        dataset_repository=dataset_repository,
        jobs_repository=jobs_repository,
        logs_repository=logs_repository,
        fetch_data=resolved_deps.fetch_data,
        fetch_rpc_chain_head=resolved_deps.fetch_rpc_chain_head,
    )
    services = OpsApiServices(
        cfg=cfg,
        dataset_controller=DatasetController(repository=dataset_repository),
        jobs_controller=JobsController(
            dataset_repository=dataset_repository,
            jobs_repository=jobs_repository,
            logs_repository=logs_repository,
            runtime_manager=runtime_manager,
        ),
        logs_controller=LogsController(
            dataset_repository=dataset_repository,
            jobs_repository=jobs_repository,
            logs_repository=logs_repository,
        ),
        abi_controller=AbiController(
            cfg=cfg,
            repository=AbiRepository(deps=resolved_deps),
            logs_repository=logs_repository,
        ),
        status_controller=StatusController(
            repository=StatusRepository(
                dataset_repository=dataset_repository,
                jobs_repository=jobs_repository,
            )
        ),
    )

    @contextlib.asynccontextmanager
    async def _lifespan(_: FastAPI) -> AsyncIterator[None]:
        try:
            await runtime_manager.recover_stale_jobs()
            yield
        finally:
            await runtime_manager.shutdown()

    app = FastAPI(title="Defind Ops API", version="0.2.0", lifespan=_lifespan)
    app.state.ops_cfg = cfg
    app.state.dataset_job_manager = runtime_manager
    app.state.ops_services = services

    app.add_middleware(
        CORSMiddleware,
        allow_origins=list(cfg.cors_origins),
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.exception_handler(OSError)
    async def _handle_os_error(_: Any, exc: OSError) -> JSONResponse:
        logger.error("ops_api_storage_unavailable", extra={"error": str(exc)}, exc_info=True)
        return JSONResponse(status_code=503, content={"detail": service_unavailable_detail(exc)})

    app.include_router(status_router)
    app.include_router(dataset_router)
    app.include_router(jobs_router)
    app.include_router(logs_router)
    app.include_router(abi_router)
    return app
