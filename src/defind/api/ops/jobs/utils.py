from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, cast

from fastapi import HTTPException

from defind.api.ops.shared.models import DatasetRef, OpsApiConfig
from defind.api.ops.shared.utils import (
    ORCHESTRATOR_RPC_MAX_RETRIES_DEFAULT,
    ORCHESTRATOR_RPC_RETRY_BACKOFF_DEFAULT_S,
    ORCHESTRATOR_TIMEOUT_DEFAULT_S,
    exception_detail,
    meta_float_value,
    meta_int_value,
)
from defind.core.config import OrchestratorConfig
from defind.dataset_state import validate_meta_runtime_fields
from defind.decoding.specs import EventRegistry


@dataclass(frozen=True)
class PreparedDatasetRun:
    config: OrchestratorConfig
    registry: EventRegistry
    dataset_id: str
    mode: Literal["backfill", "listen"]
    public_config: dict[str, Any]


def prepare_dataset_job_run(
    *,
    cfg: OpsApiConfig,
    dataset: DatasetRef,
    meta: dict[str, Any],
    mode: str,
    concurrency: int,
    origin: str,
    resume_from: int,
) -> PreparedDatasetRun:
    registry = validate_meta_runtime_fields(meta)
    listen = mode in {"listen", "both"}
    timeout_s = meta_int_value(meta, "timeout_s", default=ORCHESTRATOR_TIMEOUT_DEFAULT_S, min_value=1)
    rpc_max_retries = meta_int_value(
        meta,
        "rpc_max_retries",
        default=ORCHESTRATOR_RPC_MAX_RETRIES_DEFAULT,
        min_value=0,
    )
    rpc_retry_backoff_s = meta_float_value(
        meta,
        "rpc_retry_backoff_s",
        default=ORCHESTRATOR_RPC_RETRY_BACKOFF_DEFAULT_S,
        min_value=0.0,
    )
    public_config: dict[str, Any] = {
        "protocol": dataset.protocol,
        "contract": dataset.contract,
        "contract_address": str(meta.get("contract_address") or ""),
        "chain_id": int(meta.get("chain_id") or 0),
        "rpc_url": str(meta.get("rpc_url") or ""),
        "event_names": list(cast(list[str], meta.get("event_names") or [])),
        "registry_json": cast(dict[str, Any], meta.get("registry_json") or {}),
        "start_block": int(meta.get("start_block") or 0),
        "resume_from": int(resume_from),
        "chunk_size": int(meta.get("chunk_size") or cfg.default_chunk_size),
        "step": int(meta.get("step") or 1),
        "mode": mode,
        "concurrency": int(concurrency),
        "timeout_s": timeout_s,
        "rpc_max_retries": rpc_max_retries,
        "rpc_retry_backoff_s": rpc_retry_backoff_s,
        "origin": origin,
        "storage": str(meta.get("storage") or "s3"),
    }
    orchestrator_cfg = OrchestratorConfig(
        rpc_url=str(meta.get("rpc_url") or ""),
        address=str(meta.get("contract_address") or ""),
        topic0s=list(registry.keys()),
        start_block=int(meta.get("start_block") or resume_from),
        end_block="latest",
        protocol_slug=dataset.protocol,
        contract_slug=dataset.contract,
        step=int(meta.get("step") or 1),
        chunk_size=int(meta.get("chunk_size") or cfg.default_chunk_size),
        concurrency=int(concurrency),
        timeout_s=timeout_s,
        rpc_max_retries=rpc_max_retries,
        rpc_retry_backoff_s=rpc_retry_backoff_s,
        listen=listen,
        heartbeat_interval_s=0.0,
        lag_warn_threshold_blocks=0,
        single_writer_guard=False,
        out_root=cfg.out_root,
        s3_bucket=cfg.s3_bucket,
        s3_prefix=cfg.s3_prefix,
        s3_endpoint_url=cfg.s3_endpoint_url,
        s3_access_key=cfg.s3_access_key,
        s3_secret_key=cfg.s3_secret_key,
        s3_region=cfg.s3_region,
        s3_max_retries=cfg.s3_max_retries,
        s3_retry_backoff_s=cfg.s3_retry_backoff_s,
    )
    return PreparedDatasetRun(
        config=orchestrator_cfg,
        registry=registry,
        dataset_id=dataset.dataset_id,
        mode="listen" if listen else "backfill",
        public_config=public_config,
    )


async def validate_job_start_preflight(
    meta: dict[str, Any],
    *,
    fetch_rpc_chain_head: Any,
    resolve_public_chain_head: Any,
) -> int:
    rpc_url = str(meta.get("rpc_url") or "").strip()
    start_block = int(meta.get("start_block") or 0)
    chain_id = int(meta.get("chain_id") or 0)
    if not rpc_url:
        raise ValueError("dataset meta is missing rpc_url")
    try:
        latest = int(await fetch_rpc_chain_head(rpc_url=rpc_url))
    except Exception as exc:
        raise ValueError(
            f"unable to resolve current chain head from dataset rpc_url: {exception_detail(exc)}"
        ) from exc
    if start_block > latest:
        public_head = await resolve_public_chain_head(chain_id)
        if public_head is not None and public_head >= start_block and public_head > latest:
            raise ValueError(
                f"dataset rpc_url appears stale: rpc head {latest} is behind public chain head {public_head}; "
                "use a healthy RPC provider for this dataset"
            )
        raise ValueError(
            f"dataset start_block {start_block} is ahead of current RPC chain head {latest}; "
            "use a node on the correct chain or recreate the dataset with a valid start block"
        )
    return latest


def dataset_job_start_http_exception(exc: RuntimeError) -> HTTPException:
    message = str(exc)
    if message.startswith("writer_job_already_active:"):
        blocking_job_id = message.split(":", 1)[1]
        return HTTPException(
            status_code=409,
            detail={
                "message": "A writer job is already active on this dataset",
                "blocking_job_id": blocking_job_id,
            },
        )
    return HTTPException(status_code=400, detail=message)
