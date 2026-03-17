from __future__ import annotations

from defind.core.config import OrchestratorConfig
from defind.core.indexer_request.registry import serialize_registry
from defind.decoding.specs import EventRegistry

S3_SECRET_KEYS = frozenset({"s3_access_key", "s3_secret_key"})


def sanitize_request_payload(payload: dict[str, object]) -> dict[str, object]:
    sanitized = dict(payload)
    for key in S3_SECRET_KEYS:
        if key in sanitized:
            sanitized[key] = None
    return sanitized


def build_request_payload_from_config(
    *,
    config: OrchestratorConfig,
    registry: EventRegistry,
    job_origin: str | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "rpc_url": config.rpc_url,
        "address": config.address,
        "abi_path": None,
        "abi_json": None,
        "registry_json": serialize_registry(registry),
        "event_names": [spec.name for spec in registry.values()],
        "start_block": config.start_block,
        "end_block": config.end_block,
        "protocol_slug": config.protocol_slug,
        "contract_slug": config.contract_slug,
        "step": config.step,
        "chunk_size": config.chunk_size,
        "concurrency": config.concurrency,
        "timeout_s": config.timeout_s,
        "rpc_max_retries": config.rpc_max_retries,
        "rpc_retry_backoff_s": config.rpc_retry_backoff_s,
        "codec": config.codec,
        "listen": config.listen,
        "listen_poll_interval_s": config.listen_poll_interval_s,
        "reorg_lookback_blocks": config.reorg_lookback_blocks,
        "print_chunk_writes": config.print_chunk_writes,
        "heartbeat_interval_s": config.heartbeat_interval_s,
        "lag_warn_threshold_blocks": config.lag_warn_threshold_blocks,
        "heartbeat_key": config.heartbeat_key,
        "single_writer_guard": False,
        "writer_lock_key": config.writer_lock_key,
        "writer_lock_ttl_s": config.writer_lock_ttl_s,
        "writer_lock_refresh_s": config.writer_lock_refresh_s,
        "log_level": config.log_level,
        "log_json": config.log_json,
        "storage": "s3" if config.s3_bucket else "local",
        "out_root": None if config.s3_bucket else str(config.out_root),
        "s3_bucket": config.s3_bucket,
        "s3_prefix": config.s3_prefix,
        "s3_endpoint_url": config.s3_endpoint_url,
        "s3_access_key": config.s3_access_key,
        "s3_secret_key": config.s3_secret_key,
        "s3_region": config.s3_region,
        "s3_max_retries": config.s3_max_retries,
        "s3_retry_backoff_s": config.s3_retry_backoff_s,
    }
    if job_origin:
        payload["job_origin"] = job_origin
    return sanitize_request_payload(payload)
