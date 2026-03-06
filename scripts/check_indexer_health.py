#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import time
import urllib.error
import urllib.request
from pathlib import Path

from defind.core.interfaces import IChunkStorage
from defind.storage.local import LocalChunkStorage
from defind.storage.s3 import S3ChunkStorage


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Check indexer heartbeat freshness and lag.")
    p.add_argument("--protocol", required=True, help="Protocol slug, ex: uniswap")
    p.add_argument("--contract", required=True, help="Contract slug, ex: usdc_weth")
    p.add_argument("--heartbeat-key", default="_meta/heartbeat.json")
    p.add_argument("--max-heartbeat-age-s", type=int, default=180)
    p.add_argument("--max-lag-blocks", type=int, default=0, help="0 disables lag threshold")
    p.add_argument("--out-root", default="./data", help="Local root used when S3 is not enabled")

    p.add_argument("--s3-bucket", default=None)
    p.add_argument("--s3-prefix", default="")
    p.add_argument("--s3-endpoint-url", default=None)
    p.add_argument("--s3-access-key", default=None)
    p.add_argument("--s3-secret-key", default=None)
    p.add_argument("--s3-region", default="auto")
    p.add_argument("--s3-max-retries", type=int, default=3)
    p.add_argument("--s3-retry-backoff-s", type=float, default=0.5)

    p.add_argument(
        "--healthchecks-url",
        default=None,
        help="Optional Healthchecks.io ping URL. On failure the script calls <url>/fail.",
    )
    p.add_argument("--timeout-s", type=float, default=5.0)
    return p.parse_args()


def _build_storage(args: argparse.Namespace) -> tuple[IChunkStorage, str]:
    subpath = f"{args.protocol}/{args.contract}"
    storage: IChunkStorage
    if args.s3_bucket:
        prefix = f"{args.s3_prefix.rstrip('/')}/{subpath}/" if args.s3_prefix else f"{subpath}/"
        storage = S3ChunkStorage(
            bucket=args.s3_bucket,
            prefix=prefix,
            endpoint_url=args.s3_endpoint_url,
            access_key=args.s3_access_key,
            secret_key=args.s3_secret_key,
            region=args.s3_region,
            max_retries=args.s3_max_retries,
            retry_backoff_s=args.s3_retry_backoff_s,
        )
        location = f"s3://{args.s3_bucket}/{prefix}"
    else:
        root = Path(args.out_root) / args.protocol / args.contract
        storage = LocalChunkStorage(root)
        location = str(root)
    return storage, location


def _ping_healthchecks(url: str, *, ok: bool, timeout_s: float) -> None:
    target = url if ok else f"{url.rstrip('/')}/fail"
    req = urllib.request.Request(target, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout_s):
            return
    except urllib.error.URLError:
        return


def main() -> int:
    args = _parse_args()
    storage, location = _build_storage(args)
    now = int(time.time())

    heartbeat = storage.read_json(args.heartbeat_key)
    status = "ok"
    reason = "healthy"
    lag_blocks: int | None = None
    age_s: int | None = None

    if not isinstance(heartbeat, dict):
        status = "error"
        reason = "heartbeat_missing_or_invalid"
    else:
        try:
            ts_unix_s = int(heartbeat.get("ts_unix_s", 0))
            age_s = max(0, now - ts_unix_s)
            raw_lag = heartbeat.get("lag_blocks")
            lag_blocks = int(raw_lag) if raw_lag is not None else None
        except Exception:
            status = "error"
            reason = "heartbeat_payload_invalid"
        else:
            if age_s > args.max_heartbeat_age_s:
                status = "error"
                reason = "heartbeat_stale"
            elif args.max_lag_blocks > 0 and lag_blocks is not None and lag_blocks > args.max_lag_blocks:
                status = "error"
                reason = "lag_too_high"

    out = {
        "status": status,
        "reason": reason,
        "location": location,
        "heartbeat_key": args.heartbeat_key,
        "max_heartbeat_age_s": args.max_heartbeat_age_s,
        "max_lag_blocks": args.max_lag_blocks,
        "heartbeat_age_s": age_s,
        "lag_blocks": lag_blocks,
        "checked_at_s": now,
    }
    print(json.dumps(out, sort_keys=True))

    is_ok = status == "ok"
    if args.healthchecks_url:
        _ping_healthchecks(args.healthchecks_url, ok=is_ok, timeout_s=args.timeout_s)
    return 0 if is_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
