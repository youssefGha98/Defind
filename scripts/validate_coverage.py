#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path

from defind.core.interfaces import IChunkStorage
from defind.orchestration.validator import validate_coverage
from defind.storage.local import LocalChunkStorage
from defind.storage.s3 import S3ChunkStorage


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate chunk coverage/index consistency.")
    parser.add_argument("--protocol", required=True, help="Protocol slug, ex: uniswap")
    parser.add_argument("--contract", required=True, help="Contract slug, ex: usdc_weth")
    parser.add_argument(
        "--event",
        dest="events",
        action="append",
        required=True,
        help="Event name. Repeat this flag for every event, ex: --event Mint --event Burn",
    )
    parser.add_argument("--out-root", default="./data", help="Local root used when S3 is not enabled")
    parser.add_argument("--start-block", type=int, default=None)
    parser.add_argument("--end-block", type=int, default=None)

    parser.add_argument("--s3-bucket", default=None)
    parser.add_argument("--s3-prefix", default="")
    parser.add_argument("--s3-endpoint-url", default=None)
    parser.add_argument("--s3-access-key", default=None)
    parser.add_argument("--s3-secret-key", default=None)
    parser.add_argument("--s3-region", default="auto")
    parser.add_argument("--s3-max-retries", type=int, default=3)
    parser.add_argument("--s3-retry-backoff-s", type=float, default=0.5)
    return parser.parse_args()


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


def main() -> int:
    args = _parse_args()
    storage, location = _build_storage(args)
    report = validate_coverage(
        storage=storage,
        event_names=args.events,
        start_block=args.start_block,
        end_block=args.end_block,
    )

    out = asdict(report)
    out["location"] = location
    print(json.dumps(out, indent=2, sort_keys=True))
    return 0 if report.is_valid else 1


if __name__ == "__main__":
    raise SystemExit(main())
