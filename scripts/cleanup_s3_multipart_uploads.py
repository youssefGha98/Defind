#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os

from dotenv import load_dotenv

from defind.storage.s3 import S3ChunkStorage


def _parse_args() -> argparse.Namespace:
    load_dotenv()
    parser = argparse.ArgumentParser(description="List/abort incomplete S3 multipart uploads for one dataset prefix.")
    parser.add_argument("--protocol", required=True, help="Protocol slug, ex: uniswap")
    parser.add_argument("--contract", required=True, help="Contract slug, ex: usdc_weth")
    parser.add_argument("--key-prefix", default="", help="Optional event prefix under dataset, ex: CollectProtocol/")
    parser.add_argument(
        "--older-than-s",
        type=int,
        default=300,
        help="Only target uploads older than this age (seconds).",
    )
    parser.add_argument("--limit", type=int, default=None, help="Optional max number of uploads to process.")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Abort uploads (default is dry-run only).",
    )

    parser.add_argument("--s3-bucket", default=os.getenv("S3_BUCKET"))
    parser.add_argument("--s3-prefix", default=os.getenv("S3_PREFIX", ""))
    parser.add_argument("--s3-endpoint-url", default=os.getenv("S3_ENDPOINT_URL"))
    parser.add_argument("--s3-access-key", default=os.getenv("S3_ACCESS_KEY"))
    parser.add_argument("--s3-secret-key", default=os.getenv("S3_SECRET_KEY"))
    parser.add_argument("--s3-region", default=os.getenv("S3_REGION", "auto"))
    parser.add_argument("--s3-max-retries", type=int, default=3)
    parser.add_argument("--s3-retry-backoff-s", type=float, default=0.5)
    args = parser.parse_args()

    if not args.s3_bucket:
        parser.error("--s3-bucket is required (or set S3_BUCKET in .env)")
    if args.older_than_s < 0:
        parser.error("--older-than-s must be >= 0")
    if args.limit is not None and args.limit <= 0:
        parser.error("--limit must be > 0 when provided")
    return args


def main() -> int:
    args = _parse_args()
    contract_subpath = f"{args.protocol}/{args.contract}"
    dataset_prefix = f"{args.s3_prefix.rstrip('/')}/{contract_subpath}/" if args.s3_prefix else f"{contract_subpath}/"

    storage = S3ChunkStorage(
        bucket=args.s3_bucket,
        prefix=dataset_prefix,
        endpoint_url=args.s3_endpoint_url,
        access_key=args.s3_access_key,
        secret_key=args.s3_secret_key,
        region=args.s3_region,
        max_retries=args.s3_max_retries,
        retry_backoff_s=args.s3_retry_backoff_s,
    )

    summary = storage.cleanup_incomplete_multipart_uploads(
        key_prefix=args.key_prefix,
        older_than_s=args.older_than_s,
        dry_run=not args.apply,
        limit=args.limit,
    )
    out = {
        "bucket": args.s3_bucket,
        "dataset_prefix": dataset_prefix,
        "key_prefix": args.key_prefix,
        "mode": "apply" if args.apply else "dry-run",
        **summary,
    }
    print(json.dumps(out, indent=2, sort_keys=True))
    return 2 if summary["failed"] > 0 else 0


if __name__ == "__main__":
    raise SystemExit(main())
