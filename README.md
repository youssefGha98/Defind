# Defind

Defind is an async EVM log indexer focused on deterministic chunked indexing and long-running listen mode.

Main goals:
- fast backfill with concurrent RPC calls
- resumable indexing based on written chunk files
- local filesystem or S3-compatible storage
- operational guardrails (single writer lock, heartbeat, coverage validation)

## Install

```bash
git clone <repo-url>
cd Defind
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

## Minimal usage

```python
from pathlib import Path

from defind.abi_events import make_event_registry_from_abi
from defind.api.fetch_data import fetch_data
from defind.core.config import OrchestratorConfig

registry = make_event_registry_from_abi(Path("abis/cl_pool.json"))

cfg = OrchestratorConfig(
    rpc_url="https://ethereum-rpc.publicnode.com",
    address="0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640",
    topic0s=list(registry.keys()),
    start_block=12_376_729,
    end_block="latest",
    protocol_slug="uniswap",
    contract_slug="usdc_weth",
    step=7_000,
    chunk_size=200_000,
    concurrency=25,
    listen=True,
    out_root=Path("./data"),
)

result = await fetch_data(config=cfg, registry=registry)
print(result.contract_dir)
print(result.stats)
```

## Storage modes

Local mode:
- `s3_bucket=None`
- chunks written under `out_root/protocol_slug/contract_slug/`

S3 mode:
- set `s3_bucket` (and endpoint/credentials if needed)
- chunks written under `s3://bucket/<s3_prefix>/<protocol_slug>/<contract_slug>/`

Important S3 settings in `OrchestratorConfig`:
- `s3_bucket`
- `s3_prefix`
- `s3_endpoint_url`
- `s3_access_key`
- `s3_secret_key`
- `s3_region`
- `s3_max_retries`
- `s3_retry_backoff_s`

## Runtime guardrails

- `single_writer_guard=True`: prevents concurrent writers on the same dataset
- `writer_lock_*`: lock key/ttl/refresh tuning
- `heartbeat_interval_s`: emits heartbeat metadata
- `lag_warn_threshold_blocks`: warns when lag grows
- `reorg_lookback_blocks`: rewrites recent range in listen mode

## Ops scripts

### Validate coverage

Checks chunk coverage and index consistency.

```bash
PYTHONPATH=src .venv/bin/python scripts/validate_coverage.py \
  --protocol uniswap \
  --contract usdc_weth \
  --event Mint --event Burn --event Swap
```

### Health check

Checks heartbeat freshness and lag thresholds.

```bash
PYTHONPATH=src .venv/bin/python scripts/check_indexer_health.py \
  --protocol uniswap \
  --contract usdc_weth \
  --heartbeat-key _meta/heartbeat.json \
  --max-heartbeat-age-s 180 \
  --max-lag-blocks 300
```

### Cleanup incomplete S3 multipart uploads

Use this when runs are interrupted during parquet upload and leave `Ongoing Multipart Upload` entries.

```bash
# Dry run
PYTHONPATH=src .venv/bin/python scripts/cleanup_s3_multipart_uploads.py \
  --protocol uniswap \
  --contract usdc_weth \
  --key-prefix CollectProtocol/ \
  --older-than-s 300

# Apply cleanup
PYTHONPATH=src .venv/bin/python scripts/cleanup_s3_multipart_uploads.py \
  --protocol uniswap \
  --contract usdc_weth \
  --key-prefix CollectProtocol/ \
  --older-than-s 300 \
  --apply
```

Environment variables supported by the cleanup script:
- `S3_BUCKET`
- `S3_PREFIX`
- `S3_ENDPOINT_URL`
- `S3_ACCESS_KEY`
- `S3_SECRET_KEY`
- `S3_REGION`

### Ops API

Run the API server:

```bash
source .venv/bin/activate
defind-ops-api
```

Equivalent script command:

```bash
PYTHONPATH=src .venv/bin/python scripts/run_api_server.py
```

Main endpoints:
- `GET /status`
- `GET /datasets`
- `POST /datasets`
- `GET /datasets/{protocol}/{contract}`
- `PATCH /datasets/{protocol}/{contract}`
- `GET /datasets/{protocol}/{contract}/jobs`
- `POST /datasets/{protocol}/{contract}/jobs`
- `GET /datasets/{protocol}/{contract}/jobs/{job_id}`
- `POST /datasets/{protocol}/{contract}/jobs/{job_id}/stop`
- `POST /datasets/{protocol}/{contract}/jobs/{job_id}/restart`
- `GET /datasets/{protocol}/{contract}/jobs/{job_id}/logs`
- `GET /datasets/{protocol}/{contract}/jobs/{job_id}/logs/stream`
- `GET /datasets/{protocol}/{contract}/coverage`
- `POST /indexer/abi/etherscan`

Main env vars:
- `DEFIND_API_OUT_ROOT` (default `./data`)
- `DEFIND_API_PORT` (default `8000`)
- `DEFIND_API_CORS_ORIGINS` (default `*`)
- `DEFIND_API_LOG_LEVEL` (default `INFO`)
- `DEFIND_API_LOG_JSON` (default `true`)
- `DEFIND_API_JSON_LOG_FILE` (optional local JSONL file for Loki/Alloy ingestion)
- `DEFIND_API_LOGS_BACKEND` (`event_store` or `loki`, default `event_store`)
- `DEFIND_API_LOKI_URL` (optional, for Loki-backed log reads)
- `DEFIND_API_ETHERSCAN_API_URL` (default `https://api.etherscan.io/v2/api`)
- `DEFIND_API_ETHERSCAN_CHAIN_ID` (default `1`)
- `DEFIND_API_EVENT_HISTORY_PREFIX` (default `_meta/event_history/`)
- `DEFIND_API_EVENT_HISTORY_LIMIT` (default `10000`)
- `ETHERSCAN_API_KEY` (optional)
- `S3_BUCKET`, `S3_ENDPOINT_URL`, `S3_ACCESS_KEY`, `S3_SECRET_KEY` (for S3 mode)

Minimal dataset creation payload:

```json
{
  "protocol": "uniswap",
  "contract": "usdc_weth",
  "contract_address": "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640",
  "chain_id": 1,
  "rpc_url": "https://ethereum-rpc.publicnode.com",
  "abi_path": "abis/cl_pool.json",
  "start_block": 12376729,
  "step": 7000,
  "chunk_size": 200000,
  "storage": "s3"
}
```

Minimal job start payload:

```json
{
  "mode": "both",
  "concurrency": 25
}
```

Local Grafana/Loki setup:

- see [ops/observability/README.md](ops/observability/README.md)

## Systemd setup (VPS)

Files are in `ops/systemd/`.

Indexer service:

```bash
cp ops/systemd/defind-indexer.env.example ops/systemd/defind-indexer.env
sudo cp ops/systemd/defind-indexer.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now defind-indexer.service
```

Watchdog timer:

```bash
cp ops/systemd/defind-watchdog.env.example ops/systemd/defind-watchdog.env
sudo cp ops/systemd/defind-watchdog.service /etc/systemd/system/
sudo cp ops/systemd/defind-watchdog.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now defind-watchdog.timer
```

## CLI

```bash
defind --help
defind version
```

## Development

```bash
source .venv/bin/activate
ruff check src scripts examples tests
pytest -q
mypy .
```
