# Local Logs Stack

This folder gives you a local logs stack for Defind:

- `Grafana` for the UI
- `Loki` for log storage and search
- `Alloy` for shipping local JSON log files to Loki

## Why this setup

The API already exposes `/jobs/{id}/logs`, but the legacy storage-backed path is slow because it scans and reads many small files.

With this setup:

1. Defind writes structured JSON logs to `runtime/logs/defind-api.jsonl`
2. Alloy tails that file
3. Alloy sends lines to Loki
4. Grafana reads Loki
5. The Defind API can proxy Loki for the existing logs endpoints

## Files

- `docker-compose.yml`
  Starts Loki, Grafana, and Alloy locally.
- `loki/config.yml`
  Minimal local Loki config using filesystem storage.
- `alloy/config.alloy`
  Tails `runtime/logs/*.jsonl`, extracts JSON fields, promotes key fields as Loki labels, and pushes to Loki.
- `grafana/provisioning/datasources/loki.yaml`
  Auto-registers Loki as the default Grafana datasource.

## Prerequisites

- Docker
- Docker Compose
- The Defind API running on your host machine

## 1. Start the logs stack

From the repo root:

```bash
cd ops/observability
docker compose up -d
```

This exposes:

- Grafana: `http://127.0.0.1:3000`
- Loki: `http://127.0.0.1:3100`

Grafana default credentials in this local setup:

- username: `admin`
- password: `admin`

## 2. Run the API with JSON file logging enabled

From the repo root:

```bash
export DEFIND_API_LOG_JSON=true
export DEFIND_API_JSON_LOG_FILE=./runtime/logs/defind-api.jsonl
export DEFIND_API_LOGS_BACKEND=loki
export DEFIND_API_LOKI_URL=http://127.0.0.1:3100
defind-ops-api
```

What these variables do:

- `DEFIND_API_LOG_JSON=true`
  Keeps logs structured.
- `DEFIND_API_JSON_LOG_FILE=...`
  Writes JSON lines to a local file that Alloy can tail.
- `DEFIND_API_LOGS_BACKEND=loki`
  Makes the API read logs from Loki first.
- `DEFIND_API_LOKI_URL=...`
  Tells the API where Loki lives.

## 3. Generate some logs

Start a job from the API or UI. This should append lines to:

```bash
tail -f runtime/logs/defind-api.jsonl
```

You should see JSON lines containing fields like:

- `dataset_id`
- `job_id`
- `run_id`
- `level`
- `event`

## 4. Check Grafana

Open Grafana and go to `Explore`.

Example Loki query:

```logql
{service="defind-api", dataset_id="uniswap/usdc_weth", job_id="job-1"}
```

If logs appear there, the ingestion path is working.

## 5. Check the Defind API proxy path

Once Loki has data, the existing API endpoints should use Loki:

- `GET /datasets/{protocol}/{contract}/jobs/{job_id}/logs`
- `GET /datasets/{protocol}/{contract}/jobs/{job_id}/logs/stream`

The front can keep using the Defind backend instead of calling Loki directly.

## Notes

- This is a local/dev setup. For production, pin container versions instead of using `latest`.
- The API still keeps its lightweight internal event store as a fallback.
- Deleting a job does not delete logs already stored in Loki.
