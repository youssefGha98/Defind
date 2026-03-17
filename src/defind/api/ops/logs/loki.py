from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from typing import Any

import httpx


def _selector_value(value: str) -> str:
    return json.dumps(value)


def build_job_logs_query(
    *,
    service_name: str,
    dataset_id: str,
    job_id: str,
) -> str:
    return (
        "{"
        f"service={_selector_value(service_name)},"
        f"dataset_id={_selector_value(dataset_id)},"
        f"job_id={_selector_value(job_id)}"
        "}"
    )


def _iso_from_ns(ts_ns: int) -> str:
    return datetime.fromtimestamp(ts_ns / 1_000_000_000, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _line_to_event_row(
    *,
    ts_ns: int,
    line: str,
    labels: dict[str, str],
    dataset_id: str,
    job_id: str,
) -> dict[str, Any]:
    raw_payload: dict[str, Any]
    try:
        parsed = json.loads(line)
    except Exception:
        parsed = {"message": line}

    if isinstance(parsed, dict):
        raw_payload = dict(parsed)
    else:
        raw_payload = {"message": line}

    row_ts = str(raw_payload.get("ts") or _iso_from_ns(ts_ns))
    row_dataset_id = str(
        raw_payload.get("dataset_id")
        or raw_payload.get("datasetId")
        or labels.get("dataset_id")
        or dataset_id
    )
    row_job_id = str(
        raw_payload.get("job_id")
        or raw_payload.get("jobId")
        or labels.get("job_id")
        or job_id
    )
    run_id = raw_payload.get("run_id") or raw_payload.get("runId") or labels.get("run_id")
    if isinstance(parsed, dict):
        event_type = str(raw_payload.get("event") or raw_payload.get("eventType") or "log")
    else:
        event_type = "log"

    payload = dict(raw_payload)
    for key in ("ts", "event", "eventType", "dataset_id", "datasetId", "job_id", "jobId", "run_id", "runId"):
        payload.pop(key, None)

    return {
        "id": ts_ns,
        "tsUnixS": ts_ns // 1_000_000_000,
        "ts": row_ts,
        "eventType": event_type,
        "datasetId": row_dataset_id,
        "jobId": row_job_id,
        "runId": str(run_id) if run_id is not None else None,
        "payload": payload,
    }


def parse_loki_query_range_response(
    payload: dict[str, Any],
    *,
    dataset_id: str,
    job_id: str,
) -> list[dict[str, Any]]:
    data = payload.get("data")
    if not isinstance(data, dict):
        return []

    result = data.get("result")
    if not isinstance(result, list):
        return []

    rows: list[dict[str, Any]] = []
    for stream in result:
        if not isinstance(stream, dict):
            continue
        labels = stream.get("stream")
        values = stream.get("values")
        if not isinstance(labels, dict) or not isinstance(values, list):
            continue
        clean_labels = {str(key): str(value) for key, value in labels.items()}
        for item in values:
            if not isinstance(item, list) or len(item) != 2:
                continue
            raw_ts, raw_line = item
            try:
                ts_ns = int(str(raw_ts))
            except ValueError:
                continue
            rows.append(
                _line_to_event_row(
                    ts_ns=ts_ns,
                    line=str(raw_line),
                    labels=clean_labels,
                    dataset_id=dataset_id,
                    job_id=job_id,
                )
            )
    rows.sort(key=lambda row: int(row["id"]), reverse=True)
    return rows


class LokiClient:
    def __init__(
        self,
        *,
        base_url: str,
        service_name: str,
        timeout_s: float,
        lookback_s: int,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._service_name = service_name
        self._timeout_s = timeout_s
        self._lookback_s = lookback_s

    async def list_job_events(
        self,
        *,
        dataset_id: str,
        job_id: str,
        limit: int,
    ) -> list[dict[str, Any]]:
        end_ns = int(time.time() * 1_000_000_000)
        start_ns = max(0, end_ns - (self._lookback_s * 1_000_000_000))
        params = {
            "query": build_job_logs_query(
                service_name=self._service_name,
                dataset_id=dataset_id,
                job_id=job_id,
            ),
            "start": str(start_ns),
            "end": str(end_ns),
            "limit": str(max(1, min(1000, limit))),
            "direction": "backward",
        }
        async with httpx.AsyncClient(base_url=self._base_url, timeout=self._timeout_s) as client:
            response = await client.get("/loki/api/v1/query_range", params=params)
            response.raise_for_status()
            payload = response.json()

        return parse_loki_query_range_response(payload, dataset_id=dataset_id, job_id=job_id)[:limit]
