from __future__ import annotations

import json

from defind.api.ops.logs.loki import build_job_logs_query, parse_loki_query_range_response


def test_build_job_logs_query_escapes_selector_values() -> None:
    query = build_job_logs_query(
        service_name='defind-"api"',
        dataset_id="uniswap/usdc_weth",
        job_id="job-1",
    )

    assert 'service="defind-\\"api\\""' in query
    assert 'dataset_id="uniswap/usdc_weth"' in query
    assert 'job_id="job-1"' in query


def test_parse_loki_query_range_response_maps_json_lines() -> None:
    payload = {
        "status": "success",
        "data": {
            "resultType": "streams",
            "result": [
                {
                    "stream": {
                        "service": "defind-api",
                        "dataset_id": "uniswap/usdc_weth",
                        "job_id": "job-1",
                    },
                    "values": [
                        [
                            "1741857601000000000",
                            json.dumps(
                                {
                                    "ts": "2026-03-13T10:00:01Z",
                                    "event": "dataset_job_started",
                                    "level": "info",
                                    "logger": "ops_events",
                                    "dataset_id": "uniswap/usdc_weth",
                                    "job_id": "job-1",
                                    "run_id": "run-1",
                                    "mode": "both",
                                }
                            ),
                        ],
                        [
                            "1741857600000000000",
                            json.dumps(
                                {
                                    "ts": "2026-03-13T10:00:00Z",
                                    "event": "chunk_written",
                                    "level": "info",
                                    "logger": "defind.orchestrator",
                                    "dataset_id": "uniswap/usdc_weth",
                                    "job_id": "job-1",
                                    "run_id": "run-1",
                                    "chunks_written": 1,
                                }
                            ),
                        ],
                    ],
                }
            ]
        },
    }

    rows = parse_loki_query_range_response(
        payload,
        dataset_id="uniswap/usdc_weth",
        job_id="job-1",
    )

    assert [row["eventType"] for row in rows] == ["dataset_job_started", "chunk_written"]
    assert rows[0]["datasetId"] == "uniswap/usdc_weth"
    assert rows[0]["jobId"] == "job-1"
    assert rows[0]["runId"] == "run-1"
    assert rows[0]["payload"]["mode"] == "both"
    assert rows[1]["payload"]["chunks_written"] == 1
