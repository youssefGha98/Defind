from __future__ import annotations

import time

from defind.core.indexer_request.payloads import sanitize_request_payload
from defind.core.interfaces import IChunkStorage

INDEXER_REQUEST_KEY = "_meta/indexer_request.json"


def write_indexer_request(
    *,
    storage: IChunkStorage,
    request_payload: dict[str, object],
    source: str,
) -> None:
    dataset_id = f"{request_payload.get('protocol_slug')}/{request_payload.get('contract_slug')}"
    storage.write_json(
        INDEXER_REQUEST_KEY,
        {
            "version": 1,
            "savedAtS": int(time.time()),
            "source": source,
            "datasetId": dataset_id,
            "request": sanitize_request_payload(request_payload),
        },
    )


def read_indexer_request(storage: IChunkStorage) -> dict[str, object] | None:
    payload = storage.read_json(INDEXER_REQUEST_KEY)
    if not isinstance(payload, dict):
        return None
    request_payload = payload.get("request")
    if not isinstance(request_payload, dict):
        return None
    return sanitize_request_payload(request_payload)
