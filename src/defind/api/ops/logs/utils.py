from __future__ import annotations

import json
import logging
from typing import Any

from defind.observability import get_otel_log_fields

RESERVED_LOG_RECORD_KEYS = frozenset(logging.makeLogRecord({}).__dict__.keys())


def log_record_payload(record: logging.LogRecord) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "level": record.levelname,
        "logger": record.name,
    }
    payload.update(get_otel_log_fields())
    for key, value in record.__dict__.items():
        if key in RESERVED_LOG_RECORD_KEYS or key.startswith("_"):
            continue
        payload[key] = value
    if record.exc_info:
        payload["error"] = str(record.exc_info[1])
        payload["exc_info"] = logging.Formatter().formatException(record.exc_info)
    return payload


def matches_level_filter(payload: dict[str, Any], level: str) -> bool:
    normalized_level = level.upper().strip() or "ALL"
    payload_level = str(payload.get("level") or "").upper()
    if normalized_level == "ERROR":
        return payload_level == "ERROR"
    if normalized_level == "WARN":
        return payload_level in {"WARNING", "WARN", "ERROR"}
    return True


def encode_sse(row: dict[str, Any]) -> bytes:
    return f"data: {json.dumps(row, separators=(',', ':'))}\n\n".encode()
