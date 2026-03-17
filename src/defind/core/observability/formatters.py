from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

from defind.core.observability.otel import get_otel_log_fields

RESERVED_LOG_RECORD_KEYS = frozenset(logging.makeLogRecord({}).__dict__.keys())


def extract_extra(record: logging.LogRecord) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in record.__dict__.items():
        if key in RESERVED_LOG_RECORD_KEYS or key.startswith("_"):
            continue
        out[key] = value
    return out


def compact_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, separators=(",", ":"), sort_keys=True, default=str)


class JsonLogFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            "level": record.levelname.lower(),
            "logger": record.name,
            "event": record.getMessage(),
        }
        payload.update(get_otel_log_fields())
        payload.update(extract_extra(record))
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return compact_json(payload)


class TextLogFormatter(logging.Formatter):
    def __init__(self) -> None:
        super().__init__(datefmt="%Y-%m-%d %H:%M:%S")

    def format(self, record: logging.LogRecord) -> str:
        timestamp = self.formatTime(record, self.datefmt)
        message = f"{timestamp} [{record.levelname}] {record.getMessage()}"
        extras = extract_extra(record)
        if extras:
            message = f"{message} {compact_json(extras)}"
        if record.exc_info:
            message = f"{message}\n{self.formatException(record.exc_info)}"
        if record.stack_info:
            message = f"{message}\n{self.formatStack(record.stack_info)}"
        return message
