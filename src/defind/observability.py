from __future__ import annotations

import contextlib
import contextvars
import json
import logging
from collections.abc import Iterator, MutableMapping
from datetime import datetime, timezone
from typing import Any

_LOG_CONTEXT: contextvars.ContextVar[dict[str, Any] | None] = contextvars.ContextVar(
    "defind_log_context",
    default=None,
)
_RESERVED_LOG_RECORD_KEYS = frozenset(logging.makeLogRecord({}).__dict__.keys())


def _drop_none(values: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in values.items() if v is not None}


def _context_snapshot() -> dict[str, Any]:
    current = _LOG_CONTEXT.get()
    if isinstance(current, dict):
        return dict(current)
    return {}


def _extract_extra(record: logging.LogRecord) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in record.__dict__.items():
        if key in _RESERVED_LOG_RECORD_KEYS or key.startswith("_"):
            continue
        out[key] = value
    return out


class JsonLogFormatter(logging.Formatter):
    """Lightweight JSON formatter for machine-readable operational logs."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            "level": record.levelname.lower(),
            "logger": record.name,
            "event": record.getMessage(),
        }
        payload.update(_extract_extra(record))
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, separators=(",", ":"), sort_keys=True, default=str)


class ContextLoggerAdapter(logging.LoggerAdapter[logging.Logger]):
    """Logger adapter that enriches log records with bound async context."""

    def process(
        self,
        msg: Any,
        kwargs: MutableMapping[str, Any],
    ) -> tuple[Any, MutableMapping[str, Any]]:
        merged = _context_snapshot()
        if self.extra:
            merged.update(self.extra)

        current_extra = kwargs.get("extra")
        if isinstance(current_extra, dict):
            merged.update(current_extra)
        kwargs["extra"] = _drop_none(merged)
        return msg, kwargs


def get_logger(name: str, **bound: Any) -> ContextLoggerAdapter:
    """Return a context-aware logger for the given module name."""
    return ContextLoggerAdapter(logging.getLogger(name), _drop_none(bound))


@contextlib.contextmanager
def bind_log_context(**context: Any) -> Iterator[None]:
    """Bind key/value pairs to all logs emitted in the current async context."""
    merged = _context_snapshot()
    merged.update(_drop_none(context))
    token = _LOG_CONTEXT.set(merged)
    try:
        yield
    finally:
        _LOG_CONTEXT.reset(token)


def configure_logging(*, level: str = "INFO", json_logs: bool = True) -> None:
    """Configure root logging once; preserve existing handlers when present."""
    root = logging.getLogger()
    root.setLevel(level.upper())

    if root.handlers:
        return

    handler = logging.StreamHandler()
    if json_logs:
        handler.setFormatter(JsonLogFormatter())
    else:
        handler.setFormatter(
            logging.Formatter(
                fmt="%(asctime)s %(levelname)s %(name)s %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
    root.addHandler(handler)
