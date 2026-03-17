from __future__ import annotations

import logging
from pathlib import Path

from defind.core.observability.formatters import JsonLogFormatter, TextLogFormatter


def _set_handler_formatter(handler: logging.Handler, *, json_logs: bool) -> None:
    kind = getattr(handler, "_defind_handler_kind", "stream")
    formatter: logging.Formatter
    if kind == "json_file":
        formatter = JsonLogFormatter()
    elif json_logs:
        formatter = JsonLogFormatter()
    else:
        formatter = TextLogFormatter()
    handler.setFormatter(formatter)


def _ensure_json_file_handler(root: logging.Logger, *, json_log_file_path: Path | None) -> None:
    for handler in root.handlers:
        if getattr(handler, "_defind_handler_kind", None) == "json_file":
            current_path = getattr(handler, "_defind_log_path", None)
            if json_log_file_path is None or current_path == str(json_log_file_path):
                return

    if json_log_file_path is None:
        return

    json_log_file_path.parent.mkdir(parents=True, exist_ok=True)
    file_handler = logging.FileHandler(json_log_file_path, encoding="utf-8")
    file_handler._defind_handler_kind = "json_file"  # type: ignore[attr-defined]
    file_handler._defind_log_path = str(json_log_file_path)  # type: ignore[attr-defined]
    file_handler.setFormatter(JsonLogFormatter())
    root.addHandler(file_handler)


def configure_logging(
    *,
    level: str = "INFO",
    json_logs: bool = True,
    json_log_file_path: Path | None = None,
) -> None:
    root = logging.getLogger()
    root.setLevel(level.upper())

    if root.handlers:
        for handler in root.handlers:
            _set_handler_formatter(handler, json_logs=json_logs)
        _ensure_json_file_handler(root, json_log_file_path=json_log_file_path)
        return

    handler = logging.StreamHandler()
    handler._defind_handler_kind = "stream"  # type: ignore[attr-defined]
    _set_handler_formatter(handler, json_logs=json_logs)
    root.addHandler(handler)
    _ensure_json_file_handler(root, json_log_file_path=json_log_file_path)
