from __future__ import annotations

import contextlib
import contextvars
from collections.abc import Iterator
from typing import Any

LOG_CONTEXT: contextvars.ContextVar[dict[str, Any] | None] = contextvars.ContextVar(
    "defind_log_context",
    default=None,
)


def drop_none(values: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in values.items() if v is not None}


def context_snapshot() -> dict[str, Any]:
    current = LOG_CONTEXT.get()
    if isinstance(current, dict):
        return dict(current)
    return {}


@contextlib.contextmanager
def bind_log_context(**context: Any) -> Iterator[None]:
    merged = context_snapshot()
    merged.update(drop_none(context))
    token = LOG_CONTEXT.set(merged)
    try:
        yield
    finally:
        LOG_CONTEXT.reset(token)
