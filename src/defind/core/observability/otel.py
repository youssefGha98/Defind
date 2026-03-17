from __future__ import annotations

import contextlib
from typing import Any

from defind.core.observability.context import drop_none


def get_otel_log_fields() -> dict[str, Any]:
    try:
        from opentelemetry import trace
    except Exception:
        return {}

    span = trace.get_current_span()
    if span is None:
        return {}
    context = span.get_span_context()
    if context is None or not context.is_valid:
        return {}

    trace_flags = getattr(context, "trace_flags", None)
    trace_flags_value: str | None = None
    if trace_flags is not None:
        with contextlib.suppress(TypeError, ValueError):
            trace_flags_value = format(int(trace_flags), "02x")

    return drop_none(
        {
            "trace_id": format(context.trace_id, "032x"),
            "span_id": format(context.span_id, "016x"),
            "trace_flags": trace_flags_value,
        }
    )
