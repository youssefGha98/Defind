from __future__ import annotations

import logging
from collections.abc import MutableMapping
from typing import Any

from defind.core.observability.context import context_snapshot, drop_none
from defind.core.observability.otel import get_otel_log_fields


class ContextLoggerAdapter(logging.LoggerAdapter[logging.Logger]):
    def process(
        self,
        msg: Any,
        kwargs: MutableMapping[str, Any],
    ) -> tuple[Any, MutableMapping[str, Any]]:
        merged = context_snapshot()
        if self.extra:
            merged.update(self.extra)
        merged.update(get_otel_log_fields())

        current_extra = kwargs.get("extra")
        if isinstance(current_extra, dict):
            merged.update(current_extra)
        kwargs["extra"] = drop_none(merged)
        return msg, kwargs


def get_logger(name: str, **bound: Any) -> ContextLoggerAdapter:
    return ContextLoggerAdapter(logging.getLogger(name), drop_none(bound))
