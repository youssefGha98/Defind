"""Orchestration for streaming logs with resumability and dynamic projections.

This package provides:
- Main orchestrator (fetch_decode) for fetching, decoding, and writing logs
- Interval utilities for coverage tracking and resumability
- Manifest loading for resumable operations
"""

from defind.orchestration.orchestrator import fetch_decode
from defind.orchestration.utils import (
    iter_chunks,
    load_done_coverage,
    merge_intervals,
    subtract_iv,
    topics_fingerprint,
)

__all__ = [
    "fetch_decode",
    "iter_chunks",
    "load_done_coverage",
    "merge_intervals",
    "subtract_iv",
    "topics_fingerprint",
]
