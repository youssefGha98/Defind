"""Default event registry for Velodrome CL pools and Gauge events.

This module exposes:
- `make_registry()` → EventRegistry prefilled with common specs
- `add_event_spec(registry, spec)` → append one spec (lowercases key)
- `add_many(registry, specs)` → append multiple

Extending the system only requires adding more `EventSpec` entries here.
No adapters; all columns come from dynamic projections.
"""

from __future__ import annotations

from collections.abc import Iterable

from .specs import EventRegistry, EventSpec


def make_registry() -> EventRegistry:
    """Build the default registry with core Velodrome/Gauge event specs."""
    reg: EventRegistry = {}
    return reg


def add_event_spec(registry: EventRegistry, spec: EventSpec) -> None:
    """Insert one spec into the registry keyed by lowercased topic0."""
    registry[spec.topic0.lower()] = spec


def add_many(registry: EventRegistry, specs: Iterable[EventSpec]) -> None:
    """Insert many specs into the registry."""
    for s in specs:
        add_event_spec(registry, s)
