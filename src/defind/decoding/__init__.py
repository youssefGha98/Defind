"""Event decoding with dynamic projections.

This package provides:
- Event specification system (EventSpec, TopicFieldSpec, DataFieldSpec)
- Generic decoder that translates raw logs into ParsedEvent objects
- Registry management for event specs
- Pre-built registries for common DeFi protocols
"""

from defind.decoding.decoder import ParsedEvent, decode_event
from defind.decoding.registry import add_event_spec, add_many, make_registry
from defind.decoding.specs import (
    DataFieldSpec,
    EventRegistry,
    EventSpec,
    Projection,
    TopicFieldSpec,
)

__all__ = [
    "ParsedEvent",
    "decode_event",
    "add_event_spec",
    "add_many",
    "make_registry",
    "DataFieldSpec",
    "EventRegistry",
    "EventSpec",
    "Projection",
    "TopicFieldSpec",
]
