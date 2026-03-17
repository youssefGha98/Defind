from __future__ import annotations

from typing import Any

from defind.abi_events import AbiEvent, get_event_signature, get_event_topic0


def event_descriptors(events: dict[str, AbiEvent]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for name in sorted(events.keys()):
        event = events[name]
        out.append(
            {
                "name": event.name,
                "signature": get_event_signature(event),
                "topic0": get_event_topic0(event),
                "indexedInputs": sum(1 for item in event.inputs if item.indexed),
                "nonIndexedInputs": sum(1 for item in event.inputs if not item.indexed),
                "inputs": [
                    {
                        "name": item.name,
                        "type": item.type,
                        "indexed": item.indexed,
                    }
                    for item in event.inputs
                ],
            }
        )
    return out
