"""Registry builder utilities for creating event registries from signatures.

This module provides the core tools for building EventRegistry instances:
- Generic `make_registry()` function for single or multiple signatures
- Signature parsing helpers for converting Solidity event signatures to EventSpec
"""

from __future__ import annotations

from typing import Optional
from eth_utils import keccak

from .specs import EventRegistry, EventSpec, TopicFieldSpec, DataFieldSpec, Projection


# ---- Helpers: build specs from event signature ----
def _split_params(params_str: str) -> list[str]:
    """Split the parameter list by commas while respecting nested tuple types.

    Very lightweight splitter sufficient for typical event signatures.
    """
    items: list[str] = []
    depth = 0
    buf: list[str] = []
    for ch in params_str:
        if ch == '(':
            depth += 1
            buf.append(ch)
        elif ch == ')':
            depth -= 1
            buf.append(ch)
        elif ch == ',' and depth == 0:
            items.append(''.join(buf).strip())
            buf = []
        else:
            buf.append(ch)
    if buf:
        items.append(''.join(buf).strip())
    # Handle empty list for no params
    return [i for i in items if i]


def _parse_param(p: str, fallback_name: str) -> tuple[str, str, bool]:
    """Parse one parameter fragment into (name, abi_type, indexed)."""
    s = ' '.join(p.strip().split())  # normalize spaces
    indexed = False
    # Remove trailing/leading spaces around 'indexed'
    parts = s.replace(' indexed ', ' ').replace(' indexed', ' ').replace('indexed ', 'indexed ').split()
    # But the above could break tuple types; do a safer approach:
    if ' indexed ' in f' {s} ':
        indexed = True
        s = f' {s} '.replace(' indexed ', ' ').strip()
    tokens = s.split()
    if not tokens:
        # Should not happen, synthesize
        return (fallback_name, 'bytes32', indexed)
    if len(tokens) == 1:
        # Unnamed parameter
        abi_type = tokens[0]
        name = fallback_name
    else:
        # Last token is the name, the rest is the type (can include tuple syntax)
        name = tokens[-1]
        abi_type = ' '.join(tokens[:-1])
    return (name, abi_type, indexed)


def event_spec_from_signature(
    signature: str,
    projection: Optional[Projection] = None,
    *,
    fast_zero_words: tuple[int, ...] = (),
    drop_if_all_zero_fields: tuple[str, ...] = (),
) -> EventSpec:
    """Build an EventSpec from a Solidity event signature string.

    Example input:
      "GaugeCreated(address indexed pool, address indexed gauge, address indexed reward)"
    """
    sig = signature.strip()
    # Extract name and parameters content
    open_paren = sig.find('(')
    close_paren = sig.rfind(')')
    if open_paren == -1 or close_paren == -1 or close_paren < open_paren:
        raise ValueError(f"Invalid event signature: {signature}")
    name = sig[:open_paren].strip()
    params_str = sig[open_paren + 1 : close_paren].strip()

    # Compute topic0 hash from canonical type list (exclude names and 'indexed')
    param_parts = _split_params(params_str)
    parsed = []
    indexed_params: list[tuple[str, str]] = []  # (name, type)
    data_params: list[tuple[str, str]] = []  # (name, type)
    for i, part in enumerate(param_parts):
        name_i, abi_type_i, is_indexed = _parse_param(part, fallback_name=f"arg{i}")
        parsed.append((name_i, abi_type_i, is_indexed))
        if is_indexed:
            indexed_params.append((name_i, abi_type_i))
        else:
            data_params.append((name_i, abi_type_i))

    canonical_types = ','.join(t for (_, t, _) in parsed)
    canonical_signature = f"{name}({canonical_types})"
    topic0 = '0x' + keccak(text=canonical_signature).hex()

    # Build field specs
    topic_fields = [
        TopicFieldSpec(n, idx + 1, t) for idx, (n, t) in enumerate(indexed_params)
    ]
    data_fields = [
        DataFieldSpec(n, idx, t) for idx, (n, t) in enumerate(data_params)
    ]

    # Default projection: expose each field under its own name
    if projection is None:
        proj: dict[str, str] = {}
        for n, _t in indexed_params:
            proj[n] = f"topic.{n}"
        for n, _t in data_params:
            proj[n] = f"data.{n}"
        projection = proj

    return EventSpec(
        topic0=topic0,
        name=name,
        topic_fields=topic_fields,
        data_fields=data_fields,
        projection=projection,
        fast_zero_words=fast_zero_words,
        drop_if_all_zero_fields=drop_if_all_zero_fields,
    )


def registry_from_signature(
    signature: str,
    projection: Optional[Projection] = None,
    *,
    fast_zero_words: tuple[int, ...] = (),
    drop_if_all_zero_fields: tuple[str, ...] = (),
) -> EventRegistry:
    """Build an EventRegistry (single entry) from a signature string."""
    spec = event_spec_from_signature(
        signature,
        projection,
        fast_zero_words=fast_zero_words,
        drop_if_all_zero_fields=drop_if_all_zero_fields,
    )
    return {spec.topic0: spec}


def make_registry(
    signatures: str | list[str],
    *,
    fast_zero_words: tuple[int, ...] = (),
    drop_if_all_zero_fields: tuple[str, ...] = (),
) -> EventRegistry:
    """Create a registry from one or multiple event signatures.
    
    Args:
        signatures: Single signature string or list of signature strings
        fast_zero_words: Optional tuple of word indices for fast zero checking
        drop_if_all_zero_fields: Optional tuple of field names to drop if all zero
        
    Returns:
        EventRegistry with entries for each signature
    """
    reg: EventRegistry = {}
    
    # Normalize to list
    sig_list = [signatures] if isinstance(signatures, str) else signatures
    
    for signature in sig_list:
        event_reg = registry_from_signature(
            signature,
            fast_zero_words=fast_zero_words,
            drop_if_all_zero_fields=drop_if_all_zero_fields,
        )
        reg.update(event_reg)
    
    return reg
