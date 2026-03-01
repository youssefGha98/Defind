"""Diagnostic script: identify where rows are missing vs Dune reference.

Distinguishes two failure modes:
  1. RPC level  → eth_getLogs returns fewer logs than expected
  2. Decode level → logs arrive from RPC but are silently filtered by decode_event

Also checks per event type how many logs are "empty" (all key fields = 0),
which is what drop_if_all_zero_fields would filter if the clpool registry is used.

Usage:
    cd Defind/
    # Edit RPC_URL, ADDRESS, STEP, PROBLEM_RANGES below, then:
    .venv/bin/python debug_missing.py
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from defind.abi_events import make_event_registry_from_abi
from defind.clients.rpc import RPC
from defind.decoding.decoder import (
    _should_skip_all_zero_fields,
    _should_skip_fast_zero,
    _validate_and_get_spec,
)
from defind.decoding.registries import make_clpool_registry
from defind.decoding.utils import parse_data_word, word_at
from defind.orchestration.utils import iter_chunks

# ---------------------------------------------------------------------------
# Config — edit these
# ---------------------------------------------------------------------------

RPC_URL = "https://ethereum-rpc.publicnode.com"
ADDRESS = "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640"  # WETH/USDC 0.05%
ABI_PATH = Path("abis/cl_pool.json")
STEP = 1_000  # same step used during indexing

# (from_block, to_block inclusive, expected_row_count from Dune)
PROBLEM_RANGES = [
    (23_576_729, 23_776_728, 2475),
    (21_376_729, 21_576_728, 2225),
    (22_776_729, 22_976_728, 2921),
    (22_976_729, 23_176_728, 3951),
    (23_376_729, 23_576_728, 3519),
]

# ---------------------------------------------------------------------------
# Build "filter config" from the hardcoded clpool registry
# event_name → tuple of field names that must ALL be 0 to drop the row
# ---------------------------------------------------------------------------

def _build_filter_config() -> dict[str, tuple[str, ...]]:
    """Extract drop_if_all_zero_fields from the hardcoded clpool registry."""
    reg = make_clpool_registry()
    return {
        spec.name: spec.drop_if_all_zero_fields
        for spec in reg.values()
        if spec.drop_if_all_zero_fields
    }


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass
class SubRangeResult:
    from_block: int
    to_block: int
    raw_count: int
    error: str | None = None


@dataclass
class EventBreakdown:
    """Per-event-type counts for a range."""
    raw: int = 0              # logs recognized by the ABI registry
    empty: int = 0            # logs where all "key" fields are 0 (would be filtered by clpool registry)


@dataclass
class FilterCounts:
    empty_topics: int = 0
    unknown_topic0: int = 0
    fast_zero: int = 0
    insufficient_topics: int = 0
    insufficient_data: int = 0
    all_zero_fields: int = 0  # active filter (only if registry has drop_if_all_zero_fields set)

    @property
    def total(self) -> int:
        return (
            self.empty_topics
            + self.unknown_topic0
            + self.fast_zero
            + self.insufficient_topics
            + self.insufficient_data
            + self.all_zero_fields
        )


@dataclass
class RangeReport:
    from_block: int
    to_block: int
    expected: int
    raw_total: int
    decoded_total: int
    filters: FilterCounts
    per_event: dict[str, EventBreakdown]  # event_name → breakdown
    duplicates: int
    rpc_errors: int
    sub_ranges: list[SubRangeResult] = field(default_factory=list)

    @property
    def delta_rpc(self) -> int:
        """Negative = missing at RPC level."""
        return self.raw_total - self.expected

    @property
    def delta_decode(self) -> int:
        """Negative = logs lost during decoding."""
        return self.decoded_total - self.raw_total

    @property
    def total_empty(self) -> int:
        """Total logs that would be dropped by drop_if_all_zero_fields."""
        return sum(b.empty for b in self.per_event.values())


# ---------------------------------------------------------------------------
# Core diagnostic function
# ---------------------------------------------------------------------------


async def diagnose_range(
    rpc: RPC,
    registry: Any,
    filter_config: dict[str, tuple[str, ...]],
    from_block: int,
    to_block: int,
    expected: int,
    step: int,
    topic0s: list[str],
    address: str,
) -> RangeReport:
    all_logs = []
    sub_results: list[SubRangeResult] = []
    rpc_errors = 0

    # Sequential fetch so we can see per-sub-range counts
    for a, b in iter_chunks(from_block, to_block, step):
        try:
            logs = await rpc.get_logs(
                address=address,
                topic0s=topic0s,
                from_block=a,
                to_block=b,
            )
            all_logs.extend(logs)
            sub_results.append(SubRangeResult(a, b, len(logs)))
        except Exception as e:
            sub_results.append(SubRangeResult(a, b, 0, str(e)))
            rpc_errors += 1

    # Duplicate detection
    seen: set[tuple] = set()
    duplicates = 0
    for log in all_logs:
        key = (log.block_number, log.tx_hash, log.log_index)
        if key in seen:
            duplicates += 1
        seen.add(key)

    # Instrumented decode
    filters = FilterCounts()
    per_event: dict[str, EventBreakdown] = {}
    decoded = 0

    for ev in all_logs:
        # 1. empty topics
        if not ev.topics:
            filters.empty_topics += 1
            continue

        # 2. unknown topic0
        spec = _validate_and_get_spec(ev.topics, registry)
        if spec is None:
            filters.unknown_topic0 += 1
            continue

        # 3. parse data bytes
        data_hex = ev.data_hex[2:] if ev.data_hex.lower().startswith("0x") else ev.data_hex
        data = bytes.fromhex(data_hex) if data_hex else b""

        # 4. fast-zero filter (registry-driven)
        if _should_skip_fast_zero(spec, data):
            filters.fast_zero += 1
            continue

        # 5. topic fields bounds check
        topic_ok = True
        for tf in spec.topic_fields:
            if tf.index >= len(ev.topics):
                filters.insufficient_topics += 1
                topic_ok = False
                break
        if not topic_ok:
            continue

        # 6. data size check
        if spec.data_fields:
            need_words = max(df.word_index for df in spec.data_fields) + 1
            if len(data) < 32 * need_words:
                filters.insufficient_data += 1
                continue

        # 7. parse data values
        data_vals = {
            df.name: parse_data_word(word_at(data, df.word_index), df.type)
            for df in spec.data_fields
        }

        # 8. all-zero fields filter (registry-driven — active only if spec has it set)
        if _should_skip_all_zero_fields(spec, data_vals):
            filters.all_zero_fields += 1
            continue

        # Log is decoded
        decoded += 1
        ev_name = spec.name
        if ev_name not in per_event:
            per_event[ev_name] = EventBreakdown()
        per_event[ev_name].raw += 1

        # Shadow check: would this event be filtered by the clpool hardcoded filter?
        zero_fields = filter_config.get(ev_name, ())
        if zero_fields and all(int(data_vals.get(f, 0) or 0) == 0 for f in zero_fields):
            per_event[ev_name].empty += 1

    return RangeReport(
        from_block=from_block,
        to_block=to_block,
        expected=expected,
        raw_total=len(all_logs),
        decoded_total=decoded,
        filters=filters,
        per_event=per_event,
        duplicates=duplicates,
        rpc_errors=rpc_errors,
        sub_ranges=sub_results,
    )


# ---------------------------------------------------------------------------
# Reporter
# ---------------------------------------------------------------------------


def _sign(n: int) -> str:
    return f"+{n}" if n > 0 else str(n)


def print_report(r: RangeReport, filter_config: dict[str, tuple[str, ...]]) -> None:
    print(f"\n{'='*65}")
    print(f"Range [{r.from_block:,} .. {r.to_block:,}]  ({r.to_block - r.from_block + 1:,} blocks)")
    print(f"{'='*65}")

    print(f"  Expected (Dune) : {r.expected:>8,}")
    print(f"  Raw logs (RPC)  : {r.raw_total:>8,}  [{_sign(r.delta_rpc)} vs expected]"
          + (f"  ← PROBLEM: missing at RPC level" if r.delta_rpc < 0 else ""))
    print(f"  Decoded rows    : {r.decoded_total:>8,}  [{_sign(r.delta_decode)} vs raw]"
          + (f"  ← PROBLEM: decode filters active" if r.delta_decode < 0 else ""))

    if r.rpc_errors:
        print(f"  RPC errors      : {r.rpc_errors:>8,}  ← sub-ranges that failed!")
    if r.duplicates:
        print(f"  Duplicates      : {r.duplicates:>8,}  ← same (block, tx, log_index) seen twice")

    # Per-event breakdown
    print()
    print(f"  {'Event':<26} {'decoded':>8}  {'empty (all-zero)':>17}  fields checked")
    print(f"  {'-'*26} {'-'*8}  {'-'*17}  {'-'*30}")
    for ev_name in sorted(r.per_event):
        b = r.per_event[ev_name]
        zf = filter_config.get(ev_name, ())
        fields_str = "+".join(zf) if zf else "(no filter defined)"
        empty_marker = f"  ← {b.empty} EMPTY rows" if b.empty > 0 else ""
        print(f"  {ev_name:<26} {b.raw:>8,}  {b.empty:>17,}{empty_marker}")
        if b.empty > 0:
            print(f"  {'':26} {'':8}   fields: {fields_str}")

    total_empty = r.total_empty
    if total_empty > 0:
        print()
        missing = r.expected - r.decoded_total
        print(f"  Total empty rows (would be filtered) : {total_empty:,}")
        print(f"  Missing vs Dune                      : {missing:,}")
        if total_empty == abs(missing):
            print(f"  → MATCH: empty rows explain ALL missing rows")
        elif total_empty > 0 and missing > 0:
            pct = 100 * total_empty / missing if missing else 0
            print(f"  → empty rows account for {pct:.0f}% of missing rows")

    # Active filter counts
    print()
    print("  Active decode filters (registry-driven):")
    fc = r.filters
    for label, count in [
        ("empty_topics",        fc.empty_topics),
        ("unknown_topic0",      fc.unknown_topic0),
        ("fast_zero_words",     fc.fast_zero),
        ("insufficient_topics", fc.insufficient_topics),
        ("insufficient_data",   fc.insufficient_data),
        ("all_zero_fields",     fc.all_zero_fields),
    ]:
        if count > 0:
            print(f"    {label:<26}: {count:>6,}  ← active")
    if fc.total == 0:
        print("    (none — ABI registry has no active filters)")

    # Suspicious sub-ranges
    suspicious = [s for s in r.sub_ranges if s.error or s.raw_count == 0]
    if suspicious:
        print()
        print(f"  Suspicious sub-ranges ({len(suspicious)} / {len(r.sub_ranges)}):")
        for s in suspicious[:20]:
            if s.error:
                print(f"    [{s.from_block:,}..{s.to_block:,}]  ERROR: {s.error}")
            else:
                print(f"    [{s.from_block:,}..{s.to_block:,}]  0 logs")
        if len(suspicious) > 20:
            print(f"    ... +{len(suspicious) - 20} more")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


async def main() -> None:
    registry = make_event_registry_from_abi(ABI_PATH)
    topic0s = list(registry.keys())
    filter_config = _build_filter_config()

    print(f"ABI      : {ABI_PATH}")
    print(f"Contract : {ADDRESS}")
    print(f"Events   : {[spec.name for spec in registry.values()]}")
    print(f"Step     : {STEP:,} blocks/call")
    print(f"Ranges   : {len(PROBLEM_RANGES)}")
    print()
    print("Empty-row filter config (from clpool hardcoded registry):")
    for ev_name, fields in filter_config.items():
        print(f"  {ev_name}: drop if all({', '.join(fields)}) == 0")

    rpc = RPC(RPC_URL, timeout_s=30, max_connections=1)
    try:
        for from_b, to_b, expected in PROBLEM_RANGES:
            n_calls = (to_b - from_b + 1 + STEP - 1) // STEP
            print(f"\nFetching [{from_b:,}..{to_b:,}] ({n_calls} calls)...", end="", flush=True)
            report = await diagnose_range(
                rpc, registry, filter_config,
                from_b, to_b, expected, STEP, topic0s, ADDRESS,
            )
            print(" done")
            print_report(report, filter_config)
    finally:
        await rpc.aclose()

    print(f"\n{'='*65}")
    print("Done.")


if __name__ == "__main__":
    asyncio.run(main())
