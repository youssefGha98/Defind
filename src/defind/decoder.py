from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence
from eth_utils import to_checksum_address

from .constants import MINT_T0, BURN_T0, COLLECT_T0, COLLECTFEES_T0
from .models import DecodedRow

@dataclass(slots=True, frozen=True)
class Meta:
    block_number: int
    block_timestamp: Optional[int]
    tx_hash: str
    log_index: int
    pool: str

def _word(b: bytes, i: int) -> bytes:
    o = i * 32
    return b[o:o+32]

def _addr_from_word(w: bytes) -> str:
    return to_checksum_address("0x" + w[-20:].hex())

def _int24_from_topic_hex(t: str) -> int:
    h = t[2:] if t[:2].lower() == "0x" else t
    b = bytes.fromhex(h[-6:])
    v = int.from_bytes(b, "big")
    return v - (1 << 24) if (v & (1 << 23)) else v

def decode_event(*, topics: Sequence[str], data: bytes, meta: Meta) -> Optional[DecodedRow]:
    if not topics:
        return None

    t0 = topics[0].lower()
    top = [(t[2:] if t[:2].lower() == "0x" else t).lower() for t in topics]

    event: Optional[str] = None
    owner = sender = recipient = None
    tl = tu = None
    liq_s = amt0_s = amt1_s = None

    # MINT: ["address","uint128","uint256","uint256"]
    if t0 == MINT_T0 and len(top) >= 4 and len(data) >= 32 * 4:
        w1 = _word(data, 1)  # liquidity
        w2 = _word(data, 2)  # amount0
        w3 = _word(data, 3)  # amount1
        # fast zero check
        if not (any(w1) or any(w2) or any(w3)):
            return None

        event = "Mint"
        owner = to_checksum_address("0x" + top[1][-40:])
        tl = _int24_from_topic_hex(top[2]); tu = _int24_from_topic_hex(top[3])
        sender = _addr_from_word(_word(data, 0))
        liquidity_i = int.from_bytes(w1, "big")
        amount0_i   = int.from_bytes(w2, "big")
        amount1_i   = int.from_bytes(w3, "big")
        if liquidity_i == 0 and amount0_i == 0 and amount1_i == 0:
            return None
        liq_s, amt0_s, amt1_s = str(liquidity_i), str(amount0_i), str(amount1_i)

    # BURN: ["uint128","uint256","uint256"]
    elif t0 == BURN_T0 and len(top) >= 4 and len(data) >= 32 * 3:
        w0 = _word(data, 0)  # liquidity
        w1 = _word(data, 1)  # amount0
        w2 = _word(data, 2)  # amount1
        if not (any(w0) or any(w1) or any(w2)):
            return None

        event = "Burn"
        owner = to_checksum_address("0x" + top[1][-40:])
        tl = _int24_from_topic_hex(top[2]); tu = _int24_from_topic_hex(top[3])
        liquidity_i = int.from_bytes(w0, "big")
        amount0_i   = int.from_bytes(w1, "big")
        amount1_i   = int.from_bytes(w2, "big")
        if liquidity_i == 0 and amount0_i == 0 and amount1_i == 0:
            return None
        liq_s, amt0_s, amt1_s = str(liquidity_i), str(amount0_i), str(amount1_i)

    # COLLECT: ["address","uint128","uint128"]
    elif t0 == COLLECT_T0 and len(top) >= 4 and len(data) >= 32 * 3:
        w1 = _word(data, 1)  # amount0
        w2 = _word(data, 2)  # amount1
        if not (any(w1) or any(w2)):
            return None

        event = "Collect"
        owner = to_checksum_address("0x" + top[1][-40:])
        tl = _int24_from_topic_hex(top[2]); tu = _int24_from_topic_hex(top[3])
        recipient = _addr_from_word(_word(data, 0))
        amount0_i = int.from_bytes(w1, "big")
        amount1_i = int.from_bytes(w2, "big")
        if amount0_i == 0 and amount1_i == 0:
            return None
        amt0_s, amt1_s = str(amount0_i), str(amount1_i)

    # COLLECT FEES: ["uint128","uint128"]
    elif t0 == COLLECTFEES_T0 and len(top) >= 2 and len(data) >= 32 * 2:
        w0 = _word(data, 0)  # amount0
        w1 = _word(data, 1)  # amount1
        if not (any(w0) or any(w1)):
            return None

        event = "CollectFees"
        recipient = to_checksum_address("0x" + top[1][-40:])
        amount0_i = int.from_bytes(w0, "big")
        amount1_i = int.from_bytes(w1, "big")
        if amount0_i == 0 and amount1_i == 0:
            return None
        amt0_s, amt1_s = str(amount0_i), str(amount1_i)

    # Not one of our events / not enough data
    if event is None:
        return None

    return DecodedRow(
        block_number   = meta.block_number,
        block_timestamp= meta.block_timestamp or 0,
        tx_hash        = meta.tx_hash,
        log_index      = meta.log_index,
        pool           = to_checksum_address(meta.pool),
        event          = event,
        owner          = owner,
        sender         = sender,
        recipient      = recipient,
        tick_lower     = tl or 0,
        tick_upper     = tu or 0,
        liquidity      = liq_s,
        amount0        = amt0_s,
        amount1        = amt1_s,
    )
