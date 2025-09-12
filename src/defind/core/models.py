from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from typing import Optional, List, Literal

import pyarrow as pa

Status = Literal["started", "done", "failed"]

@dataclass(slots=True, frozen=True)
class EventLog:
    address: str                       # lowercased hex with 0x
    topics: tuple[str, ...]            # all topics, lowercased with 0x
    data_hex: str                      # hex with 0x (or "0x")
    block_number: int
    tx_hash: str                       # lowercased 0x...
    log_index: int
    block_timestamp: Optional[int] = None

@dataclass(slots=True)
class DecodedRow:
    block_number: int
    block_timestamp: int
    tx_hash: str
    log_index: int
    pool: str
    event: str                         # Mint | Burn | Collect | CollectFees
    owner: Optional[str]
    sender: Optional[str]
    recipient: Optional[str]
    tick_lower: int
    tick_upper: int
    liquidity: Optional[str]
    amount0: Optional[str]
    amount1: Optional[str]

@dataclass(slots=True)
class ChunkRecord:
    from_block: int
    to_block: int
    status: Status
    attempts: int
    error: Optional[str]
    logs: int              # raw logs fetched
    decoded: int           # kept rows
    shards: int            # shard files written due to this chunk
    updated_at: float
    filtered: int = 0      # rows skipped by fast filter

    def to_json_line(self) -> str:
        return json.dumps(asdict(self), separators=(",", ":")) + "\n"

# Arrow schema for decoded rows
LP_SCHEMA = pa.schema([
    pa.field("block_number",    pa.int64()),
    pa.field("block_timestamp", pa.int64()),
    pa.field("tx_hash",         pa.large_string()),
    pa.field("log_index",       pa.int32()),
    pa.field("pool",            pa.large_string()),
    pa.field("event",           pa.large_string()),
    pa.field("owner",           pa.large_string()),
    pa.field("sender",          pa.large_string()),
    pa.field("recipient",       pa.large_string()),
    pa.field("tick_lower",      pa.int32()),
    pa.field("tick_upper",      pa.int32()),
    pa.field("liquidity",       pa.large_string()),
    pa.field("amount0",         pa.large_string()),
    pa.field("amount1",         pa.large_string()),
])
DECODED_COLS: tuple[str, ...] = tuple(f.name for f in LP_SCHEMA)

@dataclass(slots=True)
class DecodedColumns:
    block_number: List[int]
    block_timestamp: List[int]
    tx_hash: List[str]
    log_index: List[int]
    pool: List[str]
    event: List[str]
    owner: List[Optional[str]]
    sender: List[Optional[str]]
    recipient: List[Optional[str]]
    tick_lower: List[int]
    tick_upper: List[int]
    liquidity: List[Optional[str]]
    amount0: List[Optional[str]]
    amount1: List[Optional[str]]

    @classmethod
    def empty(cls) -> "DecodedColumns":
        return cls(
            block_number=[], block_timestamp=[], tx_hash=[], log_index=[],
            pool=[], event=[], owner=[], sender=[], recipient=[],
            tick_lower=[], tick_upper=[], liquidity=[], amount0=[], amount1=[]
        )

    def append(self, row: DecodedRow) -> None:
        self.block_number.append(row.block_number)
        self.block_timestamp.append(row.block_timestamp)
        self.tx_hash.append(row.tx_hash)
        self.log_index.append(row.log_index)
        self.pool.append(row.pool)
        self.event.append(row.event)
        self.owner.append(row.owner)
        self.sender.append(row.sender)
        self.recipient.append(row.recipient)
        self.tick_lower.append(row.tick_lower)
        self.tick_upper.append(row.tick_upper)
        self.liquidity.append(row.liquidity)
        self.amount0.append(row.amount0)
        self.amount1.append(row.amount1)

    def size(self) -> int:
        return len(self.tx_hash)

    def take_first(self, n: int) -> "DecodedColumns":
        out = DecodedColumns(
            block_number=self.block_number[:n],
            block_timestamp=self.block_timestamp[:n],
            tx_hash=self.tx_hash[:n],
            log_index=self.log_index[:n],
            pool=self.pool[:n],
            event=self.event[:n],
            owner=self.owner[:n],
            sender=self.sender[:n],
            recipient=self.recipient[:n],
            tick_lower=self.tick_lower[:n],
            tick_upper=self.tick_upper[:n],
            liquidity=self.liquidity[:n],
            amount0=self.amount0[:n],
            amount1=self.amount1[:n],
        )
        # shrink current
        self.block_number = self.block_number[n:]
        self.block_timestamp = self.block_timestamp[n:]
        self.tx_hash = self.tx_hash[n:]
        self.log_index = self.log_index[n:]
        self.pool = self.pool[n:]
        self.event = self.event[n:]
        self.owner = self.owner[n:]
        self.sender = self.sender[n:]
        self.recipient = self.recipient[n:]
        self.tick_lower = self.tick_lower[n:]
        self.tick_upper = self.tick_upper[n:]
        self.liquidity = self.liquidity[n:]
        self.amount0 = self.amount0[n:]
        self.amount1 = self.amount1[n:]
        return out

    def to_arrow_table(self) -> pa.Table:
        arrays = {
            "block_number":    pa.array(self.block_number,    type=LP_SCHEMA.field("block_number").type),
            "block_timestamp": pa.array(self.block_timestamp, type=LP_SCHEMA.field("block_timestamp").type),
            "tx_hash":         pa.array(self.tx_hash,         type=LP_SCHEMA.field("tx_hash").type),
            "log_index":       pa.array(self.log_index,       type=LP_SCHEMA.field("log_index").type),
            "pool":            pa.array(self.pool,            type=LP_SCHEMA.field("pool").type),
            "event":           pa.array(self.event,           type=LP_SCHEMA.field("event").type),
            "owner":           pa.array(self.owner,           type=LP_SCHEMA.field("owner").type),
            "sender":          pa.array(self.sender,          type=LP_SCHEMA.field("sender").type),
            "recipient":       pa.array(self.recipient,       type=LP_SCHEMA.field("recipient").type),
            "tick_lower":      pa.array(self.tick_lower,      type=LP_SCHEMA.field("tick_lower").type),
            "tick_upper":      pa.array(self.tick_upper,      type=LP_SCHEMA.field("tick_upper").type),
            "liquidity":       pa.array(self.liquidity,       type=LP_SCHEMA.field("liquidity").type),
            "amount0":         pa.array(self.amount0,         type=LP_SCHEMA.field("amount0").type),
            "amount1":         pa.array(self.amount1,         type=LP_SCHEMA.field("amount1").type),
        }
        return pa.Table.from_pydict(arrays, schema=LP_SCHEMA).sort_by([
            ("block_number", "ascending"),
            ("tx_hash", "ascending"),
            ("log_index", "ascending"),
        ])


GAUGE_SCHEMA = pa.schema([
    pa.field("block_number",    pa.int64()),
    pa.field("block_timestamp", pa.int64()),
    pa.field("tx_hash",         pa.large_string()),
    pa.field("log_index",       pa.int32()),
    pa.field("gauge",           pa.large_string()),
    pa.field("event",           pa.large_string()),     # Deposit | Withdraw | Claim | NotifyReward | Transfer ...
    pa.field("user",            pa.large_string()),     # staker / account / from
    pa.field("recipient",       pa.large_string()),     # to (where relevant)
    pa.field("token_id",        pa.large_string()),     # for NFT-style gauges; keep as string to be safe
    pa.field("amount",          pa.large_string()),     # main numeric
    pa.field("amount0",         pa.large_string()),     # optional
    pa.field("amount1",         pa.large_string()),     # optional
    pa.field("reward_token",    pa.large_string()),     # optional (for NotifyReward etc.)
])

@dataclass(slots=True)
class GaugeRow:
    block_number: int
    block_timestamp: int
    tx_hash: str
    log_index: int
    gauge: str
    event: str
    user: Optional[str] = None
    recipient: Optional[str] = None
    token_id: Optional[str] = None
    amount: Optional[str] = None
    amount0: Optional[str] = None
    amount1: Optional[str] = None
    reward_token: Optional[str] = None

@dataclass(slots=True)
class GaugeColumns:
    block_number: List[int]
    block_timestamp: List[int]
    tx_hash: List[str]
    log_index: List[int]
    gauge: List[str]
    event: List[str]
    user: List[Optional[str]]
    recipient: List[Optional[str]]
    token_id: List[Optional[str]]
    amount: List[Optional[str]]
    amount0: List[Optional[str]]
    amount1: List[Optional[str]]
    reward_token: List[Optional[str]]

    @classmethod
    def empty(cls) -> "GaugeColumns":
        return cls([],[],[],[],[],[],[],[],[],[],[],[],[])

    def append(self, r: GaugeRow) -> None:
        self.block_number.append(r.block_number)
        self.block_timestamp.append(r.block_timestamp)
        self.tx_hash.append(r.tx_hash)
        self.log_index.append(r.log_index)
        self.gauge.append(r.gauge)
        self.event.append(r.event)
        self.user.append(r.user)
        self.recipient.append(r.recipient)
        self.token_id.append(r.token_id)
        self.amount.append(r.amount)
        self.amount0.append(r.amount0)
        self.amount1.append(r.amount1)
        self.reward_token.append(r.reward_token)

    def size(self) -> int: return len(self.tx_hash)

    def take_first(self, n: int) -> "GaugeColumns":
        out = GaugeColumns(
            self.block_number[:n], self.block_timestamp[:n], self.tx_hash[:n], self.log_index[:n],
            self.gauge[:n], self.event[:n], self.user[:n], self.recipient[:n], self.token_id[:n],
            self.amount[:n], self.amount0[:n], self.amount1[:n], self.reward_token[:n]
        )
        self.block_number = self.block_number[n:]; self.block_timestamp = self.block_timestamp[n:]
        self.tx_hash = self.tx_hash[n:]; self.log_index = self.log_index[n:]
        self.gauge = self.gauge[n:]; self.event = self.event[n:]
        self.user = self.user[n:]; self.recipient = self.recipient[n:]; self.token_id = self.token_id[n:]
        self.amount = self.amount[n:]; self.amount0 = self.amount0[n:]; self.amount1 = self.amount1[n:]; self.reward_token = self.reward_token[n:]
        return out

    def to_arrow_table(self) -> pa.Table:
        import pyarrow as pa
        fields = {f.name: GAUGE_SCHEMA.field(f.name).type for f in GAUGE_SCHEMA}
        return pa.Table.from_pydict({
            "block_number":    pa.array(self.block_number,    type=fields["block_number"]),
            "block_timestamp": pa.array(self.block_timestamp, type=fields["block_timestamp"]),
            "tx_hash":         pa.array(self.tx_hash,         type=fields["tx_hash"]),
            "log_index":       pa.array(self.log_index,       type=fields["log_index"]),
            "gauge":           pa.array(self.gauge,           type=fields["gauge"]),
            "event":           pa.array(self.event,           type=fields["event"]),
            "user":            pa.array(self.user,            type=fields["user"]),
            "recipient":       pa.array(self.recipient,       type=fields["recipient"]),
            "token_id":        pa.array(self.token_id,        type=fields["token_id"]),
            "amount":          pa.array(self.amount,          type=fields["amount"]),
            "amount0":         pa.array(self.amount0,         type=fields["amount0"]),
            "amount1":         pa.array(self.amount1,         type=fields["amount1"]),
            "reward_token":    pa.array(self.reward_token,    type=fields["reward_token"]),
        }).sort_by([("block_number","ascending"),("tx_hash","ascending"),("log_index","ascending")])