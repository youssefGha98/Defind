from typing import Protocol, Any
import pyarrow as pa
from defind.decoding.decoder import ParsedEvent
from defind.core.models import DecodedRow, DecodedColumns, GaugeColumns, GaugeRow

class ColumnsBuf(Protocol):
    def size(self) -> int: ...
    def take_first(self, n: int): ...
    def to_arrow_table(self) -> pa.Table: ...

class ModelAdapter(Protocol):
    tag: str  # e.g., "clpool", "clgauge", "npm"
    def new_buffer(self) -> ColumnsBuf: ...
    def row_from_parsed(self, pe: ParsedEvent) -> Any | None: ...
    def append_row(self, buf: ColumnsBuf, row: Any) -> None: ...
    def extend(self, dst: ColumnsBuf, src: ColumnsBuf) -> int: ...  # returns appended rows

# -------- CL pool adapter (uses your existing models) --------
class CLPoolAdapter:
    tag = "clpool"
    def new_buffer(self) -> DecodedColumns:
        return DecodedColumns.empty()
    def row_from_parsed(self, pe: ParsedEvent) -> DecodedRow | None:
        v = pe.values
        return DecodedRow(
            block_number=pe.meta.block_number, block_timestamp=pe.meta.block_timestamp or 0,
            tx_hash=pe.meta.tx_hash, log_index=pe.meta.log_index,
            pool=pe.pool, event=pe.name,
            owner=v.get("owner"), sender=v.get("sender"), recipient=v.get("recipient"),
            tick_lower=int(v.get("tick_lower") or 0), tick_upper=int(v.get("tick_upper") or 0),
            liquidity=v.get("liquidity"), amount0=v.get("amount0"), amount1=v.get("amount1"),
        )
    def append_row(self, buf: DecodedColumns, row: DecodedRow) -> None:
        buf.append(row)
    def extend(self, dst: DecodedColumns, src: DecodedColumns) -> int:
        n = src.size()
        # re-use your row-by-row copy like in ShardAggregator.add; simple & safe
        for i in range(n):
            dst.block_number.append(src.block_number[i]); dst.block_timestamp.append(src.block_timestamp[i])
            dst.tx_hash.append(src.tx_hash[i]); dst.log_index.append(src.log_index[i])
            dst.pool.append(src.pool[i]); dst.event.append(src.event[i])
            dst.owner.append(src.owner[i]); dst.sender.append(src.sender[i]); dst.recipient.append(src.recipient[i])
            dst.tick_lower.append(src.tick_lower[i]); dst.tick_upper.append(src.tick_upper[i])
            dst.liquidity.append(src.liquidity[i]); dst.amount0.append(src.amount0[i]); dst.amount1.append(src.amount1[i])
        return n

# -------- CL gauge adapter (new) --------
class CLGaugeAdapter:
    tag = "clgauge"
    def new_buffer(self) -> GaugeColumns:
        return GaugeColumns.empty()

    def row_from_parsed(self, pe: ParsedEvent) -> GaugeRow | None:
        v = pe.values
        amt = v.get("liquidity") or v.get("amount0") or v.get("amount1")
        token_id = v.get("amount0") if pe.name in ("Deposit", "Withdraw") else None  # ← pick it back
        return GaugeRow(
            block_number=pe.meta.block_number, block_timestamp=pe.meta.block_timestamp or 0,
            tx_hash=pe.meta.tx_hash, log_index=pe.meta.log_index,
            gauge=pe.pool, event=pe.name,
            user=v.get("owner") or v.get("sender"),
            recipient=v.get("recipient"),
            token_id=token_id,                     # ← now populated
            amount=amt,
            amount0=v.get("amount0"),
            amount1=v.get("amount1"),
            reward_token=None,
        )

    def append_row(self, buf: GaugeColumns, row: GaugeRow) -> None:
        buf.append(row)

    def extend(self, dst: GaugeColumns, src: GaugeColumns) -> int:
        n = src.size()
        for i in range(n):
            dst.block_number.append(src.block_number[i]); dst.block_timestamp.append(src.block_timestamp[i])
            dst.tx_hash.append(src.tx_hash[i]); dst.log_index.append(src.log_index[i])
            dst.gauge.append(src.gauge[i]); dst.event.append(src.event[i])
            dst.user.append(src.user[i]); dst.recipient.append(src.recipient[i]); dst.token_id.append(src.token_id[i])
            dst.amount.append(src.amount[i]); dst.amount0.append(src.amount0[i]); dst.amount1.append(src.amount1[i]); dst.reward_token.append(src.reward_token[i])
        return n
