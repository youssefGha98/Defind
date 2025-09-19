# core.models.py
from __future__ import annotations
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Any, Literal
import pyarrow as pa
import json

# === Dynamic universal buffer: any projection key becomes a column ===

_BASE_FIELDS = [
    ("block_number", pa.int64()),
    ("block_timestamp", pa.int64()),
    ("tx_hash", pa.string()),
    ("log_index", pa.int32()),
    ("contract", pa.string()),
    ("event", pa.string()),
]

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


@dataclass
class UniversalDynColumns:
    # base
    block_number: List[int] = field(default_factory=list)
    block_timestamp: List[int] = field(default_factory=list)
    tx_hash: List[str] = field(default_factory=list)
    log_index: List[int] = field(default_factory=list)
    contract: List[str] = field(default_factory=list)
    event: List[str] = field(default_factory=list)

    # dynamic columns: name -> list
    dyn: Dict[str, List[Optional[str]]] = field(default_factory=dict)
    _rows: int = 0

    @staticmethod
    def empty() -> "UniversalDynColumns":
        return UniversalDynColumns()

    def size(self) -> int:
        return self._rows

    def _append_base(self, *, block_number:int, block_timestamp:int, tx_hash:str, log_index:int, contract:str, event:str) -> None:
        self.block_number.append(int(block_number))
        self.block_timestamp.append(int(block_timestamp or 0))
        self.tx_hash.append(str(tx_hash))
        self.log_index.append(int(log_index))
        self.contract.append(str(contract))
        self.event.append(str(event))
        self._rows += 1
        # pad existing dyn columns for the new row
        for col in self.dyn.values():
            col.append(None)

    def _ensure_dyn_col(self, name: str) -> List[Optional[str]]:
        col = self.dyn.get(name)
        if col is None:
            col = [None] * self._rows
            self.dyn[name] = col
        return col

    def append_from_parsed(self, *, pe_name: str, meta, values: Dict[str, Any], contract_addr: str) -> None:
        self._append_base(
            block_number=meta.block_number,
            block_timestamp=meta.block_timestamp or 0,
            tx_hash=meta.tx_hash,
            log_index=meta.log_index,
            contract=contract_addr,
            event=pe_name,
        )
        # every key becomes a column (stored as string for safety)
        for k, v in values.items():
            if v is None:
                sval = None
            else:
                sval = str(v)
            self._ensure_dyn_col(k)[-1] = sval

    def extend(self, other: "UniversalDynColumns") -> int:
        """
        Merge `other` into self. Align dynamic columns by name; pad with None where absent.
        """
        n = other.size()
        if n == 0:
            return 0

        # Snapshot current row count BEFORE we extend base arrays
        old_rows = self._rows

        # 1) extend base arrays
        self.block_number.extend(other.block_number)
        self.block_timestamp.extend(other.block_timestamp)
        self.tx_hash.extend(other.tx_hash)
        self.log_index.extend(other.log_index)
        self.contract.extend(other.contract)
        self.event.extend(other.event)
        self._rows += n  # now = old_rows + n

        # 2) union of dynamic keys
        keys: set[str] = set(self.dyn.keys()) | set(other.dyn.keys())

        # 3) make sure each key exists and is correctly aligned
        for k in keys:
            if k not in self.dyn:
                # NEW column: fill rows that existed BEFORE this extend with None
                self.dyn[k] = [None] * old_rows

            self_col = self.dyn[k]
            ocol = other.dyn.get(k)

            if ocol is None:
                # other has no such column → pad n Nones for the rows we just added
                self_col.extend([None] * n)
            else:
                # append the other column's n values for the rows we just added
                self_col.extend(ocol)

        return n


    def take_first(self, n: int) -> "UniversalDynColumns":
        out = UniversalDynColumns()
        out.block_number, self.block_number = self.block_number[:n], self.block_number[n:]
        out.block_timestamp, self.block_timestamp = self.block_timestamp[:n], self.block_timestamp[n:]
        out.tx_hash, self.tx_hash = self.tx_hash[:n], self.tx_hash[n:]
        out.log_index, self.log_index = self.log_index[:n], self.log_index[n:]
        out.contract, self.contract = self.contract[:n], self.contract[n:]
        out.event, self.event = self.event[:n], self.event[n:]
        for k, col in self.dyn.items():
            out.dyn[k] = col[:n]
            self.dyn[k] = col[n:]
        out._rows = n
        self._rows -= n
        return out

    def to_arrow_table(self) -> pa.Table:
        fields = [pa.field(n, t) for n, t in _BASE_FIELDS]
        arrays = {
            "block_number": pa.array(self.block_number, type=pa.int64()),
            "block_timestamp": pa.array(self.block_timestamp, type=pa.int64()),
            "tx_hash": pa.array(self.tx_hash, type=pa.string()),
            "log_index": pa.array(self.log_index, type=pa.int32()),
            "contract": pa.array(self.contract, type=pa.string()),
            "event": pa.array(self.event, type=pa.string()),
        }
        # deterministic ordering of dynamic cols
        for name in sorted(self.dyn.keys()):
            fields.append(pa.field(name, pa.string()))
            arrays[name] = pa.array(self.dyn[name], type=pa.string())
        schema = pa.schema(fields)
        return pa.Table.from_pydict(arrays, schema=schema).sort_by([
            ("block_number","ascending"), ("tx_hash","ascending"), ("log_index","ascending")
        ])
