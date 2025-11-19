# Event-Based Storage Structure

## Overview

DefInd now stores events in **separate parquet files per event type** instead of combining all events in a single file. This improves query performance and simplifies event-specific analysis.

## Directory Structure

```
Data/
├── _manifests/
│   └── run_20241024_162200_{address}_{topics_fp}_{start}_{end}.jsonl
├── Swap/
│   ├── shard_00001.parquet
│   ├── shard_00002.parquet
│   └── ...
├── Mint/
│   ├── shard_00001.parquet
│   └── ...
├── Burn/
│   └── shard_00001.parquet
└── Collect/
    └── shard_00001.parquet
```

### Key Changes from Previous Structure

**Before:**
```
Data/
└── {address}__topics-{fingerprint}__universal/
    ├── shards/
    │   ├── shard_00001.parquet  (all events mixed)
    │   └── shard_00002.parquet
    └── manifests/
        └── run_*.jsonl
```

**After:**
```
Data/
├── _manifests/               (centralized manifests)
│   └── run_*.jsonl
├── {event_name_1}/           (one directory per event type)
│   └── shard_*.parquet
└── {event_name_2}/
    └── shard_*.parquet
```

## Features

### 1. **Event Separation**
- Each event type gets its own directory
- No more `event` column needed for filtering
- Faster queries when analyzing specific events

### 2. **Shard Management**
- Each event maintains its own shard sequence
- Shards are still resumable (partial shard appending)
- Default: 250,000 rows per shard

### 3. **Centralized Manifests**
- Manifests stored in `_manifests/` directory
- Track block ranges processed (not event-specific)
- Enable resumable fetching across reruns

### 4. **Dynamic Columns**
- Each event can have different dynamic columns
- Projection keys become columns automatically
- Stored as strings for Arrow compatibility

## Usage Example

```python
import asyncio
from defind.orchestration.orchestrator import fetch_decode
from defind.decoding.registries import make_clpool_registry

async def main():
    registry = make_clpool_registry()
    
    result = await fetch_decode(
        rpc_url="https://mainnet.optimism.io",
        address="0xYourContractAddress",
        topic0s=list(registry.keys()),
        start_block=120_000_000,
        end_block=120_100_000,
        out_root="./data",           # Creates event dirs here
        registry=registry,
    )
    
    # Result structure:
    # ./data/Swap/shard_00001.parquet
    # ./data/Mint/shard_00001.parquet
    # ./data/_manifests/run_*.jsonl

asyncio.run(main())
```

## Querying Data

### Using DuckDB

```python
import duckdb

# Query specific event type
df = duckdb.query("""
    SELECT * FROM 'data/Swap/*.parquet'
    WHERE block_number BETWEEN 120000000 AND 120010000
""").df()

# Join multiple event types
df = duckdb.query("""
    SELECT * FROM 'data/Swap/*.parquet'
    UNION ALL
    SELECT * FROM 'data/Mint/*.parquet'
""").df()
```

### Using Pandas/PyArrow

```python
import pandas as pd

# Read all shards for one event
df = pd.read_parquet('data/Swap/', engine='pyarrow')

# Read specific columns only
df = pd.read_parquet('data/Swap/', columns=['block_number', 'amount0', 'amount1'])
```

## Implementation Details

### EventShardWriter Class

The new `EventShardWriter` class:
1. Manages multiple `ShardWriter` instances (one per event)
2. Routes incoming rows to correct writer based on `event` field
3. Creates event directories on-demand
4. Handles concurrent writes with proper locking

### Column Routing

When processing logs:
1. Events decoded and added to a `Column` buffer
2. Buffer passed to `EventShardWriter.add()`
3. Rows grouped by `event` field
4. Each group written to its event-specific writer
5. Writers independently manage sharding

### Manifest Tracking

- Manifests remain centralized in `_manifests/`
- Track block ranges and fetch metadata
- Enable resumable operations
- Not tied to specific events (track overall progress)

## Migration Notes

**No automatic migration** from old to new structure. Options:

1. **Start fresh**: Delete old data and re-fetch
2. **Coexist**: Keep old data, fetch new ranges with new structure
3. **Manual migration**: Write a script to split old parquet files by event

## Benefits

✅ **Performance**: Query only relevant event files  
✅ **Clarity**: Obvious which events are available  
✅ **Flexibility**: Different schemas per event  
✅ **Scalability**: Parallelize event-specific operations  
✅ **Simplicity**: No need to filter by event column
