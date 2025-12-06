# DefInd 🔍

**DefInd** is a high-performance, Python-native DeFi log fetcher designed for efficient extraction and processing of Ethereum blockchain event data. Built specifically for DeFi protocols, it provides resumable, concurrent data fetching with dynamic column projections and universal storage format.

## ✨ Features

- **🚀 High Performance**: Async/concurrent fetching with configurable parallelism
- **🔄 Resumable Operations**: Live manifest system for fault-tolerant data collection
- **📊 Dynamic Projections**: Any registry key automatically becomes a column
- **💾 Universal Storage**: Efficient Parquet-based sharding with bounded memory
- **🎯 DeFi-Optimized**: Pre-built support for CL pools, Gauge, and VFAT events
- **⚡ Smart Filtering**: Fast zero-word filtering and automatic interval splitting
- **🛡️ Robust Error Handling**: Automatic retries and graceful failure recovery

## 🏗️ Architecture

```
defind/
├── core/           # Data models and constants
├── clients/        # RPC client for Ethereum nodes
├── decoding/       # Event decoding with dynamic projections
├── storage/        # Universal shards and live manifests
└── orchestration/  # Streaming fetch-decode-write pipeline
```

### Core Components

- **EventLog**: Raw RPC log records
- **UniversalDynColumns**: Dynamic columnar buffer for any projection
- **EventRegistry**: Configurable event decoding specifications
- **ShardAggregator**: Bounded-memory Parquet writer
- **LiveManifest**: Resumable operation tracking

## 🚀 Quick Start

### Installation

```bash
# Clone the repository
git clone <repository-url>
cd define

# Install dependencies
pip install -e .
```

### Basic Usage

```python
from defind.orchestration.orchestrator import fetch_decode
from defind.decoding.registries import make_clpool_registry

# Setup
registry = make_clpool_registry()

# Fetch and decode data
result = await fetch_decode(
    rpc_url="https://your-ethereum-rpc-url",
    address="0x...",  # Contract address
    topic0s=["0x..."],  # Event signatures
    start_block=18000000,
    end_block=18001000,
    registry=registry,
    out_root="./data",
    # ... other parameters
)
```

## 📋 Event Registries

DefInd comes with pre-built registries for common DeFi protocols:

### Concentrated Liquidity Pools
```python
from defind.decoding.registries import make_clpool_registry
registry = make_clpool_registry()
# Supports: Mint, Burn, Collect, CollectFees events
```

### Gauge Events
```python
from defind.decoding.registries import make_gauge_registry
registry = make_gauge_registry()
# Supports: Deposit, Withdraw, ClaimRewards events
```

### VFAT Events
```python
from defind.decoding.registries import make_vfat_registry
registry = make_vfat_registry()
# Supports: Deploy and other VFAT-specific events
```

### Custom Registries
```python
from defind.decoding.specs import EventSpec, TopicFieldSpec, DataFieldSpec

# Define custom event specification
custom_spec = EventSpec(
    topic0="0x...",
    name="CustomEvent",
    topic_fields=[
        TopicFieldSpec("user", 1, "address"),
        TopicFieldSpec("amount", 2, "uint256"),
    ],
    data_fields=[
        DataFieldSpec("timestamp", 0, "uint256"),
    ],
    projection={
        "user_address": "topic.user",
        "token_amount": "topic.amount", 
        "event_time": "data.timestamp",
    }
)
```

## 🔧 Configuration

### Key Parameters

- **`rows_per_shard`**: Number of rows per Parquet file (default: 250,000)
- **`batch_decode_rows`**: Batch size for decoding (default: 50,000)
- **`concurrency`**: Max parallel RPC requests (default: 16)

### Performance Tuning

```python
# High-throughput configuration
result = await fetch_decode(
    # ... other params
    rows_per_shard=500_000,     # Larger shards
    batch_decode_rows=100_000,  # Larger batches
    concurrency=32,             # More parallelism
)
```

## 📊 Output Format

DefInd produces Parquet files with a universal schema:

### Base Columns (always present)
- `block_number`: Block number (int64)
- `block_timestamp`: Block timestamp (int64) 
- `tx_hash`: Transaction hash (string)
- `log_index`: Log index within transaction (int32)
- `contract`: Contract address (string)
- `event`: Event name (string)

### Dynamic Columns
Any key in your event registry's `projection` mapping becomes a column automatically, stored as strings for safety with large integers.
