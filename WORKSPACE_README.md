# DefInd + Define Mono Repo

A comprehensive DeFi analytics platform combining event indexing and PnL calculation.

## Projects

### 🔍 DefInd - DeFi Event Indexer
**Location**: `src/defind/`

DefInd is a Python-native DeFi log fetcher that provides a streamlined interface for fetching, decoding, and storing Ethereum event logs with a focus on DeFi protocols.

**Features:**
- Async RPC client for high-performance event fetching
- Dynamic event decoding with configurable projections  
- Universal storage with automatic sharding
- Resumable fetching with live manifest tracking
- Rich CLI with progress tracking

### 💰 Define - DeFi PnL Calculator
**Location**: `src/define/`

Define is a comprehensive PnL calculation engine for DeFi positions, designed to work seamlessly with DefInd's indexed data.

**Features:**
- Multi-protocol PnL calculation (Uniswap V3, lending, staking)
- Comprehensive metrics (realized/unrealized PnL, fees, rewards, IL)
- Flexible price providers (CoinGecko, custom feeds)
- Rich reporting (table, detailed, JSON formats)
- Position tracking and management

## Quick Start

### Installation

```bash
# Install both projects using Rye
rye sync

# Or install individually
pip install -e src/defind/
pip install -e src/define/
```

### Full Workflow

```bash
# 1. Index blockchain events with DefInd
defind fetch-logs --registry nfpm --start-block 18000000

# 2. Build positions from indexed events
define sync-positions

# 3. Calculate PnL for positions  
define calculate-pnl --position-id "nfpm_12345" --position-type liquidity_pool

# 4. View all positions
define list-positions --active-only
```

### Python API

```python
# Index events with DefInd
from defind.orchestration.orchestrator import fetch_decode
from defind.decoding.registries import make_nfpm_registry

# Build positions and calculate PnL with Define
from define.integration.defind_reader import DefIndReader
from define.calculators.liquidity_pool import LiquidityPoolCalculator

# Load DefInd data
reader = DefIndReader("./data")
events = reader.load_events(registry_name="nfpm")
position_tracker = reader.build_positions(events)

# Calculate PnL
calculator = LiquidityPoolCalculator()
for position in position_tracker.get_active_positions():
    transactions = position_tracker.get_transactions(position.position_id)
    result = calculator.calculate(position, transactions)
    print(f"PnL: ${result.breakdown.total_pnl}")
```

## Architecture

```
mono-repo/
├── src/
│   ├── defind/           # Event indexing system
│   │   ├── cli.py        # DefInd CLI
│   │   ├── clients/      # RPC clients
│   │   ├── decoding/     # Event decoding
│   │   ├── storage/      # Data storage
│   │   └── orchestration/# Streaming operations
│   └── define/           # PnL calculation system
│       ├── cli.py        # Define CLI  
│       ├── core/         # Data models
│       ├── calculators/  # PnL engines
│       ├── positions/    # Position tracking
│       ├── pricing/      # Price providers
│       ├── reports/      # Result formatting
│       └── integration/  # DefInd integration
├── examples/             # Usage examples
└── pyproject.toml       # Workspace configuration
```

## Data Flow

1. **DefInd** indexes blockchain events → Parquet storage
2. **Define** reads indexed events → builds positions  
3. **Define** calculates PnL → generates reports

## Supported Protocols

### Event Indexing (DefInd)
- **Uniswap V3**: Pool events, NFPM position events
- **Gauge Factory**: Gauge creation and management
- **VFAT/Sickle**: Protocol-specific events
- **Custom**: Extensible registry system

### PnL Calculation (Define)
- **Liquidity Pools**: Uniswap V3 LP positions with impermanent loss
- **Lending**: Compound, Aave interest tracking
- **Staking**: Validator rewards and penalties
- **Custom**: Pluggable calculator architecture

## CLI Commands

### DefInd Commands

```bash
# Fetch Uniswap V3 position events
defind fetch-logs --registry nfpm --start-block 18000000

# Fetch pool events with custom RPC
defind fetch-logs --registry cl_pools --rpc-url https://your-node.com
```

### Define Commands

```bash
# Calculate PnL for specific position
define calculate-pnl --position-id "nfpm_12345" --position-type liquidity_pool

# List all tracked positions
define list-positions --protocol uniswap_v3 --active-only

# Sync positions from DefInd data
define sync-positions --defind-data-path ./data
```

## Examples

Run the full workflow example:

```bash
python examples/full_workflow.py
```

This demonstrates the complete pipeline from event indexing to PnL calculation.

## Development

### Setup

```bash
# Clone and install
git clone <repo-url>
cd <repo-name>
rye sync

# Install development dependencies
rye sync --all-features
```

### Testing

```bash
# Run all tests
pytest

# Test specific project
pytest src/defind/tests/
pytest src/define/tests/
```

### Code Quality

```bash
# Format code
black .
isort .

# Type checking
mypy src/defind/
mypy src/define/
```

## Performance

- **DefInd**: Async I/O, streaming processing, resumable operations
- **Define**: Efficient position tracking, pluggable price providers
- **Storage**: Columnar Parquet format with compression
- **Memory**: Bounded usage with streaming operations

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Add tests for new functionality
4. Ensure all tests pass (`pytest`)
5. Format code (`black . && isort .`)
6. Submit a pull request

## License

MIT License - see LICENSE file for details.
