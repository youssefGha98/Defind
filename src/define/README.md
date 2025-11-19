# Define - DeFi PnL Calculator

A comprehensive PnL calculation engine for DeFi positions, designed to work seamlessly with DefInd's event indexing system.

## Features

- **Multi-Protocol Support**: Calculate PnL for Uniswap V3, Sushiswap, and other DeFi protocols
- **Position Types**: Support for liquidity pools, lending, staking, and farming positions
- **Comprehensive Metrics**: Realized/unrealized PnL, fees, rewards, gas costs, and impermanent loss
- **Flexible Pricing**: Pluggable price providers (CoinGecko, custom feeds)
- **Rich Reporting**: Table, detailed, and JSON output formats

## Architecture

```
define/
├── core/           # Core data models (Position, Transaction, PnLResult)
├── calculators/    # PnL calculation engines by position type
├── positions/      # Position tracking and building from events
├── pricing/        # Price feed providers
├── reports/        # Result formatting and export
└── cli.py         # Command-line interface
```

## Quick Start

### Installation

```bash
# Install in development mode
pip install -e .

# Or install from workspace root
rye sync
```

### Basic Usage

```bash
# Calculate PnL for a position
define calculate-pnl --position-id "nfpm_12345" --position-type liquidity_pool

# List all tracked positions
define list-positions --active-only

# Sync positions from defind data
define sync-positions --defind-data-path ./data/indexed_events.parquet
```

### Python API

```python
from define.core.position import Position, PositionType
from define.calculators.liquidity_pool import LiquidityPoolCalculator
from define.pricing.coingecko import CoinGeckoPriceProvider

# Create position
position = Position(
    position_id="my_position",
    position_type=PositionType.LIQUIDITY_POOL,
    protocol="uniswap_v3"
)

# Calculate PnL
price_provider = CoinGeckoPriceProvider()
calculator = LiquidityPoolCalculator(price_provider)
result = calculator.calculate(position, transactions)

print(f"Total PnL: ${result.breakdown.total_pnl}")
```

## Integration with DefInd

Define is designed to consume data from DefInd's indexing system:

1. **DefInd** indexes blockchain events (Mint, Burn, Collect, etc.)
2. **Define** builds positions from these events
3. **Define** calculates PnL using position data and price feeds

```bash
# Typical workflow
defind fetch-logs --registry cl_pools --start-block 18000000
define sync-positions  # Reads defind's output
define calculate-pnl --position-id "pool_0x123_0xabc"
```

## Position Types

### Liquidity Pool Positions
- Uniswap V3 LP positions
- Impermanent loss calculation
- Fee collection tracking

### Lending Positions
- Compound, Aave lending
- Interest earned tracking
- Collateral ratio monitoring

### Staking Positions
- Token staking rewards
- Validator rewards
- Slashing penalties

## Price Providers

- **CoinGecko**: Historical and current prices
- **Custom**: Implement `BasePriceProvider` for custom feeds

## Output Formats

- **Table**: Quick overview of multiple positions
- **Detailed**: Comprehensive breakdown for single position
- **JSON**: Machine-readable format for integrations
- **CSV**: Export for spreadsheet analysis

## Development

```bash
# Install development dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Format code
black .
isort .

# Type checking
mypy .
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## License

MIT License - see LICENSE file for details.
