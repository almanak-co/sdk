# Migrating from Legacy BacktestEngine

This guide helps you migrate from the legacy `BacktestEngine` (block-based backtesting) to the new backtesting engines.

## Overview

The legacy `BacktestEngine` is deprecated. New projects should use:

| Engine | Best For | Requirements |
|--------|----------|--------------|
| **PnLBacktester** | Historical backtests with price data | No Anvil required |
| **PaperTrader** | Live-like simulation with real execution | Anvil fork |

## Quick Migration

### From Block Backtest to PnL Backtest

**Before (deprecated):**
```bash
almanak backtest block -s my_strategy --days 7 --chain arbitrum
```

**After (recommended):**
```bash
almanak backtest pnl -s my_strategy --start 2024-01-01 --end 2024-01-08
```

### From Block Backtest to Paper Trading

**Before (deprecated):**
```bash
almanak backtest block -s my_strategy --days 7 --chain arbitrum
```

**After (recommended):**
```bash
almanak backtest paper start -s my_strategy --chain arbitrum --duration 7d
```

## Programmatic Migration

### Legacy Code

```python
from almanak.framework.backtesting import BacktestEngine, BacktestConfig

engine = BacktestEngine()
result = engine.backtest(
    strategy=my_strategy,
    config=BacktestConfig(
        start_block=17000000,
        end_block=17100000,
        chain="ethereum",
    ),
)
```

### New PnL Backtester

```python
from almanak.framework.backtesting import PnLBacktester, PnLBacktestConfig
from datetime import datetime

backtester = PnLBacktester()
result = backtester.run(
    strategy=my_strategy,
    config=PnLBacktestConfig(
        start_date=datetime(2024, 1, 1),
        end_date=datetime(2024, 6, 1),
        chain="ethereum",
        initial_capital=10000,
    ),
)
```

### New Paper Trader

```python
from almanak.framework.backtesting import PaperTrader, PaperTraderConfig

trader = PaperTrader(
    config=PaperTraderConfig(
        chain="arbitrum",
        initial_eth=10,
        price_source="auto",  # Chainlink -> TWAP -> CoinGecko fallback
    ),
)
result = await trader.run(strategy=my_strategy, duration_seconds=3600)
```

## Feature Comparison

| Feature | BacktestEngine (deprecated) | PnLBacktester | PaperTrader |
|---------|----------------------------|---------------|-------------|
| Historical data | Block-based | Date range | Real-time |
| Price source | On-chain | CoinGecko/historical | Configurable fallback |
| Anvil required | Yes | No | Yes |
| LP fee tracking | Limited | Full | Full |
| Slippage modeling | Basic | Configurable | Real execution |
| Execution | Simulated | Simulated | Real on-fork |

## Key Differences

### PnL Backtester Advantages
- No Anvil or archive RPC required
- Uses historical price data (faster)
- Full LP fee and IL tracking
- Parameter sweep support
- Date-based ranges (more intuitive)

### Paper Trader Advantages
- Real transaction execution on Anvil fork
- Actual on-chain state
- Real slippage and gas costs
- More accurate for complex DeFi strategies

## Configuration Mapping

| BacktestConfig | PnLBacktestConfig | PaperTraderConfig |
|----------------|-------------------|-------------------|
| `start_block` | `start_date` | `fork_block` |
| `end_block` | `end_date` | `duration` |
| `chain` | `chain` | `chain` |
| `initial_balance` | `initial_capital` | `initial_eth` |
| `block_step` | `interval` | N/A |

## Need Help?

- See the backtest CLI help: `almanak backtest --help`
- Check blueprints: `blueprints/16-cli-reference.md`
- View examples in `strategies/demo/`
