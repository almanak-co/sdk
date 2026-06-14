# Almanak Backtesting Engine

A dual-engine backtesting system for institutional-grade strategy validation.

## Overview

The Almanak backtesting system provides two complementary engines:

| Engine | Best For | Requirements | Speed |
|--------|----------|--------------|-------|
| **PnL Backtester** | Historical analysis with price data | No Anvil required | Fast |
| **Paper Trader** | Live-like simulation with real execution | Anvil fork | Realistic |

## Quick Start

> **🚀 New to backtesting?** Start with our [complete working examples](../../examples/README.md) - they're copy-paste ready and include everything you need, including data providers, strategies, and visualization.

### PnL Backtesting (Recommended for Historical Analysis)

Here's a complete working example:

```python
import asyncio
from datetime import datetime, UTC
from decimal import Decimal
from almanak.framework.backtesting import PnLBacktester, PnLBacktestConfig
from almanak.framework.backtesting.pnl.engine import DefaultFeeModel, DefaultSlippageModel

# For a complete example with data provider and strategy, see:
# examples/backtest_ta_strategy.py

async def main():
    # 1. Create data provider (see examples/common/data_providers.py)
    # data_provider = RSITriggerDataProvider(start_time=..., end_time=...)
    
    # 2. Create fee and slippage models
    fee_models = {"default": DefaultFeeModel()}
    slippage_models = {"default": DefaultSlippageModel()}
    
    # 3. Create backtester
    # backtester = PnLBacktester(
    #     data_provider=data_provider,
    #     fee_models=fee_models,
    #     slippage_models=slippage_models,
    # )
    
    # 4. Configure backtest
    config = PnLBacktestConfig(
        start_time=datetime(2024, 1, 1, tzinfo=UTC),
        end_time=datetime(2024, 6, 1, tzinfo=UTC),
        initial_capital_usd=Decimal("10000"),
        interval_seconds=3600,  # Hourly ticks
    )
    
    # 5. Create strategy (see examples/ for implementations)
    # strategy = MyStrategy()
    
    # 6. Run backtest
    # result = await backtester.backtest(strategy, config)
    
    # 7. Review results
    # print(f"Total Return: {result.metrics.total_return_pct:.2f}%")
    # print(f"Sharpe Ratio: {result.metrics.sharpe_ratio:.2f}")
    # print(f"Max Drawdown: {result.metrics.max_drawdown_pct:.2f}%")

if __name__ == "__main__":
    asyncio.run(main())
```

**For a complete runnable example**, see [`examples/backtest_ta_strategy.py`](../../examples/backtest_ta_strategy.py) which includes:
- Data provider setup
- Strategy implementation
- Full backtest execution
- Visualization generation

### Paper Trading (Recommended for Live-like Simulation)

Paper trading requires Anvil fork setup. For a complete example, see the [Paper Trader documentation](../paper/README.md) or use the CLI:

```bash
# Paper trading via CLI (handles fork setup automatically)
almanak strat backtest paper start -s my_strategy --chain arbitrum
```

For programmatic usage:

```python
from almanak.framework.backtesting import PaperTrader, PaperTraderConfig
from almanak.framework.backtesting.paper import RollingForkManager, PaperPortfolioTracker

# Configure paper trading
config = PaperTraderConfig(
    chain="arbitrum",
    rpc_url="https://arb1.arbitrum.io/rpc",
    deployment_id="my_strategy",
    tick_interval_seconds=60,
)

# Create fork manager and portfolio tracker
# fork_manager = RollingForkManager(...)
# portfolio_tracker = PaperPortfolioTracker(...)

# Run paper trading
# trader = PaperTrader(fork_manager, portfolio_tracker, config)
# result = await trader.run(strategy, duration_seconds=3600)
# print(f"Net PnL: ${result.metrics.net_pnl_usd}")
```

> **Note**: Paper trading setup is more complex. Consider using the CLI command above or see the [Paper Trader documentation](../paper/README.md) for complete setup instructions.

## CLI Commands

```bash
# PnL backtest (historical simulation)
almanak strat backtest pnl -s my_strategy --start 2024-01-01 --end 2024-06-01

# Parameter sweep optimization
almanak strat backtest sweep -s my_strategy \
  --param "rsi_period:10,14,20" \
  --param "rsi_threshold:25,30,35"

# Paper trading (live-like simulation)
almanak strat backtest paper start -s my_strategy --chain arbitrum

# Monte Carlo simulation
almanak strat backtest monte-carlo -s my_strategy --runs 1000

# Walk-forward optimization
almanak strat backtest walk-forward -s my_strategy --windows 5

# Crisis scenario testing
almanak strat backtest scenario -s my_strategy --scenario terra_collapse

# Interactive dashboard
almanak strat backtest dashboard
```

## Engines Comparison

### PnL Backtester

**Best for:** Historical backtests, parameter optimization, rapid iteration

- Uses historical price data (CoinGecko, Chainlink)
- No blockchain node required
- Fast execution (~1000x real-time)
- Full LP fee and IL tracking
- Configurable slippage models

### Paper Trader

**Best for:** Pre-production validation, realistic execution testing

- Real transaction execution on Anvil forks
- Actual on-chain state
- Real slippage and gas costs
- MEV simulation
- True DeFi complexity

## Strategy Types

| Strategy Type | PnL Support | Paper Support | Key Metrics |
|---------------|-------------|---------------|-------------|
| **TA/Spot** | Full | Full | Sharpe, Win Rate, Profit Factor |
| **LP** | Full | Full | Fees, IL, Time in Range, APY |
| **Lending** | Full | Full | Health Factor, Net Yield, APY |
| **Perps** | Full | Full | Funding, Liquidations, Leverage |
| **Arbitrage** | Partial | Full | Latency Sensitivity |

## Institutional Mode

For production-grade backtests with strict data quality:

```python
config = PnLBacktestConfig(
    start_time=datetime(2024, 1, 1, tzinfo=UTC),
    end_time=datetime(2024, 6, 1, tzinfo=UTC),
    initial_capital_usd=Decimal("1000000"),
    institutional_mode=True,  # Enables strict requirements
    random_seed=42,           # Required for reproducibility
)
```

**Institutional mode enforces:**

| Setting | Value | Purpose |
|---------|-------|---------|
| `strict_reproducibility` | `True` | Audit trails |
| `allow_degraded_data` | `False` | Data quality |
| `allow_hardcoded_fallback` | `False` | Accurate valuations |
| `require_symbol_mapping` | `True` | Token identification |
| `min_data_coverage` | `>= 98%` | Data completeness |

## Configuration Reference

### Core Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `start_time` | `datetime` | required | Backtest start |
| `end_time` | `datetime` | required | Backtest end |
| `interval_seconds` | `int` | `3600` | Tick interval |
| `initial_capital_usd` | `Decimal` | `10000` | Starting capital |
| `chain` | `str` | `"arbitrum"` | Target chain |
| `tokens` | `list[str]` | `[]` | Tokens to track |

### Gas and Execution

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `include_gas_costs` | `bool` | `True` | Include gas in PnL |
| `gas_price_gwei` | `Decimal \| None` | `None` (chain-aware) | Gas price; unset resolves from the chain registry (e.g. 0.1 on Arbitrum, 22 on Ethereum) |
| `inclusion_delay_blocks` | `int` | `1` | Execution delay |

### Data Quality

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `staleness_threshold_seconds` | `int` | `3600` | Stale data threshold |
| `min_data_coverage` | `Decimal` | `0.98` | Minimum coverage |
| `random_seed` | `int` | `None` | Reproducibility seed |

## Metrics Calculated

### Standard Metrics (All Strategies)

- **Total Return %**: Net profit/loss as percentage
- **Sharpe Ratio**: Risk-adjusted returns (annualized)
- **Sortino Ratio**: Downside risk-adjusted returns
- **Max Drawdown %**: Worst peak-to-trough decline
- **Win Rate %**: Percentage of profitable trades
- **Profit Factor**: Gross profit / Gross loss

### LP-Specific Metrics

- **Total Fees Earned**: Fee income in USD
- **Impermanent Loss**: IL in USD
- **Net LP PnL**: Fees - IL
- **Time in Range %**: Percentage of time price in LP range
- **Fee APY**: Annualized fee return
- **IL-to-Fees Ratio**: Risk vs reward

### LP Fee-Accrual Model and Heuristic Assumptions

LP fee accrual (`LPBacktestAdapter`) estimates fees per tick as:

```text
fees = pool_volume_usd * fee_tier * liquidity_share * days_elapsed
liquidity_share = clamp(position_value_usd / pool_liquidity, 0.10, 1.0)
```

Both sides of the share are USD-denominated: `position_value_usd` is the
position's current USD value (not its V3 liquidity `L`), and `pool_liquidity`
is `explicit_pool_liquidity_usd` when set, else the `base_liquidity`
placeholder.

`pool_volume_usd` is resolved from the highest-trust source available, in order:

| Source | How to enable | Confidence |
|--------|---------------|------------|
| **Explicit** | `LPBacktestConfig.explicit_pool_volume_usd_daily` (+ `explicit_pool_liquidity_usd` for the share denominator) | HIGH |
| **Historical** | `use_historical_volume=True` with a pool **address** on the position; volume is fetched through the gateway DEX-volume lane (`GetDexVolumeHistory`) | per-source |
| **Heuristic fallback** | `allow_volume_fallback=True` (opt-in) | LOW |

> **VIB-4849 — no silent fabrication.** When none of the above is available, the
> adapter raises `DataSourceUnavailableError` instead of fabricating
> `pool_volume = position_value * volume_multiplier`. A wrong fee number is worse
> than a clear error. The error message states exactly what to provide.

**The heuristic fallback** (`allow_volume_fallback=True`) is a deliberately rough,
order-of-magnitude-uncertain estimate intended for quick parameter sweeps, **not**
for PnL claims. Its assumptions:

- Daily volume ≈ `position_value_usd * volume_multiplier` (default `10x`). Real
  pools vary by orders of magnitude, so this can be wildly wrong.
- A pool-TVL placeholder of `base_liquidity = 1,000,000` is used for liquidity
  share unless `explicit_pool_liquidity_usd` is set.
- A `liquidity_share` floor of 10% is applied to avoid degenerate zero-fee
  positions for tiny stakes.
- The reported fee blends the volume-based estimate with a fee-tier→APR proxy
  (stable 10% / blue-chip 20% / volatile 25% / exotic 10%), and is stamped
  `fee_confidence="low"`.

**Validating the heuristic.** `LPBacktestAdapter.validate_heuristics(samples)`
compares the heuristic output against caller-supplied ground-truth samples
(`HeuristicValidationSample`) and logs a WARNING for any sample whose relative
error exceeds 50% (configurable). It performs **no** network egress — the caller
supplies observed fees (e.g. derived from on-chain `Swap` events via the gateway,
or from a prior historical-volume run) — and returns a
`HeuristicValidationResult` per sample.

### Lending-Specific Metrics

- **Health Factor**: Collateral safety margin
- **Net Yield**: Supply interest - Borrow interest
- **Yield APY**: Annualized yield
- **Liquidation Events**: Count of liquidations
- **Leverage Ratio**: Total exposure / Equity

## Examples

> **📚 Complete Working Examples**: See [`examples/README.md`](../../examples/README.md) for detailed documentation on all example scripts.

Complete working examples are available in [`examples/`](../../examples/):

```bash
# RSI mean reversion strategy
python examples/backtest_ta_strategy.py

# Concentrated LP strategy
python examples/backtest_lp_strategy.py

# Leveraged looping strategy
python examples/backtest_looping_strategy.py
```

Each example includes:
- ✅ Complete, copy-paste ready code
- ✅ Synthetic data providers for reproducibility (no API keys needed)
- ✅ 3-panel visualization charts saved to `examples/output/`
- ✅ Verification formulas for manual auditing
- ✅ Comprehensive documentation in [`examples/README.md`](../../examples/README.md)

**Quick Start with Examples:**

1. **Install dependencies** (if not already installed):
   ```bash
   make install-dev
   uv add matplotlib  # For chart generation
   ```

2. **Run an example**:
   ```bash
   python examples/backtest_ta_strategy.py
   ```

3. **View results**: Charts are saved to `examples/output/`

See [`examples/README.md`](../../examples/README.md) for detailed explanations of each strategy, configuration options, and how to interpret results.

## Crisis Scenario Testing

Test strategies against historical market crises:

```python
from almanak.framework.backtesting import (
    BLACK_THURSDAY,
    TERRA_COLLAPSE,
    FTX_COLLAPSE,
    get_scenario_by_name,
)

# Use predefined scenario
result = await backtester.backtest(
    strategy,
    config,
    scenario=TERRA_COLLAPSE,
)

print(f"Strategy survived crash: {result.metrics.total_return_pct > -50}")
```

## Visualization

Generate publication-quality charts:

```python
from almanak.framework.backtesting import (
    plot_equity_curve,
    generate_equity_chart_html,
    generate_pnl_distribution_html,
    generate_drawdown_chart_html,
)

# Static chart
plot_equity_curve(result.equity_curve, output_path="equity.png")

# Interactive HTML charts
generate_equity_chart_html(result, output_path="equity.html")
generate_pnl_distribution_html(result, output_path="pnl_dist.html")
generate_drawdown_chart_html(result, output_path="drawdown.html")
```

## Report Generation

Generate comprehensive HTML reports:

```python
from almanak.framework.backtesting import generate_report

report = generate_report(
    result=result,
    strategy_name="My Strategy",
    include_trades=True,
    include_charts=True,
)

report.save("backtest_report.html")
```

## Parallel Execution

Run multiple backtests in parallel:

```python
from almanak.framework.backtesting import (
    BacktestTask,
    run_parallel_backtests,
)

tasks = [
    BacktestTask(strategy=strategy1, config=config1),
    BacktestTask(strategy=strategy2, config=config2),
    BacktestTask(strategy=strategy3, config=config3),
]

results = await run_parallel_backtests(tasks, max_workers=4)
```

## Parameter Optimization

### Grid Search (Sweep)

```bash
almanak strat backtest sweep -s my_strategy \
  --param "window:10,20,30" \
  --param "threshold:0.5,1.0,1.5" \
  --metric sharpe
```

### Bayesian Optimization

```bash
almanak strat backtest optimize -s my_strategy \
  --param "window:10:50" \
  --param "threshold:0.1:2.0" \
  --trials 100 \
  --metric sharpe
```

### Walk-Forward Optimization

```bash
almanak strat backtest walk-forward -s my_strategy \
  --windows 5 \
  --train-pct 0.7 \
  --metric sharpe
```

## Accuracy Expectations

Expected accuracy by strategy type and data quality:

| Strategy | FULL Data | PRE_CACHE | CURRENT_ONLY |
|----------|-----------|-----------|--------------|
| LP | 90-95% | 85-93% | 50-70% |
| Perp | 92-97% | 88-95% | 60-75% |
| Lending | 97-99% | 95-98% | 80-90% |
| Arbitrage | 70-85% | 60-80% | 20-40% |
| Spot | 93-97% | 90-95% | 65-80% |

See `docs/ACCURACY_LIMITATIONS.md` for detailed accuracy documentation.

## Migration from Legacy Engine

The block-based `BacktestEngine` is deprecated. See `MIGRATION.md` for migration guide.

**Quick migration:**

```bash
# OLD (deprecated)
almanak strat backtest block -s my_strategy --days 7

# NEW (recommended)
almanak strat backtest pnl -s my_strategy --start 2024-01-01 --end 2024-01-08
# OR
almanak strat backtest paper start -s my_strategy --chain arbitrum
```

## Architecture

```
almanak/framework/backtesting/
├── __init__.py           # Public API exports
├── README.md             # This file
├── MIGRATION.md          # Migration guide
├── engine.py             # Legacy BacktestEngine (deprecated)
├── models.py             # Shared data models
├── report_generator.py   # HTML report generation
├── visualization.py      # Chart generation
├── adapters/             # Strategy type adapters
│   ├── lp_adapter.py
│   ├── lending_adapter.py
│   ├── perp_adapter.py
│   └── arbitrage_adapter.py
├── paper/                # Paper trading engine
│   ├── engine.py         # PaperTrader
│   ├── fork_manager.py   # Anvil fork management
│   └── portfolio_tracker.py
├── pnl/                  # PnL backtesting engine
│   ├── engine.py         # PnLBacktester
│   ├── config.py         # PnLBacktestConfig
│   ├── portfolio.py      # Portfolio simulation
│   ├── calculators/      # Metric calculators
│   ├── fee_models/       # Fee modeling
│   └── providers/        # Data providers
├── scenarios/            # Crisis scenarios
│   ├── black_thursday.py
│   ├── terra_collapse.py
│   └── ftx_collapse.py
└── templates/            # Report templates
```

## Related Documentation

- [CLI documentation](https://sdk.docs.almanak.co/cli/) - CLI command reference
- `examples/README.md` - Working examples documentation
- `docs/ACCURACY_LIMITATIONS.md` - Accuracy documentation
- `MIGRATION.md` - Legacy engine migration guide
