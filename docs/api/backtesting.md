# Backtesting

Dual-engine backtesting system: PnL simulation with historical prices and paper trading on Anvil forks.

## PnL Backtester

### Historical guards and explicit research variants

PnL backtests cannot currently serve historical tick-level
`market.liquidity_depth()` data. Daily pool TVL is not a replacement for depth.
A strategy that requires this guard must keep refusing the unsupported run, or
explicitly implement a different backtest branch. The CLI never skips a guard
automatically.

Declare the exact guard contract on the strategy, using its configured pool and
protocol (the symbols below stand for those strategy settings):

```python
from almanak.framework.backtesting.pnl import HistoricalDataDependency

def backtest_data_dependencies(self, config):
    return (HistoricalDataDependency(
        dependency_id="entry_depth",
        lane="liquidity_depth",
        chain=config.chain,
        pool_address=self.pool_address,
        protocol=self.protocol,
        start_time=config.start_time,
        end_time=config.end_time,
        required_fidelity="initialized_ticks",
    ),)
```

Only the run window is currently validated; this does not certify extended
warmup coverage. Readiness records these explicit requirements with declared
provenance.

For a strategy that declares an `entry_depth` dependency and explicitly branches
on `market.backtest_guard_enabled("entry_depth")`, add this entry to its strategy
config JSON alongside its normal settings and `token_funding`:

```json
{
  "altered_backtest_guards": {
    "entry_depth": "Evaluate the signal without the live tick-depth guard"
  }
}
```

Run that configuration through the existing CLI option:

```bash
uv run almanak backtest pnl --strategy my_strategy --start 2026-08-01 --end 2026-08-07 --config-file variant.json
```

Every changed guard must name a declared dependency and include a nonempty
reason. The CLI shows the changes; the result stores their reasons and
`strategy_comparison="altered_guards_not_live_equivalent"`, with a distinct config
hash. This evaluates a changed strategy and does not demonstrate that the live
guarded strategy works. Unsupported data reads still refuse.

`--from-result result.json` preserves the recorded guard choices. An explicitly
supplied `--config-file` with conflicting guard choices is rejected; start a new
backtest to evaluate a different variant. `--allow-missing-prices` only relaxes
token-price checks and cannot bypass required historical pool analytics.

### PnLBacktester

::: almanak.framework.backtesting.PnLBacktester
    options:
      show_root_heading: true
      members_order: source

### PnLBacktestConfig

::: almanak.framework.backtesting.PnLBacktestConfig
    options:
      show_root_heading: true

## Paper Trader

### PaperTrader

::: almanak.framework.backtesting.PaperTrader
    options:
      show_root_heading: true
      members_order: source

### PaperTraderConfig

::: almanak.framework.backtesting.PaperTraderConfig
    options:
      show_root_heading: true

## Results

Canonical performance metrics (returns, Sharpe, drawdown, PnL, trade statistics) are
computed in the strategy's declared numeraire — `quote_asset` on the `@almanak_strategy`
decorator — and `performance_denomination` in the result summary names the unit. `*_usd`
counterpart fields are kept alongside, and the stored `equity_curve` itself remains
USD-valued (`value_usd`, with `numeraire_price_usd` per point). Check
`performance_denomination` matches the strategy's goal before interpreting returns — a
BTC-growth strategy declared `"USD"` reports USD performance, not BTC. Backtests read the
decorator value; the per-deployment `config.json` override is applied on live runs.

### BacktestResult

::: almanak.framework.backtesting.BacktestResult
    options:
      show_root_heading: true

### BacktestMetrics

::: almanak.framework.backtesting.BacktestMetrics
    options:
      show_root_heading: true

### PaperTradingSummary

::: almanak.framework.backtesting.PaperTradingSummary
    options:
      show_root_heading: true

## Data Providers

### HistoricalDataProvider

::: almanak.framework.backtesting.HistoricalDataProvider
    options:
      show_root_heading: true

### HistoricalDataConfig

::: almanak.framework.backtesting.HistoricalDataConfig
    options:
      show_root_heading: true

## Crisis Scenarios

### CrisisScenario

::: almanak.framework.backtesting.CrisisScenario
    options:
      show_root_heading: true

## Parallel Execution

::: almanak.framework.backtesting.run_parallel_backtests
    options:
      show_root_heading: true
