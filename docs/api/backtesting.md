# Backtesting

Dual-engine backtesting system: PnL simulation with historical prices and paper trading on Anvil forks.

## PnL Backtester

### Strategy clocks and risk baselines

Use the simulated `market.timestamp` supplied to `decide()` for strategy timing.
Creating a new snapshot inside an execution callback can return wall-clock time;
see [Time in Strategies](../getting-started.md#time-in-strategies) for the callback
pattern and the distinction between decision time and fill time. Define and
persist the appropriate [risk baseline lifecycle](../getting-started.md#risk-baseline-lifecycle)
so position reopenings and process restarts do not reset a strategy-level loss
limit.

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

::: almanak.framework.backtesting.PnLBacktester
    options:
      heading_level: 3
      members_order: source

::: almanak.framework.backtesting.PnLBacktestConfig
    options:
      heading_level: 3

## Paper Trader

::: almanak.framework.backtesting.PaperTrader
    options:
      heading_level: 3
      members_order: source

::: almanak.framework.backtesting.PaperTraderConfig
    options:
      heading_level: 3

## Results

Canonical performance metrics (returns, Sharpe, drawdown, PnL, trade statistics) are
computed in the strategy's declared numeraire — `quote_asset` on the `@almanak_strategy`
decorator — and `performance_denomination` in the result summary names the unit. `*_usd`
counterpart fields are kept alongside, and the stored `equity_curve` itself remains
USD-valued (`value_usd`, with `numeraire_price_usd` per point). Check
`performance_denomination` matches the strategy's goal before interpreting returns — a
BTC-growth strategy declared `"USD"` reports USD performance, not BTC. Backtests read the
decorator value; the per-deployment `config.json` override is applied on live runs.

::: almanak.framework.backtesting.BacktestResult
    options:
      heading_level: 3

::: almanak.framework.backtesting.BacktestMetrics
    options:
      heading_level: 3

::: almanak.framework.backtesting.PaperTradingSummary
    options:
      heading_level: 3

## Data Providers

::: almanak.framework.backtesting.HistoricalDataProvider
    options:
      heading_level: 3

::: almanak.framework.backtesting.HistoricalDataConfig
    options:
      heading_level: 3

## Crisis Scenarios

::: almanak.framework.backtesting.CrisisScenario
    options:
      heading_level: 3

## Parallel Execution

::: almanak.framework.backtesting.run_parallel_backtests
    options:
      heading_level: 3
