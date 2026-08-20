# Backtesting

Dual-engine backtesting system: PnL simulation with historical prices and paper trading on Anvil forks.

## PnL Backtester

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
