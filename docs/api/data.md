# Data Layer

Price oracles, balance providers, OHLCV sources, and the indicator/analytics primitives consumed by `MarketSnapshot`.

## Price Data

### PriceOracle

::: almanak.framework.data.PriceOracle
    options:
      show_root_heading: true

### AggregatedPrice

::: almanak.framework.data.AggregatedPrice
    options:
      show_root_heading: true

### PriceAggregator

::: almanak.framework.data.PriceAggregator
    options:
      show_root_heading: true

## Balance Data

### BalanceProvider

::: almanak.framework.data.BalanceProvider
    options:
      show_root_heading: true

## OHLCV Data

### OHLCVProvider

::: almanak.framework.data.OHLCVProvider
    options:
      show_root_heading: true

### OHLCVData

::: almanak.framework.data.OHLCVData
    options:
      show_root_heading: true

## Pool Analytics

### PoolAnalytics

::: almanak.framework.data.PoolAnalytics
    options:
      show_root_heading: true

### PoolAnalyticsReader

VIB-4727: this reader is a thin gRPC client over the gateway's
`PoolAnalyticsService`. It owns no HTTP egress; all upstream provider
calls (DefiLlama / GeckoTerminal) happen inside the gateway sidecar.
See the [Market Snapshot HOLD contract](market.md#hold-contract-for-data-unavailable-errors)
for the propagation rule strategy authors must follow.

::: almanak.framework.data.pools.analytics.PoolAnalyticsReader
    options:
      show_root_heading: true

### NullPoolAnalyticsReader

Backtest factories (`MarketSnapshotBuilder.for_pnl_backtest_state`,
`for_paper_fork`) inject this stub. It always raises
`DataSourceUnavailable("backtest")`, forcing strategies inside a
backtest to take a deterministic code path (static fee, fixture data,
or HOLD).

::: almanak.framework.data.pools.analytics.NullPoolAnalyticsReader
    options:
      show_root_heading: true

### LiquidityDepth

::: almanak.framework.data.LiquidityDepth
    options:
      show_root_heading: true

## Volatility and Risk

### RealizedVolatilityCalculator

::: almanak.framework.data.RealizedVolatilityCalculator
    options:
      show_root_heading: true

### PortfolioRiskCalculator

::: almanak.framework.data.PortfolioRiskCalculator
    options:
      show_root_heading: true

## Yield and Rates

### YieldAggregator

::: almanak.framework.data.YieldAggregator
    options:
      show_root_heading: true

### RateMonitor

::: almanak.framework.data.RateMonitor
    options:
      show_root_heading: true

### GatewayFundingRateProvider

::: almanak.framework.data.GatewayFundingRateProvider
    options:
      show_root_heading: true

## Impermanent Loss

### ILCalculator

::: almanak.framework.data.ILCalculator
    options:
      show_root_heading: true

## Data Routing

### DataRouter

::: almanak.framework.data.DataRouter
    options:
      show_root_heading: true

### CircuitBreaker

::: almanak.framework.data.CircuitBreaker
    options:
      show_root_heading: true

## Exceptions

::: almanak.framework.data.DataUnavailableError
    options:
      show_root_heading: true

::: almanak.framework.data.MarketSnapshotError
    options:
      show_root_heading: true
