# Data Layer

Market data providers, price oracles, balance providers, and the unified `MarketSnapshot` interface.

## MarketSnapshot

The primary data interface passed to `decide()`. Provides lazy access to prices, balances, indicators, and more.

::: almanak.framework.data.MarketSnapshot
    options:
      show_root_heading: true
      members_order: source

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

### FundingRateProvider

::: almanak.framework.data.FundingRateProvider
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
