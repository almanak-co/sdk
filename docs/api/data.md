# Data Layer

Price oracles, balance providers, OHLCV sources, and the indicator/analytics primitives consumed by `MarketSnapshot`.

## Data Envelope

Every gateway-backed `MarketSnapshot` accessor returns a `DataEnvelope[T]`:
`.value` holds the typed payload and `.meta` carries provenance
(source, staleness, confidence, finality). Unavailability is signalled by a
typed `MarketSnapshotError` subclass — never by missing attributes on the
payload. Fields that could not be measured are `None` **and** named in the
payload's `unmeasured_fields` (Empty != Zero contract); see the
[Market Snapshot HOLD contract](market.md#hold-contract-for-data-unavailable-errors).

### DataEnvelope

::: almanak.framework.data.DataEnvelope
    options:
      show_root_heading: true

### DataMeta

::: almanak.framework.data.DataMeta
    options:
      show_root_heading: true

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

### PoolPrice

The exact-pool price DTO returned by `MarketSnapshot.pool_price(...)` /
`pool_price_by_pair(...)` (as `DataEnvelope[PoolPrice]`).

::: almanak.framework.data.PoolPrice
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

### PoolAnalyticsResult

::: almanak.framework.data.PoolAnalyticsResult
    options:
      show_root_heading: true

### TokenPools

The result of `MarketSnapshot.token_pools(...)` (as `DataEnvelope[TokenPools]`).

::: almanak.framework.data.pools.analytics.TokenPools
    options:
      show_root_heading: true

### PoolAnalyticsReader

VIB-4727: this reader is a thin gRPC client over the gateway's
`PoolAnalyticsService`. It owns no HTTP egress; all upstream provider
calls (DefiLlama / CoinGecko Onchain) happen inside the gateway sidecar.
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

## Pool History

### PoolHistoryReader

VIB-4728: thin gRPC client over the gateway's `PoolHistoryService`. The
framework reader owns NO HTTP / GraphQL egress — all upstream provider
calls (The Graph subgraphs → DefiLlama → CoinGecko Onchain) happen inside
the gateway sidecar; the strategy container holds zero API keys.
Returns a `DataEnvelope[list[PoolSnapshot]]` covering the requested
window, with chain-aware canonical address normalization,
`unmeasured_fields`-tagged Empty != Zero semantics on every snapshot,
and explicit `TruncationReason` enum carrying soft-cap / page-cap /
provider-retention semantics. See the [Market Snapshot HOLD
contract](market.md#hold-contract-for-data-unavailable-errors) for the
``DataSourceUnavailable`` propagation rule strategy authors must follow.

::: almanak.framework.data.pools.history.PoolHistoryReader
    options:
      show_root_heading: true

### PoolSnapshot

The DTO returned per row by `PoolHistoryReader.get_pool_history(...)`.
Money fields (`tvl`, `volume_24h`, `fee_revenue_24h`, `token0_reserve`,
`token1_reserve`) are typed `Decimal | None` per the Empty != Zero
contract — a `None` field is named in `unmeasured_fields`.

::: almanak.framework.data.pools.history.PoolSnapshot
    options:
      show_root_heading: true

### NullPoolHistoryReader

VIB-4728: the backtest-deterministic stub. `MarketSnapshotBuilder.for_pnl_backtest_state`
and `for_paper_fork` inject this reader, which always raises
`DataSourceUnavailable("backtest")` so a strategy run inside a backtest
cannot make a history-driven decision implicitly. Verified — via three
armed monkeypatches on `socket.socket.connect`, `aiohttp.ClientSession`,
and `grpc.aio.{insecure,secure}_channel` — to construct ZERO network
primitives across the four-class enumeration (in-process network,
high-level child-spawn, low-level spawn syscalls, FFI).

::: almanak.framework.data.null_readers.NullPoolHistoryReader
    options:
      show_root_heading: true

### LiquidityDepth

::: almanak.framework.data.LiquidityDepth
    options:
      show_root_heading: true

### SlippageEstimate

The result of `MarketSnapshot.estimate_slippage(...)` (as
`DataEnvelope[SlippageEstimate]`). Slippage and impact are **integer basis
points** (`effective_slippage_bps`, `price_impact_bps`). When no estimate is
possible the accessor raises `SlippageEstimateUnavailableError` — it does not
return a payload with missing attributes.

::: almanak.framework.data.SlippageEstimate
    options:
      show_root_heading: true

## Volatility and Risk

### RealizedVolatilityCalculator

::: almanak.framework.data.RealizedVolatilityCalculator
    options:
      show_root_heading: true

### VolatilityResult

::: almanak.framework.data.VolatilityResult
    options:
      show_root_heading: true

### VolConeResult

::: almanak.framework.data.VolConeResult
    options:
      show_root_heading: true

### PortfolioRiskCalculator

::: almanak.framework.data.PortfolioRiskCalculator
    options:
      show_root_heading: true

### PortfolioRisk

::: almanak.framework.data.PortfolioRisk
    options:
      show_root_heading: true

### RollingSharpeResult

::: almanak.framework.data.RollingSharpeResult
    options:
      show_root_heading: true

## Yield and Rates

### YieldAggregator

::: almanak.framework.data.YieldAggregator
    options:
      show_root_heading: true

### YieldOpportunity

::: almanak.framework.data.YieldOpportunity
    options:
      show_root_heading: true

### Lending rates

Strategies read live lending rates through
[`MarketSnapshot.lending_rate(...)`](market.md) /
`MarketSnapshot.best_lending_rate(...)` — the canonical, gateway-backed
accessors. The underlying `RateMonitor` is a framework-internal gRPC client
of the gateway `RateHistoryService` and is no longer a public strategy API
(deprecated for direct use as of VIB-4859 / VIB-4869).

### LendingRateSnapshot

The per-row DTO returned by `MarketSnapshot.lending_rate_history(...)`
(as `DataEnvelope[list[LendingRateSnapshot]]`).

::: almanak.framework.data.LendingRateSnapshot
    options:
      show_root_heading: true

### GatewayFundingRateProvider

::: almanak.framework.data.GatewayFundingRateProvider
    options:
      show_root_heading: true

### FundingRateSnapshot

The per-row DTO returned by `MarketSnapshot.funding_rate_history(...)`
(as `DataEnvelope[list[FundingRateSnapshot]]`).

::: almanak.framework.data.FundingRateSnapshot
    options:
      show_root_heading: true

### FundingRateSpread

::: almanak.framework.data.FundingRateSpread
    options:
      show_root_heading: true

### LSTExchangeRate

::: almanak.framework.data.LSTExchangeRate
    options:
      show_root_heading: true

## Impermanent Loss

### ILCalculator

::: almanak.framework.data.ILCalculator
    options:
      show_root_heading: true

### ILExposure

::: almanak.framework.data.ILExposure
    options:
      show_root_heading: true

### ProjectedILResult

::: almanak.framework.data.ProjectedILResult
    options:
      show_root_heading: true

## Prediction Markets

DTOs returned by the `MarketSnapshot` prediction-market accessors.

### PredictionMarket

::: almanak.framework.data.PredictionMarket
    options:
      show_root_heading: true

### PredictionPosition

::: almanak.framework.data.PredictionPosition
    options:
      show_root_heading: true

### PredictionOrder

::: almanak.framework.data.PredictionOrder
    options:
      show_root_heading: true

## Health

### HealthReport

The report returned by `MarketSnapshot.health()`.

::: almanak.framework.data.HealthReport
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
