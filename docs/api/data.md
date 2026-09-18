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

::: almanak.framework.data.DataEnvelope
    options:
      heading_level: 3

::: almanak.framework.data.DataMeta
    options:
      heading_level: 3

## Price Data

::: almanak.framework.data.PriceOracle
    options:
      heading_level: 3

::: almanak.framework.data.AggregatedPrice
    options:
      heading_level: 3

::: almanak.framework.data.PriceAggregator
    options:
      heading_level: 3

### PoolPrice

The exact-pool price DTO returned by `MarketSnapshot.pool_price(...)` /
`pool_price_by_pair(...)` (as `DataEnvelope[PoolPrice]`).

::: almanak.framework.data.PoolPrice
    options:
      heading_level: 3

## Balance Data

::: almanak.framework.data.BalanceProvider
    options:
      heading_level: 3

## OHLCV Data

::: almanak.framework.data.OHLCVProvider
    options:
      heading_level: 3

::: almanak.framework.data.OHLCVData
    options:
      heading_level: 3

## Pool Analytics

::: almanak.framework.data.PoolAnalytics
    options:
      heading_level: 3

::: almanak.framework.data.PoolAnalyticsResult
    options:
      heading_level: 3

### TokenPools

The result of `MarketSnapshot.token_pools(...)` (as `DataEnvelope[TokenPools]`).

::: almanak.framework.data.pools.analytics.TokenPools
    options:
      heading_level: 3

### PoolAnalyticsReader

VIB-4727: this reader is a thin gRPC client over the gateway's
`PoolAnalyticsService`. It owns no HTTP egress; all upstream provider
calls (DefiLlama / CoinGecko Onchain) happen inside the gateway sidecar.
See the [Market Snapshot HOLD contract](market.md#hold-contract-for-data-unavailable-errors)
for the propagation rule strategy authors must follow.

::: almanak.framework.data.pools.analytics.PoolAnalyticsReader
    options:
      heading_level: 3

### NullPoolAnalyticsReader

Backtest factories (`MarketSnapshotBuilder.for_pnl_backtest_state`,
`for_paper_fork`) inject this stub. It always raises
`DataSourceUnavailable("backtest")`, forcing strategies inside a
backtest to take a deterministic code path (static fee, fixture data,
or HOLD).

::: almanak.framework.data.pools.analytics.NullPoolAnalyticsReader
    options:
      heading_level: 3

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
      heading_level: 3

### PoolSnapshot

The DTO returned per row by `PoolHistoryReader.get_pool_history(...)`.
Money fields (`tvl`, `volume_24h`, `fee_revenue_24h`, `token0_reserve`,
`token1_reserve`) are typed `Decimal | None` per the Empty != Zero
contract — a `None` field is named in `unmeasured_fields`.

::: almanak.framework.data.pools.history.PoolSnapshot
    options:
      heading_level: 3

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
      heading_level: 3

::: almanak.framework.data.LiquidityDepth
    options:
      heading_level: 3

### SlippageEstimate

The result of `MarketSnapshot.estimate_slippage(...)` (as
`DataEnvelope[SlippageEstimate]`). Slippage and impact are **integer basis
points** (`effective_slippage_bps`, `price_impact_bps`). When no estimate is
possible the accessor raises `SlippageEstimateUnavailableError` — it does not
return a payload with missing attributes.

::: almanak.framework.data.SlippageEstimate
    options:
      heading_level: 3

## Volatility and Risk

::: almanak.framework.data.RealizedVolatilityCalculator
    options:
      heading_level: 3

::: almanak.framework.data.VolatilityResult
    options:
      heading_level: 3

::: almanak.framework.data.VolConeResult
    options:
      heading_level: 3

::: almanak.framework.data.PortfolioRiskCalculator
    options:
      heading_level: 3

::: almanak.framework.data.PortfolioRisk
    options:
      heading_level: 3

::: almanak.framework.data.RollingSharpeResult
    options:
      heading_level: 3

## Yield and Rates

::: almanak.framework.data.YieldAggregator
    options:
      heading_level: 3

::: almanak.framework.data.YieldOpportunity
    options:
      heading_level: 3

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
      heading_level: 3

::: almanak.framework.data.GatewayFundingRateProvider
    options:
      heading_level: 3

### FundingRateSnapshot

The per-row DTO returned by `MarketSnapshot.funding_rate_history(...)`
(as `DataEnvelope[list[FundingRateSnapshot]]`).

::: almanak.framework.data.FundingRateSnapshot
    options:
      heading_level: 3

::: almanak.framework.data.FundingRateSpread
    options:
      heading_level: 3

::: almanak.framework.data.LSTExchangeRate
    options:
      heading_level: 3

## Impermanent Loss

::: almanak.framework.data.ILCalculator
    options:
      heading_level: 3

::: almanak.framework.data.ILExposure
    options:
      heading_level: 3

::: almanak.framework.data.ProjectedILResult
    options:
      heading_level: 3

## Prediction Markets

DTOs returned by the `MarketSnapshot` prediction-market accessors.

::: almanak.framework.data.PredictionMarket
    options:
      heading_level: 3

::: almanak.framework.data.PredictionPosition
    options:
      heading_level: 3

::: almanak.framework.data.PredictionOrder
    options:
      heading_level: 3

## Health

### HealthReport

The report returned by `MarketSnapshot.health()`.

::: almanak.framework.data.HealthReport
    options:
      heading_level: 3

## Data Routing

::: almanak.framework.data.DataRouter
    options:
      heading_level: 3

::: almanak.framework.data.CircuitBreaker
    options:
      heading_level: 3

## Exceptions

::: almanak.framework.data.DataUnavailableError
    options:
      heading_level: 3

::: almanak.framework.data.MarketSnapshotError
    options:
      heading_level: 3
