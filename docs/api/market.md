# Market Snapshot

::: almanak.framework.market

## Overview

`almanak.framework.market` is the canonical home for `MarketSnapshot` — the
strategy-facing market-data interface. It replaces the two legacy locations
(`almanak.framework.strategies.intent_strategy.MarketSnapshot` and
`almanak.framework.data.market_snapshot.MarketSnapshot`) that silently
diverged before VIB-4062.

## Builder factories

::: almanak.framework.market.builders.MarketSnapshotBuilder

## Typed errors

::: almanak.framework.market.errors

### HOLD contract for data-unavailable errors

Some `MarketSnapshot` accessors call out to off-chain services through
the gateway (e.g. `pool_analytics(...)`, which routes to
`PoolAnalyticsService` over gRPC). When the gateway is unreachable, the
strategy container has no fallback, so the accessor raises a typed
error such as `PoolAnalyticsUnavailableError` whose `__cause__` chain
preserves the underlying `DataSourceUnavailable`.

The runner's `classify_failure` walks `__cause__` to depth 8 and treats
`DataSourceUnavailable` as `DATA_UNAVAILABLE`, which the iteration
loop interprets as HOLD-worthy. **Strategy authors must either let
these errors propagate, or catch them and explicitly return
`Intent.hold(...)`.** A bare `except` (swallowing the typed error
without re-raising or returning HOLD) breaks the runner's HOLD
inference and the strategy will appear to "succeed with no signal"
while losing the safety contract.

```python
def decide(self, market: MarketSnapshot) -> Intent:
    try:
        analytics = market.pool_analytics(pool_address, protocol="uniswap_v3")
    except PoolAnalyticsUnavailableError:
        # Correct: surface as HOLD so the runner's data-unavailable path fires.
        return Intent.hold(reason="pool analytics unavailable")
    # ... use analytics
```

The same HOLD contract applies to `market.pool_history(...)` — backed by
`PoolHistoryReader` over the gateway-side `PoolHistoryService` (VIB-4728).
In a backtest the injected `NullPoolHistoryReader` raises
`DataSourceUnavailable("backtest")` on every call; in a live run a gateway
outage or "pool not found across all providers" surfaces as the same
typed error. The same catch-and-HOLD or let-it-propagate discipline
applies:

```python
def decide(self, market: MarketSnapshot) -> Intent:
    try:
        history = market.pool_history(
            pool_address,
            chain=self.chain,
            start_date=self.lookback_start,
            resolution="1h",
            protocol="uniswap_v3",
        )
    except DataSourceUnavailable:
        # Correct: HOLD on backtest or gateway-down; do NOT default to zeros.
        return Intent.hold(reason="pool history unavailable")
    # ... use history.value (list[PoolSnapshot])
```

## Return-type DTOs

::: almanak.framework.market.models

## Provider Protocols (sync adapters)

::: almanak.framework.market.services
