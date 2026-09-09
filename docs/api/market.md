# Market Snapshot

::: almanak.framework.market

## Overview

`almanak.framework.market` is the canonical home for `MarketSnapshot` — the
strategy-facing market-data interface. It replaces the two legacy locations
(`almanak.framework.strategies.intent_strategy.MarketSnapshot` and
`almanak.framework.data.market_snapshot.MarketSnapshot`) that silently
diverged before VIB-4062.

## The snapshot clock

`MarketSnapshot.timestamp` is the strategy's **only** legitimate clock. It is
a tz-aware `datetime`: real time on the live surface, **simulated time** in
backtests. Any time arithmetic that feeds a decision — cooldowns, cadences,
daily counters — must be measured against it; `datetime.now()` inside
`decide()` or `on_intent_executed()` mixes clocks and silently breaks every
backtest (a 24h wall-clock cooldown never expires when weeks of simulated
time replay in minutes).

```python
def decide(self, market: MarketSnapshot) -> Intent:
    now = market.timestamp
    if self._last_trade_ts and now - self._last_trade_ts < self._cooldown:
        return Intent.hold(reason="cooldown")
    ...
```

`on_intent_executed()` has no snapshot parameter — capture `market.timestamp`
in `decide()` and reuse the captured value when stamping fill state there.
See [Time in Strategies](../getting-started.md#time-in-strategies) for the
full pattern.

## Non-crypto reference prices

Use an explicit underlying instrument, separately from the execution token.
The example below enforces a strategy that requires a 120-second observation;
the BSC push feeds cannot guarantee that contract, so this example will often
HOLD even during an open session. It is a rejection example, not a provider
configuration that unblocks such a strategy:

```python
reference = market.reference_price("GOOGL", chain="bsc", quote="USD")
reason = reference.trade_block_reason(max_age_seconds=120, now=market.timestamp)
if reason is not None:
    return Intent.hold(reason=f"reference guard: {reason}")
```

BSC reference support uses these Chainlink consumer proxies:

| Instrument | Proxy | Provider heartbeat | Session calendar |
| --- | --- | --- | --- |
| `XAU` | `0x86896fEB19D8A607c3b11f2aF50A0f239Bd71CD0` | 600 seconds | CME Globex Gold |
| `GOOGL` | `0xeDA73F8acb669274B15A977Cb0cdA57a84F18c2a` | 86400 seconds | NYSE regular session |
| `TSLA` | `0xEEA2ae9c074E87596A85ABE698B2Afebc9B57893` | 86400 seconds | NYSE regular session |

The equity feeds use 8 decimals and a 0.5% deviation trigger. Their provider
metadata specifies the NYSE session convention, including holidays, early closes,
and daylight-saving changes. See the provider's [GOOGL feed](https://data.chain.link/feeds/bsc/mainnet/googl-usd)
and [TSLA feed](https://data.chain.link/feeds/bsc/mainnet/tsla-usd).

**A daily heartbeat does not guarantee a 120- or 300-second observation.** `stale`
reflects the provider heartbeat; `trade_block_reason(max_age_seconds=...)` also
enforces the strategy's stricter age limit. Preserve that limit: old observations,
closed or unknown sessions, and unavailable data must block new trades. Confidence
is the source's policy score (0.95 within heartbeat, 0.85 when stale), not a measured
statistical confidence interval. `observed_at` is the Chainlink round's `updatedAt`,
not an exchange tick timestamp or the time the gateway fetched it.

`GOOGL` refers to the catalogued Alphabet reference instrument. `GOOGLB`, `GOOGLX`,
`GOOGLON`, `GOOG`, and token contract addresses are not aliases. A strategy must
establish its exact token's underlying through authoritative issuer metadata and
apply any issuer-specific adjustment before comparing it with a raw token price.
An underlying share reference alone does not price a wrapper token. This does not change the strategy's chain,
pool, tokens, or fee tier. Reference prices never enter generic token-price
aggregation or substitute for a pool execution quote.

Historical backtests still have no historical reference-price plane. A managed
fork reads its forked feed state; neither a successful reference read nor a safe
HOLD proves the swap or teardown path was exercised.

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
