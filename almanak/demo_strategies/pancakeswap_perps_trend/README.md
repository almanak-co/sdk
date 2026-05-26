# PancakeSwap Perps Trend Follower (Demo)

Trend-following strategy on BTC/USD perpetuals at PancakeSwap Perps
(ApolloX Diamond on BSC, PCS broker id = 2).

## Strategy
Simple momentum: on each tick, compute the pct change in BTC mark vs the previous
tick. If change > +threshold bps, open a LONG. If change < -threshold bps while
holding a LONG, signal a close.

## Scope (v1)
- BSC only
- Market orders only, no SL/TP
- BTC/USD market, native BNB margin
- Long-only for simplicity (momentum doesn't flip short)

## Known limitations (v1, tracked in design doc)
1. PCS Perps is **oracle-priced** — every open/close is a two-phase flow
   (user TX → keeper fill). The demo's `on_intent_executed` callback persists
   the tradeHash returned in `result.position_id`, but filled-entry price is
   only available after the keeper settles (separate TX).
2. `PerpCloseIntent` carries an optional `position_id` (bytes32 tradeHash).
   On a bearish reversal the demo emits a real `Intent.perp_close(position_id=...)`
   that the compiler routes through ApolloX's `closeTrade(bytes32)` — no direct
   SDK call required. Partial closes (`size_usd` set) are rejected by the
   compiler since ApolloX always flattens 100% of the position; the demo
   therefore omits `size_usd` on its close intent. The direct-SDK helper
   `almanak.connectors.pancakeswap_perps.build_close_transaction(trade_hash)`
   is still available as an escape hatch.

## Files
- `strategy.py` — `PancakeSwapPerpsTrendStrategy` (IntentStrategy subclass)
- `config.json` — tunable params (collateral, size, threshold bps)

## Running
```bash
# Live BSC (requires BSC_RPC_URL + ALMANAK_PRIVATE_KEY)
almanak strat run -d almanak/demo_strategies/pancakeswap_perps_trend --once

# Local Anvil fork
almanak strat run -d almanak/demo_strategies/pancakeswap_perps_trend --network anvil --once
```
