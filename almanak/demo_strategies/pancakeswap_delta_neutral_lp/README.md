# pancakeswap_delta_neutral_lp — PancakeSwap-only Delta-Neutral LP (BSC)

Concentrated-liquidity LP on PancakeSwap V3 (WBNB/USDT 0.25% fee tier on BSC)
paired with a short BNB/USD perp on PancakeSwap Perps (ApolloX), sized to
offset the LP's WBNB notional.

This is the single-venue-family BSC counterpart to the Arbitrum
``delta_neutral_lp`` (Uniswap V3 + GMX V2) demo.

## Mechanics

As the pool price moves, the LP's WBNB balance shifts. The strategy
rebalances the perp short to match the LP's current WBNB notional whenever
the price drifts beyond a configurable threshold, so PnL comes from LP
fees rather than BNB price moves.

Phase state machine (one intent per tick):

```
    IDLE           -> LP_OPEN       -> LP_OPENED
    LP_OPENED      -> PERP_OPEN     -> HEDGED
    HEDGED         -> monitor       -> (drift > threshold) UNHEDGING
    UNHEDGING      -> PERP_CLOSE    -> REBALANCING_LP
    REBALANCING_LP -> LP_CLOSE      -> IDLE
```

## Why PancakeSwap-only?

- Same chain (BSC) — no cross-chain bridging.
- Same venue family — single set of contracts, single set of failure modes,
  one team to integrate with.
- The demo also exercises the new ``PerpCloseIntent.position_id`` cross-venue
  vocabulary extension required to close ApolloX positions through the
  IntentCompiler. Without that extension this strategy would have to drop
  out of the all-intent ethos and call ``build_close_transaction`` directly.

## Config (`config.json`)

| Field | Purpose | Default |
|---|---|---|
| `pool` | PancakeSwap V3 pool `TOKEN0/TOKEN1/FEE` | `WBNB/USDT/2500` |
| `range_width_pct` | Total LP range width (0.20 = ±10%) | `0.20` |
| `amount0` / `amount1` | LP token amounts | `0.01` / `5` |
| `perp_market` | ApolloX market | `BNB/USD` |
| `perp_price_symbol` | Token symbol used for gateway price lookup | `WBNB` |
| `perp_collateral_token` | Perp margin token (`BNB` for native) | `BNB` |
| `perp_collateral_amount` | Margin amount in collateral token | `0.05` |
| `perp_size_usd` | Floor for hedge notional (ApolloX has min size) | `30` |
| `perp_leverage` | Perp leverage | `1.5` |
| `delta_rebalance_threshold_pct` | token0 price drift that triggers rebalance | `0.05` |

## v1 Limitations

1. **Static delta estimation.** Hedge size = configured `amount0` × spot
   price. True Uniswap-V3 token0 math (sqrtPriceX96 + tick range) is not
   yet wired in. Within a moderate in-range move the approximation is on
   the same order as the rebalance threshold; large moves will leave the
   hedge a few percent off.
2. **ApolloX async fills.** Open and close are two-phase: the user-signed
   call emits a `MarketPendingTrade` event with a `tradeHash`; an off-chain
   keeper subsequently settles. The strategy persists the `tradeHash`
   from the open receipt and treats successful submission as "open" — a
   richer status-polling loop is a follow-up.
3. **Full-position close only.** ApolloX's `closeTrade(bytes32)` always
   closes the full position. The strategy closes + reopens on rebalance
   rather than resizing.
4. **Hedge floor.** ApolloX enforces a minimum position notional. The
   `perp_size_usd` config field provides a floor so small LP positions
   still get a valid (over-)hedge.

## Usage

```bash
# Anvil fork (BSC) — recommended first run
almanak strat run -d almanak/demo_strategies/pancakeswap_delta_neutral_lp --network anvil --once

# Mainnet (requires real BNB + RPC creds)
almanak strat run -d almanak/demo_strategies/pancakeswap_delta_neutral_lp --once
```

Run 2–3 ticks to walk through `IDLE -> LP_OPENED -> HEDGED`. To observe a
rebalance, lower `delta_rebalance_threshold_pct` and wait for natural drift,
or fast-forward the fork.

## Related demos / dependencies

- `pancakeswap_lp_lifecycle_bsc/` — plain PancakeSwap V3 LP without hedge.
- `pancakeswap_perps_trend/` — perp-only trend strategy (BSC).
- `delta_neutral_lp/` — Arbitrum analogue (Uniswap V3 + GMX V2).
- Vocabulary extension: `PerpCloseIntent.position_id` (commit
  ``feat(intents): extend PerpCloseIntent with position_id…`` on this branch).
