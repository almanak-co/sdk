# Multi-LP Dual Range Demo

Reference template for **multi-position dispatch** on the same pool. Opens two
Uniswap V3 concentrated-liquidity positions — a narrow (±5%) and wide (±20%)
range — and holds them until operator-initiated teardown.

This demo exists to make the correct multi-position pattern discoverable at
template-discovery time. The full design contract is in
`docs/internal/blueprints/04-strategy-layer.md` §Multi-position dispatch.

## What this demo is for

A reference for any strategy that needs more than one open position sharing
a wallet basis-pool, pool/market state, or position-registry semantic group —
e.g. two LP legs on the same pool, two `SUPPLY`s on the same Aave market, or
a hedged perp + LP pair on the same venue.

## The pattern

- **Phase machine** drives `decide()` — one Intent per iteration, never a
  list return.

  ```text
  INIT ──LP_OPEN(narrow)──▶ LP1_OPEN ──LP_OPEN(wide)──▶ BOTH_OPEN ──teardown──▶ DONE
  ```

- **Stable per-position `registry_handle`** (`leg_narrow`, `leg_wide`) — NOT
  action-scoped (`leg_narrow:open`). The same handle survives the open →
  close → rebalance lifecycle.

- **Self-sized amounts** from live `market.balance(...)` at the moment each
  leg is built — narrow leg takes `lp_capital_split_pct` of available
  balance, wide leg takes `0.99` of what remains (the 1% safety margin
  absorbs gas / dust / slippage drift between balance read and tx
  submission).

- **Partial-success guard.** `on_intent_executed` only advances the phase
  when the receipt carries a real `position_id`. A mint that lands on-chain
  without an id holds the phase put so the next iteration retries — no
  stranded slots.

## Why not return `[narrow_open, wide_open]` as a list?

The framework will accept it, but you lose:

1. Durable state checkpoint between legs.
2. Live-balance re-sizing for leg #2 against leg #1's actual on-chain output.
3. Clean partial-success state — "leg 1 succeeded, leg 2 reverted" is just
   "phase = LP1_OPEN, retry LP2 next iteration."

`Intent.sequence([open_a, open_b])` is also NOT a substitute — it serialises
dispatch order within one iteration but still commits both legs to the same
market snapshot.

See `docs/internal/blueprints/04-strategy-layer.md` §Multi-position dispatch and
`docs/internal/dual-intent-blueprint.md` for the full rationale.

## Running this demo

```bash
# Continuous run on managed Anvil with dashboard (the production-shape path):
uv run almanak strat run \
    -d almanak/demo_strategies/multi_lp_dual_range \
    --network anvil \
    --dashboard \
    --id dual-lp-demo-1

# In another terminal, signal teardown when ready:
uv run almanak strat teardown request \
    -d almanak/demo_strategies/multi_lp_dual_range \
    -s "MultiLPDualRangeStrategy:dual-lp-demo-1" \
    --wait --force
```

## Sibling reference

`strategies/accounting/lp_dual/` is a more elaborate accounting-test fixture
that exercises the same dispatch pattern with a swap-in / swap-back loop and
audit-test scaffolding. `strategies/accounting/lp_triple/` extends to three
LPs with an out-of-order middle close. This demo strips that down to just
the dispatch shape.
