# TraderJoe LP (Demo)

A **recentering** TraderJoe V2 Liquidity Book LP on Avalanche. It opens a
concentrated position around the current price, and when price drifts out of the
band it closes, rebalances inventory to ~50/50, and reopens centered on the new
price — with **hysteresis** so it doesn't thrash at the band edge.

```text
open at price P, range = P ± range_width/2
price drifts within the band      → HOLD (earn fees)
price exits the band but stays within the DEADBAND (rebalance_buffer_pct of the
   range width beyond an edge)     → HOLD (a small overshoot is not worth a churn)
price exits the deadband AND the position has lived ≥ rebalance_cooldown
                                   → CLOSE → rebalance to 50/50 → REOPEN
```

Without the deadband + cooldown a recentering LP thrashes (close→reopen→close)
when price oscillates around the edge, bleeding gas and realizing IL on every
crossing.

## Chain

avalanche

## Configuration (`config.json`)

| Key | Meaning |
|---|---|
| `pool` | `TOKEN_X/TOKEN_Y/BIN_STEP` (e.g. `WAVAX/USDC/20`). |
| `range_width_pct` | Total LP range width (0.10 = ±5% around price). |
| `amount_x` / `amount_y` / `num_bins` | Initial inventory + bin count. |
| `min_position_usd` | Minimum total inventory (USD) to (re)open. |
| `rebalance_buffer_pct` | Deadband beyond the range before recentering, as a fraction of range width (0.5 = rebalance only once price is half-a-range past an edge). |
| `rebalance_cooldown_minutes` | A freshly-opened position must live this long before it can be closed to rebalance. |

## Quick Start

```bash
almanak strat demo --name traderjoe_lp
cd traderjoe_lp
uv run almanak strat run --network anvil --interval 15
```
