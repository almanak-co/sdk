# RSI Martingale Short

Short tokens that are overbought (RSI > 75) after a significant rally, using
martingale-style position sizing on GMX V2 perpetual futures.

## Thesis

A retracement is near-certain after a parabolic move. The question is *when*
and *how deep*. Start with a small short position, and double down up to 5
times if the pump continues. Target a 30% retracement from the local top.

The martingale sizing ensures you survive being early: most of your capital
deploys at higher prices (better short entries), and the retracement pays
off the entire pyramid.

## How It Works

1. **Scan**: Watch RSI and 24h price change. Enter when RSI > 75 and rally
   exceeds threshold.
2. **Initial short**: Deploy ~1.6% of risk budget as collateral (with 5 max
   doublings and $100 budget, initial = ~$1.59).
3. **Double down**: If price rises another 10% from last entry, deploy 2x
   the previous collateral as a new short.
4. **Take profit**: When price drops 30% from the local top, close everything.
5. **Hard stop**: If all doublings are exhausted and price rises 15% more,
   cut losses.
6. **Time stop**: Close after 168 hours (1 week) regardless.
7. **Cooldown**: Wait 60 minutes before re-entering.

## Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `market` | `ETH/USD` | GMX market to trade |
| `collateral_token` | `USDC` | Collateral token (stablecoin for shorts) |
| `risk_budget_usd` | `100` | Total collateral across all levels |
| `leverage` | `2.0` | Leverage per position |
| `rsi_threshold` | `75` | RSI level to trigger entry |
| `rsi_period` | `14` | RSI lookback period |
| `rally_threshold_24h_pct` | `5` | Min 24h price change for entry |
| `max_doublings` | `5` | Maximum martingale doublings |
| `doubling_trigger_pct` | `10` | Price rise % to trigger next doubling |
| `retracement_target_pct` | `30` | Retracement % for take profit |
| `hard_stop_above_last_entry_pct` | `15` | Hard stop after all doublings |
| `time_stop_hours` | `168` | Max hours to hold |
| `max_slippage_pct` | `1.0` | Max slippage for GMX orders |
| `cooldown_minutes` | `60` | Cooldown between trades |

## Running

```bash
# Single iteration
almanak strat run -d strategies/incubating/rsi_martingale_short --once

# On Anvil fork
almanak strat run -d strategies/incubating/rsi_martingale_short --network anvil --once

# Force open a position (testing)
# Set "force_action": "open" in config.json
```

## Key Risks

- Market can stay irrational longer than you can stay solvent (hence small sizing)
- Funding rates on perps erode profits on long-duration holds
- Flash crashes may cause poor fills on take-profit
- Tokens can rally 200%+ before retracing (need enough doublings)
