# Buy The Dip Strategy

RSI-based accumulation strategy that buys a target token on oversold signals and takes partial profit on overbought signals, guaranteeing net accumulation over time.

## How It Works

1. Monitors the RSI of the base token (e.g., WETH)
2. When RSI **crosses into** oversold territory (< 30): buys `buy_percentage` of the quote token balance
3. When RSI **crosses into** overbought territory (> 70): sells `sell_percentage` of the base token balance
4. Because `sell_percentage < buy_percentage`, the strategy accumulates the base token over time
5. If the quote balance is below `dust_threshold_usd` when an oversold signal triggers, the strategy terminates

### Signal Change Detection

The strategy only trades on RSI **zone transitions**, not on RSI simply being in a zone. If RSI drops to 25 and stays below 30 for hours, only the first crossing triggers a buy. This prevents spam trading during extended oversold/overbought periods.

### Cooldown

A configurable cooldown (`cooldown_minutes`) prevents rapid-fire trades even when RSI oscillates around a threshold.

## Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `base_token` | `WETH` | Token to accumulate |
| `quote_token` | `USDC` | Stable coin used for buying |
| `rsi_period` | `14` | RSI calculation period |
| `rsi_oversold` | `30` | RSI threshold for buy signals |
| `rsi_overbought` | `70` | RSI threshold for sell signals |
| `buy_percentage` | `0.20` | % of quote balance to spend per buy |
| `sell_percentage` | `0.15` | % of base balance to sell per sell (must be < buy_percentage) |
| `cooldown_minutes` | `60` | Minimum minutes between trades |
| `dust_threshold_usd` | `0.50` | Minimum quote balance to keep trading |
| `max_slippage_bps` | `100` | Maximum slippage in basis points |

## Usage

```bash
# Single iteration (dry run)
almanak strat run -d strategies/incubating/buy_the_dip --once --dry-run

# Continuous on Anvil fork
almanak strat run -d strategies/incubating/buy_the_dip --network anvil --interval 60

# Fresh start (clears previous state)
almanak strat run -d strategies/incubating/buy_the_dip --fresh --network anvil --interval 60
```

## State Persistence

The strategy persists RSI signal state, trade counts, and cooldown timestamps across restarts via `get_persistent_state()` / `load_persistent_state()`.

## Teardown

Supports the framework teardown system. On teardown, swaps all base token holdings back to the quote token.
