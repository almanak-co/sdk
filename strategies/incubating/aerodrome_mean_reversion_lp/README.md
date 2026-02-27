# Aerodrome Mean-Reversion LP Strategy

Provides liquidity on Aerodrome (Base) when the market is range-bound, and exits when a trend develops.

## Concept

LP positions earn fees when prices oscillate around a mean (range-bound markets) but suffer impermanent loss when prices trend directionally. This strategy uses RSI as a regime detector:

- **RSI 40-60** (range-bound): Open LP to capture swap fees
- **RSI < 30 or > 70** (trending): Close LP to avoid impermanent loss
- **RSI 30-40 or 60-70** (neutral): Hold current position

## Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| pool | WETH/USDC | Aerodrome pool pair |
| stable | false | Pool type (stable or volatile) |
| amount0 | 0.001 | Amount of token0 (WETH) |
| amount1 | 3 | Amount of token1 (USDC) |
| rsi_period | 14 | RSI calculation period |
| rsi_timeframe | 4h | OHLCV candle timeframe |
| rsi_lower | 40 | Lower RSI bound for range-bound regime |
| rsi_upper | 60 | Upper RSI bound for range-bound regime |
| force_action | "" | Force "open" or "close" for testing |

## Usage

```bash
# Test on Anvil (force open)
almanak strat run -d strategies/incubating/aerodrome_mean_reversion_lp --network anvil --once

# RSI-based decision (remove force_action from config)
almanak strat run -d strategies/incubating/aerodrome_mean_reversion_lp --network anvil --once
```

## Kitchen Loop Context

Created in Iteration 5 to test:
- First LP intent (LPOpenIntent) in kitchenloop
- Aerodrome connector (untested in kitchenloop)
- Base chain for LP operations (only swaps tested previously)
- RSI indicator combined with LP management
