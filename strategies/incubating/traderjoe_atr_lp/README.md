# TraderJoe V2 ATR-Adaptive LP Strategy

ATR-adaptive liquidity provision on TraderJoe V2's Liquidity Book (Avalanche).

## Concept

Uses ATR (Average True Range) to dynamically size LP range widths:

- **Low volatility** (ATR < 2%): Tight range (5% width) -- concentrate liquidity for maximum fee capture
- **Normal volatility** (ATR 2-5%): Normal range (10% width) -- balanced risk/return
- **High volatility** (ATR > 5%): No LP -- impermanent loss risk too high

## Run

```bash
almanak strat run -d strategies/incubating/traderjoe_atr_lp --network anvil --once
```

## Config

| Parameter | Default | Description |
|-----------|---------|-------------|
| pool | WAVAX/USDC/20 | Pool identifier (TOKEN_X/TOKEN_Y/BIN_STEP) |
| amount_x | 1 | WAVAX to provide |
| amount_y | 30 | USDC to provide |
| atr_period | 14 | ATR calculation period |
| atr_timeframe | 4h | OHLCV candle timeframe |
| atr_low_pct | 2.0 | ATR% below which vol is "low" |
| atr_high_pct | 5.0 | ATR% above which vol is "high" |
| range_tight_pct | 0.05 | Range width in low vol (5%) |
| range_normal_pct | 0.10 | Range width in normal vol (10%) |
| force_action | "" | Force "open" or "close" for testing |

## Kitchen Loop

Created in Iteration 6. First strategy to test:
- Avalanche chain
- TraderJoe V2 connector
- ATR indicator
- Volatility-adaptive LP range sizing
