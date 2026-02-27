# Bollinger Band Volatility Trader

**Chain**: Base
**Protocol**: Uniswap V3 (default swap routing)
**Kitchen Loop**: Iteration 4

## Strategy Logic

Uses Bollinger Bands to detect volatility regimes and trade ETH/USDC mean reversion:

| Condition | Action | Rationale |
|-----------|--------|-----------|
| Squeeze + near lower band | BUY | Mean reversion entry in quiet market |
| Expansion + near upper band | SELL | Profit-taking during vol spike |
| Above upper band | SELL | Stop-out on extreme extension |
| Otherwise | HOLD | Wait for signal |

## What This Tests

- Base chain (first kitchenloop ideate test on Base)
- Bollinger Bands indicator (`market.bollinger_bands()`)
- Default swap routing on Base
- Bandwidth and percent_b calculations

## Running

```bash
# Test on Anvil (force_action="buy" in config)
almanak strat run -d strategies/incubating/bb_volatility_trader --network anvil --once
```

## Configuration

See `config.json` for all parameters. Key settings:

- `trade_size_usd`: Amount per trade in USD
- `bb_period/bb_std_dev/bb_timeframe`: Bollinger Band parameters
- `buy_percent_b/sell_percent_b`: Signal thresholds
- `squeeze_bandwidth/expansion_bandwidth`: Volatility regime boundaries
- `force_action`: Set to "buy" or "sell" for testing
