# Config-Driven RSI Swap Strategy

This demo keeps the historical `uniswap_rsi` directory and `demo_uniswap_rsi` strategy name, but the strategy code is now protocol-agnostic for supported spot-swap connectors. Change `protocol`, `chain`, `base_token`, `quote_token`, and funding in config; keep `strategy.py` unchanged.

## Examples

Default config: Uniswap V3 WETH/USDC on Ethereum.

```bash
uv run almanak strat run -d almanak/demo_strategies/uniswap_rsi --network anvil --interval 60
```

TraderJoe V2 WAVAX/USDC on Avalanche:

```bash
uv run almanak strat run \
  -d almanak/demo_strategies/uniswap_rsi \
  --config almanak/demo_strategies/uniswap_rsi/config.traderjoe_avalanche.json \
  --network anvil \
  --interval 60
```

For production-like validation, run continuously and use the separate teardown command when you want to unwind.

## Configuration

```json
{
    "chain": "arbitrum",
    "protocol": "uniswap_v3",
    "base_token": "WETH",
    "quote_token": "USDC",
    "trade_size_usd": 3,
    "rsi_period": 14,
    "rsi_oversold": 40,
    "rsi_overbought": 70,
    "max_slippage_bps": 100
}
```

The strategy validates that the configured protocol is supported on the configured chain before it starts. Current supported protocol families are `uniswap_v3`, `traderjoe_v2`, `aerodrome`, `pancakeswap_v3`, and `sushiswap_v3`.

## Behavior

The strategy monitors RSI for `base_token`.

- RSI <= `rsi_oversold`: swap quote token into base token.
- RSI >= `rsi_overbought`: swap base token into quote token.
- Otherwise: hold.

The dashboard uses the shared TA dashboard template and reads the same config fields, including `protocol`, `chain`, and token pair.
