# Swap-and-Supply IntentSequence Strategy

Kitchen Loop Iteration 15: First-ever IntentSequence test.

## What it does

1. **Swap**: Buys WETH with USDC on Uniswap V3 (fixed USD amount)
2. **Supply**: Supplies ALL received WETH to Aave V3 as collateral (amount="all")

The two steps execute as an IntentSequence, with the swap output amount chained
to the supply input via `amount="all"`.

## Why it matters

- First strategy to use IntentSequence (never tested in 14 iterations)
- Validates amount chaining between swap output and supply input
- Tests multi-protocol composability (Uniswap V3 + Aave V3)
- Exercises the most pervasive latent bug: 11/13 strategies affected by amount chaining gap

## Running

```bash
# On Anvil fork (recommended for testing)
almanak strat run -d strategies/incubating/swap_and_supply --network anvil --once

# With forced execution
# Set "force_action": "execute" in config.json
```

## Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| swap_amount_usd | 5 | USD value of USDC to swap |
| max_slippage_bps | 100 | Max slippage in bps (1%) |
| base_token | WETH | Token to buy and supply |
| quote_token | USDC | Token to sell |
| lending_protocol | aave_v3 | Where to supply |
| force_action | "" | Set to "execute" to bypass signals |
