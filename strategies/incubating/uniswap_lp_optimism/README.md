# Uniswap V3 LP on Optimism

Kitchen Loop Iteration 18 -- first kitchenloop test on Optimism chain.

## What it does

Opens a concentrated Uniswap V3 liquidity position (WETH/USDC, 0.05% fee tier)
with a 20% price range width centered on the current ETH/USDC price.

## Gap filled

First kitchenloop strategy on Optimism. Validates chain support, token resolution,
Anvil fork, gateway auto-start, and wallet funding on OP chain.

## Run

```bash
almanak strat run -d strategies/incubating/uniswap_lp_optimism --network anvil --once
```
