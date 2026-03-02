# Morpho Blue Simple Supply

Supply wstETH as collateral to a Morpho Blue market on Ethereum.

## What It Does

1. Checks wstETH balance in wallet
2. If balance >= configured supply_amount, supplies wstETH as collateral to the wstETH/WETH Morpho Blue market
3. Holds after supplying

## Market

- **Market**: wstETH/WETH (94.5% LLTV)
- **Chain**: Ethereum
- **Collateral Token**: wstETH
- **Loan Token**: WETH

## Running

```bash
# Test on Anvil (auto-starts gateway + Anvil fork)
almanak strat run -d strategies/incubating/morpho_blue_supply --network anvil --once
```

## Kitchen Loop

- **Iteration**: 8
- **Source**: VIB-165
- **Gap Filled**: First kitchenloop test of Morpho Blue connector
