# Aave V3 Lending Lifecycle on Sonic

Full Aave V3 lending lifecycle on Sonic chain: supply collateral, borrow, repay, and withdraw.

## What It Does

1. **Supply** USDC as collateral to Aave V3 on Sonic
2. **Borrow** WETH against the collateral (30% LTV)
3. **Repay** the borrowed WETH
4. **Withdraw** the USDC collateral

## Quick Start

```bash
# Run full lifecycle on Anvil fork
almanak strat run -d almanak/demo_strategies/aave_v3_lending_sonic --network anvil --once

# Supply only
# Edit config.json: "force_action": "supply"
almanak strat run -d almanak/demo_strategies/aave_v3_lending_sonic --network anvil --once
```

## Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `collateral_token` | USDC | Token to supply as collateral |
| `collateral_amount` | 100 | Amount to supply |
| `borrow_token` | WETH | Token to borrow |
| `ltv_target` | 0.3 | Target LTV ratio (30%) |
| `borrow_amount_override` | 0.01 | Fixed borrow amount (bypasses price lookup) |
| `force_action` | lifecycle | Action mode: supply, borrow, repay, withdraw, lifecycle |

## Sonic-Specific Notes

- Sonic native token is S (wrapped: wS)
- Aave V3 pool: `0x5362dBb1e601abF3a4c14c22ffEdA64042E5eAA3`
- Chainlink feeds available: ETH/USD, USDC/USD, S/USD
- USDC on Sonic is bridged: `0x29219dd400f2Bf60E5a23d13Be72B486D4038894`
