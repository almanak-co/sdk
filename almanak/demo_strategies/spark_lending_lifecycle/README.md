# Spark Lending Lifecycle

Full Spark Protocol lending lifecycle on Ethereum: supply wstETH collateral, borrow DAI, repay DAI, withdraw wstETH.

## What It Does

1. **SUPPLY** wstETH as collateral to Spark
2. **BORROW** DAI against the wstETH collateral (variable rate)
3. **REPAY** DAI debt (repay_full=True)
4. **WITHDRAW** wstETH collateral

## Quick Start

```bash
# Run full lifecycle on Anvil fork
almanak strat run -d almanak/demo_strategies/spark_lending_lifecycle --network anvil --once

# Run a single step
# Edit config.json: "force_action": "supply"
almanak strat run -d almanak/demo_strategies/spark_lending_lifecycle --network anvil --once
```

## Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `collateral_token` | `wstETH` | Token to supply as collateral |
| `collateral_amount` | `1` | Amount of collateral to supply |
| `borrow_token` | `DAI` | Token to borrow |
| `ltv_target` | `0.3` | Target loan-to-value ratio (30%) |
| `borrow_amount_override` | `500` | Fixed borrow amount (bypasses price calc) |
| `force_action` | `lifecycle` | Action mode: supply/borrow/repay/withdraw/lifecycle |

## About Spark

Spark (SparkLend) is a MakerDAO/Sky ecosystem fork of Aave V3, focused on DAI-centric lending. Key differences from Aave V3:

- All supplied assets are automatically used as collateral (cannot disable per-asset)
- DAI-centric markets with MakerDAO governance
- Only variable rate borrowing (stable rate deprecated)
