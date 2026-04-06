# PancakeSwap V3 + Aave V3 Carry Trade on BSC

T2 multi-protocol carry trade combining Aave V3 lending with PancakeSwap V3 swaps on BNB Smart Chain.

## What It Does

**Entry:**
1. **BORROW** - Supply WBNB collateral to Aave V3, borrow USDC at 30% LTV
2. **SWAP** - Swap borrowed USDC to USDT via PancakeSwap V3

**Teardown:**
3. **SWAP_BACK** - Swap USDT back to USDC via PancakeSwap V3
4. **REPAY** - Repay Aave V3 USDC debt (repay_full=True)
5. **WITHDRAW** - Withdraw WBNB collateral (withdraw_all=True)

## Quick Start

```bash
# Run on Anvil fork (use --interval for multi-step lifecycle)
almanak strat run -d almanak/demo_strategies/pancakeswap_aave_carry_bsc --network anvil --interval 5
```

## Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `collateral_token` | `WBNB` | Token to supply as collateral |
| `collateral_amount` | `0.5` | Amount of collateral |
| `borrow_token` | `USDC` | Token to borrow |
| `swap_to_token` | `USDT` | Token to swap borrowed funds into |
| `ltv_target` | `0.3` | Target loan-to-value ratio (30%) |

## BSC Notes

- USDC and USDT on BSC have **18 decimals** (not 6 like other chains)
- WBNB is the native wrapped token (not WETH)
- PancakeSwap V3 is the primary DEX on BSC
