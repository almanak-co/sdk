# PancakeSwap V3 + Aave V3 Carry Trade on BSC

T2 multi-protocol carry trade combining Aave V3 lending with PancakeSwap V3 swaps on BNB Smart Chain.

## What It Does

**Entry (`decide()` — establishes the carry, then HOLDs it):**
1. **SUPPLY** - Supply WBNB collateral to Aave V3
2. **BORROW** - Borrow USDC against it at 30% LTV
3. **SWAP** - Swap borrowed USDC to USDT via PancakeSwap V3
4. **HOLD** - The carry is on; a carry earns by being held, so it is unwound on a
   teardown signal — not auto-closed one iteration after opening.

**Teardown (`generate_teardown_intents()` — HF-safe unwind):**
1. **SWAP** the held USDT back to USDC (close the swap leg), then
2. delegate the Aave unwind to the framework's HF-safe
   [`generate_lending_unwind`](../../framework/teardown/lending_unwind.py) primitive
   (VIB-5467 / TD-09). It sizes every leg from the **live** `variableDebt`/`balanceOf`
   and drives debt to a true zero (sourcing accrued-interest shortfall from
   collateral) **before** the final `withdraw_all`.

> **Why the unwind is not run in `decide()`:** a carry round-trips borrow → swap →
> swap-back, so the wallet ends holding less USDC than the interest-grown debt. A naive
> `repay_full → withdraw_all` there leaves *dust debt* and the `withdraw_all` reverts
> `HealthFactorLowerThanLiquidationThreshold`, stranding the collateral (VIB-5637 /
> VIB-5448). The teardown lane's HF-safe primitive + fresh-state guard prevent that.

## Quick Start

```bash
# Establish the carry on an Anvil fork (SUPPLY → BORROW → SWAP, then HOLD)
almanak strat run -d almanak/demo_strategies/pancakeswap_aave_carry_bsc --network anvil --interval 5

# Unwind it HF-safely via the teardown signal
almanak strat teardown request -s <deployment_id> --wait
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
