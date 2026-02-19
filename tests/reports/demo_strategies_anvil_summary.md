# Demo Strategies Anvil Test Summary

**Date:** 2026-02-15
**Network:** Anvil (local fork)
**Total Strategies Tested:** 15
**Branch:** `chore/reorganize-strategies` (worktree)

## Summary Table

| # | Strategy | Chain | Status | TX Hash(es) | Notes |
|---|----------|-------|--------|-------------|-------|
| 1 | aave_borrow | arbitrum | PASS | `0x8d4316...`, `0xac07e8...`, `0xe2ba70...` | Supplied 0.002 WETH to Aave V3 (step 1 of supply+borrow) |
| 2 | aerodrome_lp | base | PASS | `0xa09d93...`, `0x6d12b7...`, `0x97aa55...` | Opened WETH/USDC LP position |
| 3 | almanak_rsi | base | PASS | `0x59b0b1...`, `0x2f53bb...`, `0x0fc215...` | Initial buy: 5 USDC -> 2360 ALMANAK |
| 4 | copy_trader | arbitrum | PASS (HOLD) | - | No leader activity detected, correct HOLD |
| 5 | enso_rsi | base | PASS (HOLD) | - | RSI 51.10 neutral zone, correct HOLD |
| 6 | enso_uniswap_arbitrage | base | PARTIAL | `0x4b7986...`, `0xf11bcb...`, `0x5f3547...` | Leg 1 (Enso swap) succeeded; leg 2 failed: `amount='all' must be resolved` |
| 7 | ethena_yield | ethereum | PASS | `0x3c021f...`, `0xf12905...`, `0x2e8de1...` | Swapped 5 USDC -> 5.176 USDe via Enso |
| 8 | morpho_looping | ethereum | PASS | `0xdab288...`, `0x4f2a62...` | Supplied 0.1 wstETH to Morpho Blue. wstETH funding fix verified |
| 9 | pancakeswap_simple | arbitrum | PASS | `0x825dcf...`, `0x1e4401...` | Swapped 0.0049 WETH -> 9.97 USDC |
| 10 | pendle_basics | plasma | FAIL | - | FUSDT0 token unknown to Anvil funding system |
| 11 | spark_lender | ethereum | PASS | `0xad2120...` | Supplied 5 DAI to Spark (after 1 retry). DAI funding fix verified |
| 12 | sushiswap_lp | arbitrum | PASS | `0x23931f...`, `0x1f2f32...`, `0x973b1b...`, `0x697d3f...` | Opened LP position #32640 on SushiSwap V3 |
| 13 | traderjoe_lp | avalanche | PASS | `0xf01f04...`, `0xcbc9ee...`, `0x36f913...` | Opened LP position on TraderJoe V2 Liquidity Book |
| 14 | uniswap_lp | arbitrum | PASS | `0xd64991...`, `0x5dcb4c...`, `0x12e94c...`, `0xdd9562...` | Opened LP position #5309167 on Uniswap V3 |
| 15 | uniswap_rsi | ethereum | PASS (HOLD) | - | RSI 51.15 neutral zone, correct HOLD |

## Status Definitions

- **PASS**: Strategy ran, produced at least 1 on-chain transaction
- **PASS (HOLD)**: Strategy ran successfully but decided to HOLD (no trade signal triggered). Valid behavior.
- **PARTIAL**: Strategy started but encountered a non-fatal issue (e.g., first leg succeeded, second failed)
- **FAIL**: Strategy could not run or produce meaningful output

## Tally

**10 PASS / 3 PASS (HOLD) / 1 PARTIAL / 1 FAIL** out of 15 total

## Detailed Notes

### PASS Strategies

**aave_borrow** - Executed supply step (step 1 of 2-step supply+borrow flow). 3 TXs, 245,625 total gas. Strategy state progressed idle -> supplied. Would borrow on next run.

**aerodrome_lp** - Opened WETH/USDC LP position on Base. 3 TXs (2 approvals + 1 add liquidity), 264,807 total gas.

**almanak_rsi** - Initialization buy on first run. Bought 2360 ALMANAK for 5 USDC on Base via Uniswap V3. 3 TXs, 233,892 total gas. Config change: reduced `initial_capital_usdc` from 20 to 10.

**ethena_yield** - Swapped 5 USDC -> 5.176 USDe via Enso aggregator on Ethereum. 3 TXs (2 approvals + swap), 787,931 total gas. `force_action: "swap"` was already set.

**morpho_looping** - Supplied 0.1 wstETH as collateral to Morpho Blue on Ethereum. 2 TXs, 88,435 total gas. This validates the `anvil_funding` fix -- wstETH was successfully funded via the new explicit config.

**pancakeswap_simple** - Swapped 0.0049 WETH -> 9.97 USDC on PancakeSwap V3 Arbitrum. 2 TXs, 209,588 total gas.

**spark_lender** - Supplied 5 DAI to Spark on Ethereum. Initial attempt reverted, auto-retry succeeded. 1 TX (200,539 gas). This validates the `anvil_funding` fix -- DAI was successfully funded via the new explicit config.

**sushiswap_lp** - Opened WETH/USDC LP position #32640 on SushiSwap V3 Arbitrum. 4 TXs, 619,691 total gas. `force_action: "open"` was already set.

**traderjoe_lp** - Opened WAVAX/USDC LP position on TraderJoe V2 Liquidity Book (Avalanche). 3 TXs, 687,172 total gas. 11 bins around current price.

**uniswap_lp** - Opened WETH/USDC LP position #5309167 on Uniswap V3 Arbitrum. 4 TXs, 540,079 total gas. Temporarily added `force_action: "open"`, restored after.

### PASS (HOLD) Strategies

**copy_trader** - No leader wallet activity in lookback window (50 blocks). Correct HOLD behavior for copy trading with no signals.

**enso_rsi** - RSI at 51.10 (neutral zone 30-70). Correct HOLD -- no buy/sell signal.

**uniswap_rsi** - RSI at 51.15 (neutral zone 40-70). Correct HOLD -- no buy/sell signal.

### PARTIAL Strategies

**enso_uniswap_arbitrage** - Leg 1 (Enso USDC->WETH swap) succeeded with 3 TXs (1,794,924 gas). Leg 2 (Uniswap WETH->USDC swap) failed with error: `amount='all' must be resolved before compilation`. Framework bug in intent sequencing -- `amount="all"` not resolved from previous step output.

### FAILED Strategies

**pendle_basics** - Anvil funding system does not recognize FUSDT0 token for Plasma chain. Error: `Unknown token FUSDT0 for chain plasma, skipping`. Wallet received 0 FUSDT0 despite config requesting 10,000. Strategy correctly detected insufficient balance and held. Fix needed: Register FUSDT0 token address in Anvil funding registry for Plasma chain.

## Config Changes Made During Testing

| Strategy | Change | Reason |
|----------|--------|--------|
| almanak_rsi | `initial_capital_usdc`: 20 -> 10 | Budget cap compliance |
| uniswap_lp | Added then removed `force_action: "open"` | Trigger immediate trade |

## Key Findings

1. **`anvil_funding` fix works** -- morpho_looping (wstETH) and spark_lender (DAI) both pass now after adding explicit token funding to config.json
2. **pendle_basics needs token registry update** -- FUSDT0 on Plasma is not in the Anvil funding system's token lookup
3. **Intent sequencing bug** -- `amount="all"` in chained IntentSequence is not resolved between steps (affects enso_uniswap_arbitrage)
