# Demo Strategy Regression Report (Anvil)

**Date:** 2026-03-03
**Network:** Anvil (local fork)
**Iteration:** Kitchen Loop iter 29c regress
**Total strategies tested:** 13

## Summary Table

| # | Strategy | Chain | Status | Suspicious | TX Hash | Notes |
|---|----------|-------|--------|------------|---------|-------|
| 1 | aave_borrow | arbitrum | PASS | 5 (0 err) | `8ab7bec6...013ad` | Supply WETH, 3 TXs (approve+supply+setCollateral) |
| 2 | aerodrome_lp | base | PASS | 4 (0 err) | `659a80...0a7f` | LP_OPEN, 3 TXs (2 approves + addLiquidity) |
| 3 | almanak_rsi | base | PASS | 4 (0 err) | `87d4775f...e4505d` | Init swap 10 USDC -> ALMANAK, 2 TXs |
| 4 | enso_rsi | base | PASS | 4 (0 err) | `ad5587fc...d7a0` | force_action=buy, $3 USDC -> WETH via Enso, 2 TXs |
| 5 | ethena_yield | ethereum | PASS | 5 (0 err) | `51bc1c98...833de` | 5 USDC -> USDe via Enso, 2 TXs |
| 6 | morpho_looping | ethereum | PASS | 5 (0 err) | `6080e74f...cbb4f` | Supply 0.1 wstETH collateral, 2 TXs |
| 7 | pancakeswap_simple | arbitrum | PASS | 4 (0 err) | `50b4ed19...d958` | Swap 0.0051 WETH -> USDC, 2 TXs |
| 8 | pendle_basics | arbitrum | FAIL | 5 (3 err) | - | Pre-existing VIB-297: wstETH Chainlink ~$12.3B |
| 9 | spark_lender | ethereum | PASS | 5 (1 err) | `d01fdb78...` | Supply 5 DAI, first-TX revert then retry success |
| 10 | sushiswap_lp | arbitrum | PASS | 3 (0 err) | `7692fe67...96eb` | LP_OPEN NFT #34626, 3 TXs |
| 11 | traderjoe_lp | avalanche | PASS | 5 (0 err) | `625c887d...3ddb` | LP_OPEN, 3 TXs (2 approves + addLiquidity) |
| 12 | uniswap_lp | arbitrum | PASS | 5 (0 err) | `4d1da7f8...55be` | LP_OPEN NFT #5341845, 3 TXs |
| 13 | uniswap_rsi | ethereum | PASS(HOLD) | 4 (0 err) | - | RSI=44.98 in neutral zone [40-70], correct HOLD |

## Tally

**11 PASS / 1 PASS(HOLD) / 0 PARTIAL / 1 FAIL** out of 13 total

## Failures

### pendle_basics (FAIL - pre-existing)

**Root cause:** VIB-297 (wstETH Chainlink oracle on Arbitrum returns ~$12.3B instead of ~$2.4K). The price aggregator detects 5,079,157x divergence between Chainlink ($12.3B) and CoinGecko ($2,418), rejects both as outliers, and raises `AllDataSourcesFailed`. The strategy catches the ValueError and returns HOLD with no transactions.

**Status:** Pre-existing since iter 20+. Not a regression from iter 29c changes.

## Suspicious Behaviour Summary

| Strategy | Findings | Errors | Top Issues |
|----------|----------|--------|------------|
| pendle_basics | 5 | 3 | Chainlink wstETH ~$12.3B, price aggregator AllDataSourcesFailed |
| spark_lender | 5 | 1 | First-TX revert (gas underestimation), retry recovers |
| traderjoe_lp | 5 | 0 | LP TX mislabeled as swap, duplicate receipt parsing |
| enso_rsi | 4 | 0 | Amount chaining gap (Enso extract_swap_amounts) |

**Aggregate stats:**
- Total suspicious findings across all strategies: 58
- Strategies with ERROR-level findings: 2 (pendle_basics, spark_lender)
- Most common patterns: (1) Port not freed after 5s (12/13), (2) No Alchemy/CoinGecko API keys (13/13), (3) Placeholder prices in Anvil mode (5/13)

## Comparison to Previous Iterations

| Metric | Iter 32 | Iter 29 (prev) | Iter 29c |
|--------|---------|----------------|----------|
| PASS | 10 | 12 | 11 |
| PASS(HOLD) | 1 | 0 | 1 |
| FAIL | 2 | 1 | 1 |
| pendle_basics | FAIL | FAIL | FAIL |
| traderjoe_lp | FAIL | PASS | PASS |

traderjoe_lp improved from intermittent FAIL (Avalanche public RPC storage access limitation) to stable PASS.
