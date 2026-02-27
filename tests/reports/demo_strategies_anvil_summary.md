# Demo Strategy Regression Report - Anvil (Iteration 27)

**Date:** 2026-02-27
**Network:** Anvil (local fork)
**Total strategies tested:** 13
**Iteration:** Kitchen Loop #27

## Summary Table

| # | Strategy | Chain | Status | Suspicious | TX Count | Notes |
|---|----------|-------|--------|------------|----------|-------|
| 1 | aave_borrow | arbitrum | PASS | 7 (0 err) | 3 | Supply + collateral enable |
| 2 | aerodrome_lp | base | PASS | 7 (0 err) | 3 | LP Open |
| 3 | almanak_rsi | base | PASS | 8 (0 err) | 2 | Init swap USDC->ALMANAK |
| 4 | enso_rsi | base | FAIL | 4 (2 err) | 0 | Missing ENSO_API_KEY (pre-existing) |
| 5 | ethena_yield | ethereum | PASS | 3 (0 err) | 2 | USDC->USDe swap via Enso |
| 6 | morpho_looping | ethereum | PASS | 4 (0 err) | 2 | wstETH supply to Morpho |
| 7 | pancakeswap_simple | arbitrum | PASS | 7 (0 err) | 2 | WETH->USDC swap |
| 8 | pendle_basics | arbitrum | FAIL | 8 (3 err) | 0 | Corrupt WSTETH Chainlink price (VIB-297) |
| 9 | spark_lender | ethereum | PASS | 4 (1 err) | 3 | Supply DAI (1st attempt reverted, retry OK) |
| 10 | sushiswap_lp | arbitrum | PASS | 6 (0 err) | 3 | LP Open position #33860 |
| 11 | traderjoe_lp | avalanche | PASS | 7 (0 err) | 3 | LP Open |
| 12 | uniswap_lp | arbitrum | PASS | 2 (0 err) | 3 | LP Open position #5332419 |
| 13 | uniswap_rsi | ethereum | PASS (HOLD) | 4 (0 err) | 0 | RSI=52.92, neutral zone, valid HOLD |

## Tally

- **10 PASS** (produced on-chain transactions)
- **1 PASS (HOLD)** (valid hold decision, no trade signal)
- **0 PARTIAL**
- **2 FAIL** (pre-existing issues, not regressions)

**11/13 passed (85%)** -- Both failures are pre-existing issues, not regressions from iteration 27 changes.

## Failure Analysis

### enso_rsi (FAIL) - Pre-existing
- **Root cause**: `ENSO_API_KEY` not set in `.env`. Enso Finance API requires authentication.
- **Regression?**: No. Persistent environment configuration issue.

### pendle_basics (FAIL) - Pre-existing (VIB-297)
- **Root cause**: Chainlink WSTETH/USD aggregator on Arbitrum returns corrupt price (~$12.3B).
- **Regression?**: No. VIB-297 fix (PR #401) addressed Ethereum but not Arbitrum.

## Notable Finding: spark_lender Gas Estimation

spark_lender's first attempt reverted because the static gas fallback (165,000) from PR #421's
multi-TX bundle fix is insufficient for Spark's `supply()` call (needs ~200,539). Retry succeeded
(single-TX, proper estimation). Minor regression from PR #421 -- should be tracked.

## Suspicious Behaviour Summary

| Strategy | Findings | Errors | Top Issues |
|----------|----------|--------|------------|
| aave_borrow | 7 | 0 | Token resolution (BTC, STETH, RDNT, MAGIC, WOO) |
| aerodrome_lp | 7 | 0 | Token resolution (BTC, WBTC, STETH, CBETH) |
| almanak_rsi | 8 | 0 | Token resolution (BTC, WBTC, STETH, CBETH) |
| enso_rsi | 4 | 2 | Missing ENSO_API_KEY |
| ethena_yield | 3 | 0 | Placeholder prices warning |
| morpho_looping | 4 | 0 | 9 tokens missing from Ethereum registry |
| pancakeswap_simple | 7 | 0 | Token resolution (BTC, STETH, RDNT, MAGIC, WOO) |
| pendle_basics | 8 | 3 | Corrupt WSTETH Chainlink price |
| spark_lender | 4 | 1 | TX revert on multi-TX gas fallback |
| sushiswap_lp | 6 | 0 | Token resolution (BTC, STETH, RDNT, MAGIC, WOO) |
| traderjoe_lp | 7 | 0 | Receipt parser mislabels LP as swap |
| uniswap_lp | 2 | 0 | Clean run |
| uniswap_rsi | 4 | 0 | Token resolution (9 tokens on Ethereum) |

**Aggregate stats:**
- Total suspicious findings: 71
- Strategies with ERROR-level findings: 3 (enso_rsi, pendle_basics, spark_lender)
- Strategies with clean logs (0-2 findings): 1 (uniswap_lp)
- Most common patterns:
  1. Token resolution failures for common symbols (BTC, STETH, RDNT, etc.) -- 10/13 strategies
  2. "Port not freed after 5s" Anvil cleanup warning -- ~8 strategies
  3. Missing API keys (Alchemy, CoinGecko, Enso) -- expected in test environments
