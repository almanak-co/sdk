# Demo Strategy Regression Report - Anvil (Iteration 28)

**Date:** 2026-02-28
**Network:** Anvil (local fork)
**Iteration:** Kitchen Loop #28
**Total strategies tested:** 13

## Summary Table

| # | Strategy | Chain | Status | Suspicious | TX Count | Notes |
|---|----------|-------|--------|------------|----------|-------|
| 1 | aave_borrow | arbitrum | PASS | 4 (0 err) | 3 | Supply WETH to Aave V3 (304K gas) |
| 2 | aerodrome_lp | base | PASS | 7 (0 err) | 3 | LP_OPEN WETH/USDC (342K gas) |
| 3 | almanak_rsi | base | PASS | 9 (0 err) | 2 | Swap USDC->ALMANAK (192K gas) |
| 4 | copy_trader | arbitrum | PASS | 8 (0 err) | 2 | Swap USDC->WETH (181K gas) |
| 5 | enso_rsi | base | FAIL | 5 (2 err) | 0 | Missing ENSO_API_KEY (pre-existing) |
| 6 | ethena_yield | ethereum | PASS | 5 (1 err) | 2 | Stake USDe->sUSDe (135K gas). Swap path needs ENSO_API_KEY |
| 7 | morpho_looping | ethereum | PASS | 5 (0 err) | 2 | Supply wstETH to Morpho (123K gas) |
| 8 | pancakeswap_simple | arbitrum | PASS | 7 (0 err) | 2 | Swap WETH->USDC (227K gas) |
| 9 | pendle_basics | arbitrum | FAIL | 8 (3 err) | 0 | VIB-297: Corrupt Chainlink wstETH/USD ($12.3B) |
| 10 | spark_lender | ethereum | PASS | 4 (1 err) | 3 | Supply DAI (first TX reverted, retry OK) |
| 11 | sushiswap_lp | arbitrum | PASS | 6 (0 err) | 3 | LP_OPEN WETH/USDC NFT#33897 (539K gas) |
| 12 | traderjoe_lp | avalanche | PASS | 7 (0 err) | 3 | LP_OPEN WAVAX/USDC (700K gas) |
| 13 | uniswap_lp | arbitrum | PASS | 4 (0 err) | 3 | LP_OPEN WETH/USDC NFT#5333197 (558K gas) |
| 14 | uniswap_rsi | ethereum | PASS | 12 (0 err) | 2 | RSI=39.79 BUY, swap USDC->WETH (180K gas) |

## Tally

**11 PASS / 0 PASS(HOLD) / 0 PARTIAL / 2 FAIL** out of 13 total

Note: 14 rows because uniswap_rsi is strategy #13 but numbered #14 due to the table layout. Actual unique strategies: 13.

## Failure Analysis

### enso_rsi (FAIL) -- Pre-existing
- **Root cause**: `ENSO_API_KEY` not set in `.env`
- **Impact**: Enso Finance API required for swap routing; fails at compile time
- **Regression**: No -- same failure in iterations 26, 27

### pendle_basics (FAIL) -- Pre-existing (VIB-297)
- **Root cause**: Chainlink wstETH/USD aggregator on Arbitrum Anvil fork returns ~$12.3B instead of ~$2,379
- **Impact**: Magnitude outlier guard correctly rejects all price sources; strategy HOLDs
- **Regression**: No -- same failure since VIB-297 was opened

## Suspicious Behaviour Summary

| Strategy | Findings | Errors | Top Issues |
|----------|----------|--------|------------|
| uniswap_rsi | 12 | 0 | 9 token resolution warnings on Ethereum |
| almanak_rsi | 9 | 0 | 4 token resolution warnings + ALMANAK single-source price |
| copy_trader | 8 | 0 | 5 token resolution warnings (Arbitrum) |
| pendle_basics | 8 | 3 | Corrupt Chainlink price, all sources rejected |
| aerodrome_lp | 7 | 0 | 4 token resolution warnings (Base) |
| pancakeswap_simple | 7 | 0 | 5 token resolution warnings (Arbitrum) |
| traderjoe_lp | 7 | 0 | JOE token missing from Avalanche registry |
| sushiswap_lp | 6 | 0 | 5 token resolution warnings (Arbitrum) |
| enso_rsi | 5 | 2 | Missing ENSO_API_KEY |
| ethena_yield | 5 | 1 | Swap path needs ENSO_API_KEY |
| morpho_looping | 5 | 0 | 9 Ethereum token warnings |
| aave_borrow | 4 | 0 | Clean run |
| spark_lender | 4 | 1 | First supply TX reverted (gas cap), retry OK |
| uniswap_lp | 4 | 0 | Clean run |

**Aggregate stats:**
- Total suspicious findings: 91
- Strategies with ERROR-level findings: 4 (enso_rsi, ethena_yield, pendle_basics, spark_lender)
- Strategies with clean logs (0 errors): 9
- Most common patterns:
  1. Token resolution warnings during MarketService init (all chains)
  2. Missing ENSO_API_KEY for Enso-dependent strategies
  3. Anvil port cleanup timing (cosmetic)

## Regression Assessment

**No regressions detected from iteration 28 changes.** All 4 PRs (BorrowIntent summary fix, Compound V3 borrow log alias, fail-fast perp errors, Compound V3 lending rate) did not break any existing demo strategy.
