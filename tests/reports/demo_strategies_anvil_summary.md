# Demo Strategy Regression Report

**Date:** 2026-03-06
**Network:** Anvil (local fork)
**Iteration:** 52 (BenQi lending lifecycle)
**Total strategies tested:** 17 demo + 2 incubating = 19 runs
**Note:** ALCHEMY_API_KEY was empty; all strategies used public RPCs for forking

## Summary Table

| # | Strategy | Chain | Status | Suspicious | TX Hash | Notes |
|---|----------|-------|--------|------------|---------|-------|
| 1 | aave_borrow | arbitrum | PASS | 3 (0 err) | f577687f...f4f0 | Supply + set collateral (3 TXs) |
| 2 | aerodrome_lp | base | PASS | 5 (0 err) | 74986ac5...6e07 | LP addLiquidity (3 TXs) |
| 3 | aerodrome_paper_trade | base | PASS | 3 (0 err) | b0a99de6...1d2e | LP via RSI trigger (3 TXs) |
| 4 | almanak_rsi | base | FAIL | 5 (2 err) | - | No CoinGecko key for ALMANAK token |
| 5 | copy_trader | arbitrum | PASS (HOLD) | 5 (2 err) | - | No leader activity on fork |
| 6 | enso_rsi | base | PASS | 4 (0 err) | b8ef26...f829 | USDC->WETH swap via Enso (2 TXs) |
| 7 | enso_uniswap_arbitrage | base | PASS | 5 (0 err) | multiple | 4-TX arb round-trip |
| 8 | ethena_yield | ethereum | PASS | 4 (1 err) | acb8d6ba...9eee | Swap succeeded on retry |
| 9 | mantle_swap | mantle | FAIL | 4 (3 err) | - | No Chainlink feeds + CoinGecko rate limit |
| 10 | metamorpho_base_yield | base | PASS | 4 (0 err) | e70eddf7...c6c4 | 50 USDC vault deposit (2 TXs) |
| 11 | metamorpho_eth_yield | ethereum | PASS | 4 (1 err) | 06d78d53...f299 | 40 USDC vault deposit (2 TXs) |
| 12 | morpho_looping | ethereum | PASS | 5 (2 err) | 89dfc184...1548 | wstETH supply collateral (2 TXs) |
| 13 | pancakeswap_simple | arbitrum | PASS | 2 (1 err) | 45b07098...5976 | WETH->USDC swap (2 TXs) |
| 14 | pendle_basics | arbitrum | PASS | 4 (0 err) | 7b3e0562...c78c7 | wstETH->PT swap (2 TXs) |
| 15 | spark_lender | ethereum | PASS | 4 (1 err) | 7677afda...119f | DAI supply, retry after gas cap |
| 16 | sushiswap_lp | arbitrum | PASS | 4 (0 err) | multiple | LP #34820 minted (3 TXs) |
| 17 | traderjoe_lp | avalanche | PASS | 4 (0 err) | 56fef928...bba60 | LP addLiquidity (3 TXs) |
| 18 | uniswap_lp | arbitrum | PASS | 5 (0 err) | 5397ab5c...edf0 | LP #5347822 minted (3 TXs) |
| 19 | uniswap_rsi | ethereum | PASS | 5 (0 err) | fdecee56...0bb3 | USDC->WETH swap (2 TXs) |

## Tally

**15 PASS / 1 PASS(HOLD) / 0 PARTIAL / 2 FAIL out of 17 demo strategies**

(Plus 2 incubating strategies tested: enso_uniswap_arbitrage PASS, copy_trader PASS(HOLD))

## Status Definitions

- **PASS**: Strategy ran, produced at least 1 on-chain transaction
- **PASS (HOLD)**: Strategy ran successfully but decided to HOLD (no trade signal)
- **PARTIAL**: Strategy started but encountered a non-fatal issue
- **FAIL**: Strategy could not run

## Failure Analysis

### almanak_rsi (FAIL)
ALMANAK token has no Chainlink price feed on Base. CoinGecko free tier was rate-limited. The strategy correctly returned HOLD rather than executing an unpriced swap. Fix: set `ALMANAK_GATEWAY_COINGECKO_API_KEY`.

### mantle_swap (FAIL)
Mantle chain has zero Chainlink feeds registered. CoinGecko free tier was simultaneously rate-limited. Strategy returned HOLD correctly. Fix: set `ALMANAK_GATEWAY_COINGECKO_API_KEY` or register Chainlink feeds for Mantle.

## Suspicious Behaviour Summary

### Most common patterns

| Pattern | Count | Severity | Assessment |
|---------|-------|----------|------------|
| CoinGecko rate limiting (free tier) | 15/17 | WARNING | Expected without API key |
| Pendle circular import at startup | 17/17 | WARNING | Pre-existing bug in incubating strategy |
| Placeholder prices in IntentCompiler | 12/17 | WARNING | Expected in Anvil mode |
| No Alchemy/CoinGecko API key | 10/17 | INFO | Expected in dev environment |

### Aggregate stats

- Total suspicious findings: ~78
- Strategies with ERROR-level findings: 7
- Strategies with clean logs (0 findings): 0
- Most common: CoinGecko rate limiting, pendle circular import, placeholder prices

## Notable Issues (recurring)

1. **Spark gas cap**: Static 165K gas cap too low for Spark supply (~200K); always requires retry
2. **Ethena 2-TX route**: Enso 2-TX route consistently reverts (selector 0xef3dcb2f); 1-TX retry succeeds
3. **Pendle circular import**: Every strategy logs this error from `pendle_pt_swap_arbitrum`
4. **Mantle pricing**: No Chainlink feeds registered for Mantle chain

## Comparison with Iteration 51

| Metric | Iter 51 | Iter 52 | Delta |
|--------|---------|---------|-------|
| PASS | 8 | 15 | +7 |
| PASS (HOLD) | 1 | 1 | 0 |
| FAIL | 1 | 2 | +1 |
| Total tested | 10 | 19 | +9 |

Key changes: ethena_yield recovered (now PASS via retry), almanak_rsi regressed (CoinGecko dependency). Full coverage run (all 17 demo + 2 incubating) vs selective in iter 51.
