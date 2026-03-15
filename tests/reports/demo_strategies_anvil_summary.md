# Demo Strategy Regression Report — Iteration 81

**Date**: 2026-03-16
**Network**: Anvil (local fork)
**Total strategies tested**: 24 EVM (8 Solana skipped — Anvil is EVM-only)

## Summary Table

| # | Strategy | Chain | Status | Suspicious | TX Hash | Notes |
|---|----------|-------|--------|------------|---------|-------|
| 1 | aave_borrow | arbitrum | PASS | 1 (0 err) | 0xa6208c76...ecd3 | Supply 0.002 WETH + set collateral (3 TXs) |
| 2 | aave_pnl_lending | arbitrum | PASS | 2 (0 err) | 0xe054ec16... | Supply 0.4 WETH to Aave V3 (3 TXs) |
| 3 | aerodrome_lp | base | PASS | 2 (0 err) | 0x0327a3...fff2 | LP_OPEN 0.001 WETH + 0.04 USDC (3 TXs) |
| 4 | aerodrome_paper_trade | base | PASS | 3 (0 err) | 0xc2e91365...a13e | LP_OPEN via RSI signal (3 TXs) |
| 5 | almanak_rsi | base | PASS | 5 (0 err) | 0x717eef17... | Swap 10 USDC -> 4616 ALMANAK via Uniswap V3 (3 TXs) |
| 6 | balancer_flash_arb | arbitrum | FAIL | 2 (1 err) | - | Pre-existing: _fetch_prices_for_intent doesn't walk callback_intents |
| 7 | compound_paper_trade | base | PASS | 2 (0 err) | 0xa6e5fa85...4137 | Supply 100 USDC to Compound V3 (2 TXs) |
| 8 | enso_rsi | base | PASS | 0 | 0x37c4462a... | Swap USDC->WETH via Enso (2 TXs) |
| 9 | enso_uniswap_arbitrage | base | PASS | 2 (0 err) | 0x56b3f1...9aac | 2-leg arb USDC->WETH->USDC (4 TXs) |
| 10 | ethena_yield | ethereum | PASS | 3 (0 err) | 0x465e3870... | Swap 5 USDC -> 5.13 USDe via Enso (2 TXs) |
| 11 | mantle_mnt_accumulator | mantle | PASS | 3 (0 err) | 0xffe7c73f... | Profit take: 15 WMNT -> 11.70 USDT (2 TXs, retried once) |
| 12 | mantle_swap | mantle | PASS | 3 (0 err) | 0x93ea461a... | RSI BUY: USDT->WETH via Agni (2 TXs) |
| 13 | metamorpho_base_yield | base | PASS | 2 (0 err) | 0x1d5aa85e...f528 | Deposit 50 USDC to Moonwell MetaMorpho vault (2 TXs) |
| 14 | metamorpho_eth_yield | ethereum | PASS | 2 (0 err) | 0x3209518a... | Deposit 100 USDC to Steakhouse vault (2 TXs) |
| 15 | morpho_looping | ethereum | PASS | 3 (0 err) | 0x7e8882...4104 | Supply 0.028 wstETH collateral to Morpho Blue (2 TXs) |
| 16 | morpho_paper_trade | ethereum | PASS | 3 (0 err) | 0xe054ec16... | Supply 0.028 wstETH collateral (2 TXs) |
| 17 | pancakeswap_lp | arbitrum | PASS | 4 (1 err) | 0xc6f301ee...901e | LP_OPEN position #339767, WETH/USDC (3 TXs) |
| 18 | pancakeswap_simple | arbitrum | PASS | 2 (0 err) | 0xc53715ab...6461 | Swap WETH->USDC via PancakeSwap V3 (2 TXs) |
| 19 | pendle_basics | arbitrum | PASS | 2 (1 err) | 0xfa811c86...597a | Swap wstETH->PT-wstETH via Pendle (2 TXs) |
| 20 | rsi_macd_lp | arbitrum | PASS (HOLD) | 2 (0 err) | - | RSI=56.4, MACD neutral — correct HOLD |
| 21 | spark_lender | ethereum | PASS | 0 | 0xa7c0603b... | Supply 5 DAI to Spark (2 TXs) |
| 22 | sushiswap_lp | arbitrum | PASS | 0 | 0xc6f301ee...901e | LP_OPEN position #35563, WETH/USDC (3 TXs) |
| 23 | traderjoe_lp | avalanche | PASS | 4 (0 err) | 0xe220c6c9...eaa4 | LP_OPEN WAVAX/USDC via TraderJoe V2 (3 TXs) |
| 24 | uniswap_lp | arbitrum | PASS | 2 (0 err) | 0x919f0145... | LP_OPEN position #5366280, WETH/USDC (3 TXs) |
| 25 | uniswap_rsi | ethereum | PASS (HOLD) | 2 (0 err) | - | RSI=53.74, neutral — correct HOLD |

## Status Definitions

- **PASS**: Strategy ran, produced at least 1 on-chain transaction
- **PASS (HOLD)**: Strategy ran successfully but decided to HOLD (no trade signal triggered). Valid behavior.
- **PARTIAL**: Strategy started but encountered a non-fatal issue
- **FAIL**: Strategy could not run (import error, compilation error, etc.)

## Tally

**21 PASS / 2 PASS(HOLD) / 0 PARTIAL / 1 FAIL** out of 24 total

## Suspicious Behaviour Summary

### Per-strategy breakdown (strategies with findings > 0)

| Strategy | Findings | Errors | Top Issues |
|----------|----------|--------|------------|
| almanak_rsi | 5 | 0 | CoinGecko rate limits, ALMANAK 1/4 sources (DexScreener only) |
| pancakeswap_lp | 4 | 1 | aiohttp session leak, bin_ids warning |
| traderjoe_lp | 4 | 0 | Double receipt parse, misleading swap label in LP |
| aerodrome_paper_trade | 3 | 0 | Placeholder prices, CoinGecko, insecure mode |
| ethena_yield | 3 | 0 | CoinGecko rate limit for USDe |
| mantle_mnt_accumulator | 3 | 0 | TX deadline expired (auto-retried), evm_snapshot unsupported |
| mantle_swap | 3 | 0 | USDT stablecoin fallback, CoinGecko |
| morpho_looping | 3 | 0 | wstETH 2/4 sources, placeholder prices |
| morpho_paper_trade | 3 | 0 | wstETH 2/4 sources, placeholder prices |
| balancer_flash_arb | 2 | 1 | Missing WETH price in callback compilation |
| pendle_basics | 2 | 1 | PT-wstETH has no price source (all 4 fail) |
| enso_uniswap_arbitrage | 2 | 0 | Uniswap V3 USDC decimals hardcoded to 18 |

### Aggregate stats

- Total suspicious findings across all strategies: ~56
- Strategies with ERROR-level findings: 3 (balancer_flash_arb, pancakeswap_lp, pendle_basics)
- Strategies with clean logs (0 findings): 3 (enso_rsi, spark_lender, sushiswap_lp)
- Most common patterns:
  1. CoinGecko rate limiting / no API key (20/24 strategies)
  2. Insecure mode warning — expected in Anvil dev (18/24 strategies)
  3. Placeholder prices in compiler (6/24 strategies)

## Changes vs Iteration 80

- mantle_swap upgraded from PASS(HOLD) to PASS — RSI naturally triggered a BUY signal
- almanak_rsi stable at PASS — DexScreener pricing for ALMANAK token reliable
- Unit tests: 9415 passed (up from 9360 in iter 80, +55 new tests)
- balancer_flash_arb remains the only FAIL (pre-existing flash loan callback price oracle gap)
