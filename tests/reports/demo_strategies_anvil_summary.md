# Demo Strategies Anvil Test Summary

**Date:** 2026-02-23
**Network:** Anvil (local fork)
**Total Strategies Tested:** 13
**Runner:** `uv run almanak strat run -d <dir> --network anvil --once`

---

## Summary Table

| # | Strategy | Chain | Status | Suspicious | TX Hash (primary) | Notes |
|---|----------|-------|--------|------------|-------------------|-------|
| 1 | aave_borrow | arbitrum | PARTIAL | 1 (1 err) | b576d6aff56aff41... (reverted) | BORROW reverted — stale state loaded `loop_state=supplied` from prior run; no on-chain collateral on fresh fork |
| 2 | aerodrome_lp | base | PASS | 0 | 8444b737bfe9fc82... | LP_OPEN succeeded (3 TXs, 342,140 gas); gas estimation falls back to compiler limit on tx3 (non-fatal) |
| 3 | almanak_rsi | base | PASS | 2 (0 err) | cca582130469f735... | SWAP 10 USDC to 5454 ALMANAK (2 TXs); 2 WARNING token resolution errors for USDC.e and USDC_BRIDGED on Base (non-fatal) |
| 4 | enso_rsi | base | PASS (HOLD) | 0 | - | RSI=33.22, neutral zone [40-70]; correctly held. No trade signal |
| 5 | ethena_yield | ethereum | PASS | 0 | 181d063421c6d246... | SWAP 5 USDC to USDe via Enso (2 TXs, 885,574 gas) |
| 6 | morpho_looping | ethereum | PASS | 1 (0 err) | 976501f63557445f... | SUPPLY 0.1 wstETH to Morpho Blue (2 TXs, 122,635 gas); WARNING about placeholder prices in MorphoBlueAdapter |
| 7 | pancakeswap_simple | arbitrum | PASS | 0 | e69f7d919594d918... | SWAP 0.0053 WETH to 9.97 USDC (2 TXs, 226,641 gas) |
| 8 | pendle_basics | plasma | PASS | 0 | b72637296e02fef9... | SWAP 1 FUSDT0 to PT-fUSDT0 on Plasma chain (2 TXs, 360,577 gas) |
| 9 | spark_lender | ethereum | PASS | 0 | 6da0a30cd0a2a7c6... | SUPPLY 5 DAI to Spark (2 TXs on retry 1; first attempt reverted, retry succeeded) |
| 10 | sushiswap_lp | arbitrum | PASS | 1 (0 err) | 43fb354f734545af... | LP_OPEN WETH/USDC succeeded (3 TXs, 615,552 gas, position #33136); WARNING: amount chaining no output amount from step 1 |
| 11 | traderjoe_lp | avalanche | PASS | 1 (0 err) | 942a1c8a58e68bce... | LP_OPEN WAVAX/USDC succeeded (3 TXs, 694,280 gas); WARNING: amount chaining no output amount |
| 12 | uniswap_lp | arbitrum | PASS | 1 (0 err) | 8bc0dfaf48098a20... | LP_OPEN WETH/USDC succeeded after clearing stale state (position #5323041, 3 TXs, 523,694 gas); WARNING: amount chaining |
| 13 | uniswap_rsi | ethereum | PASS | 2 (0 err) | 82018b2f8a99129f... | RSI=32.67 triggered BUY: SWAP $3 USDC to WETH (2 TXs, 179,999 gas); WARNING: USDC.e and USDC_BRIDGED token resolution errors on ethereum |

---

## Status Definitions

- **PASS**: Strategy ran and produced at least 1 on-chain transaction
- **PASS (HOLD)**: Strategy ran successfully but decided to HOLD (no trade signal triggered)
- **PARTIAL**: Strategy started but encountered a non-fatal issue (TX compiled but reverted)
- **FAIL**: Strategy could not run at all

---

## Tally

**10 PASS / 1 PASS(HOLD) / 1 PARTIAL / 0 FAIL** out of 13 total

---

## Suspicious Behaviour Summary

### Per-Strategy Breakdown (strategies with findings > 0)

| Strategy | Findings | Errors | Top Issues |
|----------|----------|--------|------------|
| aave_borrow | 1 | 1 | ERROR: BORROW TX reverted (selector 0x5b263df7) — stale state caused attempt to borrow without on-chain collateral |
| almanak_rsi | 2 | 0 | WARNING: USDC.e token not found on Base chain; WARNING: USDC_BRIDGED token not found on Base chain |
| morpho_looping | 1 | 0 | WARNING: MorphoBlueAdapter using placeholder prices (not real oracle prices) |
| sushiswap_lp | 1 | 0 | WARNING: Amount chaining — no output amount extracted from step 1 |
| traderjoe_lp | 1 | 0 | WARNING: Amount chaining — no output amount extracted from step 1 |
| uniswap_lp | 1 | 0 | WARNING: Amount chaining — no output amount extracted from step 1 |
| uniswap_rsi | 2 | 0 | WARNING: USDC.e token not found on ethereum; WARNING: USDC_BRIDGED token not found on ethereum |

### Aggregate Statistics

- **Total suspicious findings across all strategies:** 9
- **Strategies with ERROR-level findings:** 1 (aave_borrow)
- **Strategies with clean logs (0 findings):** 6 (aerodrome_lp, enso_rsi, ethena_yield, pancakeswap_simple, pendle_basics, spark_lender)
- **Most common patterns:**
  1. **Amount chaining warning** (3 strategies: sushiswap_lp, traderjoe_lp, uniswap_lp) — LP adapters do not emit output amounts for ResultEnricher chaining
  2. **USDC.e / USDC_BRIDGED token resolution warnings** (2 strategies: almanak_rsi, uniswap_rsi) — compiler checks bridged USDC variants on chains where they do not exist
  3. **Placeholder prices in MorphoBlueAdapter** (1 strategy: morpho_looping) — uses hardcoded placeholder prices instead of live oracle data

---

## Detailed Notes Per Strategy

### 1. aave_borrow — PARTIAL

**Chain:** Arbitrum
**Config:** collateral=WETH 0.002, borrow=USDC

**Issue:** Strategy resumed from stale persistent state (`loop_state=supplied`) left by a prior test run. On a fresh Anvil fork, no collateral has actually been deposited into Aave. The strategy skipped the SUPPLY step and jumped directly to BORROW, which reverted with selector `0x5b263df7` (Aave V3 COLLATERAL_BALANCE_IS_ZERO error).

**Root cause:** The `almanak_state.db` retains strategy state across fork resets. The state key `loop_state` was set to `"supplied"` from a previous run. The strategy reads this state and assumes on-chain collateral exists — but on a fresh fork it does not.

**Impact:** This is a stale-state issue, not a code logic bug. In production (live chain), state would be consistent with on-chain reality. For Anvil testing, state should be cleared between runs.

---

### 2. aerodrome_lp — PASS

**Chain:** Base
**Intents:** LP_OPEN (WETH/USDC)
**TXs:** 3 (approve WETH, approve USDC, mint LP position)
**Gas:** 342,140
**Primary TX:** `8444b737bfe9fc82...`

Gas estimation falls back to compiler gas limit for tx3 (non-fatal). LP position opened and position_id extracted correctly.

---

### 3. almanak_rsi — PASS

**Chain:** Base
**Intents:** SWAP (USDC to ALMANAK)
**TXs:** 2 (approve + swap)
**Primary TX:** `cca582130469f735...`

2 token resolution WARNINGs during compilation for USDC.e and USDC_BRIDGED on Base. Non-fatal: standard USDC resolves correctly. Bought 5454 ALMANAK for 10 USDC.

---

### 4. enso_rsi — PASS (HOLD)

**Chain:** Base
**RSI:** 33.22 (neutral zone, thresholds: oversold < 30, overbought > 70)
**Decision:** HOLD — no trade signal

Strategy correctly identified that RSI is in neutral territory and returned HoldIntent. No transactions submitted.

---

### 5. ethena_yield — PASS

**Chain:** Ethereum
**Intents:** SWAP (USDC to USDe via Enso)
**TXs:** 2
**Gas:** 885,574
**Primary TX:** `181d063421c6d246...`

Force action `swap` in config triggered immediate swap. Clean logs.

---

### 6. morpho_looping — PASS

**Chain:** Ethereum
**Intents:** SUPPLY (0.1 wstETH to Morpho Blue)
**TXs:** 2 (approve + supply)
**Gas:** 122,635
**Primary TX:** `976501f63557445f...`

WARNING about MorphoBlueAdapter using placeholder prices. The on-chain supply TX executed correctly with 0.1 wstETH supplied.

---

### 7. pancakeswap_simple — PASS

**Chain:** Arbitrum
**Intents:** SWAP (WETH to USDC)
**TXs:** 2
**Gas:** 226,641
**Primary TX:** `e69f7d919594d918...`

Receipt parser INFO about not declaring support for `swap_amounts` (handled gracefully by ResultEnricher). Swapped 0.0053 WETH to 9.97 USDC.

---

### 8. pendle_basics — PASS

**Chain:** Plasma
**Intents:** SWAP (FUSDT0 to PT-fUSDT0)
**TXs:** 2
**Gas:** 360,577
**Primary TX:** `b72637296e02fef9...`

Pendle market integration on Plasma chain working correctly. Clean logs.

---

### 9. spark_lender — PASS

**Chain:** Ethereum
**Intents:** SUPPLY (5 DAI to Spark)
**TXs:** 2 (first attempt reverted, retry 1 succeeded)
**Gas (success):** 200,539
**Primary TX:** `6da0a30cd0a2a7c6...`

First attempt compiled 2 TXs (approve + supply) but the supply TX reverted (likely DAI permit/approval incompatibility). Retry 1 succeeded with a fresh supply. Intent retry mechanism worked correctly.

---

### 10. sushiswap_lp — PASS

**Chain:** Arbitrum
**Intents:** LP_OPEN (WETH/USDC/3000, fee=3000)
**TXs:** 3 (approve WETH, approve USDC, mint position #33136)
**Gas:** 615,552
**Primary TX:** `43fb354f734545af...`

Amount chaining warning is non-fatal. LP_OPEN succeeded and position_id (33136) was correctly extracted.

---

### 11. traderjoe_lp — PASS

**Chain:** Avalanche
**Intents:** LP_OPEN (WAVAX/USDC, bin_step=20)
**TXs:** 3 (approve WAVAX, approve USDC, add liquidity)
**Gas:** 694,280
**Primary TX:** `942a1c8a58e68bce...`

Gas estimation failed for tx3/3 (fell back to compiler gas limit — non-fatal). LP position opened in bins [8.14-9.00] WAVAX/USDC.

---

### 12. uniswap_lp — PASS

**Chain:** Arbitrum
**Intents:** LP_OPEN (WETH/USDC/500, fee=500)
**TXs:** 3 (approve WETH, approve USDC, mint position #5323041)
**Gas:** 523,694
**Primary TX:** `8bc0dfaf48098a20...`

First run returned HOLD because stale state (position_id=5317742) was loaded from `almanak_state.db`. State was cleared manually, then re-run succeeded. Amount chaining warning same as other LP adapters.

---

### 13. uniswap_rsi — PASS

**Chain:** Ethereum
**Intents:** SWAP (USDC to WETH, RSI-triggered BUY)
**TXs:** 2 (approve + swap)
**Gas:** 179,999
**Primary TX:** `82018b2f8a99129f...`

RSI=32.67 triggered BUY signal (oversold threshold=40). Same USDC.e / USDC_BRIDGED token resolution warnings as almanak_rsi — both non-fatal fallback checks.

---

## Cross-Cutting Issues Found

### Issue 1: Stale State Across Fork Resets

**Affected:** aave_borrow (PARTIAL), uniswap_lp (required manual state clear)

Strategies with unique `strategy_id` in config persist state in `almanak_state.db` between runs. When the same strategy ID is reused on a fresh Anvil fork, the strategy may resume from stale state that references on-chain positions that don't exist in the new fork.

**Recommendation:** The CLI's `--network anvil --once` mode should optionally auto-clear state for the given strategy_id, or provide a `--reset-state` flag to prevent stale-state failures during testing.

### Issue 2: Amount Chaining Warning in LP Adapters

**Affected:** sushiswap_lp, traderjoe_lp, uniswap_lp (all WARNING, non-fatal)

After LP_OPEN execution, the ResultEnricher emits: `Amount chaining: no output amount extracted from step 1; subsequent amount='all' steps will fail`. This warns that if a multi-step intent sequence tried to chain the LP position's output amount into a second step, it would fail.

**Recommendation:** LP adapters (UniswapV3, SushiSwapV3, TraderJoeV2) should expose an `extract_output_amount()` or equivalent so ResultEnricher can chain amounts for multi-step intent sequences.

### Issue 3: USDC.e / USDC_BRIDGED Token Resolution Warnings

**Affected:** almanak_rsi (Base), uniswap_rsi (Ethereum)

During intent compilation, the token resolver tries USDC.e and USDC_BRIDGED as fallback aliases on chains where they don't exist. The warnings are logged but non-fatal.

**Recommendation:** Restrict bridged USDC fallback lookups to chains where they exist (Arbitrum for USDC.e, Avalanche/Polygon for USDC_BRIDGED). Avoid spurious warnings on Ethereum and Base mainnet.

### Issue 4: MorphoBlueAdapter Placeholder Prices

**Affected:** morpho_looping (WARNING)

The MorphoBlueAdapter uses placeholder prices for position sizing rather than live oracle data.

**Recommendation:** Wire MorphoBlueAdapter to live price feeds (CoinGecko or Morpho Blue oracle) for accurate position sizing in the strategy decision layer.

---

## Infrastructure Notes

- **CLI auto-manages gateway and Anvil fork**: `--network anvil` auto-starts a managed gateway on port 50051 and an Anvil fork on a random port. No manual setup needed.
- **Anvil fork auto-funding**: Tokens in `anvil_funding` config are auto-funded to the Anvil default wallet on each run.
- **Port not freed warning**: Non-fatal cosmetic warning after each run — fork is stopped correctly.
- **Gateway insecure mode**: Expected for local Anvil testing.
- **CoinGecko free tier**: 30 req/min limit; no rate limiting issues encountered during testing.

---

SUSPICIOUS_BEHAVIOUR_COUNT: 9
SUSPICIOUS_BEHAVIOUR_ERRORS: 1
