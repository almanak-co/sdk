# Incubating Strategies - Anvil Test Summary

**Date**: 2026-02-20
**Network**: Anvil (local fork)
**Total Strategies**: 29
**Status**: COMPLETE

## Summary Table

| # | Strategy | Chain | Status | TX Hash | Notes |
|---|----------|-------|--------|---------|-------|
| 1 | bb_perps | arbitrum | FAIL | 0x0b22...(reverted) | GMX V2 oracle validation fails on Anvil fork. 4 TX attempts all reverted. |
| 2 | buy_the_dip | arbitrum | PASS | 0xa03e...f5 | Swapped 10 USDC -> 0.0051 WETH. Added anvil_funding + set rsi_oversold=99. |
| 3 | CCStratBT | arbitrum | PASS (HOLD) | - | Needs 3+ ticks to buffer prices for indicators. No force_action. --once = 1 tick only. |
| 4 | copy_trader_swap | arbitrum | PASS | 0xfa7d...8a | Swapped USDC->WETH via replay signal. Fixed float Decimals + filters->global_policy. |
| 5 | copy_trader | arbitrum | PASS (HOLD) | - | No leader activity in Anvil fork block window. No force_action/replay configured. |
| 6 | enso_uniswap_arbitrage | base | PARTIAL | 0xac39...f0 | Step 1 (Enso swap) OK. Step 2 failed: amount='all' chaining broken -- no swap_amounts extracted from Enso receipt. |
| 7 | ethena_leverage_loop | ethereum | PASS | 0x1052...84 | Swapped 50 USDC -> 50.04 USDe via Enso. First step of leverage loop. |
| 8 | ethena_pt_leverage | ethereum | PASS (HOLD) | - | 50 USDC < hardcoded 100 USDC entry threshold. FLASH_LOAN intent not yet supported anyway. |
| 9 | gmx_perps | arbitrum | FAIL | 0x6b6c...(reverted) | GMX V2 oracle/keeper incompatible with Anvil fork. 4 TX attempts all reverted. |
| 10 | kraken_rebalancer | arbitrum | PASS (HOLD) | - | CEX strategy -- runs in simulation mode without Kraken API keys. No on-chain TX expected. |
| 11 | leverage_loop_cross_chain | base | PARTIAL | 0x089c...f4 | Step 1 (Enso cross-chain swap) OK. Bridge timeout after 300s -- no relayer on Anvil fork. |
| 12 | lido_staker | ethereum | FAIL | 0xa1e1...(partial) | stETH deposit OK but wstETH wrap reverts (approval issue on Anvil). Use receive_wrapped=false as workaround. |
| 13 | macd_momentum | base | PASS | 0x0d0b...ec | Swapped $5 USDC -> 0.0025 WETH via Enso. Added anvil_funding. |
| 14 | momentum_accumulation | arbitrum | PASS | 0x023b...e9 | Swapped 30 USDC -> 0.0153 WETH via Uniswap V3 (init_swap phase). Added anvil_funding. |
| 15 | morpho_aave_arb | ethereum | PASS | 0x34a1...1b | Supplied 0.013 wstETH to Morpho Blue. Used APY overrides + force_protocol=morpho. |
| 16 | morpho_leverage_lst | ethereum | PASS | 0x7204...ce | Supplied 0.01 wstETH to Morpho Blue (SETUP phase). Reduced initial_collateral to $24. |
| 17 | multi_signal_accumulator | arbitrum | PASS | 0x2166...3f | Swapped $5 USDC -> 0.0026 WETH via Enso. Added anvil_funding. |
| 18 | pendle_aave_spread | arbitrum | FAIL | - | SDK API mismatch: TokenBalance vs Decimal comparison + missing market.lending_rate(). |
| 19 | pendle_pt_rotator | arbitrum | FAIL | - | TokenBalance vs Decimal comparison crash in _buy_pt_tranche(). Same bug as pendle_aave_spread. |
| 20 | pendle_pt_wsteth_leverage | arbitrum | PASS | 0x9ca9...ef | Swapped WSTETH -> PT-wstETH via Pendle. Fell back to swap-only (no Morpho market on Arb). |
| 21 | pendle_rwa_yt_yield | ethereum | PASS | 0x9363...25 | Swapped sUSDe -> YT-sUSDe via Pendle. YT swap succeeded (previously expected to fail). |
| 22 | polymarket_arbitrage | polygon | FAIL | - | CLOB-based strategy -- no on-chain execution possible on Anvil. No Polymarket API keys. |
| 23 | polymarket_signal_trader | polygon | FAIL | - | CLOB-based strategy -- no on-chain execution on Anvil. No Polymarket API keys. |
| 24 | rsi_martingale_short | arbitrum | FAIL | 0x7012...(reverted) | GMX V2 PERP_OPEN reverted -- missing USDC approval + zero balance on Anvil. Same GMX issue as bb_perps. |
| 25 | senior_quant_copy_trader | arbitrum | PARTIAL | 0xe075...(approve OK, swap reverted) | Signal+sizing OK. Swap STF revert due to LocalSimulator price limitation. Fixed float Decimals. |
| 26 | traderjoe_fee_rotator | avalanche | PASS | 0x709f...34 | LP opened on TraderJoe WAVAX/USDC pool. 3 TXs confirmed. |
| 27 | traderjoe_vol_rebalancer | avalanche | PASS | 0x65ab...4e | LP opened on TraderJoe WAVAX/USDC pool. 3 TXs confirmed. |
| 28 | vault_yield_rotator | ethereum | PASS | 0x9237...ef | Swapped 50 USDC -> WETH via Uniswap V3. Removed placeholder vault addresses. |
| 29 | whale_follower | arbitrum | PASS (HOLD) | - | No leader activity in Anvil fork block window. Reactive copy-trader, needs live whale swaps. |

## Status Definitions

- **PASS**: Strategy ran, produced at least 1 on-chain transaction (swap, LP, lend, etc.)
- **PASS (HOLD)**: Strategy ran successfully but decided to HOLD (no trade signal triggered). Valid behavior.
- **PARTIAL**: Strategy started but encountered a non-fatal issue (e.g., step 1 succeeded, step 2 failed)
- **FAIL**: Strategy could not complete (reverts, API mismatch, missing infrastructure)

## Final Tally

**13 PASS / 5 PASS(HOLD) / 3 PARTIAL / 8 FAIL out of 29 total**

### Pass Rate: 62% (18/29 ran successfully, 13 with on-chain TXs)

## Failure Analysis

### Structural / Expected Failures (not bugs)
| Strategy | Root Cause | Fixable? |
|----------|-----------|----------|
| bb_perps | GMX V2 oracle/keeper needs live infrastructure, incompatible with Anvil | No -- requires live keepers |
| gmx_perps | Same GMX V2 Anvil incompatibility | No -- same root cause |
| rsi_martingale_short | GMX V2 + missing USDC approval in adapter | Partially -- approval is a bug, but GMX still won't work on Anvil |
| polymarket_arbitrage | CLOB-based, no on-chain execution path | No -- by design |
| polymarket_signal_trader | CLOB-based, no on-chain execution path | No -- by design |
| lido_staker | wstETH wrap approval fails on Anvil; works with receive_wrapped=false | Yes -- config workaround exists |

### SDK Bugs Found
| Strategy | Bug | Impact |
|----------|-----|--------|
| pendle_aave_spread | `TokenBalance` vs `Decimal` comparison (`>` operator) | Crashes strategy before any trade |
| pendle_pt_rotator | Same `TokenBalance` vs `Decimal` bug | Crashes strategy before any trade |
| enso_uniswap_arbitrage | Enso receipt parser missing `extract_swap_amounts()` -- breaks amount='all' chaining | Step 2 of sequences fails |
| copy_trader_swap | Float values in SafeDecimal fields + `filters` key rejected by V2 schema | Config bug, silent fallback |
| senior_quant_copy_trader | Same float/filters config bug | Config bug, silent fallback |

### Common Config Issues
- Many strategies missing `anvil_funding` block -- required for managed Anvil forks
- Copy trading strategies use JSON floats where SafeDecimal requires strings
- Copy trading strategies use `filters` key instead of V2's `global_policy`

## Chains Tested

| Chain | Strategies | Pass | Fail |
|-------|-----------|------|------|
| Arbitrum | 14 | 9 | 5 |
| Ethereum | 7 | 6 | 1 |
| Base | 3 | 2 | 1 (partial) |
| Avalanche | 2 | 2 | 0 |
| Polygon | 2 | 0 | 2 |
| Plasma | 1 | 0 | 0 (not in incubating) |

## Protocols Exercised

Uniswap V3, Enso, Morpho Blue, Pendle, TraderJoe V2, Aave V3, Ethena, Lido, GMX V2, Polymarket, Kraken (CEX)
