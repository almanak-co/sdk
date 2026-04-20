# Aerodrome LP Teardown Analysis Report (Run 2)

**Date**: 2026-02-26
**Wallet**: `0x0738Ea642faA28fFc588717625e45F3078fDBAC9`
**Chain**: Base (mainnet)
**Strategy**: aerodrome_lp (WETH/USDC volatile pool)
**Invocation**: `--once --teardown-after --network mainnet` (in-process via CliRunner)

---

## Executive Summary

**TEARDOWN FAILED.** The strategy successfully opened an LP position (3 TXs confirmed on-chain), and the `--teardown-after` flag correctly triggered an in-process teardown with preserved state. However, the teardown LP_CLOSE compilation failed because the Aerodrome adapter could not resolve the LP pool token address through the TokenResolver. The LP position remains on-chain.

**Root Cause**: `AerodromeAdapter.build_remove_liquidity()` tries to resolve `0xcDAC0d6c6C59727a65F871236188350531885C43` (the vAMM-WETH/USDC LP token) via `TokenResolver`. This address is not in the static token registry, and the gateway's on-chain lookup via `TokenService/GetTokenMetadata` times out (30s per attempt). All 4 retry attempts failed identically.

---

## Strategy Execution Timeline

| Step | Time | Status | Details |
|------|------|--------|---------|
| Gateway start | 13:07:07 | OK | Mainnet mode, port 50051 |
| LP_OPEN compile | 13:07:08 | OK | 3 TXs, 312k gas estimated |
| TX 1: Approve WETH | 13:07:17 | OK | `0xe7aa5800...` block 42660945, 26,443 gas |
| TX 2: Approve USDC | 13:07:19 | OK | `0x690d5dfe...` block 42660946, 38,685 gas |
| TX 3: addLiquidity | 13:07:21 | OK | `0x69dfce9b...` block 42660947, 199,679 gas |
| LP_OPEN result enrichment | 13:07:35 | OK | Liquidity extracted |
| `on_intent_executed` | 13:07:35 | OK | `_has_position = True`, state saved |
| Teardown signal | 13:07:35 | OK | SOFT mode, `--teardown-after` flag |
| Teardown acknowledged | 13:07:35 | OK | `generate_teardown_intents()` called, 1 LP_CLOSE intent |
| LP_CLOSE attempt 1/4 | 13:07:35 | FAIL | TokenNotFoundError on LP token (30s timeout) |
| LP_CLOSE attempt 2/4 | 13:08:07 | FAIL | Same error (retry 1/3) |
| LP_CLOSE attempt 3/4 | 13:08:39 | FAIL | Same error (retry 2/3) |
| LP_CLOSE attempt 4/4 | 13:09:13 | FAIL | Same error (retry 3/3, final) |
| Final status | 13:09:44 | TEARDOWN FAILED | "manual intervention may be required" |

**Total gas used for LP_OPEN**: 264,807 (across 3 TXs)
**Teardown gas used**: 0 (never reached TX submission)

---

## On-Chain Transactions

| Step | TX Hash | Block | Gas Used | Status |
|------|---------|-------|----------|--------|
| Approve WETH | `0xe7aa58008b592acaa7922c60dd90f551efcf33d0f3d3de30d5eee6659201c7f1` | 42660945 | 26,443 | SUCCESS |
| Approve USDC | `0x690d5dfe27e6ee79da6bb877a9a111d5bbeb4e3337d2b5c0d652ce12ee0793ba` | 42660946 | 38,685 | SUCCESS |
| addLiquidity | `0x69dfce9b868c4209f01d83d7b24f329e4dc2ace46e1447ef8fda0121747a5e41` | 42660947 | 199,679 | SUCCESS |
| removeLiquidity (teardown) | N/A | N/A | N/A | NEVER SUBMITTED |

---

## Portfolio Comparison: Before vs After

### Token Balances

| Token | Before | After | Delta | Notes |
|-------|--------|-------|-------|-------|
| ETH (native) | 0.000202882 | 0.000200535 | -0.000002347 | Gas costs for 3 TXs |
| WETH | 0.001109033 | 0.001089695 | -0.000019338 | ~0.001 went to LP (partial) |
| USDC | 1.032384 | 0.992384 | **-0.040000** | Exactly 0.04 sent to LP per config |
| USDT | 0.070000 | 0.070000 | 0 | Unchanged |
| ALMANAK | 4535.7255 | 4535.7255 | 0 | Unchanged |
| vAMM-WETH/USDC | 2.588e-09 | 3.451e-09 | **+8.63e-10** | LP tokens INCREASED |

### DeFi Positions (DeBank `complex_protocol_list`)

| Protocol | Position | Before Value | After Value | Delta | Status |
|----------|----------|-------------|-------------|-------|--------|
| Aerodrome | WETH/USDC LP (volatile) | $0.240 | $0.320 | +$0.080 | **STILL OPEN** |

**Aerodrome LP Detail (Post-test)**:
- WETH in pool: 7.733e-05 (was 5.799e-05, +1.934e-05)
- USDC in pool: 0.1600 (was 0.1200, +0.0400)
- Net USD value: $0.32 (was $0.24)

### Other Protocol Positions

**None.** No lending, staking, farming, or other DeFi positions exist. The Aerodrome LP is the only protocol position. There are no "funny positions" in lending or staking protocols.

### Total Chain Balance

| Metric | Before | After | Delta |
|--------|--------|-------|-------|
| Total USD (Base) | $12.964 | $12.960 | -$0.004 |

The ~$0.004 loss is gas fees from the 3 LP_OPEN transactions.

---

## Teardown Verdict: FAILED

**The strategy did NOT teardown its resources.** Evidence:

1. **vAMM-WETH/USDC LP token balance increased** from 2.588e-09 to 3.451e-09
2. **Aerodrome LP position still shows in DeBank** at $0.32 (up from $0.24)
3. **WETH and USDC remain locked in pool** (7.7e-05 WETH + 0.16 USDC)
4. **Logs explicitly confirm**: "AerodromeLPStrategy teardown incomplete - manual intervention may be required"
5. **Teardown request status**: `failed` (persisted in state DB)

---

## Root Cause Analysis

### The Bug: LP token address not resolvable by TokenResolver

**Error message** (repeated 8 times across 4 retries):
```
TokenNotFoundError: Cannot resolve token '0xcDAC0d6c6C59727a65F871236188350531885C43' on base:
Address not found in registry for base.
```

**Call chain**:
1. `StrategyRunner` -> `generate_teardown_intents()` returns LP_CLOSE intent (WORKS - state preserved via `--teardown-after`)
2. `IntentCompiler` -> compiles LP_CLOSE -> calls `AerodromeAdapter.build_remove_liquidity()`
3. `AerodromeAdapter` -> needs to resolve LP pool token `0xcDAC0d6c6C59727a65F871236188350531885C43`
4. `TokenResolver.resolve("0xcDAC0d6c6C59727a65F871236188350531885C43", "base")` -> not in static registry
5. Falls through to gateway on-chain lookup via `TokenService/GetTokenMetadata`
6. Gateway RPC call to Base mainnet -> **times out after 30s**
7. `TokenNotFoundError` raised -> adapter returns error -> compiler fails -> retry

### Why it times out

The `TokenService/GetTokenMetadata` gRPC call has a 30s deadline. The LP token address `0xcDAC0d6c6C59727a65F871236188350531885C43` is a valid ERC-20 contract (Aerodrome's vAMM-WETH/USDC pool), but the gateway's on-chain lookup may be hitting RPC issues or the call pattern is inefficient for pool tokens.

### Contrast with previous run

A previous analysis (Run 1) found a different bug: `get_open_positions()` returned an empty list because in-memory state was lost. This run proves that the `--teardown-after` in-process path correctly preserves state (`_has_position = True`), and `generate_teardown_intents()` IS called. The failure occurs downstream at the adapter/token-resolution level.

---

## Bugs Found

### Bug 1: CLI wrapper missing --teardown-after passthrough (Severity: Medium)
**File**: `almanak/cli/cli.py`, `strategy_run()` function (line ~826)
The `strategy_run()` wrapper does not have a `--teardown-after` click option and does not pass it through to `framework_run_cmd` via `ctx.invoke()`. The option only exists on the inner `run()` command in `almanak/framework/cli/run.py`.

### Bug 2: Aerodrome LP token not resolvable for teardown (Severity: High)
**File**: `almanak/framework/connectors/aerodrome/adapter.py`
The `build_remove_liquidity()` method tries to resolve the Aerodrome pool LP token (`vAMM-WETH/USDC`) through `TokenResolver`, which doesn't know about protocol-specific LP tokens. The gateway on-chain fallback then times out. Fix options:
- Register the LP token in the resolver during `addLiquidity` (it already knows the pool address)
- Skip resolver for LP tokens and assume ERC-20 standard (18 decimals for Aerodrome pools)
- Use a direct `decimals()` call instead of the full TokenResolver pipeline

### Bug 3 (from Run 1, still relevant): `get_open_positions()` relies on in-memory state
**File**: `strategies/demo/aerodrome_lp/strategy.py`, line ~477
When invoked via the teardown CLI (not `--teardown-after`), a fresh strategy instance is created where `_has_position = False`. The teardown CLI's `get_open_positions()` check short-circuits before `generate_teardown_intents()` is ever called. This bug is bypassed by `--teardown-after` but still affects standalone teardown.

---

## Debug Log Files

| File | Description |
|------|-------------|
| `tests/reports/aerodrome_lp_teardown_preflight.md` | Pre-flight portfolio snapshot |
| `tests/reports/aerodrome_lp_console_log.txt` | Full console output from strategy run |
| `tests/reports/aerodrome_lp_debug_log.json` | JSON-format debug log |
| `tests/reports/aerodrome_lp_teardown_analysis.md` | This report |
