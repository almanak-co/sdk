# E2E Strategy Test Report: enso_uniswap_arbitrage

**Date:** 2026-02-06 09:18 UTC
**Result:** PARTIAL / FAIL
**Duration:** 3 minutes

---

## Summary

The enso_uniswap_arbitrage strategy executed only the first step of the arbitrage sequence (buy WETH via Enso) but did not complete the full round-trip arbitrage (sell WETH on Uniswap). This is due to a framework limitation where the single-chain StrategyRunner only executes the first intent in an IntentSequence.

**Verdict:** FAIL - Full lifecycle did not complete. Only 1 of 2 intents executed.

---

## Configuration

| Field | Value |
|-------|-------|
| Strategy | enso_uniswap_arbitrage |
| Chain | base |
| Network | Anvil fork |
| Port | 8548 |
| Trade Size | $0.40 |
| Mode | buy_enso_sell_uniswap |
| Base Token | WETH |
| Quote Token | USDC |
| Max Slippage | 1.0% |

---

## Lifecycle Phases

### Phase 1: Setup
- [x] Anvil started on port 8548 (Base)
- [x] Gateway started on port 50051
- [x] Wallet funded: 100 ETH, 1000 USDC, 1 WETH

### Phase 2: Strategy Execution
- [x] Strategy ran with --once flag
- [x] Step 1 intent executed: SWAP $0.40 USDC -> WETH via Enso
- [ ] Step 2 intent executed: SWAP ALL WETH -> USDC via Uniswap V3
- [ ] Full arbitrage completed

**Issue:** Single-chain orchestrator only executed first intent from sequence. Framework limitation noted in logs:
```text
Note: decide() returned 2 intents but single-chain orchestrator only
executes the first. Use multi-chain config for full support.
```

### Phase 3: Teardown
- N/A - Strategy did not reach completion state

### Phase 4: Verification
- [x] Verified partial execution
- Final balances:
  - USDC: 999.60 (spent 0.40 as expected)
  - WETH: 1.0002093 (gained 0.0002093 WETH)

---

## Execution Log Highlights

### Strategy Run (Phase 2)
```text
[2026-02-06T02:18:19] Executing buy_enso_sell_uniswap arbitrage: USDC -> WETH -> USDC
[2026-02-06T02:18:19] ARB SEQUENCE: Buy $0.40 WETH via Enso → Sell on Uniswap V3
[2026-02-06T02:18:19] demo_enso_uniswap_arbitrage intent sequence (2 steps):
[2026-02-06T02:18:19]    1. SWAP: $0.40 USDC → WETH (slippage: 1.00%) via enso
[2026-02-06T02:18:19]    2. SWAP: ALL WETH → USDC (slippage: 1.00%) via uniswap_v3
[2026-02-06T02:18:19] Note: decide() returned 2 intents but single-chain orchestrator only executes the first.

[2026-02-06T02:18:21] Getting Enso route: USDC -> WETH, amount=400000
[2026-02-06T02:18:21] Route found: 0x833589fC... -> 0x42000000..., amount_out=209348457202657, price_impact=4bp
[2026-02-06T02:18:21] Compiled SWAP (Enso): 0.4000 USDC → 0.0002 WETH (min: 0.0002 WETH)
[2026-02-06T02:18:21]    Slippage: 1.00% | Impact: 4bp (0.04%) | Txs: 2 | Gas: 597,841

[2026-02-06T02:18:27] Execution successful: gas_used=473561, tx_count=2
```

---

## Transactions

| Phase | Intent | Protocol | Amount | Gas Used | Status |
|-------|--------|----------|--------|----------|--------|
| Execute | APPROVE | Enso | - | ~30,000 | ✅ |
| Execute | SWAP | Enso | $0.40 USDC -> WETH | ~443,561 | ✅ |
| Execute | SWAP | Uniswap V3 | ALL WETH -> USDC | - | ❌ Not executed |

**Total Gas Used:** 473,561 (first step only)
**Expected Gas Estimate:** 597,841 (both steps)

---

## Final Verification

```bash
=== Final Wallet Balances ===
USDC balance: 999.600000 USDC
WETH balance: 1.000209348457202552 WETH

Changes from initial:
  USDC: 1000 -> 999.600000 (spent 0.400000 USDC) ✅
  WETH: 1.0 -> 1.000209348457202552 (gained 0.000209348457202552 WETH) ✅
```

The first step executed correctly:
- Spent exactly $0.40 USDC as configured
- Received ~0.0002093 WETH from Enso aggregator
- Price impact was 4bp (0.04%)

---

## Issues Encountered

### 1. Port Mismatch
**Issue:** Initial attempt used port 8547 for Base, but SDK code expects Base on port 8548.
**Resolution:** Restarted Anvil on correct port 8548.
**Root Cause:** Discrepancy between agent instructions (port 8547) and SDK code (port 8548).

### 2. Gateway Background Process
**Issue:** Gateway process died when started with `&` operator.
**Resolution:** Used `nohup bash -c '...'` to keep gateway running.

### 3. Incomplete Sequence Execution
**Issue:** StrategyRunner only executed first intent from IntentSequence.
**Root Cause:** Single-chain orchestrator limitation - not designed for multi-step sequences.
**Impact:** Full arbitrage did not complete, only buy side executed.

---

## Conclusion

**FAIL** - Strategy did not complete full lifecycle.

The strategy successfully executed the first intent (buy WETH via Enso) but failed to complete the second intent (sell WETH on Uniswap). This is due to a framework limitation where the single-chain StrategyRunner does not fully support IntentSequence execution.

For a complete arbitrage test, the strategy would need:
1. Multi-chain orchestrator configuration, OR
2. Refactoring to return single combined intent, OR
3. Manual execution of both steps separately

The partial execution demonstrates:
- ✅ Enso integration works correctly
- ✅ Transaction compilation and submission works
- ✅ Gateway and Anvil fork setup works
- ❌ Full arbitrage lifecycle not tested
- ❌ Uniswap V3 sell side not tested

**Recommendation:** Mark as INCOMPLETE. Requires framework enhancement to support full IntentSequence execution in single-chain mode, or strategy refactoring to use alternative pattern.
