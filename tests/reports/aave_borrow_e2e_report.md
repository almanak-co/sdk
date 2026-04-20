# E2E Strategy Test Report: aave_borrow

**Date:** 2026-02-05 17:04
**Result:** PASS
**Duration:** 7 minutes

---

## Summary

Full E2E lifecycle test of aave_borrow demo strategy on Arbitrum Anvil fork completed successfully. The strategy executed SUPPLY → BORROW → HOLD → REPAY → WITHDRAW → SWAP teardown sequence with all intents succeeding and position fully closed.

---

## Configuration

| Field | Value |
|-------|-------|
| Strategy | aave_borrow |
| Chain | Arbitrum (42161) |
| Network | Anvil fork |
| Port | 8545 |
| Collateral Token | WETH |
| Collateral Amount | 0.002 WETH |
| Borrow Token | USDC |
| LTV Target | 50% |
| Min Health Factor | 2.0 |
| Interest Rate Mode | Variable |

---

## Lifecycle Phases

### Phase 1: Setup
- [x] Anvil started on port 8545 (Arbitrum fork)
- [x] Gateway started on port 50051
- [x] Wallet funded:
  - 100 ETH (gas)
  - 1 WETH (wrapped from ETH)
  - 10,000 USDC (storage slot method)

### Phase 2: Strategy Execution
- [x] Strategy ran successfully
- [x] Initial intents executed:
  1. SUPPLY: 0.002 WETH to Aave V3 as collateral
  2. BORROW: 2.0 USDC from Aave V3 (50% LTV)
- [x] Reached HOLD state after 3 iterations

### Phase 3: Teardown
- [x] Teardown signal sent (mode: SOFT, reason: "E2E test teardown")
- [x] Teardown intents executed:
  1. REPAY: 2.000001 USDC (full debt + interest)
  2. WITHDRAW: 0.002000000117528454 WETH (collateral + yield)
  3. SWAP: 0.002 WETH → 3.98 USDC
- [x] Strategy shutdown cleanly

### Phase 4: Verification
- [x] No remaining debt/positions in Aave
- [x] Final balances:
  - WETH: 0.998000000117528454 (net -0.002 WETH)
  - USDC: 10,003.982442 (net +3.98 USDC)

---

## Execution Log Highlights

### Strategy Run (Phase 2)

```
[2026-02-05 17:02:45] 📥 SUPPLY intent: 0.0020 WETH to Aave V3
[2026-02-05 17:02:46] Compiled SUPPLY: 0.0020 WETH to aave_v3 (as collateral)
                      Txs: 3 | Gas: 530,000
[2026-02-05 17:02:56] Status: SUCCESS | Intent: SUPPLY | Gas used: 304610 | Duration: 12671ms
[2026-02-05 17:02:56] 🔍 Parsed Aave V3: SUPPLY 2,000,000,000,000,000 to 0x82af...bab1

[2026-02-05 17:03:11] 📤 BORROW intent: Collateral=$3.99, LTV=50%, Borrow=2.0000 USDC
[2026-02-05 17:03:11] Compiled BORROW: Supply 0 WETH (collateral) -> Borrow 2.0000 USDC
                      Protocol: aave_v3 | Txs: 1 | Gas: 450,000
[2026-02-05 17:03:25] Status: SUCCESS | Intent: BORROW | Gas used: 303322 | Duration: 14221ms
[2026-02-05 17:03:25] 🔍 Parsed Aave V3: BORROW 2,000,000 (variable) from 0xaf88...5831

[2026-02-05 17:03:37] ⏸️ demo_aave_borrow HOLD: Loop complete - position established
[2026-02-05 17:03:37] Status: HOLD | Intent: HOLD | Duration: 14ms
[2026-02-05 17:03:52] Status: HOLD | Intent: HOLD | Duration: 10ms
[2026-02-05 17:04:03] Status: HOLD | Intent: HOLD | Duration: 1545ms
```

### Teardown Run (Phase 3)

```
[2026-02-05 17:04:37] Found active teardown request for demo_aave_borrow: mode=SOFT, status=cancel_window
[2026-02-05 17:04:37] 🛑 demo_aave_borrow entering TEARDOWN mode (3 intents to execute)

[2026-02-05 17:04:39] 🛑 Executing teardown intent 1/3: REPAY
[2026-02-05 17:04:40] Compiled REPAY: USDC, full=True, 2 txs, 330000 gas
[2026-02-05 17:04:41] Execution successful: gas_used=230673, tx_count=2
[2026-02-05 17:04:41] 🔍 Parsed Aave V3: REPAY 2,000,001 to 0xaf88...5831

[2026-02-05 17:04:41] 🛑 Executing teardown intent 2/3: WITHDRAW
[2026-02-05 17:04:41] Compiled WITHDRAW: WETH, all=True, 1 txs, 250000 gas
[2026-02-05 17:04:44] Execution successful: gas_used=165538, tx_count=1
[2026-02-05 17:04:44] 🔍 Parsed Aave V3: WITHDRAW 2,000,000,117,528,454 from 0x82af...bab1

[2026-02-05 17:04:44] 🛑 Executing teardown intent 3/3: SWAP
[2026-02-05 17:04:45] ✅ Compiled SWAP: 0.0020 WETH → 3.9454 USDC (min: 3.9257 USDC)
                      Slippage: 0.50% | Txs: 2 | Gas: 280,000
[2026-02-05 17:04:50] Execution successful: gas_used=177936, tx_count=2
[2026-02-05 17:04:50] 🛑 demo_aave_borrow teardown complete - shutting down strategy runner
```

---

## Transactions

| Phase | Intent | Gas Used | Tx Count | Status |
|-------|--------|----------|----------|--------|
| Execute | SUPPLY | 304,610 | 3 | ✅ |
| Execute | BORROW | 303,322 | 1 | ✅ |
| Execute | HOLD | 0 | 0 | ✅ |
| Teardown | REPAY | 230,673 | 2 | ✅ |
| Teardown | WITHDRAW | 165,538 | 1 | ✅ |
| Teardown | SWAP | 177,936 | 2 | ✅ |
| **Total** | **-** | **1,182,079** | **9** | **✅** |

---

## Final Verification

### Aave Position Data

```
getUserAccountData(0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266):
  totalCollateralBase: 0
  totalDebtBase: 0
  availableBorrowsBase: 0
  currentLiquidationThreshold: 0
  ltv: 0
  healthFactor: MAX (no debt)
```

**Result**: Position fully closed, no debt remaining.

### Token Balances

| Token | Initial | Final | Change |
|-------|---------|-------|--------|
| ETH | 100.0 ETH | 98.998 ETH | -1.002 ETH (gas) |
| WETH | 1.0 WETH | 0.998 WETH | -0.002 WETH (swapped) |
| USDC | 10,000 USDC | 10,003.98 USDC | +3.98 USDC (from WETH swap) |

**Net Result**: Strategy converted 0.002 WETH to 3.98 USDC through the borrow cycle.

---

## Performance Metrics

| Metric | Value |
|--------|-------|
| Total execution time | ~7 minutes |
| Time to HOLD | ~54 seconds |
| Teardown execution time | ~13 seconds |
| Total gas used | 1,182,079 |
| Number of transactions | 9 |
| Average gas per tx | 131,342 |
| State transitions | IDLE → SUPPLIED → BORROWED → HOLD → TEARDOWN → CLOSED |

---

## Notable Events

1. **Successful WETH Wrapping**: ETH wrapped to WETH via deposit() call.

2. **Storage Slot Funding**: USDC balance set via `anvil_setStorageAt` on slot 9.

3. **Accurate Debt Calculation**: REPAY intent correctly calculated debt + interest (2.000001 USDC).

4. **Yield on Collateral**: Withdrew slightly more WETH than supplied (0.002000000117528454 vs 0.002).

5. **Nonce Recovery**: WITHDRAW intent encountered nonce error on first attempt, successfully retried with corrected nonce.

6. **Clean Shutdown**: Strategy detected teardown signal and executed all teardown intents before shutting down.

---

## Issues Encountered

### Minor: Nonce Error on WITHDRAW

**Error**: `{'code': -32003, 'message': 'nonce too low'}`

**Resolution**: Intent state machine automatically retried with exponential backoff (1.09s delay), succeeded on retry.

**Impact**: None - built-in retry mechanism handled gracefully.

---

## Conclusion

**PASS** - Full E2E lifecycle completed successfully: setup → execute → hold → teardown → verified closed.

The aave_borrow strategy demonstrated:
- Correct Aave V3 integration for supply and borrow operations
- Proper state management across execution and teardown phases
- Accurate debt tracking and full repayment
- Clean position closure with all assets returned to wallet
- Robust error handling (nonce retry)
- Complete teardown sequence with final SWAP to convert collateral

All success criteria met:
- [x] Anvil and Gateway started successfully
- [x] Strategy executed initial intents (SUPPLY, BORROW)
- [x] Strategy reached HOLD state
- [x] Teardown signal was accepted
- [x] Teardown intents executed successfully (REPAY, WITHDRAW, SWAP)
- [x] On-chain verification shows position closed

**Test Status**: PASS ✅
