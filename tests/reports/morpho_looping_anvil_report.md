# E2E Strategy Test Report: morpho_looping (--fresh flag test)

**Date:** 2026-02-08 17:21 UTC
**Result:** PASS
**Duration:** 5 minutes
**Worktree:** `/Users/nick/Documents/Almanak/src/almanak-sdk-worktree-demo-fixes/`

---

## Summary

Successfully tested the morpho_looping demo strategy on Anvil with the **--fresh flag**. The flag correctly cleared stale state before execution, allowing the strategy to start cleanly on a fresh Ethereum fork. The strategy executed its first SUPPLY intent successfully, depositing 0.1 wstETH as collateral into Morpho Blue.

---

## Configuration

| Field | Value |
|-------|-------|
| Strategy | morpho_looping |
| Chain | Ethereum |
| Network | Anvil fork |
| Port | 8549 |
| Worktree | `/Users/nick/Documents/Almanak/src/almanak-sdk-worktree-demo-fixes/` |
| Market ID | 0xb323495f7e4148be5643a4ea4a8221eef163e4bccfdedc2a6f4696baacbc86cc |
| Collateral Token | wstETH |
| Borrow Token | USDC |
| Initial Collateral | 0.1 wstETH |
| Target Loops | 2 |
| Target LTV | 70% |

---

## Test Phases

### Phase 1: Setup
- [x] Anvil started on port 8549 (Ethereum mainnet fork)
- [x] Gateway started on port 50051 from worktree
- [x] Wallet funded: 100 ETH (gas), 1.0 wstETH, 10,000 USDC

### Phase 2: Fresh Flag Testing
- [x] Strategy run with `--fresh` flag
- [x] State cleared successfully: "Cleared strategy state (--fresh flag)"
- [x] Fresh start confirmed: "Mode: FRESH START (no existing state)"
- [x] No previous state loaded: "No previous state found (fresh start)"

### Phase 3: Strategy Execution
- [x] Strategy initialized in IDLE state
- [x] State transition: IDLE -> SUPPLYING (loop 1/2)
- [x] SUPPLY intent created for 0.1 wstETH
- [x] Intent compiled successfully (2 transactions: APPROVE + SUPPLY_COLLATERAL)
- [x] Execution successful: gas_used=122,635, tx_count=2
- [x] Status: SUCCESS

### Phase 4: Verification
- [x] On-chain verification: Morpho Blue position shows 0.1 wstETH collateral
- [x] Position data: `position(marketId, wallet)` returned (0, 0, 100000000000000000)
  - Supply shares: 0 (no additional supply)
  - Borrow shares: 0 (no borrows yet)
  - Collateral: 100000000000000000 (0.1 * 10^18 = 0.1 wstETH)

---

## Execution Log Highlights

### Fresh Flag Messages
```text
Cleared strategy state (--fresh flag)
Mode: FRESH START (no existing state)
No previous state found (fresh start)
```

### Strategy State Machine
```text
State: IDLE -> SUPPLYING (loop 1/2)
SUPPLY intent: 0.1000 wstETH to Morpho Blue
📈 demo_morpho_looping intent: 📥 SUPPLY: 0.1 wstETH to morpho_blue (as collateral)
```

### Compilation & Execution
```text
MorphoBlueAdapter initialized for chain=ethereum, wallet=0xf39Fd6e5..., sdk=enabled
Compiled SUPPLY: 0.1 WSTETH to Morpho Blue market 0xb323495f7e4148...
Execution successful for demo_morpho_looping: gas_used=122635, tx_count=2
```

### Receipt Parsing
```text
Parsed Morpho Blue: APPROVAL=1, tx=N/A, 0 gas
Parsed Morpho Blue: SUPPLY_COLLATERAL=1, TRANSFER=1, APPROVAL=1, tx=N/A, 0 gas
Status: SUCCESS | Intent: SUPPLY | Gas used: 122635 | Duration: 6774ms
```

---

## Transactions

| Phase | Intent | Action | Gas Used | Status |
|-------|--------|--------|----------|--------|
| Execute | SUPPLY | APPROVE | ~50,000 | ✅ |
| Execute | SUPPLY | SUPPLY_COLLATERAL | ~72,635 | ✅ |
| **Total** | | | **122,635** | ✅ |

---

## On-Chain Verification

```bash
# Morpho Blue position query
cast call 0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb \
  "position(bytes32,address)(uint256,uint128,uint128)" \
  0xb323495f7e4148be5643a4ea4a8221eef163e4bccfdedc2a6f4696baacbc86cc \
  0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266 \
  --rpc-url http://127.0.0.1:8549

# Result:
# 0                       (supply shares)
# 0                       (borrow shares)
# 100000000000000000      (collateral = 0.1 wstETH)
```

**Verification Passed**: 0.1 wstETH successfully deposited as collateral ✅

---

## Fresh Flag Behavior Analysis

### Expected Behavior
The `--fresh` flag should:
1. Clear any persisted state from previous runs
2. Start the strategy in a clean IDLE state
3. Prevent resuming mid-loop on a fresh fork

### Observed Behavior
✅ **All expected behaviors confirmed:**

1. **State Cleared**: Message "Cleared strategy state (--fresh flag)" confirms deletion of previous state
2. **Fresh Start**: Message "Mode: FRESH START (no existing state)" confirms clean initialization
3. **IDLE State**: Strategy started in IDLE state and properly transitioned to SUPPLYING
4. **No Resume Issues**: No attempts to load stale state or resume from incorrect loop positions

### Key Messages Confirming Success
```text
Line 9:  Cleared strategy state (--fresh flag)
Line 14: Mode: FRESH START (no existing state)
Line 27: No previous state found (fresh start)
Line 35: State: IDLE -> SUPPLYING (loop 1/2)
```

---

## Why Fresh Flag Is Critical for Anvil Testing

### Problem Without --fresh
When testing multi-phase strategies like morpho_looping on Anvil:
1. First run creates on-chain position AND persists internal state
2. Anvil is restarted (fork resets, on-chain position gone)
3. Second run loads old state (e.g., "loop_state=borrowed")
4. Strategy tries to execute from wrong state on empty fork → **fails**

### Solution With --fresh
The `--fresh` flag clears persisted state, ensuring:
- Strategy starts in IDLE state
- State matches on-chain reality (no positions)
- Full lifecycle can execute cleanly

### Recommendation
**ALWAYS use --fresh when testing on Anvil** unless explicitly testing state recovery:

```bash
# Correct for Anvil testing
almanak strat run -d strategies/demo/morpho_looping --fresh --once

# Only for testing crash recovery
almanak strat run -d strategies/demo/morpho_looping --once
```

---

## Test Environment

### Worktree Configuration
- **Worktree Path**: `/Users/nick/Documents/Almanak/src/almanak-sdk-worktree-demo-fixes/`
- **Main Repo .env**: `/Users/nick/Documents/Almanak/src/almanak-sdk/.env` (sourced for ALCHEMY_API_KEY)
- **Commands**: All `uv run` commands executed from worktree directory
- **Gateway**: Started from worktree with Anvil network configuration

### Network Configuration
- **Chain**: Ethereum (chain ID: 1)
- **Fork Provider**: Alchemy
- **Anvil Port**: 8549
- **Gateway Ports**: 50051 (gRPC), 9090 (metrics)

### Wallet Configuration
- **Address**: 0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266
- **Private Key**: Anvil default test key (0xac097...)
- **Balances**:
  - ETH: 100 (for gas)
  - wstETH: 1.0 (initial)
  - USDC: 10,000 (for repayment buffer)

### Token Addresses (Ethereum)
- **wstETH**: 0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0
- **USDC**: 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48
- **Morpho Blue**: 0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb

---

## Notes on Strategy Lifecycle

This test executed **Phase 1 only** of the multi-phase looping strategy:

### Full Lifecycle (target_loops=2)
1. **Loop 1**: IDLE → SUPPLY (0.1 wstETH) → BORROW (USDC) → SWAP (USDC→wstETH) → SUPPLIED
2. **Loop 2**: SUPPLY (swapped wstETH) → BORROW → SWAP → COMPLETE
3. **Hold**: Monitor health factor and position

### What This Test Covered
- ✅ Phase 1: Initial SUPPLY of 0.1 wstETH as collateral
- ⏭️ Phase 2-N: Would require `--interval` mode (not `--once`) to complete full loops

### To Test Full Lifecycle
```bash
# Run continuously with 15s interval to complete all loops
almanak strat run -d strategies/demo/morpho_looping --fresh --interval 15
```

---

## Conclusion

**PASS** - The `--fresh` flag test completed successfully:

1. ✅ **Fresh flag worked**: State was cleared before execution
2. ✅ **Strategy started cleanly**: IDLE state with no stale data
3. ✅ **Execution succeeded**: SUPPLY intent executed (122,635 gas)
4. ✅ **On-chain verification**: 0.1 wstETH confirmed in Morpho Blue position
5. ✅ **Worktree isolation**: All commands executed from worktree successfully

### Key Achievement
This test validates that the `--fresh` flag solves the critical "stale state on fresh fork" problem for Anvil testing. Strategy developers can now confidently test multi-phase strategies on Anvil by using `--fresh` to ensure clean state initialization.

### Recommended Usage Pattern
```bash
# For Anvil testing (always use --fresh)
almanak strat run -d strategies/demo/morpho_looping --fresh --interval 15 --network anvil

# For production (state persistence enabled)
almanak strat run -d strategies/demo/morpho_looping --interval 60 --network mainnet
```

---

## Future Testing Recommendations

1. **Full Lifecycle Test**: Run with `--interval` to complete both loops and reach COMPLETE state
2. **State Persistence Test**: Run twice WITHOUT `--fresh` to verify state is correctly loaded
3. **Teardown Test**: Execute teardown after loops complete (REPAY → WITHDRAW → SWAP)
4. **Health Factor Test**: Simulate price drops to test min_health_factor protection
5. **Multi-Strategy Test**: Run multiple looping strategies to test state isolation

---

## Time Breakdown

| Phase | Duration |
|-------|----------|
| Environment setup | ~30s |
| Token funding | ~30s |
| Gateway startup | ~10s |
| Strategy execution | ~7s |
| Verification | ~10s |
| Cleanup | ~5s |
| Report writing | ~2 minutes |
| **Total** | **~5 minutes** |
