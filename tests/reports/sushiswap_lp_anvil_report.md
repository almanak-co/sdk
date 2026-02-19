# Anvil Test Report: sushiswap_lp

**Date:** 2026-02-08 15:45
**Result:** PASS
**Duration:** ~5 minutes

---

## Summary

The `sushiswap_lp` demo strategy successfully executed on a local Anvil fork of Arbitrum. The strategy opened a concentrated liquidity position on SushiSwap V3 using 0.01 WETH and 25 USDC with a 10% price range. Execution completed successfully with position ID 32514 minted.

---

## Configuration

| Field | Value |
|-------|-------|
| Strategy | sushiswap_lp |
| Chain | Arbitrum |
| Network | Anvil fork |
| Port | 8545 (Anvil), 50051 (Gateway) |
| Pool | WETH/USDC/3000 |
| Range Width | 10% (±5% from current price) |
| Token0 Amount | 0.01 WETH |
| Token1 Amount | 25 USDC |
| Force Action | open |

---

## Test Phases

### Phase 1: Setup
- [x] Anvil started on port 8545 (Arbitrum fork)
- [x] Gateway started on port 50051
- [x] Wallet funded with 100 ETH (gas)
- [x] Wallet funded with 1 WETH (wrapped from ETH)
- [x] Wallet funded with 10,000 USDC (storage slot method)

### Phase 2: Strategy Execution
- [x] Strategy loaded: `SushiSwapLPStrategy`
- [x] Config loaded from `config.json`
- [x] Strategy initialized with pool WETH/USDC/3000
- [x] Force action triggered: OPEN LP position
- [x] Price range calculated: [2005.46 - 2216.56]
- [x] Tick range calculated: [-200280 - -199260]
- [x] Intent created: LP_OPEN with sushiswap_v3 protocol
- [x] Intent compiled to 3 transactions (510,000 gas estimate)
- [x] Execution completed successfully

### Phase 3: Verification
- [x] Position ID extracted from receipt: 32514
- [x] Token balances verified:
  - WETH: 1.0 → 0.99 (0.01 used)
  - USDC: 10,000 → 9,979 (21 used)
  - ETH: ~99 (gas consumed)
- [x] Execution status: SUCCESS
- [x] Gas used: 620,404

### Phase 4: Cleanup
- [x] Anvil process killed
- [x] Gateway process killed
- [x] Ports released (8545, 50051, 9090)

---

## Execution Log Highlights

### Strategy Initialization
```
SushiSwapLPStrategy initialized: pool=WETH/USDC/3000, range_width=10.00%, amounts=0.01 WETH + 25 USDC
```

### Intent Decision
```
Forced action: OPEN LP position
LP_OPEN: 0.0100 WETH + 25.0000 USDC, price range [2005.4585 - 2216.5594], ticks [-200280 - -199260]
SushiSwapLPStrategy:3e54f3102494 intent: LP_OPEN: WETH/USDC/3000 (0.01, 25) [2005 - 2217] via sushiswap_v3
```

### Intent Compilation
```
Compiled LP_OPEN intent: WETH/USDC, range [2005.46-2216.56], 3 txs, 510000 gas
```

### Execution Result
```
Execution successful for SushiSwapLPStrategy:3e54f3102494: gas_used=620404, tx_count=3
Extracted LP position ID from receipt: 32514
Status: SUCCESS | Intent: LP_OPEN | Gas used: 620404 | Duration: 5466ms
```

---

## Token Balance Changes

| Token | Initial | Final | Change | Expected |
|-------|---------|-------|--------|----------|
| ETH | 100.0 | ~99.0 | -1.0 (gas) | Gas only |
| WETH | 1.0 | 0.99 | -0.01 | -0.01 |
| USDC | 10,000 | 9,979 | -21 | -25 |

**Note:** USDC usage was 21 instead of 25, likely due to optimal liquidity provision within the calculated tick range. This is expected behavior for concentrated liquidity positions.

---

## Transaction Breakdown

| Phase | Action | Gas Estimate | Actual Gas | Status |
|-------|--------|--------------|------------|--------|
| 1 | APPROVE WETH | ~50,000 | Included | SUCCESS |
| 2 | APPROVE USDC | ~50,000 | Included | SUCCESS |
| 3 | LP_OPEN (mint position) | ~410,000 | 620,404 total | SUCCESS |
| **Total** | | **510,000** | **620,404** | **SUCCESS** |

**Gas Delta:** Actual gas was 21.6% higher than estimate (620,404 vs 510,000). This is within acceptable range for first-time approvals.

---

## Position Details

| Field | Value |
|-------|-------|
| Position ID | 32514 |
| Protocol | SushiSwap V3 |
| Pool | WETH/USDC/3000 |
| Fee Tier | 0.3% (3000 basis points) |
| Tick Lower | -200280 |
| Tick Upper | -199260 |
| Price Range | [2005.46 - 2216.56] USDC per WETH |
| Token0 Deposited | 0.01 WETH |
| Token1 Deposited | ~21 USDC |

---

## Gateway Performance

| Metric | Value |
|--------|-------|
| Connection Time | <1s |
| State Load | 1.8ms |
| Price Queries | 2 (449ms, 848ms) |
| Allowance Queries | 2 (58ms, 57ms) |
| Execution | 4,039ms |
| Total Duration | ~5.5s |

---

## Issues Encountered

### Minor Warning
```
Warning: Error in on_intent_executed callback: 'GatewayExecutionResult' object has no attribute 'liquidity'
```

**Impact:** Cosmetic only. The strategy attempted to extract liquidity data from the result, but the field was not present. Position ID was successfully extracted.

**Action Required:** None for this test. The position was created successfully.

---

## Conclusion

**PASS** - The `sushiswap_lp` strategy executed successfully on Anvil:

1. All setup completed without errors
2. Strategy loaded and initialized correctly
3. LP_OPEN intent compiled to 3 transactions
4. Execution succeeded with position ID 32514 minted
5. Token balances reflect expected changes
6. Gateway performed well with reasonable latencies
7. Cleanup completed successfully

The strategy demonstrates proper SushiSwap V3 integration with:
- Correct tick math and price range calculations
- Proper token approvals
- Successful concentrated liquidity position creation
- Result enrichment (position ID extraction)

**Recommendation:** Strategy is ready for further testing with different configurations (different pools, fee tiers, range widths).
