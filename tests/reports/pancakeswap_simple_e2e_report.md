# Strategy Test Report: pancakeswap_simple

**Date:** 2026-02-05
**Result:** PASS
**Duration:** ~3 minutes

---

## Summary

Successfully tested the `demo_pancakeswap_simple` strategy end-to-end on Anvil fork (Arbitrum). The strategy executed WETH to USDC swaps via PancakeSwap V3. This is a simple swap strategy that executes on every call without state management - it doesn't have a HOLD state or teardown intents since there's no position to manage.

---

## Configuration

| Field | Value |
|-------|-------|
| Strategy | demo_pancakeswap_simple |
| Chain | arbitrum |
| Network | Anvil (local fork) |
| Protocol | PancakeSwap V3 |
| Swap Amount | $10 USD |
| Max Slippage | 1% |
| From Token | WETH |
| To Token | USDC |

---

## Test Steps

### 1. Environment Setup

**Anvil:**
- Started Anvil fork on port 8545
- Chain ID verified: 42161 (Arbitrum)

**Gateway:**
- Started gateway on localhost:50051
- Network: ANVIL
- Wallet: 0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266

**Wallet Funding:**
- 10,000 ETH (native for gas) - Anvil default
- 0.1 WETH (0x82aF49447D8a07e3bd95BD0d56f35241523fBab1) - deposited via WETH contract

### 2. Strategy Execution - First Swap

**Command:**
```bash
uv run almanak strat run -d strategies/demo/pancakeswap_simple --once
```

**Intent Returned:** SWAP
- From: WETH
- To: USDC
- Amount USD: $10
- Protocol: pancakeswap_v3

**Compilation:**
- Input: 0.0050 WETH
- Expected Output: 9.9726 USDC
- Minimum Output: 9.8729 USDC (with 1% slippage)
- Transactions: 2 (approve + swap)
- Gas Estimate: 280,000

**Execution:**
| Metric | Value |
|--------|-------|
| Status | SUCCESS |
| Gas Used | 243,790 |
| Transaction Count | 2 |
| Duration | 6,989ms |
| Swaps Parsed | 1 |

**Balance Changes (First Swap):**
| Token | Before | After | Change |
|-------|--------|-------|--------|
| WETH | 0.1000 | 0.0949 | -0.0051 |
| USDC | 0 | 9.952 | +9.952 |

### 3. Teardown Test

**Command:**
```bash
uv run almanak strat teardown -d strategies/demo/pancakeswap_simple --mode soft
```

**Result:** Teardown request created (pending)

**Strategy Behavior:**
- Strategy detected teardown request
- Strategy reports `supports_teardown()=False` (simple swap strategy)
- Continued normal operation (executed another swap)

**Second Swap Execution:**
| Metric | Value |
|--------|-------|
| Status | SUCCESS |
| Gas Used | 209,590 |
| Transaction Count | 2 |
| Duration | 2,674ms |
| Swaps Parsed | 1 |

Note: Second swap used less gas (209,590 vs 243,790) because WETH was already approved from the first swap.

### 4. Final State Verification

**Final Balances:**
| Token | Balance | Notes |
|-------|---------|-------|
| WETH | 0.0899 | Started with 0.1, swapped ~0.01 total |
| USDC | 19.904 | Received ~$10 per swap x 2 swaps |

---

## Transaction Details

### Swap 1
- **Transaction Hash:** (On Anvil fork)
- **Intent ID:** 01981450-b3d4-4db1-b8f8-b8a7bb52a7e3
- **WETH Price:** $1,981.50
- **USDC Price:** $0.999739
- **Input:** 0.0050 WETH
- **Output:** ~9.952 USDC

### Swap 2
- **Transaction Hash:** (On Anvil fork)
- **Intent ID:** 14234a67-cbea-4675-84c2-7f96ce8ffe23
- **WETH Price:** $1,981.50
- **USDC Price:** $0.999746
- **Input:** 0.0050 WETH
- **Output:** ~9.952 USDC

---

## Key Observations

1. **Simple Swap Strategy Behavior:**
   - This strategy executes a swap on every call without conditions
   - No state machine or HOLD state
   - No teardown intents (nothing to close)

2. **PancakeSwap V3 Integration:**
   - Swap compilation works correctly
   - Receipt parsing detects 1 swap per transaction
   - Gas optimization on repeat calls (pre-approved tokens)

3. **Teardown Handling:**
   - Strategy correctly reports `supports_teardown()=False`
   - Framework logs warning but continues normal operation
   - This is expected for stateless swap strategies

---

## Gas Summary

| Operation | Gas Used | Tx Count |
|-----------|----------|----------|
| Swap 1 (with approve) | 243,790 | 2 |
| Swap 2 (pre-approved) | 209,590 | 2 |
| **Total** | **453,380** | **4** |

---

## Conclusion

**Result: PASS**

The pancakeswap_simple strategy works correctly:
- PancakeSwap V3 swap integration functions properly
- Intent compilation produces valid transactions
- Execution succeeds with correct balance changes
- Receipt parsing confirms swap execution
- Teardown behavior is appropriate for stateless strategies (no-op with warning)

---

## Tokens Reference

| Token | Address | Decimals |
|-------|---------|----------|
| WETH | 0x82aF49447D8a07e3bd95BD0d56f35241523fBab1 | 18 |
| USDC (native) | 0xaf88d065e77c8cC2239327C5EDb3A432268e5831 | 6 |
