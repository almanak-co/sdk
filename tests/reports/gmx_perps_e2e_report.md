# E2E Strategy Test Report: gmx_perps

**Date:** 2026-02-06 14:07
**Result:** BLOCKED (GMX V2 Anvil limitations)
**Duration:** 25 minutes

---

## Summary

The gmx_perps demo strategy E2E test on Arbitrum Anvil fork encountered known limitations with GMX V2's asynchronous order execution architecture. The strategy intent logic works correctly, but order creation fails due to GMX V2 SDK ABI encoding issues with the current contract version.

**Key Finding:** GMX V2 perpetual futures require keeper execution which does not function on Anvil forks. Even if order creation succeeded, positions would not open without simulating keeper execution.

---

## Configuration

| Field | Value |
|-------|-------|
| Strategy | gmx_perps |
| Chain | Arbitrum (42161) |
| Network | Anvil fork |
| Port | 8545 |
| Market | ETH/USD |
| Collateral Token | WETH |
| Collateral Amount | 0.0005 WETH |
| Leverage | 2.0x |
| Position Direction | LONG |
| Max Slippage | 2.0% |
| Hold Duration | 5 minutes |

---

## Lifecycle Phases

### Phase 1: Setup
- [x] Anvil started on port 8545 (Arbitrum fork, block ~428952301)
- [x] Gateway started on port 50051
- [x] Wallet funded:
  - ~10,000 ETH (gas + execution fees)
  - 0.5 WETH (collateral)

### Phase 2: Strategy Execution
- [x] Strategy loaded and initialized correctly
- [x] Decision logic executed:
  - Detected no open position
  - Created PERP_OPEN intent: LONG ETH/USD, $1.85 size (2.0x leverage)
- [x] Intent compiled to GMX V2 multicall transaction
- [ ] **FAILED**: Order creation reverted with `EmptyAccount()` error

### Phase 3: Teardown
- [ ] Not executed (blocked by Phase 2 failure)

### Phase 4: Verification
- [ ] Not applicable

---

## Technical Analysis

### GMX V2 Architecture Challenges

1. **Asynchronous Order Execution**: GMX V2 uses an order-based system where:
   - User submits order via `ExchangeRouter.createOrder()`
   - Order is stored in GMX's order book
   - Keepers execute the order asynchronously (typically within seconds on mainnet)
   - On Anvil forks, keepers don't run - orders remain pending forever

2. **Order Creation Failure**:
   - Error: `EmptyAccount()` (selector: `0xdd7016a2`)
   - Indicates GMX's internal validation found an invalid/empty account address
   - The SDK's ABI encoding may not match the current GMX V2 contract structure

3. **SDK Updates Required**:
   - Fixed `timezone.utc` → `UTC` import issue in strategy.py
   - Added `sendWnt` call for execution fee in SDK multicall
   - Further ABI encoding fixes needed for CreateOrderParams struct

### Relevant Contract Addresses

| Contract | Address |
|----------|---------|
| ExchangeRouter | 0x1C3fa76e6E1088bCE750f23a5BFcffa1efEF6A41 |
| OrderVault | 0x31eF83a530Fde1B38EE9A18093A333D8Bbbc40D5 |
| ETH/USD Market | 0x70d95587d40A2caf56bd97485aB3Eec10Bee6336 |
| DataStore | 0xFD70de6b91282D8017aA4E741e9Ae325CAb992d8 |
| WETH | 0x82aF49447D8a07e3bd95BD0d56f35241523fBab1 |

---

## Bugs Fixed During Testing

### 1. Strategy timezone.utc Import Error
**File:** `strategies/demo/gmx_perps/strategy.py`
**Issue:** Used `timezone.utc` but only imported `UTC` from datetime
**Fix:** Changed all `datetime.now(timezone.utc)` to `datetime.now(UTC)`
**Lines affected:** 294, 370, 438

### 2. GMX V2 SDK Missing Execution Fee sendWnt
**File:** `almanak/framework/connectors/gmx_v2/sdk.py`
**Issue:** Multicall didn't include `sendWnt` for execution fee to OrderVault
**Fix:** Added separate `sendWnt` call for execution fee before collateral sendWnt
**Impact:** Both increase and decrease order builders updated

---

## Recommendations

1. **GMX V2 SDK Refactor**: The current SDK needs significant updates to match GMX V2's latest contract interface:
   - Review GMX V2's official TypeScript SDK for correct parameter encoding
   - Test against GMX V2 subgraph to verify order parameter formats
   - Consider using GMX's official keepers simulation for testing

2. **Alternative Testing Approach**: For GMX V2 testing on Anvil:
   - Test order creation only (transaction submission)
   - Use GMX's testnet contracts (Arbitrum Sepolia) with real keepers
   - Or simulate keeper execution by calling `OrderHandler.executeOrder()` with keeper role

3. **Manual Testing Required**: Full E2E testing of GMX V2 perpetuals should be done on:
   - Arbitrum Sepolia testnet with small positions
   - Or mainnet with minimal amounts after SDK fixes

---

## Transaction Attempts

| Attempt | TX Hash | Result | Error |
|---------|---------|--------|-------|
| 1 | c520cb8d43... | Reverted | InsufficientWntAmountForExecutionFee |
| 2 | ddc9b66d52... | Reverted | EmptyAccount() |
| 3 | 2188afefd5... | Reverted | EmptyAccount() |
| 4 | eae1da1f52... | Reverted | EmptyAccount() |

---

## Conclusion

The gmx_perps strategy correctly implements the intent logic for GMX V2 perpetual futures, but the SDK's transaction encoding requires updates to work with the current GMX V2 contracts on Arbitrum. This is a known integration complexity with GMX V2's sophisticated order system.

**Status: BLOCKED** - Pending GMX V2 SDK fixes for production-ready E2E testing.

---

*Generated by Almanak Strategy Testing Framework v2*
