# E2E Test Report: aerodrome_lp (Base)

## Test Summary

| Field | Value |
|-------|-------|
| Strategy | `aerodrome_lp` |
| Chain | Base |
| Network | Anvil (local fork) |
| Anvil Port | 8548 |
| Test Date | 2026-02-06 |
| Status | **PASS** |

## Test Configuration

```json
{
  "pool": "WETH/USDC",
  "stable": false,
  "amount0": "0.001",
  "amount1": "0.04",
  "chain": "base",
  "network": "anvil"
}
```

## Token Addresses (Base)

| Token | Address |
|-------|---------|
| WETH | `0x4200000000000000000000000000000000000006` |
| USDC | `0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913` |
| Aerodrome WETH/USDC Pool | `0xcDAC0d6c6C59727a65F871236188350531885C43` |
| Aerodrome Factory | `0x420DD381b31aEf6683db6B902084cB0FFECe40Da` |
| Aerodrome Router | `0xcF77a3Ba9A5CA399B7c97c74d54e5b1Beb874E43` |

## Test Execution

### Setup

1. Started Anvil fork for Base on port 8548
2. Started Gateway with Base/Anvil configuration
3. Funded wallet with:
   - 0.5 WETH (deposited from ETH)
   - 500 USDC (via storage slot 9)

### Intent Execution Results

#### LP_OPEN (Run 1)

| Metric | Value |
|--------|-------|
| Intent | LP_OPEN |
| Pool | WETH/USDC/volatile |
| Amount0 | 0.001 WETH |
| Amount1 | 0.04 USDC |
| Transactions | 3 (approve WETH, approve USDC, addLiquidity) |
| Gas Used | 342,140 |
| Duration | 5,051ms |
| Result | SUCCESS |

#### LP_OPEN (Run 2)

| Metric | Value |
|--------|-------|
| Intent | LP_OPEN |
| Pool | WETH/USDC/volatile |
| Amount0 | 0.001 WETH |
| Amount1 | 0.04 USDC |
| Transactions | 3 |
| Gas Used | 242,881 |
| Duration | 1,175ms |
| Result | SUCCESS |

Note: Second LP_OPEN used less gas (242k vs 342k) because tokens were already approved.

#### LP_CLOSE (Teardown)

| Metric | Value |
|--------|-------|
| Intent | LP_CLOSE |
| Pool | WETH/USDC/volatile |
| LP Tokens Removed | 1,824,627,260 wei |
| Transactions | 2 (approve LP, removeLiquidity) |
| Gas Used | 228,125 |
| Duration | 2,343ms |
| Result | SUCCESS |

### On-Chain Verification

#### Before LP_CLOSE
- LP Token Balance: 1,824,627,260

#### After LP_CLOSE
- LP Token Balance: 0 (fully withdrawn)
- WETH Balance: 499,999,999,999,954,824 (~0.5 WETH)
- USDC Balance: 499,999,999 (~500 USDC)

## Total Gas Used

| Phase | Gas |
|-------|-----|
| LP_OPEN (x2) | 585,021 |
| LP_CLOSE | 228,125 |
| **Total** | **813,146** |

## Lifecycle Flow

```text
┌─────────────────────────────────────────────────────────┐
│                    AERODROME LP E2E                      │
├─────────────────────────────────────────────────────────┤
│  1. LP_OPEN                                              │
│     └─> Deposit 0.001 WETH + 0.04 USDC                  │
│     └─> Receive LP tokens (1,824,627,260 wei)           │
│     └─> 342,140 gas (first), 242,881 gas (second)       │
├─────────────────────────────────────────────────────────┤
│  2. HOLD (State monitoring)                              │
│     └─> Strategy monitors position                       │
│     └─> Note: Strategy uses in-memory state tracking    │
├─────────────────────────────────────────────────────────┤
│  3. LP_CLOSE (via force_action=close)                   │
│     └─> Remove all LP tokens                             │
│     └─> Return WETH + USDC to wallet                     │
│     └─> 228,125 gas                                      │
└─────────────────────────────────────────────────────────┘
```

## Notes

1. **Environment Variable Required**: LP_CLOSE requires `ALMANAK_BASE_RPC_URL` environment variable to be set to query pool address from factory. Without this, the SDK cannot find the pool.

2. **Port Configuration**: Base chain uses port 8548 in the gateway's default ANVIL_CHAIN_PORTS mapping (not 8547 as mentioned in some documentation).

3. **Storage Slot**: USDC on Base uses storage slot 9 for balances (same as Arbitrum).

4. **State Management**: The aerodrome_lp demo strategy uses in-memory state tracking (`_has_position`), which resets on each run. To test teardown, use `force_action: close` in config.

5. **Gas Optimization**: Second LP_OPEN is cheaper because token approvals are already in place.

## Recommendations

- Document the `ALMANAK_BASE_RPC_URL` requirement for Aerodrome LP_CLOSE operations
- Consider persisting position state for proper teardown flow
- Add port 8548 for Base to PRD documentation

## Conclusion

The aerodrome_lp strategy successfully completes the full LP lifecycle on Base Anvil fork:
- LP_OPEN: Adds liquidity to Aerodrome WETH/USDC volatile pool
- LP_CLOSE: Removes all liquidity and returns tokens to wallet

All acceptance criteria have been met.
