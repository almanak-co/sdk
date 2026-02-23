# E2E Strategy Test Report: bb_perps (Anvil)

**Date:** 2026-02-20 07:10
**Result:** FAIL
**Mode:** Anvil
**Duration:** ~7 minutes (two runs)

## Configuration

| Field | Value |
|-------|-------|
| Strategy | BBPerpsStrategy (demo_bb_perps) |
| Chain | arbitrum |
| Network | Anvil fork (managed, port auto-assigned ~50660) |
| Protocol | gmx_v2 |
| Market | ETH/USD |

## Config Changes Made

| Field | Before | After | Reason |
|-------|--------|-------|--------|
| `anvil_funding.ETH` | (absent) | 100 | Required for gas + GMX V2 keeper fee (~0.002 ETH) |
| `anvil_funding.WETH` | (absent) | 1 | Required collateral token for PERP_OPEN |

**Budget check:** `collateral_amount = 0.0005 WETH` at ETH ~$1,957 = ~$0.98 collateral, $2.94 position size at 3x leverage. Well under the $50 cap. No change needed.

**`force_action`** was already set to `"long"` in the original config — no modification required.

## Execution

### Run 1 (without anvil_funding)

The CLI auto-starts a managed Anvil fork on a dynamic port. The wallet on that fork had zero
WETH. The manually pre-funded Anvil on port 8545 was not used by the strategy runner.

- WETH balance on managed fork: 0.000000 (need 0.000500)
- Diagnostic: "Insufficient balance for: WETH"
- Result: PERP_OPEN reverted (balance error)

### Run 2 (with anvil_funding added to config.json)

The managed gateway correctly funded the wallet:
- ETH: 99.998815 (need ~0.0015)
- WETH: 1.000000 (need 0.000500)

Strategy decision path:
- ETH price fetched: $1,957.53
- `force_action: long` triggered immediately (bypassed BB signal logic)
- PERP_OPEN intent created: LONG ETH/USD, $2.94 size, 3.0x leverage via gmx_v2
- Intent compiled: 1 tx, 3,900,000 gas

Execution attempts (3 retries exhausted):

| Attempt | TX Hash | Status |
|---------|---------|--------|
| 0 | `0b221e1675723f4c1f7486643f4bda03e4d3d6aa9e6e6693938a80de99f9a750` | REVERTED |
| 1 | `11edfc553bc474f7a1f14e2e45bc3a943a85c6433fdb1a6afd9d04578ef99e1b` | REVERTED |
| 2 | `4891f430ee880b5d2c523a1d8c7687fa7857cb18628c9e03e0be5203972488b1` | REVERTED |
| 3 | `c3830300b99ce498be70890f76536e3365116b00976fd974b035f5fe30f77666` | REVERTED |

Post-revert diagnostic confirmed balances were sufficient:
```
Native ETH: 99.998815 (need 0.001500) -- OK
WETH: 1.000000 (need 0.000500) -- OK
Likely Cause: Unknown - balances appear sufficient
Suggestions: Check token approvals, Verify contract parameters
```

## Root Cause Analysis

The GMX V2 `ExchangeRouter.createOrder()` call reverts on an Anvil fork. This is a known
limitation of testing GMX V2 perp orders against a forked chain:

GMX V2 market orders require a **keeper/oracle execution step**. The `createOrder` call itself
places an order in GMX's order book, then a separate keeper bot reads the oracle price and
executes the order. On a live fork of Arbitrum mainnet, the GMX contracts are present and the
`createOrder` transaction should normally succeed (it just creates the order). However, the
revert here suggests either:

1. **Oracle price validation**: GMX V2's `createOrder` calls `validateOracleBlockNumbers()` which
   checks that the oracle price is recent. On Anvil, block timestamps may be stale or the oracle
   contract state is inconsistent with what the strategy sends.
2. **Token approval**: WETH approval to GMX's `Router`/`ExchangeRouter` may not be in place.
   The GMX adapter should handle approval, but the approval tx may also be reverting silently
   given the same "Unknown" revert reason pattern.

The revert reason is `Unknown` in all 4 attempts, suggesting the failure happens deep in the
GMX contract call stack and Anvil is not decoding the custom error selector.

## Errors Encountered

| Error | Details |
|-------|---------|
| Run 1: Insufficient WETH balance | Managed Anvil fork had no WETH; config lacked `anvil_funding` |
| Run 2: GMX V2 PERP_OPEN revert (x4) | All 4 attempts reverted with `Unknown` reason despite sufficient balances |

## On-Chain Transactions

Transactions were submitted to the Anvil fork (not real mainnet). All reverted.

| TX Hash | Status |
|---------|--------|
| `0b221e16...a750` | REVERTED (Anvil) |
| `11edfc55...9e1b` | REVERTED (Anvil) |
| `4891f430...88b1` | REVERTED (Anvil) |
| `c3830300...7666` | REVERTED (Anvil) |

## Result

**FAIL** — Strategy ran, produced PERP_OPEN intent correctly, and submitted 4 on-chain
transactions (on Anvil), but all reverted. The strategy decision logic and framework
integration work correctly. The failure is at the GMX V2 protocol level on a forked Anvil
environment: GMX V2's `createOrder` call reverts with an undecodable error, likely due to
oracle price validation or approval state issues specific to the fork context.

## Recommendations

1. **GMX V2 on Anvil**: Investigate whether the GMX V2 adapter needs to impersonate a GMX
   keeper or set oracle price data (via `anvil_setStorageAt`) before `createOrder` can succeed
   on a fork. The `gmx_perps` demo strategy in `strategies/demo/gmx_perps/` may have relevant
   patterns to compare.
2. **Approval debug**: Add explicit WETH approval trace logging to the GMX V2 adapter to confirm
   whether the approval step succeeds before the `createOrder` call.
3. **Revert decoding**: Enable `cast run --trace` on one of the failed Anvil tx hashes to get the
   full call stack and identify exactly which GMX internal call fails.
