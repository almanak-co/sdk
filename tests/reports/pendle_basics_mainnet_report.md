# E2E Strategy Test Report: pendle_basics (Mainnet)

**Date:** 2026-02-20 01:24
**Result:** PASS
**Mode:** Mainnet (live on-chain)
**Chain:** Plasma (HyperEVM, Chain ID: 9745)
**Duration:** ~8 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | demo_pendle_basics |
| Chain | plasma |
| Network | Mainnet |
| Wallet | 0x54776446Aa29Fc49d152B4850bD410eA1E4d24bF |
| Market | PT-fUSDT0-26FEB2026 (`0x0cb289e9df2d0dcfe13732638c89655fb80c2be2`) |
| Trade size | 1 FUSDT0 (~$1.00 USD, within $5 budget cap) |

## Config Changes Made

Added `"network": "mainnet"` to `config.json` for the test run. Restored to original (removed field) after test.

## Funding Challenges

FUSDT0 (Fluid USDT0) is a Fluid ERC4626 vault token on Plasma chain with known liquidity issues:
- DEX liquidity for ETH/FUSDT0 on KyberSwap is extremely thin (1 ETH -> 0.09 USDT0 only)
- FUSDT0 cannot be obtained via direct DEX swaps at reasonable rates

**Funding path used:**
1. Bridge 1.5 USDC from Arbitrum to Plasma (received 1.595157 USDT0 on Plasma) via Enso + Stargate
2. Deposit 1.1 USDT0 into FUSDT0 ERC4626 vault directly (received 1.085867 FUSDT0)

## Wallet Preparation

| Token | Required | Had Before | Action | Method | Result |
|-------|----------|------------|--------|--------|--------|
| ETH (Plasma) | ~0.001 for gas | 9.922 ETH | None needed | existing | 9.922 ETH |
| FUSDT0 | 1.0 | 0.0 | Bridge + deposit | Cross-chain bridge + ERC4626 deposit | 1.085867 FUSDT0 |
| USDC (Arbitrum) | 1.5 (source) | 5.16 | Spend | source for bridge | -1.5 USDC |

### Funding Transactions

**Arbitrum approvals:**
- USDC -> Permit2 approval: [0x4a6eaa5d65ddf99336f60a4b74710f854c31936e14c530a83aa1da3eb11d66a5](https://arbiscan.io/tx/0x4a6eaa5d65ddf99336f60a4b74710f854c31936e14c530a83aa1da3eb11d66a5)
- Permit2 -> Enso router approval: [0xd9296721888659bcd4b5a5577ba2d17afad38dbda03f164a407505d4476992f9](https://arbiscan.io/tx/0xd9296721888659bcd4b5a5577ba2d17afad38dbda03f164a407505d4476992f9)
- USDC -> Enso router direct approval: [0xd74c1692ef8953c78e3ad30759223363eba84626fee7094d14673514fc3d5258](https://arbiscan.io/tx/0xd74c1692ef8953c78e3ad30759223363eba84626fee7094d14673514fc3d5258)

**Cross-chain bridge (Arbitrum -> Plasma via Enso + Stargate):**
- Bridge TX (Arbitrum): [0xb162db2d87eba10f7307bde3a798a3d75f3f2b2cf8aa350ed7844ccc19a1d81e](https://arbiscan.io/tx/0xb162db2d87eba10f7307bde3a798a3d75f3f2b2cf8aa350ed7844ccc19a1d81e)
  - Route: USDC (Arbitrum) -> USDT (Nordstern) -> USDT0 (Plasma) via Stargate bridge
  - Result: 1.595157 USDT0 arrived on Plasma (gas used: 834,275)

**Plasma chain funding:**
- USDT0 -> FUSDT0 vault deposit: 1.1 USDT0 deposited, received 1.085867 FUSDT0
  - USDT0 approve TX: 0xc65a522c61e0696b538ce1f74e20ae00c03c26c5ee9c169c76a0a0b4d332550a
  - Deposit TX: 0x9a2a91490d9554ec5027c66e702cdb1b9a3e0882deb6aa008986561f2cf6ac6d

Note: A first bridge attempt reverted (0x5dfe747c...) because the Enso cross-chain route requires a direct ERC20 approval to the Enso router (not Permit2). This was fixed by adding the direct approval.

## Balance Gate

```
  ETH:    9.922236 (need ~0.001 for gas)  OK
  FUSDT0: 1.085867 (need 1.0)             OK
  GATE: PASS
```

## Strategy Execution

- Strategy started fresh (no prior state in almanak_state.db)
- Strategy detected FUSDT0 balance: 1.085867 (> 1.0 required)
- Decision: Enter Pendle position (swap FUSDT0 -> PT-fUSDT0)
- Pendle SDK initialized for plasma chain, router: `0x888888888889758F76e7103c6CbF23ABbF58F946`
- Intent compiled: 2 transactions (approval + swap)

### Key Log Output

```text
Entering Pendle position: Swapping 1 FUSDT0 for PT-fUSDT0
demo_pendle_basics intent: SWAP: 1 FUSDT0 -> PT-fUSDT0 (slippage: 1.00%) via pendle
PendleSDK initialized for chain=plasma, router=0x888888888889758F76e7103c6CbF23ABbF58F946
Compiling Pendle SWAP: FUSDT0 -> PT-fUSDT0, amount=1000000, market=0x0cb289e9...
Compiled Pendle SWAP intent: FUSDT0 -> PT-fUSDT0, 2 txs, 480000 gas
Transaction submitted: tx_hash=def03df0..., latency=864.5ms
Transaction submitted: tx_hash=df287f78..., latency=398.5ms
Transaction confirmed: tx_hash=def03df0..., block=14593683, gas_used=29116
Transaction confirmed: tx_hash=df287f78..., block=14593684, gas_used=296285
EXECUTED: SWAP completed successfully
  Txs: 2 (def03d...8cca, df287f...8d69) | 325,401 gas
Status: SUCCESS | Intent: SWAP | Gas used: 325401 | Duration: 25233ms
Iteration completed successfully.
```

## Transactions (Strategy)

| Step | TX Hash | Block | Gas Used | Status |
|------|---------|-------|----------|--------|
| Approval (FUSDT0) | `0xdef03df03fa186a01ff043536aa0d81d2f20162cabf64227f9fb5a8c0f348cca` | 14593683 | 29,116 | SUCCESS |
| Swap (FUSDT0->PT-fUSDT0) | `0xdf287f7899297440a0afdfa59597b1ac0ae2c00606302cd608c8c5f95e638d69` | 14593684 | 296,285 | SUCCESS |

Note: Plasma (HyperEVM) does not have a public block explorer like Arbiscan. Transactions confirmed on-chain via receipt.

## Minor Issues Observed

1. **Permit2 vs direct approval for cross-chain routes**: Enso's cross-chain routes (via Nordstern/Stargate) require a direct ERC20 approval to the Enso router, NOT via Permit2. The Permit2 path is only used for same-chain swaps. The initial bridge TX reverted because only Permit2 was approved. This was resolved by adding a direct approval.

2. **Amount chaining warning**: `Amount chaining: no output amount extracted from step 1; subsequent amount='all' steps will fail` - minor warning, did not affect execution since the swap completed successfully.

3. **FUSDT0 DEX liquidity**: KyberSwap on Plasma has essentially no liquidity for ETH/FUSDT0 pairs. Only the ERC4626 vault deposit path (USDT0 -> FUSDT0) is viable. This makes funding complex and requires a multi-step cross-chain bridge.

## Result

**PASS** - The pendle_basics strategy successfully executed a SWAP of 1 FUSDT0 for PT-fUSDT0-26FEB2026 on the Plasma chain (HyperEVM) via Pendle protocol. Two transactions confirmed on-chain (approval + swap), total gas used: 325,401. Funding required a cross-chain bridge from Arbitrum (USDC -> USDT0 via Stargate) followed by an ERC4626 vault deposit (USDT0 -> FUSDT0).

---

PREFLIGHT_CHECKLIST:
  STATE_CLEARED: YES
  BALANCE_CHECKED: YES
  TOKENS_NEEDED: 1.0 FUSDT0, 0.001 ETH (gas)
  TOKENS_AVAILABLE: 0.0 FUSDT0, 9.922 ETH (Plasma)
  FUNDING_NEEDED: YES
  FUNDING_ATTEMPTED: YES
  FUNDING_METHOD: Method D (cross-chain bridge Arbitrum->Plasma via Enso+Stargate) + ERC4626 vault deposit
  FUNDING_TX: 0xb162db2d87eba10f7307bde3a798a3d75f3f2b2cf8aa350ed7844ccc19a1d81e (bridge), 0x9a2a91490d9554ec5027c66e702cdb1b9a3e0882deb6aa008986561f2cf6ac6d (deposit)
  BALANCE_GATE: PASS
  STRATEGY_RUN: YES
