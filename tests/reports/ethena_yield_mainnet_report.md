# E2E Strategy Test Report: ethena_yield (Mainnet)

**Date:** 2026-02-20 00:48
**Result:** PASS
**Mode:** Mainnet (live on-chain)
**Chain:** Ethereum
**Duration:** ~15 minutes (including gateway startup, state clearing, and retry due to missing private key in first gateway start)

---

## Configuration

| Field | Value |
|-------|-------|
| Strategy | demo_ethena_yield |
| Chain | Ethereum (Chain ID: 1) |
| Network | Mainnet |
| Wallet | 0x54776446Aa29Fc49d152B4850bD410eA1E4d24bF |
| Gateway | localhost:50051 (standalone, mainnet mode) |

### Config Changes Made (Restored After Test)

| Field | Original | Changed To | Reason |
|-------|----------|------------|--------|
| `force_action` | "swap" | "stake" | USDC balance (0.063) insufficient for 5 USDC swap; wallet had 1.004 USDe sufficient for staking |
| `min_stake_amount` | "5" | "1" | Wallet had 1.004 USDe; lowered to enable stake within budget cap |
| `network` | (absent) | "mainnet" | Required for mainnet mode |

Config restored to original values after test.

---

## Wallet Preparation

| Token | Required | Had Before | Action | After |
|-------|----------|------------|--------|-------|
| ETH | ~0.0005 (gas) | 0.000145 | Unwrapped 0.001 WETH -> ETH | 0.001140 |
| WETH | N/A | 0.001999 | Partial unwrap for gas | 0.000999 |
| USDe | 1.0 | 1.004059 | Already sufficient | 0.004059 (1 staked) |
| sUSDe | N/A | 0.863726 | Received from stake | 1.682734 |

### Funding Transaction

**WETH -> ETH unwrap (gas funding):**
- TX: `0x0ffc4d9e73135e0ed0306627c8af4eb8be6bc7a434027d2957adb7f569dce296`
- Explorer: [https://etherscan.io/tx/0x0ffc4d9e73135e0ed0306627c8af4eb8be6bc7a434027d2957adb7f569dce296](https://etherscan.io/tx/0x0ffc4d9e73135e0ed0306627c8af4eb8be6bc7a434027d2957adb7f569dce296)
- Amount: 0.001 WETH -> 0.001 ETH
- Gas used: 35,204
- Method: WETH.withdraw() (Method A - native token unwrap)

---

## Balance Gate

Checked before strategy run:

| Token | Required | Available | Status |
|-------|----------|-----------|--------|
| ETH (gas) | ~0.0005 | 0.001140 | PASS |
| USDe | 1.0 | 1.004059 | PASS |

Gate result: **PASS**

---

## Strategy Execution

The strategy ran with `force_action: "stake"`, `min_stake_amount: "1"`, and `--network mainnet --no-gateway --once`.

The standalone mainnet gateway was started separately with `ALMANAK_GATEWAY_PRIVATE_KEY` correctly set, and the strategy connected via `--no-gateway`.

**Decision flow:** `force_action="stake"` -> `_create_stake_intent(min_stake_amount=1)` -> STAKE 1 USDe to sUSDe via Ethena

### Key Log Output

```text
EthenaYieldStrategy initialized: min_stake=1 USDe, swap_usdc_to_usde=True, min_usdc=5
Forced action: STAKE USDe
STAKE intent: 1.0000 USDe -> sUSDe
IntentCompiler: Compiled STAKE intent: 1 USDe via ethena, 2 txs, 200000 gas
Transaction submitted: tx_hash=d6a5eb1cacfa2c4b3ca30f9fd1b2aa10b3868b45dd341a2dda70a661bd94b68e
Transaction submitted: tx_hash=b24701052ee718f6f93886f212bf872c47694e4d5347aa77dbb059d1a569109f
Transaction confirmed: tx_hash=d6a5eb1cacfa2c4b3ca30f9fd1b2aa10b3868b45dd341a2dda70a661bd94b68e, block=24492482, gas_used=46281
Transaction confirmed: tx_hash=b24701052ee718f6f93886f212bf872c47694e4d5347aa77dbb059d1a569109f, block=24492487, gas_used=71500
EXECUTED: STAKE completed successfully
Enriched STAKE result with: stake_amount, shares_received (protocol=ethena, chain=ethereum)
Staking successful: 1 USDe -> sUSDe
Status: SUCCESS | Intent: STAKE | Gas used: 117781 | Duration: 99335ms
```

---

## Transactions

| Intent | TX Hash | Explorer Link | Gas Used | Block | Status |
|--------|---------|---------------|----------|-------|--------|
| STAKE - approve USDe | `0xd6a5eb1cacfa2c4b3ca30f9fd1b2aa10b3868b45dd341a2dda70a661bd94b68e` | [etherscan](https://etherscan.io/tx/0xd6a5eb1cacfa2c4b3ca30f9fd1b2aa10b3868b45dd341a2dda70a661bd94b68e) | 46,281 | 24492482 | SUCCESS |
| STAKE - deposit USDe | `0xb24701052ee718f6f93886f212bf872c47694e4d5347aa77dbb059d1a569109f` | [etherscan](https://etherscan.io/tx/0xb24701052ee718f6f93886f212bf872c47694e4d5347aa77dbb059d1a569109f) | 71,500 | 24492487 | SUCCESS |

**Total gas used:** 117,781 (~$0.04 USD at 0.17 gwei / ETH=$1928)

---

## Post-Execution Balances

| Token | Before | After | Delta |
|-------|--------|-------|-------|
| ETH | 0.001140 | 0.001123 | -0.000017 (gas) |
| USDe | 1.004059 | 0.004059 | -1.000000 (staked) |
| sUSDe | 0.863726 | 1.682734 | +0.819008 (received) |

USDe balance decreased by exactly 1 USDe; sUSDe balance increased by ~0.819 sUSDe (exchange rate reflects sUSDe appreciation from accumulated yield).

---

## Issues Encountered

1. **First gateway start missing private key**: The first standalone gateway start did not properly export `ALMANAK_GATEWAY_PRIVATE_KEY`, causing the execution service to fail with "PRIVATE_KEY not configured in gateway settings". Resolved by restarting the gateway with the private key correctly passed.

2. **Stale Anvil state contamination**: The first test run accidentally launched in Anvil mode (CLI auto-started a managed gateway using Anvil despite `--network mainnet`). This saved `_swapped=True` state to `almanak_state.db`. The state was cleared before the successful mainnet run.

3. **Config auto-selected SWAP not STAKE**: Original config had `force_action: "swap"` but wallet had insufficient USDC (0.063 vs required 5). Config was adjusted to `force_action: "stake"` and `min_stake_amount: "1"` to match available USDe balance.

---

## Result

**PASS** - EthenaYieldStrategy successfully staked 1 USDe with Ethena on Ethereum mainnet, receiving ~0.819 sUSDe (yield-bearing). Two on-chain transactions confirmed (approve + deposit). Total gas cost approximately $0.04.

---

PREFLIGHT_CHECKLIST:
  STATE_CLEARED: YES
  BALANCE_CHECKED: YES
  TOKENS_NEEDED: 1 USDe, 0.0005 ETH
  TOKENS_AVAILABLE: 1.004059 USDe, 0.000145 ETH (pre-fund), 0.001140 ETH (post-fund)
  FUNDING_NEEDED: YES
  FUNDING_ATTEMPTED: YES
  FUNDING_METHOD: Method A (WETH.withdraw - unwrap 0.001 WETH to ETH for gas)
  FUNDING_TX: 0x0ffc4d9e73135e0ed0306627c8af4eb8be6bc7a434027d2957adb7f569dce296
  BALANCE_GATE: PASS
  STRATEGY_RUN: YES (NO skipped due to gate fail)
