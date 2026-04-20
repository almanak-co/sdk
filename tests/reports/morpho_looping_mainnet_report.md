# E2E Strategy Test Report: morpho_looping (Mainnet)

**Date:** 2026-02-20 00:57
**Result:** PASS
**Mode:** Mainnet (live on-chain)
**Chain:** Ethereum
**Duration:** ~8 minutes

---

## Configuration

| Field | Value |
|-------|-------|
| Strategy | demo_morpho_looping |
| Chain | ethereum |
| Network | Mainnet |
| Wallet | 0x54776446Aa29Fc49d152B4850bD410eA1E4d24bF |
| Market ID | 0xb323495f7e4148be5643a4ea4a8221eef163e4bccfdedc2a6f4696baacbc86cc |
| Collateral Token | wstETH |
| Borrow Token | USDC |

**Config changes made for test (restored after):**
- `initial_collateral`: `"0.1"` -> `"0.001"` (budget cap: $5 max; 0.001 wstETH ~$2.35)
- `target_loops`: `2` -> `1`
- `force_action`: `""` -> `"supply"` (triggers immediate supply without waiting for state machine)

---

## Wallet Preparation

**Pre-test balances on Ethereum:**

| Token | Had Before | Required | Source |
|-------|-----------|----------|--------|
| ETH | 0.001123 | ~0.0005 (gas) | Existing |
| stETH | 0.001547 | 0 (used for wrap) | Existing |
| wstETH | 0.000000 | 0.001 | Acquired via wrap |
| USDC | 0.000064 | 0 | Existing |

**wstETH Acquisition Path:** stETH -> wstETH wrap (no bridge needed)

The wallet had 0.001547 stETH on Ethereum which was sufficient to wrap into the needed 0.001 wstETH.

### Funding Transactions

**TX 1: Approve stETH for wstETH contract**
- Hash: `0xba4e52daf733fa7860c0b87b158525efd21b1db4ee3ea2a92af55bfb4a93adb2`
- [https://etherscan.io/tx/0xba4e52daf733fa7860c0b87b158525efd21b1db4ee3ea2a92af55bfb4a93adb2](https://etherscan.io/tx/0xba4e52daf733fa7860c0b87b158525efd21b1db4ee3ea2a92af55bfb4a93adb2)

**TX 2: Wrap stETH -> wstETH**
- Hash: `0x9dd0427758239f93b1b17f3ef1ea3c1b14dc9f4b8625ddfc6414c72e1dbfdc44`
- [https://etherscan.io/tx/0x9dd0427758239f93b1b17f3ef1ea3c1b14dc9f4b8625ddfc6414c72e1dbfdc44](https://etherscan.io/tx/0x9dd0427758239f93b1b17f3ef1ea3c1b14dc9f4b8625ddfc6414c72e1dbfdc44)
- Result: Received 0.001260 wstETH

**Post-funding balances:**
| Token | Balance |
|-------|---------|
| ETH | 0.001099 |
| wstETH | 0.001260 |

**Balance Gate: PASS** (0.001260 wstETH >= 0.001 required; 0.001099 ETH >= 0.0005 for gas)

---

## Strategy Execution

Strategy ran with `--network mainnet --once` using `force_action: supply`.

The strategy's state machine was in `idle` state; with `force_action: "supply"`, it immediately issued a SUPPLY_COLLATERAL intent to deposit 0.001 wstETH into the Morpho Blue wstETH/USDC market.

### Key Log Output

```text
[INFO] Forced action: SUPPLY collateral
[INFO] SUPPLY intent: 0.0010 wstETH to Morpho Blue
[INFO] Compiled SUPPLY: 0.001 WSTETH to Morpho Blue market 0xb323495f7e4148...
[WARN] Gas estimate tx[0]: raw=29,464 buffered=32,410 (x1.1) < compiler=88,000, using compiler limit
[WARN] Gas estimation failed for tx 2/2: ('execution reverted: transferFrom reverted'...
       Using compiler-provided gas limit.
[INFO] Transaction submitted: tx_hash=eb499d8f62c7658cdce3d2c7f8eaab7c9ec5e6c33c19746bf94484e31d0666c7
[INFO] Transaction submitted: tx_hash=a36b10a4bfb8376aa87ccb5a41f91b0ab1be7e04dad77fef7dec774fc131deb5
[INFO] Transaction confirmed: tx_hash=eb499d...66c7, block=24492526, gas_used=29116
[INFO] Transaction confirmed: tx_hash=a36b10...deb5, block=24492531, gas_used=59307
[INFO] EXECUTED: SUPPLY completed successfully
[INFO] Txs: 2 (eb499d...66c7, a36b10...deb5) | 88,423 gas
Status: SUCCESS | Intent: SUPPLY | Gas used: 88423 | Duration: 117362ms
```

**Note on gas estimation warning:** The gas estimator failed for tx 2 (the supply tx) with "transferFrom reverted". This is a known simulation limitation — the Alchemy simulator tried to simulate the supply before the approve was confirmed on-chain. The strategy framework correctly fell back to the compiler-provided gas limit (88,000), and both transactions confirmed successfully.

---

## Transactions

| Step | Intent | TX Hash | Explorer Link | Gas Used | Status |
|------|--------|---------|---------------|----------|--------|
| Approve wstETH | (system) | `eb499d8f62c7658cdce3d2c7f8eaab7c9ec5e6c33c19746bf94484e31d0666c7` | [etherscan](https://etherscan.io/tx/0xeb499d8f62c7658cdce3d2c7f8eaab7c9ec5e6c33c19746bf94484e31d0666c7) | 29,116 | SUCCESS |
| Supply Collateral | SUPPLY | `a36b10a4bfb8376aa87ccb5a41f91b0ab1be7e04dad77fef7dec774fc131deb5` | [etherscan](https://etherscan.io/tx/0xa36b10a4bfb8376aa87ccb5a41f91b0ab1be7e04dad77fef7dec774fc131deb5) | 59,307 | SUCCESS |

**Total on-chain transactions: 4** (2 funding: approve+wrap stETH; 2 strategy: approve+supply wstETH to Morpho)

**Total gas used (strategy): 88,423**

---

## wstETH Acquisition Notes

The wallet had no wstETH but had stETH from previous activity. The wrapping path was:

1. Approve stETH (`0xae7ab96520de3a18e5e111b5eaab095312d7fe84`) for the wstETH contract (`0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0`)
2. Call `wrap(1546954309576267)` on the wstETH contract - received 0.001260 wstETH

This is Method A (direct wrap) with stETH as the source. The Lido wstETH contract's `wrap()` function directly converts stETH to the wrapped version. No bridge needed.

---

## Errors / Observations

- **Gas simulation warning (non-blocking):** `execution reverted: transferFrom reverted` during Alchemy simulation of the supply tx. This occurs because the simulator attempted to simulate the supply before the approve was on-chain. Framework handled it correctly by using the compiler's gas limit instead. Both transactions confirmed successfully.

- **Amount chaining warning (non-blocking):** `Amount chaining: no output amount extracted from step 1; subsequent amount='all' steps will fail`. This is informational for strategies that chain outputs (e.g., use the supply output amount as borrow input). Not relevant for this test since we only ran 1 intent.

---

## Result

**PASS** - The morpho_looping strategy successfully supplied 0.001 wstETH as collateral to Morpho Blue market on Ethereum mainnet. Both the approve and supply transactions confirmed on-chain (blocks 24492526 and 24492531). The strategy executed the SUPPLY intent end-to-end within the $5 budget cap.

---

PREFLIGHT_CHECKLIST:
  STATE_CLEARED: YES
  BALANCE_CHECKED: YES
  TOKENS_NEEDED: 0.001 wstETH, 0.0005 ETH
  TOKENS_AVAILABLE: 0.001260 wstETH (after wrap), 0.001099 ETH
  FUNDING_NEEDED: YES
  FUNDING_ATTEMPTED: YES
  FUNDING_METHOD: Method A (stETH -> wstETH wrap via Lido wstETH contract)
  FUNDING_TX: 0xba4e52daf733fa7860c0b87b158525efd21b1db4ee3ea2a92af55bfb4a93adb2 (approve), 0x9dd0427758239f93b1b17f3ef1ea3c1b14dc9f4b8625ddfc6414c72e1dbfdc44 (wrap)
  BALANCE_GATE: PASS
  STRATEGY_RUN: YES
