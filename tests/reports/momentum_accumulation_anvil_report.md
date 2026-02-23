# E2E Strategy Test Report: momentum_accumulation (Anvil)

**Date:** 2026-02-20 08:26
**Result:** PASS
**Mode:** Anvil
**Duration:** ~4 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | momentum_accumulation |
| Chain | arbitrum |
| Network | Anvil fork (Arbitrum mainnet fork, block 434028223) |
| Anvil Port | 64695 (auto-assigned by managed gateway) |
| Protocol | uniswap_v3 |
| Pool | 0xC6962004f452bE9203591991D15f6b388e09E8D0 (USDC/WETH 500) |
| Target Token | WETH |
| Stable Token | USDC |

## Config Changes Made

The original `config.json` had no `anvil_funding` section. The managed gateway
requires this field to auto-fund the test wallet on its internally-managed Anvil fork.
The following was added:

```json
"anvil_funding": {
    "ETH": 100,
    "USDC": 100,
    "WETH": 1
}
```

**Budget cap compliance:** The init_swap swaps 30% of stable capital = 30% of 100 USDC = 30 USDC.
This is well below the $50 per-trade cap.

The strategy has no `force_action` field and no trade size amount fields in config (all
percentages). The strategy starts in `init_swap` phase by default, so the very first run
always triggers an immediate swap - no additional forcing was needed.

## Execution

### Setup

- Managed gateway auto-started Anvil fork of Arbitrum at block 434028223
- Managed gateway auto-funded wallet `0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266` with:
  - 100 ETH (native, via anvil_setBalance)
  - 100 USDC (via storage slot 9)
  - 1 WETH (via storage slot 51)
- Gateway gRPC server on 127.0.0.1:50051

### Strategy Run

- Strategy started in phase `init_swap` (fresh start, no prior state)
- Intent: SWAP 30.000000 USDC -> WETH (1% max slippage) via uniswap_v3
- Compiled: 30.0000 USDC -> 0.0150 WETH (min: 0.0148 WETH)
- Two transactions submitted and confirmed:
  - Tx 1 (approval): `0xb9be0de84d1306651131efb34b7861625bf7ef8cbd19a96c6dcbd76b9b8b1687` (gas: 55,449)
  - Tx 2 (swap):     `0x023beb22013d272d477a3af0af3c66828ec6d56af623673b28ae27b756cc9fe9` (gas: 125,570)
- Total gas used: 181,019
- Phase advanced: `init_swap` -> `init_lp` (via `on_intent_executed` callback)
- Parsed swap result: 0.0153 WETH received

### Key Log Output

```text
Init: swapping 30.000000 USDC -> WETH
intent: SWAP: 30.000000 USDC -> WETH (slippage: 1.00%) via uniswap_v3
Compiled SWAP: 30.0000 USDC -> 0.0150 WETH (min: 0.0148 WETH)
Transaction submitted: tx_hash=b9be0de84d...1687
Transaction submitted: tx_hash=023beb220...9fe9
Transaction confirmed: tx_hash=b9be0de8..., block=434028226, gas_used=55449
Transaction confirmed: tx_hash=023beb22..., block=434028227, gas_used=125570
EXECUTED: SWAP completed successfully | Txs: 2 | 181,019 gas
Init swap complete -> opening LP
Status: SUCCESS | Intent: SWAP | Gas used: 181019 | Duration: 20102ms
```

## Transactions

| Step | TX Hash | Gas Used | Status |
|------|---------|----------|--------|
| USDC approval | `0xb9be0de84d1306651131efb34b7861625bf7ef8cbd19a96c6dcbd76b9b8b1687` | 55,449 | SUCCESS |
| Uniswap V3 swap | `0x023beb22013d272d477a3af0af3c66828ec6d56af623673b28ae27b756cc9fe9` | 125,570 | SUCCESS |

## Result

**PASS** - The `momentum_accumulation` strategy successfully executed its `init_swap` phase,
swapping 30 USDC to WETH on a Uniswap V3 Arbitrum fork. Both the approval and swap
transactions were confirmed on-chain (Anvil fork). The strategy correctly advanced
to the `init_lp` phase after execution. The next run would open an LP position.
