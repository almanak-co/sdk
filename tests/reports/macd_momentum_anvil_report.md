# E2E Strategy Test Report: macd_momentum (Anvil)

**Date:** 2026-02-20 08:22
**Result:** PASS
**Mode:** Anvil
**Duration:** ~4 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | macd_momentum (incubating) |
| Chain | base |
| Network | Anvil fork (Base mainnet) |
| Anvil Port | 63886 (managed, auto-assigned) |
| Strategy ID | demo_macd_momentum |
| Wallet | 0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266 |

## Config Changes Made

The original `config.json` lacked an `anvil_funding` block, which caused the first run to fail
with `ERC20: transfer amount exceeds balance` (the managed gateway created its own Anvil fork
without any funded tokens). Added `anvil_funding` to enable the gateway to seed the wallet:

```json
"anvil_funding": {
    "ETH": 10,
    "USDC": 1000
}
```

No changes were made to trade size (already `"5"` USD, well under the $50 cap).
`force_action: "buy"` was already set in config; no modification needed.

## Execution

### Setup

- Managed gateway auto-started on 127.0.0.1:50052
- Anvil fork of Base mainnet started on port 63886 (block 42393196, chain ID 8453)
- Wallet funded by gateway: 10 ETH + 1000 USDC via storage slot 9
- WETH price fetched: $1,963.55 (CoinGecko)

### Strategy Decision

- `force_action: buy` triggered immediately
- Intent: SWAP $5.00 USDC -> WETH via Enso
- Enso route found: USDC -> WETH, amount_out = 0.0025 WETH, price impact = 0bp

### Transactions

| Step | TX Hash | Block | Gas Used | Status |
|------|---------|-------|----------|--------|
| USDC approve (Permit2) | `42044fd4f0fa421c016a1fb08025e530243fedbfe34302c63ce7c449157d28ae` | 42393198 | 55,437 | SUCCESS |
| Enso swap (USDC -> WETH) | `0d0ba88e27a40c49b846eb3545ffc47b82b874d9fb4eb15b7990ce13bf38c8ec` | 42393199 | 415,053 | SUCCESS |

Total gas used: 470,490

### Key Log Output

```text
Force action: buy
Aggregated price for WETH/USD: 1963.55 (confidence: 1.00, sources: 1/1, outliers: 0)
intent: SWAP: $5.00 USDC -> WETH (slippage: 1.00%) via enso
Compiled SWAP (Enso): 5.0000 USDC -> 0.0025 WETH (min: 0.0025 WETH)
Slippage: 1.00% | Impact: N/A | Txs: 2 | Gas: 569,043
Transaction confirmed: tx_hash=42044f...28ae, block=42393198, gas_used=55437
Transaction confirmed: tx_hash=0d0ba8...c8ec, block=42393199, gas_used=415053
EXECUTED: SWAP completed successfully
Txs: 2 (42044f...28ae, 0d0ba8...c8ec) | 470,490 gas
Status: SUCCESS | Intent: SWAP | Gas used: 470490 | Duration: 21795ms
Iteration completed successfully.
```

## Warnings Noted (Non-fatal)

- `Amount chaining: no output amount extracted from step 1; subsequent amount='all' steps will fail`
  This is a minor warning relevant only if a sell-all teardown follows immediately in the same run.
  It does not affect the buy swap result.

## First Run Failure (Root Cause)

The first attempt failed because the managed gateway creates its own Anvil fork and the config
lacked `anvil_funding`. Without it, the gateway skipped wallet seeding and the USDC transfer
reverted on every attempt (3 retries). Adding `anvil_funding` to `config.json` resolved this.

## Result

**PASS** - Strategy produced 2 on-chain transactions (USDC Permit2 approval + Enso swap).
SWAP of $5.00 USDC -> 0.0025 WETH executed successfully on Anvil fork of Base.
