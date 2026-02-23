# E2E Strategy Test Report: vault_yield_rotator (Anvil)

**Date:** 2026-02-20 09:26
**Result:** PASS
**Mode:** Anvil
**Duration:** ~4 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | VaultYieldRotator |
| Strategy ID | demo_vault_yield_rotator |
| Chain | ethereum |
| Network | Anvil fork (port auto-assigned by managed gateway) |
| Wallet | 0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266 (Anvil default) |

## Config Changes Made

The following changes were made to `strategies/incubating/vault_yield_rotator/config.json` for the test run, then restored afterward:

| Field | Original | Test Value | Reason |
|-------|----------|------------|--------|
| `trade_size_usd` | 100 | 50 | Budget cap ($50 max per trade) |
| `rsi_oversold` | 35 | 99 | Force immediate BUY signal (USDC -> WETH) |
| `vault` block | present (placeholder addresses) | removed | Removed to avoid failure on placeholder `0xVAULT_ADDRESS_HERE` addresses |
| `anvil_funding` | absent | `{ETH:10, USDC:500, WETH:1}` | Required for managed gateway to fund the Anvil wallet |

All values were restored to their originals after the test.

**Note on vault block removal:** The vault block contained placeholder addresses (`0xVAULT_ADDRESS_HERE`, `0xVALUATOR_ADDRESS_HERE`). The strategy docstring explicitly states the vault block is optional -- removing it disables vault wrapping without any code changes required.

## Execution

### Setup
- Managed gateway auto-started Anvil fork of Ethereum mainnet on port 59002
- Block: 24497156 (Ethereum mainnet fork)
- Wallet auto-funded via `anvil_funding` config key: 10 ETH, 500 USDC, 1 WETH
- Gateway started on 127.0.0.1:50052

### Strategy Run
- RSI computed: 44.50 (using 14-period RSI on WETH/USD via Binance OHLCV)
- RSI 44.50 <= rsi_oversold 99 triggered BUY signal
- Compiled SWAP: 50.00 USDC -> ~0.0254 WETH at WETH price $1,966.95, slippage 1%
- 2 transactions submitted (approve + swap via Uniswap V3)

### Key Log Output

```text
BUY: RSI=44.50 < 99
demo_vault_yield_rotator intent: SWAP: $50.00 USDC -> WETH (slippage: 1.00%) via uniswap_v3
Compiled SWAP: 50.0000 USDC -> 0.0253 WETH (min: 0.0251 WETH)
Slippage: 1.00% | Txs: 2 | Gas: 260,000
Transaction submitted: tx_hash=61eab6183c632af649934edc808f9e3fceaef7df3e438f8e98e8b56151682663
Transaction submitted: tx_hash=92370b6cfe0940c883df9b80815e21e56f5b5d461ef6c1a58589c25ea80bfdef
Transaction confirmed: tx_hash=61eab618..., block=24497159, gas_used=55570  (USDC approve)
Transaction confirmed: tx_hash=92370b6c..., block=24497160, gas_used=124496  (Uniswap V3 swap)
EXECUTED: SWAP completed successfully
Txs: 2 (61eab6...2663, 92370b...fdef) | 180,066 gas
Enriched SWAP result with: swap_amounts (protocol=uniswap_v3, chain=ethereum)
Status: SUCCESS | Intent: SWAP | Gas used: 180066 | Duration: 20803ms
```

## Transactions (Anvil - local fork only)

| Step | TX Hash | Gas Used | Status |
|------|---------|----------|--------|
| USDC approve | `0x61eab6183c632af649934edc808f9e3fceaef7df3e438f8e98e8b56151682663` | 55,570 | SUCCESS |
| Uniswap V3 SWAP (50 USDC -> WETH) | `0x92370b6cfe0940c883df9b80815e21e56f5b5d461ef6c1a58589c25ea80bfdef` | 124,496 | SUCCESS |

Total gas: 180,066

## Non-Critical Warnings

- `USDC.e` and `USDC_BRIDGED` token resolution warnings -- benign, only native USDC was needed
- `Port 59002 not freed after 5.0s` -- normal race condition during shutdown, non-fatal

## Result

**PASS** - VaultYieldRotator executed a USDC -> WETH swap of $50 successfully on an Ethereum Anvil fork. Both on-chain transactions (approve + swap) confirmed on-chain. The vault block was removed for testing because it contained placeholder addresses; the strategy works correctly without it.
