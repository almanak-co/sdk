# E2E Strategy Test Report: ethena_yield (Anvil)

**Date:** 2026-03-03 11:54
**Result:** PASS
**Mode:** Anvil
**Duration:** ~5 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | EthenaYieldStrategy (demo_ethena_yield) |
| Chain | ethereum |
| Network | Anvil fork (public RPC: https://ethereum-rpc.publicnode.com) |
| Anvil Port | 51753 (managed, auto-assigned) |
| Fork Block | 24576759 |
| Force Action | swap (USDC -> USDe) |

## Config Changes Made

None. The config already had `force_action: "swap"` and `min_usdc_amount: "5"`, well within the $500 budget cap.

## Execution

### Setup

- [x] Managed gateway auto-started on port 50052 (network=anvil)
- [x] Anvil fork of Ethereum started on port 51753 (block 24576759, chain_id=1) using free public RPC (publicnode.com) -- no Alchemy key configured
- [x] Wallet auto-funded by managed gateway via `anvil_funding` config:
  - 100 ETH, 10,000 USDC (slot 9), 1 WETH (slot 3), 1,000 USDe (brute-force slot 2)
- [x] Wallet: `0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266` (Anvil default)
- Note: Strategy resumed from existing persisted state (prior run had `swapped: True`); `force_action` overrode this.

### Strategy Run

- [x] Intent emitted: SWAP 5.0 USDC -> USDe via Enso aggregator
- [x] Route found: 5.0 USDC -> 5.0038 USDe (min: 4.9788 USDe, price impact: 0bp)
- [x] 2 transactions submitted and confirmed:
  - TX 1 (approve): `0x680fd139af53fb0f941745ec283f59709151ff84c34cd3038e507d7a058bee1e` (block 24576765, gas 55,558)
  - TX 2 (swap): `0x51bc1c98e523e32b04a7641da790cfec4f5c31a7bed6e2dba13179a8828833de` (block 24576766, gas 618,236)
- [x] Total gas used: 673,794
- [x] Final status: `SUCCESS | Intent: SWAP | Gas used: 673794 | Duration: 48114ms`

### Key Log Output

```text
2026-03-03T11:54:05.864234Z [info] Forced action: SWAP USDC -> USDe
2026-03-03T11:54:05.864268Z [info] SWAP intent: 5.0000 USDC -> USDe via Enso (slippage=0.5%)
2026-03-03T11:54:08.562373Z [info] Route found: 0xA0b86991... -> 0x4c9EDD58..., amount_out=5003784052264016464, price_impact=0bp
2026-03-03T11:54:08.928772Z [info] Compiled SWAP (Enso): 5.0000 USDC -> 5.0038 USDE (min: 4.9788 USDE)
2026-03-03T11:54:08.929023Z [info] Slippage: 0.50% | Impact: N/A | Txs: 2 | Gas: 848,027
2026-03-03T11:54:19.589710Z [info] Transaction submitted: tx_hash=680fd139...ee1e, latency=5.7ms
2026-03-03T11:54:19.904261Z [info] Transaction confirmed: tx_hash=680fd139...ee1e, block=24576765, gas_used=55558
2026-03-03T11:54:19.907124Z [info] Transaction submitted: tx_hash=51bc1c98...33de, latency=2.2ms
2026-03-03T11:54:43.878880Z [info] Transaction confirmed: tx_hash=51bc1c98...33de, block=24576766, gas_used=618236
2026-03-03T11:54:53.965248Z [info] EXECUTED: SWAP completed successfully
2026-03-03T11:54:53.965429Z [info] Txs: 2 (680fd1...ee1e, 51bc1c...33de) | 673,794 gas
2026-03-03T11:54:53.977143Z [info] Swap successful: 5 USDC -> USDe
Status: SUCCESS | Intent: SWAP | Gas used: 673794 | Duration: 48114ms
Iteration completed successfully.
```

## Transactions (Anvil -- not mainnet)

| Step | TX Hash | Gas Used | Status |
|------|---------|----------|--------|
| TX 1: Approve USDC | `0x680fd139af53fb0f941745ec283f59709151ff84c34cd3038e507d7a058bee1e` | 55,558 | SUCCESS |
| TX 2: Swap USDC -> USDe | `0x51bc1c98e523e32b04a7641da790cfec4f5c31a7bed6e2dba13179a8828833de` | 618,236 | SUCCESS |

*(Anvil local fork transactions -- no block explorer links)*

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | gateway | WARNING | Placeholder prices in IntentCompiler | `IntentCompiler using PLACEHOLDER PRICES. Slippage calculations will be INCORRECT. This is only acceptable for unit tests.` |
| 2 | gateway | INFO | No CoinGecko API key - on-chain pricing fallback | `No CoinGecko API key -- using on-chain pricing (Chainlink oracles) with free CoinGecko as fallback.` |
| 3 | gateway | INFO | No API key / public RPC with rate limit risk | `No API key configured -- using free public RPC for ethereum (rate limits may apply)` |
| 4 | strategy | INFO | Stale state on resume | `Mode: RESUME (existing state found)` with `swapped: True, swapped_amount: 5` -- `force_action` overrode this correctly |
| 5 | strategy | WARNING | Port not freed within grace period | `Port 51753 not freed after 5.0s` |

### Analysis

- **Finding 1 (Placeholder prices)**: Known Anvil mode limitation. The IntentCompiler does not receive live prices from the gateway when compiling on Anvil, so slippage calculations use placeholder values. The swap itself is protected by Enso's own min-output calculation, but the local slippage guard is ineffective. This is a data flow gap that affects all Anvil runs.
- **Finding 2 (CoinGecko fallback)**: Expected for this `.env` configuration. On-chain Chainlink pricing is used as primary; this is acceptable. INFO severity only.
- **Finding 3 (Public RPC)**: No Alchemy key configured. Public RPC works but may have rate limits under load. Not an issue for single test runs.
- **Finding 4 (Stale state)**: Strategy loaded prior run's state showing `swapped: True`. The `force_action` config flag correctly overrode this. In production without `force_action`, the strategy would return HOLD on first run due to stale state. Test harnesses should clear state between runs.
- **Finding 5 (Port cleanup)**: Cosmetic -- Anvil stops correctly but the port linger check fires before the OS releases it. Non-blocking.

No zero prices, transaction reverts, token resolution errors, or Enso API failures observed.

## Result

**PASS** - The ethena_yield strategy successfully executed a USDC -> USDe swap via the Enso aggregator on an Ethereum Anvil fork, confirming 2 on-chain transactions (673,794 gas total). The placeholder prices warning (Finding 1) is a pre-existing Anvil mode limitation; the swap executed correctly using Enso's real on-chain route and min-output protection.

---

SUSPICIOUS_BEHAVIOUR_COUNT: 5
SUSPICIOUS_BEHAVIOUR_ERRORS: 0
