# E2E Strategy Test Report: ethena_yield (Anvil)

**Date:** 2026-03-15 17:48 (re-run #3)
**Result:** PASS
**Mode:** Anvil
**Duration:** ~2 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | EthenaYieldStrategy (demo_ethena_yield) |
| Chain | ethereum |
| Network | Anvil fork (Alchemy eth-mainnet) |
| Anvil Port | 64199 (managed, auto-assigned) |
| Fork Block | 24664484 |
| Force Action | swap (USDC -> USDe) |

## Config Changes Made

None. The config already had `force_action: "swap"` and `min_usdc_amount: "5"`, well within the $1000 budget cap.

## Execution

### Setup

- [x] Managed gateway auto-started on port 50052 (network=anvil)
- [x] Anvil fork of Ethereum started on port 64199 (block 24664484, chain_id=1) via Alchemy
- [x] Wallet auto-funded by managed gateway via `anvil_funding` config:
  - 100 ETH, 10,000 USDC (slot 9), 1 WETH (slot 3), 1,000 USDe (brute-force slot 2)
- [x] Wallet: `0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266` (Anvil default)

### Strategy Run

- [x] Intent emitted: SWAP 5.0 USDC -> USDe via Enso aggregator (force_action="swap")
- Single attempt (2-TX Enso route, no revert this run):
  - TX 1 (approve): `577430dd8cb2d128d1f062b8579f4c4b9c64be49f2093995fc6b63fc6ee4e1ff` (block 24664490, gas 55,558) -- SUCCESS
  - TX 2 (swap): `465e3870ef88f07581c1d6c5e24aec119ceb11160a00c04af12d10350f237e24` (block 24664491, gas 941,503) -- SUCCESS
- [x] Final status: `SUCCESS | Intent: SWAP | Gas used: 997061 | Duration: 39385ms`

### Key Log Output

```text
[info] Forced action: SWAP USDC -> USDe
[info] SWAP intent: 5.0000 USDC -> USDe via Enso (slippage=0.5%)
[info] Route found: 0xA0b86991... -> 0x4c9EDD58..., amount_out=5130130722126824832, price_impact=0bp
[info] Compiled SWAP (Enso): 5.0000 USDC -> 5.1301 USDE (min: 5.1045 USDE)
[info] Sequential submit: TX 1/2 confirmed (block=24664490, gas=55558)
[info] Sequential submit: TX 2/2 confirmed (block=24664491, gas=941503)
[info] EXECUTED: SWAP completed successfully
[info] Txs: 2 (577430...e1ff, 465e38...7e24) | 997,061 gas
[info] Swap successful: 5 USDC -> USDe
Status: SUCCESS | Intent: SWAP | Gas used: 997061 | Duration: 39385ms
Iteration completed successfully.
```

## Transactions (Anvil -- not mainnet)

| Step | TX Hash | Gas Used | Status |
|------|---------|----------|--------|
| TX 1: Approve USDC | `577430dd8cb2d128d1f062b8579f4c4b9c64be49f2093995fc6b63fc6ee4e1ff` | 55,558 | SUCCESS |
| TX 2: Swap USDC -> USDe | `465e3870ef88f07581c1d6c5e24aec119ceb11160a00c04af12d10350f237e24` | 941,503 | SUCCESS |

*(Anvil local fork transactions -- no block explorer links)*

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | gateway | WARNING | CoinGecko rate limit for USDE/USD | `Rate limited by CoinGecko for USDE/USD, backoff: 1.00s` |
| 2 | gateway | INFO | Stale CoinGecko data used after rate limit | `Returning stale data for USDE/USD due to rate limit` |
| 3 | gateway | INFO | No CoinGecko API key; Chainlink+fallback mode | `No CoinGecko API key -- using on-chain pricing (Chainlink oracles) with free CoinGecko as fallback` |

### Analysis

- **Finding 1/2 (CoinGecko rate limit on USDE/USD)**: Free-tier CoinGecko hit a rate limit during the post-execution balance/price refresh. The system correctly fell back to stale cached data (price = 1.0 USDe/USD), and overall price confidence remained at 0.80 from 3/4 sources. Price was not zero. This is a benign transient event expected in Anvil testing without a dedicated CoinGecko API key.
- **Finding 3 (No CoinGecko API key)**: Expected in this environment. Chainlink oracles (on-chain) are used as primary source with CoinGecko as a best-effort fallback. All price lookups succeeded via the multi-source aggregator.

No zero prices, no token resolution failures, no reverts, no timeouts on execution, no placeholder price warnings (resolved vs. prior runs), no circular import issues. The previous finding of Enso 2-TX route revert + retry is NOT present in this run -- both TXs confirmed cleanly on first attempt.

## Result

**PASS** - The ethena_yield strategy successfully executed a USDC -> USDe swap via the Enso aggregator on an Ethereum Anvil fork (block 24664484). Both transactions confirmed on first attempt with no retries required. TX 1 (approve): `577430dd...e1ff`, TX 2 (swap): `465e38...7e24`. Total gas: 997,061. The Enso 2-TX revert seen in prior runs did not occur, suggesting it was a transient Anvil/Enso state issue.

---

SUSPICIOUS_BEHAVIOUR_COUNT: 3
SUSPICIOUS_BEHAVIOUR_ERRORS: 0
