# E2E Strategy Test Report: ethena_yield (Anvil)

**Date:** 2026-02-22 20:50
**Result:** PASS
**Mode:** Anvil
**Duration:** ~2 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | ethena_yield |
| Chain | ethereum |
| Network | Anvil fork (Ethereum mainnet) |
| Anvil Port | 60470 (managed gateway auto-port) |
| Wallet | 0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266 |

### Config Values

```json
{
    "min_stake_amount": "5",
    "min_usdc_amount": "5",
    "swap_usdc_to_usde": true,
    "force_action": "swap",
    "chain": "ethereum"
}
```

Config changes made: None. Trade size is 5 USDC (well under the $100 budget cap). `force_action: "swap"` was already set to trigger an immediate SWAP of 5 USDC -> USDe via Enso.

## Execution

### Setup
- Anvil fork of Ethereum mainnet started (chain_id=1 confirmed)
- Managed gateway auto-started on port 50052
- Wallet funded by managed gateway: 100 ETH, 10,000 USDC, 1 WETH
- Strategy loaded persisted state (swapped=True, staked=True from a prior run), but `force_action: "swap"` bypassed state checks and forced a fresh swap

### Strategy Run

The strategy emitted a SWAP intent: 5 USDC -> USDe via Enso aggregator.

Enso route resolved successfully:
- Route: `0xA0b86991...` (USDC) -> `0x4c9EDD58...` (USDe)
- Amount out: 5.0056 USDe (min: 4.9805 USDe with 0.5% slippage)
- Price impact: 0 bp

Two transactions submitted and confirmed:

| # | Purpose | TX Hash | Block | Gas Used |
|---|---------|---------|-------|----------|
| 1 | Approve (USDC -> Permit2) | `927b6096ad83d624306237df50b4801102aadd7f315a3ec55dbaf4ad046328bb` | 24514915 | 55,558 |
| 2 | Enso Swap (USDC -> USDe) | `57c288001ed9185cc47712e50d5fc27dc797acd668350acebf2076e46f7401bd` | 24514916 | 490,260 |

**Total gas used: 545,818**

### Key Log Output

```text
[info] Forced action: SWAP USDC -> USDe
[info] SWAP intent: 5.0000 USDC -> USDe via Enso (slippage=0.5%)
[info] EthenaYieldStrategy intent: SWAP: 5 USDC -> USDe (slippage: 0.50%) via enso
[info] Getting Enso route: USDC -> USDE, amount=5000000
[info] Route found: 0xA0b86991... -> 0x4c9EDD58..., amount_out=5005566999634751419, price_impact=0bp
[info] Compiled SWAP (Enso): 5.0000 USDC -> 5.0056 USDE (min: 4.9805 USDE)
[info] Transaction confirmed: tx_hash=927b60...28bb, block=24514915, gas_used=55558
[info] Transaction confirmed: tx_hash=57c288...01bd, block=24514916, gas_used=490260
[info] EXECUTED: SWAP completed successfully
[info] Txs: 2 (927b60...28bb, 57c288...01bd) | 545,818 gas
[info] Swap successful: 5 USDC -> USDe
Status: SUCCESS | Intent: SWAP | Gas used: 545818 | Duration: 21648ms
```

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | strategy | INFO | Insecure mode (expected) | `INSECURE MODE: Auth interceptor disabled - no auth_token configured. This is acceptable for local development on 'anvil'.` |
| 2 | strategy | INFO | CoinGecko free tier | `COINGECKO_API_KEY not configured - CoinGecko will use free tier API (30 requests/minute limit)` |
| 3 | strategy | WARNING | Placeholder prices in compiler | `IntentCompiler using PLACEHOLDER PRICES. Slippage calculations will be INCORRECT. This is only acceptable for unit tests.` |
| 4 | strategy | WARNING | Amount chaining failure | `Amount chaining: no output amount extracted from step 1; subsequent amount='all' steps will fail` |
| 5 | strategy | INFO | Port not freed timely | `Port 60470 not freed after 5.0s` |

**Finding analysis:**

- **Finding 1 (Insecure mode)**: Expected for Anvil testing. Not a bug.
- **Finding 2 (CoinGecko free tier)**: Normal for local dev. Rate limit could cause intermittent failures in intensive batch testing but was not triggered here.
- **Finding 3 (Placeholder prices)**: The IntentCompiler does not have real price data during compilation on Anvil, so slippage bounds are computed from placeholder values. In practice, Enso provides the real `amount_out`, so the actual swap route is correct. However, the slippage guard in the compiler is inaccurate. This is a known limitation in Anvil mode. Would be an ERROR in production if placeholder prices were used for real slippage enforcement.
- **Finding 4 (Amount chaining)**: After the SWAP step completed, the framework could not extract the exact USDe output amount from the Enso swap receipt. If a subsequent intent used `amount='all'` USDe (e.g., to stake immediately after swapping), it would fail. This indicates the Enso receipt parser does not return `swap_amounts` in a way the runner can use for chaining. No impact on this single-step strategy run.
- **Finding 5 (Port not freed)**: Minor infrastructure issue during shutdown. Not a strategy bug.

## Result

**PASS** - The ethena_yield strategy successfully executed a SWAP of 5 USDC -> ~5.006 USDe via the Enso aggregator on an Ethereum mainnet Anvil fork, submitting 2 on-chain transactions (approve + swap) totalling 545,818 gas.

---

SUSPICIOUS_BEHAVIOUR_COUNT: 5
SUSPICIOUS_BEHAVIOUR_ERRORS: 0
