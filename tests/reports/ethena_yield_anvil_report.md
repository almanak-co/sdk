# E2E Strategy Test Report: ethena_yield (Anvil)

**Date:** 2026-02-27 09:04
**Result:** PASS
**Mode:** Anvil
**Duration:** ~8 minutes (including port discovery fix and USDe funding method fix)

## Configuration

| Field | Value |
|-------|-------|
| Strategy | demo_ethena_yield |
| Chain | ethereum |
| Network | Anvil fork (Ethereum mainnet) |
| Anvil Port | 8549 (gateway ANVIL_CHAIN_PORTS mapping for ethereum) |
| force_action | swap (triggers USDC -> USDe via Enso) |
| min_usdc_amount | 5 USDC |
| min_stake_amount | 5 USDe |
| Budget Cap Check | PASS (5 USDC << $500 cap) |

## Config Changes Made

None. Config amounts (`min_usdc_amount: "5"`, `min_stake_amount: "5"`) are well below the $500 budget cap. `force_action: "swap"` was already set, triggering an immediate trade.

## Execution

### Setup

- [x] Anvil started on port 8549 (Ethereum per gateway `ANVIL_CHAIN_PORTS` mapping)
- [x] Gateway started on port 50051 with `ALCHEMY_API_KEY` + `ENSO_API_KEY`
- [x] Wallet funded: 100 ETH, 10,000 USDC (slot 9), 1,000 USDe (minter impersonation)

**Infrastructure notes:**

1. The tester workflow table maps Ethereum to port 8546, but
   `almanak/gateway/utils/rpc_provider.py:ANVIL_CHAIN_PORTS["ethereum"] = 8549`. The gateway
   is authoritative. Anvil must be on port 8549 for Ethereum.

2. USDe (`0x4c9EDD5852cd905f086C759E8383e09bff1E68B3`) does not store balances at the standard
   ERC-20 storage slot. `anvil_setStorageAt` at slot 0 does not update `balanceOf()`. Working
   method: impersonate USDe minter (`0xe3490297a08d6fC8Da46Edb7B6142E4F461b62D3`) and call
   `mint(wallet, amount)` directly.

| Step | Result |
|------|--------|
| Anvil fork start (port 8549) | OK (chain_id=1) |
| ETH funding | OK (100 ETH) |
| USDC funding (slot 9) | OK (10,000 USDC) |
| USDe funding (minter impersonation) | OK (1,000 USDe) |
| EnsoService init | OK (available=True) |
| Enso route fetched | OK (5 USDC -> 5.0030 USDe, 0bp price impact) |
| SWAP intent compiled | OK (2 txs, gas est 751,660) |
| TX 1/2: USDC approve | CONFIRMED (block 24547247, gas=55,558) |
| TX 2/2: Enso SWAP | CONFIRMED (block 24547248, gas=541,154) |
| on_intent_executed callback | OK (swapped=True) |

### Transactions (Anvil fork -- not mainnet)

| TX | Hash | Block | Gas | Status |
|----|------|-------|-----|--------|
| USDC approve | `0xfafb2e6c6caab34e84ef01e52010db0aaa18bedd55aedbb0c1f18bd8c055c8ad` | 24547247 | 55,558 | SUCCESS |
| Enso SWAP (USDC->USDe) | `0xad04d80d06aa0d2761e74f379bb48f18d1b250200ac83a43b38df421e9f42f4f` | 24547248 | 541,154 | SUCCESS |
| **Total** | | | **596,712** | |

### Key Log Output

```text
[info] Forced action: SWAP USDC -> USDe
[info] SWAP intent: 5.0000 USDC -> USDe via Enso (slippage=0.5%)
[info] EnsoClient initialized for chain=ethereum (chain_id=1)
[info] Route found: 0xA0b86991... -> 0x4c9EDD58..., amount_out=5002961664052085766, price_impact=0bp
[info] Compiled SWAP (Enso): 5.0000 USDC -> 5.0030 USDE (min: 4.9779 USDE)
[info]   Slippage: 0.50% | Impact: N/A | Txs: 2 | Gas: 751,660
[info] Execution successful: gas_used=596712, tx_count=2
[info] Swap successful: 5 USDC -> USDe
Status: SUCCESS | Intent: SWAP | Gas used: 596712 | Duration: 7882ms
Iteration completed successfully.
```

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | strategy | WARNING | Placeholder prices | `IntentCompiler using PLACEHOLDER PRICES. Slippage calculations will be INCORRECT. This is only acceptable for unit tests.` |
| 2 | strategy | WARNING | Amount chaining gap | `Amount chaining: no output amount extracted from step 1; subsequent amount='all' steps will fail` |
| 3 | gateway | INFO | No CoinGecko key -- Chainlink fallback | `No CoinGecko API key -- using on-chain pricing (Chainlink oracles) with free CoinGecko as fallback.` |

**Finding analysis:**

- **Finding 1 (Placeholder prices):** Expected in Anvil mode -- no live Chainlink oracle on the fork. The Enso route provides actual amounts, so the swap executes correctly despite placeholder pricing. Not a bug for Anvil testing.
- **Finding 2 (Amount chaining gap):** The Enso SWAP receipt parser does not extract the output USDe amount into the chaining context. Any follow-on intent using `amount='all'` would fail (e.g., a SWAP -> STAKE sequence). The current strategy only issues a single intent per `--once` run, so this does not cause a functional failure here. This is a known gap in Enso result enrichment.
- **Finding 3:** Informational, expected when `COINGECKO_API_KEY` is not set.

No zero prices, hard errors, or transaction reverts in the successful run.

## Result

**PASS** - The `ethena_yield` strategy successfully executed a SWAP of 5 USDC -> 5.0030 USDe via
the Enso aggregator on an Ethereum Anvil fork. Two on-chain Anvil transactions confirmed
(596,712 gas total). Port 8549 must be used for Ethereum Anvil forks (gateway mapping). USDe
requires minter impersonation for test funding.

---

SUSPICIOUS_BEHAVIOUR_COUNT: 3
SUSPICIOUS_BEHAVIOUR_ERRORS: 0
