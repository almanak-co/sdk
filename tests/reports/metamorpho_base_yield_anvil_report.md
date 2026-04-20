# E2E Strategy Test Report: metamorpho_base_yield (Anvil)

**Date:** 2026-03-16 01:13 (kitchen-iter-81)
**Result:** PASS
**Mode:** Anvil
**Duration:** ~3 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | metamorpho_base_yield |
| Chain | base |
| Network | Anvil fork (Alchemy) |
| Anvil Port | 52995 (auto-assigned by managed gateway) |
| Vault | Moonwell Flagship USDC (0xc1256Ae5FF1cf2719D4937adb3bbCCab2E00A2Ca) |
| Deposit Amount | 50 USDC |

## Config Changes Made

None. `deposit_amount` is already 50 USDC (within $1000 budget cap). No `force_action` field supported by this strategy.

## Execution

### Setup
- [x] Managed gateway auto-started by `--network anvil` flag
- [x] Anvil fork started: Base mainnet, block 43404530, chain_id 8453 (Alchemy RPC)
- [x] Wallet funded from config `anvil_funding`: 10 ETH, 50,000 USDC (slot 9)
- [x] Wallet: 0x54776446Aa29Fc49d152B4850bD410eA1E4d24bF
- [x] Strategy started in FRESH START mode (no existing state)

### Strategy Run
- [x] Strategy executed with `--network anvil --once`
- [x] State machine: `idle` -> decided VAULT_DEPOSIT -> `depositing` -> `deposited`
- [x] Intent: VAULT_DEPOSIT of 50 USDC into Moonwell Flagship vault
- [x] Compilation: SUCCESS (MetaMorphoAdapter, 2 transactions: USDC approve + MetaMorpho deposit)
- [x] Simulation: SUCCESS (2 TXs, total gas estimate 730,819)
- [x] Execution: SUCCESS (sequential submit, both TXs confirmed on-chain)
- [x] Result enricher extracted `deposit_data` from vault receipt

### Key Log Output

```text
[INFO] Anvil fork started: port=52995, block=43404530, chain_id=8453
[INFO] Funded 0x54776446... with 10 ETH
[INFO] Funded 0x54776446... with USDC via known slot 9
[INFO] DEPOSIT: 50 USDC into Moonwell vault on Base (available: 50000, alloc cap: 80%)
[INFO] Compiled VAULT_DEPOSIT: 50 USDC into vault 0xc1256Ae5...
[INFO] Simulation successful: 2 transaction(s), total gas: 730819
[INFO] Transaction confirmed: tx_hash=ee668abc...04a9, block=43404532, gas_used=55437
[INFO] Transaction confirmed: tx_hash=1d5aa85e...f528, block=43404533, gas_used=348827
[INFO] EXECUTED: VAULT_DEPOSIT completed successfully
[INFO] Txs: 2 (ee668a...04a9, 1d5aa8...f528) | 404,264 gas
[INFO] Enriched VAULT_DEPOSIT result with: deposit_data (protocol=metamorpho, chain=base)
[INFO] VAULT_DEPOSIT successful -> state=deposited
Status: SUCCESS | Intent: VAULT_DEPOSIT | Gas used: 404264 | Duration: 25557ms
```

## Transactions

| # | TX Hash | Block | Gas Used | Action |
|---|---------|-------|----------|--------|
| 1 | `0xee668abc0a85b0435b127e23f6fc111749f2ae688e1ec608547196c0be3104a9` | 43404532 | 55,437 | USDC approve |
| 2 | `0x1d5aa85eab3e4c82032cf0933749428fe87a8324ce4533eb509be46a2354f528` | 43404533 | 348,827 | MetaMorpho deposit |

**Total gas used:** 404,264

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | gateway | INFO | No CoinGecko API key, using fallback | `No CoinGecko API key -- using on-chain pricing (Chainlink oracles) with free CoinGecko as fallback.` |
| 2 | strategy | WARNING | Placeholder prices in compiler | `IntentCompiler using PLACEHOLDER PRICES. Slippage calculations will be INCORRECT. This is only acceptable for unit tests.` |

**Notes:**

- Finding 1 (INFO): Expected behaviour when no `COINGECKO_API_KEY` is set. Chainlink oracles are the primary source; USDC resolved at 1.0 with confidence 1.00 across 4/4 sources. No rate limiting observed this run.
- Finding 2 (Placeholder prices): Expected Anvil behaviour. The IntentCompiler uses placeholder prices when live gateway prices are not injected at compile time. For a VAULT_DEPOSIT (no swap, no slippage parameter), this has zero impact on execution correctness.

## Result

**PASS** - `metamorpho_base_yield` executed a successful VAULT_DEPOSIT of 50 USDC into the Moonwell Flagship USDC MetaMorpho vault on a Base Anvil fork (iter-81). Both transactions (USDC approval + vault deposit) confirmed on-chain with 404,264 total gas. State transitioned from `idle` to `deposited`. Result enricher extracted `deposit_data` from the vault receipt. No CoinGecko rate limiting observed (improved from iter-52, where 2 rate-limit retries were logged).

---

SUSPICIOUS_BEHAVIOUR_COUNT: 2
SUSPICIOUS_BEHAVIOUR_ERRORS: 0
