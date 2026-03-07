# E2E Strategy Test Report: metamorpho_base_yield (Anvil)

**Date:** 2026-03-06 05:15 (kitchen-iter-52)
**Result:** PASS
**Mode:** Anvil
**Duration:** ~4 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | metamorpho_base_yield |
| Chain | base |
| Network | Anvil fork (publicnode.com) |
| Anvil Port | 56729 (auto-assigned by managed gateway) |
| Vault | Moonwell Flagship USDC (0xc1256Ae5FF1cf2719D4937adb3bbCCab2E00A2Ca) |

## Config Changes Made

| Field | Original | Modified | Reason |
|-------|----------|---------|--------|
| `deposit_amount` | `"1000"` | `"50"` | Budget cap: $50 maximum per trade |
| `min_deposit_usd` | `"100"` | `"10"` | Proportionally reduced so strategy proceeds |

Config restored to original values after the test.

## Execution

### Setup
- [x] Stale strategy state cleared from `almanak_state.db` (prior run had `state=deposited, epoch=1`)
- [x] Managed gateway auto-started by `--network anvil` flag
- [x] Anvil fork started: Base mainnet, block 42979785, chain_id 8453
- [x] Public RPC used: https://base-rpc.publicnode.com (no ALCHEMY_API_KEY configured)
- [x] Wallet funded from config `anvil_funding`: 10 ETH, 50,000 USDC
- [x] Wallet: 0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266 (Anvil default)
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
[INFO] Anvil fork started: port=56729, block=42979785, chain_id=8453
[INFO] Funded 0xf39Fd6e5... with 10 ETH
[INFO] Funded 0xf39Fd6e5... with USDC via known slot 9
[INFO] DEPOSIT: 50 USDC into Moonwell vault on Base (available: 50000, alloc cap: 80%)
[INFO] Compiled VAULT_DEPOSIT: 50 USDC into vault 0xc1256Ae5...
[INFO] Simulation successful: 2 transaction(s), total gas: 730819
[INFO] Transaction confirmed: tx_hash=fcdf4864...d817, block=42979787, gas_used=55437
[INFO] Transaction confirmed: tx_hash=e70eddf7...c6c4, block=42979788, gas_used=348827
[INFO] EXECUTED: VAULT_DEPOSIT completed successfully
[INFO] Txs: 2 (fcdf48...d817, e70edd...c6c4) | 404,264 gas
[INFO] Enriched VAULT_DEPOSIT result with: deposit_data (protocol=metamorpho, chain=base)
[INFO] VAULT_DEPOSIT successful -> state=deposited
Status: SUCCESS | Intent: VAULT_DEPOSIT | Gas used: 404264 | Duration: 38342ms
```

## Transactions

| # | TX Hash | Block | Gas Used | Action |
|---|---------|-------|----------|--------|
| 1 | `0xfcdf4864dcd5082ecff169db4acca2d38b48f35261ca12bb7e0f11219109d817` | 42979787 | 55,437 | USDC approve |
| 2 | `0xe70eddf7f7098e32c1a13e46806f53ab0ec3c01fefd9c5fffe36cf291f7cc6c4` | 42979788 | 348,827 | MetaMorpho deposit |

**Total gas used:** 404,264

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | gateway | WARNING | CoinGecko rate limited | `Rate limited by CoinGecko for USDC/USD, backoff: 1.00s` |
| 2 | gateway | WARNING | CoinGecko rate limited (2nd attempt) | `Rate limited by CoinGecko for USDC/USD, backoff: 2.00s` |
| 3 | strategy | WARNING | Placeholder prices in compiler | `IntentCompiler using PLACEHOLDER PRICES. Slippage calculations will be INCORRECT. This is only acceptable for unit tests.` |
| 4 | gateway | INFO | No CoinGecko API key, using fallback | `No CoinGecko API key -- using on-chain pricing (Chainlink oracles) with free CoinGecko as fallback.` |

**Notes:**

- Findings 1 and 2 (CoinGecko rate limiting): The free CoinGecko tier hits rate limits quickly when no `COINGECKO_API_KEY` is configured. Despite rate limits, USDC/USD price was resolved at 1.00 (confidence 0.90, 1/2 sources) via on-chain Chainlink fallback. Not a blocker but adds ~3s of latency per price fetch. Resolution: set `ALMANAK_GATEWAY_COINGECKO_API_KEY` in `.env`.
- Finding 3 (Placeholder prices): Expected Anvil behaviour. The IntentCompiler uses placeholder prices when live gateway prices are not injected at compile time. For a VAULT_DEPOSIT (no swap, no slippage parameter), this has zero impact on execution correctness.
- Finding 4 (INFO): Expected behaviour when no `COINGECKO_API_KEY` is set. On-chain Chainlink oracle provides the primary price.

## Result

**PASS** - `metamorpho_base_yield` executed a successful VAULT_DEPOSIT of 50 USDC into the Moonwell Flagship USDC MetaMorpho vault on a Base Anvil fork. Both transactions (USDC approval + vault deposit) confirmed on-chain with 404,264 total gas. State transitioned from `idle` to `deposited`. Result enricher extracted `deposit_data` from the vault receipt.

---

SUSPICIOUS_BEHAVIOUR_COUNT: 4
SUSPICIOUS_BEHAVIOUR_ERRORS: 0
