# E2E Strategy Test Report: enso_rsi (Anvil)

**Date:** 2026-02-27 15:49
**Result:** FAIL
**Mode:** Anvil
**Duration:** ~2 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | demo_enso_rsi |
| Chain | base (Chain ID: 8453) |
| Network | Anvil fork (managed, auto-started by CLI) |
| Anvil Port | 58054 (auto-assigned by managed gateway) |
| Anvil Fork Block | 42709011 |
| Trade Size | $3 USD (original, unchanged — well under $500 cap) |
| Base Token | WETH |
| Quote Token | USDC |

## Config Changes Made

| Field | Before | After (test) | Restored |
|-------|--------|--------------|---------|
| `force_action` | (absent) | `"buy"` | removed (restored to original) |

`force_action` was added temporarily and removed after the test.
Trade size was already `"3"` (well under $500 cap); no change needed.

## Execution

### Setup
- [x] Killed existing gateway processes on ports 50051 and 9090 before run
- [x] Managed gateway auto-started by CLI on 127.0.0.1:50051
- [x] Anvil fork auto-started on port 58054 (forked Base from https://base-rpc.publicnode.com at block 42709011)
- [ ] Wallet NOT funded -- managed gateway skipped Anvil funding because `ALMANAK_PRIVATE_KEY` was not found in the subprocess environment (warning logged)
- [x] `force_action: "buy"` set in config to trigger immediate trade

**Note on funding skip:** The gateway logged `No wallet address or ALMANAK_PRIVATE_KEY set -- skipping Anvil funding`. The key is in `.env` but was not exported into the subprocess started by the managed gateway. Since the strategy failed at intent compilation (before any on-chain call), the missing funding did not affect the outcome. However, this is a secondary concern for other strategies that do reach execution.

### Strategy Run
- [x] Strategy executed with `--network anvil --once`
- [x] Force action "buy" triggered: strategy returned SWAP intent ($3.00 USDC -> WETH via Enso)
- [x] Intent compiler attempted to call Enso API to obtain a route
- [ ] FAILED: `EnsoConfigError: Configuration Error: API key is required. Set ENSO_API_KEY env var or pass api_key.`
- [ ] 3 retries attempted, all failed with the same error
- [ ] Intent exhausted all retries; run terminated with `EXECUTION_FAILED`

### Root Cause

`ENSO_API_KEY` is empty in `.env`. The Enso compiler requires a live API key to call the
Enso Finance REST API for routing -- this call happens at compile time, even on Anvil.
The gateway's `EnsoService` was also initialized with `available=False` for the same reason.
Without a valid key the strategy cannot execute any swap at all.

### Key Log Output
```text
info    EnsoService initialized: available=False [almanak.gateway.services.enso_service]
info    Force action requested: buy [strategy_module]
info    BUY via Enso: $3.00 USDC -> WETH, slippage=1.0% [strategy_module]
info    Getting Enso route: USDC -> WETH, amount=3000000 [almanak.framework.intents.compiler]
error   Failed to compile Enso SWAP intent: Configuration Error: API key is required.
        Set ENSO_API_KEY env var or pass api_key. (Parameter: api_key)
warning Step error: Configuration Error: API key is required... (retry 0/3)
warning Step error: Configuration Error: API key is required... (retry 1/3)
warning Step error: Configuration Error: API key is required... (retry 2/3)
warning Step error: Configuration Error: API key is required... (retry 3/3)
error   Intent failed after 3 retries: Configuration Error: API key is required...
Status: EXECUTION_FAILED | Intent: SWAP | Error: Configuration Error: API key is required... | Duration: 7336ms
```

## Transactions

No on-chain transactions were produced. The failure occurred at intent compilation (pre-chain),
before any transaction was submitted.

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | gateway | INFO | EnsoService unavailable (no API key) | `EnsoService initialized: available=False` |
| 2 | strategy | WARNING | Wallet funding skipped by managed gateway | `No wallet address or ALMANAK_PRIVATE_KEY set -- skipping Anvil funding` |
| 3 | strategy | WARNING | Placeholder prices in compiler | `IntentCompiler using PLACEHOLDER PRICES. Slippage calculations will be INCORRECT. This is only acceptable for unit tests.` |
| 4 | strategy | ERROR | Missing ENSO_API_KEY (root cause) | `Failed to compile Enso SWAP intent: Configuration Error: API key is required. Set ENSO_API_KEY env var or pass api_key.` |
| 5 | strategy | ERROR | Intent exhausted retries | `Intent failed after 3 retries: Configuration Error: API key is required.` |

### Findings Analysis

**Finding 1 - EnsoService unavailable (INFO)**: The gateway correctly detected that `ENSO_API_KEY`
is missing and initialized `EnsoService` as `available=False`. However, the strategy compiler still
tries to call the Enso API directly (not routing through the gateway's EnsoService abstraction).

**Finding 2 - Wallet funding skipped (WARNING)**: The managed gateway subprocess did not inherit
`ALMANAK_PRIVATE_KEY` from the environment. Anvil test wallet was not funded. For strategies that
reach execution, this would cause on-chain reverts due to zero token balances. The enso_rsi
strategy in particular uses USDC as the swap input; with no USDC balance, even a successful Enso
compile would revert.

**Finding 3 - Placeholder Prices (WARNING)**: Known Anvil limitation. IntentCompiler uses
placeholder prices in managed Anvil mode because no live price oracle is connected at compile time.
This means slippage enforcement is not accurate.

**Findings 4-5**: Direct cause of the FAIL. All compile attempts hit the same configuration error.

## Result

**FAIL** -- The `enso_rsi` strategy requires a valid `ENSO_API_KEY` to call the Enso Finance API
at compile time. With `ENSO_API_KEY=` (empty) in `.env`, no swap can be compiled or executed even
on Anvil. Set `ENSO_API_KEY` in `.env` and re-run to get a PASS result.

---

SUSPICIOUS_BEHAVIOUR_COUNT: 5
SUSPICIOUS_BEHAVIOUR_ERRORS: 2
