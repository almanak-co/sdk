# E2E Strategy Test Report: enso_rsi (Anvil)

**Date:** 2026-02-27 08:54
**Result:** FAIL
**Mode:** Anvil
**Duration:** ~12 seconds

## Configuration

| Field | Value |
|-------|-------|
| Strategy | demo_enso_rsi |
| Chain | base (Chain ID: 8453) |
| Network | Anvil fork (managed, auto-started by CLI) |
| Anvil Port | 63203 (auto-assigned by managed gateway) |
| Trade Size | $3 USD (original, unchanged — well under $500 cap) |
| Base Token | WETH |
| Quote Token | USDC |

## Config Changes Made

| Field | Before | After | Reason |
|-------|--------|-------|--------|
| `force_action` | not set | `"buy"` | Trigger immediate trade (removed after test) |

`force_action` was added temporarily and restored to original (removed) after the test.
Trade size was already $3 (well under $500 cap); no change needed.

## Execution

### Setup

- Killed existing gateway processes on ports 50051 and 9090 before run
- Managed gateway auto-started by CLI on 127.0.0.1:50051
- Anvil fork auto-started on port 63203 (forked Base from https://base-rpc.publicnode.com at block 42696565)
- Wallet `0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266` funded by managed gateway:
  - 100 ETH
  - 1 WETH (via known slot 3)
  - 10,000 USDC (via known slot 9)
- `force_action: "buy"` set in config to trigger immediate trade

### Strategy Run

- Strategy executed with `--network anvil --once`
- Force action "buy" triggered: strategy returned SWAP intent ($3.00 USDC -> WETH via Enso)
- Intent compiler attempted to call Enso API to obtain a route
- **FAILED**: `EnsoConfigError: Configuration Error: API key is required. Set ENSO_API_KEY env var or pass api_key.`
- 3 retries attempted, all failed with the same error
- Intent exhausted all retries; run terminated with `EXECUTION_FAILED`

### Root Cause

`ENSO_API_KEY` is empty in `.env`. The Enso compiler requires a live API key to call the Enso
Finance REST API for routing — this call happens at compile time, even on Anvil. The gateway's
`EnsoService` was also initialized with `available=False` for the same reason.

A previous run (2026-02-27 02:23) succeeded by sourcing `ENSO_API_KEY` from `.power-env`.
Without a valid key the strategy cannot execute any swap at all.

### Key Log Output

```text
info   EnsoService initialized: available=False [almanak.gateway.services.enso_service]
info   Force action requested: buy [strategy_module]
info   BUY via Enso: $3.00 USDC -> WETH, slippage=1.0% [strategy_module]
info   Getting Enso route: USDC -> WETH, amount=3000000 [almanak.framework.intents.compiler]
error  Failed to compile Enso SWAP intent: Configuration Error: API key is required.
       Set ENSO_API_KEY env var or pass api_key. (Parameter: api_key)
warning Step error: Configuration Error: API key is required... (retry 0/3)
warning Step error: Configuration Error: API key is required... (retry 1/3)
warning Step error: Configuration Error: API key is required... (retry 2/3)
warning Step error: Configuration Error: API key is required... (retry 3/3)
error  Intent failed after 3 retries: Configuration Error: API key is required...
Status: EXECUTION_FAILED | Intent: SWAP | Error: Configuration Error: API key is required... | Duration: 7305ms
```

## Transactions

No on-chain transactions were produced. The failure occurred at intent compilation (pre-chain),
before any transaction was submitted.

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | gateway | INFO | EnsoService unavailable | `EnsoService initialized: available=False` |
| 2 | strategy | WARNING | Placeholder prices in compiler | `IntentCompiler using PLACEHOLDER PRICES. Slippage calculations will be INCORRECT. This is only acceptable for unit tests.` |
| 3 | strategy | ERROR | Missing ENSO_API_KEY (root cause) | `Failed to compile Enso SWAP intent: Configuration Error: API key is required. Set ENSO_API_KEY env var or pass api_key.` |
| 4 | strategy | ERROR | Intent exhausted retries | `Intent failed after 3 retries: Configuration Error: API key is required.` |

### Findings Analysis

**Finding #1 - EnsoService unavailable (INFO)**: The gateway correctly detected that `ENSO_API_KEY`
is missing and initialized `EnsoService` as `available=False`. However, the strategy compiler still
tries to call the Enso API directly (not routing through the gateway's EnsoService abstraction).
This is the root cause of failure.

**Finding #2 - Placeholder Prices (WARNING)**: Known Anvil limitation. The IntentCompiler uses
placeholder prices in managed Anvil mode because no live price oracle is connected. This warning is
expected and does not affect correctness in isolation, but it means slippage is not reliably enforced.

**Findings #3-4**: Direct cause of the FAIL. All 4 compile attempts (initial + 3 retries) hit the
same configuration error because no API key is available.

## Result

**FAIL** — The `enso_rsi` strategy requires a valid `ENSO_API_KEY` to call the Enso Finance API
at compile time. With `ENSO_API_KEY=` (empty) in `.env`, no swap can be compiled or executed even
on Anvil. Set `ENSO_API_KEY` in `.env` and re-run to get a PASS result.

---

SUSPICIOUS_BEHAVIOUR_COUNT: 4
SUSPICIOUS_BEHAVIOUR_ERRORS: 2
