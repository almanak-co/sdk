# E2E Strategy Test Report: balancer_flash_arb (Anvil)

**Date:** 2026-03-16 00:37
**Result:** FAIL
**Mode:** Anvil
**Duration:** ~4 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | balancer_flash_arb |
| Chain | arbitrum |
| Network | Anvil fork (managed, auto-started) |
| Anvil Port | 62359 (auto-assigned by managed gateway) |
| Config changes | None (flash_loan_amount_usd=1000 already at budget cap, force_action=flash_loan already set) |

## Execution

### Setup
- [x] Anvil started on port 62359 (managed gateway auto-started its own fork)
- [x] Gateway started on port 50052 (managed, auto-started by `strat run`)
- [x] Wallet funded: 100 ETH, 1 WETH, 10,000 USDC (via managed gateway anvil_funding config)

### Strategy Run
- [x] Strategy executed with `--network anvil --once`
- [ ] Intent compiled: FAIL -- FLASH_LOAN intent failed compilation on all 3 retries

### Intent Execution Summary

| Intent | Step | Result |
|--------|------|--------|
| FLASH_LOAN (provider=balancer, token=USDC, amount=1000) | Compilation | FAIL: "Price for 'WETH' is missing in the price oracle" |
| SWAP callback 1 (USDC->WETH via Enso) | Compilation | SUCCESS (compiled correctly) |
| SWAP callback 2 (WETH->USDC via Enso, amount='all') | Pre-flight estimate | FAIL: `_estimate_callback_output` calls `_require_token_price('WETH')` but WETH price is not in oracle |

No on-chain transaction was attempted.

### Root Cause Analysis

The failure is a **price oracle gap in `_fetch_prices_for_intent`** for FLASH_LOAN intents.

**Code path:**

1. `inner_runner.py:_fetch_prices_for_intent()` extracts token symbols to pre-fetch prices from the gateway
2. It scans only top-level intent params keys: `from_token`, `to_token`, `token`, `token_a`, `token_b`, `borrow_token`, `collateral_token`
3. For `FLASH_LOAN`, only the top-level `token` field (`USDC`) is found -- callback intent tokens (`WETH`) are nested inside `callback_intents` and are never extracted
4. Price oracle is updated with only `{USDC: 0.9999}` -- no ETH/WETH price
5. `_compile_flash_loan` calls `_estimate_callback_output` on the first callback (USDC->WETH)
6. `_estimate_callback_output` calls `_calculate_expected_output(USDC, WETH)`
7. `_require_token_price('WETH')` checks the oracle -- not found. Tries `_WRAPPED_TO_NATIVE['WETH'] = 'ETH'` -- also not found
8. Raises `ValueError: Price for 'WETH' is missing`

**The fix** requires `_fetch_prices_for_intent` (or a caller) to also extract token symbols from `callback_intents` when the intent type is `FLASH_LOAN`.

**File:** `almanak/framework/runner/inner_runner.py`, function `_fetch_prices_for_intent`, line ~217

**Fix pattern:**
```python
# For flash loan intents, also extract tokens from callback_intents
for cb in intent_params.get("callback_intents", []):
    for key in ("from_token", "to_token", "token"):
        val = cb.get(key)
        if val and not val.startswith("0x"):
            symbols.add(val)
```

### Key Log Output
```text
2026-03-15T17:37:19.575248Z [info] Getting Enso route: USDC -> WETH, amount=1000000000
2026-03-15T17:37:23.865574Z [info] Route found: USDC -> WETH, amount_out=476921335346741902, price_impact=1bp
2026-03-15T17:37:23.865813Z [info] Compiled SWAP (Enso): 1000.0000 USDC → 0.4769 WETH (min: 0.4722 WETH)
2026-03-15T17:37:23.866665Z [error] Failed to compile FLASH_LOAN intent: Price for 'WETH' is missing in the price oracle.
...
2026-03-15T17:37:39.391794Z [error] Intent failed after 3 retries: Price for 'WETH' is missing in the price oracle.
2026-03-15T17:37:39.398513Z [error] PRE-EXECUTION FAILURE: Price for 'WETH' is missing in the price oracle.
  Intent: FLASH_LOAN | Chain: arbitrum
  No on-chain transaction was attempted (compilation or validation error).
Status: EXECUTION_FAILED | Duration: 20356ms
```

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | strategy | ERROR | Missing WETH price in oracle during FLASH_LOAN compilation | `Failed to compile FLASH_LOAN intent: Price for 'WETH' is missing in the price oracle. Compilation requires a valid price to calculate amounts and slippage.` |
| 2 | strategy | WARNING | Intent retry storm (same error repeated 4x) | `Step error: Price for 'WETH' is missing in the price oracle. (retry 0/3 ... retry 3/3)` |
| 3 | strategy | INFO | No CoinGecko API key, using Chainlink + fallback | `No CoinGecko API key -- using on-chain pricing (Chainlink oracles) with free CoinGecko as fallback.` |

Note: Finding #1 is the root cause bug. Finding #2 is a consequence (retries don't help because
the price fetch is structurally broken for flash loan callback tokens). Finding #3 is normal
for this environment and does not contribute to the failure.

## Result

**FAIL** - The `balancer_flash_arb` strategy fails at compilation with "Price for 'WETH' is missing
in the price oracle" because `_fetch_prices_for_intent` in `inner_runner.py` does not extract token
symbols from nested `callback_intents` inside FLASH_LOAN intents -- only the top-level `token`
field (USDC) is fetched, leaving WETH with no price for the `_estimate_callback_output` step.

**Required fix:** `almanak/framework/runner/inner_runner.py:_fetch_prices_for_intent` -- add extraction
of tokens from `intent_params["callback_intents"]` when present.
