# E2E Strategy Test Report: morpho_looping (Anvil)

**Date:** 2026-02-23 03:54
**Result:** PASS
**Mode:** Anvil
**Duration:** ~3 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | demo_morpho_looping |
| Chain | ethereum |
| Network | Anvil fork (Ethereum mainnet, block 24514931) |
| Anvil Port | 8546 (manually started; managed gateway used port 61407) |

## Config Changes Made

| Field | Original | Changed To | Reason |
|-------|----------|------------|--------|
| `initial_collateral` | "0.1" | "0.02" | Reduce trade size below $100 cap (~$47.65 at wstETH=$2382.58) |
| `force_action` | "" | "supply" | Trigger immediate SUPPLY trade on first run |

Config was restored to original values after the test.

## Execution

### Setup
- Anvil fork started on port 8546 (Ethereum mainnet, chain ID 1)
- Gateway auto-started by `strat run` on port 50052
- Wallet funded by managed gateway: 100 ETH, 1 wstETH, 10,000 USDC
- Wallet: `0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266` (Anvil default)

### Strategy Run
- Executed with `--network anvil --once`
- Strategy loaded stale state from previous run (loop_state=idle, HF=0) -- clean start
- `force_action="supply"` triggered immediate SUPPLY intent
- Intent compiled: SUPPLY 0.02 wstETH to Morpho Blue market `0xb323495f7e4148...`
- Two transactions submitted (approval + supply collateral)

### Transaction Results

| TX # | Purpose | TX Hash | Block | Gas Used | Status |
|------|---------|---------|-------|----------|--------|
| 1 | wstETH Approve | `9da366cc44e8be96625131a7abf5dd9b5e238c60b2e9eeea63a493170537c2ae` | 24514934 | 46,204 | SUCCESS |
| 2 | Supply Collateral | `35e8023795f07030b1df4f01787e3ab126b7765173a58ad61a185b4d35817def` | 24514935 | 76,395 | SUCCESS |

**Total gas used:** 122,599

### Prices Fetched
- wstETH/USD: $2,382.58 (CoinGecko, confidence 1.00)
- USDC/USD: $1.00 (CoinGecko, confidence 1.00)

### Key Log Output
```text
[info] Forced action: SUPPLY collateral
[info] SUPPLY intent: 0.0200 wstETH to Morpho Blue
[info] Compiled SUPPLY: 0.02 WSTETH to Morpho Blue market 0xb323495f7e4148...
[info] Transaction confirmed: tx_hash=9da366cc..., block=24514934, gas_used=46204
[info] Transaction confirmed: tx_hash=35e80237..., block=24514935, gas_used=76395
[info] EXECUTED: SUPPLY completed successfully
[info] Txs: 2 (9da366...c2ae, 35e802...7def) | 122,599 gas
Status: SUCCESS | Intent: SUPPLY | Gas used: 122599 | Duration: 19433ms
```

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | gateway | WARNING | No CoinGecko API key | `COINGECKO_API_KEY not configured - CoinGecko will use free tier API (30 requests/minute limit)` |
| 2 | strategy | WARNING | MorphoBlueAdapter missing price oracle | `MorphoBlueAdapter: No price_oracle or price_provider provided. Using placeholder prices. For production, use create_adapter_with_prices()` |
| 3 | strategy | WARNING | Gas estimation revert during simulation | `Gas estimation failed for tx 2/2: ('execution reverted: revert: transferFrom reverted', ...). Using compiler-provided gas limit.` |
| 4 | strategy | WARNING | Amount chaining output not extracted | `Amount chaining: no output amount extracted from step 1; subsequent amount='all' steps will fail` |

**Analysis of findings:**

- **Finding 1** (CoinGecko free tier): Prices fetched successfully for both wstETH and USDC despite no API key. Normal in dev/test environments -- informational.
- **Finding 2** (Placeholder prices in adapter): The MorphoBlueAdapter executed successfully but warns it used placeholder prices internally rather than a live oracle. For production, borrow amount sizing would be miscalibrated without a real price feed wired in via `create_adapter_with_prices()`.
- **Finding 3** (Gas estimation revert): The gas estimator simulated tx 2 (supply collateral) before tx 1 (approval) was on-chain, so the simulation reverted with `transferFrom reverted`. Expected behavior -- the orchestrator correctly fell back to the compiler-provided gas limit, and actual execution succeeded in order (approval first, then supply). Not a real bug but indicates the local simulator does not simulate a bundle atomically.
- **Finding 4** (Amount chaining): The Morpho Blue receipt parser did not extract an output amount from the SUPPLY step. If a subsequent intent in the same run used `amount="all"`, it would fail. For this single-intent forced run this is benign, but indicates a gap in the Morpho receipt parser's extraction methods.

## Result

**PASS** - The morpho_looping strategy executed a SUPPLY of 0.02 wstETH to Morpho Blue on an Ethereum Anvil fork, producing 2 confirmed on-chain transactions (46,204 + 76,395 gas). Four warnings were found -- none blocked execution, but the placeholder price oracle and amount chaining gaps are worth tracking for production readiness.

---

SUSPICIOUS_BEHAVIOUR_COUNT: 4
SUSPICIOUS_BEHAVIOUR_ERRORS: 0
