# E2E Strategy Test Report: morpho_looping (Anvil)

**Date:** 2026-02-27 09:12
**Result:** PASS
**Mode:** Anvil
**Duration:** ~2 minutes (excluding first-run failure, see note below)

## Configuration

| Field | Value |
|-------|-------|
| Strategy | demo_morpho_looping |
| Chain | ethereum |
| Network | Anvil fork (publicnode.com, block 24547286) |
| Anvil Port | 51630 (auto-assigned by framework) |
| Market ID | 0xb323495f7e4148be5643a4ea4a8221eef163e4bccfdedc2a6f4696baacbc86cc |
| Collateral | 0.1 wstETH (~$250 at $2,495 price) |
| Borrow token | USDC |
| Target loops | 2 |
| Target LTV | 70% |
| Wallet | 0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266 (Anvil default) |

## Config Changes Made

| Field | Original | Changed To | Restored |
|-------|----------|------------|---------|
| `force_action` | `""` | `"supply"` | `""` |

The `force_action` was set to `"supply"` to trigger an immediate `SUPPLY_COLLATERAL` intent on the first run, bypassing the idle state machine's balance check. Restored to `""` after the test.

The `initial_collateral` of `0.1 wstETH` at ~$2,495/wstETH equals ~$250, which is within the $500 budget cap. No amount change was needed.

## Execution

### Setup
- [x] Anvil fork auto-started by managed gateway for Ethereum at block 24547286
- [x] Gateway auto-started on port 50051
- [x] Wallet funded automatically via `anvil_funding` config: 100 ETH, 1 wstETH, 10,000 USDC

### First Run Failure (Key Finding)

The first run attempt failed with:
```
PRIVATE_KEY not configured in gateway settings
```
The `ALMANAK_PRIVATE_KEY` value from `.env` was NOT propagated to the managed gateway's internal settings. The managed gateway requires `ALMANAK_GATEWAY_PRIVATE_KEY` to be set explicitly. The second run set `ALMANAK_GATEWAY_PRIVATE_KEY=<key>` as an environment variable and succeeded.

**This is a friction issue**: users with `ALMANAK_PRIVATE_KEY` in `.env` will get a cryptic error when the managed gateway cannot find the key.

### Strategy Run (Second Attempt — PASS)

- [x] Strategy executed with `--network anvil --once` and `ALMANAK_GATEWAY_PRIVATE_KEY` set
- [x] Intent executed: `SUPPLY` (0.1 wstETH to Morpho Blue as collateral)
- [x] 2 transactions submitted and confirmed on Anvil fork

### Prices Fetched
- wstETH/USD: $2,495.76 (Chainlink oracle, confidence: 0.90, 1/2 sources)
- USDC/USD: $1.00 (Chainlink + CoinGecko, confidence: 1.00, 2/2 sources)

### Key Log Output

```text
Anvil fork started for ethereum on port 51630 (fork: https://ethereum-rpc.publicnode.com)
Funded 0xf39Fd6e5... with 100 ETH
Funded 0xf39Fd6e5... with wstETH via known slot 0
Funded 0xf39Fd6e5... with USDC via known slot 9

Aggregated price for wstETH/USD: 2495.76 (confidence: 0.90, sources: 1/2, outliers: 0)
Aggregated price for USDC/USD: 1.0 (confidence: 1.00, sources: 2/2, outliers: 0)

Forced action: SUPPLY collateral
SUPPLY intent: 0.1000 wstETH to Morpho Blue
Compiled SUPPLY: 0.1 WSTETH to Morpho Blue market 0xb323495f7e4148...

Simulating 2 transaction(s) via eth_estimateGas
Simulation successful: 2 transaction(s), total gas: 244228

Transaction submitted: tx_hash=095536d5...39eb
Transaction confirmed: block=24547289, gas_used=46228

Transaction submitted: tx_hash=7047c67f...4f74
Transaction confirmed: block=24547290, gas_used=76407

EXECUTED: SUPPLY completed successfully
   Txs: 2 (095536...39eb, 7047c6...4f74) | 122,635 gas

Status: SUCCESS | Intent: SUPPLY | Gas used: 122635 | Duration: 19960ms
```

## Transactions (Anvil)

| # | Intent | TX Hash | Block | Gas Used | Status |
|---|--------|---------|-------|----------|--------|
| 1/2 | APPROVE (wstETH for Morpho) | `095536d51d5f7edf3608b4ac4569de440309e63f6d61d86a79873a10005239eb` | 24547289 | 46,228 | SUCCESS |
| 2/2 | SUPPLY_COLLATERAL (0.1 wstETH) | `7047c67fea41535a3e4357e829c851acb8cfbf9f42e09989100897dd3cd54f74` | 24547290 | 76,407 | SUCCESS |
| **Total** | | | | **122,635** | |

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | strategy | WARNING | 9 common Ethereum tokens missing from registry | `token_resolution_error token=BTC chain=ethereum error_type=TokenNotFoundError` (also COMP, MKR, SNX, LDO, STETH, CBETH, RETH, SOL) |
| 2 | strategy | WARNING | MorphoBlueAdapter using placeholder prices | `MorphoBlueAdapter: No price_oracle or price_provider provided. Using placeholder prices. For production, use create_adapter_with_prices().` |
| 3 | strategy | INFO | wstETH price only 1 source (Chainlink; CoinGecko unavailable) | `Aggregated price for wstETH/USD: 2495.76 (confidence: 0.90, sources: 1/2, outliers: 0)` |
| 4 | gateway | INFO | Public RPC in use (no ALCHEMY_API_KEY) | `No API key configured -- using free public RPC for ethereum (rate limits may apply)` |

**Analysis:**

- **Finding 1 (WARNING):** The MarketService pre-fetches prices for a list of common tokens. BTC, COMP, MKR, SNX, LDO, STETH, CBETH, RETH, and SOL are all missing from the static Ethereum token registry. This causes 9 `TokenNotFoundError` warnings on every strategy iteration. The registry should include these standard Ethereum tokens. The STETH suggestion "Did you mean 'WSTETH'?" indicates an alias gap — strategies that reference STETH will fail silently. Not a blocker for this test but a real coverage gap in the registry.
- **Finding 2 (WARNING):** MorphoBlueAdapter initializes without a price oracle. Supply-only intents work fine, but any intent requiring LTV/health-factor calculation (BORROW, REPAY, position monitoring) will use placeholder prices. This is a production correctness risk for full looping runs.
- **Finding 3 (INFO):** wstETH price was sourced from only 1 of 2 configured sources (Chainlink on-chain succeeded; CoinGecko unavailable without API key). Confidence of 0.90 is acceptable but suboptimal.
- **Finding 4 (INFO):** Free public RPC used. Functioned correctly for this test. May hit rate limits under concurrent strategy runs.

## First Run Root Cause

`ALMANAK_PRIVATE_KEY` set in `.env` is not automatically used by the managed gateway. The gateway requires `ALMANAK_GATEWAY_PRIVATE_KEY` to be set as a separate env var. The managed gateway warning `No wallet address or ALMANAK_PRIVATE_KEY set -- skipping Anvil funding` also appeared in the first run, indicating the key lookup was failing entirely. Setting `ALMANAK_GATEWAY_PRIVATE_KEY` explicitly resolved both the funding skip and the execution failure.

## Result

**PASS** - The morpho_looping strategy successfully executed a `SUPPLY_COLLATERAL` intent on an Ethereum Anvil fork, submitting 2 transactions (wstETH APPROVE + SUPPLY_COLLATERAL) with 122,635 total gas. The strategy compiled the Morpho Blue SUPPLY intent correctly, funded the wallet from Anvil, and recorded a TRADE timeline event. A first-run failure due to private key not being propagated to the managed gateway was resolved by explicitly setting `ALMANAK_GATEWAY_PRIVATE_KEY`.

---

SUSPICIOUS_BEHAVIOUR_COUNT: 4
SUSPICIOUS_BEHAVIOUR_ERRORS: 0
