# E2E Strategy Test Report: morpho_looping (Anvil)

**Date:** 2026-02-27 16:06
**Result:** PASS
**Mode:** Anvil
**Duration:** ~25 seconds

## Configuration

| Field | Value |
|-------|-------|
| Strategy | demo_morpho_looping |
| Chain | ethereum |
| Network | Anvil fork (publicnode.com, block 24549347) |
| Anvil Port | 65340 (auto-assigned by framework) |
| Market ID | 0xb323495f7e4148be5643a4ea4a8221eef163e4bccfdedc2a6f4696baacbc86cc |
| Collateral | 0.1 wstETH (~$239 at $2390/wstETH) |
| Borrow Token | USDC |
| Target Loops | 2 |
| Target LTV | 70% |
| Wallet | 0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266 (Anvil default) |

## Config Changes Made

| Field | Before | After Test (restored) |
|-------|--------|----------------------|
| `force_action` | `""` | Set to `"supply"` for test, restored to `""` |

The `force_action` was set to `"supply"` to trigger an immediate SUPPLY_COLLATERAL intent on the first `--once` run, bypassing the idle state machine's balance check. Restored to `""` after the test.

No amount changes needed: 0.1 wstETH at ~$2,390/wstETH equals ~$239, well within the $500 budget cap.

## Execution

### Setup

- Anvil fork auto-started by managed gateway for Ethereum mainnet at block 24549347
- Fork source: `https://ethereum-rpc.publicnode.com` (no Alchemy API key configured)
- Gateway auto-started on port 50051
- Wallet funded via `anvil_funding` config: 100 ETH, 1 wstETH, 10,000 USDC
  - wstETH funded via storage slot 0 (known slot)
  - USDC funded via storage slot 9 (known slot)

### Strategy Run

With `force_action = "supply"`, the strategy immediately created a SUPPLY_COLLATERAL intent for 0.1 wstETH to Morpho Blue market `0xb323495f...`.

Prices fetched:
- wstETH/USD: $2,390.17 (Chainlink oracle, confidence: 0.90, sources: 1/2)
- USDC/USD: $0.99996 (Chainlink + CoinGecko, confidence: 1.00, sources: 2/2)

Two transactions were compiled, simulated, submitted, and confirmed:

| # | Description | Block | Gas Used | TX Hash |
|---|-------------|-------|----------|---------|
| 1/2 | wstETH APPROVE to Morpho Blue | 24549350 | 46,228 | `60b18e6f30b8a01e21a35e989346aed122145ba087f37dc4cc66f3cc01fbff3d` |
| 2/2 | SUPPLY_COLLATERAL (0.1 wstETH) | 24549351 | 76,407 | `0afb863ff42d64b32a949f60c99ec7bfe778438e3c9b258bfc305024450fa436` |
| **Total** | | | **122,635** | |

### Key Log Output

```text
Anvil fork started: port=65340, block=24549347, chain_id=1
Funded 0xf39Fd6e5... with 100 ETH
Funded 0xf39Fd6e5... with wstETH via known slot 0
Funded 0xf39Fd6e5... with USDC via known slot 9

Aggregated price for wstETH/USD: 2390.17 (confidence: 0.90, sources: 1/2, outliers: 0)
Aggregated price for USDC/USD: 0.99996 (confidence: 1.00, sources: 2/2, outliers: 0)

Forced action: SUPPLY collateral
SUPPLY intent: 0.1000 wstETH to Morpho Blue
Compiled SUPPLY: 0.1 WSTETH to Morpho Blue market 0xb323495f7e4148...

Simulating 2 transaction(s) via eth_estimateGas
Simulation successful: 2 transaction(s), total gas: 244228

Transaction submitted: tx_hash=60b18e...ff3d
Transaction confirmed: block=24549350, gas_used=46228
Transaction submitted: tx_hash=0afb86...a436
Transaction confirmed: block=24549351, gas_used=76407

EXECUTED: SUPPLY completed successfully
   Txs: 2 (60b18e...ff3d, 0afb86...a436) | 122,635 gas
Parsed Morpho Blue: SUPPLY_COLLATERAL=1, TRANSFER=1, APPROVAL=1

Status: SUCCESS | Intent: SUPPLY | Gas used: 122635 | Duration: 25177ms
```

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | strategy | WARNING | 9 tokens missing from Ethereum registry (price prefetch) | `token_resolution_error token=BTC chain=ethereum error_type=TokenNotFoundError` (also COMP, MKR, SNX, LDO, STETH, CBETH, RETH, SOL) |
| 2 | strategy | WARNING | MorphoBlueAdapter using placeholder prices | `MorphoBlueAdapter: No price_oracle or price_provider provided. Using placeholder prices. For production, use create_adapter_with_prices().` |
| 3 | strategy | INFO | wstETH price from only 1/2 sources (CoinGecko unavailable) | `Aggregated price for wstETH/USD: 2390.17 (confidence: 0.90, sources: 1/2, outliers: 0)` |
| 4 | strategy | INFO | Free public RPC in use (no ALCHEMY_API_KEY configured) | `No API key configured -- using free public RPC for ethereum (rate limits may apply)` |
| 5 | strategy | INFO | Cosmetic port cleanup delay | `Port 65340 not freed after 5.0s` |

### Analysis

**Finding 1 (WARNING):** The gateway's MarketService pre-fetches prices for a list of common tokens on startup. Nine tokens — BTC, COMP, MKR, SNX, LDO, STETH, CBETH, RETH, SOL — are not in the static Ethereum registry. The `STETH` failure is notable: the correct symbol is `WSTETH`, and the suggestion "Did you mean 'WSTETH'?" confirms the alias is missing. Any strategy that references `STETH` directly will fail. These 9 warnings appear on every strategy iteration and represent a data quality gap in the registry.

**Finding 2 (WARNING):** `MorphoBlueAdapter` initialises without an injected price oracle. For SUPPLY_COLLATERAL this is benign (no price needed to compute the deposit amount), but for BORROW and health-factor monitoring intents the adapter would rely on placeholder values — a correctness risk in full looping runs.

**Finding 3 (INFO):** wstETH pricing relied on only Chainlink (1/2 sources). CoinGecko is unavailable without an API key. The 0.90 confidence score is acceptable but degrades price reliability.

**Finding 4 (INFO):** Free public RPC via `publicnode.com` was used. The test succeeded cleanly; no rate limiting was observed. This is expected behaviour when `ALCHEMY_API_KEY` is not configured.

**Finding 5 (INFO):** Minor cosmetic warning about Anvil port cleanup not completing within 5 seconds — non-critical, Anvil was stopped successfully.

## Result

**PASS** - The morpho_looping strategy successfully compiled and executed a SUPPLY_COLLATERAL intent on an Ethereum Anvil fork. Two transactions were confirmed on-chain (wstETH APPROVE + SUPPLY_COLLATERAL) with 122,635 total gas. The strategy ran cleanly from fresh state without any execution errors.

---

SUSPICIOUS_BEHAVIOUR_COUNT: 5
SUSPICIOUS_BEHAVIOUR_ERRORS: 0
