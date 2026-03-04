# E2E Strategy Test Report: morpho_looping (Anvil)

**Date:** 2026-03-03 12:01 UTC
**Result:** PASS
**Mode:** Anvil
**Duration:** ~5 minutes total (~22 seconds strategy execution)

## Configuration

| Field | Value |
|-------|-------|
| Strategy | demo_morpho_looping |
| Chain | ethereum |
| Network | Anvil fork (publicnode.com, block 24576794) |
| Anvil Port | 53080 (auto-assigned by managed gateway) |
| Market ID | 0xb323495f7e4148be5643a4ea4a8221eef163e4bccfdedc2a6f4696baacbc86cc |
| Collateral Token | wstETH |
| Borrow Token | USDC |
| Initial Collateral | 0.1 wstETH (~$241 @ $2,409) |
| Target Loops | 2 |
| Target LTV | 70% |
| Min Health Factor | 1.5 |
| Wallet | 0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266 (Anvil default) |

## Budget Cap Check

`initial_collateral = 0.1 wstETH`. At $2,409/wstETH = ~$241. Within $500 cap. No amount
changes needed.

## Config Changes Made

| Field | Original | Changed To | Reason | Restored |
|-------|----------|------------|--------|---------|
| `force_action` | `""` | `"supply"` | Trigger immediate SUPPLY on `--once` run (state machine would otherwise advance to SUPPLY from IDLE on the first iteration, which also works, but `force_action` makes it explicit) | Yes |

Config was restored to original values after the run.

## Execution

### Setup
- Anvil fork auto-started by managed gateway at block 24576794
- Fork source: free public Ethereum RPC (https://ethereum-rpc.publicnode.com) - ALCHEMY_API_KEY is empty in .env
- Gateway started on managed port 50052 (insecure mode - acceptable for anvil)
- Wallet funded via `anvil_funding` config: 100 ETH, 1 wstETH, 10,000 USDC
  - wstETH: funded via storage slot 0
  - USDC: funded via storage slot 9

### Strategy Run
- [x] Strategy executed with `--network anvil --once`
- [x] Intent: SUPPLY_COLLATERAL - 0.1 wstETH to Morpho Blue market
- [x] Compilation successful (2 actions: APPROVE + SUPPLY_COLLATERAL)
- [x] Simulation successful via eth_estimateGas (LocalSimulator)
- [x] 2 transactions submitted and confirmed sequentially

### Price Data
- wstETH/USD: $2,409.65 (confidence: 0.90, sources: 1/2 - Chainlink only)
- USDC/USD: $0.9999865 (confidence: 1.00, sources: 2/2)

### Transaction Details

| Step | TX Hash | Block | Gas Used | Status |
|------|---------|-------|----------|--------|
| Approve wstETH | `0xfbf96c8aa77b72fcd175d3b351d9241f37698dae557429ba2d766d4ab0e30400` | 24576797 | 46,228 | SUCCESS |
| Supply Collateral | `0x6080e74f03183a5f93636c8fee600d62ba7360793d0f6b5369361e58ce9cbb4f` | 24576798 | 76,407 | SUCCESS |
| **Total** | — | — | **122,635** | **SUCCESS** |

### Key Log Output

```text
Anvil fork started: port=53080, block=24576794, chain_id=1
Fork source: https://ethereum-rpc.publicnode.com
Funded 0xf39Fd6e5... with 100 ETH
Funded 0xf39Fd6e5... with wstETH via known slot 0
Funded 0xf39Fd6e5... with USDC via known slot 9

Aggregated price for wstETH/USD: 2409.65 (confidence: 0.90, sources: 1/2, outliers: 0)
Aggregated price for USDC/USD: 0.9999865 (confidence: 1.00, sources: 2/2, outliers: 0)

Forced action: SUPPLY collateral
SUPPLY intent: 0.1000 wstETH to Morpho Blue
Compiled SUPPLY: 0.1 WSTETH to Morpho Blue market 0xb323495f7e4148...

Simulating 2 transaction(s) via eth_estimateGas
Transaction 2/2: skipping estimation (multi-TX dependent), using compiler gas_limit=198000
Simulation successful: 2 transaction(s), total gas: 244228

Sequential submit: TX 1/2
Transaction confirmed: tx_hash=fbf96c8a...0400, block=24576797, gas_used=46228
Sequential submit: TX 2/2
Transaction confirmed: tx_hash=6080e74f...bb4f, block=24576798, gas_used=76407

EXECUTED: SUPPLY completed successfully
   Txs: 2 (fbf96c...0400, 6080e7...bb4f) | 122,635 gas
Parsed Morpho Blue: APPROVAL=1, tx=0xfbf9...0400, 46,228 gas
Parsed Morpho Blue: SUPPLY_COLLATERAL=1, TRANSFER=1, APPROVAL=1, tx=0x6080...bb4f, 76,407 gas

Status: SUCCESS | Intent: SUPPLY | Gas used: 122635 | Duration: 22199ms
```

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | strategy | WARNING | MorphoBlueAdapter using placeholder prices | `MorphoBlueAdapter: No price_oracle or price_provider provided. Using placeholder prices. For production, use create_adapter_with_prices().` |
| 2 | strategy | INFO | No Alchemy API key - using free public RPC with rate limits | `No API key configured -- using free public RPC for ethereum (rate limits may apply)` |
| 3 | strategy | INFO | wstETH price only from 1/2 sources (Chainlink only, no CoinGecko) | `Aggregated price for wstETH/USD: 2409.65 (confidence: 0.90, sources: 1/2, outliers: 0)` |
| 4 | strategy | WARNING | Anvil port not freed within 5s (cosmetic) | `Port 53080 not freed after 5.0s` |
| 5 | strategy | INFO | INSECURE MODE (expected for Anvil) | `INSECURE MODE: Auth interceptor disabled - no auth_token configured` |

### Analysis

**Finding 1 (WARNING) - MorphoBlueAdapter placeholder prices:** The adapter is initialized
without a live price oracle or price provider. For SUPPLY_COLLATERAL this is benign (no price
needed at the connector level). However, for subsequent BORROW intents in the looping cycle,
health-factor calculations and borrow amount sizing inside the adapter will use hardcoded
placeholder values rather than live prices. The strategy-level `decide()` fetches live prices
independently via the gateway, but the Morpho connector layer does not receive them. This
represents a production correctness risk for the BORROW and health-factor steps.

**Finding 2 (INFO) - No Alchemy key:** `ALCHEMY_API_KEY` is unset in `.env`. The SDK
automatically falls back to `publicnode.com` free public Ethereum RPC, which worked for this
test but is rate-limited and may fail under repeated or parallel strategy runs.

**Finding 3 (INFO) - wstETH price from 1/2 sources:** With no CoinGecko key, the price
aggregator relies solely on Chainlink for wstETH, yielding 0.90 confidence. Acceptable for
testing.

**Finding 4 (WARNING) - Port cleanup delay:** Cosmetic. The managed Anvil fork's port was not
released within the 5-second window. Non-blocking; shutdown succeeded.

**Finding 5 (INFO):** Expected and benign for Anvil mode.

## Result

**PASS** - morpho_looping successfully compiled and executed a `SUPPLY_COLLATERAL` intent on an
Ethereum Anvil fork, depositing 0.1 wstETH into Morpho Blue market `0xb323495f...`. Two on-chain
transactions confirmed (APPROVE + SUPPLY_COLLATERAL) with 122,635 total gas. The primary finding
worth ticketing is that `MorphoBlueAdapter` initializes without a price provider and falls back
to placeholder prices, which will cause incorrect BORROW amounts and health factor calculations
in the subsequent looping steps.

---

SUSPICIOUS_BEHAVIOUR_COUNT: 5
SUSPICIOUS_BEHAVIOUR_ERRORS: 0
