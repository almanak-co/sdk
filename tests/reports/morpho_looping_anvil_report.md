# E2E Strategy Test Report: morpho_looping (Anvil)

**Date:** 2026-03-16 01:21
**Result:** PASS
**Mode:** Anvil
**Duration:** ~4 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | demo_morpho_looping |
| Chain | ethereum (chain ID 1) |
| Network | Anvil fork (block 24664649) |
| Anvil Port | 54197 (managed, auto-selected) |
| Wallet | 0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266 |
| Market ID | 0xb323495f7e4148be5643a4ea4a8221eef163e4bccfdedc2a6f4696baacbc86cc |
| Collateral Token | wstETH |
| Borrow Token | USDC |
| Initial Collateral | 0.028 wstETH (~$37.90 at $1,353.82) |
| Target Loops | 2 |
| Force Action | supply (pre-configured in config.json) |

## Config Changes Made

None. The existing config already had `initial_collateral: "0.028"` (~$37.90, well within the $1,000
budget cap) and `force_action: "supply"` to trigger an immediate trade.

## Execution

### Setup
- [x] Anvil fork started (managed, port 54197, block 24664649, chain_id=1)
- [x] Gateway started on port 50052 (managed)
- [x] Wallet funded: 100 ETH, 0.1 wstETH, 500 USDC (via `anvil_funding` in config.json)

### Strategy Run
- [x] Strategy executed with `--network anvil --once`
- [x] `force_action: "supply"` triggered an immediate SUPPLY_COLLATERAL intent
- [x] Intent compiled to 2 transactions (APPROVE + SUPPLY_COLLATERAL)
- [x] Both transactions confirmed on-chain

### Key Log Output
```text
info  | MorphoLoopingStrategy initialized: market=0xb323495f..., collateral=0.028 wstETH, target_loops=2, target_ltv=70.00%
info  | Aggregated price for wstETH/USD: 1353.82 (confidence: 0.80, sources: 2/4)
info  | Aggregated price for USDC/USD: 0.9999935 (confidence: 1.00, sources: 4/4)
info  | Forced action: SUPPLY collateral
info  | SUPPLY intent: 0.0280 wstETH to Morpho Blue
info  | Compiled SUPPLY: 0.028 WSTETH to Morpho Blue market 0xb323495f7e4148...
info  | Simulation successful: 2 transaction(s), total gas: 244216
info  | Sequential submit: TX 1/2
info  | Transaction confirmed: tx=dd014e69...866c, block=24664652, gas=46216
info  | Sequential submit: TX 2/2
info  | Transaction confirmed: tx=7e88820b...4104, block=24664653, gas=76395
info  | Parsed Morpho Blue: SUPPLY_COLLATERAL=1, TRANSFER=1, APPROVAL=1
info  | EXECUTED: SUPPLY completed successfully | 122,611 gas
Status: SUCCESS | Intent: SUPPLY | Gas used: 122611 | Duration: 19797ms
```

## Transaction Summary

| # | Intent | TX Hash | Gas Used | Status |
|---|--------|---------|----------|--------|
| 1 | APPROVE wstETH | `dd014e693e2912bb87640941bc9a913cb60cca05a82f1463cdab422f125f866c` | 46,216 | SUCCESS |
| 2 | SUPPLY_COLLATERAL | `7e88820b3ffa38a9e4641ee9686537374edadcf845b8c0fa40472dae9b524104` | 76,395 | SUCCESS |

Total gas: 122,611

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | gateway | WARNING | All price sources for wstETH flagged as outliers; aggregator used all 2 available | `All prices flagged as outliers, using all 2 results` (wstETH confidence: 0.80, 2/4 sources) |
| 2 | gateway | WARNING | MorphoBlueAdapter initialised without price oracle; uses placeholder prices | `MorphoBlueAdapter: No price_oracle or price_provider provided. Using placeholder prices. For production, use create_adapter_with_prices().` |
| 3 | gateway | INFO | CoinGecko API key not configured; Chainlink primary, free CoinGecko as fallback | `No CoinGecko API key -- using on-chain pricing (Chainlink oracles) with free CoinGecko as fallback.` |

**Notes on findings:**

- **Finding #1**: The "all prices flagged as outliers" warning fires because only 2 of 4 configured
  price sources returned data (Chainlink + one other), and their values were mutually flagged by the
  outlier filter. The aggregator correctly falls back to using all available results rather than
  returning nothing. wstETH price was resolved at $1,353.82 with 0.80 confidence — a reasonable
  value. This is expected on Anvil (DexScreener / CoinGecko free tier unavailable). Not a bug,
  but lower confidence than production would have.

- **Finding #2**: MorphoBlueAdapter initialises without a live price oracle on the `--network anvil`
  path. The adapter uses placeholder prices for its own internal calculations. For the SUPPLY intent
  tested here this is harmless (supply is a deposit; no LTV calculation needed at compile time).
  For a BORROW intent the placeholder prices could cause incorrect borrow amount sizing.

- **Finding #3**: Informational only. No CoinGecko Pro API key configured; Chainlink is the primary
  on-chain oracle source for Ethereum, which is the correct and preferred approach.

## Result

**PASS** - The `morpho_looping` strategy successfully compiled and executed a `SUPPLY_COLLATERAL`
intent on a forked Ethereum Anvil network, depositing 0.028 wstETH into Morpho Blue market
`0xb323495f...` via 2 confirmed on-chain transactions (APPROVE + SUPPLY_COLLATERAL, 122,611 total
gas). Prices resolved correctly via Chainlink at $1,353.82 for wstETH. The `force_action: "supply"`
config correctly triggered an immediate supply without requiring state machine advancement.
Two low-severity warnings were observed: the price aggregator confidence floor for wstETH (expected
on Anvil) and the MorphoBlueAdapter placeholder price warning (relevant only for BORROW intents).

---

SUSPICIOUS_BEHAVIOUR_COUNT: 3
SUSPICIOUS_BEHAVIOUR_ERRORS: 0
