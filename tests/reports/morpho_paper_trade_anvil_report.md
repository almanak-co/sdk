# E2E Strategy Test Report: morpho_paper_trade (Anvil)

**Date:** 2026-03-16 (run at 2026-03-15 18:24 UTC)
**Result:** PASS
**Mode:** Anvil
**Duration:** ~3 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | demo_morpho_paper_trade |
| Chain | ethereum |
| Network | Anvil fork |
| Anvil Port | 54841 (managed, auto-started by runner) |
| Collateral | 0.028 wstETH |
| Borrow token | USDC |
| LTV target | 0.5 |

**Config changes:** None. `collateral_amount` of 0.028 wstETH (~$37.89 at $1353.36) is well within the $1000 budget cap. No `force_action` field exists in this strategy; the strategy naturally supplies collateral on first run from the "idle" state.

## Execution

### Setup
- Anvil fork of Ethereum started on port 54841 (managed by runner via `anvil_funding` config)
- Gateway started on port 50052
- Wallet funded: 100 ETH, 1 wstETH, 10,000 USDC (via `anvil_funding` config)

### Strategy Run
- Strategy executed with `--network anvil --once`
- State: `idle` -> `supplying` -> `supplied`
- Intent: SUPPLY 0.028 wstETH to Morpho Blue market `0xb323495f7e4148...` on Ethereum

| Intent | TX Hash | Gas Used | Status |
|--------|---------|----------|--------|
| Approve wstETH | `0x42b0e982...7c66` | 46,216 | SUCCESS |
| SUPPLY_COLLATERAL | `0xe054ec16...0417` | 76,395 | SUCCESS |

Total gas: 122,611 across 2 transactions.

### Key Log Output
```text
2026-03-15T18:24:37.768417Z [info] SUPPLY 0.028 wstETH to Morpho Blue market at $1353.36 [strategy_module]
2026-03-15T18:24:56.688160Z [info] Txs: 2 (42b0e9...7c66, e054ec...0417) | 122,611 gas [orchestrator]
2026-03-15T18:24:56.691623Z [info] Parsed Morpho Blue: SUPPLY_COLLATERAL=1, TRANSFER=1, APPROVAL=1, tx=0xe054...0417, 76,395 gas
Status: SUCCESS | Intent: SUPPLY | Gas used: 122611 | Duration: 19800ms
```

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | gateway | INFO | No CoinGecko API key; using Chainlink primary | `No CoinGecko API key -- using on-chain pricing (Chainlink oracles) with free CoinGecko as fallback.` |
| 2 | gateway | WARNING | Price aggregator outlier handling | `All prices flagged as outliers, using all 2 results` (wstETH, confidence 0.80, 2/4 sources) |
| 3 | gateway | WARNING | MorphoBlue adapter using placeholder prices | `MorphoBlueAdapter: No price_oracle or price_provider provided. Using placeholder prices. For production, use create_adapter_with_prices().` |

**Notes on findings:**

- Finding 1 (INFO): Normal for local dev without `ALMANAK_GATEWAY_COINGECKO_API_KEY`. No impact on execution.
- Finding 2 (WARNING): The price aggregator had only 2/4 sources respond for wstETH and both were flagged as potential outliers; the aggregator correctly fell back to using all results. Final price ($1353.36) is plausible. This is worth monitoring - with only 2 sources, confidence was 0.80 which is acceptable but not ideal. Not a blocking issue.
- Finding 3 (WARNING): The MorphoBlue adapter was initialized without a price oracle. It uses placeholder prices internally for pre-trade validation. This is expected in the current Anvil test flow; the actual strategy prices come from `market.price()` via the gateway, so this does not affect correctness of the supply amount. However, this warning indicates a potential gap in collateral valuation accuracy for the adapter's internal checks.

## Result

**PASS** - Strategy executed successfully: SUPPLY intent placed 0.028 wstETH as collateral into Morpho Blue market on forked Ethereum, producing 2 confirmed transactions (approve + supply_collateral) with 122,611 total gas used.

---

SUSPICIOUS_BEHAVIOUR_COUNT: 3
SUSPICIOUS_BEHAVIOUR_ERRORS: 0
