# E2E Strategy Test Report: morpho_looping (Anvil)

**Date:** 2026-03-06 05:28
**Result:** PASS
**Mode:** Anvil
**Duration:** ~4 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | demo_morpho_looping |
| Chain | Ethereum (chain ID 1) |
| Network | Anvil fork (public RPC, block 24594255) |
| Anvil Port | 59904 (auto-assigned by managed gateway) |
| Wallet | 0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266 |

## Config Changes Made

| Field | Original | Changed To | Reason |
|-------|----------|------------|--------|
| `initial_collateral` | `"0.1"` | `"0.01"` | Budget cap: 0.1 wstETH ~$340 > $50 limit; 0.01 wstETH ~$34 |
| `force_action` | `""` | `"supply"` | Trigger immediate SUPPLY on `--once` run |

Both changes were **restored** to their original values after the test.

## Execution

### Setup
- [x] Anvil fork started (Ethereum mainnet via public RPC, block 24594255, chain_id=1)
- [x] Managed gateway started on port 50053
- [x] Wallet funded: 100 ETH, 1 wstETH, 10,000 USDC (via `anvil_funding` in config)

### Strategy Run
- [x] Strategy executed with `--network anvil --once`
- [x] Intent: SUPPLY 0.01 wstETH to Morpho Blue market as collateral
- [x] 2 transactions submitted and confirmed

### Key Log Output
```text
info  | MorphoLoopingStrategy initialized: market=0xb323495f..., collateral=0.01 wstETH, target_loops=2, target_ltv=70.00%
info  | Forced action: SUPPLY collateral
info  | SUPPLY intent: 0.0100 wstETH to Morpho Blue
info  | Compiled SUPPLY: 0.01 WSTETH to Morpho Blue market 0xb323495f7e4148...
info  | Simulation successful: 2 transaction(s), total gas: 244216
info  | Sequential submit: TX 1/2
info  | Transaction confirmed: tx=962ca3e5...9902, block=24594258, gas=46216
info  | Sequential submit: TX 2/2
info  | Transaction confirmed: tx=89dfc184...1548, block=24594259, gas=76395
info  | Parsed Morpho Blue: SUPPLY_COLLATERAL=1, TRANSFER=1, APPROVAL=1
info  | EXECUTED: SUPPLY completed successfully | 122,611 gas
Status: SUCCESS | Intent: SUPPLY | Gas used: 122611 | Duration: 19768ms
```

## Transaction Summary

| # | Intent | TX Hash | Gas Used | Status |
|---|--------|---------|----------|--------|
| 1 | APPROVE wstETH | `962ca3e593667299a4ea0288981490b14c77e8b52bef94753ccda853062c9902` | 46,216 | SUCCESS |
| 2 | SUPPLY_COLLATERAL | `89dfc1846dfcfa9d0705bf24902092dded64936a8bfee6c323621a5c80da1548` | 76,395 | SUCCESS |

Total gas: 122,611

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | gateway | ERROR | Chainlink returned empty eth_call for wstETH/USD on Anvil fork | `Chainlink RPC call failed for WSTETH/USD: Empty eth_call result (id=2, to=0x164b276057258d81941072EB5f9D7f71C3dD94b8)` |
| 2 | gateway | ERROR | CoinGecko rate-limited immediately (free tier, startup cold call) | `Data source 'coingecko' rate limited. Retry after 1s` |
| 3 | strategy | WARNING | IntentCompiler used placeholder prices; slippage calculations unreliable | `IntentCompiler using PLACEHOLDER PRICES. Slippage calculations will be INCORRECT.` |
| 4 | gateway | WARNING | MorphoBlueAdapter has no price oracle wired in; uses placeholder prices | `MorphoBlueAdapter: No price_oracle or price_provider provided. Using placeholder prices.` |
| 5 | strategy | WARNING | pendle_pt_swap_arbitrum failed to import at startup (circular import) | `Failed to import strategy pendle_pt_swap_arbitrum (retry failed): circular import in almanak/__init__.py` |

**Notes:**

- Findings 1-4 share the same root cause: wstETH price resolution degrades on an Anvil fork when Alchemy is unconfigured. Chainlink's aggregator contract returns an empty result against the public RPC. CoinGecko free tier is immediately rate-limited at cold start. Together this means both price sources fail on the very first call. The strategy's fallback default (`$3400`) was not needed here because `force_action=supply` bypassed the price-gated state machine entirely and the SUPPLY amount is explicit. However, for the BORROW step (which calculates amounts from price), this failure would propagate to incorrect borrow sizing.
- Finding 5 (circular import in pendle_pt_swap_arbitrum) is a pre-existing incubating strategy import issue unrelated to this test.

## Result

**PASS** - The SUPPLY_COLLATERAL intent compiled and executed successfully on an Ethereum Anvil fork (block 24594255), depositing 0.01 wstETH into Morpho Blue market `0xb323495f...` via 2 confirmed on-chain transactions (122,611 total gas). The price oracle layer degraded gracefully (no crash), but the wstETH/USD price was unavailable from both Chainlink (empty result) and CoinGecko (rate limit) — this is a data layer concern that would affect borrow amount calculations in subsequent loop iterations.

---

SUSPICIOUS_BEHAVIOUR_COUNT: 5
SUSPICIOUS_BEHAVIOUR_ERRORS: 2
