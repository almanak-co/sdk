# E2E Strategy Test Report: sushiswap_lp (Anvil)

**Date:** 2026-03-06 05:49
**Result:** PASS
**Mode:** Anvil
**Duration:** ~4 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | demo_sushiswap_lp |
| Chain | arbitrum |
| Network | Anvil fork |
| Anvil Port | 64412 (auto-managed by strategy runner) |
| Pool | WETH/USDC/3000 |
| Range Width | 10% |
| amount0 | 0.001 WETH |
| amount1 | 3 USDC |
| force_action | open |

**Config changes made:** None. Amounts (0.001 WETH + 3 USDC, ~$5 total) are well within the $50
budget cap. `force_action` was already set to `"open"` in config.json.

**Note on ALCHEMY_API_KEY:** The `.env` at the repo root has `ALCHEMY_API_KEY=` (empty). The strategy
runner automatically fell back to the public Arbitrum RPC (`https://arbitrum-one-rpc.publicnode.com`)
which worked fine for this test.

## Execution

### Setup
- [x] Anvil fork auto-managed by CLI on port 64412 (Arbitrum block 438740334, via publicnode.com RPC)
- [x] Gateway auto-started on port 50053 (insecure mode, acceptable for Anvil)
- [x] Wallet 0xf39Fd6e5... funded automatically: 100 ETH, 1 WETH (slot 51), 10,000 USDC (slot 9)
- [x] Prices fetched: WETH/USD = $2,082.35 (confidence 1.00, 2 sources), USDC/USD = $1.00

### Strategy Run
- [x] Strategy executed with `--network anvil --once`
- [x] `force_action: "open"` triggered LP_OPEN intent immediately
- [x] Price range computed: [1978.23 - 2186.47] USDC/WETH (ticks -200400 to -199440)
- [x] Intent compiled: 3 transactions (WETH approve + USDC approve + lp_mint), 660,000 gas estimated
- [x] Simulation passed via eth_estimateGas: 923,788 gas (multi-TX sequential)
- [x] All 3 transactions submitted and confirmed on sequential blocks 438740337-439
- [x] LP position ID extracted from SushiSwap V3 receipt: **34820**
- [x] ResultEnricher populated: position_id, tick_lower, tick_upper, liquidity
- [x] Liquidity: 2,000,918,325,713

### Key Log Output

```text
info  Aggregated price for WETH/USD: 2082.34829 (confidence: 1.00, sources: 2/2, outliers: 0)
warn  Rate limited by CoinGecko for USDC/USD, backoff: 1.00s
info  Aggregated price for USDC/USD: 1.00 (confidence: 0.90, sources: 1/2, outliers: 0)
info  Forced action: OPEN LP position
info  LP_OPEN: 0.0010 WETH + 3.0000 USDC, price range [1978.2309 - 2186.4657], ticks [-200400 - -199440]
info  Compiled LP_OPEN intent: WETH/USDC, range [1978.23-2186.47], 3 txs (approve + approve + lp_mint), 660000 gas
info  Simulation successful: 3 transaction(s), total gas: 923788
info  Sequential submit: TX 1/3 confirmed (block=438740337, gas=53440)
info  Sequential submit: TX 2/3 confirmed (block=438740338, gas=55437)
info  Sequential submit: TX 3/3 confirmed (block=438740339, gas=511475)
info  EXECUTED: LP_OPEN completed successfully | Txs: 3 | 620,352 gas
info  Extracted LP position ID from receipt: 34820
info  Enriched LP_OPEN result with: position_id, tick_lower, tick_upper, liquidity (protocol=sushiswap_v3, chain=arbitrum)
info  SushiSwap V3 LP position opened: position_id=34820, liquidity=2000918325713
Status: SUCCESS | Intent: LP_OPEN | Gas used: 620352 | Duration: 35808ms
```

### Transaction Details

| # | TX Hash | Type | Gas Used | Status |
|---|---------|------|----------|--------|
| 1 | `7a1f682d9fe9a1c47d250a1a21e02f826ca4bd0173251700e817fe0990cf7ae2` | WETH approve | 53,440 | SUCCESS |
| 2 | `1807ca21a77e5ce51f73ae0e4c8a8803e7602feaf8151dc50ee3e9a0f5e65043` | USDC approve | 55,437 | SUCCESS |
| 3 | `c4de4a368265ac9e7b6f5d9abe1fda0ad67b6fe09f5f060c945edf0d14bf906d` | lp_mint | 511,475 | SUCCESS |

**Total gas:** 620,352 (Anvil transactions - no block explorer links)

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | strategy | WARNING | CoinGecko rate limited (no API key) | `Rate limited by CoinGecko for USDC/USD, backoff: 1.00s` |
| 2 | strategy | INFO | Circular import in unrelated incubating strategy | `Failed to import strategy strategies.incubating.pendle_pt_swap_arbitrum.strategy (retry failed): cannot import name 'IntentStrategy' from partially initialized module 'almanak'` |
| 3 | strategy | INFO | CoinGecko fallback mode active | `No CoinGecko API key -- using on-chain pricing (Chainlink oracles) with free CoinGecko as fallback` |
| 4 | gateway | WARNING | Insecure mode (expected for Anvil) | `INSECURE MODE: Auth interceptor disabled - no auth_token configured. This is acceptable for local development on 'anvil'.` |

**Notes:**
- Finding #1: USDC price was fetched with only 0.90 confidence (1/2 sources) due to the rate limit.
  The on-chain Chainlink oracle resolved it correctly ($1.00), so no impact this run. In high-frequency
  production without `COINGECKO_API_KEY` this could cause transient price flapping.
- Finding #2: Pre-existing circular import bug in the incubating `pendle_pt_swap_arbitrum` strategy.
  Surfaces on every runner start but does not affect sushiswap_lp.
- Finding #3: Expected operational fallback; on-chain Chainlink was primary source (confidence 1.00 for WETH).
- Finding #4: Expected insecure-mode warning for local Anvil development. Not a security issue.

No zero prices, token resolution errors, transaction reverts, timeouts, NaN/None values, or stale data detected.

## Result

**PASS** - The `sushiswap_lp` strategy on Anvil (Arbitrum fork) successfully opened a SushiSwap V3
concentrated liquidity position (NFT tokenId 34820) in 3 confirmed transactions with 620,352 total
gas used. Receipt parsing and ResultEnricher extraction (position_id, tick bounds, liquidity) all
succeeded.

---

SUSPICIOUS_BEHAVIOUR_COUNT: 4
SUSPICIOUS_BEHAVIOUR_ERRORS: 0
