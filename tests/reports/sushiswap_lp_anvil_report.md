# E2E Strategy Test Report: sushiswap_lp (Anvil)

**Date:** 2026-03-16 01:43
**Result:** PASS
**Mode:** Anvil
**Duration:** ~3 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | demo_sushiswap_lp |
| Chain | arbitrum |
| Network | Anvil fork |
| Anvil Port | 58121 (auto-assigned by managed gateway) |
| Pool | WETH/USDC/3000 |
| Range Width | 10% |
| amount0 | 0.001 WETH |
| amount1 | 3 USDC |
| force_action | open |

**Config changes made:** None. Trade sizes (0.001 WETH + 3 USDC, ~$5 total) were already well within the $1000 budget cap. `force_action` was already set to `"open"` in config.json.

## Execution

### Setup
- [x] Anvil fork auto-managed by CLI on port 58121 (Arbitrum block 442136722)
- [x] Gateway auto-started on port 50052 (insecure mode, acceptable for Anvil)
- [x] Wallet 0xf39Fd6e5... funded automatically: 100 ETH, 1 WETH (slot 51), 10,000 USDC (slot 9)
- [x] Prices fetched: WETH/USD = $2110.77 (confidence 1.00, 4 sources), USDC/USD = $1.00 (confidence 1.00, 4 sources)

### Strategy Run
- [x] Strategy executed with `--network anvil --once`
- [x] `force_action: "open"` triggered LP_OPEN intent immediately
- [x] Price range computed: [2005.27 - 2216.35] USDC/WETH (ticks -200280 to -199260)
- [x] Intent compiled: 3 transactions (WETH approve + USDC approve + lp_mint), 660,000 gas estimated
- [x] Simulation passed via eth_estimateGas: 923,788 gas (multi-TX sequential)
- [x] All 3 transactions submitted and confirmed on sequential blocks 442136725-727
- [x] LP position ID extracted from SushiSwap V3 receipt: **35563**
- [x] ResultEnricher populated: position_id, tick_lower, tick_upper, liquidity
- [x] Liquidity: 2,099,658,943,592

### Key Log Output

```text
info  Aggregated price for WETH/USD: 2110.765 (confidence: 1.00, sources: 4/4, outliers: 0)
info  Aggregated price for USDC/USD: 0.9999795 (confidence: 1.00, sources: 4/4, outliers: 0)
info  Forced action: OPEN LP position
info  LP_OPEN: 0.0010 WETH + 3.0000 USDC, price range [2005.2679 - 2216.3487], ticks [-200280 - -199260]
info  Compiled LP_OPEN intent: WETH/USDC, range [2005.27-2216.35], 3 txs (approve + approve + lp_mint), 660000 gas
info  Simulation successful: 3 transaction(s), total gas: 923788
info  Sequential submit: TX 1/3 confirmed (block=442136725, gas=53440)
info  Sequential submit: TX 2/3 confirmed (block=442136726, gas=55437)
info  Sequential submit: TX 3/3 confirmed (block=442136727, gas=511503)
info  EXECUTED: LP_OPEN completed successfully | Txs: 3 | 620,380 gas
info  Extracted LP position ID from receipt: 35563
info  Enriched LP_OPEN result with: position_id, tick_lower, tick_upper, liquidity (protocol=sushiswap_v3, chain=arbitrum)
info  SushiSwap V3 LP position opened: position_id=35563, liquidity=2099658943592
Status: SUCCESS | Intent: LP_OPEN | Gas used: 620380 | Duration: 28476ms
```

### Transaction Details

| # | TX Hash | Type | Gas Used | Status |
|---|---------|------|----------|--------|
| 1 | `ee486d5a3998610b647bdaf48423bdaa265bcb42a74359e52cd2e5c75a7343d0` | WETH approve | 53,440 | SUCCESS |
| 2 | `d87dd716ca0ac4b114dc6b1646847d067b3787081f3bf0d289dea0d168cf0a7c` | USDC approve | 55,437 | SUCCESS |
| 3 | `c6f301ee9720ca868da5c675b502dc9946a6faa6b2e0d71cb39386f1fd9a901e` | lp_mint | 511,503 | SUCCESS |

**Total gas:** 620,380 (Anvil transactions - no block explorer links)

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|

None detected. All price sources returned full 4/4 confidence. No zero prices, no API failures, no token resolution errors, no reverts, no timeouts, no NaN/None values, no stale data.

Two expected operational notices were observed and are not anomalies:
- Insecure mode warning: expected for Anvil local development.
- No CoinGecko API key: noted at gateway startup; all 4 price sources (Chainlink, Binance, DexScreener, CoinGecko) responded successfully with confidence 1.00.

## Result

**PASS** - The `sushiswap_lp` strategy on Anvil (Arbitrum fork) successfully opened a SushiSwap V3
concentrated liquidity position (NFT tokenId 35563) in 3 confirmed transactions with 620,380 total
gas used. Receipt parsing and ResultEnricher extraction (position_id, tick bounds, liquidity) all
succeeded.

---

SUSPICIOUS_BEHAVIOUR_COUNT: 0
SUSPICIOUS_BEHAVIOUR_ERRORS: 0
