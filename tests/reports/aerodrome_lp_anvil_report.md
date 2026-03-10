# E2E Strategy Test Report: aerodrome_lp (Anvil)

**Date:** 2026-03-05 21:42 UTC
**Result:** PASS
**Mode:** Anvil
**Duration:** ~4 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | aerodrome_lp (`demo_aerodrome_lp`) |
| Chain | base |
| Network | Anvil fork (Base mainnet, block 42978791) |
| Anvil Port | 65354 (auto-assigned by managed gateway) |
| Pool | WETH/USDC volatile |
| Amount0 | 0.001 WETH |
| Amount1 | 0.04 USDC |
| Force Action | open (pre-set in config) |

## Config Changes Made

- `"network"` changed from `"mainnet"` to `"anvil"` before the run, restored after.
- Amounts were already within budget cap: 0.001 WETH (~$2.08) + 0.04 USDC = well under $50. No reduction needed.

## Execution

### Setup
- Managed gateway auto-started with its own Anvil fork (Base at block 42978791, chain ID 8453, port 65354)
- Gateway started on port 50053
- Wallet `0xf39Fd6e5...` funded by managed gateway: 100 ETH, 1 WETH, 10,000 USDC (from `anvil_funding` config)

### Strategy Run
- Strategy executed with `--network anvil --once`
- `force_action=open` triggered LP_OPEN intent immediately
- Prices fetched: WETH/USD = 2080.74 (confidence 0.90, 1/2 sources via Chainlink), USDC/USD = 1.00
- Intent compiled to 3 transactions: WETH approve + USDC approve + addLiquidity
- All 3 transactions confirmed sequentially on Anvil fork

### Transactions

| TX # | Action | Hash | Block | Gas Used | Status |
|------|--------|------|-------|----------|--------|
| 1/3 | WETH approve | `0x9986dabed5143b483cefa11dcf4a6dcd79c12f4d1e25c68b08b2b7a08a6a6447` | 42978794 | 46,343 | CONFIRMED |
| 2/3 | USDC approve | `0x4117eea19cf54b12a9e4b5ec02ce50301b818d475f522a3f59ee843ef11c0d6a` | 42978795 | 55,785 | CONFIRMED |
| 3/3 | addLiquidity | `0x74986ac579728c519b3edae5b7dcb260b81c586e4c6b75dbfc5f2559d2186e07` | 42978796 | 240,012 | CONFIRMED |

**Total gas used:** 342,140

### Key Log Output
```text
Aggregated price for WETH/USD: 2080.74 (confidence: 0.90, sources: 1/2, outliers: 0)
Aggregated price for USDC/USD: 1.00 (confidence: 0.90, sources: 1/2, outliers: 0)
Forced action: OPEN LP position
LP_OPEN: 0.0010 WETH + 0.0400 USDC, pool_type=volatile
Compiled Aerodrome LP_OPEN intent: WETH/USDC, stable=False, 3 txs (approve + approve + add_liquidity), 312000 gas
Simulation successful: 3 transaction(s), total gas: 445343
Sequential submit: TX 1/3 confirmed (block=42978794, gas=46343)
Sequential submit: TX 2/3 confirmed (block=42978795, gas=55785)
Sequential submit: TX 3/3 confirmed (block=42978796, gas=240012)
EXECUTED: LP_OPEN completed successfully
Txs: 3 (9986da...6447, 4117ee...0d6a, 74986a...6e07) | 342,140 gas
Parsed Aerodrome add liquidity: token0/token1, tx=0x7498...6e07, 240,012 gas
Enriched LP_OPEN result with: liquidity (protocol=aerodrome, chain=base)
Aerodrome LP position opened successfully
Status: SUCCESS | Intent: LP_OPEN | Gas used: 342140 | Duration: 34422ms
```

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | strategy | WARNING | CoinGecko rate limit (no API key, free tier) | `Rate limited by CoinGecko for WETH/USD, backoff: 1.00s` |
| 2 | strategy | WARNING | CoinGecko rate limit (no API key, free tier) | `Rate limited by CoinGecko for USDC/USD, backoff: 2.00s` |
| 3 | strategy | INFO | Circular import in unrelated incubating strategy | `Failed to import strategy strategies.incubating.pendle_pt_swap_arbitrum.strategy: cannot import name 'IntentStrategy' from partially initialized module 'almanak'` |
| 4 | strategy | INFO | No CoinGecko API key (expected for Anvil dev) | `No CoinGecko API key -- using on-chain pricing (Chainlink oracles) with free CoinGecko as fallback.` |
| 5 | strategy | INFO | No Alchemy key, free public RPC | `No API key configured -- using free public RPC for base (rate limits may apply)` |

**Notes:**
- Findings 1-2: CoinGecko rate limits are expected on free tier with no API key. The system retried and successfully obtained WETH and USDC prices via Chainlink on-chain oracles. Non-blocking; prices are valid.
- Finding 3: Pre-existing circular import in `pendle_pt_swap_arbitrum` incubating strategy. Non-blocking; does not affect `aerodrome_lp`. Worth fixing separately.
- Findings 4-5: Expected for a local Anvil dev environment with no API keys configured.
- No zero prices, no reverts, no token resolution failures, no execution errors detected.

## Result

**PASS** -- The `aerodrome_lp` strategy compiled an `LP_OPEN` intent and executed 3 on-chain transactions (WETH approve, USDC approve, addLiquidity) on an Anvil-forked Base at block 42978791. All 3 transactions confirmed. Prices fetched correctly via Chainlink. The Aerodrome receipt parser correctly identified the addLiquidity event and the `ResultEnricher` enriched the result with liquidity data.

SUSPICIOUS_BEHAVIOUR_COUNT: 5
SUSPICIOUS_BEHAVIOUR_ERRORS: 0
