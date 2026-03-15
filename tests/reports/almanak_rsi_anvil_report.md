# E2E Strategy Test Report: almanak_rsi (Anvil)

**Date:** 2026-03-16 00:34
**Result:** PASS
**Mode:** Anvil
**Duration:** ~3 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | almanak_rsi |
| Chain | base |
| Network | Anvil fork (managed, fork of base-mainnet via Alchemy, block=43403339) |
| Anvil Port | 61940 (auto-started by CLI managed gateway) |
| Config Changes | None (initial_capital_usdc=20, well within 1000 USD budget cap; no force_action field) |

## Execution

### Setup
- [x] Gateway auto-managed by CLI on port 50052 (anvil network)
- [x] Anvil Base fork auto-started on port 61940 (via ALCHEMY_API_KEY)
- [x] Wallet auto-funded via config `anvil_funding`: 100 ETH, 10,000 USDC, 1 WETH

### Strategy Run
- [x] Strategy executed with `--network anvil --once`
- [x] Initialization path taken (first run, `_initialized=False`)
- [x] Intent: SWAP 10.00 USDC -> ALMANAK (half of initial capital, as designed)
- [x] Compiled: 10.0000 USDC -> 4617.8786 ALMANAK (min out: 4571.6999 ALMANAK, 1% slippage)
- [x] 3 transactions submitted and confirmed (approve + permit2 + swap)

### Key Log Output
```text
INITIALIZATION: First run - buying ALMANAK for $10.00 (half of initial capital)
intent: SWAP: 10.000000 USDC -> ALMANAK (slippage: 1.00%) via uniswap_v3
Compiled SWAP: 10.0000 USDC -> 4617.8786 ALMANAK (min: 4571.6999 ALMANAK)
Sequential submit: TX 1/3 confirmed (block=43403342, gas=33501)
Sequential submit: TX 2/3 confirmed (block=43403343, gas=55437)
Sequential submit: TX 3/3 confirmed (block=43403344, gas=150645)
EXECUTED: SWAP completed successfully
Txs: 3 (09cd1a...a269, 6d9d1b...d366, 717eef...cb9a) | 239,583 gas
Initialization swap succeeded - strategy is now initialized
Trade executed successfully (total trades: 1)
Status: SUCCESS | Intent: SWAP | Gas used: 239583 | Duration: 28382ms
```

## Transactions (Anvil - not real)

| TX # | TX Hash | Gas Used | Status |
|------|---------|----------|--------|
| 1/3 (USDC approve) | `09cd1ac93fe35e5947d31595f05e6ad0796d5c999dbdf8eb4bc3162d2cf8a269` | 33,501 | SUCCESS |
| 2/3 (Permit2 approve) | `6d9d1ba69f5e9d392a0ec05780191fcb946d17d3079fd4f6ea47e9a57ed4d366` | 55,437 | SUCCESS |
| 3/3 (Uniswap V3 swap) | `717eef17a8968980ba18e4207679aff1a980c195b40b23867f5eaf4109d4cb9a` | 150,645 | SUCCESS |

**Total gas used:** 239,583

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | strategy | WARNING | CoinGecko rate limit (USDC/USD, free tier) | `Rate limited by CoinGecko for USDC/USD, backoff: 1.00s` |
| 2 | strategy | WARNING | CoinGecko rate limit (ALMANAK/USD, free tier) | `Rate limited by CoinGecko for ALMANAK/USD, backoff: 2.00s` |
| 3 | strategy | INFO | ALMANAK price from only 1 of 4 sources (DexScreener only) | `Aggregated price for ALMANAK/USD: 0.002159 (confidence: 0.70, sources: 1/4, outliers: 0)` |
| 4 | strategy | INFO | No CoinGecko API key, using Chainlink as primary | `No CoinGecko API key -- using on-chain pricing (Chainlink oracles) with free CoinGecko as fallback.` |
| 5 | strategy | INFO | slippage=N/A in receipt parser output | `Parsed Uniswap V3 swap: 0.0000 token0 -> 4616.0143 token1, slippage=N/A` |

**Notes:**
- Items 1 & 2: CoinGecko free-tier rate limits on cold start are expected without an API key. Both resolved via exponential backoff and execution completed successfully. The strategy that previously FAILED (2026-03-06) due to this issue now succeeds because DexScreener provided the ALMANAK price before CoinGecko retried.
- Item 3: ALMANAK has no Chainlink feed on Base; DexScreener is the sole working price source (confidence 0.70). Acceptable for slippage protection at this price point ($0.002159).
- Item 4: Expected startup message, no operational impact.
- Item 5: `slippage=N/A` in receipt parser is a cosmetic limitation -- the actual swap output (4616.0143 ALMANAK) exceeded the minimum guaranteed output (4571.6999), confirming no slippage violation.

No zero prices, no ERROR-level log lines, no token resolution failures, no reverts.

## Regression Note

The previous run (2026-03-06) was a **FAIL** due to CoinGecko rate limiting exhausting all price sources. This run **PASSES** because DexScreener now resolves the ALMANAK price independently of CoinGecko, providing sufficient data for slippage protection. The fix was in the pricing aggregator layer.

## Result

**PASS** - The almanak_rsi strategy executed the initialization SWAP (10 USDC -> ~4616 ALMANAK via Uniswap V3 on Base Anvil fork) successfully across 3 transactions with total gas of 239,583 units.

---

SUSPICIOUS_BEHAVIOUR_COUNT: 5
SUSPICIOUS_BEHAVIOUR_ERRORS: 0
