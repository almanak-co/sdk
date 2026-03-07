# E2E Strategy Test Report: almanak_rsi (Anvil)

**Date:** 2026-03-06 04:54
**Result:** FAIL
**Mode:** Anvil
**Duration:** ~3 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | almanak_rsi |
| Chain | base |
| Network | Anvil fork (auto-managed, fork of https://base-rpc.publicnode.com, block=42979143) |
| Anvil Port | 51544 (auto-started by CLI managed gateway) |
| Config Changes | None (initial_capital_usdc=20, under $50 cap; no force_action field in this strategy) |

## Execution

### Setup
- [x] Gateway auto-managed by CLI on port 50053 (anvil network)
- [x] Anvil Base fork auto-started on port 51544 (public RPC -- ALCHEMY_API_KEY not set in .env)
- [x] Wallet auto-funded via config `anvil_funding`: 100 ETH, 10000 USDC, 1 WETH

### Strategy Run
- [x] Strategy executed with `--network anvil --once`
- Result: **HOLD** -- initialization swap was not attempted

The strategy's initialization phase (`_handle_initialization`) calls `market.price(ALMANAK)` to
pre-populate the price cache for slippage protection before executing the initial buy. This call
failed because:

1. ALMANAK has no Chainlink feed on Base (only ETH, BTC, LINK, USDC, DAI feeds exist)
2. CoinGecko free-tier fallback was rate-limited immediately on cold start (no API key configured)

The strategy correctly handled this failure by returning HOLD instead of executing an unpriced
initialization swap. No on-chain transaction was submitted.

### Key Log Output
```text
[info]    No API key configured -- using free public RPC for base (rate limits may apply)
[warning] Rate limited by CoinGecko for USDC/USD, backoff: 1.00s
[warning] Rate limited by CoinGecko for ALMANAK/USD, backoff: 2.00s
[error]   All data sources failed for ALMANAK/USD:
          onchain: No Chainlink feed for ALMANAK on base. Available: [ETH/USD, BTC/USD, LINK/USD, USDC/USD, DAI/USD]
          coingecko: Data source 'coingecko' rate limited. Retry after 2s
[error]   GetPrice failed for ALMANAK/USD: All data sources failed
[error]   Gateway price request failed for ALMANAK/USD: StatusCode.INTERNAL
[warning] Price oracle failed for ALMANAK/USD: All data sources failed
[warning] Could not pre-populate price data for initialization: Cannot determine price for ALMANAK/USD
[info]    almanak_rsi HOLD: Price data unavailable for init swap: Cannot determine price for ALMANAK/USD
Status: HOLD | Intent: HOLD | Duration: 1200ms
Iteration completed successfully.
```

## On-Chain Transactions

None. No intent was compiled or submitted.

## Root Cause Analysis

The `almanak_rsi` strategy trades a custom token (ALMANAK) that has no Chainlink price feed
on Base. The gateway's primary pricing source (on-chain Chainlink oracle) is unavailable for this
token. The only remaining source is CoinGecko, which was rate-limited on the very first request
because no `ALMANAK_GATEWAY_COINGECKO_API_KEY` is configured in `.env`.

With a CoinGecko API key configured, the strategy is expected to resolve the price and execute the
initialization SWAP successfully (as confirmed by the previous passing run from 2026-03-05).

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | strategy | ERROR | All price sources failed for ALMANAK/USD | `All data sources failed for ALMANAK/USD: onchain: No Chainlink feed for ALMANAK on base; coingecko: rate limited. Retry after 2s` |
| 2 | strategy | ERROR | Gateway gRPC call failed with INTERNAL status | `Gateway price request failed for ALMANAK/USD: StatusCode.INTERNAL ... All data sources failed` |
| 3 | strategy | WARNING | CoinGecko rate-limited on USDC (cold start) | `Rate limited by CoinGecko for USDC/USD, backoff: 1.00s` |
| 4 | strategy | WARNING | CoinGecko rate-limited on ALMANAK (cold start) | `Rate limited by CoinGecko for ALMANAK/USD, backoff: 2.00s` |
| 5 | strategy | INFO | Circular import failure in unrelated incubating strategy | `Failed to import strategy strategies.incubating.pendle_pt_swap_arbitrum.strategy (retry failed): cannot import name 'IntentStrategy' from partially initialized module 'almanak'` |

**Notes:**
- Findings #1-4 are causally linked: ALMANAK has no on-chain price feed and CoinGecko free tier
  is rate-limited on cold start. Root fix: configure `ALMANAK_GATEWAY_COINGECKO_API_KEY` in `.env`.
- Finding #5 is a pre-existing circular import bug in `pendle_pt_swap_arbitrum` (incubating),
  unrelated to this strategy. Should be tracked as a separate issue.

## Result

**FAIL** - The `almanak_rsi` strategy returned HOLD on initialization because the ALMANAK/USD
price could not be fetched: no Chainlink feed exists for ALMANAK on Base, and the free CoinGecko
tier was rate-limited immediately on cold start (no `ALMANAK_GATEWAY_COINGECKO_API_KEY` set).
No on-chain transaction was submitted. With a CoinGecko API key, the strategy is expected to pass.

---

SUSPICIOUS_BEHAVIOUR_COUNT: 5
SUSPICIOUS_BEHAVIOUR_ERRORS: 2
