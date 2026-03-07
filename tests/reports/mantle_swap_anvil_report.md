# E2E Strategy Test Report: mantle_swap (Anvil)

**Date:** 2026-03-05 22:10
**Result:** FAIL
**Mode:** Anvil
**Duration:** ~4 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | demo_mantle_swap |
| Chain | mantle (chain ID 5000) |
| Network | Anvil fork of https://rpc.mantle.xyz |
| Anvil Port | 8556 |
| Base Token | WETH |
| Quote Token | USDT |
| Trade Size | $5 USD (under $50 cap, no change needed) |
| RSI Period | 14 |
| RSI Oversold | 35 |
| RSI Overbought | 65 |

## Config Changes Made

None. `trade_size_usd` is 5, well under the $50 budget cap. Strategy does not support `force_action`.

## Execution

### Setup
- [x] Killed existing Anvil and Gateway processes on ports 8556, 50051, 9090
- [x] Anvil started on port 8556 (Mantle fork via https://rpc.mantle.xyz)
- [x] Gateway started successfully (managed gateway auto-started on port 50053 as subprocess)
- [x] Wallet funded: 1000 MNT (native), 0.1 WETH (slot 0), 10,000 USDT (slot 0)

### Strategy Run
- [x] Strategy executed with `--network anvil --once`
- [x] Managed gateway auto-started on 127.0.0.1:50053 with Anvil fork on port 55656
- [x] Auto-funding from `anvil_funding` config applied successfully (MNT, WETH, USDT)
- [x] Strategy iteration completed in 89ms
- [x] Intent: HOLD — price fetch failed for WETH/USD
- No swap transaction submitted

### Key Log Output
```text
[info]    No API key configured -- using free public RPC for mantle (rate limits may apply)
[info]    Anvil fork started: port=55656, block=92309167, chain_id=5000
[info]    Funded 0xf39Fd6e5... with 1000 ETH
[info]    Funded 0xf39Fd6e5... with WETH via brute-force slot 0
[info]    Funded 0xf39Fd6e5... with USDT via brute-force slot 0
[warning] Rate limited by CoinGecko for WETH/USD, backoff: 1.00s
[error]   All data sources failed for WETH/USD: {
            'onchain': "Data source 'onchain' unavailable: No Chainlink feed for WETH on mantle. Available feeds: []",
            'coingecko': "Data source 'coingecko' rate limited. Retry after 1s"
          }
[error]   GetPrice failed for WETH/USD: All data sources failed
[error]   Error in decide(): Cannot determine price for WETH/USD
[info]    demo_mantle_swap HOLD: Error: Cannot determine price for WETH/USD
Status: HOLD | Intent: HOLD | Duration: 89ms
Iteration completed successfully.
```

### Root Cause

The strategy returned HOLD due to a simultaneous failure of both price data sources:

1. **No Chainlink feed for WETH on Mantle** — the on-chain (primary) pricing source has no Chainlink feeds registered for the Mantle chain (`Available feeds: []`). The framework correctly falls back to CoinGecko.
2. **CoinGecko free tier rate limited immediately** — the free tier was rate limited from a previous test session (`Rate limited by CoinGecko for WETH/USD, backoff: 1.00s`). With no retry before giving up, both sources failed together.

The strategy's `decide()` method correctly caught the `ValueError` and returned `Intent.hold(reason="Error: ...")` rather than crashing. However, the strategy was unable to evaluate RSI or execute any trade.

**Mitigations available:**
- Set `ALMANAK_GATEWAY_COINGECKO_API_KEY` for a paid CoinGecko tier with higher rate limits
- Run tests with a longer gap between consecutive gateway sessions to avoid rate limit carryover

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | strategy | ERROR | No Chainlink feed on Mantle — on-chain pricing unavailable | `Data source 'onchain' unavailable: No Chainlink feed for WETH on mantle. Available feeds: []` |
| 2 | strategy | ERROR | CoinGecko free tier rate limited on first request | `Rate limited by CoinGecko for WETH/USD, backoff: 1.00s` |
| 3 | strategy | ERROR | All price sources failed — strategy cannot operate | `All data sources failed for WETH/USD` |
| 4 | strategy | WARNING | Circular import in incubating strategy (pre-existing, unrelated) | `Failed to import strategy strategies.incubating.pendle_pt_swap_arbitrum.strategy: cannot import name 'IntentStrategy' from partially initialized module 'almanak'` |

**Notes:**
- Findings #1-3 are the direct cause of the FAIL. Mantle has no Chainlink oracles, making it entirely dependent on CoinGecko for pricing. The free CoinGecko tier is fragile under rapid re-testing conditions.
- Finding #4 is a pre-existing circular import defect in an unrelated incubating strategy.
- The strategy itself is correctly implemented — it handled the error gracefully and returned HOLD.

## Chain-Specific Notes (Mantle)

- **Native gas token**: MNT (funded as native balance, correct chain ID 5000)
- **WETH address**: `0xdEAddEaDdeadDEadDEADDEAddEADDEAddead1111` (bridged WETH)
- **USDT address**: `0x201EBa5CC46D216Ce6DC03F6a759e8E766e956aE` (6 decimals)
- **Anvil port**: 8556 (correctly configured in `ANVIL_CHAIN_PORTS`)
- **RPC**: Public `https://rpc.mantle.xyz` (Alchemy does not support Mantle)
- **Chainlink oracles**: None registered for Mantle in the gateway — all pricing depends on CoinGecko

## Transactions

None — the strategy returned HOLD before any intent was compiled or executed.

## Result

**FAIL** — Strategy returned HOLD on first iteration due to complete price data unavailability. No Chainlink feeds exist for WETH on Mantle, and CoinGecko free tier was rate limited from a prior session. No on-chain transaction was produced. The strategy code itself is correct; the failure is in the data layer.

---

SUSPICIOUS_BEHAVIOUR_COUNT: 4
SUSPICIOUS_BEHAVIOUR_ERRORS: 3
