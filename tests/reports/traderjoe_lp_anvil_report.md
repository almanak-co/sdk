# E2E Strategy Test Report: traderjoe_lp (Anvil)

**Date:** 2026-03-06 05:55
**Result:** PASS
**Mode:** Anvil
**Duration:** ~3 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | traderjoe_lp |
| Chain | avalanche |
| Network | Anvil fork (publicnode.com, block 79660060) |
| Anvil Port | 49341 (auto-assigned by managed gateway) |
| Pool | WAVAX/USDC/20 |
| amount_x | 0.001 WAVAX |
| amount_y | 3 USDC |
| num_bins | 11 |
| range_width_pct | 10% |
| Budget check | ~$2.94 total (0.001 WAVAX @ $9.43 + 3 USDC) -- well within $50 cap |

## Config Changes Made

- Added `"force_action": "open"` temporarily to trigger an immediate LP_OPEN intent (restored after test).
- Amounts (0.001 WAVAX + 3 USDC) were already well within the $50 budget cap; no amount changes needed.

## Setup

- `uv run almanak strat run -d strategies/demo/traderjoe_lp --network anvil --once` auto-started the managed gateway and Anvil fork.
- Forked Avalanche (chain ID 43114) from `https://avalanche-c-chain-rpc.publicnode.com` at block 79660060.
  - Note: `ALCHEMY_API_KEY` is empty in `.env`; the framework correctly fell back to the public RPC.
- Wallet funded by managed gateway from `anvil_funding` config: 100 AVAX, 100 WAVAX (slot 3), 10,000 USDC (slot 9).
- Gateway started on 127.0.0.1:50053 (managed mode).

## Strategy Execution

Strategy detected `force_action = "open"` and immediately returned an LP_OPEN intent.

### Intent Flow

| Step | Detail |
|------|--------|
| Price fetched | WAVAX/USD = $9.428 (sources: 2/2, confidence 1.00) |
| Price fetched | USDC/USD = $1.00 (sources: 1/2, confidence 0.90 -- CoinGecko rate-limited, on-chain fallback) |
| Computed price range | [8.9567 - 9.8995] USDC/WAVAX (10% width) |
| Intent | LP_OPEN: WAVAX/USDC/20 (0.001, 3) [9 - 10] via traderjoe_v2 |
| Compilation | 3 txs: approve WAVAX + approve USDC + traderjoe_v2_add_liquidity, 860,000 gas |
| Simulation | PASS (LocalSimulator / eth_estimateGas), total gas 904,123 |

### Transaction Results

| TX # | Purpose | Hash | Block | Gas Used | Status |
|------|---------|------|-------|----------|--------|
| 1/3 | WAVAX approve | `72a2b9bb4f419562a2fda9276b13680af9748694c817d0cc76e1894131509232` | 79660063 | 46,123 | SUCCESS |
| 2/3 | USDC approve | `ca3d8ab3d449f3f82eca300f840ff15144b3b27639bdbd84fd14dbb8d6eba34f` | 79660064 | 55,437 | SUCCESS |
| 3/3 | add_liquidity | `56fef9282c2ff835214d9a2c93fcbb9df44a13e8938c74147d04c827546bba60` | 79660065 | 598,194 | SUCCESS |

**Total gas used: 699,754 | Duration: 37,316ms**

### Key Log Output

```text
Aggregated price for WAVAX/USD: 9.428063915 (confidence: 1.00, sources: 2/2, outliers: 0)
Aggregated price for USDC/USD: 1.00 (confidence: 0.90, sources: 1/2, outliers: 0)
Forced action: OPEN LP position
LP_OPEN: 0.0010 WAVAX + 3.0000 USDC, price range [8.9567 - 9.8995], bin_step=20
Compiled TraderJoe V2 LP_OPEN intent: WAVAX/USDC, bin_step=20, 3 txs (approve + approve + traderjoe_v2_add_liquidity), 860000 gas
Simulation successful: 3 transaction(s), total gas: 904123
EXECUTED: LP_OPEN completed successfully
   Txs: 3 (72a2b9...9232, ca3d8a...a34f, 56fef9...ba60) | 699,754 gas
Enriched LP_OPEN result with: bin_ids (protocol=traderjoe_v2, chain=avalanche)
TraderJoe LP position opened successfully
Status: SUCCESS | Intent: LP_OPEN | Gas used: 699754 | Duration: 37316ms
Iteration completed successfully.
```

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | strategy | WARNING | CoinGecko rate limit on USDC price | `Rate limited by CoinGecko for USDC/USD, backoff: 1.00s` -- price fell back to on-chain; resolved correctly (confidence 0.90) |
| 2 | strategy | INFO | Pendle incubating strategy circular import | `Failed to import strategy strategies.incubating.pendle_pt_swap_arbitrum.strategy (retry failed): cannot import name 'IntentStrategy' from partially initialized module 'almanak'` -- unrelated to traderjoe_lp, cosmetic |
| 3 | strategy | WARNING | LP add_liquidity TX mislabeled as "swap" in receipt parser log | `Parsed TraderJoe V2 swap: 1,000,000,000,000,000 -> 5` -- raw wei shown without decimal conversion; underlying LP succeeded and bin_ids were correctly extracted |
| 4 | strategy | INFO | Each receipt parsed twice by TraderJoe V2 parser | All 3 TX receipts appear in parser logs exactly twice -- ResultEnricher invokes parser twice per receipt; non-blocking but generates redundant log noise |

**Finding 1**: CoinGecko free-tier rate limit caused a 1-second backoff for USDC/USD. Price was
recovered from on-chain (Chainlink) successfully at confidence 0.90. Non-blocking but a known
limitation of the free CoinGecko tier.

**Finding 2**: The `pendle_pt_swap_arbitrum` incubating strategy has a circular import error at
startup. This is pre-existing and unrelated to traderjoe_lp. Worth tracking as a separate ticket.

**Finding 3**: TraderJoe V2 emits an internal Swap event during `add_liquidity` calls. The receipt
parser detects this event and logs it as a "swap" with raw wei amounts (not human-readable decimals).
The LP open operation succeeded and bin_ids were correctly enriched. The log label is misleading.
This is a persisting issue across multiple test runs.

**Finding 4**: All 3 TX receipts are passed to the TraderJoe V2 receipt parser twice each. This
appears to be a double-invocation in the ResultEnricher/orchestrator path. Non-blocking but creates
noise and slightly increases processing time.

## Result

**PASS** - The traderjoe_lp strategy on Avalanche Anvil executed successfully. The LP_OPEN intent
compiled to 3 transactions, all simulated and confirmed on the Anvil fork. bin_ids were correctly
extracted by the ResultEnricher. No hard errors or reverts. The strategy completed with
`Status: SUCCESS | Gas used: 699,754 | Duration: 37,316ms`.

---

SUSPICIOUS_BEHAVIOUR_COUNT: 4
SUSPICIOUS_BEHAVIOUR_ERRORS: 0
