# E2E Strategy Test Report: traderjoe_lp (Anvil)

**Date:** 2026-03-16 01:47
**Result:** PASS
**Mode:** Anvil
**Duration:** ~3 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | demo_traderjoe_lp |
| Chain | avalanche |
| Network | Anvil fork (Alchemy, block 80450400) |
| Anvil Port | 58607 (auto-assigned by managed gateway) |
| Pool | WAVAX/USDC/20 |
| Amount X | 0.001 WAVAX |
| Amount Y | 3 USDC |
| Num Bins | 11 |
| Range Width | 10% |
| Budget check | ~$3.10 total (0.001 WAVAX @ $9.75 + 3 USDC) -- well within $1000 cap |

## Config Changes Made

- Added `"force_action": "open"` temporarily to trigger an immediate LP_OPEN intent (restored after run).
- Trade sizes (0.001 WAVAX + 3 USDC) were already well within the $1000 budget cap. No amount changes needed.

## Setup

- `uv run almanak strat run -d strategies/demo/traderjoe_lp --network anvil --once` auto-started the managed gateway and Anvil fork.
- Forked Avalanche (chain ID 43114) from Alchemy at block 80450400.
- Wallet (`0x54776446...`) funded by managed gateway from `anvil_funding` config: 100 AVAX, 100 WAVAX (slot 3), 10,000 USDC (slot 9).
- Gateway started on 127.0.0.1:50052 (managed mode).

## Strategy Execution

Strategy detected `force_action = "open"` and immediately returned an LP_OPEN intent.

### Intent Flow

| Step | Detail |
|------|--------|
| Price fetched | WAVAX/USD = $9.75005 (sources: 4/4, confidence 1.00) |
| Price fetched | USDC/USD = $1.00 (sources: 4/4, confidence 1.00) |
| Computed price range | [9.2625 - 10.2376] USDC/WAVAX (10% width) |
| Intent | LP_OPEN: WAVAX/USDC/20 (0.001, 3) [9 - 10] via traderjoe_v2 |
| Compilation | 3 txs: approve_reset + approve + traderjoe_v2_add_liquidity, 860,000 gas |
| Simulation | PASS (LocalSimulator / eth_estimateGas), total gas 896,411 |

### Transaction Results

| TX # | Purpose | Hash | Block | Gas Used | Status |
|------|---------|------|-------|----------|--------|
| 1/3 | WAVAX approve reset | `0xcc1ce937...bcdf` | 80450403 | 33,501 | SUCCESS |
| 2/3 | WAVAX approve | `0x544cfe49...cba9` | 80450404 | 55,437 | SUCCESS |
| 3/3 | add_liquidity | `0xe220c6c9...eaa4` | 80450405 | 460,902 | SUCCESS |

**Total gas used: 549,840 | Duration: 28,342ms**

### Key Log Output

```text
Aggregated price for WAVAX/USD: 9.75005 (confidence: 1.00, sources: 4/4, outliers: 0)
Aggregated price for USDC/USD: 1.0 (confidence: 1.00, sources: 4/4, outliers: 0)
Forced action: OPEN LP position
LP_OPEN: 0.0010 WAVAX + 3.0000 USDC, price range [9.2625 - 10.2376], bin_step=20
Compiled TraderJoe V2 LP_OPEN intent: WAVAX/USDC, bin_step=20, 3 txs (approve_reset + approve + traderjoe_v2_add_liquidity), 860000 gas
Simulation successful: 3 transaction(s), total gas: 896411
EXECUTED: LP_OPEN completed successfully
   Txs: 3 (cc1ce9...bcdf, 544cfe...cba9, e220c6...eaa4) | 549,840 gas
Enriched LP_OPEN result with: bin_ids (protocol=traderjoe_v2, chain=avalanche)
TraderJoe LP position opened successfully
Status: SUCCESS | Intent: LP_OPEN | Gas used: 549840 | Duration: 28342ms
Iteration completed successfully.
```

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | strategy | WARNING | LP add_liquidity TX logged as "swap" in receipt parser | `Parsed TraderJoe V2 swap: 1,000,000,000,000,000 -> 5, tx=0xe220...eaa4` -- raw wei shown; internally correct, LP succeeded and bin_ids enriched |
| 2 | strategy | INFO | Each receipt parsed twice by TraderJoe V2 parser | All 3 TX receipts appear in parser logs exactly twice -- ResultEnricher invokes parser twice per receipt; non-blocking but generates redundant log noise |
| 3 | strategy | INFO | No CoinGecko API key (non-blocking) | `No CoinGecko API key -- using on-chain pricing (Chainlink oracles) with free CoinGecko as fallback` -- expected for local dev; 4-source pricing still achieved full confidence |
| 4 | strategy | INFO | Insecure mode warning | `INSECURE MODE: Auth interceptor disabled -- acceptable for local development on 'anvil'` -- expected for Anvil testing |

**Finding 1**: TraderJoe V2 emits an internal Swap event during `add_liquidity` calls. The receipt
parser detects this and logs it as a "swap" with raw wei amounts rather than human-readable decimals.
The LP open succeeded and bin_ids were correctly enriched. This is a persistent cosmetic log issue
across multiple test runs -- misleading label, not a bug in execution.

**Finding 2**: All 3 TX receipts are passed to the TraderJoe V2 receipt parser twice each. This
appears to be a double-invocation in the ResultEnricher/orchestrator path. Non-blocking but generates
log noise and slightly increases processing time.

**Findings 3 and 4**: Both are expected and benign for Anvil mode. No zero prices, API failures,
reverts, token resolution errors, or actual timeouts detected.

## Result

**PASS** - The traderjoe_lp strategy on Avalanche Anvil executed successfully. The LP_OPEN intent
compiled to 3 transactions, all simulated and confirmed on the Anvil fork. bin_ids were correctly
extracted by the ResultEnricher. No hard errors or reverts. The strategy completed with
`Status: SUCCESS | Gas used: 549,840 | Duration: 28,342ms`.

---

SUSPICIOUS_BEHAVIOUR_COUNT: 4
SUSPICIOUS_BEHAVIOUR_ERRORS: 0
