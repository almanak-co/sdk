# E2E Strategy Test Report: traderjoe_lp (Anvil)

**Date:** 2026-03-03 12:28
**Result:** PASS
**Mode:** Anvil
**Duration:** ~5 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | traderjoe_lp |
| Chain | avalanche |
| Network | Anvil fork |
| Anvil Port | 8547 (auto-selected by managed gateway on ephemeral port 57787) |
| Pool | WAVAX/USDC/20 |
| amount_x | 0.001 WAVAX |
| amount_y | 3 USDC |
| num_bins | 11 |
| range_width_pct | 10% |

## Config Changes Made

- Added `"force_action": "open"` temporarily to trigger an immediate LP_OPEN intent (restored after test)
- Amounts (0.001 WAVAX + 3 USDC) were already within the $500 budget cap; no reduction needed

## Execution

### Setup
- Anvil fork of Avalanche (chain ID 43114) started using public RPC `https://avalanche-c-chain-rpc.publicnode.com` (ALCHEMY_API_KEY not set in .env)
- Note: The `--network anvil` managed gateway auto-starts its own Anvil fork; the manually started fork on port 8547 was superseded by the ephemeral fork on port 57787
- Wallet funded by managed gateway: AVAX (native), WAVAX (slot 3), USDC (slot 9) per `anvil_funding` config
- Gateway started on port 50052 (managed mode)

### Strategy Run
- Strategy executed with `uv run almanak strat run -d strategies/demo/traderjoe_lp --network anvil --once`
- Price fetched: WAVAX = $9.07, USDC = $1.00 (2/2 sources, confidence 1.00)
- LP range calculated: [8.6162, 9.5232] USDC/WAVAX (10% width, centered on market price)
- Intent returned: LP_OPEN on WAVAX/USDC/20 via traderjoe_v2
- Compilation: 3 transactions (2x approve + 1x add_liquidity), estimated 860,000 gas
- Simulation: PASS (LocalSimulator via eth_estimateGas)
- Execution: All 3 transactions confirmed

| TX # | Hash | Block | Gas Used | Action |
|------|------|-------|----------|--------|
| 1/3 | `9e26c1bb...2341` | 79456699 | 46,123 | WAVAX approve |
| 2/3 | `b4fa8569...c8fc` | 79456700 | 55,437 | USDC approve |
| 3/3 | `625c887d...3ddb` | 79456701 | 598,366 | traderjoe_v2 add_liquidity |

**Total gas used:** 699,926

- Result Enricher: bin_ids extracted successfully (`Enriched LP_OPEN result with: bin_ids`)
- Strategy callback: `TraderJoe LP position opened successfully`

### Key Log Output
```text
Aggregated price for WAVAX/USD: 9.069546545000001 (confidence: 1.00, sources: 2/2, outliers: 0)
Aggregated price for USDC/USD: 0.99998 (confidence: 1.00, sources: 2/2, outliers: 0)
Forced action: OPEN LP position
LP_OPEN: 0.0010 WAVAX + 3.0000 USDC, price range [8.6162 - 9.5232], bin_step=20
Compiled TraderJoe V2 LP_OPEN intent: WAVAX/USDC, bin_step=20, 3 txs (approve + approve + traderjoe_v2_add_liquidity), 860000 gas
EXECUTED: LP_OPEN completed successfully
   Txs: 3 (9e26c1...2341, b4fa85...c8fc, 625c88...3ddb) | 699,926 gas
Enriched LP_OPEN result with: bin_ids (protocol=traderjoe_v2, chain=avalanche)
TraderJoe LP position opened successfully
Status: SUCCESS | Intent: LP_OPEN | Gas used: 699926 | Duration: 40398ms
Iteration completed successfully.
```

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | strategy | INFO | Insecure mode (expected for Anvil) | `INSECURE MODE: Auth interceptor disabled - no auth_token configured. This is acceptable for local development on 'anvil'.` |
| 2 | strategy | WARNING | Port not freed cleanly | `Port 57787 not freed after 5.0s` |
| 3 | strategy | INFO | No Alchemy key, using free public RPC | `No API key configured -- using free public RPC for avalanche (rate limits may apply)` |
| 4 | strategy | WARNING | Receipt parsed twice per TX | Each of 3 txs parsed by TraderJoe V2 receipt parser exactly twice (lines 130-132 then 133-135). Double-parse bug in result enrichment pipeline. |
| 5 | strategy | WARNING | LP add_liquidity TX logged as "swap" with near-zero output | `Parsed TraderJoe V2 swap: 1,000,000,000,000,000 to 5, tx=0x625c...3ddb` — the add_liquidity TX is labeled as a "swap" with raw amounts (0.001 WAVAX wei to 5 micro-USDC). Parser detects internal Swap event inside the add_liquidity call but labels it misleadingly, and shows raw wei values without decimal conversion. |

**Finding 4 detail** - Duplicate receipt parsing: Each of the 3 transactions is parsed by the TraderJoe V2 receipt parser exactly twice. This is a double-parse bug in the result enrichment pipeline. It does not cause incorrect behavior but generates redundant log noise and indicates the enricher or orchestrator is calling the parser twice per receipt.

**Finding 5 detail** - The receipt parser classifies the add_liquidity TX (0x625c) as "swap" with `amount_in=1,000,000,000,000,000` (= 0.001 WAVAX in wei) and `amount_out=5` (= 0.000005 USDC in raw units). While the LP operation succeeded (bin_ids were enriched correctly), the receipt log label is misleading. The `5` raw USDC output represents an internal routing swap within the add_liquidity call but is presented without decimal conversion. Should be labeled `LP_OPEN` not `swap`, and amounts should be shown in human-readable decimal form.

## Result

**PASS** - The traderjoe_lp strategy on Avalanche Anvil executed successfully: LP_OPEN intent compiled, simulated, and executed across 3 on-chain transactions with all receipts confirmed, bin_ids extracted, and the strategy callback invoked. Two WARNING-level suspicious findings (duplicate receipt parsing, misleading "swap" log for LP tx) are non-blocking but worth investigating.

---

SUSPICIOUS_BEHAVIOUR_COUNT: 5
SUSPICIOUS_BEHAVIOUR_ERRORS: 0
