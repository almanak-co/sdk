# E2E Strategy Test Report: uniswap_lp (Anvil)

**Date:** 2026-02-27 16:52-16:53
**Result:** PASS
**Mode:** Anvil
**Duration:** ~45 seconds

## Configuration

| Field | Value |
|-------|-------|
| Strategy | demo_uniswap_lp |
| Chain | arbitrum |
| Network | Anvil fork (public RPC: arbitrum-one-rpc.publicnode.com) |
| Anvil Port | 52409 (auto-assigned by managed gateway) |
| Pool | WETH/USDC/500 |
| amount0 | 0.001 WETH |
| amount1 | 3 USDC |
| range_width_pct | 20% |
| Budget cap | $500 (no change needed; 0.001 WETH + 3 USDC well within cap) |

## Config Changes Made

- Temporarily added `"force_action": "open"` to trigger immediate LP open on first run
- Restored to original after test (field removed)

## Execution

### Setup
- [x] Managed gateway auto-started on port 50052 (insecure mode, acceptable for Anvil)
- [x] Anvil fork started on port 52409 (block 436575775, chain_id=42161)
- [x] Wallet (0xf39Fd6e5...) auto-funded: 100 ETH, 1 WETH (slot 51), 10,000 USDC (slot 9)
- [x] RPC: https://arbitrum-one-rpc.publicnode.com (ALCHEMY_API_KEY not set in .env)

### Strategy Run
- [x] Strategy executed with `--network anvil --once`
- [x] force_action="open" triggered LP_OPEN immediately
- [x] ETH price: $1,927.01 (WETH/USD, confidence 1.00, 2/2 sources)
- [x] USDC price: $0.9999 (confidence 1.00, 2/2 sources)
- [x] LP range calculated: [$1,734.39 - $2,119.81] (±10% of current ETH price)
- [x] Compiled: 3 txs (approve WETH + approve USDC + lp_mint), 660,000 gas estimated
- [x] All 3 transactions confirmed on Anvil fork
- [x] LP position opened: position_id=5333197
- [x] Result enriched: position_id, tick_lower, tick_upper, liquidity

### Transaction Details (Anvil local fork -- not on public chain)

| # | TX Hash | Block | Gas Used | Status |
|---|---------|-------|----------|--------|
| 1 (WETH approve) | `1fa2a8b73ad94905b00540aafec27e12ceac90004a602e09343dca244aef2f21` | 436575778 | 53,440 | SUCCESS |
| 2 (USDC approve) | `66e7d3d8d346880b2dad0f0ec0ec40e34d0a94a33c063d234840ad71ce5bbea5` | 436575779 | 55,437 | SUCCESS |
| 3 (LP mint) | `e16571c18e2f6ba214742ef6edc4f8b818113daee4f96eaff9a3b9a8cca218a9` | 436575780 | 448,917 | SUCCESS |

**Total gas used: 557,794 | LP Position ID: 5333197**

### Key Log Output
```text
Aggregated price for WETH/USD: 1927.0055655 (confidence: 1.00, sources: 2/2, outliers: 0)
Aggregated price for USDC/USD: 0.9999495 (confidence: 1.00, sources: 2/2, outliers: 0)
Forced action: OPEN LP position
LP_OPEN: 0.0010 WETH + 3.0000 USDC, range [$1,734.39 - $2,119.81]
Compiled LP_OPEN intent: WETH/USDC, range [1734.39-2119.81], 3 txs (approve + approve + lp_mint), 660000 gas
EXECUTED: LP_OPEN completed successfully
Txs: 3 (1fa2a8...2f21, 66e7d3...bea5, e16571...18a9) | 557,794 gas
Extracted LP position ID from receipt: 5333197
LP position opened successfully: position_id=5333197
Status: SUCCESS | Intent: LP_OPEN | Gas used: 557794 | Duration: 35468ms
Iteration completed successfully.
```

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | gateway | WARNING | Token resolution failures (BTC, STETH, RDNT, MAGIC, WOO) during market service init | `token_resolution_error token=BTC chain=arbitrum error_type=TokenNotFoundError detail=Symbol 'BTC' not found in registry for arbitrum` (same for STETH, RDNT, MAGIC, WOO) |
| 2 | gateway | INFO | No Alchemy API key -- free public RPC used (rate limit risk) | `No API key configured -- using free public RPC for arbitrum (rate limits may apply)` |
| 3 | gateway | INFO | UniswapV3ReceiptParser does not declare support for `bin_ids` (expected by LP_OPEN enricher) | `Parser UniswapV3ReceiptParser does not declare support for 'bin_ids' (expected by LP_OPEN)` |
| 4 | gateway | WARNING | Anvil port not freed within 5 seconds during shutdown | `Port 52409 not freed after 5.0s` |

**Notes:**
- Finding #1: The gateway's market service init attempts to resolve BTC, STETH, RDNT, MAGIC, and WOO on Arbitrum (likely a default price-watch list). These symbols don't exist under those exact names on Arbitrum (correct forms: WBTC, WSTETH). WARNING-level only, did not block execution.
- Finding #2: ALCHEMY_API_KEY is not set in .env. The gateway auto-falls-back to the publicnode RPC. Acceptable for local testing; an Alchemy key should be configured for production or CI use.
- Finding #3: Informational only. `bin_ids` is a TraderJoe V2 field that leaked into the LP_OPEN enricher's expected-field list. UniswapV3 correctly does not declare it. No impact on enrichment; position_id, tick_lower, tick_upper, liquidity all extracted correctly.
- Finding #4: Cosmetic. Anvil port release is slightly delayed on shutdown. No functional impact.

No zero prices, no failed API fetches, no ERROR log lines, no transaction reverts, no timeouts.

## Result

**PASS** - The uniswap_lp strategy successfully executed an LP_OPEN on an Anvil fork of Arbitrum,
minting Uniswap V3 position #5333197 (WETH/USDC/500, ±10% range around $1,927) via 3
on-chain transactions (557,794 total gas). No blocking errors encountered.

---

SUSPICIOUS_BEHAVIOUR_COUNT: 4
SUSPICIOUS_BEHAVIOUR_ERRORS: 0
