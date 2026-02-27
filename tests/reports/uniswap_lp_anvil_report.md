# E2E Strategy Test Report: uniswap_lp (Anvil)

**Date:** 2026-02-27 17:00
**Result:** PASS
**Mode:** Anvil
**Duration:** ~7 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | demo_uniswap_lp |
| Chain | arbitrum |
| Network | Anvil fork (public RPC: arbitrum.meowrpc.com) |
| Anvil Port | 8545 |
| Pool | WETH/USDC/500 |
| amount0 | 0.001 WETH |
| amount1 | 3 USDC |
| range_width_pct | 20% |
| Budget cap | $500 (no change needed; 0.001 WETH + 3 USDC well within cap) |

## Config Changes Made

None. The config amounts (0.001 WETH + 3 USDC) are well under the $500 budget cap and `force_action`
was not set (strategy proceeded directly to LP_OPEN on fresh start, no position found).

Note: `ALCHEMY_API_KEY` is empty in `.env`. Anvil was forked using the public RPC
`https://arbitrum.meowrpc.com`. Wallet was manually funded: 100 ETH, 1 WETH (wrapped),
10,000 USDC (storage slot 9).

## Execution

### Setup
- [x] Anvil started on port 8545 (forked Arbitrum, chain ID 42161)
- [x] Gateway connected on port 50051 (pre-existing managed process)
- [x] Wallet (0xf39Fd6e5...) funded: 100 ETH, 1 WETH, 10,000 USDC

### Strategy Run
- [x] Fresh start (no existing state found)
- [x] Strategy detected no open position and triggered LP_OPEN immediately
- [x] LP range calculated: [$1,811.81 - $2,214.43] (±10% of current ETH price)
- [x] LP_OPEN intent compiled: 3 transactions (approve + approve + lp_mint), 660,000 gas estimated
- [x] Execution successful: gas_used=523,594, tx_count=3
- [x] LP position ID extracted from receipt: **5332419**
- [x] Result enriched: `position_id`, `tick_lower`, `tick_upper`, `liquidity`

### Transaction Hashes (Anvil local fork -- not on public chain)

| Intent | Gas Used | Status |
|--------|----------|--------|
| WETH Approve | ~53,440 | SUCCESS |
| USDC Approve | ~55,437 | SUCCESS |
| LP Mint | ~414,717 | SUCCESS |

**Total gas used: 523,594 | LP Position ID: 5332419**

### Key Log Output
```text
[10:00:44] No position found - opening new LP position
[10:00:44] LP_OPEN: 0.0010 WETH + 3.0000 USDC, range [$1,811.81 - $2,214.43]
[10:00:44] Compiled LP_OPEN intent: WETH/USDC, range [1811.81-2214.43], 3 txs (approve + approve + lp_mint), 660000 gas
[10:00:54] Execution successful for demo_uniswap_lp: gas_used=523594, tx_count=3
[10:00:54] Extracted LP position ID from receipt: 5332419
[10:00:54] Enriched LP_OPEN result with: position_id, tick_lower, tick_upper, liquidity (protocol=uniswap_v3, chain=arbitrum)
[10:00:54] LP position opened successfully: position_id=5332419

Status: SUCCESS | Intent: LP_OPEN | Gas used: 523594 | Duration: 12871ms
Iteration completed successfully.
```

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | strategy | INFO | No Alchemy API key / public RPC fallback | `No API key configured -- using free public RPC for arbitrum (rate limits may apply)` |
| 2 | strategy | INFO | Parser capability gap (bin_ids) | `Parser UniswapV3ReceiptParser does not declare support for 'bin_ids' (expected by LP_OPEN)` |

**Notes:**
- Finding #1: Expected in this environment. `ALCHEMY_API_KEY` is not configured in `.env`.
  The gateway automatically falls back to the public Arbitrum RPC. This is a production observation:
  an Alchemy key should be set for stable, rate-limit-free operation.
- Finding #2: Informational only. `bin_ids` is a TraderJoe V2 field. UniswapV3 does not use bin IDs
  and correctly does not declare support for this field. The log line may cause confusion but is
  harmless. No impact on result enrichment (position_id, tick_lower, tick_upper, liquidity all
  extracted successfully).

No zero prices, no failed API fetches, no ERROR log lines, no token resolution failures, no reverts,
no timeouts.

## Result

**PASS** - The uniswap_lp strategy successfully executed an LP_OPEN on an Anvil fork of Arbitrum,
minting Uniswap V3 position #5332419 (WETH/USDC/500, ±10% range around ~$2,013) via 3
on-chain transactions (523,594 total gas). No blocking errors encountered.

---

SUSPICIOUS_BEHAVIOUR_COUNT: 2
SUSPICIOUS_BEHAVIOUR_ERRORS: 0
