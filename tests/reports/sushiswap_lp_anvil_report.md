# E2E Strategy Test Report: sushiswap_lp (Anvil)

**Date:** 2026-02-23 04:12
**Result:** PASS
**Mode:** Anvil
**Duration:** ~4 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | demo_sushiswap_lp |
| Chain | arbitrum |
| Network | Anvil fork |
| Anvil Port | 64404 (auto-assigned by managed gateway) |
| Pool | WETH/USDC/3000 |
| amount0 | 0.001 WETH |
| amount1 | 3 USDC |
| force_action | open |

**Config changes:** None. Amounts (0.001 WETH ~$1.94, 3 USDC) are well under the $100 budget cap. `force_action` was already set to `"open"`.

## Execution

### Setup
- [x] Anvil fork started (managed by CLI auto-start on port 64404, chain ID 42161)
- [x] Gateway started on port 50052 (managed mode)
- [x] Wallet funded by managed gateway: 100 ETH, 1 WETH (slot 51), 10,000 USDC (slot 9)

### Strategy Run
- [x] Strategy executed with `--network anvil --once`
- [x] Price fetched: WETH = $1,941.29, USDC = $0.9999
- [x] Price range calculated: [1844.39 - 2038.54] USDC/WETH, ticks [-201120, -200100]
- [x] LP_OPEN intent triggered by `force_action = open`
- [x] Compiled: 3 transactions, estimated 660,000 gas
- [x] All 3 transactions confirmed on-chain (Anvil fork)
- [x] LP position opened: **position_id = 32979**, liquidity = 2,223,696,811,742
- [x] Result enrichment: position_id, tick_lower, tick_upper, liquidity extracted from receipt

### Transaction Hashes (Anvil fork)

| # | Purpose | TX Hash | Gas Used | Block |
|---|---------|---------|----------|-------|
| 1 | WETH approval (Permit2) | `3563bd4af6fbdd73e676208c51e9e59234da96088a75bab2eb1dba9d80e9b433` | 53,440 | 434905487 |
| 2 | Permit2 internal approval | `b20c409b8decc6a2efab80bf21c8ec9fe5959a920de056fd814516a896b30217` | 55,437 | 434905488 |
| 3 | SushiSwap V3 mint (LP_OPEN) | `623a78671a429ae5b87e9c2be15b443ab277b54322e95d3e4b03e77bf03428a3` | 430,239 | 434905489 |

**Total gas used:** 539,116

### Key Log Output

```text
Aggregated price for WETH/USD: 1941.29 (confidence: 1.00, sources: 1/1, outliers: 0)
Aggregated price for USDC/USD: 0.999909 (confidence: 1.00, sources: 1/1, outliers: 0)
Forced action: OPEN LP position
LP_OPEN: 0.0010 WETH + 3.0000 USDC, price range [1844.3933 - 2038.5400], ticks [-201120 - -200100]
Compiled LP_OPEN intent: WETH/USDC, range [1844.39-2038.54], 3 txs, 660000 gas
EXECUTED: LP_OPEN completed successfully
   Txs: 3 (3563bd...b433, b20c40...0217, 623a78...28a3) | 539,116 gas
Extracted LP position ID from receipt: 32979
Enriched LP_OPEN result with: position_id, tick_lower, tick_upper, liquidity (protocol=sushiswap_v3, chain=arbitrum)
SushiSwap V3 LP position opened: position_id=32979, liquidity=2223696811742
Status: SUCCESS | Intent: LP_OPEN | Gas used: 539116 | Duration: 25017ms
```

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | strategy | INFO | No CoinGecko API key | `COINGECKO_API_KEY not configured - CoinGecko will use free tier API (30 requests/minute limit)` |
| 2 | strategy | WARNING | Gas estimate below compiler floor | `Gas estimate tx[0]: raw=53,788 buffered=80,682 (x1.5) < compiler=120,000, using compiler limit` |
| 3 | strategy | WARNING | Gas estimate below compiler floor | `Gas estimate tx[1]: raw=55,819 buffered=83,728 (x1.5) < compiler=120,000, using compiler limit` |
| 4 | strategy | WARNING | Amount chaining warning | `Amount chaining: no output amount extracted from step 1; subsequent amount='all' steps will fail` |

**Analysis:**
- **Finding 1 (INFO):** No CoinGecko API key is normal for local development. Prices were fetched successfully from the free tier. Not a real issue.
- **Findings 2 & 3 (WARNING):** Gas simulator returned lower estimates than the compiler floor for the two approval transactions. The orchestrator correctly deferred to the compiler limit (120,000 gas each). Approvals actually used only 53,440 and 55,437 gas respectively, so the compiler limit was conservative but safe. Not blocking.
- **Finding 4 (WARNING):** The `Amount chaining` warning fires because this LP_OPEN flow does not use an `amount='all'` chained step -- LP_OPEN compiles to discrete amounts (0.001 WETH + 3 USDC), not a chained output. The warning is spurious for LP_OPEN intents and does not affect correctness. Worth investigating whether the warning should be suppressed for non-swap flows.

No zero prices, no API fetch failures, no reverts, no token resolution errors, no timeouts.

## Result

**PASS** - SushiSwap V3 LP_OPEN executed successfully on Anvil (Arbitrum fork). Position #32979 opened with 0.001 WETH + 3 USDC across ticks [-201120, -200100], 3 transactions confirmed, 539,116 total gas used.

SUSPICIOUS_BEHAVIOUR_COUNT: 4
SUSPICIOUS_BEHAVIOUR_ERRORS: 0
