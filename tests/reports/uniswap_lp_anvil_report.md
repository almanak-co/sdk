# E2E Strategy Test Report: uniswap_lp (Anvil)

**Date:** 2026-03-06 00:01
**Result:** PASS
**Mode:** Anvil
**Duration:** ~4 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | uniswap_lp |
| Chain | arbitrum |
| Network | Anvil fork (managed, auto-started by CLI on port 50633) |
| Fork Block | 438743117 |
| Pool | WETH/USDC/500 |
| Amount WETH | 0.001 |
| Amount USDC | 3 |
| Range Width | 20% |

**Config changes:** None. Trade sizes (0.001 WETH + 3 USDC, ~$5 total) are well within the $50 budget cap. No `force_action` needed - strategy opened an LP position immediately on a fresh state run.

## Execution

### Setup
- Previous Anvil/Gateway processes killed
- `--network anvil --once` CLI flag auto-started a managed Anvil fork on port 50633 (Arbitrum, publicnode.com free RPC) and gateway on port 50053
- Wallet `0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266` auto-funded via config `anvil_funding`:
  - ETH: 100 ETH
  - WETH: 1 WETH (via storage slot 51)
  - USDC: 10,000 USDC (via storage slot 9)

### Strategy Run
- Strategy executed with `--network anvil --once`
- Fresh state - no existing LP position
- Price fetched: WETH = $2,082.08 (confidence: 1.00, 2/2 sources), USDC = $1.00 (confidence: 0.90, 1/2 sources)
- Range computed: [$1,873.87 - $2,290.29] (20% range around current ETH price)
- 3 transactions compiled and executed sequentially: approve WETH, approve USDC, lp_mint
- LP position opened: **position_id = 5347822**
- ResultEnricher extracted: position_id, tick_lower, tick_upper, liquidity from IncreaseLiquidity event

## Transactions (Anvil local fork - not on public chain)

| # | Type | TX Hash | Block | Gas Used | Status |
|---|------|---------|-------|----------|--------|
| 1 | APPROVE (WETH) | `0x5b465b7d4e4ade4eb85baee03755b1f69c24feb8a8a20eade110d9fab9097166` | 438743120 | 53,440 | SUCCESS |
| 2 | APPROVE (USDC) | `0x80ce71556b0912d344ff9144bae0445a4f9c25cad67df10f253abf2db79f2d01` | 438743121 | 55,437 | SUCCESS |
| 3 | LP_MINT | `0x5397ab5c2765ad679b492069e00c038f2ed20615f7c131216596ea442036edf0` | 438743122 | 417,799 | SUCCESS |

**Total gas used: 526,676 | LP Position ID: 5347822 | Execution time: 39,076ms**

### Key Log Output

```text
UniswapLPStrategy initialized: pool=WETH/USDC/500, range_width=20.0%, amounts=0.001 WETH + 3 USDC
Aggregated price for WETH/USD: 2082.0830427749997 (confidence: 1.00, sources: 2/2, outliers: 0)
Aggregated price for USDC/USD: 1.00 (confidence: 0.90, sources: 1/2, outliers: 0)
No position found - opening new LP position
LP_OPEN: 0.0010 WETH + 3.0000 USDC, range [$1,873.87 - $2,290.29]
Compiled LP_OPEN intent: WETH/USDC, range [1873.87-2290.29], 3 txs (approve + approve + lp_mint), 660000 gas
Simulation successful: 3 transaction(s), total gas: 923788
EXECUTED: LP_OPEN completed successfully
   Txs: 3 (5b465b...7166, 80ce71...2d01, 5397ab...edf0) | 526,676 gas
Extracted LP position ID from receipt: 5347822
Enriched LP_OPEN result with: position_id, tick_lower, tick_upper, liquidity (protocol=uniswap_v3, chain=arbitrum)
LP position opened successfully: position_id=5347822
Status: SUCCESS | Intent: LP_OPEN | Gas used: 526676 | Duration: 39076ms
Iteration completed successfully.
```

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | strategy | WARNING | CoinGecko rate limited for USDC/USD (3 times, exponential backoff) | `Rate limited by CoinGecko for USDC/USD, backoff: 1.00s` / `2.00s` / `4.00s` |
| 2 | strategy | INFO | USDC/USD price resolved with reduced confidence (1/2 sources due to rate limiting) | `Aggregated price for USDC/USD: 1.00 (confidence: 0.90, sources: 1/2, outliers: 0)` |
| 3 | strategy | INFO | Circular import error for unrelated incubating strategy at startup | `Failed to import strategy strategies.incubating.pendle_pt_swap_arbitrum.strategy (retry failed): cannot import name 'IntentStrategy'` |
| 4 | strategy | INFO | Parser capability gap: bin_ids not declared by UniswapV3ReceiptParser | `Parser UniswapV3ReceiptParser does not declare support for 'bin_ids' (expected by LP_OPEN)` |
| 5 | gateway | INFO | Port conflict on 50051 (manually started gateway rejected; managed gateway used 50053) | `OSError: [Errno 48] Address already in use` |

**Notes:**
- Finding 1-2: CoinGecko free tier was rate-limited 3x while fetching USDC price. The system handled this gracefully with exponential backoff and fell back to on-chain Chainlink pricing (confidence 0.90 vs 1.00 with both sources). No zero prices. Non-blocking but worth noting for production environments without an API key.
- Finding 3: `pendle_pt_swap_arbitrum` incubating strategy has a circular import bug. Logged as a startup warning only. Unrelated to this test.
- Finding 4: `UniswapV3ReceiptParser` does not declare support for `bin_ids` (a TraderJoe V2 concept). ResultEnricher logged this as INFO but extracted all relevant LP fields correctly (position_id, tick_lower, tick_upper, liquidity). Benign.
- Finding 5: The manually pre-started gateway on port 50051 conflicted with the CLI's auto-managed gateway. The CLI gateway ran correctly on port 50053. No test impact.

No zero prices, no failed API fetches, no ERROR-level log lines, no transaction reverts, no token resolution failures detected.

## Result

**PASS** - The uniswap_lp strategy successfully executed LP_OPEN on an Anvil fork of Arbitrum, minting Uniswap V3 position #5347822 (WETH/USDC 0.05% fee pool, 20% price range around WETH = $2,082) via 3 sequential on-chain transactions totalling 526,676 gas. All intents compiled, simulated, submitted, and confirmed without errors. Position ID was correctly extracted from the IncreaseLiquidity event and persisted to state.

---

SUSPICIOUS_BEHAVIOUR_COUNT: 5
SUSPICIOUS_BEHAVIOUR_ERRORS: 0
