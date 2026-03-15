# E2E Strategy Test Report: uniswap_lp (Anvil)

**Date:** 2026-03-16 (run executed 2026-03-15T18:50-18:51 UTC)
**Result:** PASS
**Mode:** Anvil
**Duration:** ~3 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | demo_uniswap_lp |
| Chain | arbitrum |
| Network | Anvil fork (managed, auto-started by CLI on port 59277) |
| Fork Block | 442138481 |
| Pool | WETH/USDC/500 |
| Amount WETH | 0.001 |
| Amount USDC | 3 |
| Range Width | 20% |

**Config changes:** Added `"force_action": "open"` temporarily to force an immediate LP_OPEN intent. Restored to original after the test. Trade sizes (0.001 WETH + 3 USDC, ~$5 total) are well within the $1000 budget cap.

## Execution

### Setup
- Previous Anvil/Gateway processes killed
- `--network anvil --once` CLI flag auto-started a managed Anvil fork on port 59277 (Arbitrum) and gateway on port 50052
- Wallet `0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266` auto-funded via config `anvil_funding`:
  - ETH: 100 ETH
  - WETH: 1 WETH (via storage slot 51)
  - USDC: 10,000 USDC (via storage slot 9)

### Strategy Run
- Strategy executed with `--network anvil --once`
- Fresh state - no existing LP position
- Price fetched: WETH = $2,100.23 (confidence: 1.00, 4/4 sources), USDC = $1.00 (confidence: 1.00, 4/4 sources)
- Range computed: [$1,890.26 - $2,310.32] (20% range around current ETH price)
- `force_action = "open"` triggered immediate LP_OPEN
- 3 transactions compiled and executed sequentially: approve WETH, approve USDC, lp_mint
- LP position opened: **position_id = 5366280**
- ResultEnricher extracted: position_id, tick_lower, tick_upper, liquidity from IncreaseLiquidity event
- Position ID saved to persistent state

## Transactions (Anvil local fork)

| # | Type | TX Hash | Block | Gas Used | Status |
|---|------|---------|-------|----------|--------|
| 1 | APPROVE (WETH) | `8180f3d81c63121a77e5c661aaa040be537023055796ac06dde7fdafd0dca9a8` | 442138484 | 53,440 | SUCCESS |
| 2 | APPROVE (USDC) | `93d9f14a6b8f268f343189f65cc96fd0bb71f2992baa2c914c42f410c9823473` | 442138485 | 55,437 | SUCCESS |
| 3 | LP_MINT | `919f0145d9d72e9b7bf3a761a4e19fd8557e09955c52a89db5b53bcac9becc8f` | 442138486 | 414,733 | SUCCESS |

**Total gas used: 523,610 | LP Position ID: 5366280 | Execution time: 28,097ms**

### Key Log Output

```text
UniswapLPStrategy initialized: pool=WETH/USDC/500, range_width=20.0%, amounts=0.001 WETH + 3 USDC
Aggregated price for WETH/USD: 2100.225225 (confidence: 1.00, sources: 4/4, outliers: 0)
Aggregated price for USDC/USD: 0.999969 (confidence: 1.00, sources: 4/4, outliers: 0)
Forced action: OPEN LP position
LP_OPEN: 0.0010 WETH + 3.0000 USDC, range [$1,890.26 - $2,310.32]
Compiled LP_OPEN intent: WETH/USDC, range [1890.26-2310.32], 3 txs (approve + approve + lp_mint), 660000 gas
Simulation successful: 3 transaction(s), total gas: 923788
EXECUTED: LP_OPEN completed successfully
   Txs: 3 (8180f3...a9a8, 93d9f1...3473, 919f01...cc8f) | 523,610 gas
Extracted LP position ID from receipt: 5366280
Enriched LP_OPEN result with: position_id, tick_lower, tick_upper, liquidity (protocol=uniswap_v3, chain=arbitrum)
LP position opened successfully: position_id=5366280
Status: SUCCESS | Intent: LP_OPEN | Gas used: 523610 | Duration: 28097ms
Iteration completed successfully.
```

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | gateway | INFO | CoinGecko fallback mode (no API key) | `No CoinGecko API key -- using on-chain pricing (Chainlink oracles) with free CoinGecko as fallback.` |
| 2 | strategy | INFO | Parser capability gap: bin_ids not declared by UniswapV3ReceiptParser | `Parser UniswapV3ReceiptParser does not declare support for 'bin_ids' (expected by LP_OPEN)` |

**Notes:**
- Finding 1: Expected in local dev without a CoinGecko Pro key. All 4 sources returned prices with confidence 1.00 (Chainlink + Binance + DexScreener + free CoinGecko). No degraded pricing this run (unlike previous iter which hit rate limits).
- Finding 2: `UniswapV3ReceiptParser` does not declare support for `bin_ids` (a TraderJoe V2 concept). ResultEnricher logs this as INFO but extracted all relevant LP fields correctly (position_id, tick_lower, tick_upper, liquidity). Benign schema mismatch.

No zero prices, no failed API fetches, no WARNING or ERROR log lines, no transaction reverts, no token resolution failures detected.

## Result

**PASS** - The `uniswap_lp` strategy successfully executed LP_OPEN on an Anvil fork of Arbitrum, minting Uniswap V3 position #5366280 (WETH/USDC 0.05% fee pool, 20% price range around WETH = $2,100) via 3 sequential on-chain transactions totalling 523,610 gas. All intents compiled, simulated, submitted, and confirmed without errors. Position ID was correctly extracted from the IncreaseLiquidity event and persisted to state.

---

SUSPICIOUS_BEHAVIOUR_COUNT: 2
SUSPICIOUS_BEHAVIOUR_ERRORS: 0
