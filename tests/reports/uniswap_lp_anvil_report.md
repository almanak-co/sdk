# E2E Strategy Test Report: uniswap_lp (Anvil)

**Date:** 2026-03-03 19:35
**Result:** PASS
**Mode:** Anvil
**Duration:** ~4 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | demo_uniswap_lp |
| Chain | arbitrum |
| Network | Anvil fork (publicnode.com, port auto-assigned by managed gateway) |
| Pool | WETH/USDC/500 |
| Amount0 | 0.001 WETH |
| Amount1 | 3 USDC |
| Range Width | 20% (±10% from current price) |

## Config Changes Made

- Added `"force_action": "open"` temporarily to trigger an immediate LP_OPEN on first run.
- **Restored** after test (field removed from config.json).
- Trade size (0.001 WETH + 3 USDC) is well within the $500 budget cap.

## Execution

### Setup
- [x] Previous Anvil/Gateway processes killed
- [x] Managed gateway auto-started with fresh Anvil fork on Arbitrum (publicnode.com)
- [x] Wallet 0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266 auto-funded via config anvil_funding:
  - ETH: 100 ETH
  - WETH: 1 WETH (via storage slot 51)
  - USDC: 10,000 USDC (via storage slot 9)

### Strategy Run
- [x] Strategy executed with `--network anvil --once`
- [x] Intent: LP_OPEN on WETH/USDC/500
- [x] Price fetched: WETH = $1,971.35, USDC = $0.9999 (Chainlink on-chain, confidence 1.00, 2/2 sources)
- [x] Range calculated: [$1,774.28 - $2,168.56] (±10%)
- [x] 3 transactions submitted sequentially and confirmed
- [x] LP position opened: position_id = **5341845**
- [x] ResultEnricher extracted: position_id, tick_lower, tick_upper, liquidity

### Transactions (Anvil local fork - not on public chain)

| # | Type | TX Hash | Block | Gas Used | Status |
|---|------|---------|-------|----------|--------|
| 1 | APPROVE (WETH) | `37a373f99f0f2970d1979bfd3fb640cb18c1b79a4fd6c39d5be592fca9b86204` | 437899187 | 53,440 | SUCCESS |
| 2 | APPROVE (USDC) | `e57e0c0cfe8e97f2cffca1f84ca58eeee9e3be351f93af6af53b56b6af0ade36` | 437899188 | 55,437 | SUCCESS |
| 3 | LP_MINT | `4d1da7f8338698fe18a77f79fd9668dada2731c90b77af11e108256b9eef55be` | 437899189 | 414,829 | SUCCESS |

**Total gas used: 523,706 | LP Position ID: 5341845**

### Key Log Output

```text
2026-03-03T12:34:27.021114Z [info] Aggregated price for WETH/USD: 1971.3457285650002 (confidence: 1.00, sources: 2/2)
2026-03-03T12:34:27.347227Z [info] Aggregated price for USDC/USD: 0.999962 (confidence: 1.00, sources: 2/2)
2026-03-03T12:34:27.349025Z [info] Forced action: OPEN LP position
2026-03-03T12:34:27.350646Z [info] LP_OPEN: 0.0010 WETH + 3.0000 USDC, range [$1,774.28 - $2,168.56]
2026-03-03T12:34:28.543257Z [info] Compiled LP_OPEN intent: WETH/USDC, range [1774.28-2168.56], 3 txs (approve + approve + lp_mint), 660000 gas
2026-03-03T12:34:32.911010Z [info] Simulation successful: 3 transaction(s), total gas: 923788
2026-03-03T12:35:00.837155Z [info] EXECUTED: LP_OPEN completed successfully
2026-03-03T12:35:00.837573Z [info]    Txs: 3 (37a373...6204, e57e0c...de36, 4d1da7...55be) | 523,706 gas
2026-03-03T12:35:00.840192Z [info] Extracted LP position ID from receipt: 5341845
2026-03-03T12:35:00.840597Z [info] Enriched LP_OPEN result with: position_id, tick_lower, tick_upper, liquidity (protocol=uniswap_v3, chain=arbitrum)
2026-03-03T12:35:00.853011Z [info] LP position opened successfully: position_id=5341845
Status: SUCCESS | Intent: LP_OPEN | Gas used: 523706 | Duration: 36150ms
Iteration completed successfully.
```

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | strategy | WARNING | Insecure mode (expected for Anvil dev) | `INSECURE MODE: Auth interceptor disabled - no auth_token configured. This is acceptable for local development on 'anvil'.` |
| 2 | strategy | INFO | No CoinGecko API key - on-chain Chainlink pricing active | `No CoinGecko API key -- using on-chain pricing (Chainlink oracles) with free CoinGecko as fallback.` |
| 3 | strategy | INFO | Compiler using placeholder prices on initial compile | `IntentCompiler initialized for chain=arbitrum, ... using_placeholders=True` |
| 4 | strategy | INFO | Parser capability mismatch (bin_ids field not declared) | `Parser UniswapV3ReceiptParser does not declare support for 'bin_ids' (expected by LP_OPEN)` |
| 5 | strategy | WARNING | Anvil port not freed after 5s (cosmetic, fork shutdown race) | `Port 58935 not freed after 5.0s [almanak.framework.anvil.fork_manager]` |

**Notes:**
- Finding 1: Expected and correct for Anvil dev mode. Not a bug.
- Finding 2: Expected when `COINGECKO_API_KEY` is not configured. Both prices were fetched via
  Chainlink on-chain oracles (2/2 sources, confidence 1.00). Acceptable for Anvil testing.
- Finding 3: The compiler initializes with placeholders before gateway price data is available.
  Second compilation (when run) used real prices (`using_placeholders=False`). Normal startup sequence.
- Finding 4: `UniswapV3ReceiptParser` does not declare support for `bin_ids` (a TraderJoe V2 concept).
  Benign parser metadata gap - all relevant fields (position_id, tick_lower, tick_upper, liquidity)
  were enriched correctly. Noisy but harmless.
- Finding 5: Cosmetic port-cleanup race condition on managed Anvil fork shutdown. Non-blocking.

No zero prices, no failed API fetches, no ERROR-level log lines, no transaction reverts,
no timeouts, no token resolution failures detected.

## Result

**PASS** - The uniswap_lp strategy successfully executed an LP_OPEN on an Anvil fork of Arbitrum,
minting Uniswap V3 position #5341845 (WETH/USDC 0.05% pool, 20% range around $1,971 ETH price)
via 3 on-chain transactions totaling 523,706 gas. No blocking errors encountered.

---

SUSPICIOUS_BEHAVIOUR_COUNT: 5
SUSPICIOUS_BEHAVIOUR_ERRORS: 0
