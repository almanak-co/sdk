# E2E Strategy Test Report: pancakeswap_lp (Anvil)

**Date:** 2026-03-15 18:27
**Result:** PASS
**Mode:** Anvil
**Duration:** ~2 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | pancakeswap_lp |
| Chain | arbitrum |
| Network | Anvil fork |
| Anvil Port | 55195 (managed, auto-started by CLI) |
| Pool | WETH/USDC/500 |
| Amount0 | 0.001 WETH |
| Amount1 | 3 USDC |
| Range Width | 20% |

Config changes made: None. Amounts (0.001 WETH + 3 USDC ~= $5) are well within the $1000 budget cap.

## Execution

### Setup
- [x] Anvil fork started (managed by CLI on port 55195, Arbitrum fork block 442132848)
- [x] Gateway started on port 50052 (managed)
- [x] Wallet funded: 100 ETH, 1 WETH, 10,000 USDC via anvil_funding config

### Strategy Run
- [x] Strategy executed with `--network anvil --once`
- [x] Intent: LP_OPEN (WETH/USDC/500, range [1897.99 - 2319.77] at WETH price $2108.88)
- [x] 3 transactions submitted and confirmed (approve WETH, approve USDC, lp_mint)
- [x] LP position opened: position_id=339767
- [x] Result enriched with position_id, tick_lower, tick_upper, liquidity

### Transaction Hashes (Anvil fork - not real chain)

| TX | Hash | Gas Used | Status |
|----|------|----------|--------|
| 1 (WETH approve) | `9621c25f262199bf30049dbd14d6c67535d190d5b85236be21387f7a26fdbba3` | 53,440 | SUCCESS |
| 2 (USDC approve) | `069c996283ab06c413bb2c2121cf2986d4dbca1f029aeab855f39e0758172cf3` | 55,437 | SUCCESS |
| 3 (LP mint) | `3af21e4e73ec9b549ec5c60c4a23896e3cc6a000749d31dae87d77b5a5e903f1` | 468,269 | SUCCESS |

**Total gas used:** 577,146

### Key Log Output
```text
Opening PancakeSwap V3 LP: 0.001 WETH + 3 USDC, range [1897.99 - 2319.77]
Compiled LP_OPEN intent: WETH/USDC, range [1897.99-2319.77], 3 txs (approve + approve + lp_mint), 660000 gas
Simulation successful: 3 transaction(s), total gas: 923788
EXECUTED: LP_OPEN completed successfully
  Txs: 3 (9621c2...bba3, 069c99...2cf3, 3af21e...03f1) | 577,146 gas
Extracted PancakeSwap V3 LP position ID: 339767
Enriched LP_OPEN result with: position_id, tick_lower, tick_upper, liquidity
PancakeSwap V3 LP opened: position_id=339767
Status: SUCCESS | Intent: LP_OPEN | Gas used: 577146 | Duration: 28152ms
Iteration completed successfully.
```

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | strategy | INFO | INSECURE MODE warning (expected for Anvil) | `INSECURE MODE: Auth interceptor disabled - no auth_token configured. This is acceptable for local development on 'anvil'.` |
| 2 | strategy | WARNING | Missing parser support for `bin_ids` field | `Parser PancakeSwapV3ReceiptParser does not declare support for 'bin_ids' (expected by LP_OPEN)` |
| 3 | strategy | ERROR | Unclosed aiohttp client session on shutdown | `Unclosed client session: <aiohttp.client.ClientSession object at 0x117adcdd0>` |
| 4 | strategy | INFO | No CoinGecko API key - fallback mode | `No CoinGecko API key -- using on-chain pricing (Chainlink oracles) with free CoinGecko as fallback.` |

**Notes:**
- Finding #1 is expected and benign for Anvil mode.
- Finding #2 (`bin_ids`) is worth noting: `PancakeSwapV3ReceiptParser` does not declare support for the `bin_ids` extraction field. This is a TraderJoe-style field not applicable to PancakeSwap V3 (concentrated liquidity, not bin-based), so it is functionally harmless. However, the `ResultEnricher` still logs it at INFO level, suggesting the LP_OPEN result spec may include TraderJoe fields that are not filtered out for PancakeSwap.
- Finding #3 (unclosed aiohttp session) is a minor resource leak on shutdown. It does not affect correctness but may indicate an aiohttp session is not being properly closed in the gateway teardown path.
- Finding #4 is expected/informational — no CoinGecko API key is set, but 4-source pricing (Chainlink + Binance + DexScreener + CoinGecko free) returned confident prices (confidence: 1.00).

## Result

**PASS** - PancakeSwap V3 LP_OPEN executed successfully on Arbitrum Anvil fork: 3 transactions confirmed, LP position #339767 opened with 0.001 WETH + 3 USDC in range [1897.99 - 2319.77].

---

SUSPICIOUS_BEHAVIOUR_COUNT: 4
SUSPICIOUS_BEHAVIOUR_ERRORS: 1
