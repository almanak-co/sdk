# E2E Strategy Test Report: sushiswap_lp (Anvil)

**Date:** 2026-02-27 09:46
**Result:** PASS
**Mode:** Anvil
**Duration:** ~4 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | demo_sushiswap_lp |
| Chain | arbitrum |
| Network | Anvil fork (public RPC: arbitrum-one-rpc.publicnode.com) |
| Anvil Port | 57122 (managed, auto-started by CLI) |
| Fork Block | 436473047 |
| Pool | WETH/USDC/3000 |
| amount0 | 0.001 WETH |
| amount1 | 3 USDC |
| force_action | open |

**Config changes:** None. Amounts (0.001 WETH ~$2.02 at WETH=$2021.59, 3 USDC) are well under the $500 budget cap. `force_action` was already set to `"open"`.

Note: `ALCHEMY_API_KEY` was absent from `.env`; the framework automatically fell back to public Arbitrum RPC.

## Execution

### Setup

- [x] Managed gateway auto-started Anvil fork on port 57122 (arbitrum, block 436473047, chain_id=42161)
- [x] Wallet funded by managed gateway: 100 ETH, 1 WETH (slot 51), 10,000 USDC (slot 9)
- [x] Gateway started on 127.0.0.1:50052 (insecure/anvil mode)

### Strategy Run

- [x] Strategy executed: `uv run almanak strat run -d strategies/demo/sushiswap_lp --network anvil --once`
- [x] Intent returned: LP_OPEN (triggered by `force_action=open`)
- [x] WETH price fetched: $2,021.59 (Chainlink primary + CoinGecko free tier fallback, 2 sources, confidence 1.00)
- [x] USDC price fetched: $0.999958 (2 sources, confidence 1.00)
- [x] Tick range calculated: [-200700, -199740] (price range [1920.59 - 2122.76] USDC/WETH)
- [x] Compiled: 3 transactions (approve WETH + approve USDC + lp_mint), estimated 660,000 gas
- [x] Simulation successful: 3 transactions, 923,788 gas (tx 2 & 3 used compiler estimates)
- [x] All 3 transactions confirmed sequentially on-chain (Anvil fork)
- [x] LP position opened: **position_id = 33860**, liquidity = 1,981,164,434,984
- [x] Result enrichment: position_id, tick_lower, tick_upper, liquidity extracted from receipt

### Transactions (Anvil fork)

| # | Purpose | TX Hash | Gas Used | Block |
|---|---------|---------|----------|-------|
| 1 | WETH approval (Permit2) | `9f0028fc2988a15b77b0a080914d742cfd7a0bd215516df32906273dc73fb146` | 53,440 | 436473050 |
| 2 | Permit2 internal approval | `31c7f4c22b7114dc845a91625a30b06eb493416e59928dc150a56eb9dad1659d` | 55,437 | 436473051 |
| 3 | SushiSwap V3 mint (LP_OPEN) | `f346feaf2a5811615ec37369480e3551f27b7e65bc5d02fd645e28797890e789` | 506,685 | 436473052 |

**Total gas used:** 615,562

### Key Log Output

```text
info  Anvil fork started: port=57122, block=436473047, chain_id=42161
info  Funded 0xf39Fd6e5... with 100 ETH
info  Funded 0xf39Fd6e5... with WETH via known slot 51
info  Funded 0xf39Fd6e5... with USDC via known slot 9
info  Aggregated price for WETH/USD: 2021.5900000000001 (confidence: 1.00, sources: 2/2, outliers: 0)
info  Aggregated price for USDC/USD: 0.999958 (confidence: 1.00, sources: 2/2, outliers: 0)
info  Forced action: OPEN LP position
info  LP_OPEN: 0.0010 WETH + 3.0000 USDC, price range [1920.5912 - 2122.7587], ticks [-200700 - -199740]
info  Compiled LP_OPEN intent: WETH/USDC, range [1920.59-2122.76], 3 txs (approve + approve + lp_mint), 660000 gas
info  Simulation successful: 3 transaction(s), total gas: 923788
info  EXECUTED: LP_OPEN completed successfully
info     Txs: 3 (9f0028...b146, 31c7f4...659d, f346fe...e789) | 615,562 gas
info  Extracted LP position ID from receipt: 33860
info  Enriched LP_OPEN result with: position_id, tick_lower, tick_upper, liquidity (protocol=sushiswap_v3, chain=arbitrum)
info  SushiSwap V3 LP position opened: position_id=33860, liquidity=1981164434984
Status: SUCCESS | Intent: LP_OPEN | Gas used: 615562 | Duration: 35836ms
```

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | strategy | WARNING | Token resolution: BTC not found | `token_resolution_error token=BTC chain=arbitrum error_type=TokenNotFoundError ... Did you mean 'WBTC'?` |
| 2 | strategy | WARNING | Token resolution: STETH not found | `token_resolution_error token=STETH chain=arbitrum error_type=TokenNotFoundError ... Did you mean 'WSTETH'?` |
| 3 | strategy | WARNING | Token resolution: RDNT not found | `token_resolution_error token=RDNT chain=arbitrum error_type=TokenNotFoundError` |
| 4 | strategy | WARNING | Token resolution: MAGIC not found | `token_resolution_error token=MAGIC chain=arbitrum error_type=TokenNotFoundError` |
| 5 | strategy | WARNING | Token resolution: WOO not found | `token_resolution_error token=WOO chain=arbitrum error_type=TokenNotFoundError` |
| 6 | strategy | WARNING | Anvil port not freed (cosmetic) | `Port 57122 not freed after 5.0s` (cleanup race condition, process still terminated) |

**Analysis of findings:**

- **Findings 1-5 (WARNING):** During MarketService initialization the gateway pre-resolves a standard token watchlist. Five tokens (`BTC`, `STETH`, `RDNT`, `MAGIC`, `WOO`) use bare symbols not present in the Arbitrum registry. `BTC` should be `WBTC`, `STETH` should be `WSTETH`; `RDNT`, `MAGIC`, `WOO` may simply not be in the static registry. None of these are used by this strategy so execution was unaffected. This is a recurring data quality issue across multiple prior runs - the price source's token watchlist needs alias cleanup.
- **Finding 6 (WARNING):** Cosmetic cleanup race - Anvil process did not release its port within the 5s grace period. The process was still terminated successfully. Not a strategy bug.

No zero prices, no API fetch failures, no reverts, no on-chain errors. No ERROR-level log entries.

## Result

**PASS** - SushiSwap V3 LP_OPEN executed successfully on Anvil (Arbitrum fork). Position #33860 opened with 0.001 WETH + 3 USDC across ticks [-200700, -199740], 3 transactions confirmed, 615,562 total gas used. Price sources: 2/2 with confidence 1.00 (Chainlink + CoinGecko free tier).

---

SUSPICIOUS_BEHAVIOUR_COUNT: 6
SUSPICIOUS_BEHAVIOUR_ERRORS: 0
