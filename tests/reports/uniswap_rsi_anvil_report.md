# E2E Strategy Test Report: uniswap_rsi (Anvil)

**Date:** 2026-02-27 16:57
**Result:** PASS
**Mode:** Anvil
**Duration:** ~32 seconds (including Anvil fork startup)

## Configuration

| Field | Value |
|-------|-------|
| Strategy | demo_uniswap_rsi |
| Chain | ethereum |
| Network | Anvil fork (publicnode free RPC, block 24549604) |
| Anvil Port | 53702 (auto-assigned by managed gateway) |
| trade_size_usd | $3.00 (well within $500 cap, no change needed) |
| rsi_period | 14 |
| rsi_oversold | 40 |
| rsi_overbought | 70 |
| base_token | WETH |
| quote_token | USDC |

**Config changes made:** None. `trade_size_usd` was already $3, well under the $500 cap. The strategy does not support `force_action`; however, the existing `rsi_oversold=40` threshold caused the RSI=39.79 market condition to naturally trigger a BUY signal.

## Execution

### Setup
- [x] Managed gateway auto-started on 127.0.0.1:50051 (network=anvil)
- [x] Anvil fork started on port 53702 (forked from https://ethereum-rpc.publicnode.com at block 24549604, chain_id=1)
- [x] Wallet funded: 100 ETH, 1 WETH (slot 3), 10000 USDC (slot 9) for 0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266

### Strategy Run
- [x] Strategy executed with `--network anvil --once`
- [x] WETH price: $1,930.73 (aggregated from on-chain Chainlink + CoinGecko free tier, 2 sources, confidence 1.00)
- [x] USDC price: $0.9999955
- [x] OHLCV data: 34 candles from Binance (28 finalized, 6 provisional)
- [x] RSI(14) = 39.79 -- below `rsi_oversold=40` threshold -- BUY signal triggered
- [x] Intent: SWAP $3.00 USDC -> WETH via Uniswap V3
- [x] Compiled: 3.0000 USDC -> 0.0015 WETH (min: 0.0015 WETH), 1.00% slippage, 2 txs, gas estimate 260,000
- [x] 2 transactions submitted and confirmed:
  - TX 1 (ERC-20 approve): `0xb08f7717eb3eddf5b41798b0b44b528357542990ceb231144de11d5eec2590b3` (block 24549607, gas 55,558)
  - TX 2 (swap):           `0x2a34fac4bbfc1dc02b6a36f1d1c727026f91328d3c9aa07b9a3b9c4e3946b4f9` (block 24549608, gas 124,527)
- [x] Total gas used: 180,085
- [x] Swap result: received 0.0016 WETH

### Key Log Output
```text
info  Anvil fork started: port=53702, block=24549604, chain_id=1
info  Funded 0xf39Fd6e5... with 100 ETH
info  Funded 0xf39Fd6e5... with WETH via known slot 3
info  Funded 0xf39Fd6e5... with USDC via known slot 9
info  Aggregated price for WETH/USD: 1930.728470605 (confidence: 1.00, sources: 2/2, outliers: 0)
info  ohlcv_fetched provider=binance instrument=WETH/USD candles=34 finalized=28 provisional=6
info  BUY SIGNAL: RSI=39.79 < 40 (oversold) | Buying $3.00 of WETH
info  Compiled SWAP: 3.0000 USDC -> 0.0015 WETH (min: 0.0015 WETH) | Slippage: 1.00% | Txs: 2 | Gas: 260,000
info  Simulation successful: 2 transaction(s), total gas: 253,937
info  Transaction confirmed: tx_hash=b08f77...90b3, block=24549607, gas_used=55558
info  Transaction confirmed: tx_hash=2a34fa...b4f9, block=24549608, gas_used=124527
info  EXECUTED: SWAP completed successfully | Txs: 2 (b08f77...90b3, 2a34fa...b4f9) | 180,085 gas
info  Parsed Uniswap V3 swap: 0.0000 token0 -> 0.0016 token1, slippage=N/A, tx=0x2a34...b4f9
Status: SUCCESS | Intent: SWAP | Gas used: 180085 | Duration: 26947ms
```

## On-Chain Transactions

| Step | TX Hash | Block | Gas Used | Status |
|------|---------|-------|----------|--------|
| APPROVE (USDC) | `0xb08f7717eb3eddf5b41798b0b44b528357542990ceb231144de11d5eec2590b3` | 24549607 | 55,558 | SUCCESS |
| SWAP (USDC->WETH) | `0x2a34fac4bbfc1dc02b6a36f1d1c727026f91328d3c9aa07b9a3b9c4e3946b4f9` | 24549608 | 124,527 | SUCCESS |

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | gateway | WARNING | Token resolution failure: BTC | `token_resolution_error token=BTC chain=ethereum ... Symbol 'BTC' not found in registry for ethereum. Did you mean 'WBTC'?` |
| 2 | gateway | WARNING | Token resolution failure: COMP | `token_resolution_error token=COMP chain=ethereum: Symbol 'COMP' not found in registry` |
| 3 | gateway | WARNING | Token resolution failure: MKR | `token_resolution_error token=MKR chain=ethereum: Symbol 'MKR' not found in registry` |
| 4 | gateway | WARNING | Token resolution failure: SNX | `token_resolution_error token=SNX chain=ethereum: Symbol 'SNX' not found in registry` |
| 5 | gateway | WARNING | Token resolution failure: LDO | `token_resolution_error token=LDO chain=ethereum: Symbol 'LDO' not found in registry` |
| 6 | gateway | WARNING | Token resolution failure: STETH | `token_resolution_error token=STETH chain=ethereum ... Did you mean 'WSTETH'?` |
| 7 | gateway | WARNING | Token resolution failure: CBETH | `token_resolution_error token=CBETH chain=ethereum: Symbol 'CBETH' not found in registry` |
| 8 | gateway | WARNING | Token resolution failure: RETH | `token_resolution_error token=RETH chain=ethereum: Symbol 'RETH' not found in registry` |
| 9 | gateway | WARNING | Token resolution failure: SOL | `token_resolution_error token=SOL chain=ethereum: Symbol 'SOL' not found in registry` |
| 10 | strategy | INFO | slippage=N/A in receipt parse | `Parsed Uniswap V3 swap: 0.0000 token0 -> 0.0016 token1, slippage=N/A, tx=0x2a34...b4f9` |
| 11 | gateway | INFO | Free public RPC (no Alchemy key) | `No API key configured -- using free public RPC for ethereum (rate limits may apply)` |
| 12 | gateway | INFO | Anvil port cleanup delay (cosmetic) | `Port 53702 not freed after 5.0s` |

### Findings Analysis

**Findings 1-9 (WARNING - 9 token resolution failures):** The gateway's Chainlink/price-source initialization batch-resolves a preset list of common tokens on startup. Nine symbols (BTC, COMP, MKR, SNX, LDO, STETH, CBETH, RETH, SOL) are absent from the ethereum static token registry. The strategy's own tokens (WETH, USDC) resolved correctly and execution was unaffected. The gateway suggestions ("Did you mean 'WBTC'?", "Did you mean 'WSTETH'?") confirm the wrapped versions exist in the registry but the unwrapped symbols do not. These 9 per-run WARNINGs are noisy. The static token registry for ethereum should be expanded to include BTC->WBTC aliasing, STETH->WSTETH aliasing, and entries for COMP, MKR, SNX, LDO, CBETH, RETH, SOL.

**Finding 10 (INFO - slippage=N/A):** The Uniswap V3 receipt parser logs `slippage=N/A` after parsing the swap event. This means the parser cannot compute actual vs expected slippage from the on-chain receipt. The expected amounts from the intent compiler do not appear to be flowing into the receipt parser. Not a blocking issue but reduces observability -- operators cannot confirm slippage bounds were respected post-execution.

**Finding 11 (INFO):** No Alchemy API key in `.env`. Graceful fallback to publicnode free RPC is working. Pricing and execution succeeded. Acceptable for local Anvil testing; may hit rate limits during sustained multi-strategy runs.

**Finding 12 (INFO):** Cosmetic Anvil port release timing. No impact on test correctness.

## Result

**PASS** - Strategy executed successfully end-to-end. RSI(14)=39.79 fell below `rsi_oversold=40`, triggering a BUY signal. The framework compiled a USDC->WETH swap on Uniswap V3, simulated and submitted 2 transactions (approve + swap), both confirmed on the Ethereum Anvil fork. The wallet received 0.0016 WETH for $3.00 USDC. 9 token resolution WARNINGs at startup are a known registry gap (unwrapped token symbols missing on ethereum) and do not block execution.

---

SUSPICIOUS_BEHAVIOUR_COUNT: 12
SUSPICIOUS_BEHAVIOUR_ERRORS: 0
