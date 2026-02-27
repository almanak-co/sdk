# E2E Strategy Test Report: uniswap_rsi (Anvil)

**Date:** 2026-02-27 10:05
**Result:** PASS
**Mode:** Anvil
**Duration:** ~13 seconds (including Anvil fork startup)

## Configuration

| Field | Value |
|-------|-------|
| Strategy | uniswap_rsi |
| Chain | ethereum |
| Network | Anvil fork (publicnode.com, block 24547550) |
| Anvil Port | 60790 (auto-assigned by managed gateway) |
| trade_size_usd | $3 (within $500 cap, no change needed) |
| rsi_period | 14 |
| rsi_oversold | 40 |
| rsi_overbought | 70 |
| base_token | WETH |
| quote_token | USDC |

**Config changes made:** None. `trade_size_usd` was already $3, well under the $500 cap. The strategy does not support `force_action`.

## Execution

### Setup
- [x] Managed gateway auto-started on 127.0.0.1:50052 (network=anvil)
- [x] Anvil fork started on port 60790 (forked from https://ethereum-rpc.publicnode.com at block 24547550, chain_id=1)
- [x] Wallet funded: 100 ETH, 1 WETH (slot 3), 10000 USDC (slot 9) for 0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266

### Strategy Run
- [x] Strategy executed with `--network anvil --once`
- [x] WETH price: $2,011.455 (aggregated from on-chain Chainlink + CoinGecko free tier, 2 sources, confidence 1.00)
- [x] USDC price: $0.9999945
- [x] OHLCV data: 34 candles from Binance (28 finalized, 6 provisional)
- [x] RSI(14) = 52.92 -- in neutral zone [40-70]
- [x] Intent: HOLD (no on-chain transaction)

### Key Log Output

```text
Anvil fork started: port=60790, block=24547550, chain_id=1
Funded 0xf39Fd6e5... with 100 ETH
Funded 0xf39Fd6e5... with WETH via known slot 3
Funded 0xf39Fd6e5... with USDC via known slot 9
AggregatedPrice for WETH/USD: 2011.455 (confidence: 1.00, sources: 2/2, outliers: 0)
ohlcv_fetched provider=binance instrument=WETH/USD candles=34 finalized=28 provisional=6
demo_uniswap_rsi HOLD: RSI=52.92 in neutral zone [40-70] (hold #1)
Status: HOLD | Intent: HOLD | Duration: 2639ms
```

## On-Chain Transactions

None. RSI=52.92 is in the neutral zone, so the strategy correctly returned HOLD. No swap was executed.

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | strategy | WARNING | Token resolution failures x9 (BTC, COMP, MKR, SNX, LDO, STETH, CBETH, RETH, SOL) | `token_resolution_error token=BTC chain=ethereum error_type=TokenNotFoundError ... Symbol 'BTC' not found in registry for ethereum` |
| 2 | strategy | INFO | No Alchemy key -- using free public RPC | `No API key configured -- using free public RPC for ethereum (rate limits may apply)` |
| 3 | strategy | INFO | No CoinGecko key -- using on-chain primary with free CoinGecko fallback | `No CoinGecko API key -- using on-chain pricing (Chainlink oracles) with free CoinGecko as fallback` |
| 4 | strategy | INFO | Anvil port cleanup timeout (cosmetic) | `Port 60790 not freed after 5.0s` |

### Findings Analysis

**Finding 1 (WARNING - 9 token resolution failures):** The gateway's Chainlink price source initialization batch-resolves a list of common tokens (BTC, COMP, MKR, SNX, LDO, STETH, CBETH, RETH, SOL) on the target chain to build its oracle map. Nine of these symbols are absent from the ethereum static token registry. The strategy's actual tokens (WETH, USDC) resolved correctly, so this does not affect execution. However, the volume of warnings (9 per run) is noisy. Known missing tokens: `BTC` (registry has `WBTC`), `STETH` (registry has `WSTETH`), and 7 others. The static token registry for ethereum should be expanded or the Chainlink init batch should be filtered to only symbols known to exist on the target chain.

**Findings 2-3 (INFO):** Expected in this environment -- no Alchemy or CoinGecko API keys configured in `.env`. Graceful degradation to public endpoints and on-chain oracles is working correctly. Prices were fetched successfully.

**Finding 4 (INFO):** Cosmetic Anvil cleanup timing warning -- the port release takes slightly longer than the 5s shutdown timeout. Does not affect test correctness or reproducibility.

## Result

**PASS** - The strategy executed cleanly on an Ethereum Anvil fork. RSI=52.92 placed the strategy correctly in HOLD (neutral zone [40-70]). No on-chain transactions were submitted. Price data (WETH=$2,011.46, USDC=$1.00) and OHLCV data (34 Binance candles) were fetched successfully. The 9 token resolution warnings at startup are a known gap: common token symbols (BTC, COMP, MKR, etc.) are missing from the ethereum static registry, causing noisy but non-blocking warnings on every run.

---

SUSPICIOUS_BEHAVIOUR_COUNT: 4
SUSPICIOUS_BEHAVIOUR_ERRORS: 0
