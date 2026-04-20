# E2E Strategy Test Report: rsi_macd_lp (Anvil)

**Date:** 2026-03-16 01:37
**Result:** PASS
**Mode:** Anvil
**Duration:** ~4 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | rsi_macd_lp |
| Chain | arbitrum |
| Network | Anvil fork |
| Anvil Port | 8545 (pre-existing) / 57128 (managed, auto-started) |
| Pool | WETH/USDC/500 |
| RSI thresholds | oversold=35, overbought=65 |
| MACD params | fast=12, slow=26, signal=9 |
| LP range | 15% |
| amount0 | 0.001 WETH |
| amount1 | 3 USDC |

## Config Changes Made

None. Trade sizes (0.001 WETH + 3 USDC, ~$5 total) are well within the $1000 budget cap.
The strategy does not support a `force_action` parameter — entry requires RSI < 35 AND MACD bullish confluence.

## Execution

### Setup
- Anvil fork of Arbitrum started (managed, port 57128)
- Gateway started on port 50052 (managed)
- Wallet funded by framework: 100 ETH, 1 WETH, 10000 USDC via `anvil_funding` config block

### Strategy Run
- Strategy executed with `--network anvil --once`
- Intent returned: **HOLD**
- Reason: RSI=56.4 (not oversold; threshold=35), MACD histogram=-0.993624 (neutral)
- No LP_OPEN transaction was submitted (entry conditions not met)
- Duration: 343ms

### Key Log Output
```text
2026-03-15T18:37:33.342313Z [info] ohlcv_fetched provider=binance instrument=WETH/USD candles=34 finalized=28 provisional=6
2026-03-15T18:37:33.474379Z [info] ohlcv_fetched provider=binance instrument=WETH/USD candles=85 finalized=79 provisional=6
2026-03-15T18:37:33.475248Z [info] Signals: RSI=56.4, MACD histogram=-0.993624, bullish=False, bearish=False
2026-03-15T18:37:33.475573Z [info] demo_rsi_macd_lp HOLD: Waiting for confluence: RSI=56.4, MACD=neutral
Status: HOLD | Intent: HOLD | Duration: 343ms
Iteration completed successfully.
```

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | strategy | WARNING | Insecure mode (expected) | `INSECURE MODE: Auth interceptor disabled - no auth_token configured. This is acceptable for local development on 'anvil'.` |
| 2 | strategy | INFO | No CoinGecko API key (expected) | `No CoinGecko API key -- using on-chain pricing (Chainlink oracles) with free CoinGecko as fallback.` |

Both findings are expected and benign for local Anvil testing. No zero prices, no API failures, no token resolution errors, no reverts detected.

## Result

**PASS** — Strategy executed cleanly on Anvil, fetched RSI and MACD data from Binance OHLCV, correctly evaluated confluence conditions (RSI=56.4 above oversold threshold of 35), and returned HOLD as expected. No transaction was submitted because entry conditions were not met, which is correct behaviour.

---

SUSPICIOUS_BEHAVIOUR_COUNT: 2
SUSPICIOUS_BEHAVIOUR_ERRORS: 0
