# E2E Strategy Test Report: uniswap_rsi (Anvil)

**Date:** 2026-03-03 19:42
**Result:** PASS
**Mode:** Anvil
**Duration:** ~4 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | demo_uniswap_rsi |
| Chain | ethereum |
| Network | Anvil fork (ethereum-rpc.publicnode.com, block 24576999) |
| Anvil Port | 60273 (auto-allocated by managed gateway) |
| trade_size_usd | $3 (within $500 budget cap, no change needed) |
| rsi_period | 14 |
| rsi_oversold | 40 |
| rsi_overbought | 70 |
| base_token | WETH |
| quote_token | USDC |

**Config changes made:** None. `trade_size_usd` was already $3, well under the $500 cap. The strategy does not support `force_action`. RSI at run time was 44.98 (neutral zone), so the strategy returned HOLD.

**Note on Alchemy key:** `.env` has an empty `ALCHEMY_API_KEY`. Anvil forking used `https://ethereum.publicnode.com` (free public endpoint). The managed gateway auto-detected no API key and fell back to `https://ethereum-rpc.publicnode.com`.

## Execution

### Setup
- Managed gateway auto-started on 127.0.0.1:50052 (network=anvil)
- Anvil fork started on port 60273 (forked from https://ethereum-rpc.publicnode.com at block 24576999, chain_id=1)
- Wallet funded automatically from config `anvil_funding`: 100 ETH, 1 WETH (slot 3), 10,000 USDC (slot 9) for 0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266

### Strategy Run
- Strategy executed with `--network anvil --once`
- WETH price: $1,967.65 (aggregated from on-chain Chainlink + CoinGecko free tier, 2 sources, confidence 1.00)
- USDC price: $0.999964 (2 sources, confidence 1.00)
- OHLCV data: 34 candles from Binance (28 finalized, 6 provisional)
- RSI(14) = 44.98 -- in neutral zone [40-70] -- HOLD returned
- No on-chain transaction submitted (correct behaviour for HOLD)
- Iteration completed successfully in 2,458ms

### Key Log Output
```text
info  Anvil fork started: port=60273, block=24576999, chain_id=1
info  Funded 0xf39Fd6e5... with 100 ETH
info  Funded 0xf39Fd6e5... with WETH via known slot 3
info  Funded 0xf39Fd6e5... with USDC via known slot 9
info  Aggregated price for WETH/USD: 1967.6452195 (confidence: 1.00, sources: 2/2, outliers: 0)
info  ohlcv_fetched provider=binance instrument=WETH/USD candles=34 finalized=28 provisional=6
info  Aggregated price for USDC/USD: 0.9999644999999999 (confidence: 1.00, sources: 2/2, outliers: 0)
info  demo_uniswap_rsi HOLD: RSI=44.98 in neutral zone [40-70] (hold #1)
Status: HOLD | Intent: HOLD | Duration: 2458ms
Iteration completed successfully.
```

## On-Chain Transactions

None. Strategy returned HOLD (RSI=44.98 in neutral zone [40-70]). No transactions submitted.

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | gateway | WARNING | INSECURE MODE (expected for Anvil) | `INSECURE MODE: Auth interceptor disabled - no auth_token configured. This is acceptable for local development on 'anvil'.` |
| 2 | strategy | WARNING | Port not freed after 5s | `Port 60273 not freed after 5.0s` |
| 3 | strategy | INFO | No API key -- public RPC rate limits possible | `No API key configured -- using free public RPC for ethereum (rate limits may apply)` |
| 4 | gateway | INFO | No CoinGecko API key -- on-chain pricing primary | `No CoinGecko API key -- using on-chain pricing (Chainlink oracles) with free CoinGecko as fallback.` |

### Findings Analysis

**Finding 1 (WARNING):** INSECURE MODE is explicitly expected and correct for Anvil. The gateway message itself states "This is acceptable for local development on 'anvil'." Not a real issue.

**Finding 2 (WARNING):** The managed gateway's internal Anvil process did not release port 60273 within 5 seconds after stop was requested. The process eventually terminates but the warning fires first. This is a cosmetic cleanup race condition. No impact on correctness, but could be a resource-leak concern in sustained test suites.

**Finding 3 (INFO):** No Alchemy API key in `.env`. Graceful fallback to publicnode free RPC is working. Pricing and execution succeeded with valid prices. Acceptable for local Anvil testing; may cause rate limit errors under sustained multi-strategy load.

**Finding 4 (INFO):** No CoinGecko API key. On-chain pricing (Chainlink oracles) is used as primary, with free-tier CoinGecko as fallback. Both sources returned consistent data with 100% confidence and no outliers. No functional impact.

No zero prices, API errors, token resolution failures, reverts, NaN values, or ERROR-level findings detected.

## Result

**PASS** - Strategy executed successfully end-to-end on an Ethereum Anvil fork at block 24576999. Market data fetched correctly (WETH: $1,967.65, USDC: $0.9999 at 100% confidence from 2 sources). RSI(14)=44.98 fell in the neutral zone [40-70], so the strategy correctly returned HOLD with no on-chain transaction. The run completed in 2,458ms.

---

SUSPICIOUS_BEHAVIOUR_COUNT: 4
SUSPICIOUS_BEHAVIOUR_ERRORS: 0
