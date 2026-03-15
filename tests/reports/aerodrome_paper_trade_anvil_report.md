# E2E Strategy Test Report: aerodrome_paper_trade (Anvil)

**Date:** 2026-03-16 00:30
**Result:** PASS
**Mode:** Anvil
**Duration:** ~4 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | aerodrome_paper_trade |
| Chain | base |
| Network | Anvil fork (auto-managed by CLI) |
| Anvil Port | 61358 (auto-assigned by managed gateway) |
| Fork Block | 43403240 |
| Pool | WETH/USDC (volatile) |
| Amount0 | 0.001 WETH |
| Amount1 | 3 USDC |
| RSI Period | 14 |
| RSI Range | [35, 65] |

## Config Changes Made

None. Trade sizes (0.001 WETH + 3 USDC, approximately $5 at test-time prices) are well within the $1000 budget cap. No `force_action` field is supported by this strategy. The `"network": "mainnet"` setting in config.json is overridden at runtime by the `--network anvil` CLI flag.

## Execution

### Setup
- [x] Managed gateway auto-started on 127.0.0.1:50052 (anvil mode)
- [x] Anvil fork started for Base on port 61358 (chain_id=8453, block=43403240)
- [x] Wallet 0xf39Fd6e5... funded automatically from `anvil_funding` config: 100 ETH, 1 WETH (slot 3), 10,000 USDC (slot 9)

### Strategy Run
- [x] RSI(14) = 52.9 — within range [35, 65] — LP_OPEN triggered
- [x] Aerodrome LP_OPEN compiled: 3 transactions (approve WETH + approve USDC + add_liquidity)
- [x] All 3 transactions executed and confirmed on Anvil
- [x] Receipt parsed; liquidity data extracted and attached to result

### Transactions

| Step | TX Hash | Block | Gas Used | Status |
|------|---------|-------|----------|--------|
| Approve WETH | `53d75fe39bdca7beb5529a5ee8e0b55cff7e6631ee2446be84a79ffc369cee73` | 43403243 | 46,343 | SUCCESS |
| Approve USDC | `dd79b784e4ef3e438c98e3e8c584e98951f951e0f11e8f6f59af47f77c8b8634` | 43403244 | 55,785 | SUCCESS |
| Add Liquidity | `c2e91365783119a829a69ec9608683b9c6ea0eb6ebd033d1511589583efca13e` | 43403245 | 239,728 | SUCCESS |

**Total gas used:** 341,856

### Key Log Output

```text
RSI(14) = 52.9
RSI in range (52.9), opening LP
LP_OPEN: 0.0010 WETH + 3.0000 USDC (WETH/USDC/volatile)
Compiled Aerodrome LP_OPEN intent: WETH/USDC, stable=False, 3 txs (approve + approve + add_liquidity), 312000 gas
Aggregated price for WETH/USD: 2095.867702085 (confidence: 1.00, sources: 4/4, outliers: 0)
Aggregated price for USDC/USD: 0.9999595 (confidence: 1.00, sources: 4/4, outliers: 0)
EXECUTED: LP_OPEN completed successfully
   Txs: 3 (53d75f...ee73, dd79b7...8634, c2e913...a13e) | 341,856 gas
Enriched LP_OPEN result with: liquidity (protocol=aerodrome, chain=base)
LP position opened in WETH/USDC
Status: SUCCESS | Intent: LP_OPEN | Gas used: 341856 | Duration: 28175ms
```

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | strategy | WARNING | Insecure mode (expected for local Anvil) | `INSECURE MODE: Auth interceptor disabled - no auth_token configured. This is acceptable for local development on 'anvil'.` |
| 2 | strategy | WARNING | Placeholder prices for slippage (expected for Anvil) | `IntentCompiler using PLACEHOLDER PRICES. Slippage calculations will be INCORRECT. This is only acceptable for unit tests.` |
| 3 | strategy | INFO | CoinGecko key absent, using on-chain fallback | `No CoinGecko API key -- using on-chain pricing (Chainlink oracles) with free CoinGecko as fallback.` |

All three findings are expected and normal for Anvil mode:

- **Warning 1** (insecure mode): Intentional for local Anvil testing. Not a real issue.
- **Warning 2** (placeholder prices): The IntentCompiler uses placeholder prices for slippage at the compiler layer; the Aerodrome adapter itself confirmed `using_placeholders=False`. Execution succeeded regardless. This is the standard Anvil behaviour.
- **Finding 3** (no CoinGecko key): The 4-source pricing stack (Chainlink + Binance + DexScreener + CoinGecko free) operated with full confidence (1.00, 4/4 sources). No pricing degradation.

No zero prices, no failed API fetches, no reverts, no token resolution failures, no timeouts detected.

## Result

**PASS** — The aerodrome_paper_trade strategy executed LP_OPEN on Aerodrome (Base) via Anvil fork. RSI(14)=52.9 triggered an LP open for 0.001 WETH + 3 USDC. All 3 transactions (approve WETH, approve USDC, add_liquidity) confirmed on-chain. Receipt parsing and result enrichment succeeded; liquidity data extracted and attached to result.

---

SUSPICIOUS_BEHAVIOUR_COUNT: 3
SUSPICIOUS_BEHAVIOUR_ERRORS: 0
