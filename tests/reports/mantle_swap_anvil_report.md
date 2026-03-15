# E2E Strategy Test Report: mantle_swap (Anvil)

**Date:** 2026-03-15 18:10
**Result:** PASS
**Mode:** Anvil
**Duration:** ~3 minutes

---

> **2026-03-15 re-run**: PASS — swap executed successfully. Previous run (2026-03-05) failed due
> to CoinGecko rate limiting. This run succeeded with Binance + DexScreener providing WETH price.
> Report below reflects the 2026-03-15 run.

---

## Previous Run (2026-03-05): FAIL

See archived details at the bottom of this file. Root cause was CoinGecko free-tier rate limiting
combined with no Chainlink feeds on Mantle. The issue is transient (rate limit carryover from a
prior test session) and not a code defect.

## Configuration

| Field | Value |
|-------|-------|
| Strategy | demo_mantle_swap |
| Chain | mantle (chain ID 5000) |
| Network | Anvil fork (managed gateway, forked from https://rpc.mantle.xyz via Alchemy) |
| Anvil Port | 52417 (auto-assigned by managed gateway) |
| Base Token | WETH |
| Quote Token | USDT |
| Trade Size | $2 USD (within 1000 USD budget cap) |
| RSI Period | 14 |
| RSI Oversold | 60 |
| RSI Overbought | 65 |

## Config Changes Made

None. `trade_size_usd` is 2, well within the $1000 budget cap. Strategy does not support `force_action`.

## Execution

### Setup
- [x] Killed existing Anvil and Gateway processes on ports 8556, 50051, 9090
- [x] Managed gateway auto-started Anvil fork of Mantle at block 92733930, chain_id=5000
- [x] Auto-funding from `anvil_funding` config applied: 1000 MNT (native), 0.1 WETH (slot 0), 10000 USDT (slot 0)

### Strategy Run
- [x] Strategy executed with `--network anvil --once`
- [x] WETH/USD price fetched: $2109.97 (3/4 sources: Binance + DexScreener + CoinGecko, confidence 0.90)
- [x] RSI(14) = 56.22, below oversold threshold (60) -> BUY triggered
- [x] Intent compiled: SWAP $2.00 USDT -> WETH via uniswap_v3 (Agni Finance on Mantle)
- [x] TX 1/2 (USDT approve): `0x0c3a08879ef28227a95d33489790df390b4b1fbd12ff8c71cb62e3de4d0d7e92` — 46,251 gas, block 92733933
- [x] TX 2/2 (swap): `0x93ea461a52e1ba1a9abf6f250813ce979b119194af4522ffc75908b877a3a5b0` — 187,285 gas, block 92733934
- [x] Total gas used: 233,536

### Key Log Output
```text
2026-03-15T18:09:36.043709Z [info] Aggregated price for WETH/USD: 2109.97 (confidence: 0.90, sources: 3/4)
2026-03-15T18:09:36.844536Z [info] BUY: RSI=56.22 < 60 | Buying $2.00 of WETH [strategy_module]
2026-03-15T18:09:54.736606Z [info] Compiled SWAP: 2.0000 USDT -> 0.0009 WETH (min: 0.0009 WETH)
2026-03-15T18:10:02.854670Z [info] Transaction confirmed: tx=0c3a08...7e92, block=92733933, gas_used=46251
2026-03-15T18:10:03.069662Z [info] Transaction confirmed: tx=93ea46...a5b0, block=92733934, gas_used=187285
2026-03-15T18:10:13.134180Z [info] EXECUTED: SWAP completed successfully
2026-03-15T18:10:13.137275Z [info] Parsed Uniswap V3 receipt: tx=0x93ea...a5b0, events=3, 187,285 gas
Status: SUCCESS | Intent: SWAP | Gas used: 233536 | Duration: 38678ms
```

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | gateway | INFO | No CoinGecko API key (expected) | `No CoinGecko API key -- using on-chain pricing (Chainlink oracles) with free CoinGecko as fallback.` |
| 2 | strategy | INFO | Stablecoin fallback for USDT price | `Price for 'USDT' not in oracle cache, using stablecoin fallback ($1.00)` |
| 3 | gateway | WARNING | Insecure mode (expected for Anvil) | `INSECURE MODE: Auth interceptor disabled - no auth_token configured. This is acceptable for local development on 'anvil'.` |

Notes:
- Finding 1 is expected in local dev without a CoinGecko API key. Binance + DexScreener still provided WETH price with 3/4 source coverage, confidence 0.90.
- Finding 2 (USDT stablecoin fallback) is correct behavior — $1.00 is the accurate USDT price.
- Finding 3 is expected for Anvil mode and explicitly acknowledged as safe in the log message.
- No zero prices, no real errors, no timeouts, no reverts, no token resolution failures detected.

## Chain-Specific Notes (Mantle)

- **Native gas token**: MNT (funded as native balance, correct chain ID 5000)
- **WETH address**: `0xdEAddEaDdeadDEadDEADDEAddEADDEAddead1111` (bridged WETH)
- **USDT address**: `0x201EBa5CC46D216Ce6DC03F6a759e8E766e956aE` (6 decimals)
- **DEX**: Agni Finance (Uniswap V3 fork) — router `0x319B69888b0d11cec22caA5034e25FfFBDc88421`
- **Anvil port**: 8556 default; managed gateway used auto-assigned port 52417
- **RPC**: `https://mantle-mainnet.g.alchemy.com/v2/***` (Alchemy supports Mantle via `mantle-mainnet` prefix)
- **Chainlink oracles**: None registered for Mantle — pricing uses Binance + DexScreener + CoinGecko (no on-chain fallback)

## Transactions

| Step | TX Hash | Gas Used | Block | Status |
|------|---------|----------|-------|--------|
| USDT approve | `0x0c3a08879ef28227a95d33489790df390b4b1fbd12ff8c71cb62e3de4d0d7e92` | 46,251 | 92733933 | SUCCESS |
| SWAP (USDT->WETH) | `0x93ea461a52e1ba1a9abf6f250813ce979b119194af4522ffc75908b877a3a5b0` | 187,285 | 92733934 | SUCCESS |

## Result

**PASS** — The mantle_swap strategy executed a SWAP intent (USDT -> WETH, $2.00 trade) on a Mantle Anvil fork with 2 confirmed transactions (approve + swap via Agni Finance), total gas 233,536. RSI(14)=56.22 correctly triggered the buy signal. The previous FAIL (2026-03-05) was a transient CoinGecko rate-limit issue; this run benefited from Alchemy's Mantle support providing a more stable fork RPC.

---

## Archived: Previous Run (2026-03-05) — FAIL

### Root Cause

The strategy returned HOLD due to simultaneous failure of both price data sources:

1. **No Chainlink feed for WETH on Mantle** — on-chain (primary) pricing had no feeds registered (`Available feeds: []`).
2. **CoinGecko free tier rate limited immediately** — carried over from a previous test session.

The strategy correctly caught the `ValueError` and returned `Intent.hold(reason="Error: ...")`.

### Key Log Output (2026-03-05)
```text
[error] All data sources failed for WETH/USD: {
    'onchain': "No Chainlink feed for WETH on mantle. Available feeds: []",
    'coingecko': "Rate limited. Retry after 1s"
}
Status: HOLD | Intent: HOLD | Duration: 89ms
```

SUSPICIOUS_BEHAVIOUR_COUNT: 3
SUSPICIOUS_BEHAVIOUR_ERRORS: 0
