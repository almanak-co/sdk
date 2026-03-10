# E2E Strategy Test Report: aerodrome_paper_trade (Anvil)

**Date:** 2026-03-05 21:48
**Result:** PASS
**Mode:** Anvil
**Duration:** ~2 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | aerodrome_paper_trade |
| Chain | base |
| Network | Anvil fork (auto-managed by CLI) |
| Anvil Port | 50227 (auto-assigned by CLI) |
| Fork Block | 42978965 |
| Fork RPC | https://base-rpc.publicnode.com (public fallback, no Alchemy key) |

## Config Changes Made

| Field | Before | After (test) | Restored |
|-------|--------|--------------|---------|
| `network` | `mainnet` | `anvil` | `mainnet` |

Trade amounts were already within budget cap: `0.001 WETH + 3 USDC` (~$5 at test-time prices). No amount changes needed.

No `force_action` field in this strategy. The RSI defaulted to 57.8 (in-range 35-65), which naturally triggered an LP_OPEN.

## Execution

### Setup
- [x] Config updated: `network` set to `anvil`
- [x] CLI auto-started Anvil fork of Base at port 50227
- [x] CLI auto-funded wallet 0xf39Fd6e5...: 100 ETH, 1 WETH (slot 3), 10000 USDC (slot 9)
- [x] Gateway started on port 50051 (insecure mode, anvil network)

### Strategy Run
- [x] RSI(14) fetched successfully from Binance via OHLCV router: 57.8 (in range)
- [x] LP_OPEN intent triggered: 0.001 WETH + 3 USDC in WETH/USDC volatile pool on Aerodrome
- [x] Intent compiled to 3 transactions (WETH approve, USDC approve, add_liquidity)
- [x] All 3 transactions confirmed on Anvil fork

### Transactions

| # | Intent | TX Hash | Block | Gas Used | Status |
|---|--------|---------|-------|----------|--------|
| 1 | WETH Approve | `d27515b63a5b947d27a2a658fbddb6afab3777ab19a7d287c6f6085d0740390b` | 42978968 | 46,343 | SUCCESS |
| 2 | USDC Approve | `aa8cd1d6228f97651bc07033af057b79a2150d24c6df23a9bb53ed3919c25a39` | 42978969 | 55,785 | SUCCESS |
| 3 | Add Liquidity | `b0a99de62034b060ba398c267096d25fbdcc157a77d8ad0ed09494159eea1d2e` | 42978970 | 239,728 | SUCCESS |

**Total gas: 341,856**

### Key Log Output

```text
RSI(14) = 57.8
RSI in range (57.8), opening LP
LP_OPEN: 0.0010 WETH + 3.0000 USDC (WETH/USDC/volatile)
Compiled Aerodrome LP_OPEN intent: WETH/USDC, stable=False, 3 txs (approve + approve + add_liquidity), 312000 gas
EXECUTED: LP_OPEN completed successfully
  Txs: 3 (d27515...390b, aa8cd1...5a39, b0a99d...1d2e) | 341,856 gas
Enriched LP_OPEN result with: liquidity (protocol=aerodrome, chain=base)
LP position opened in WETH/USDC
Status: SUCCESS | Intent: LP_OPEN | Gas used: 341856 | Duration: 41832ms
```

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | strategy | WARNING | CoinGecko rate limiting (4 hits, exponential backoff up to 8s) | `Rate limited by CoinGecko for WETH/USD, backoff: 1.00s` ... `backoff: 8.00s` |
| 2 | strategy | WARNING | IntentCompiler using placeholder prices during compilation | `IntentCompiler using PLACEHOLDER PRICES. Slippage calculations will be INCORRECT. This is only acceptable for unit tests.` |
| 3 | strategy | WARNING | Circular import error for unrelated incubating strategy at startup | `Failed to import strategy strategies.incubating.pendle_pt_swap_arbitrum.strategy (retry failed): cannot import name 'IntentStrategy' from partially initialized module 'almanak'` |

**Assessment:**

- **CoinGecko rate limiting** (WARNING): Expected on free tier with no API key. The system handles it gracefully with exponential backoff (1s, 2s, 4s, 8s) and falls back to on-chain Chainlink pricing. Prices were correctly resolved (WETH = $2080.74, USDC = $1.00). Not a blocking issue but adds ~9 seconds of latency per run.
- **Placeholder prices** (WARNING): The IntentCompiler warns that it is using placeholder prices for slippage calculations. This is cosmetic for Anvil testing — the Aerodrome adapter itself confirmed `using_placeholders=False`. Slippage was applied at the adapter level, not the compiler level.
- **Circular import in pendle_pt_swap_arbitrum** (WARNING severity, non-blocking): An incubating strategy fails to import at startup due to a circular import in `almanak/__init__.py`. This is pre-existing and does not affect the tested strategy. However, it is surfaced at startup which may confuse users.

## Result

**PASS** - The aerodrome_paper_trade strategy executed successfully on an Anvil Base fork. RSI(14)=57.8 triggered an LP_OPEN for 0.001 WETH + 3 USDC on the Aerodrome WETH/USDC volatile pool. All 3 transactions (approve x2, add_liquidity) confirmed on-chain in ~42 seconds total. No blocking errors.

SUSPICIOUS_BEHAVIOUR_COUNT: 3
SUSPICIOUS_BEHAVIOUR_ERRORS: 0
