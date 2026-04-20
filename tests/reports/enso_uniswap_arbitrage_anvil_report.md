# E2E Strategy Test Report: enso_uniswap_arbitrage (Anvil)

**Date:** 2026-03-15 17:52
**Result:** PASS
**Mode:** Anvil
**Duration:** ~3 minutes

---

> **[2026-03-15 update]** Re-run for kitchen loop iter-81. PASS again. New finding: Uniswap V3
> receipt parser falls back to 18 decimals for USDC on Base, causing `0.0000 token1` in log
> output (display/data quality bug). See Suspicious Behaviour section.

---

**[Previous run: 2026-03-06 06:21 — PASS]**

## Configuration

| Field | Value |
|-------|-------|
| Strategy | enso_uniswap_arbitrage |
| Strategy Path | strategies/incubating/enso_uniswap_arbitrage/ |
| Chain | base |
| Network | Anvil fork (public RPC: base-rpc.publicnode.com) |
| Anvil Port | 54827 (Base chain, auto-started by managed gateway) |
| Trade Size | $0.40 USD (well within $50 budget cap) |
| Mode | buy_enso_sell_uniswap |
| Config Changes | None (trade_size_usd was already $0.40) |

**Note:** The strategy lives at `strategies/incubating/enso_uniswap_arbitrage/` (not `demo/`). The prompt referenced `strategies/demo/enso_uniswap_arbitrage` which does not exist in this worktree.

**Config changes:** None. `trade_size_usd = "0.4"` is well under the $1000 budget cap. No `force_action` field exists — `decide()` always executes the arbitrage unconditionally.

**Previous run (2026-03-05):** FAIL - `amount="all"` chaining was broken because Enso receipt parser lacked `extract_swap_amounts()`. Fixed by 2026-03-06.

## Execution

### Setup
- Anvil fork of Base auto-started by managed gateway on port 54827 using publicnode.com public RPC
- Managed gateway auto-started on port 50053
- Managed gateway auto-funded wallet: 100 ETH, 1 WETH (slot 3), 10,000 USDC (slot 9)

### Strategy Run
- Strategy executed with `uv run almanak strat run -d strategies/incubating/enso_uniswap_arbitrage --network anvil --once`
- The strategy always executes the arbitrage sequence (no HOLD logic)
- Returned an `IntentSequence` with 2 steps, both executed successfully

### Sequence Execution

| Step | Intent | Protocol | Status | TX Hashes |
|------|--------|----------|--------|-----------|
| 1a | SWAP Approve (Enso) | Enso | SUCCESS | `edd8dfc3...c24b` |
| 1b | SWAP Execute (Enso) | Enso | SUCCESS | `fdea753e...d436` |
| 2a | SWAP Approve (Uniswap V3) | Uniswap V3 | SUCCESS | `8a8475d9...0c10` |
| 2b | SWAP Execute (Uniswap V3) | Uniswap V3 | SUCCESS | `46403dbf...889b` |

**Step 1 details (Enso):**
- Enso route found: USDC (0x8335...) -> WETH (0x4200...), amount_out=193,113,552,956,996 (~0.0002 WETH)
- Price impact: 0 bp
- Compiled: 0.4000 USDC -> 0.0002 WETH (min: 0.0002 WETH), gas estimate: 1,433,327
- Gas used: 55,437 (approve) + 1,098,804 (swap) = 1,154,241 total
- Both TXs confirmed (blocks 42981723, 42981724)

**Step 2 details (Uniswap V3):**
- `amount="all"` resolved correctly to 0.000193113552956996 WETH (output of step 1)
- Compiled: 0.0002 WETH -> 0.3851 USDC (min: 0.3812 USDC), gas estimate: 280,000
- Gas used: 46,031 (approve) + 114,137 (swap) = 160,168 total
- Both TXs confirmed (blocks 42981725, 42981726)

### Key Log Output
```text
EnsoUniswapArbitrageStrategy initialized: trade_size=$0.4, slippage=1.0%, pair=WETH/USDC, mode=buy_enso_sell_uniswap
Executing buy_enso_sell_uniswap arbitrage: USDC -> WETH -> USDC
ARB SEQUENCE: Buy $0.40 WETH via Enso -> Sell on Uniswap V3
Route found: 0x833589fC... -> 0x42000000..., amount_out=193113552956996, price_impact=0bp
Compiled SWAP (Enso): 0.4000 USDC -> 0.0002 WETH (min: 0.0002 WETH)
  Slippage: 1.00% | Impact: N/A | Txs: 2 | Gas: 1,433,327
EXECUTED: SWAP completed successfully | Txs: 2 (edd8df...c24b, fdea75...d436) | 1,154,241 gas
Resolving amount='all' to 0.000193113552956996 for intent 2/2
Compiled SWAP: 0.0002 WETH -> 0.3851 USDC (min: 0.3812 USDC)
  Slippage: 1.00% | Txs: 2 | Gas: 280,000
EXECUTED: SWAP completed successfully | Txs: 2 (8a8475...0c10, 46403d...889b) | 160,168 gas
Enriched SWAP result with: swap_amounts (protocol=uniswap_v3, chain=base)
Status: SUCCESS | Intent: SWAP | Gas used: 160168 | Duration: 98144ms
Iteration completed successfully.
```

## Suspicious Behaviour (2026-03-15 run)

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | strategy | WARNING | Insecure gateway mode (expected for Anvil) | `INSECURE MODE: Auth interceptor disabled - no auth_token configured. This is acceptable for local development on 'anvil'.` |
| 2 | strategy | WARNING | Uniswap V3 receipt parser: zero amount_out display for USDC | `Parsed Uniswap V3 swap: 0.0002 token0 -> 0.0000 token1, slippage=N/A, tx=0x56b3...9aac` |

**Notes on findings:**
- Finding #1 (Insecure mode): Expected and safe for Anvil local testing.
- Finding #2 (Zero amount_out in receipt parser): Root cause confirmed in
  `almanak/framework/connectors/uniswap_v3/receipt_parser.py` lines 413-417. When token decimals
  are unresolved at parse time, the parser falls back to 18 decimals. USDC has 6 decimals. With
  18-decimal fallback, the raw USDC amount (~401,000 microUSDC = 401000) is divided by 10^18
  yielding ~4e-13, which displays as `0.0000` with `:.4f`. The on-chain swap itself executed
  correctly (compiler showed "0.0002 WETH -> 0.4010 USDC"), but `SwapAmounts.amount_out_decimal`
  in the enriched result would carry the wrong value. This is a data quality bug affecting any
  code that consumes `result.swap_amounts.amount_out` downstream (e.g., PnL tracking, teardown
  valuation).

**CoinGecko fallback (INFO):** `No CoinGecko API key -- using on-chain pricing (Chainlink oracles) with free CoinGecko as fallback.` Non-blocking. Prices resolved correctly (USDC: $0.9999, WETH: $2097, all 4 sources).

## Result

**PASS** — Both legs of the Enso-Uniswap arbitrage sequence executed on-chain against the Base
Anvil fork. All 4 transactions confirmed. The `amount="all"` chaining resolved correctly to
`0.000191778881417066 WETH`. One data quality bug identified: Uniswap V3 receipt parser falls
back to 18 decimals for USDC on Base (should be 6), producing `0.0000` for `amount_out_decimal`
in the enriched `SwapAmounts` result.

---

### Previous Run Suspicious Behaviour (2026-03-06)

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | strategy | WARNING | Placeholder prices | `IntentCompiler using PLACEHOLDER PRICES. Slippage calculations will be INCORRECT.` |
| 2 | strategy | WARNING | Insecure gateway mode | `INSECURE MODE: Auth interceptor disabled...` |
| 3 | strategy | INFO | No Alchemy key, using public RPC | `No API key configured -- using free public RPC for base (rate limits may apply)` |
| 4 | strategy | INFO | No CoinGecko key | `No CoinGecko API key -- using on-chain pricing...` |
| 5 | strategy | WARNING | Unrelated strategy import failure | `Failed to import strategy strategies.incubating.pendle_pt_swap_arbitrum.strategy...` |

SUSPICIOUS_BEHAVIOUR_COUNT: 2
SUSPICIOUS_BEHAVIOUR_ERRORS: 0
