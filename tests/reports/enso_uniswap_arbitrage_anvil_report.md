# E2E Strategy Test Report: enso_uniswap_arbitrage (Anvil)

**Date:** 2026-03-06 06:21
**Result:** PASS
**Mode:** Anvil
**Duration:** ~2 minutes

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

**Note:** The strategy lives at `strategies/incubating/enso_uniswap_arbitrage/` (not `demo/`). The prompt referenced `strategies/demo/enso_uniswap_arbitrage` which does not exist in this worktree. The ALCHEMY_API_KEY is not set in `.env`; the framework fell back to the free public RPC (`base-rpc.publicnode.com`).

**Previous run (2026-03-05):** FAIL - `amount="all"` chaining was broken because Enso receipt parser lacked `extract_swap_amounts()`. This run confirms that bug has been fixed.

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

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | strategy | WARNING | Placeholder prices (fired twice, once per intent) | `IntentCompiler using PLACEHOLDER PRICES. Slippage calculations will be INCORRECT. This is only acceptable for unit tests.` |
| 2 | strategy | WARNING | Insecure gateway mode | `INSECURE MODE: Auth interceptor disabled - no auth_token configured. This is acceptable for local development on 'anvil'.` |
| 3 | strategy | INFO | No Alchemy key, using public RPC | `No API key configured -- using free public RPC for base (rate limits may apply)` |
| 4 | strategy | INFO | No CoinGecko key, using on-chain fallback | `No CoinGecko API key -- using on-chain pricing (Chainlink oracles) with free CoinGecko as fallback.` |
| 5 | strategy | WARNING | Unrelated strategy import failure | `Failed to import strategy strategies.incubating.pendle_pt_swap_arbitrum.strategy (retry failed): cannot import name 'IntentStrategy' from partially initialized module 'almanak' (most likely due to a circular import)` |

**Notes on findings:**
- Finding #1 (Placeholder prices): The IntentCompiler used placeholder prices because no live price feed was available in Anvil mode. Slippage protection may not function precisely. This is a known Anvil limitation but worth tracking since it affects risk controls.
- Finding #2 (Insecure mode): Expected and safe for Anvil.
- Finding #3 & #4 (No API keys): Non-blocking; strategy executed successfully using public RPC and on-chain pricing fallback.
- Finding #5 (Pendle circular import): Bug in `strategies/incubating/pendle_pt_swap_arbitrum/strategy.py` - unrelated to the tested strategy but indicates a broken incubating strategy that should be fixed.

## Result

**PASS** - The `enso_uniswap_arbitrage` strategy completed the full two-step cross-protocol arbitrage sequence. Both intents in the `Intent.sequence()` executed: (1) USDC->WETH via Enso (2 TXs confirmed), (2) WETH->USDC via Uniswap V3 using `amount="all"` chaining (2 TXs confirmed). The `amount="all"` chaining that failed in the previous run (2026-03-05) now works correctly - the Enso receipt enrichment is properly passing the output amount to the next step.

---

SUSPICIOUS_BEHAVIOUR_COUNT: 5
SUSPICIOUS_BEHAVIOUR_ERRORS: 0
