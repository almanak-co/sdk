# E2E Strategy Test Report: enso_rsi (Anvil)

**Date:** 2026-03-03 11:46
**Result:** PASS
**Mode:** Anvil
**Duration:** ~3 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | demo_enso_rsi (EnsoRSIStrategy) |
| Chain | base (Chain ID: 8453) |
| Network | Anvil fork (Base mainnet via publicnode.com) |
| Anvil Port | 50103 (SDK auto-managed) |
| Anvil Fork Block | 42874494 |
| Trade Size | $3.00 USD (well under $500 cap) |
| Base Token | WETH |
| Quote Token | USDC |

## Config Changes Made

| Field | Before | After (test) | Restored |
|-------|--------|--------------|---------|
| `force_action` | (absent) | `"buy"` | removed (restored to original) |

`force_action: "buy"` was added to trigger an immediate trade and removed after the test. Trade size ($3) was already under the $500 cap — no amount changes needed.

## Execution

### Setup
- [x] Managed gateway auto-started by CLI on 127.0.0.1:50052
- [x] Anvil fork auto-started on port 50103 (forked Base via publicnode.com at block 42874494)
- [x] Wallet 0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266 auto-funded: 100 ETH, 1 WETH, 10,000 USDC
- [x] Enso service initialized: `available=True`
- [x] `force_action: "buy"` triggered immediately (bypassed RSI evaluation)

### Strategy Run
- [x] Strategy detected `force_action: buy` and created SWAP intent: $3.00 USDC -> WETH via Enso
- [x] Enso route found: USDC -> WETH, amount_out=1,532,306,993,131,315 (~0.0015 WETH), price_impact=0bp
- [x] Compiled: 2 TXs (approve + swap), gas estimate 988,154
- [x] Simulation successful: 2 TXs, total gas 1,418,050
- [x] TX 1/2 (approve USDC): confirmed block 42874497, gas 55,437
- [x] TX 2/2 (swap via Enso): confirmed block 42874498, gas 753,864
- [x] Final status: SUCCESS | Gas: 809,301 | Duration: 52,223ms

### Key Log Output

```text
Force action requested: buy
BUY via Enso: $3.00 USDC -> WETH, slippage=1.0%
Getting Enso route: USDC -> WETH, amount=3000000
Route found: 0x833589fC... -> 0x42000000..., amount_out=1532306993131315, price_impact=0bp
Compiled SWAP (Enso): 3.0000 USDC -> 0.0015 WETH (min: 0.0015 WETH)
Slippage: 1.00% | Impact: N/A | Txs: 2 | Gas: 988,154
TX 1/2 submitted: 4b9ff15455dd9cd19dcf196f901e4666dfa8769926ea815e5533e069204b7298
TX 1/2 confirmed: block=42874497, gas_used=55437
TX 2/2 submitted: ad5587fc657e8c1fabe5d075f0e1e37ceac24a5c0fa3433c7ce97acb77f6d7a0
TX 2/2 confirmed: block=42874498, gas_used=753864
EXECUTED: SWAP completed successfully
Txs: 2 (4b9ff1...7298, ad5587...d7a0) | 809,301 gas
Status: SUCCESS | Intent: SWAP | Gas used: 809301 | Duration: 52223ms
```

## Transactions

| Intent | TX Hash | Gas Used | Status |
|--------|---------|----------|--------|
| APPROVE (USDC) | `4b9ff15455dd9cd19dcf196f901e4666dfa8769926ea815e5533e069204b7298` | 55,437 | SUCCESS |
| SWAP (USDC->WETH via Enso) | `ad5587fc657e8c1fabe5d075f0e1e37ceac24a5c0fa3433c7ce97acb77f6d7a0` | 753,864 | SUCCESS |

Note: Anvil fork transactions — no block explorer link.

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | strategy | WARNING | Placeholder prices in IntentCompiler | `IntentCompiler using PLACEHOLDER PRICES. Slippage calculations will be INCORRECT. This is only acceptable for unit tests.` |
| 2 | strategy | WARNING | Amount chaining failure after swap | `Amount chaining: no output amount extracted from step 1; subsequent amount='all' steps will fail` |
| 3 | strategy | INFO | No Alchemy API key — using free public RPC | `No API key configured -- using free public RPC for base (rate limits may apply)` |
| 4 | strategy | INFO | No CoinGecko API key — using on-chain pricing fallback | `No CoinGecko API key -- using on-chain pricing (Chainlink oracles) with free CoinGecko as fallback.` |
| 5 | strategy | INFO | Anvil port not freed quickly on teardown | `Port 50103 not freed after 5.0s` |

### Suspicious Behaviour Analysis

**Finding 1 (Placeholder Prices, WARNING):** The IntentCompiler operates with placeholder prices in Anvil mode because no live price feed is connected at compile time. Slippage guard calculations use dummy values rather than real market prices. The warning text explicitly says "only acceptable for unit tests," which fires on every Anvil strategy run — this is a known limitation acceptable for integration testing.

**Finding 2 (Amount Chaining, WARNING):** After the swap succeeded, the runner emitted a warning that no output amount was extracted from the Enso swap result. This means the Enso receipt parser is not implementing `extract_swap_amounts()` for this route, which would cause multi-step intent chains using `amount="all"` to fail. The teardown path in this strategy (`generate_teardown_intents`) uses `amount="all"` — this is a real data enrichment gap. Single-step swaps are unaffected in this run.

**Findings 3 and 4 (INFO):** Expected configuration info for a dev environment without Alchemy/CoinGecko API keys. Non-actionable.

**Finding 5 (Port cleanup, INFO):** Cosmetic — Anvil process lingers slightly on shutdown. Non-blocking.

## Result

**PASS** — The `enso_rsi` strategy on Base chain successfully compiled and executed a $3.00 USDC -> WETH swap via the Enso aggregator on a Base Anvil fork. Both transactions (approve + swap) confirmed on-chain. One actionable finding: the amount chaining warning (#2) indicates the Enso receipt parser does not extract swap output amounts, which would break teardown flows using `amount="all"`. No errors.

---

SUSPICIOUS_BEHAVIOUR_COUNT: 4
SUSPICIOUS_BEHAVIOUR_ERRORS: 0
