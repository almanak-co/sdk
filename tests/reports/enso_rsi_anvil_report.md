# E2E Strategy Test Report: enso_rsi (Anvil)

**Date:** 2026-03-16 (run at 2026-03-15T17:44 UTC)
**Result:** PASS
**Mode:** Anvil
**Duration:** ~3 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | demo_enso_rsi (EnsoRSIStrategy) |
| Chain | base (Chain ID: 8453) |
| Network | Anvil fork (Base mainnet via Alchemy) |
| Anvil Port | 63502 (SDK auto-managed) |
| Trade Size | $3.00 USD (well under $1000 cap) |
| Base Token | WETH |
| Quote Token | USDC |

## Config Changes Made

| Field | Before | After (test) | Restored |
|-------|--------|--------------|---------|
| `force_action` | (absent) | `"buy"` | removed (restored to original) |

`force_action: "buy"` was added to trigger an immediate trade, bypassing RSI evaluation.
Trade size ($3) was already under the $1000 budget cap -- no amount changes needed.
Config restored to original state after test.

## Execution

### Setup
- [x] Anvil started on port 8547 (Base fork, pre-run); managed gateway auto-started on port 63502
- [x] Managed gateway auto-started by CLI on 127.0.0.1:50052 (network=anvil, anvil chains: base)
- [x] Anvil fork auto-started on port 63502 (forked Base at block 43403652)
- [x] Wallet 0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266 auto-funded: 100 ETH, 1 WETH, 10,000 USDC
- [x] Enso service initialized: `available=True`
- [x] `force_action: "buy"` triggered immediately (bypassed RSI evaluation)

### Strategy Run
- [x] Strategy detected `force_action: buy` and created SWAP intent: $3.00 USDC -> WETH via Enso
- [x] Enso route found: USDC -> WETH, amount_out=1,430,501,182,317,547 (~0.00143 WETH), price_impact=2bp
- [x] Compiled: 2 TXs (approve + swap), gas estimate 597,404
- [x] Simulation successful: 2 TXs, total gas 831,925
- [x] TX 1/2 (approve USDC): `c3c3d729e3fa5981c1db48188e0826c05bcd367880c1be439ce0e2c35e3e869c`, confirmed block 43403655, gas 55,437
- [x] TX 2/2 (swap via Enso): `37c4462a5bc128418a885f00b5aea5f1974ecf3fcebe5bb361f6e77ca3132964`, confirmed block 43403656, gas 417,778
- [x] Result enricher: SwapAmounts extracted (protocol=enso, chain=base)
- [x] Final status: SUCCESS | Gas: 473,215 | Duration: 29,981ms

### Key Log Output

```text
Force action requested: buy
BUY via Enso: $3.00 USDC -> WETH, slippage=1.0%
Getting Enso route: USDC -> WETH, amount=3000001
Route found: 0x833589fC... -> 0x42000000..., amount_out=1430501182317547, price_impact=2bp
Compiled SWAP (Enso): 3.0000 USDC -> 0.0014 WETH (min: 0.0014 WETH)
Slippage: 1.00% | Impact: 2bp (0.02%) | Txs: 2 | Gas: 597,404
Transaction confirmed: tx_hash=c3c3d729..., block=43403655, gas_used=55437
Transaction confirmed: tx_hash=37c4462a..., block=43403656, gas_used=417778
EXECUTED: SWAP completed successfully
Txs: 2 (c3c3d7...869c, 37c446...2964) | 473,215 gas
Enriched SWAP result with: swap_amounts (protocol=enso, chain=base)
Status: SUCCESS | Intent: SWAP | Gas used: 473215 | Duration: 29981ms
```

## Transactions

| Intent | TX Hash | Gas Used | Status |
|--------|---------|----------|--------|
| APPROVE (USDC) | `c3c3d729e3fa5981c1db48188e0826c05bcd367880c1be439ce0e2c35e3e869c` | 55,437 | SUCCESS |
| SWAP (USDC->WETH via Enso) | `37c4462a5bc128418a885f00b5aea5f1974ecf3fcebe5bb361f6e77ca3132964` | 417,778 | SUCCESS |

Note: Anvil fork transactions -- no block explorer link.

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | strategy | INFO | No CoinGecko key -- on-chain pricing active (expected) | `No CoinGecko API key -- using on-chain pricing (Chainlink oracles) with free CoinGecko as fallback.` |
| 2 | strategy | INFO | Insecure mode warning (expected for Anvil) | `INSECURE MODE: Auth interceptor disabled - no auth_token configured. This is acceptable for local development on 'anvil'.` |

### Suspicious Behaviour Analysis

**Finding 1 (INFO):** Expected configuration note for a dev environment without a CoinGecko API key. The 4-source price aggregator (Chainlink + Binance + DexScreener + CoinGecko) returned full-confidence prices (1.00, 4/4 sources, 0 outliers) for both USDC and WETH. Non-actionable.

**Finding 2 (INFO):** Expected and correct for Anvil. The insecure mode is explicitly flagged as acceptable for local development. Non-actionable.

No zero prices, no failed API fetches, no token resolution failures, no reverts, no timeouts, no placeholder price warnings (improvement vs prior run).

## Notable Improvements vs Prior Run (2026-03-05)

- Placeholder price warning is gone -- the IntentCompiler now operates with live prices, not placeholder values.
- Swap gas usage dropped significantly: 997,006 gas (prior) vs 473,215 gas (this run). Likely a route optimization by Enso.
- Price impact visible: 2bp (0.02%) compared to 0bp reported previously.
- SwapAmounts result enrichment confirmed working.

## Result

**PASS** -- The `enso_rsi` strategy on Base chain successfully compiled and executed a $3.00 USDC -> WETH swap via the Enso aggregator on a Base Anvil fork. Both transactions (approve + swap) confirmed on-chain with 473,215 total gas in ~30 seconds. No suspicious behaviour detected.

---

SUSPICIOUS_BEHAVIOUR_COUNT: 0
SUSPICIOUS_BEHAVIOUR_ERRORS: 0
