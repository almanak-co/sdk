# E2E Strategy Test Report: enso_uniswap_arbitrage (Mainnet)

**Date:** 2026-02-10 01:58
**Result:** FAIL
**Mode:** Mainnet (live on-chain)
**Chain:** base
**Duration:** ~7 seconds

## Configuration

| Field | Value |
|-------|-------|
| Strategy | enso_uniswap_arbitrage |
| Chain | base |
| Network | Mainnet |
| Wallet | 0x54776446Aa29Fc49d152B4850bD410eA1E4d24bF |

## Wallet Preparation

| Token | Required | Had Before | Funded | Method |
|-------|----------|------------|--------|--------|
| ETH   | ~$0.40 gas | 0.001737 ETH (~$3.61) | 0 | existing |
| WETH  | ~$0.40 | 0.001577 WETH (~$3.28) | 0 | existing |
| USDC  | ~$0.40 | 2.244317 USDC | 0 | existing |

**Funding TX(s):** None (wallet already funded)

**Balance Gate Result:** PASS - wallet has sufficient funds for $0.40 arbitrage

## Strategy Execution

**Strategy ran with:** `--network mainnet --once`

**Expected Intents:**
1. SWAP: $0.40 USDC → WETH via Enso
2. SWAP: ALL WETH → USDC via Uniswap V3

**Actual Execution:** Intent compilation failed before any transactions were submitted

### Key Log Output
```text
[2026-02-09T18:58:04.907261Z] info: 📈 demo_enso_uniswap_arbitrage intent sequence (2 steps):
[2026-02-09T18:58:04.907291Z] info:    1. 🔄 SWAP: $0.40 USDC → WETH (slippage: 1.00%) via enso
[2026-02-09T18:58:04.907316Z] info:    2. 🔄 SWAP: ALL WETH → USDC (slippage: 1.00%) via uniswap_v3
[2026-02-09T18:58:04.907338Z] info: Note: decide() returned 2 intents but single-chain orchestrator only executes the first.

[2026-02-09T18:58:04.911963Z] error: Failed to compile Enso SWAP intent: Price for 'USDC' is missing in the price oracle.

Traceback:
  File "almanak/framework/intents/compiler.py", line 2330, in _compile_enso_swap
    amount_in = self._usd_to_token_amount(intent.amount_usd, from_token)
  File "almanak/framework/intents/compiler.py", line 6772, in _usd_to_token_amount
    price = self._require_token_price(token.symbol)
  File "almanak/framework/intents/compiler.py", line 6995, in _require_token_price
    raise ValueError: Price for 'USDC' is missing in the price oracle.

[2026-02-09T18:58:05.991743Z] error: Failed to compile Enso SWAP intent (retry 1/3)
[2026-02-09T18:58:08.025260Z] error: Failed to compile Enso SWAP intent (retry 2/3)
[2026-02-09T18:58:12.146827Z] error: Failed to compile Enso SWAP intent (retry 3/3)

[2026-02-09T18:58:12.147725Z] error: Intent failed after 3 retries
```

### Gateway Log Highlights
```text
2026-02-10 01:57:43,424 - almanak.gateway.services.enso_service - INFO - EnsoService initialized: available=True
2026-02-10 01:57:43,427 - almanak.gateway.server - INFO - Gateway gRPC server started on 127.0.0.1:50051
```

No price oracle or token resolution errors in gateway logs.

## Transactions

No transactions were executed. Intent compilation failed before transaction creation.

## Root Cause Analysis

**Immediate Cause:** `IntentCompiler._require_token_price()` could not find USDC price for Base chain

**Technical Details:**
- Compiler requires USDC price to convert `amount_usd="0.4"` to token amount
- Price oracle does not have USDC price data for Base mainnet
- This is a price provider issue, not an Enso or execution issue

**Code Location:**
- `almanak/framework/intents/compiler.py:6995` - `_require_token_price()`
- `almanak/framework/intents/compiler.py:6772` - `_usd_to_token_amount()`
- `almanak/framework/intents/compiler.py:2330` - `_compile_enso_swap()`

**Why this matters:**
- Strategy wallet has sufficient USDC and WETH
- EnsoService is available and initialized
- Gateway is running correctly
- **But:** Compiler cannot convert USD amounts without price data

## Result

**FAIL** - Price oracle missing USDC price for Base chain. Intent compilation failed before any transactions were submitted.

**Error Summary:** `ValueError: Price for 'USDC' is missing in the price oracle. Compilation requires a valid price to calculate amounts and slippage.`

**Recommendation:**
1. Add USDC price provider for Base chain (CoinGecko, CryptoCompare, or hardcode $1.00 for stablecoins)
2. Or allow intent to specify token amounts directly instead of USD amounts
3. Test again after price oracle supports Base/USDC

**Config Restored:** `network` reset to `"anvil"` in `config.json`
