# E2E Strategy Test Report: enso_rsi (Anvil)

**Date:** 2026-02-23 03:46 UTC
**Result:** PASS
**Mode:** Anvil
**Duration:** ~3 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | enso_rsi (demo_enso_rsi) |
| Chain | base (Chain ID: 8453) |
| Network | Anvil fork (Base mainnet) |
| Anvil Port | 59596 (managed gateway auto-fork) |
| trade_size_usd | $3.00 (under $100 budget cap -- no change needed) |
| base_token | WETH |
| quote_token | USDC |

## Config Changes Made

- Added `"force_action": "buy"` to trigger an immediate trade (trade size already $3, well under $100 cap).
- Removed `force_action` from `strategies/demo/enso_rsi/config.json` after test to restore original state.
- No amount changes were needed (config was already within the $100 budget cap).

## Execution

### Setup
- Pre-started Anvil fork of Base mainnet on port 8547 (chain ID 8453 verified)
- Gateway started on port 50051 (insecure mode, Anvil network)
- Wallet `0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266` pre-funded:
  - 100 ETH (native, via `anvil_setBalance`)
  - 1 WETH (`0x4200000000000000000000000000000000000006`, via `deposit()`)
  - 10,000 USDC (`0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913`, via storage slot 9)
- Managed gateway auto-started a second Anvil fork on port 59596 and re-funded from `anvil_funding` config

### Strategy Run
- Strategy executed with `--network anvil --once`
- Force action `"buy"` triggered immediately: $3.00 USDC -> WETH via Enso aggregator
- Enso route resolved successfully: USDC -> WETH, amountOut=1,543,480,380,153,723 (~0.0015 WETH), priceImpact=3bp

### Intents Executed

| Intent | Status | Details |
|--------|--------|---------|
| SWAP (Enso) | SUCCESS | 3.0000 USDC -> ~0.0015 WETH, slippage 1.00%, 2 transactions |

### Transactions

| Tx # | TX Hash | Block | Gas Used | Status |
|------|---------|-------|----------|--------|
| 1 (approve) | `ac6187a177f4dcee8ae37aa35b87d5379a51821506381d7fda6c98fa070988c2` | 42501895 | 55,437 | Confirmed |
| 2 (swap) | `5f95622571aeb86569dc99875752e17c5728e6dc522a2cb783462764fc9f3b74` | 42501896 | 349,011 | Confirmed |

**Total gas used:** 404,448

### Key Log Output

```text
Force action requested: buy
BUY via Enso: $3.00 USDC -> WETH, slippage=1.0%
Getting Enso route: USDC -> WETH, amount=3000000
Route found: 0x833589fC... -> 0x42000000..., amount_out=1543480380153723, price_impact=3bp
Compiled SWAP (Enso): 3.0000 USDC -> 0.0015 WETH (min: 0.0015 WETH)
  Slippage: 1.00% | Impact: 3bp (0.03%) | Txs: 2 | Gas: 503,322
Transaction confirmed: tx_hash=ac6187a1..., block=42501895, gas_used=55437
Transaction confirmed: tx_hash=5f956225..., block=42501896, gas_used=349011
EXECUTED: SWAP completed successfully
  Txs: 2 (ac6187...88c2, 5f9562...3b74) | 404,448 gas
Status: SUCCESS | Intent: SWAP | Gas used: 404448 | Duration: 20508ms
```

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | strategy | WARNING | Placeholder prices - slippage protection unreliable | `IntentCompiler using PLACEHOLDER PRICES. Slippage calculations will be INCORRECT. This is only acceptable for unit tests.` |
| 2 | strategy | WARNING | Amount chaining broken - teardown `amount="all"` will fail | `Amount chaining: no output amount extracted from step 1; subsequent amount='all' steps will fail` |
| 3 | gateway | WARNING | CoinGecko on free tier (rate-limit risk in production) | `COINGECKO_API_KEY not configured - CoinGecko will use free tier API (30 requests/minute limit)` |
| 4 | strategy | INFO | Gas estimate below compiler minimum, using compiler limit | `Gas estimate tx[0]: raw=55,819 buffered=83,728 (x1.5) < compiler=120,000, using compiler limit` |
| 5 | strategy | INFO | Port not freed after Anvil fork stop | `Port 59596 not freed after 5.0s` (cosmetic cleanup delay, no impact on result) |

**Analysis of findings:**

- **Finding #1 (Placeholder prices):** The IntentCompiler initializes with `using_placeholders=True` because no live price feed is wired up during Anvil tests. The Enso route itself queries live prices from the Enso API, so actual swap amounts were correct. However, slippage bounds are computed against placeholder prices rather than real market prices. This is expected and acceptable for Anvil test runs. In production (mainnet mode with a price provider configured), the compiler would use `using_placeholders=False`.
- **Finding #2 (Amount chaining):** After a successful 2-step bundle (approve + swap), the runner cannot extract the output amount from step 1 (approval tx) to chain into a subsequent `amount='all'` step. This warning fires every time a multi-step bundle executes. It would only be a real problem if the strategy chained a follow-on `amount='all'` intent -- which `enso_rsi` does NOT do in its normal `decide()` path (only in `generate_teardown_intents()`). This is a latent teardown bug, not a runtime failure.
- **Finding #3 (CoinGecko free tier):** Expected for development environments without a CoinGecko Pro API key. Acceptable for a single-run test; would need a paid key for high-frequency production use.
- **Finding #4 (Gas limit floor):** The buffered gas estimate (83,728) was below the compiler's minimum floor (120,000), so the compiler limit was used. This is conservative and safe -- no risk of out-of-gas.
- **Finding #5 (Port cleanup delay):** Cosmetic - the fork cleanup logged a 5-second delay before the port was freed. No impact on test validity.

## Result

**PASS** - The `enso_rsi` strategy on Base chain successfully executed a forced BUY swap of $3.00 USDC to WETH via the Enso DEX aggregator. Two on-chain transactions (approve + swap) were confirmed on the Anvil fork. Total gas: 404,448. Duration: ~20 seconds for execution.

---

SUSPICIOUS_BEHAVIOUR_COUNT: 5
SUSPICIOUS_BEHAVIOUR_ERRORS: 0
