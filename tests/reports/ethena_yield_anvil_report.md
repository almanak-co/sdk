# E2E Strategy Test Report: ethena_yield (Anvil)

**Date:** 2026-02-27 15:59
**Result:** PASS (stake path) / FAIL (swap path -- ENSO_API_KEY not set)
**Mode:** Anvil
**Duration:** ~5 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | EthenaYieldStrategy (demo_ethena_yield) |
| Chain | ethereum |
| Network | Anvil fork (public RPC: https://ethereum-rpc.publicnode.com) |
| Anvil Port | 62292 (managed, auto-assigned) |
| Fork Block | 24549314 |

## Config Changes Made

| Field | Original | Changed To | Restored |
|-------|----------|-----------|---------|
| `force_action` | `"swap"` | `"stake"` | YES -- restored to `"swap"` after run |

**Reason for change:** The `force_action: "swap"` (USDC → USDe via Enso) is blocked by a missing `ENSO_API_KEY`. Changed to `"stake"` to validate the primary DeFi action (USDe → sUSDe deposit with Ethena). Config was restored to original value after the test.

## Execution

### Setup
- Managed gateway auto-started on port 50051 (network=anvil)
- Anvil fork of Ethereum started on port 62292 (block 24549314, chain_id=1)
- Wallet auto-funded by managed gateway via `anvil_funding` config:
  - 100 ETH, 10,000 USDC (slot 9), 1 WETH (slot 3), 1,000 USDe (brute-force slot 2)

### Run 1: force_action=swap -- FAIL

- Intent: SWAP 5 USDC → USDe via Enso
- Result: EXECUTION_FAILED (3 retries exhausted)
- Error: `EnsoConfigError: Configuration Error: API key is required. Set ENSO_API_KEY env var`
- No on-chain transactions submitted

### Run 2: force_action=stake -- PASS

- Intent: STAKE 5 USDe → sUSDe via Ethena
- Result: SUCCESS
- On-chain transactions: 2 TXs confirmed (approve + deposit)
- Total gas used: 134,881

### Key Log Output (Run 2)
```text
Forced action: STAKE USDe
STAKE intent: 5.0000 USDe -> sUSDe
EthenaAdapter initialized for chain=ethereum, wallet=0xf39Fd6e5...
Compiled STAKE intent: 5 USDe via ethena, 2 txs, 200000 gas
Simulation successful: 2 transaction(s), total gas: 211281
Gas estimate tx[0]: raw=46,281 buffered=50,909 (x1.1)
Gas estimate tx[1]: raw=165,000 buffered=181,500 (x1.1)
Transaction submitted: tx_hash=82f629a2..., confirmed block=24549320, gas=46281
Transaction submitted: tx_hash=b2390039..., confirmed block=24549321, gas=88600
EXECUTED: STAKE completed successfully
  Txs: 2 (82f629...6e5f, b23900...373e) | 134,881 gas
Parsed Ethena receipt: tx=0xb2390039..., stakes=1, withdraws=0
Enriched STAKE result with: stake_amount, shares_received (protocol=ethena, chain=ethereum)
Staking successful: 5 USDe -> sUSDe
Status: SUCCESS | Intent: STAKE | Gas used: 134881 | Duration: 26282ms
Iteration completed successfully.
```

## Transactions (Anvil -- not mainnet)

| Step | TX Hash | Gas Used | Status |
|------|---------|----------|--------|
| TX 1: Approve USDe | `82f629a25a14271501b4836648a4002187b96731fc2c3057d89d1c4bea3f6e5f` | 46,281 | SUCCESS |
| TX 2: Stake USDe → sUSDe | `b23900399c32f3b5d52a7e2e28c5f7c5ca0d6002b156922df06c255df5c1373e` | 88,600 | SUCCESS |

*(Anvil local fork transactions -- no block explorer links)*

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | strategy | ERROR | Enso API key missing (Run 1 only) | `Failed to compile Enso SWAP intent: Configuration Error: API key is required. Set ENSO_API_KEY env var or pass api_key. (Parameter: api_key)` |
| 2 | strategy | WARNING | Placeholder prices in IntentCompiler | `IntentCompiler using PLACEHOLDER PRICES. Slippage calculations will be INCORRECT. This is only acceptable for unit tests.` |
| 3 | gateway | INFO | No public RPC API key (rate limits possible) | `No API key configured -- using free public RPC for ethereum (rate limits may apply)` |
| 4 | gateway | INFO | No CoinGecko API key -- on-chain fallback | `No CoinGecko API key -- using on-chain pricing (Chainlink oracles) with free CoinGecko as fallback.` |
| 5 | gateway | WARNING | 9 tokens not in Ethereum registry (Run 1 only) | `token_resolution_error token=BTC ... COMP ... MKR ... SNX ... LDO ... STETH ... CBETH ... RETH ... SOL chain=ethereum error_type=TokenNotFoundError` |

### Analysis

- **Finding 1 (Enso API key)**: Hard blocker for `force_action: "swap"`. The USDC→USDe swap via Enso aggregator requires `ENSO_API_KEY`. Without it, all 3 retries fail immediately. The strategy's default config has swap as the primary action; this makes the full strategy flow non-functional without the key. No graceful fallback is implemented (e.g., fall back to staking existing USDe balance).
- **Finding 2 (Placeholder prices)**: Expected in Anvil mode. No live Chainlink oracle feed on the fork when pricing context is not available. For the STAKE intent this is low-risk (no slippage parameter). For SWAP intents it would be a real issue.
- **Finding 3-4 (Missing API keys)**: Expected for a bare `.env`. Public RPC and on-chain pricing fallbacks worked for the stake path.
- **Finding 5 (Token registry gaps)**: Multiple well-known Ethereum tokens (BTC, COMP, MKR, SNX, LDO, STETH, CBETH, RETH, SOL) are missing from the static token registry for Ethereum. Raised during market service initialization. These are all real Ethereum tokens (WBTC, stETH, cbETH, rETH, etc.) that should be registered. BTC/SOL are non-Ethereum but the resolver should recognize them as non-EVM and return a clearer error.

No zero prices, transaction reverts, or gas-related errors in the successful run.

## Result

**PASS (stake path)** -- The Ethena STAKE intent (USDe → sUSDe deposit) executed successfully on Ethereum Anvil fork, confirming 2 transactions (134,881 gas). The receipt parser correctly extracted `stakes=1` and enriched the result with `stake_amount` and `shares_received`.

**FAIL (swap path)** -- The `force_action: "swap"` (USDC → USDe via Enso) fails hard without `ENSO_API_KEY`. This is the default configured action, meaning the strategy is non-functional out-of-the-box unless `ENSO_API_KEY` is set or the user has a native USDe balance.

---

SUSPICIOUS_BEHAVIOUR_COUNT: 5
SUSPICIOUS_BEHAVIOUR_ERRORS: 1
