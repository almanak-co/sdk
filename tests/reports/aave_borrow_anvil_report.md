# E2E Strategy Test Report: aave_borrow (Anvil)

**Date:** 2026-02-27 15:20-15:21 UTC
**Result:** PASS
**Mode:** Anvil
**Duration:** ~3 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | demo_aave_borrow |
| Chain | arbitrum |
| Network | Anvil fork (publicnode.com -- no Alchemy key configured) |
| Anvil Port | 62497 (managed, auto-started by CLI) |
| Collateral | 0.002 WETH (~$3.90 at $1950/WETH) |
| Borrow Token | USDC |
| LTV Target | 50% |
| Min Health Factor | 2.0 |

## Config Changes Made

- Added `"force_action": "supply"` to trigger immediate SUPPLY intent (restored after test).

Trade size: 0.002 WETH * $1950 = ~$3.90 USD. Well within the $500 budget cap.

## Execution

### Setup
- [x] Anvil fork auto-started by CLI on port 62497 (Arbitrum mainnet fork at block 436553549)
- [x] Managed gateway auto-started on port 50052 (insecure mode for Anvil)
- [x] Wallet (0xf39Fd6e5...) funded: 100 ETH, 1 WETH, 10,000 USDC (via config `anvil_funding`)

### Strategy Run
- [x] Strategy executed with `--network anvil --once`
- [x] `force_action: supply` triggered immediate SUPPLY intent
- [x] SUPPLY intent compiled to 3 transactions (approve + supply + setUserUseReserveAsCollateral)
- [x] All 3 transactions confirmed successfully on Anvil
- [x] Receipt parser enriched result with `supply_amount`, `a_token_received`, `supply_rate`
- [x] Strategy state transitioned: restored `supplied` from prior state (force_action overrides state machine)

### Transactions

| # | Description | TX Hash | Block | Gas Used | Status |
|---|-------------|---------|-------|----------|--------|
| 1 | approve WETH | `0xb7fe9c1ca77507c0d2123c2291e729351397387fa052029191c652ea061967a2` | 436553552 | 53,440 | SUCCESS |
| 2 | supply WETH to Aave V3 | `0x01a809853b799d109f297b0527718041e977b101d9fe8d160e5807c9d693f338` | 436553553 | 205,598 | SUCCESS |
| 3 | setUserUseReserveAsCollateral | `0x05aeb3954b4df3ce672a3d4301974d76397ef21981f8dbe9f405164f4ff2727b` | 436553554 | 45,572 | SUCCESS |

Total gas: 304,610

### Key Log Output
```text
[info] Aggregated price for WETH/USD: 1950.326103525 (confidence: 1.00, sources: 2/2, outliers: 0)
[info] Aggregated price for USDC/USD: 1.0 (confidence: 1.00, sources: 2/2, outliers: 0)
[info] Forced action: SUPPLY collateral
[info] SUPPLY intent: 0.0020 WETH to Aave V3
[info] Compiled SUPPLY: 0.0020 WETH to aave_v3 (as collateral) | Txs: 3 | Gas: 530,000
[info] Simulation successful: 3 transaction(s), total gas: 728788
[info] EXECUTED: SUPPLY completed successfully | Txs: 3 | 304,610 gas
[info] Parsed Aave V3: SUPPLY 2,000,000,000,000,000 to 0x82af...bab1, tx=0x01a8...f338, 205,598 gas
[info] Enriched SUPPLY result with: supply_amount, a_token_received, supply_rate
[info] Supply successful - state: supplied
Status: SUCCESS | Intent: SUPPLY | Gas used: 304610 | Duration: 42262ms
```

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | strategy | INFO | No Alchemy API key; public RPC used | `No API key configured -- using free public RPC for arbitrum (rate limits may apply)` |
| 2 | gateway | INFO | No CoinGecko API key; on-chain Chainlink used as primary | `No CoinGecko API key -- using on-chain pricing (Chainlink oracles) with free CoinGecko as fallback` |
| 3 | gateway | WARNING | Insecure mode active (expected for local dev) | `INSECURE MODE: Auth interceptor disabled - no auth_token configured. This is acceptable for local development on 'anvil'.` |
| 4 | strategy | INFO | Anvil port not freed within 5s after stop | `Port 62497 not freed after 5.0s` |

**Analysis:**
- Findings 1 and 2: No Alchemy or CoinGecko API keys in `.env` -- public/free-tier fallbacks used. Prices were correctly resolved via Chainlink on-chain oracles (WETH=$1950.33, USDC=$1.00). No zero-price issue.
- Finding 3: Insecure gateway mode is explicitly expected and acceptable for Anvil local testing.
- Finding 4: Minor OS-level timing issue with port cleanup after managed Anvil stops; does not affect correctness.

No zero prices, no reverts, no API fetch failures, no token resolution errors, no NaN/None values detected.

## Result

**PASS** - The aave_borrow strategy successfully compiled and executed a SUPPLY intent on Arbitrum (Anvil fork): 0.002 WETH supplied to Aave V3 via 3 on-chain transactions totalling 304,610 gas. Prices resolved correctly from Chainlink on-chain oracles. Result enrichment (supply_amount, a_token_received, supply_rate) worked correctly.

---

SUSPICIOUS_BEHAVIOUR_COUNT: 4
SUSPICIOUS_BEHAVIOUR_ERRORS: 0
