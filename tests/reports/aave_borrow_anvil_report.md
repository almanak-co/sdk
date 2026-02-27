# E2E Strategy Test Report: aave_borrow (Anvil)

**Date:** 2026-02-27 08:39-08:40 UTC
**Result:** PASS
**Mode:** Anvil
**Duration:** ~2 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | demo_aave_borrow |
| Chain | arbitrum |
| Network | Anvil fork |
| Anvil Port | 58618 (managed, auto-started by CLI) |
| Collateral | 0.002 WETH (~$4.06 at $2031 ETH) |
| Borrow Token | USDC |
| LTV Target | 50% |
| Min Health Factor | 2.0 |

## Config Changes Made

- Added `"force_action": "supply"` to trigger immediate SUPPLY intent (restored after test).

Trade size: 0.002 WETH * $2,031.60 = ~$4.06 USD. Well within the $500 budget cap.

## Execution

### Setup
- [x] Anvil fork auto-started by CLI on port 58618 (Arbitrum mainnet fork at block 436456995)
- [x] Managed gateway auto-started on port 50052 (insecure mode for Anvil)
- [x] Wallet (0xf39Fd6e5...) funded: 100 ETH, 1 WETH, 10,000 USDC (via config `anvil_funding`)

### Strategy Run
- [x] Strategy executed with `--network anvil --once`
- [x] `force_action: supply` triggered immediate SUPPLY intent
- [x] SUPPLY intent compiled to 3 transactions (approve + supply + setUserUseReserveAsCollateral)
- [x] All 3 transactions confirmed successfully on Anvil
- [x] Receipt parser enriched result with `supply_amount`, `a_token_received`, `supply_rate`
- [x] Strategy state transitioned: `idle -> supplied`

### Transactions

| # | Description | TX Hash | Block | Gas Used | Status |
|---|-------------|---------|-------|----------|--------|
| 1 | approve WETH | `0x5573f9ca...3452` | 436456998 | 53,440 | SUCCESS |
| 2 | supply WETH to Aave V3 | `0xff3dc8aa...6d3a` | 436456999 | 205,598 | SUCCESS |
| 3 | setUserUseReserveAsCollateral | `0x31b5be48...7cc7` | 436457000 | 45,572 | SUCCESS |

Total gas: 304,610

### Key Log Output
```text
[info] Forced action: SUPPLY collateral
[info] SUPPLY intent: 0.0020 WETH to Aave V3
[info] Compiled SUPPLY: 0.0020 WETH to aave_v3 (as collateral) | Txs: 3 | Gas: 530,000
[info] Simulation successful: 3 transaction(s), total gas: 310817
[info] EXECUTED: SUPPLY completed successfully | Txs: 3 | 304,610 gas
[info] Parsed Aave V3: SUPPLY 2,000,000,000,000,000 to 0x82af...bab1, tx=0xff3d...6d3a, 205,598 gas
[info] Enriched SUPPLY result with: supply_amount, a_token_received, supply_rate
[info] Supply successful - state: supplied
Status: SUCCESS | Intent: SUPPLY | Gas used: 304610 | Duration: 42187ms
```

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | gateway | WARNING | Token resolution: `BTC` not in arbitrum registry | `token_resolution_error token=BTC chain=arbitrum error_type=TokenNotFoundError ... Did you mean 'WBTC'?` |
| 2 | gateway | WARNING | Token resolution: `STETH` not in arbitrum registry | `token_resolution_error token=STETH chain=arbitrum error_type=TokenNotFoundError ... Did you mean 'WSTETH'?` |
| 3 | gateway | WARNING | Token resolution: `RDNT` not in arbitrum registry | `token_resolution_error token=RDNT chain=arbitrum error_type=TokenNotFoundError` |
| 4 | gateway | WARNING | Token resolution: `MAGIC` not in arbitrum registry | `token_resolution_error token=MAGIC chain=arbitrum error_type=TokenNotFoundError` |
| 5 | gateway | WARNING | Token resolution: `WOO` not in arbitrum registry | `token_resolution_error token=WOO chain=arbitrum error_type=TokenNotFoundError` |
| 6 | gateway | INFO | No CoinGecko API key; using on-chain Chainlink pricing as primary | `No CoinGecko API key -- using on-chain pricing (Chainlink oracles) with free CoinGecko as fallback` |
| 7 | gateway | INFO | Port not freed within 5s after managed Anvil stop | `Port 58618 not freed after 5.0s` |

**Analysis:**
- Findings 1-5 (token resolution warnings): Fire during `MarketService` initialisation for tokens absent from the arbitrum registry or using non-canonical aliases (`BTC` vs `WBTC`, `STETH` vs `WSTETH`). The tokens `RDNT`, `MAGIC`, `WOO` are legitimate Arbitrum tokens that should be added to the static registry. This is a data-layer gap, not a strategy bug. The `BTC`/`STETH` aliases should be added as symbol mappings to `WBTC`/`WSTETH`.
- Finding 6 (no CoinGecko key): Expected for Anvil testing; on-chain Chainlink pricing worked correctly (WETH=$2,031.60, USDC=$0.9999). No zero-price issue.
- Finding 7 (port cleanup): Minor timing issue in the Anvil fork manager cleanup; does not affect correctness.

No zero prices, no reverts, no actual API fetch failures, no timeouts triggered.

## Result

**PASS** - The aave_borrow strategy successfully compiled and executed a SUPPLY intent on Arbitrum (Anvil fork): 0.002 WETH supplied to Aave V3 via 3 on-chain transactions totalling 304,610 gas. The strategy's state machine correctly transitioned from `idle` to `supplied` after successful execution.

---

SUSPICIOUS_BEHAVIOUR_COUNT: 7
SUSPICIOUS_BEHAVIOUR_ERRORS: 0
