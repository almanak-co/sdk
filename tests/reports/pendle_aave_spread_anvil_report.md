# E2E Strategy Test Report: pendle_aave_spread (Anvil)

**Date:** 2026-02-20 08:43
**Result:** FAIL
**Mode:** Anvil
**Duration:** ~1 minute

## Configuration

| Field | Value |
|-------|-------|
| Strategy | pendle_aave_spread |
| Chain | arbitrum |
| Network | Anvil fork (Arbitrum, port 51361 managed) |
| Wallet | 0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266 (Anvil default) |

## Config Changes Made

The original `config.json` had trade sizes that would exceed the $50 USD budget cap:

| Field | Original Value | Modified Value | Reason |
|-------|----------------|----------------|--------|
| `anvil_funding.WSTETH` | `5` | `0.014` | 5 WSTETH ≈ $12,000 >> $50 cap; 0.014 WSTETH ≈ $33.60 |
| `min_rotation_amount` | `0.1` | `0.005` | Reduced so 0.014 WSTETH balance clears the minimum threshold |

No `force_action` parameter is supported by this strategy; the state machine triggers an initial supply automatically when a non-zero balance is detected in MONITORING phase.

## Execution

### Setup
- [x] Anvil fork of Arbitrum started (managed by `almanak strat run`, port 51361)
- [x] Gateway started on port 50052 (managed)
- [x] Wallet funded: 100 ETH (native), 0.014 WSTETH (slot 1)
- [x] WSTETH confirmed: `14000000000000000` raw = 0.014 WSTETH
- [x] WSTETH price fetched: $2,400.46 → 0.014 WSTETH ≈ $33.60 (within $50 cap)

### Strategy Run

The managed gateway auto-started a fresh Anvil fork (port 51361) and funded the wallet
using the `anvil_funding` config values, then ran a single iteration.

**Result: HOLD — no on-chain transaction produced.**

Two errors prevented the strategy from executing a trade:

1. **Balance comparison type error** (strategy bug):
   ```
   Balance check failed: '>' not supported between instances of 'TokenBalance' and 'decimal.Decimal'
   ```
   In `_handle_monitoring()`, the strategy calls `market.balance(self.base_token)` which returns
   a `TokenBalance` object. The code then does `base_balance > self.min_rotation_amount` where
   `min_rotation_amount` is a `Decimal`. The `TokenBalance` type does not support direct
   comparison with `Decimal`, causing the balance check to fail with an exception.

2. **Missing `lending_rate` method** (strategy bug):
   ```
   Aave rate unavailable: 'MarketSnapshot' object has no attribute 'lending_rate'
   ```
   The strategy calls `market.lending_rate("aave_v3", self.base_token, side="supply")` in
   `_get_aave_rate()`. The `MarketSnapshot` object does not expose a `lending_rate()` method
   as of the current SDK version.

Because the balance check failed, the strategy fell through to `_compute_and_report_spread()`,
which also failed to compute the spread (both Aave rate and Pendle yield returned `None`).
The strategy returned `HOLD`.

### Key Log Output

```text
[info]  PendleAaveSpread initialized: spread_entry=2.0%, spread_exit=0.5%, max_rotation=50%, maturity=2026-06-25
[info]  Funded 0xf39Fd6e5... with 100 ETH
[info]  Funded 0xf39Fd6e5... with WSTETH via brute-force slot 1
[info]  Aggregated price for WSTETH/USD: 2400.46
[warn]  Balance check failed: '>' not supported between instances of 'TokenBalance' and 'decimal.Decimal'
[warn]  Aave rate unavailable: 'MarketSnapshot' object has no attribute 'lending_rate'
[info]  pendle_aave_spread HOLD: Monitoring: spread=?%, Aave=?%, Pendle=?%, maturity=124d
Status: HOLD | Intent: HOLD | Duration: 551ms
```

## On-Chain Transactions

**None.** The strategy did not produce any on-chain transactions.

## Root Cause Analysis

Two SDK API incompatibilities in the strategy code:

### Bug 1: `TokenBalance` vs `Decimal` comparison

**File:** `strategies/incubating/pendle_aave_spread/strategy.py`, line 193

```python
# Buggy:
base_balance = market.balance(self.base_token)
if base_balance > self.min_rotation_amount:  # TypeError: TokenBalance vs Decimal
```

`market.balance()` returns a `TokenBalance` object. The strategy needs to extract the
numeric amount from it, e.g. `float(base_balance)` or `base_balance.amount` (depending on
the `TokenBalance` API).

### Bug 2: `market.lending_rate()` does not exist

**File:** `strategies/incubating/pendle_aave_spread/strategy.py`, line 386-387

```python
# Buggy:
rate = market.lending_rate("aave_v3", self.base_token, side="supply")
```

`MarketSnapshot` does not have a `lending_rate()` method. The Aave supply rate is not
currently exposed through the `MarketSnapshot` API. The strategy would need to use an
alternative data source (e.g., direct on-chain call via `market.rpc_call()`, or a custom
lending rate provider injected via the gateway).

## Result

**FAIL** — Strategy produced HOLD on first iteration due to two API incompatibilities:
(1) `TokenBalance` cannot be directly compared to `Decimal` in the balance check, and
(2) `MarketSnapshot.lending_rate()` does not exist in the current SDK. No on-chain
transaction was executed.

## Recommended Fixes

1. Fix balance comparison: use `Decimal(str(base_balance.amount))` or cast via the
   `TokenBalance` API to get a numeric value before comparing to `self.min_rotation_amount`.
2. Implement or stub `lending_rate` data access: either add a `lending_rate()` method to
   `MarketSnapshot` backed by the gateway's integration service, or use a fallback
   (e.g., hardcoded wstETH Aave APY estimate) when the live rate is unavailable.
