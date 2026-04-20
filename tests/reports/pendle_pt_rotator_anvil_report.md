# E2E Strategy Test Report: pendle_pt_rotator (Anvil)

**Date:** 2026-02-20 15:47
**Result:** FAIL
**Mode:** Anvil
**Duration:** ~3 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | pendle_pt_rotator |
| Chain | arbitrum |
| Network | Anvil fork (auto-managed by CLI) |
| Anvil Port | 52339 (auto-assigned by managed gateway) |
| Wallet | 0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266 |

## Config Changes Made

The following fields were modified temporarily to force an immediate trade and stay within the $50 budget cap, then restored after the run:

| Field | Original | Modified | Reason |
|-------|----------|----------|--------|
| `rsi_entry_threshold` | 40 | 100 | Force immediate entry (RSI is always < 100) |
| `require_macd_confirmation` | true | false | Skip MACD gate to ensure entry triggers |
| `trade_size_token` | 0.5 | 0.055 | Budget cap: tranche = 0.055 * 0.25 = 0.01375 wstETH (~$33 at $2,401/wstETH) |

Config has been restored to original values after the test run.

## Execution

### Setup
- Anvil (manually started on port 8545, but CLI auto-started its own fork on port 52339)
- Gateway auto-started by CLI on port 50052
- Wallet funded by the managed gateway via `anvil_funding` config: 100 ETH + 10 wstETH (slot 1)

### Strategy Run
- Entry signal was triggered correctly: `RSI=43.6 (threshold=100), MACD=bullish`
- Strategy advanced from IDLE to ACCUMULATING and called `_buy_pt_tranche()`
- Run crashed in `_buy_pt_tranche()` before any transaction was submitted

### Key Log Output

```text
[info] Entry signal: RSI=43.6 (threshold=100), MACD=bullish
[error] Error in decide(): '<' not supported between instances of 'TokenBalance' and 'decimal.Decimal'
Traceback (most recent call last):
  File "strategy.py", line 180, in decide
    return self._handle_idle(market, days_to_maturity)
  File "strategy.py", line 232, in _handle_idle
    return self._buy_pt_tranche(market)
  File "strategy.py", line 324, in _buy_pt_tranche
    tranche_amount = min(self.trade_size_token * self.tranche_pct, available)
                     ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
TypeError: '<' not supported between instances of 'TokenBalance' and 'decimal.Decimal'
[info] pendle_pt_rotator HOLD: Error: '<' not supported between instances of 'TokenBalance' and 'decimal.Decimal'
Status: HOLD | Intent: HOLD | Duration: 1973ms
```

## Bug Identified

**Location**: `strategies/incubating/pendle_pt_rotator/strategy.py`, line 324 in `_buy_pt_tranche()`

**Root Cause**: `market.balance(self.base_token)` returns a `TokenBalance` object, not a raw `Decimal`. The code then tries to use `min()` to compare `Decimal` (from `self.trade_size_token * self.tranche_pct`) with `TokenBalance`, which fails because `TokenBalance` and `Decimal` are incompatible types.

**Failing Code**:
```python
available = market.balance(self.base_token)  # returns TokenBalance, not Decimal
tranche_amount = min(self.trade_size_token * self.tranche_pct, available)  # TypeError
```

**Fix Required**: Convert `TokenBalance` to `Decimal` before the comparison. The `TokenBalance` object likely has an `.amount` attribute or can be converted via `Decimal(str(available))` or `available.amount`.

Example fix:
```python
available = Decimal(str(market.balance(self.base_token)))  # convert TokenBalance -> Decimal
tranche_amount = min(self.trade_size_token * self.tranche_pct, available)
```

The same pattern is used in `_handle_accumulating()` at lines 254-264, which would hit the same issue.

## Transactions

None. Strategy exited before any transaction was submitted.

## Result

**FAIL** - The strategy's entry signal triggered correctly (RSI=43.6 matched the forced threshold of 100), and the managed gateway auto-funded the Anvil wallet with wstETH successfully, but the strategy crashed in `_buy_pt_tranche()` due to a `TypeError`: `market.balance()` returns a `TokenBalance` object and the code compares it directly with a `Decimal` using `min()`. No on-chain transaction was produced.
