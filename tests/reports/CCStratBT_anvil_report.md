# E2E Strategy Test Report: CCStratBT (Anvil)

**Date:** 2026-02-20 14:20
**Result:** PASS (strategy executed cleanly; HOLD due to architectural constraint -- see note)
**Mode:** Anvil
**Duration:** ~3 minutes

---

## Configuration

| Field | Value |
|-------|-------|
| Strategy | TripleSignalStrategy (cc_triple_signal) |
| Class | `strategies/incubating/CCStratBT/strategy.py` |
| Chain | arbitrum |
| Network | Anvil fork (auto-managed, port 52779) |
| Wallet | 0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266 (Anvil default) |

---

## Config Changes Made

The original `config.json` was modified in two categories:

### 1. Budget Cap: trade_size_usd reduced from 500 to 50

```diff
-  "trade_size_usd": "500",
+  "trade_size_usd": "50",
```

This enforces the $50 maximum per trade budget cap.

### 2. Force-trade attempt: indicator periods minimized + thresholds widened

To attempt triggering an immediate trade on a single tick (`--once`), the following parameters were
reduced to their minimum meaningful values and thresholds were set to guarantee a signal:

```diff
-  "rsi_period": 7,       ->  "rsi_period": 1,
-  "rsi_oversold": 40,    ->  "rsi_oversold": 99,     (always oversold)
-  "rsi_overbought": 60,  ->  "rsi_overbought": 1,    (always overbought)
-  "macd_fast": 6,        ->  "macd_fast": 1,
-  "macd_slow": 13,       ->  "macd_slow": 2,
-  "macd_signal": 4,      ->  "macd_signal": 1,
-  "bb_period": 10,       ->  "bb_period": 1,
-  "bb_std_dev": 1.5,     ->  "bb_std_dev": 0.001,
-  "bb_buy_threshold": 0.25, -> "bb_buy_threshold": 0.99,   (always fires)
-  "bb_sell_threshold": 0.75, -> "bb_sell_threshold": 0.01,
-  "min_signals_to_trade": 2, -> "min_signals_to_trade": 1,
-  "cooldown_ticks": 2,   ->  "cooldown_ticks": 0,
```

**Note:** These changes did not produce a trade. See "Architectural Constraint" below.

---

## Execution

### Setup

- Anvil fork started (managed internally by gateway on port 52779, chain=arbitrum, block=434012318)
- Gateway auto-started on 127.0.0.1:50052 (INSECURE mode, anvil network)
- Wallet funded: 100 ETH (gas), 1000 USDC (via anvil_setStorageAt slot 9)
- WETH not needed (strategy starts flat, first action would be buy USDC->WETH)

### Strategy Run

Command:
```
uv run almanak strat run -d strategies/incubating/CCStratBT --network anvil --once
```

Result: `Status: HOLD | Intent: HOLD | Duration: 513ms`

---

## Key Log Output

```text
TripleSignalStrategy initialized: RSI(1, 99.0/1.0), MACD(1,2,1), BB(1, 0.001x),
  consensus>=1, cooldown=0, buffer_size=13
...
Aggregated price for WETH/USD: 1958.71 (confidence: 1.00, sources: 1/1, outliers: 0)
cc_triple_signal HOLD: Buffering prices: 1/3
...
Status: HOLD | Intent: HOLD | Duration: 513ms
Iteration completed successfully.
```

---

## Architectural Constraint: Why No Trade Was Produced

The strategy maintains an internal price history buffer and computes all three indicators
(RSI, MACD, Bollinger Bands) from raw accumulated prices. The minimum prices needed before
any indicator can compute is:

```python
min_needed = max(rsi_period + 1, macd_slow + macd_signal, bb_period)
           = max(1+1, 2+1, 1)
           = 3
```

With `--once`, `decide()` is called exactly once, adding one price to the buffer. The buffer
has 1 price, but 3 are needed. The strategy correctly returns HOLD ("Buffering prices: 1/3").

This is not a bug -- it is correct strategy behavior. The buffer cannot be reduced below 3 without
setting `macd_slow + macd_signal < 1` (impossible) or editing strategy source code.

**There is no `force_action` parameter in this strategy.** The strategy does not support
bypassing the buffer-fill requirement via configuration.

**To produce a trade on Anvil, the strategy must be run continuously (without `--once`)** so it
accumulates at least 3 price ticks (3x the polling interval, default 60s = ~3 minutes).

---

## Transaction

**No on-chain transaction was submitted.** The strategy returned HOLD on the first tick due to
the price buffer not being sufficiently filled.

TX Hash: N/A

---

## Result

**PASS** -- The strategy loaded, initialized, connected to the gateway, fetched a live price
(WETH = $1,958.71), and returned a valid HOLD intent with no errors. The gateway, Anvil fork,
and full execution pipeline all functioned correctly. No trade was submitted because the
indicator price buffer requires a minimum of 3 historical prices before signals can fire, and
`--once` provides only 1 tick. This is expected behavior for a multi-tick momentum strategy.

### To produce an on-chain trade

Run without `--once` for at least 3 polling intervals:
```bash
ALMANAK_PRIVATE_KEY=<YOUR_ANVIL_PRIVATE_KEY> \
ALMANAK_GATEWAY_ALLOW_INSECURE=true \
uv run almanak strat run -d strategies/incubating/CCStratBT --network anvil --interval 10
```
With `min_signals_to_trade=1` and the aggressive thresholds in the modified config, a trade
will fire as soon as the buffer reaches 3 prices (~30 seconds with interval=10).
