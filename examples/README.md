# Almanak SDK Backtest Examples

This directory contains comprehensive backtest examples demonstrating the Almanak SDK's
backtesting capabilities for institutional-grade strategy validation.

## Overview

Each example runs a different strategy type through the PnL backtesting engine, generating:
- Console output with detailed metrics and verification formulas
- Publication-quality visualizations saved to `output/`

## Prerequisites

```bash
# Install dependencies
cd /path/to/almanak-sdk
make install-dev

# matplotlib is required for chart generation
uv add matplotlib
```

No API keys are needed - all examples use synthetic data for reproducibility.

## Running the Examples

```bash
# From the almanak-sdk root directory:

# Run TA Strategy (RSI mean reversion)
python examples/backtest_ta_strategy.py

# Run LP Strategy (concentrated liquidity)
python examples/backtest_lp_strategy.py

# Run Looping Strategy (leveraged yield)
python examples/backtest_looping_strategy.py
```

## Example 1: TA Strategy (RSI Mean Reversion)

**File:** `backtest_ta_strategy.py`

**Strategy Logic:**
- Buy when RSI drops below 30 (oversold condition)
- Sell when RSI rises above 70 (overbought condition)
- Fixed trade size of $500 per trade

**Configuration:**
| Parameter | Value |
|-----------|-------|
| Initial Capital | $10,000 |
| Trade Size | $500 |
| RSI Period | 14 |
| Oversold Threshold | 30 |
| Overbought Threshold | 70 |
| Backtest Period | 30 days |

**Output Charts:**
- `output/ta_strategy_complete.png` - 3-panel visualization:
  - Equity curve with buy/hold benchmark
  - Trade PnL distribution histogram
  - Price with RSI indicator and signals

**Metrics Calculated:**
- Total Return %
- Sharpe Ratio (annualized)
- Sortino Ratio (annualized)
- Max Drawdown %
- Win Rate
- Profit Factor
- Average Trade PnL

**Verification:**
Each metric includes the formula used for calculation, enabling manual verification.

---

## Example 2: LP Strategy (Concentrated Liquidity)

**File:** `backtest_lp_strategy.py`

**Strategy Logic:**
- Open Uniswap V3 LP position with concentrated range
- Track fee accrual (only when price is in range)
- Track impermanent loss as price moves
- Compare net LP PnL vs HODL baseline

**Configuration:**
| Parameter | Value |
|-----------|-------|
| Initial Position | 1 ETH + 3000 USDC |
| Entry Price | $3,000 |
| LP Range | $2,800 - $3,200 |
| Fee Tier | 0.3% |
| Backtest Period | 30 days |

**Output Charts:**
- `output/lp_strategy_complete.png` - 3-panel visualization:
  - Price with LP range boundaries (green/red shading for in/out of range)
  - Fee accrual vs Impermanent Loss over time
  - Net LP PnL (Fees - IL)

**Metrics Calculated:**
- Total Fees Earned (USD)
- Impermanent Loss (USD)
- Net LP PnL (Fees - IL)
- Time in Range %
- Fee APY
- IL-to-Fees Ratio
- HODL Comparison

**Key Concepts:**

*Impermanent Loss (IL):*
IL occurs when the price of assets in an LP position diverges from the entry price.
For concentrated liquidity, IL is amplified compared to full-range positions.

*Fee Accrual:*
Fees only accumulate when price is within the LP range. Time in range directly
impacts fee income.

*HODL Baseline:*
Compares LP strategy returns against simply holding the initial assets without
providing liquidity.

---

## Example 3: Looping Strategy (Leveraged Yield)

**File:** `backtest_looping_strategy.py`

**Strategy Logic:**
1. Supply wstETH as collateral
2. Borrow USDC at 75% LTV
3. Swap borrowed USDC back to wstETH
4. Repeat to achieve ~3x leverage
5. Monitor health factor, deleverage if HF < 1.5

**Configuration:**
| Parameter | Value |
|-----------|-------|
| Initial Capital | $10,000 |
| Target Loops | 3 |
| Target LTV | 75% |
| Liquidation LTV | 85% |
| Min Health Factor | 1.5 |
| Supply APY | 4% |
| Borrow APY | 6% |
| Backtest Period | 30 days |

**Output Charts:**
- `output/looping_strategy_complete.png` - 3-panel visualization:
  - Leverage ratio over time
  - Health factor with warning/liquidation thresholds
  - Interest earned vs paid

**Metrics Calculated:**
- Final Leverage Ratio
- Minimum Health Factor
- Health Factor Warnings
- Liquidations Count
- Total Supply Interest Earned
- Total Borrow Interest Paid
- Net Yield (USD)
- Yield APY
- Max Drawdown %

**Key Concepts:**

*Health Factor (HF):*
```
HF = (collateral_value * liquidation_threshold) / borrowed_value
```
- HF > 1.5: Safe zone
- HF 1.0-1.5: Warning zone (may deleverage)
- HF < 1.0: Liquidation

*Leverage Ratio:*
```
Leverage = total_exposure / equity
         = collateral / (collateral - borrowed)
```

*Net Interest Spread:*
- Supply APY: 4% (earning)
- Borrow APY: 6% (paying)
- Net spread is negative (-2%)
- Profit comes from collateral price appreciation, not interest

---

## Interpreting Results

### Risk Metrics

| Metric | Good Value | Description |
|--------|------------|-------------|
| Sharpe Ratio | > 1.0 | Risk-adjusted return (higher = better) |
| Sortino Ratio | > 1.5 | Downside-adjusted return (higher = better) |
| Max Drawdown | < 20% | Worst peak-to-trough decline |
| Win Rate | > 50% | Percentage of profitable trades |
| Profit Factor | > 1.5 | Gross profit / Gross loss |

### LP-Specific Metrics

| Metric | Interpretation |
|--------|----------------|
| Time in Range | Higher = more fee accrual |
| IL-to-Fees Ratio | < 1.0 means fees exceed IL |
| Fee APY | Annualized return from fees only |

### Leverage Metrics

| Metric | Safe Range |
|--------|------------|
| Health Factor | > 1.5 (ideally > 2.0) |
| Leverage Ratio | < 4x for most protocols |
| Yield APY | Depends on interest spread and price action |

---

## Synthetic Data Providers

Each example uses deterministic synthetic price data for reproducibility:

**RSITriggerDataProvider:**
- Creates price patterns that trigger RSI oversold/overbought signals
- Phases: Decline -> Recovery -> Rally -> Decline -> Consolidation

**LPRangeDataProvider:**
- Creates price movement across LP range boundaries
- Phases: In-range -> Exit above -> Re-enter -> Exit below -> Re-enter

**LendingDataProvider:**
- Creates volatility to test health factor monitoring
- Phases: Stable -> Sharp decline -> Recovery -> Decline -> Recovery

---

## Verification Guide

### Manual Sharpe Ratio Verification

```python
# Collect daily returns from equity curve
daily_returns = [(day_n - day_n-1) / day_n-1 for each day]

# Calculate Sharpe
mean_return = average(daily_returns)
std_dev = standard_deviation(daily_returns)
risk_free_daily = 0.05 / 365  # 5% annual

sharpe = (mean_return - risk_free_daily) / std_dev * sqrt(365)
```

### Manual Max Drawdown Verification

```python
# Track running peak
peak = initial_value
max_drawdown = 0

for value in equity_curve:
    if value > peak:
        peak = value
    drawdown = (value - peak) / peak
    if drawdown < max_drawdown:
        max_drawdown = drawdown

# Result is negative percentage (e.g., -15% = 15% drawdown)
```

### Manual Win Rate Verification

```python
# Count profitable vs total trades
wins = count(trades where sell_price > buy_price)
total = count(all completed trades)
win_rate = wins / total * 100
```

---

## Extending the Examples

### Adding New Strategies

1. Create a new strategy class implementing `decide(market)` method
2. Use appropriate data provider or create custom one
3. Configure `PnLBacktestConfig` with correct parameters
4. Run through `PnLBacktester`

### Customizing Data Providers

```python
from examples.common.data_providers import SyntheticDataProvider

class MyCustomProvider(SyntheticDataProvider):
    def _generate_prices(self) -> None:
        # Generate custom price patterns
        for i in range(self.num_hours):
            # Your price generation logic
            self._prices[i] = calculated_price
```

### Adding New Metrics

See `examples/common/chart_helpers.py` for metric calculation patterns.

---

## Output Directory

All generated charts and reports are saved to `examples/output/`:

```
examples/output/
    ta_strategy_complete.png      # RSI strategy 3-panel chart
    ta_strategy_metrics.png       # Metrics summary table
    lp_strategy_complete.png      # LP strategy 3-panel chart
    looping_strategy_complete.png # Leverage strategy 3-panel chart
```

---

## Troubleshooting

**"matplotlib not installed":**
```bash
uv add matplotlib
```

**Import errors:**
Run from the almanak-sdk root directory, not from examples/:
```bash
cd /path/to/almanak-sdk
python examples/backtest_ta_strategy.py
```

**Charts not generating:**
Check that the `examples/output/` directory exists and is writable.
