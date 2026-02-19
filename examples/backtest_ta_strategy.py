#!/usr/bin/env python3
"""Technical Analysis Strategy Backtest Example.

This example demonstrates backtesting an RSI-based mean reversion strategy
using the Almanak SDK's PnL backtesting engine.

Strategy Logic:
- Buy when RSI drops below 30 (oversold condition)
- Sell when RSI rises above 70 (overbought condition)
- Fixed trade size of $500 per trade

Output:
- Console: Metrics summary with verification formulas
- Charts: Complete 3-panel visualization saved to examples/output/

Usage:
    python examples/backtest_ta_strategy.py

Requirements:
    - matplotlib (for chart generation)
    - No API keys needed (uses synthetic data)
"""

import asyncio
import sys
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
from almanak.framework.backtesting.pnl.engine import (
    DefaultFeeModel,
    DefaultSlippageModel,
    PnLBacktester,
)
from almanak.framework.data.indicators.rsi import RSICalculator
from examples.common.chart_helpers import (
    TradeSignal,
    calculate_equity_curve,
    calculate_metrics,
    calculate_trade_pnls,
    generate_complete_chart,
    generate_metrics_table,
    print_metrics_with_verification,
)
from examples.common.data_providers import RSITriggerDataProvider

# =============================================================================
# Configuration
# =============================================================================

OUTPUT_DIR = Path(__file__).parent / "output"

# Strategy parameters
RSI_PERIOD = 14
RSI_OVERSOLD = 30.0
RSI_OVERBOUGHT = 70.0
TRADE_AMOUNT_USD = Decimal("500")
INITIAL_CAPITAL = Decimal("10000")

# Backtest parameters
START_TIME = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
END_TIME = START_TIME + timedelta(days=30)
INTERVAL_SECONDS = 3600  # Hourly


# =============================================================================
# RSI Strategy
# =============================================================================


@dataclass
class MockSwapIntent:
    """Mock swap intent for RSI strategy execution."""

    intent_type: str = "SWAP"
    from_token: str = "USDC"
    to_token: str = "WETH"
    amount_usd: Decimal = Decimal("500")
    protocol: str = "uniswap_v3"


class RSIMeanReversionStrategy:
    """RSI-based mean reversion strategy.

    Classic RSI strategy:
    - Buy when RSI < 30 (oversold) -> expect price to rise
    - Sell when RSI > 70 (overbought) -> expect price to fall

    This demonstrates a simple but effective technical analysis approach
    that's commonly used in quantitative trading.
    """

    def __init__(
        self,
        rsi_period: int = 14,
        oversold_threshold: float = 30.0,
        overbought_threshold: float = 70.0,
        trade_amount_usd: Decimal = Decimal("500"),
    ):
        """Initialize RSI strategy.

        Args:
            rsi_period: Number of periods for RSI calculation
            oversold_threshold: RSI level below which to buy
            overbought_threshold: RSI level above which to sell
            trade_amount_usd: USD amount per trade
        """
        self._rsi_period = rsi_period
        self._oversold = oversold_threshold
        self._overbought = overbought_threshold
        self._trade_amount = trade_amount_usd

        # Price history for RSI calculation
        self._price_history: list[Decimal] = []

        # Track signals for visualization
        self.signals: list[TradeSignal] = []
        self._current_timestamp: datetime | None = None
        self._current_price: Decimal = Decimal("0")

        # Track RSI values for charting
        self.rsi_values: list[float | None] = []

        # Track position state to alternate buy/sell
        self._in_position = False

    @property
    def strategy_id(self) -> str:
        return "rsi_mean_reversion_demo"

    def _calculate_rsi(self) -> float | None:
        """Calculate RSI from current price history."""
        if len(self._price_history) < self._rsi_period + 1:
            return None

        return RSICalculator.calculate_rsi_from_prices(self._price_history, self._rsi_period)

    def decide(self, market: Any) -> MockSwapIntent | None:
        """Decide whether to trade based on RSI.

        Args:
            market: MarketSnapshot with price() method and timestamp

        Returns:
            SwapIntent if signal triggered, None otherwise
        """
        # Extract price and timestamp from market snapshot
        try:
            eth_price = market.price("WETH")
        except (ValueError, AttributeError):
            try:
                eth_price = market.price("ETH")
            except (ValueError, AttributeError):
                self.rsi_values.append(None)
                return None

        self._current_timestamp = getattr(market, "timestamp", None) or getattr(market, "_timestamp", None)
        if self._current_timestamp is None:
            self.rsi_values.append(None)
            return None

        if eth_price is None:
            self.rsi_values.append(None)
            return None

        self._current_price = eth_price
        self._price_history.append(eth_price)

        # Keep only necessary history
        max_history = self._rsi_period + 50
        if len(self._price_history) > max_history:
            self._price_history = self._price_history[-max_history:]

        # Calculate RSI
        rsi = self._calculate_rsi()
        self.rsi_values.append(rsi)

        if rsi is None:
            return None

        # Generate signals
        if rsi < self._oversold and not self._in_position:
            # Oversold - buy signal
            self._in_position = True
            signal = TradeSignal(
                timestamp=self._current_timestamp,
                signal_type="BUY",
                price=eth_price,
                indicator_value=rsi,
            )
            self.signals.append(signal)
            return MockSwapIntent(
                from_token="USDC",
                to_token="WETH",
                amount_usd=self._trade_amount,
            )

        elif rsi > self._overbought and self._in_position:
            # Overbought - sell signal
            self._in_position = False
            signal = TradeSignal(
                timestamp=self._current_timestamp,
                signal_type="SELL",
                price=eth_price,
                indicator_value=rsi,
            )
            self.signals.append(signal)
            return MockSwapIntent(
                from_token="WETH",
                to_token="USDC",
                amount_usd=self._trade_amount,
            )

        return None


# =============================================================================
# Main Execution
# =============================================================================


async def run_backtest() -> None:
    """Run the RSI strategy backtest and generate visualizations."""
    print("\n" + "=" * 70)
    print("       RSI MEAN REVERSION STRATEGY BACKTEST")
    print("=" * 70)
    print("\nConfiguration:")
    print(f"  Period: {START_TIME.date()} to {END_TIME.date()} (30 days)")
    print(f"  Interval: {INTERVAL_SECONDS // 3600} hour(s)")
    print(f"  Initial Capital: ${INITIAL_CAPITAL:,}")
    print(f"  Trade Size: ${TRADE_AMOUNT_USD}")
    print(f"  RSI Period: {RSI_PERIOD}")
    print(f"  Oversold Threshold: {RSI_OVERSOLD}")
    print(f"  Overbought Threshold: {RSI_OVERBOUGHT}")

    # Create data provider with deterministic price patterns
    data_provider = RSITriggerDataProvider(
        start_time=START_TIME,
        base_price=Decimal("3000"),
    )

    # Create strategy
    strategy = RSIMeanReversionStrategy(
        rsi_period=RSI_PERIOD,
        oversold_threshold=RSI_OVERSOLD,
        overbought_threshold=RSI_OVERBOUGHT,
        trade_amount_usd=TRADE_AMOUNT_USD,
    )

    # Configure backtest
    config = PnLBacktestConfig(
        start_time=START_TIME,
        end_time=END_TIME,
        interval_seconds=INTERVAL_SECONDS,
        initial_capital_usd=INITIAL_CAPITAL,
        tokens=["WETH", "USDC"],
        include_gas_costs=True,
    )

    # Create backtester
    backtester = PnLBacktester(
        data_provider=data_provider,
        fee_models={"default": DefaultFeeModel()},
        slippage_models={"default": DefaultSlippageModel()},
    )

    print("\nRunning backtest...")

    # Run backtest
    result = await backtester.backtest(strategy, config)

    # Check result
    if not result.success:
        print(f"Backtest failed: {result.error}")
        return

    print("Backtest completed successfully!")
    print(f"\n  Total trading signals: {len(strategy.signals)}")
    buy_signals = [s for s in strategy.signals if s.signal_type == "BUY"]
    sell_signals = [s for s in strategy.signals if s.signal_type == "SELL"]
    print(f"    Buy signals (RSI < {RSI_OVERSOLD}): {len(buy_signals)}")
    print(f"    Sell signals (RSI > {RSI_OVERBOUGHT}): {len(sell_signals)}")

    # Collect price data for visualization
    prices = [data_provider.get_price_at_index(i) for i in range(720)]

    # Calculate equity curve
    equity_curve = calculate_equity_curve(
        prices=prices,
        signals=strategy.signals,
        start_time=START_TIME,
        initial_capital=float(INITIAL_CAPITAL),
        trade_amount=float(TRADE_AMOUNT_USD),
    )

    # Calculate trade PnLs
    trade_pnls = calculate_trade_pnls(strategy.signals, float(TRADE_AMOUNT_USD))

    # Calculate metrics
    metrics = calculate_metrics(
        equity_curve=equity_curve,
        trade_pnls=trade_pnls,
        initial_capital=float(INITIAL_CAPITAL),
    )

    # Print metrics with verification
    print_metrics_with_verification(
        metrics=metrics,
        equity_curve=equity_curve,
        trade_pnls=trade_pnls,
    )

    # Generate visualization
    print("\nGenerating visualization...")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    chart_path = OUTPUT_DIR / "ta_strategy_complete.png"
    success = generate_complete_chart(
        prices=prices,
        signals=strategy.signals,
        equity_curve=equity_curve,
        indicator_values=strategy.rsi_values,
        start_time=START_TIME,
        output_path=chart_path,
        title="RSI Mean Reversion Strategy - 30 Day Backtest",
        indicator_name="RSI(14)",
        indicator_thresholds=(RSI_OVERSOLD, RSI_OVERBOUGHT),
    )

    if success:
        print(f"\nVisualization saved to: {chart_path}")
    else:
        print("\nFailed to generate visualization (matplotlib may not be installed)")

    # Save metrics table as image
    metrics_path = OUTPUT_DIR / "ta_strategy_metrics.png"
    generate_metrics_table(metrics, output_path=metrics_path)

    # Print verification instructions
    print("\n" + "-" * 70)
    print("VERIFICATION INSTRUCTIONS")
    print("-" * 70)
    print("""
To verify these results:

1. Total Return Calculation:
   - Start with $10,000
   - Each BUY invests $500 in ETH
   - Each SELL converts ETH back to USD
   - Final value should match equity curve end point

2. Sharpe Ratio Calculation:
   - Collect daily returns from equity curve
   - mean = average(daily_returns)
   - std = standard_deviation(daily_returns)
   - Sharpe = (mean - 0.05/365) / std * sqrt(365)

3. Win Rate Calculation:
   - Count profitable trades (sell_price > buy_price)
   - Win Rate = profitable_trades / total_completed_trades * 100

4. Visual Verification:
   - Green markers should appear when RSI drops below 30
   - Red markers should appear when RSI rises above 70
   - Equity curve should show gains during profitable trades
    """)

    print("\n" + "=" * 70)
    print("                    BACKTEST COMPLETE")
    print("=" * 70 + "\n")


def main() -> None:
    """Entry point for the example."""
    asyncio.run(run_backtest())


if __name__ == "__main__":
    main()
