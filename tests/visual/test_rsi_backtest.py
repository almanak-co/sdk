"""Visual test for RSI strategy backtest.

This test generates visual charts showing RSI strategy behavior during a
30-day backtest on ETH/USDC. The charts can be visually inspected by humans
to verify correct signal generation and execution.

Output:
- tests/visual/output/rsi_backtest.png (price + RSI panels)
- tests/visual/output/rsi_backtest_complete.png (full 3-panel visualization)

Charts generated:
- Basic chart (rsi_backtest.png):
  - Top panel: ETH price with buy/sell markers
  - Bottom panel: RSI indicator with overbought/oversold zones

- Complete chart (rsi_backtest_complete.png):
  - Top panel: Equity curve with buy/hold benchmark and drawdown highlighting
  - Middle panel: PnL histogram showing trade distribution
  - Bottom panel: RSI indicator with signal markers

Usage:
    pytest tests/visual/test_rsi_backtest.py -v -s
"""

import math
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
from almanak.framework.backtesting.pnl.data_provider import (
    OHLCV,
    HistoricalDataConfig,
    MarketState,
)
from almanak.framework.backtesting.pnl.engine import (
    DefaultFeeModel,
    DefaultSlippageModel,
    PnLBacktester,
)
from almanak.framework.data.indicators.rsi import RSICalculator

# =============================================================================
# Output Configuration
# =============================================================================

OUTPUT_DIR = Path(__file__).parent / "output"


# =============================================================================
# Mock Data Provider with Realistic Price Movement
# =============================================================================


class RSITestDataProvider:
    """Data provider that generates price data suitable for RSI strategy testing.

    Creates synthetic price data with deliberate oversold and overbought
    conditions to trigger RSI signals for visual verification.
    """

    def __init__(
        self,
        start_time: datetime,
        base_eth_price: Decimal = Decimal("3000"),
    ):
        """Initialize the data provider.

        Args:
            start_time: Start timestamp for the series
            base_eth_price: Starting ETH price
        """
        self._start_time = start_time
        self._base_eth_price = base_eth_price
        self._prices: dict[int, Decimal] = {}
        self._generate_prices()

    def _generate_prices(self) -> None:
        """Generate synthetic price series with RSI-triggering patterns.

        Creates price movement that includes:
        - Periods of decline (to trigger oversold RSI < 30)
        - Periods of increase (to trigger overbought RSI > 70)
        - Recovery and consolidation phases
        """
        # 30 days * 24 hours = 720 hourly data points
        num_hours = 720

        for i in range(num_hours):
            # Create distinct phases to trigger RSI signals:
            # Phase 1 (days 0-5): Initial decline -> oversold
            # Phase 2 (days 5-10): Recovery from oversold
            # Phase 3 (days 10-18): Rally -> overbought
            # Phase 4 (days 18-23): Decline from overbought
            # Phase 5 (days 23-30): Consolidation with small moves

            day = i // 24
            hour_in_day = i % 24

            if day < 5:
                # Steady decline: ~20% drop over 5 days
                decline_pct = -0.04 * (day + hour_in_day / 24)
                price_factor = Decimal(str(1.0 + decline_pct))
            elif day < 10:
                # Recovery from oversold: bounce back 15%
                day_in_phase = day - 5 + hour_in_day / 24
                recovery_pct = -0.20 + 0.03 * day_in_phase
                price_factor = Decimal(str(1.0 + recovery_pct))
            elif day < 18:
                # Strong rally: push to overbought (~25% gain from base)
                day_in_phase = day - 10 + hour_in_day / 24
                rally_pct = -0.05 + 0.0375 * day_in_phase
                price_factor = Decimal(str(1.0 + rally_pct))
            elif day < 23:
                # Decline from overbought
                day_in_phase = day - 18 + hour_in_day / 24
                decline_pct = 0.25 - 0.05 * day_in_phase
                price_factor = Decimal(str(1.0 + decline_pct))
            else:
                # Consolidation around base price with small waves
                day_in_phase = day - 23 + hour_in_day / 24
                wave = 0.02 * math.sin(day_in_phase * math.pi / 2)
                price_factor = Decimal(str(1.0 + wave))

            # Add small noise for realism (deterministic based on index)
            noise = Decimal(str(0.001 * math.sin(i * 0.5)))
            self._prices[i] = self._base_eth_price * (price_factor + noise)

    def get_price_at_index(self, index: int) -> Decimal:
        """Get the ETH price at a given hourly index."""
        return self._prices.get(index, self._base_eth_price)

    async def get_price(self, token: str, timestamp: datetime) -> Decimal:
        """Get price for token at specific timestamp."""
        token = token.upper()
        if token in ("USDC", "USDT"):
            return Decimal("1")

        delta = timestamp - self._start_time
        index = int(delta.total_seconds() / 3600)
        return self.get_price_at_index(index)

    async def get_ohlcv(
        self,
        token: str,
        start: datetime,
        end: datetime,
        interval_seconds: int = 3600,
    ) -> list[OHLCV]:
        """Get OHLCV data for token."""
        result = []
        current = start
        while current <= end:
            price = await self.get_price(token, current)
            result.append(
                OHLCV(
                    timestamp=current,
                    open=price,
                    high=price * Decimal("1.001"),
                    low=price * Decimal("0.999"),
                    close=price,
                    volume=Decimal("1000000"),
                )
            )
            current += timedelta(seconds=interval_seconds)
        return result

    async def iterate(
        self, config: HistoricalDataConfig
    ) -> AsyncIterator[tuple[datetime, MarketState]]:
        """Iterate through historical data with mock prices."""
        current = config.start_time
        index = 0
        interval_delta = timedelta(seconds=config.interval_seconds)

        while current <= config.end_time:
            prices = {}
            for token in config.tokens:
                token = token.upper()
                if token in ("USDC", "USDT"):
                    prices[token] = Decimal("1")
                elif token in ("WETH", "ETH"):
                    prices[token] = self.get_price_at_index(index)
                else:
                    prices[token] = Decimal("1")

            market_state = MarketState(
                timestamp=current,
                prices=prices,
                chain=config.chains[0] if config.chains else "arbitrum",
                block_number=15000000 + index * 100,
                gas_price_gwei=Decimal("30"),
            )
            yield current, market_state

            index += 1
            current += interval_delta

    @property
    def provider_name(self) -> str:
        return "rsi_test_mock"

    @property
    def supported_tokens(self) -> list[str]:
        return ["WETH", "ETH", "USDC", "USDT"]

    @property
    def supported_chains(self) -> list[str]:
        return ["arbitrum", "ethereum"]

    @property
    def min_timestamp(self) -> datetime | None:
        return self._start_time

    @property
    def max_timestamp(self) -> datetime | None:
        return self._start_time + timedelta(days=30)


# =============================================================================
# RSI Strategy
# =============================================================================


@dataclass
class MockSwapIntent:
    """Mock swap intent for RSI strategy."""

    intent_type: str = "SWAP"
    from_token: str = "USDC"
    to_token: str = "WETH"
    amount_usd: Decimal = Decimal("1000")
    protocol: str = "uniswap_v3"


@dataclass
class TradeSignal:
    """Record of a trade signal generated by the strategy."""

    timestamp: datetime
    signal_type: str  # "BUY" or "SELL"
    price: Decimal
    rsi: float


class RSIStrategy:
    """Simple RSI strategy: buy when RSI<30, sell when RSI>70.

    This strategy demonstrates classic RSI mean-reversion trading:
    - When RSI drops below 30 (oversold), buy ETH
    - When RSI rises above 70 (overbought), sell ETH

    Tracks signals for visualization.
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

        # Track position state to alternate buy/sell
        self._in_position = False

    @property
    def strategy_id(self) -> str:
        return "rsi_visual_test_strategy"

    def _calculate_rsi(self) -> float | None:
        """Calculate RSI from current price history."""
        if len(self._price_history) < self._rsi_period + 1:
            return None

        return RSICalculator.calculate_rsi_from_prices(
            self._price_history, self._rsi_period
        )

    def decide(self, market: Any) -> MockSwapIntent | None:
        """Decide whether to trade based on RSI.

        Args:
            market: MarketSnapshot with price() method and timestamp

        Returns:
            SwapIntent if signal triggered, None otherwise
        """
        # Extract price and timestamp from market snapshot
        # MarketSnapshot uses .price("TOKEN") method, not .prices dict
        try:
            eth_price = market.price("WETH")
        except (ValueError, AttributeError):
            try:
                eth_price = market.price("ETH")
            except (ValueError, AttributeError):
                return None

        self._current_timestamp = getattr(market, "timestamp", None) or getattr(
            market, "_timestamp", None
        )
        if self._current_timestamp is None:
            return None

        if eth_price is None:
            return None

        self._current_price = eth_price
        self._price_history.append(eth_price)

        # Keep only necessary history
        max_history = self._rsi_period + 50
        if len(self._price_history) > max_history:
            self._price_history = self._price_history[-max_history:]

        # Calculate RSI
        rsi = self._calculate_rsi()
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
                rsi=rsi,
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
                rsi=rsi,
            )
            self.signals.append(signal)
            return MockSwapIntent(
                from_token="WETH",
                to_token="USDC",
                amount_usd=self._trade_amount,
            )

        return None


# =============================================================================
# Chart Generation
# =============================================================================


def generate_rsi_backtest_chart(
    data_provider: RSITestDataProvider,
    strategy: RSIStrategy,
    start_time: datetime,
    output_path: Path,
) -> bool:
    """Generate the RSI backtest visualization chart.

    Creates a two-panel figure:
    - Top panel: ETH price with buy (green) and sell (red) markers
    - Bottom panel: RSI indicator with 30/70 threshold lines

    Args:
        data_provider: Provider with price data
        strategy: Strategy with recorded signals
        start_time: Start time of the backtest
        output_path: Path to save the PNG file

    Returns:
        True if chart generated successfully, False otherwise
    """
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        print("matplotlib not installed. Run: uv add matplotlib")
        return False

    # Prepare data
    num_hours = 720  # 30 days
    timestamps = [start_time + timedelta(hours=i) for i in range(num_hours)]
    prices = [float(data_provider.get_price_at_index(i)) for i in range(num_hours)]

    # Calculate RSI for each point (starting from period+1)
    rsi_period = 14
    rsi_values: list[float | None] = [None] * rsi_period
    price_decimals = [Decimal(str(p)) for p in prices]

    for i in range(rsi_period, num_hours):
        try:
            rsi = RSICalculator.calculate_rsi_from_prices(
                price_decimals[: i + 1], rsi_period
            )
            rsi_values.append(rsi)
        except Exception:
            rsi_values.append(None)

    # Create figure with two subplots
    fig, (ax1, ax2) = plt.subplots(
        2, 1, figsize=(14, 10), height_ratios=[2, 1], sharex=True
    )
    fig.suptitle(
        "RSI Strategy Backtest - ETH/USDC (30 Days)",
        fontsize=14,
        fontweight="bold",
    )

    # Top panel: Price chart
    ax1.plot(timestamps, prices, linewidth=1.5, color="#2196F3", label="ETH Price")
    ax1.fill_between(timestamps, prices, alpha=0.1, color="#2196F3")

    # Add buy/sell markers
    buy_times = [s.timestamp for s in strategy.signals if s.signal_type == "BUY"]
    buy_prices = [float(s.price) for s in strategy.signals if s.signal_type == "BUY"]
    sell_times = [s.timestamp for s in strategy.signals if s.signal_type == "SELL"]
    sell_prices = [float(s.price) for s in strategy.signals if s.signal_type == "SELL"]

    if buy_times:
        ax1.scatter(
            buy_times,
            buy_prices,
            marker="^",
            s=150,
            c="green",
            label=f"Buy (RSI<30) - {len(buy_times)} trades",
            zorder=5,
            edgecolors="darkgreen",
            linewidths=1.5,
        )
    if sell_times:
        ax1.scatter(
            sell_times,
            sell_prices,
            marker="v",
            s=150,
            c="red",
            label=f"Sell (RSI>70) - {len(sell_times)} trades",
            zorder=5,
            edgecolors="darkred",
            linewidths=1.5,
        )

    ax1.set_ylabel("Price (USD)", fontsize=11)
    ax1.legend(loc="upper left", fontsize=10)
    ax1.grid(True, alpha=0.3)
    ax1.set_title("Price Chart with Trading Signals", fontsize=12)

    # Bottom panel: RSI chart
    rsi_timestamps = timestamps[rsi_period:]
    rsi_plot_values = [v for v in rsi_values[rsi_period:] if v is not None]
    rsi_plot_times = rsi_timestamps[: len(rsi_plot_values)]

    ax2.plot(
        rsi_plot_times, rsi_plot_values, linewidth=1.5, color="#9C27B0", label="RSI(14)"
    )

    # Add threshold lines
    ax2.axhline(y=70, color="red", linestyle="--", linewidth=1, label="Overbought (70)")
    ax2.axhline(
        y=30, color="green", linestyle="--", linewidth=1, label="Oversold (30)"
    )
    ax2.axhline(y=50, color="gray", linestyle=":", linewidth=0.5, alpha=0.5)

    # Fill zones
    ax2.fill_between(rsi_plot_times, 70, 100, alpha=0.1, color="red")
    ax2.fill_between(rsi_plot_times, 0, 30, alpha=0.1, color="green")

    # Add RSI signal markers
    for signal in strategy.signals:
        # Find the RSI value at signal time
        ax2.scatter(
            [signal.timestamp],
            [signal.rsi],
            marker="o" if signal.signal_type == "BUY" else "s",
            s=80,
            c="green" if signal.signal_type == "BUY" else "red",
            zorder=5,
            edgecolors="black",
            linewidths=1,
        )

    ax2.set_ylabel("RSI", fontsize=11)
    ax2.set_xlabel("Date", fontsize=11)
    ax2.set_ylim(0, 100)
    ax2.legend(loc="upper right", fontsize=9)
    ax2.grid(True, alpha=0.3)
    ax2.set_title("RSI Indicator with Signal Points", fontsize=12)

    # Format x-axis
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()

    # Save
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)

    print(f"\nChart saved to: {output_path}")
    return True


@dataclass
class EquityPoint:
    """A point on the equity curve."""

    timestamp: datetime
    portfolio_value: float
    buy_hold_value: float


def calculate_equity_curve(
    data_provider: RSITestDataProvider,
    strategy: RSIStrategy,
    start_time: datetime,
    initial_capital: float = 10000.0,
    trade_amount: float = 500.0,
) -> list[EquityPoint]:
    """Calculate equity curve from strategy signals.

    Args:
        data_provider: Provider with price data
        strategy: Strategy with recorded signals
        start_time: Start time of the backtest
        initial_capital: Starting capital in USD
        trade_amount: Amount traded per signal

    Returns:
        List of EquityPoint with portfolio and buy-hold values
    """
    num_hours = 720  # 30 days
    equity_curve: list[EquityPoint] = []

    # Strategy state
    cash = initial_capital
    eth_held = 0.0
    start_price = float(data_provider.get_price_at_index(0))

    # Buy and hold: invest 50% at start
    buy_hold_eth = (initial_capital * 0.5) / start_price
    buy_hold_cash = initial_capital * 0.5

    # Build signal lookup
    signal_times = {s.timestamp: s for s in strategy.signals}

    for i in range(num_hours):
        timestamp = start_time + timedelta(hours=i)
        price = float(data_provider.get_price_at_index(i))

        # Check for signals at this timestamp
        if timestamp in signal_times:
            signal = signal_times[timestamp]
            if signal.signal_type == "BUY" and cash >= trade_amount:
                # Buy ETH
                eth_bought = trade_amount / price
                cash -= trade_amount
                eth_held += eth_bought
            elif signal.signal_type == "SELL" and eth_held > 0:
                # Sell ETH worth trade_amount or all held
                eth_to_sell = min(eth_held, trade_amount / price)
                cash += eth_to_sell * price
                eth_held -= eth_to_sell

        # Calculate current values
        portfolio_value = cash + (eth_held * price)
        buy_hold_value = buy_hold_cash + (buy_hold_eth * price)

        equity_curve.append(
            EquityPoint(
                timestamp=timestamp,
                portfolio_value=portfolio_value,
                buy_hold_value=buy_hold_value,
            )
        )

    return equity_curve


def calculate_drawdown(equity_curve: list[EquityPoint]) -> list[float]:
    """Calculate drawdown percentage from equity curve.

    Args:
        equity_curve: List of equity points

    Returns:
        List of drawdown percentages (0 to -100 scale)
    """
    drawdowns: list[float] = []
    peak = equity_curve[0].portfolio_value if equity_curve else 0.0

    for point in equity_curve:
        if point.portfolio_value > peak:
            peak = point.portfolio_value
        drawdown = ((point.portfolio_value - peak) / peak) * 100 if peak > 0 else 0.0
        drawdowns.append(drawdown)

    return drawdowns


def calculate_trade_pnls(
    strategy: RSIStrategy,
    data_provider: RSITestDataProvider,
    start_time: datetime,
    trade_amount: float = 500.0,
) -> list[float]:
    """Calculate PnL for each completed trade (buy followed by sell).

    Args:
        strategy: Strategy with recorded signals
        data_provider: Provider with price data
        start_time: Start time of the backtest
        trade_amount: Amount traded per signal

    Returns:
        List of PnL values for each completed trade
    """
    pnls: list[float] = []
    buy_price: float | None = None

    for signal in strategy.signals:
        if signal.signal_type == "BUY":
            buy_price = float(signal.price)
        elif signal.signal_type == "SELL" and buy_price is not None:
            sell_price = float(signal.price)
            # PnL = (sell - buy) / buy * 100 (percentage)
            pnl_pct = ((sell_price - buy_price) / buy_price) * 100
            pnls.append(pnl_pct)
            buy_price = None

    return pnls


def generate_complete_rsi_chart(
    data_provider: RSITestDataProvider,
    strategy: RSIStrategy,
    start_time: datetime,
    output_path: Path,
    initial_capital: float = 10000.0,
    trade_amount: float = 500.0,
) -> bool:
    """Generate the complete RSI backtest visualization with 3 panels.

    Creates a three-panel figure:
    - Top panel: Equity curve with buy/hold benchmark and drawdown highlighting
    - Middle panel: PnL histogram showing trade distribution
    - Bottom panel: RSI indicator with signal markers

    Args:
        data_provider: Provider with price data
        strategy: Strategy with recorded signals
        start_time: Start time of the backtest
        output_path: Path to save the PNG file
        initial_capital: Starting capital in USD
        trade_amount: Amount per trade in USD

    Returns:
        True if chart generated successfully, False otherwise
    """
    try:
        import matplotlib.pyplot as plt
        import numpy as np
    except ImportError:
        print("matplotlib not installed. Run: uv add matplotlib")
        return False

    # Calculate data
    equity_curve = calculate_equity_curve(
        data_provider, strategy, start_time, initial_capital, trade_amount
    )
    drawdowns = calculate_drawdown(equity_curve)
    trade_pnls = calculate_trade_pnls(strategy, data_provider, start_time, trade_amount)

    # Prepare RSI data
    num_hours = 720
    timestamps = [start_time + timedelta(hours=i) for i in range(num_hours)]
    prices = [float(data_provider.get_price_at_index(i)) for i in range(num_hours)]
    rsi_period = 14
    rsi_values: list[float | None] = [None] * rsi_period
    price_decimals = [Decimal(str(p)) for p in prices]

    for i in range(rsi_period, num_hours):
        try:
            rsi = RSICalculator.calculate_rsi_from_prices(
                price_decimals[: i + 1], rsi_period
            )
            rsi_values.append(rsi)
        except Exception:
            rsi_values.append(None)

    # Create figure with three subplots
    fig, (ax1, ax2, ax3) = plt.subplots(
        3, 1, figsize=(14, 12), height_ratios=[2, 1, 1]
    )
    fig.suptitle(
        "RSI Strategy Complete Backtest Analysis - ETH/USDC (30 Days)",
        fontsize=14,
        fontweight="bold",
    )

    # =========================================================================
    # Top panel: Equity curve with drawdown
    # =========================================================================
    eq_timestamps = [p.timestamp for p in equity_curve]
    portfolio_values = [p.portfolio_value for p in equity_curve]
    buy_hold_values = [p.buy_hold_value for p in equity_curve]

    # Plot equity curves
    ax1.plot(
        eq_timestamps,
        portfolio_values,
        linewidth=2,
        color="#2196F3",
        label="RSI Strategy",
    )
    ax1.plot(
        eq_timestamps,
        buy_hold_values,
        linewidth=1.5,
        color="#FF9800",
        linestyle="--",
        label="Buy & Hold (50%)",
    )

    # Fill area between strategy and benchmark
    ax1.fill_between(
        eq_timestamps,
        portfolio_values,
        buy_hold_values,
        where=np.array(portfolio_values) >= np.array(buy_hold_values),
        interpolate=True,
        alpha=0.2,
        color="green",
        label="Outperformance",
    )
    ax1.fill_between(
        eq_timestamps,
        portfolio_values,
        buy_hold_values,
        where=np.array(portfolio_values) < np.array(buy_hold_values),
        interpolate=True,
        alpha=0.2,
        color="red",
        label="Underperformance",
    )

    # Create secondary axis for drawdown
    ax1_dd = ax1.twinx()
    ax1_dd.fill_between(
        eq_timestamps,
        drawdowns,
        0,
        alpha=0.3,
        color="red",
        label="Drawdown",
    )
    ax1_dd.set_ylabel("Drawdown (%)", fontsize=10, color="red")
    ax1_dd.tick_params(axis="y", labelcolor="red")
    ax1_dd.set_ylim(-50, 10)  # Drawdown scale

    # Add trade markers on equity curve
    for signal in strategy.signals:
        # Find the equity value at signal time
        idx = int((signal.timestamp - start_time).total_seconds() / 3600)
        if 0 <= idx < len(portfolio_values):
            marker = "^" if signal.signal_type == "BUY" else "v"
            color = "green" if signal.signal_type == "BUY" else "red"
            ax1.scatter(
                [signal.timestamp],
                [portfolio_values[idx]],
                marker=marker,
                s=100,
                c=color,
                zorder=5,
                edgecolors="black",
                linewidths=1,
            )

    ax1.set_ylabel("Portfolio Value ($)", fontsize=11)
    ax1.legend(loc="upper left", fontsize=9)
    ax1.grid(True, alpha=0.3)
    ax1.set_title("Equity Curve vs Buy & Hold Benchmark", fontsize=12)

    # Add final return annotation
    final_strategy = portfolio_values[-1]
    final_buyhold = buy_hold_values[-1]
    strategy_return = ((final_strategy - initial_capital) / initial_capital) * 100
    buyhold_return = ((final_buyhold - initial_capital) / initial_capital) * 100

    ax1.annotate(
        f"Strategy: {strategy_return:+.1f}%\nBuy&Hold: {buyhold_return:+.1f}%",
        xy=(0.98, 0.05),
        xycoords="axes fraction",
        fontsize=10,
        ha="right",
        va="bottom",
        bbox={"boxstyle": "round", "facecolor": "wheat", "alpha": 0.8},
    )

    # =========================================================================
    # Middle panel: PnL histogram
    # =========================================================================
    if trade_pnls:
        # Color bars based on positive/negative
        colors = ["green" if pnl >= 0 else "red" for pnl in trade_pnls]
        ax2.bar(range(len(trade_pnls)), trade_pnls, color=colors, edgecolor="black", linewidth=0.5)

        # Add zero line
        ax2.axhline(y=0, color="black", linewidth=1)

        # Add statistics
        avg_pnl = sum(trade_pnls) / len(trade_pnls)
        winning = [p for p in trade_pnls if p >= 0]
        win_rate = len(winning) / len(trade_pnls) * 100 if trade_pnls else 0

        stats_text = f"Trades: {len(trade_pnls)} | Win Rate: {win_rate:.0f}% | Avg: {avg_pnl:+.2f}%"
        ax2.set_title(f"Trade PnL Distribution - {stats_text}", fontsize=12)

        ax2.set_xlabel("Trade #", fontsize=10)
        ax2.set_ylabel("PnL (%)", fontsize=10)
        ax2.grid(True, alpha=0.3, axis="y")
    else:
        ax2.text(
            0.5,
            0.5,
            "No completed trades to display",
            ha="center",
            va="center",
            fontsize=12,
            transform=ax2.transAxes,
        )
        ax2.set_title("Trade PnL Distribution", fontsize=12)

    # =========================================================================
    # Bottom panel: RSI with signals
    # =========================================================================
    rsi_timestamps = timestamps[rsi_period:]
    rsi_plot_values = [v for v in rsi_values[rsi_period:] if v is not None]
    rsi_plot_times = rsi_timestamps[: len(rsi_plot_values)]

    ax3.plot(
        rsi_plot_times, rsi_plot_values, linewidth=1.5, color="#9C27B0", label="RSI(14)"
    )

    # Add threshold lines
    ax3.axhline(y=70, color="red", linestyle="--", linewidth=1, label="Overbought (70)")
    ax3.axhline(y=30, color="green", linestyle="--", linewidth=1, label="Oversold (30)")
    ax3.axhline(y=50, color="gray", linestyle=":", linewidth=0.5, alpha=0.5)

    # Fill zones
    ax3.fill_between(rsi_plot_times, 70, 100, alpha=0.1, color="red")
    ax3.fill_between(rsi_plot_times, 0, 30, alpha=0.1, color="green")

    # Add RSI signal markers
    for signal in strategy.signals:
        ax3.scatter(
            [signal.timestamp],
            [signal.rsi],
            marker="o" if signal.signal_type == "BUY" else "s",
            s=80,
            c="green" if signal.signal_type == "BUY" else "red",
            zorder=5,
            edgecolors="black",
            linewidths=1,
        )

    ax3.set_ylabel("RSI", fontsize=11)
    ax3.set_xlabel("Date", fontsize=11)
    ax3.set_ylim(0, 100)
    ax3.legend(loc="upper right", fontsize=9)
    ax3.grid(True, alpha=0.3)
    ax3.set_title("RSI Indicator with Signal Points", fontsize=12)

    # Format x-axis
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()

    # Save
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)

    print(f"\nComplete chart saved to: {output_path}")
    return True


# =============================================================================
# Test
# =============================================================================


class TestRSIBacktestVisual:
    """Visual tests for RSI strategy backtest."""

    @pytest.mark.asyncio
    async def test_rsi_strategy_30_day_backtest(self) -> None:
        """Run 30-day RSI backtest and generate visualization.

        This test:
        1. Creates a mock data provider with RSI-triggering price patterns
        2. Runs an RSI strategy (buy RSI<30, sell RSI>70)
        3. Generates a two-panel chart (price + RSI)
        4. Saves the chart to tests/visual/output/rsi_backtest.png

        The chart can be visually inspected to verify:
        - Buy signals occur in oversold zones (RSI < 30)
        - Sell signals occur in overbought zones (RSI > 70)
        - Price and RSI panels are aligned
        """
        # Setup
        start_time = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
        end_time = start_time + timedelta(days=30)

        data_provider = RSITestDataProvider(
            start_time=start_time,
            base_eth_price=Decimal("3000"),
        )

        config = PnLBacktestConfig(
            start_time=start_time,
            end_time=end_time,
            interval_seconds=3600,  # 1 hour intervals
            initial_capital_usd=Decimal("10000"),
            tokens=["WETH", "USDC"],
            include_gas_costs=True,
        )

        strategy = RSIStrategy(
            rsi_period=14,
            oversold_threshold=30.0,
            overbought_threshold=70.0,
            trade_amount_usd=Decimal("500"),
        )

        backtester = PnLBacktester(
            data_provider=data_provider,
            fee_models={"default": DefaultFeeModel()},
            slippage_models={"default": DefaultSlippageModel()},
        )

        # Run backtest
        result = await backtester.backtest(strategy, config)

        # Verify backtest completed
        assert result.success, f"Backtest failed: {result.error}"
        assert result.metrics is not None

        # Verify we got some signals
        assert len(strategy.signals) > 0, "Expected at least one trading signal"

        # Count signal types
        buy_signals = [s for s in strategy.signals if s.signal_type == "BUY"]
        sell_signals = [s for s in strategy.signals if s.signal_type == "SELL"]

        print("\n=== RSI Strategy Backtest Results ===")
        print("Duration: 30 days (hourly intervals)")
        print(f"Total signals: {len(strategy.signals)}")
        print(f"  Buy signals (RSI<30): {len(buy_signals)}")
        print(f"  Sell signals (RSI>70): {len(sell_signals)}")
        print(f"Total trades: {result.metrics.total_trades}")
        print(f"Final portfolio value: ${result.final_capital_usd:.2f}")
        print(f"Total return: {result.metrics.total_return_pct * 100:.2f}%")

        # Generate visualization
        output_path = OUTPUT_DIR / "rsi_backtest.png"
        chart_generated = generate_rsi_backtest_chart(
            data_provider=data_provider,
            strategy=strategy,
            start_time=start_time,
            output_path=output_path,
        )

        assert chart_generated, "Failed to generate chart"
        assert output_path.exists(), f"Chart file not found: {output_path}"

        print(f"\nVisualization saved to: {output_path}")
        print("=====================================\n")

        # Verify signals are in correct zones
        for signal in buy_signals:
            assert signal.rsi < 30, f"Buy signal RSI {signal.rsi} should be < 30"

        for signal in sell_signals:
            assert signal.rsi > 70, f"Sell signal RSI {signal.rsi} should be > 70"

    @pytest.mark.asyncio
    async def test_rsi_strategy_complete_visualization(self) -> None:
        """Run 30-day RSI backtest and generate complete 3-panel visualization.

        This test:
        1. Creates a mock data provider with RSI-triggering price patterns
        2. Runs an RSI strategy (buy RSI<30, sell RSI>70)
        3. Generates a three-panel chart:
           - Equity curve with buy/hold benchmark and drawdown highlighting
           - PnL histogram showing trade distribution
           - RSI indicator with signal markers
        4. Saves the chart to tests/visual/output/rsi_backtest_complete.png

        The chart can be visually inspected to verify:
        - Equity curve tracks portfolio value over time
        - Buy/hold benchmark provides performance comparison
        - Drawdown highlighting shows risk periods
        - PnL histogram shows trade profitability distribution
        - RSI signals align with trading activity
        """
        # Setup
        start_time = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
        end_time = start_time + timedelta(days=30)
        initial_capital = Decimal("10000")
        trade_amount = Decimal("500")

        data_provider = RSITestDataProvider(
            start_time=start_time,
            base_eth_price=Decimal("3000"),
        )

        config = PnLBacktestConfig(
            start_time=start_time,
            end_time=end_time,
            interval_seconds=3600,  # 1 hour intervals
            initial_capital_usd=initial_capital,
            tokens=["WETH", "USDC"],
            include_gas_costs=True,
        )

        strategy = RSIStrategy(
            rsi_period=14,
            oversold_threshold=30.0,
            overbought_threshold=70.0,
            trade_amount_usd=trade_amount,
        )

        backtester = PnLBacktester(
            data_provider=data_provider,
            fee_models={"default": DefaultFeeModel()},
            slippage_models={"default": DefaultSlippageModel()},
        )

        # Run backtest
        result = await backtester.backtest(strategy, config)

        # Verify backtest completed
        assert result.success, f"Backtest failed: {result.error}"
        assert result.metrics is not None

        # Verify we got some signals
        assert len(strategy.signals) > 0, "Expected at least one trading signal"

        # Count signal types
        buy_signals = [s for s in strategy.signals if s.signal_type == "BUY"]
        sell_signals = [s for s in strategy.signals if s.signal_type == "SELL"]

        print("\n=== RSI Strategy Complete Visualization ===")
        print("Duration: 30 days (hourly intervals)")
        print(f"Total signals: {len(strategy.signals)}")
        print(f"  Buy signals (RSI<30): {len(buy_signals)}")
        print(f"  Sell signals (RSI>70): {len(sell_signals)}")
        print(f"Total trades: {result.metrics.total_trades}")
        print(f"Final portfolio value: ${result.final_capital_usd:.2f}")
        print(f"Total return: {result.metrics.total_return_pct * 100:.2f}%")

        # Calculate equity curve for additional statistics
        equity_curve = calculate_equity_curve(
            data_provider=data_provider,
            strategy=strategy,
            start_time=start_time,
            initial_capital=float(initial_capital),
            trade_amount=float(trade_amount),
        )

        # Calculate drawdown statistics
        drawdowns = calculate_drawdown(equity_curve)
        max_drawdown = min(drawdowns)

        # Calculate trade PnL statistics
        trade_pnls = calculate_trade_pnls(
            strategy=strategy,
            data_provider=data_provider,
            start_time=start_time,
            trade_amount=float(trade_amount),
        )

        print("\n--- Additional Statistics ---")
        print(f"Max drawdown: {max_drawdown:.2f}%")
        if trade_pnls:
            avg_pnl = sum(trade_pnls) / len(trade_pnls)
            winning_trades = len([p for p in trade_pnls if p >= 0])
            win_rate = winning_trades / len(trade_pnls) * 100
            print(f"Completed trades: {len(trade_pnls)}")
            print(f"Win rate: {win_rate:.1f}%")
            print(f"Average PnL: {avg_pnl:+.2f}%")

        # Generate complete visualization
        output_path = OUTPUT_DIR / "rsi_backtest_complete.png"
        chart_generated = generate_complete_rsi_chart(
            data_provider=data_provider,
            strategy=strategy,
            start_time=start_time,
            output_path=output_path,
            initial_capital=float(initial_capital),
            trade_amount=float(trade_amount),
        )

        assert chart_generated, "Failed to generate complete chart"
        assert output_path.exists(), f"Chart file not found: {output_path}"

        print(f"\nComplete visualization saved to: {output_path}")
        print("==========================================\n")
