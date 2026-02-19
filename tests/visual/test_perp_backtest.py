"""Visual test for perpetual futures strategy backtest.

This test generates visual charts showing perp position behavior during a
30-day backtest on ETH. The charts can be visually inspected by humans
to verify correct signal generation, liquidation price tracking, and execution.

Output:
- tests/visual/output/perp_backtest.png (price chart with entry/exit markers and liquidation line)
- tests/visual/output/perp_backtest_complete.png (full 3-panel visualization)

Charts generated:
- Main chart (perp_backtest.png):
  - ETH price line
  - Entry markers (green up arrow for long, red down arrow for short)
  - Exit markers (corresponding close markers)
  - Liquidation price line (dashed red)
  - SMA line for signal reference

- Complete chart (perp_backtest_complete.png):
  - Top panel: Price chart with entry/exit markers and liquidation lines
  - Middle panel: Cumulative funding payments over time
  - Bottom panel: Margin utilization % and leverage over time

Usage:
    pytest tests/visual/test_perp_backtest.py -v -s
"""

import math
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from almanak.framework.backtesting.pnl.calculators.liquidation import LiquidationCalculator
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

# =============================================================================
# Output Configuration
# =============================================================================

OUTPUT_DIR = Path(__file__).parent / "output"


# =============================================================================
# Mock Data Provider with Price Movement Suitable for Perp Strategy
# =============================================================================


class PerpTestDataProvider:
    """Data provider that generates price data suitable for perp strategy testing.

    Creates synthetic price data with trending movements that cross the SMA
    to trigger long/short signals for visual verification.
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
        """Generate synthetic price series suitable for SMA crossover strategy.

        Creates price movement that includes:
        - Uptrends (price > SMA, triggers long signals)
        - Downtrends (price < SMA, triggers short signals)
        - Volatility for realistic market conditions
        """
        # 30 days * 24 hours = 720 hourly data points
        num_hours = 720

        for i in range(num_hours):
            # Create distinct phases with trending behavior:
            # Phase 1 (days 0-7): Initial uptrend (long opportunity)
            # Phase 2 (days 7-14): Reversal and downtrend (short opportunity)
            # Phase 3 (days 14-21): Recovery uptrend (long opportunity)
            # Phase 4 (days 21-28): Consolidation with volatility
            # Phase 5 (days 28-30): Final trend

            day = i / 24  # Fractional day

            if day < 7:
                # Phase 1: Uptrend - price rises above base
                progress = day / 7
                trend = 0.15 * progress  # Rise 15% over 7 days
                price = float(self._base_eth_price) * (1 + trend)
            elif day < 14:
                # Phase 2: Downtrend - price falls below base
                progress = (day - 7) / 7
                # Start from +15%, end at -10%
                trend = 0.15 - 0.25 * progress
                price = float(self._base_eth_price) * (1 + trend)
            elif day < 21:
                # Phase 3: Recovery uptrend
                progress = (day - 14) / 7
                # Start from -10%, rise to +20%
                trend = -0.10 + 0.30 * progress
                price = float(self._base_eth_price) * (1 + trend)
            elif day < 28:
                # Phase 4: Consolidation with volatility
                progress = (day - 21) / 7
                base_trend = 0.20 - 0.10 * progress  # Gradual decline from +20% to +10%
                # Add sinusoidal volatility
                volatility = 0.05 * math.sin(progress * math.pi * 6)
                price = float(self._base_eth_price) * (1 + base_trend + volatility)
            else:
                # Phase 5: Final trend (slight upward)
                progress = (day - 28) / 2
                trend = 0.10 + 0.05 * progress
                price = float(self._base_eth_price) * (1 + trend)

            # Add small noise for realism (deterministic based on index)
            noise = 15 * math.sin(i * 0.7) + 10 * math.cos(i * 1.3)
            self._prices[i] = Decimal(str(price + noise))

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
                    high=price * Decimal("1.002"),
                    low=price * Decimal("0.998"),
                    close=price,
                    volume=Decimal("5000000"),
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
        return "perp_test_mock"

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
# SMA Calculator
# =============================================================================


def calculate_sma(prices: list[Decimal], period: int) -> Decimal | None:
    """Calculate Simple Moving Average.

    Args:
        prices: List of price values (most recent last)
        period: Number of periods for SMA

    Returns:
        SMA value or None if not enough data
    """
    if len(prices) < period:
        return None
    return sum(prices[-period:]) / Decimal(period)


# =============================================================================
# Perp Strategy - SMA Crossover
# =============================================================================


@dataclass
class MockPerpOpenIntent:
    """Mock perp open intent for SMA crossover strategy."""

    intent_type: str
    token: str
    collateral_usd: Decimal
    leverage: Decimal
    protocol: str = "gmx_v2"

    @classmethod
    def long(
        cls, token: str, collateral_usd: Decimal, leverage: Decimal
    ) -> "MockPerpOpenIntent":
        return cls(
            intent_type="PERP_OPEN_LONG",
            token=token,
            collateral_usd=collateral_usd,
            leverage=leverage,
        )

    @classmethod
    def short(
        cls, token: str, collateral_usd: Decimal, leverage: Decimal
    ) -> "MockPerpOpenIntent":
        return cls(
            intent_type="PERP_OPEN_SHORT",
            token=token,
            collateral_usd=collateral_usd,
            leverage=leverage,
        )


@dataclass
class MockPerpCloseIntent:
    """Mock perp close intent."""

    intent_type: str = "PERP_CLOSE"
    position_id: str = ""


@dataclass
class MockHoldIntent:
    """Mock hold intent (no action)."""

    intent_type: str = "HOLD"
    reason: str = "Waiting"


@dataclass
class PerpTradeSignal:
    """Record of a perp trade signal generated by the strategy."""

    timestamp: datetime
    signal_type: str  # "LONG_OPEN", "LONG_CLOSE", "SHORT_OPEN", "SHORT_CLOSE"
    price: Decimal
    sma: Decimal
    leverage: Decimal
    liquidation_price: Decimal | None = None


class SMACrossoverPerpStrategy:
    """SMA crossover strategy for perpetual futures.

    This strategy demonstrates basic perp trading:
    - Go LONG when price crosses above SMA
    - Go SHORT when price crosses below SMA
    - Close positions when signal reverses

    Tracks signals and liquidation prices for visualization.
    """

    def __init__(
        self,
        sma_period: int = 24,  # 24-hour SMA
        leverage: Decimal = Decimal("5"),
        collateral_usd: Decimal = Decimal("1000"),
        maintenance_margin: Decimal = Decimal("0.05"),
    ):
        """Initialize SMA crossover perp strategy.

        Args:
            sma_period: Number of periods for SMA calculation
            leverage: Leverage multiplier for positions
            collateral_usd: Collateral amount per position
            maintenance_margin: Maintenance margin ratio for liquidation calc
        """
        self._sma_period = sma_period
        self._leverage = leverage
        self._collateral_usd = collateral_usd
        self._maintenance_margin = maintenance_margin

        # Price history for SMA calculation
        self._price_history: list[Decimal] = []

        # Track position state
        self._current_position: str | None = None  # "LONG", "SHORT", or None
        self._entry_price: Decimal | None = None

        # Track signals for visualization
        self.signals: list[PerpTradeSignal] = []
        self._current_timestamp: datetime | None = None

        # Calculators
        self._liquidation_calculator = LiquidationCalculator()

    @property
    def strategy_id(self) -> str:
        return "sma_perp_visual_test_strategy"

    def _calculate_liquidation_price(
        self, entry_price: Decimal, is_long: bool
    ) -> Decimal:
        """Calculate liquidation price for a position."""
        return self._liquidation_calculator.calculate_liquidation_price(
            entry_price=entry_price,
            leverage=self._leverage,
            maintenance_margin=self._maintenance_margin,
            is_long=is_long,
        )

    def decide(
        self, market: Any
    ) -> MockPerpOpenIntent | MockPerpCloseIntent | MockHoldIntent:
        """Decide whether to open/close perp position based on SMA crossover.

        Args:
            market: MarketSnapshot with price() method and timestamp

        Returns:
            PerpOpenIntent, PerpCloseIntent, or HoldIntent
        """
        # Extract price and timestamp from market snapshot
        try:
            eth_price = market.price("WETH")
        except (ValueError, AttributeError):
            try:
                eth_price = market.price("ETH")
            except (ValueError, AttributeError):
                return MockHoldIntent(reason="No ETH price available")

        self._current_timestamp = getattr(market, "timestamp", None) or getattr(
            market, "_timestamp", None
        )
        if self._current_timestamp is None:
            return MockHoldIntent(reason="No timestamp")

        if eth_price is None:
            return MockHoldIntent(reason="No ETH price")

        self._price_history.append(eth_price)

        # Keep only necessary history
        max_history = self._sma_period + 50
        if len(self._price_history) > max_history:
            self._price_history = self._price_history[-max_history:]

        # Calculate SMA
        sma = calculate_sma(self._price_history, self._sma_period)
        if sma is None:
            return MockHoldIntent(reason="Not enough data for SMA")

        # Trading logic based on price vs SMA
        price_above_sma = eth_price > sma
        price_below_sma = eth_price < sma

        # If we have a position, check for close signal
        if self._current_position == "LONG" and price_below_sma:
            # Close long - price crossed below SMA
            self._current_position = None
            entry = self._entry_price
            self._entry_price = None

            self.signals.append(
                PerpTradeSignal(
                    timestamp=self._current_timestamp,
                    signal_type="LONG_CLOSE",
                    price=eth_price,
                    sma=sma,
                    leverage=self._leverage,
                    liquidation_price=self._calculate_liquidation_price(
                        entry or eth_price, is_long=True
                    ),
                )
            )
            return MockPerpCloseIntent()

        elif self._current_position == "SHORT" and price_above_sma:
            # Close short - price crossed above SMA
            self._current_position = None
            entry = self._entry_price
            self._entry_price = None

            self.signals.append(
                PerpTradeSignal(
                    timestamp=self._current_timestamp,
                    signal_type="SHORT_CLOSE",
                    price=eth_price,
                    sma=sma,
                    leverage=self._leverage,
                    liquidation_price=self._calculate_liquidation_price(
                        entry or eth_price, is_long=False
                    ),
                )
            )
            return MockPerpCloseIntent()

        # If no position, check for open signal
        if self._current_position is None:
            if price_above_sma:
                # Open long - price above SMA
                self._current_position = "LONG"
                self._entry_price = eth_price
                liq_price = self._calculate_liquidation_price(eth_price, is_long=True)

                self.signals.append(
                    PerpTradeSignal(
                        timestamp=self._current_timestamp,
                        signal_type="LONG_OPEN",
                        price=eth_price,
                        sma=sma,
                        leverage=self._leverage,
                        liquidation_price=liq_price,
                    )
                )
                return MockPerpOpenIntent.long(
                    token="ETH",
                    collateral_usd=self._collateral_usd,
                    leverage=self._leverage,
                )

            elif price_below_sma:
                # Open short - price below SMA
                self._current_position = "SHORT"
                self._entry_price = eth_price
                liq_price = self._calculate_liquidation_price(eth_price, is_long=False)

                self.signals.append(
                    PerpTradeSignal(
                        timestamp=self._current_timestamp,
                        signal_type="SHORT_OPEN",
                        price=eth_price,
                        sma=sma,
                        leverage=self._leverage,
                        liquidation_price=liq_price,
                    )
                )
                return MockPerpOpenIntent.short(
                    token="ETH",
                    collateral_usd=self._collateral_usd,
                    leverage=self._leverage,
                )

        return MockHoldIntent(reason="No signal")


# =============================================================================
# Chart Generation
# =============================================================================


def generate_perp_backtest_chart(
    data_provider: PerpTestDataProvider,
    strategy: SMACrossoverPerpStrategy,
    start_time: datetime,
    output_path: Path,
) -> bool:
    """Generate the perp backtest visualization chart.

    Creates a figure showing:
    - ETH price line
    - SMA line
    - Entry markers (green up for long, red down for short)
    - Exit markers (corresponding close markers)
    - Liquidation price lines (dashed, per position)

    Args:
        data_provider: Provider with price data
        strategy: Strategy with recorded signals
        start_time: Start time of the backtest
        output_path: Path to save the PNG file

    Returns:
        True if chart generated successfully, False otherwise
    """
    try:
        import matplotlib.patches as mpatches
        import matplotlib.pyplot as plt
        from matplotlib.dates import DateFormatter
    except ImportError:
        print("matplotlib not installed. Run: uv add matplotlib")
        return False

    # Prepare data
    num_hours = 720  # 30 days
    timestamps = [start_time + timedelta(hours=i) for i in range(num_hours)]
    prices = [float(data_provider.get_price_at_index(i)) for i in range(num_hours)]

    # Calculate SMA for each point
    sma_period = 24
    sma_values: list[float | None] = [None] * (sma_period - 1)
    for i in range(sma_period - 1, num_hours):
        sma_sum = sum(prices[i - sma_period + 1 : i + 1])
        sma_values.append(sma_sum / sma_period)

    # Create figure
    fig, ax = plt.subplots(figsize=(16, 10))
    fig.suptitle(
        "Perpetual Futures Strategy Backtest - ETH (30 Days)\nSMA Crossover: Long when Price > SMA, Short when Price < SMA",
        fontsize=14,
        fontweight="bold",
    )

    # Plot price line
    ax.plot(
        timestamps,
        prices,
        linewidth=2,
        color="#2196F3",
        label="ETH Price",
        zorder=3,
    )

    # Plot SMA line
    sma_timestamps = timestamps[sma_period - 1 :]
    sma_plot_values = [v for v in sma_values[sma_period - 1 :] if v is not None]
    ax.plot(
        sma_timestamps,
        sma_plot_values,
        linewidth=1.5,
        color="#FF9800",
        linestyle="--",
        label=f"SMA({sma_period})",
        zorder=2,
    )

    # Add signal markers and liquidation lines
    long_opens = [s for s in strategy.signals if s.signal_type == "LONG_OPEN"]
    long_closes = [s for s in strategy.signals if s.signal_type == "LONG_CLOSE"]
    short_opens = [s for s in strategy.signals if s.signal_type == "SHORT_OPEN"]
    short_closes = [s for s in strategy.signals if s.signal_type == "SHORT_CLOSE"]

    # Plot long entry markers (green up arrow)
    if long_opens:
        ax.scatter(
            [s.timestamp for s in long_opens],
            [float(s.price) for s in long_opens],
            marker="^",
            s=200,
            c="green",
            label=f"Long Open ({len(long_opens)})",
            zorder=5,
            edgecolors="darkgreen",
            linewidths=2,
        )

    # Plot long close markers (green square)
    if long_closes:
        ax.scatter(
            [s.timestamp for s in long_closes],
            [float(s.price) for s in long_closes],
            marker="s",
            s=150,
            c="lightgreen",
            label=f"Long Close ({len(long_closes)})",
            zorder=5,
            edgecolors="green",
            linewidths=2,
        )

    # Plot short entry markers (red down arrow)
    if short_opens:
        ax.scatter(
            [s.timestamp for s in short_opens],
            [float(s.price) for s in short_opens],
            marker="v",
            s=200,
            c="red",
            label=f"Short Open ({len(short_opens)})",
            zorder=5,
            edgecolors="darkred",
            linewidths=2,
        )

    # Plot short close markers (red square)
    if short_closes:
        ax.scatter(
            [s.timestamp for s in short_closes],
            [float(s.price) for s in short_closes],
            marker="s",
            s=150,
            c="salmon",
            label=f"Short Close ({len(short_closes)})",
            zorder=5,
            edgecolors="red",
            linewidths=2,
        )

    # Draw liquidation price lines for each position period
    # Connect open to close with liquidation price
    all_signals = sorted(strategy.signals, key=lambda s: s.timestamp)

    for i, signal in enumerate(all_signals):
        if signal.signal_type in ("LONG_OPEN", "SHORT_OPEN") and signal.liquidation_price:
            # Find the corresponding close
            close_time = None
            for j in range(i + 1, len(all_signals)):
                next_signal = all_signals[j]
                if signal.signal_type == "LONG_OPEN" and next_signal.signal_type == "LONG_CLOSE":
                    close_time = next_signal.timestamp
                    break
                elif signal.signal_type == "SHORT_OPEN" and next_signal.signal_type == "SHORT_CLOSE":
                    close_time = next_signal.timestamp
                    break

            if close_time is None:
                close_time = timestamps[-1]

            # Draw liquidation line for this position period
            liq_price = float(signal.liquidation_price)
            line_color = "darkred" if signal.signal_type == "LONG_OPEN" else "darkblue"
            ax.hlines(
                y=liq_price,
                xmin=signal.timestamp,
                xmax=close_time,
                colors=line_color,
                linestyles=":",
                linewidths=1.5,
                alpha=0.7,
                zorder=1,
            )

    # Add legend entry for liquidation lines
    liq_long_patch = mpatches.Patch(
        color="darkred", alpha=0.7, linestyle=":", label="Liq Price (Long)"
    )
    liq_short_patch = mpatches.Patch(
        color="darkblue", alpha=0.7, linestyle=":", label="Liq Price (Short)"
    )

    # Statistics annotation
    total_longs = len(long_opens)
    total_shorts = len(short_opens)
    stats_text = (
        f"Total Trades: {total_longs + total_shorts}\n"
        f"Long Trades: {total_longs}\n"
        f"Short Trades: {total_shorts}\n"
        f"Leverage: {strategy._leverage}x"
    )
    ax.text(
        0.02,
        0.98,
        stats_text,
        transform=ax.transAxes,
        fontsize=11,
        verticalalignment="top",
        bbox={"boxstyle": "round", "facecolor": "wheat", "alpha": 0.8},
    )

    # Add legend
    handles, labels = ax.get_legend_handles_labels()
    handles.extend([liq_long_patch, liq_short_patch])
    ax.legend(handles=handles, loc="upper right", fontsize=10)

    # Formatting
    ax.set_xlabel("Date", fontsize=12)
    ax.set_ylabel("ETH Price (USD)", fontsize=12)
    ax.grid(True, alpha=0.3)

    # Format x-axis dates
    ax.xaxis.set_major_formatter(DateFormatter("%m/%d"))
    plt.xticks(rotation=45, ha="right")

    # Set y-axis limits with padding
    min_price = min(prices)
    max_price = max(prices)
    price_range = max_price - min_price
    ax.set_ylim(min_price - price_range * 0.15, max_price + price_range * 0.1)

    ax.set_title(
        "ETH Price with Trade Signals (5x Leverage)",
        fontsize=12,
    )

    plt.tight_layout()

    # Save
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)

    print(f"\nChart saved to: {output_path}")
    return True


# =============================================================================
# Perp Metrics Calculation
# =============================================================================


@dataclass
class PerpMetricsPoint:
    """A point in the perp metrics time series."""

    timestamp: datetime
    price: Decimal
    cumulative_funding_usd: Decimal
    margin_utilization_pct: Decimal  # 0-100 scale
    leverage: Decimal
    position_type: str | None  # "LONG", "SHORT", or None
    position_value_usd: Decimal


def calculate_perp_metrics_over_time(
    data_provider: PerpTestDataProvider,
    strategy: SMACrossoverPerpStrategy,
    start_time: datetime,
    collateral_usd: Decimal = Decimal("1000"),
    total_capital_usd: Decimal = Decimal("10000"),
    funding_rate: Decimal = Decimal("0.0001"),  # 0.01% per hour
) -> list[PerpMetricsPoint]:
    """Calculate perp metrics (funding, margin utilization, leverage) over time.

    Args:
        data_provider: Provider with price data
        strategy: Strategy with recorded signals
        start_time: Start time of the backtest
        collateral_usd: Collateral per position
        total_capital_usd: Total portfolio capital
        funding_rate: Hourly funding rate (default 0.01%)

    Returns:
        List of PerpMetricsPoint with metrics at each hour
    """
    metrics: list[PerpMetricsPoint] = []

    num_hours = 720  # 30 days
    cumulative_funding = Decimal("0")

    # Sort signals by timestamp
    all_signals = sorted(strategy.signals, key=lambda s: s.timestamp)

    # Build a mapping of time -> position state
    position_periods: list[tuple[datetime, datetime, str, Decimal]] = []

    for i, signal in enumerate(all_signals):
        if signal.signal_type == "LONG_OPEN":
            # Find corresponding close
            close_time = None
            for j in range(i + 1, len(all_signals)):
                if all_signals[j].signal_type == "LONG_CLOSE":
                    close_time = all_signals[j].timestamp
                    break
            if close_time is None:
                close_time = start_time + timedelta(hours=num_hours)
            position_periods.append(
                (signal.timestamp, close_time, "LONG", signal.leverage)
            )
        elif signal.signal_type == "SHORT_OPEN":
            # Find corresponding close
            close_time = None
            for j in range(i + 1, len(all_signals)):
                if all_signals[j].signal_type == "SHORT_CLOSE":
                    close_time = all_signals[j].timestamp
                    break
            if close_time is None:
                close_time = start_time + timedelta(hours=num_hours)
            position_periods.append(
                (signal.timestamp, close_time, "SHORT", signal.leverage)
            )

    def get_position_at_time(
        ts: datetime,
    ) -> tuple[str | None, Decimal]:
        """Get the position type and leverage at a given time."""
        for open_time, close_time, pos_type, leverage in position_periods:
            if open_time <= ts < close_time:
                return pos_type, leverage
        return None, Decimal("0")

    for i in range(num_hours):
        timestamp = start_time + timedelta(hours=i)
        current_price = data_provider.get_price_at_index(i)

        # Determine position state at this time
        position_type, leverage = get_position_at_time(timestamp)

        # Calculate position value and margin utilization
        if position_type is not None:
            position_value = collateral_usd * leverage
            margin_used = collateral_usd
            margin_utilization_pct = (margin_used / total_capital_usd) * Decimal("100")

            # Calculate funding payment for this hour
            # Longs pay when funding rate is positive, shorts receive
            hourly_funding = position_value * funding_rate
            if position_type == "LONG":
                cumulative_funding -= hourly_funding  # Longs pay
            else:
                cumulative_funding += hourly_funding  # Shorts receive
        else:
            position_value = Decimal("0")
            margin_utilization_pct = Decimal("0")
            leverage = Decimal("0")

        metrics.append(
            PerpMetricsPoint(
                timestamp=timestamp,
                price=current_price,
                cumulative_funding_usd=cumulative_funding,
                margin_utilization_pct=margin_utilization_pct,
                leverage=leverage,
                position_type=position_type,
                position_value_usd=position_value,
            )
        )

    return metrics


def generate_perp_complete_chart(
    data_provider: PerpTestDataProvider,
    strategy: SMACrossoverPerpStrategy,
    perp_metrics: list[PerpMetricsPoint],
    start_time: datetime,
    output_path: Path,
) -> bool:
    """Generate complete perp backtest visualization with 3 panels.

    Creates a figure showing:
    - Top panel: Price chart with entry/exit markers and liquidation lines
    - Middle panel: Cumulative funding payments over time
    - Bottom panel: Margin utilization % and leverage over time

    Args:
        data_provider: Provider with price data
        strategy: Strategy with recorded signals
        perp_metrics: Pre-calculated perp metrics over time
        start_time: Start time of the backtest
        output_path: Path to save the PNG file

    Returns:
        True if chart generated successfully, False otherwise
    """
    try:
        import matplotlib.patches as mpatches
        import matplotlib.pyplot as plt
        from matplotlib.dates import DateFormatter
    except ImportError:
        print("matplotlib not installed. Run: uv add matplotlib")
        return False

    # Prepare data
    num_hours = 720  # 30 days
    timestamps = [start_time + timedelta(hours=i) for i in range(num_hours)]
    prices = [float(data_provider.get_price_at_index(i)) for i in range(num_hours)]

    # Calculate SMA for each point
    sma_period = 24
    sma_values: list[float | None] = [None] * (sma_period - 1)
    for i in range(sma_period - 1, num_hours):
        sma_sum = sum(prices[i - sma_period + 1 : i + 1])
        sma_values.append(sma_sum / sma_period)

    # Extract metrics for plotting
    funding_usd = [float(m.cumulative_funding_usd) for m in perp_metrics]
    margin_util_pct = [float(m.margin_utilization_pct) for m in perp_metrics]
    leverage_values = [float(m.leverage) for m in perp_metrics]

    # Create figure with 3 subplots
    fig, (ax1, ax2, ax3) = plt.subplots(
        3, 1, figsize=(16, 14), height_ratios=[2, 1.5, 1], sharex=True
    )
    fig.suptitle(
        "Perpetual Futures Strategy Backtest - ETH (30 Days) - Complete Analysis",
        fontsize=14,
        fontweight="bold",
    )

    # ==========================================================================
    # Top Panel: Price Chart with Signals
    # ==========================================================================

    # Plot price line
    ax1.plot(
        timestamps,
        prices,
        linewidth=2,
        color="#2196F3",
        label="ETH Price",
        zorder=3,
    )

    # Plot SMA line
    sma_timestamps = timestamps[sma_period - 1 :]
    sma_plot_values = [v for v in sma_values[sma_period - 1 :] if v is not None]
    ax1.plot(
        sma_timestamps,
        sma_plot_values,
        linewidth=1.5,
        color="#FF9800",
        linestyle="--",
        label=f"SMA({sma_period})",
        zorder=2,
    )

    # Add signal markers
    long_opens = [s for s in strategy.signals if s.signal_type == "LONG_OPEN"]
    long_closes = [s for s in strategy.signals if s.signal_type == "LONG_CLOSE"]
    short_opens = [s for s in strategy.signals if s.signal_type == "SHORT_OPEN"]
    short_closes = [s for s in strategy.signals if s.signal_type == "SHORT_CLOSE"]

    if long_opens:
        ax1.scatter(
            [s.timestamp for s in long_opens],
            [float(s.price) for s in long_opens],
            marker="^",
            s=150,
            c="green",
            label=f"Long Open ({len(long_opens)})",
            zorder=5,
            edgecolors="darkgreen",
            linewidths=1.5,
        )

    if long_closes:
        ax1.scatter(
            [s.timestamp for s in long_closes],
            [float(s.price) for s in long_closes],
            marker="s",
            s=100,
            c="lightgreen",
            label=f"Long Close ({len(long_closes)})",
            zorder=5,
            edgecolors="green",
            linewidths=1.5,
        )

    if short_opens:
        ax1.scatter(
            [s.timestamp for s in short_opens],
            [float(s.price) for s in short_opens],
            marker="v",
            s=150,
            c="red",
            label=f"Short Open ({len(short_opens)})",
            zorder=5,
            edgecolors="darkred",
            linewidths=1.5,
        )

    if short_closes:
        ax1.scatter(
            [s.timestamp for s in short_closes],
            [float(s.price) for s in short_closes],
            marker="s",
            s=100,
            c="salmon",
            label=f"Short Close ({len(short_closes)})",
            zorder=5,
            edgecolors="red",
            linewidths=1.5,
        )

    # Draw liquidation price lines for each position period
    all_signals = sorted(strategy.signals, key=lambda s: s.timestamp)
    for i, signal in enumerate(all_signals):
        if signal.signal_type in ("LONG_OPEN", "SHORT_OPEN") and signal.liquidation_price:
            close_time = None
            for j in range(i + 1, len(all_signals)):
                next_signal = all_signals[j]
                if signal.signal_type == "LONG_OPEN" and next_signal.signal_type == "LONG_CLOSE":
                    close_time = next_signal.timestamp
                    break
                elif signal.signal_type == "SHORT_OPEN" and next_signal.signal_type == "SHORT_CLOSE":
                    close_time = next_signal.timestamp
                    break

            if close_time is None:
                close_time = timestamps[-1]

            liq_price = float(signal.liquidation_price)
            line_color = "darkred" if signal.signal_type == "LONG_OPEN" else "darkblue"
            ax1.hlines(
                y=liq_price,
                xmin=signal.timestamp,
                xmax=close_time,
                colors=line_color,
                linestyles=":",
                linewidths=1.5,
                alpha=0.7,
                zorder=1,
            )

    # Statistics annotation
    stats_text = (
        f"Total Trades: {len(long_opens) + len(short_opens)}\n"
        f"Long: {len(long_opens)} | Short: {len(short_opens)}\n"
        f"Leverage: {strategy._leverage}x"
    )
    ax1.text(
        0.02,
        0.98,
        stats_text,
        transform=ax1.transAxes,
        fontsize=10,
        verticalalignment="top",
        bbox={"boxstyle": "round", "facecolor": "wheat", "alpha": 0.8},
    )

    # Legend
    handles, labels = ax1.get_legend_handles_labels()
    liq_long_patch = mpatches.Patch(color="darkred", alpha=0.7, label="Liq Price (Long)")
    liq_short_patch = mpatches.Patch(color="darkblue", alpha=0.7, label="Liq Price (Short)")
    handles.extend([liq_long_patch, liq_short_patch])
    ax1.legend(handles=handles, loc="upper right", fontsize=9)

    ax1.set_ylabel("ETH Price (USD)", fontsize=11)
    ax1.grid(True, alpha=0.3)
    ax1.set_title("Price Chart with Trade Signals", fontsize=12)

    # Set y-axis limits
    min_price = min(prices)
    max_price = max(prices)
    price_range = max_price - min_price
    ax1.set_ylim(min_price - price_range * 0.15, max_price + price_range * 0.1)

    # ==========================================================================
    # Middle Panel: Cumulative Funding Payments
    # ==========================================================================

    # Plot cumulative funding
    ax2.plot(
        timestamps,
        funding_usd,
        linewidth=2,
        color="#9C27B0",
        label="Cumulative Funding",
    )

    # Fill positive area green, negative area red
    positive_mask = [f >= 0 for f in funding_usd]
    ax2.fill_between(
        timestamps,
        0,
        funding_usd,
        where=positive_mask,
        alpha=0.3,
        color="#4CAF50",
        interpolate=True,
    )
    ax2.fill_between(
        timestamps,
        0,
        funding_usd,
        where=[not p for p in positive_mask],
        alpha=0.3,
        color="#F44336",
        interpolate=True,
    )

    # Zero line
    ax2.axhline(y=0, color="gray", linestyle="-", linewidth=1, alpha=0.7)

    # Final funding annotation
    final_funding = funding_usd[-1]
    funding_color = "#4CAF50" if final_funding >= 0 else "#F44336"
    funding_text = f"Final Funding: ${final_funding:,.2f}"
    ax2.text(
        0.02,
        0.98 if final_funding >= 0 else 0.05,
        funding_text,
        transform=ax2.transAxes,
        fontsize=11,
        fontweight="bold",
        verticalalignment="top" if final_funding >= 0 else "bottom",
        color=funding_color,
        bbox={"boxstyle": "round", "facecolor": "white", "alpha": 0.8},
    )

    ax2.set_ylabel("Funding (USD)", fontsize=11)
    ax2.legend(loc="upper right", fontsize=9)
    ax2.grid(True, alpha=0.3)
    ax2.set_title("Cumulative Funding Payments (+ = received, - = paid)", fontsize=12)

    # ==========================================================================
    # Bottom Panel: Margin Utilization and Leverage
    # ==========================================================================

    # Create twin axis for leverage
    ax3_twin = ax3.twinx()

    # Plot margin utilization
    ax3.plot(
        timestamps,
        margin_util_pct,
        linewidth=2,
        color="#FF5722",
        label="Margin Utilization %",
    )
    ax3.fill_between(timestamps, 0, margin_util_pct, alpha=0.2, color="#FF5722")

    # Plot leverage on twin axis
    ax3_twin.plot(
        timestamps,
        leverage_values,
        linewidth=2,
        color="#2196F3",
        linestyle="--",
        label="Leverage",
    )

    # Annotations
    max_margin = max(margin_util_pct)
    nonzero_leverage = [lev for lev in leverage_values if lev > 0]
    avg_leverage = sum(leverage_values) / len(nonzero_leverage) if nonzero_leverage else 0
    metrics_text = (
        f"Max Margin Util: {max_margin:.1f}%\n"
        f"Avg Leverage: {avg_leverage:.1f}x (when in position)"
    )
    ax3.text(
        0.02,
        0.98,
        metrics_text,
        transform=ax3.transAxes,
        fontsize=10,
        verticalalignment="top",
        bbox={"boxstyle": "round", "facecolor": "lightyellow", "alpha": 0.8},
    )

    ax3.set_ylabel("Margin Utilization (%)", fontsize=11, color="#FF5722")
    ax3_twin.set_ylabel("Leverage (x)", fontsize=11, color="#2196F3")
    ax3.set_xlabel("Date", fontsize=11)

    # Combine legends
    lines1, labels1 = ax3.get_legend_handles_labels()
    lines2, labels2 = ax3_twin.get_legend_handles_labels()
    ax3.legend(lines1 + lines2, labels1 + labels2, loc="upper right", fontsize=9)

    ax3.grid(True, alpha=0.3)
    ax3.set_title("Margin Utilization & Leverage Over Time", fontsize=12)

    # Set axis limits
    ax3.set_ylim(0, max(max_margin * 1.2, 15))
    ax3_twin.set_ylim(0, max(max(leverage_values) * 1.2, 6))

    # Format x-axis dates
    ax3.xaxis.set_major_formatter(DateFormatter("%m/%d"))
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


class TestPerpBacktestVisual:
    """Visual tests for perpetual futures strategy backtest."""

    @pytest.mark.asyncio
    async def test_perp_strategy_30_day_backtest(self) -> None:
        """Run 30-day perp backtest and generate visualization.

        This test:
        1. Creates a mock data provider with trending price patterns
        2. Runs an SMA crossover perp strategy (long when price > SMA, short when price < SMA)
        3. Generates a chart showing price with entry/exit markers and liquidation lines
        4. Saves the chart to tests/visual/output/perp_backtest.png

        The chart can be visually inspected to verify:
        - Long entries occur when price crosses above SMA
        - Short entries occur when price crosses below SMA
        - Liquidation price lines are correctly positioned
        - Entry/exit markers align with signal logic
        """
        # Setup
        start_time = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
        end_time = start_time + timedelta(days=30)

        data_provider = PerpTestDataProvider(
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

        strategy = SMACrossoverPerpStrategy(
            sma_period=24,
            leverage=Decimal("5"),
            collateral_usd=Decimal("1000"),
            maintenance_margin=Decimal("0.05"),
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

        # Count signals by type
        long_opens = [s for s in strategy.signals if s.signal_type == "LONG_OPEN"]
        long_closes = [s for s in strategy.signals if s.signal_type == "LONG_CLOSE"]
        short_opens = [s for s in strategy.signals if s.signal_type == "SHORT_OPEN"]
        short_closes = [s for s in strategy.signals if s.signal_type == "SHORT_CLOSE"]

        print("\n=== Perp Strategy Backtest Results ===")
        print("Duration: 30 days (hourly intervals)")
        print(f"Leverage: {strategy._leverage}x")
        print(f"Collateral per trade: ${strategy._collateral_usd}")
        print(f"\nTotal signals: {len(strategy.signals)}")
        print(f"  Long opens: {len(long_opens)}")
        print(f"  Long closes: {len(long_closes)}")
        print(f"  Short opens: {len(short_opens)}")
        print(f"  Short closes: {len(short_closes)}")
        print(f"\nTotal trades: {result.metrics.total_trades}")
        print(f"Final portfolio value: ${result.final_capital_usd:.2f}")

        # Verify we got some signals
        assert len(strategy.signals) > 0, "Expected at least one trading signal"

        # Verify liquidation prices are calculated for all open signals
        for signal in strategy.signals:
            if signal.signal_type in ("LONG_OPEN", "SHORT_OPEN"):
                assert signal.liquidation_price is not None, (
                    f"Liquidation price should be set for {signal.signal_type}"
                )
                # Verify liquidation price direction
                if signal.signal_type == "LONG_OPEN":
                    assert signal.liquidation_price < signal.price, (
                        "Long liquidation price should be below entry price"
                    )
                else:
                    assert signal.liquidation_price > signal.price, (
                        "Short liquidation price should be above entry price"
                    )

        # Generate visualization
        output_path = OUTPUT_DIR / "perp_backtest.png"
        chart_generated = generate_perp_backtest_chart(
            data_provider=data_provider,
            strategy=strategy,
            start_time=start_time,
            output_path=output_path,
        )

        assert chart_generated, "Failed to generate chart"
        assert output_path.exists(), f"Chart file not found: {output_path}"

        print(f"\nVisualization saved to: {output_path}")
        print("=====================================\n")

        # Verify signal logic
        for signal in long_opens:
            assert signal.price > signal.sma, (
                f"Long open price {signal.price} should be above SMA {signal.sma}"
            )

        for signal in short_opens:
            assert signal.price < signal.sma, (
                f"Short open price {signal.price} should be below SMA {signal.sma}"
            )

    @pytest.mark.asyncio
    async def test_perp_strategy_complete_visualization(self) -> None:
        """Generate complete perp backtest visualization with funding and margin.

        This test creates a 3-panel visualization:
        1. Top panel: Price chart with entry/exit markers and liquidation lines
        2. Middle panel: Cumulative funding payments over time
        3. Bottom panel: Margin utilization % and leverage over time

        The chart can be visually inspected to verify:
        - Funding accumulates during position holding periods
        - Longs pay funding (negative) when funding rate is positive
        - Shorts receive funding (positive) when funding rate is positive
        - Margin utilization shows collateral locked as % of portfolio
        - Leverage reflects the configured multiplier when in position
        """
        # Setup
        start_time = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
        end_time = start_time + timedelta(days=30)
        collateral_usd = Decimal("1000")
        initial_capital = Decimal("10000")
        leverage = Decimal("5")

        data_provider = PerpTestDataProvider(
            start_time=start_time,
            base_eth_price=Decimal("3000"),
        )

        config = PnLBacktestConfig(
            start_time=start_time,
            end_time=end_time,
            interval_seconds=3600,
            initial_capital_usd=initial_capital,
            tokens=["WETH", "USDC"],
            include_gas_costs=True,
        )

        strategy = SMACrossoverPerpStrategy(
            sma_period=24,
            leverage=leverage,
            collateral_usd=collateral_usd,
            maintenance_margin=Decimal("0.05"),
        )

        backtester = PnLBacktester(
            data_provider=data_provider,
            fee_models={"default": DefaultFeeModel()},
            slippage_models={"default": DefaultSlippageModel()},
        )

        # Run backtest to populate strategy signals
        result = await backtester.backtest(strategy, config)

        assert result.success, f"Backtest failed: {result.error}"

        # Calculate perp metrics over time
        perp_metrics = calculate_perp_metrics_over_time(
            data_provider=data_provider,
            strategy=strategy,
            start_time=start_time,
            collateral_usd=collateral_usd,
            total_capital_usd=initial_capital,
            funding_rate=Decimal("0.0001"),  # 0.01% per hour
        )

        # Print metrics summary
        print("\n=== Perp Complete Backtest Analysis ===")
        print("Duration: 30 days (hourly intervals)")
        print(f"Leverage: {leverage}x")
        print(f"Collateral per position: ${collateral_usd}")
        print(f"Total capital: ${initial_capital}")

        # Count signals
        long_opens = [s for s in strategy.signals if s.signal_type == "LONG_OPEN"]
        short_opens = [s for s in strategy.signals if s.signal_type == "SHORT_OPEN"]
        print(f"\nTotal trades: {len(long_opens) + len(short_opens)}")
        print(f"  Long trades: {len(long_opens)}")
        print(f"  Short trades: {len(short_opens)}")

        # Calculate time in position
        in_position_hours = sum(1 for m in perp_metrics if m.position_type is not None)
        in_position_pct = (in_position_hours / len(perp_metrics)) * 100
        print(f"Time in position: {in_position_pct:.1f}%")

        # Final metrics
        final_metrics = perp_metrics[-1]
        print(f"\nFinal Cumulative Funding: ${final_metrics.cumulative_funding_usd:.2f}")

        # Find max margin utilization
        max_margin_util = max(m.margin_utilization_pct for m in perp_metrics)
        print(f"Max Margin Utilization: {max_margin_util:.1f}%")

        # Calculate total funding paid vs received
        total_funding_paid = sum(
            abs(perp_metrics[i].cumulative_funding_usd - perp_metrics[i - 1].cumulative_funding_usd)
            for i in range(1, len(perp_metrics))
            if perp_metrics[i].cumulative_funding_usd < perp_metrics[i - 1].cumulative_funding_usd
        )
        total_funding_received = sum(
            perp_metrics[i].cumulative_funding_usd - perp_metrics[i - 1].cumulative_funding_usd
            for i in range(1, len(perp_metrics))
            if perp_metrics[i].cumulative_funding_usd > perp_metrics[i - 1].cumulative_funding_usd
        )
        print(f"Total funding paid: ${total_funding_paid:.2f}")
        print(f"Total funding received: ${total_funding_received:.2f}")

        # Generate complete visualization
        output_path = OUTPUT_DIR / "perp_backtest_complete.png"
        chart_generated = generate_perp_complete_chart(
            data_provider=data_provider,
            strategy=strategy,
            perp_metrics=perp_metrics,
            start_time=start_time,
            output_path=output_path,
        )

        assert chart_generated, "Failed to generate complete chart"
        assert output_path.exists(), f"Chart file not found: {output_path}"

        print(f"\nComplete visualization saved to: {output_path}")
        print("=====================================\n")

        # Verify metrics calculations
        assert len(perp_metrics) == 720, "Expected 720 hourly data points"

        # Verify margin utilization is only non-zero when in position
        for m in perp_metrics:
            if m.position_type is None:
                assert m.margin_utilization_pct == Decimal("0"), (
                    f"Margin utilization should be 0 when not in position: {m.margin_utilization_pct}"
                )
                assert m.leverage == Decimal("0"), (
                    f"Leverage should be 0 when not in position: {m.leverage}"
                )
            else:
                assert m.margin_utilization_pct > Decimal("0"), (
                    f"Margin utilization should be > 0 when in position: {m.margin_utilization_pct}"
                )
                assert m.leverage == leverage, (
                    f"Leverage should be {leverage}x when in position: {m.leverage}"
                )

        # Verify funding direction based on position type
        # Note: Funding only changes when in position
        for i in range(1, len(perp_metrics)):
            prev = perp_metrics[i - 1]
            curr = perp_metrics[i]

            if prev.position_type == "LONG":
                # Longs pay funding (cumulative should decrease)
                assert curr.cumulative_funding_usd <= prev.cumulative_funding_usd, (
                    f"Long position should pay funding: prev={prev.cumulative_funding_usd}, "
                    f"curr={curr.cumulative_funding_usd}"
                )
            elif prev.position_type == "SHORT":
                # Shorts receive funding (cumulative should increase)
                assert curr.cumulative_funding_usd >= prev.cumulative_funding_usd, (
                    f"Short position should receive funding: prev={prev.cumulative_funding_usd}, "
                    f"curr={curr.cumulative_funding_usd}"
                )
