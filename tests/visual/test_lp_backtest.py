"""Visual test for LP strategy backtest.

This test generates visual charts showing LP position behavior during a
30-day backtest on ETH/USDC. The charts can be visually inspected by humans
to verify correct range tracking and fee accrual.

Output:
- tests/visual/output/lp_backtest.png (price chart with range boundaries)
- tests/visual/output/lp_backtest_complete.png (full 3-panel visualization)

Charts generated:
- Main chart (lp_backtest.png):
  - Price line with LP range boundaries (upper/lower)
  - Green shading for in-range periods (actively providing liquidity)
  - Red shading for out-of-range periods (not providing liquidity)
  - Position entry/exit markers

- Complete chart (lp_backtest_complete.png):
  - Top panel: Position range chart with price and boundaries
  - Middle panel: Fee accrual (cumulative) and IL over time
  - Bottom panel: Net LP PnL (fees - IL)

Usage:
    pytest tests/visual/test_lp_backtest.py -v -s
"""

import math
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from almanak.framework.backtesting.pnl.calculators.impermanent_loss import (
    ImpermanentLossCalculator,
)
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
# Mock Data Provider with Price Movement Across LP Range
# =============================================================================


class LPTestDataProvider:
    """Data provider that generates price data suitable for LP strategy testing.

    Creates synthetic price data that moves in and out of a defined LP range
    to demonstrate in-range vs out-of-range behavior for visual verification.
    """

    def __init__(
        self,
        start_time: datetime,
        base_eth_price: Decimal = Decimal("3000"),
        range_lower: Decimal = Decimal("2800"),
        range_upper: Decimal = Decimal("3200"),
    ):
        """Initialize the data provider.

        Args:
            start_time: Start timestamp for the series
            base_eth_price: Starting ETH price
            range_lower: Lower bound of LP range for visualization
            range_upper: Upper bound of LP range for visualization
        """
        self._start_time = start_time
        self._base_eth_price = base_eth_price
        self._range_lower = range_lower
        self._range_upper = range_upper
        self._prices: dict[int, Decimal] = {}
        self._generate_prices()

    def _generate_prices(self) -> None:
        """Generate synthetic price series that moves in and out of LP range.

        Creates price movement that includes:
        - Periods within the LP range (in-range, earning fees)
        - Periods below the LP range (out-of-range, 100% token0)
        - Periods above the LP range (out-of-range, 100% token1)
        - Crossings of the range boundaries
        """
        # 30 days * 24 hours = 720 hourly data points
        num_hours = 720

        # Price phases to demonstrate range behavior:
        # Phase 1 (days 0-5): Start in range, move towards upper bound
        # Phase 2 (days 5-10): Exit above range, stay out
        # Phase 3 (days 10-15): Re-enter range from above
        # Phase 4 (days 15-20): Move towards lower bound and exit below
        # Phase 5 (days 20-25): Stay below range
        # Phase 6 (days 25-30): Re-enter range from below

        range_mid = (self._range_lower + self._range_upper) / 2
        range_width = self._range_upper - self._range_lower

        for i in range(num_hours):
            day = i / 24  # Fractional day

            if day < 5:
                # Phase 1: Start at mid-range, move toward upper bound
                progress = day / 5
                target = range_mid + (range_width * Decimal("0.4") * Decimal(str(progress)))
                price = float(target)
            elif day < 10:
                # Phase 2: Exit above range, go to upper + 10%
                progress = (day - 5) / 5
                above_range = float(self._range_upper) * 1.15
                start = float(self._range_upper) + float(range_width) * 0.4 * 0.2
                price = start + (above_range - start) * progress
            elif day < 15:
                # Phase 3: Come back into range from above
                progress = (day - 10) / 5
                above_range = float(self._range_upper) * 1.15
                target = float(range_mid)
                price = above_range + (target - above_range) * progress
            elif day < 20:
                # Phase 4: Move toward lower bound and exit below
                progress = (day - 15) / 5
                start = float(range_mid)
                below_range = float(self._range_lower) * 0.90
                price = start + (below_range - start) * progress
            elif day < 25:
                # Phase 5: Stay below range
                progress = (day - 20) / 5
                below_range = float(self._range_lower) * 0.88
                # Small wave pattern while below range
                wave = 50 * math.sin(progress * math.pi * 4)
                price = below_range + wave
            else:
                # Phase 6: Re-enter range from below
                progress = (day - 25) / 5
                below_range = float(self._range_lower) * 0.88
                target = float(range_mid) + float(range_width) * 0.2
                price = below_range + (target - below_range) * progress

            # Add small noise for realism (deterministic based on index)
            noise = 10 * math.sin(i * 0.3)
            self._prices[i] = Decimal(str(price + noise))

    def get_price_at_index(self, index: int) -> Decimal:
        """Get the ETH price at a given hourly index."""
        return self._prices.get(index, self._base_eth_price)

    @property
    def range_lower(self) -> Decimal:
        """Get the lower bound of the LP range."""
        return self._range_lower

    @property
    def range_upper(self) -> Decimal:
        """Get the upper bound of the LP range."""
        return self._range_upper

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
        return "lp_test_mock"

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
# LP Strategy
# =============================================================================


@dataclass
class MockLPOpenIntent:
    """Mock LP open intent for LP strategy."""

    intent_type: str = "LP_OPEN"
    pool: str = "ETH/USDC"
    amount0: Decimal = Decimal("1")  # 1 ETH
    amount1: Decimal = Decimal("3000")  # 3000 USDC
    range_lower: Decimal = Decimal("2800")
    range_upper: Decimal = Decimal("3200")
    protocol: str = "uniswap_v3"


@dataclass
class MockHoldIntent:
    """Mock hold intent (no action)."""

    intent_type: str = "HOLD"
    reason: str = "Waiting"


@dataclass
class RangeEvent:
    """Record of a range status change."""

    timestamp: datetime
    price: Decimal
    is_in_range: bool
    event_type: str  # "ENTER_RANGE", "EXIT_ABOVE", "EXIT_BELOW"


class LPStrategy:
    """Simple LP strategy: provide liquidity in ETH/USDC pool.

    This strategy demonstrates LP position behavior:
    - Opens an LP position at the start
    - Tracks range status throughout the backtest
    - Records when price enters/exits the position range
    """

    def __init__(
        self,
        range_lower: Decimal = Decimal("2800"),
        range_upper: Decimal = Decimal("3200"),
        initial_eth: Decimal = Decimal("1"),
        initial_usdc: Decimal = Decimal("3000"),
    ):
        """Initialize LP strategy.

        Args:
            range_lower: Lower price bound for LP position
            range_upper: Upper price bound for LP position
            initial_eth: Amount of ETH to provide
            initial_usdc: Amount of USDC to provide
        """
        self._range_lower = range_lower
        self._range_upper = range_upper
        self._initial_eth = initial_eth
        self._initial_usdc = initial_usdc

        # Track position state
        self._position_opened = False
        self._last_in_range: bool | None = None

        # Track range events for visualization
        self.range_events: list[RangeEvent] = []
        self._current_timestamp: datetime | None = None
        self._current_price: Decimal = Decimal("0")

    @property
    def strategy_id(self) -> str:
        return "lp_visual_test_strategy"

    @property
    def range_lower(self) -> Decimal:
        """Get the lower bound of the LP range."""
        return self._range_lower

    @property
    def range_upper(self) -> Decimal:
        """Get the upper bound of the LP range."""
        return self._range_upper

    def _is_in_range(self, price: Decimal) -> bool:
        """Check if price is within LP range."""
        return self._range_lower <= price <= self._range_upper

    def decide(self, market: Any) -> MockLPOpenIntent | MockHoldIntent:
        """Decide whether to open LP position or hold.

        Args:
            market: MarketSnapshot with price() method and timestamp

        Returns:
            LPOpenIntent on first tick, HoldIntent thereafter
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

        self._current_price = eth_price

        # Track range events
        current_in_range = self._is_in_range(eth_price)

        if self._last_in_range is not None and current_in_range != self._last_in_range:
            # Range status changed
            if current_in_range:
                event_type = "ENTER_RANGE"
            elif eth_price > self._range_upper:
                event_type = "EXIT_ABOVE"
            else:
                event_type = "EXIT_BELOW"

            self.range_events.append(
                RangeEvent(
                    timestamp=self._current_timestamp,
                    price=eth_price,
                    is_in_range=current_in_range,
                    event_type=event_type,
                )
            )

        self._last_in_range = current_in_range

        # Open position on first tick
        if not self._position_opened:
            self._position_opened = True
            return MockLPOpenIntent(
                pool="ETH/USDC",
                amount0=self._initial_eth,
                amount1=self._initial_usdc,
                range_lower=self._range_lower,
                range_upper=self._range_upper,
            )

        return MockHoldIntent(reason="Position already open")


# =============================================================================
# Chart Generation
# =============================================================================


@dataclass
class RangePeriod:
    """A period of in-range or out-of-range status."""

    start_idx: int
    end_idx: int
    is_in_range: bool


def identify_range_periods(
    prices: list[Decimal],
    range_lower: Decimal,
    range_upper: Decimal,
) -> list[RangePeriod]:
    """Identify contiguous periods of in-range and out-of-range.

    Args:
        prices: List of price values
        range_lower: Lower bound of LP range
        range_upper: Upper bound of LP range

    Returns:
        List of RangePeriod objects
    """
    if not prices:
        return []

    periods: list[RangePeriod] = []
    current_in_range = range_lower <= prices[0] <= range_upper
    start_idx = 0

    for i, price in enumerate(prices):
        in_range = range_lower <= price <= range_upper
        if in_range != current_in_range:
            # Status changed, close current period
            periods.append(
                RangePeriod(
                    start_idx=start_idx,
                    end_idx=i - 1,
                    is_in_range=current_in_range,
                )
            )
            start_idx = i
            current_in_range = in_range

    # Close final period
    periods.append(
        RangePeriod(
            start_idx=start_idx,
            end_idx=len(prices) - 1,
            is_in_range=current_in_range,
        )
    )

    return periods


def generate_lp_backtest_chart(
    data_provider: LPTestDataProvider,
    strategy: LPStrategy,
    start_time: datetime,
    output_path: Path,
) -> bool:
    """Generate the LP backtest visualization chart.

    Creates a figure showing:
    - ETH price line
    - LP range boundaries (upper/lower horizontal lines)
    - Green shading for in-range periods
    - Red shading for out-of-range periods
    - Range entry/exit event markers

    Args:
        data_provider: Provider with price data
        strategy: Strategy with recorded range events
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
    prices = [data_provider.get_price_at_index(i) for i in range(num_hours)]
    prices_float = [float(p) for p in prices]

    range_lower = float(strategy.range_lower)
    range_upper = float(strategy.range_upper)

    # Identify in-range and out-of-range periods
    range_periods = identify_range_periods(prices, strategy.range_lower, strategy.range_upper)

    # Create figure
    fig, ax = plt.subplots(figsize=(16, 8))
    fig.suptitle(
        "LP Position Backtest - ETH/USDC (30 Days)",
        fontsize=14,
        fontweight="bold",
    )

    # Add range zone shading first (behind everything)
    for period in range_periods:
        color = "green" if period.is_in_range else "red"
        alpha = 0.15 if period.is_in_range else 0.10
        ax.axvspan(
            timestamps[period.start_idx],
            timestamps[period.end_idx],
            alpha=alpha,
            color=color,
            zorder=1,
        )

    # Plot price line
    ax.plot(
        timestamps,
        prices_float,
        linewidth=2,
        color="#2196F3",
        label="ETH Price",
        zorder=3,
    )

    # Plot range boundaries
    ax.axhline(
        y=range_upper,
        color="#4CAF50",
        linestyle="--",
        linewidth=2,
        label=f"Range Upper (${range_upper:,.0f})",
        zorder=2,
    )
    ax.axhline(
        y=range_lower,
        color="#4CAF50",
        linestyle="--",
        linewidth=2,
        label=f"Range Lower (${range_lower:,.0f})",
        zorder=2,
    )

    # Fill the range zone with very light green
    ax.fill_between(
        timestamps,
        [range_lower] * len(timestamps),
        [range_upper] * len(timestamps),
        alpha=0.05,
        color="green",
        zorder=0,
    )

    # Add markers for range entry/exit events
    for event in strategy.range_events:
        if event.event_type == "ENTER_RANGE":
            marker = "^"
            color = "green"
        elif event.event_type == "EXIT_ABOVE":
            marker = "v"
            color = "red"
        else:  # EXIT_BELOW
            marker = "v"
            color = "orange"

        ax.scatter(
            [event.timestamp],
            [float(event.price)],
            marker=marker,
            s=150,
            c=color,
            zorder=5,
            edgecolors="black",
            linewidths=1.5,
        )

    # Calculate in-range percentage
    in_range_hours = sum(
        period.end_idx - period.start_idx + 1
        for period in range_periods
        if period.is_in_range
    )
    in_range_pct = (in_range_hours / num_hours) * 100

    # Add statistics annotation
    stats_text = (
        f"In-Range: {in_range_pct:.1f}% of time\n"
        f"Range Events: {len(strategy.range_events)}\n"
        f"Range Width: ${range_upper - range_lower:,.0f}"
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

    # Add legend with custom patches for range periods
    handles, labels = ax.get_legend_handles_labels()
    in_range_patch = mpatches.Patch(color="green", alpha=0.3, label="In-Range Period")
    out_range_patch = mpatches.Patch(color="red", alpha=0.2, label="Out-of-Range Period")
    handles.extend([in_range_patch, out_range_patch])

    ax.legend(handles=handles, loc="upper right", fontsize=10)

    # Formatting
    ax.set_xlabel("Date", fontsize=12)
    ax.set_ylabel("ETH Price (USD)", fontsize=12)
    ax.grid(True, alpha=0.3)

    # Format x-axis dates
    ax.xaxis.set_major_formatter(DateFormatter("%m/%d"))
    plt.xticks(rotation=45, ha="right")

    # Set y-axis limits to show context around range
    y_margin = (range_upper - range_lower) * 0.5
    ax.set_ylim(range_lower - y_margin, range_upper + y_margin)

    # Add title with range info
    ax.set_title(
        f"Position Range: ${range_lower:,.0f} - ${range_upper:,.0f}",
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
# LP Metrics Calculation
# =============================================================================


@dataclass
class LPMetricsPoint:
    """A point in the LP metrics time series."""

    timestamp: datetime
    price: Decimal
    cumulative_fees_usd: Decimal
    impermanent_loss_usd: Decimal
    net_pnl_usd: Decimal  # fees - IL
    is_in_range: bool


def calculate_lp_metrics_over_time(
    data_provider: LPTestDataProvider,
    start_time: datetime,
    entry_price: Decimal,
    initial_value_usd: Decimal,
    range_lower: Decimal,
    range_upper: Decimal,
    fee_tier: Decimal = Decimal("0.003"),  # 0.3% fee tier
    liquidity: Decimal = Decimal("1000000"),  # Position liquidity
) -> list[LPMetricsPoint]:
    """Calculate LP metrics (fees, IL, net PnL) over time.

    Args:
        data_provider: Provider with price data
        start_time: Start time of the backtest
        entry_price: Price at position entry
        initial_value_usd: Initial position value in USD
        range_lower: Lower price bound of LP position
        range_upper: Upper price bound of LP position
        fee_tier: Pool fee tier (e.g., 0.003 for 0.3%)
        liquidity: Position liquidity for IL calculation

    Returns:
        List of LPMetricsPoint with metrics at each hour
    """
    il_calculator = ImpermanentLossCalculator()
    metrics: list[LPMetricsPoint] = []

    num_hours = 720  # 30 days
    cumulative_fees = Decimal("0")

    # Convert price range to ticks (simplified, using log approximation)
    # tick = log(price) / log(1.0001)
    # For simplicity, we use a wide range that captures the price movement
    tick_lower = -50000  # Roughly $1000
    tick_upper = 50000  # Roughly $10000

    for i in range(num_hours):
        timestamp = start_time + timedelta(hours=i)
        current_price = data_provider.get_price_at_index(i)

        # Check if in range
        is_in_range = range_lower <= current_price <= range_upper

        # Calculate fee accrual (only when in range)
        if is_in_range:
            # Simulate fee accrual: base daily volume * fee tier * liquidity share
            # Assume $10M daily volume, position is ~1% of pool liquidity
            hourly_volume = Decimal("10000000") / Decimal("24")  # ~$417k/hour
            liquidity_share = Decimal("0.01")  # 1% of pool
            hourly_fees = hourly_volume * fee_tier * liquidity_share
            cumulative_fees += hourly_fees

        # Calculate impermanent loss
        il_pct, _, _ = il_calculator.calculate_il_v3(
            entry_price=entry_price,
            current_price=current_price,
            tick_lower=tick_lower,
            tick_upper=tick_upper,
            liquidity=liquidity,
        )

        # IL in USD terms
        il_usd = initial_value_usd * il_pct

        # Net PnL = fees earned - IL
        net_pnl = cumulative_fees - il_usd

        metrics.append(
            LPMetricsPoint(
                timestamp=timestamp,
                price=current_price,
                cumulative_fees_usd=cumulative_fees,
                impermanent_loss_usd=il_usd,
                net_pnl_usd=net_pnl,
                is_in_range=is_in_range,
            )
        )

    return metrics


def generate_lp_complete_chart(
    data_provider: LPTestDataProvider,
    strategy: LPStrategy,
    lp_metrics: list[LPMetricsPoint],
    start_time: datetime,
    output_path: Path,
) -> bool:
    """Generate complete LP backtest visualization with 3 panels.

    Creates a figure showing:
    - Top panel: Price chart with LP range boundaries and in/out-of-range shading
    - Middle panel: Fee accrual (cumulative) and IL over time
    - Bottom panel: Net LP PnL (fees - IL)

    Args:
        data_provider: Provider with price data
        strategy: Strategy with recorded range events
        lp_metrics: Pre-calculated LP metrics over time
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
    prices = [data_provider.get_price_at_index(i) for i in range(num_hours)]
    prices_float = [float(p) for p in prices]

    range_lower = float(strategy.range_lower)
    range_upper = float(strategy.range_upper)

    # Extract metrics for plotting
    fees_usd = [float(m.cumulative_fees_usd) for m in lp_metrics]
    il_usd = [float(m.impermanent_loss_usd) for m in lp_metrics]
    net_pnl = [float(m.net_pnl_usd) for m in lp_metrics]

    # Identify in-range and out-of-range periods
    range_periods = identify_range_periods(prices, strategy.range_lower, strategy.range_upper)

    # Create figure with 3 subplots
    fig, (ax1, ax2, ax3) = plt.subplots(
        3, 1, figsize=(16, 14), height_ratios=[2, 1.5, 1], sharex=True
    )
    fig.suptitle(
        "LP Position Backtest - ETH/USDC (30 Days) - Complete Analysis",
        fontsize=14,
        fontweight="bold",
    )

    # ==========================================================================
    # Top Panel: Price Chart with Range
    # ==========================================================================

    # Add range zone shading first (behind everything)
    for period in range_periods:
        color = "green" if period.is_in_range else "red"
        alpha = 0.15 if period.is_in_range else 0.10
        ax1.axvspan(
            timestamps[period.start_idx],
            timestamps[period.end_idx],
            alpha=alpha,
            color=color,
            zorder=1,
        )

    # Plot price line
    ax1.plot(
        timestamps,
        prices_float,
        linewidth=2,
        color="#2196F3",
        label="ETH Price",
        zorder=3,
    )

    # Plot range boundaries
    ax1.axhline(
        y=range_upper,
        color="#4CAF50",
        linestyle="--",
        linewidth=2,
        label=f"Range Upper (${range_upper:,.0f})",
        zorder=2,
    )
    ax1.axhline(
        y=range_lower,
        color="#4CAF50",
        linestyle="--",
        linewidth=2,
        label=f"Range Lower (${range_lower:,.0f})",
        zorder=2,
    )

    # Fill the range zone with very light green
    ax1.fill_between(
        timestamps,
        [range_lower] * len(timestamps),
        [range_upper] * len(timestamps),
        alpha=0.05,
        color="green",
        zorder=0,
    )

    # Add markers for range entry/exit events
    for event in strategy.range_events:
        if event.event_type == "ENTER_RANGE":
            marker = "^"
            color = "green"
        elif event.event_type == "EXIT_ABOVE":
            marker = "v"
            color = "red"
        else:  # EXIT_BELOW
            marker = "v"
            color = "orange"

        ax1.scatter(
            [event.timestamp],
            [float(event.price)],
            marker=marker,
            s=100,
            c=color,
            zorder=5,
            edgecolors="black",
            linewidths=1,
        )

    # Calculate in-range percentage
    in_range_hours = sum(
        period.end_idx - period.start_idx + 1
        for period in range_periods
        if period.is_in_range
    )
    in_range_pct = (in_range_hours / num_hours) * 100

    # Add statistics annotation
    stats_text = (
        f"In-Range: {in_range_pct:.1f}%\n"
        f"Range Events: {len(strategy.range_events)}"
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
    in_range_patch = mpatches.Patch(color="green", alpha=0.3, label="In-Range")
    out_range_patch = mpatches.Patch(color="red", alpha=0.2, label="Out-of-Range")
    handles.extend([in_range_patch, out_range_patch])
    ax1.legend(handles=handles, loc="upper right", fontsize=9)

    ax1.set_ylabel("ETH Price (USD)", fontsize=11)
    ax1.grid(True, alpha=0.3)
    ax1.set_title("Position Range Chart", fontsize=12)

    # Set y-axis limits
    y_margin = (range_upper - range_lower) * 0.5
    ax1.set_ylim(range_lower - y_margin, range_upper + y_margin)

    # ==========================================================================
    # Middle Panel: Fee Accrual and IL
    # ==========================================================================

    # Plot cumulative fees (positive, green)
    ax2.plot(
        timestamps,
        fees_usd,
        linewidth=2,
        color="#4CAF50",
        label="Cumulative Fees",
    )
    ax2.fill_between(timestamps, 0, fees_usd, alpha=0.2, color="#4CAF50")

    # Plot IL (negative impact, red)
    ax2.plot(
        timestamps,
        il_usd,
        linewidth=2,
        color="#F44336",
        label="Impermanent Loss",
    )
    ax2.fill_between(timestamps, 0, il_usd, alpha=0.2, color="#F44336")

    # Add zero line
    ax2.axhline(y=0, color="gray", linestyle="-", linewidth=0.5, alpha=0.5)

    # Final values annotation
    final_fees = fees_usd[-1]
    final_il = il_usd[-1]
    metrics_text = (
        f"Final Fees: ${final_fees:,.2f}\n"
        f"Final IL: ${final_il:,.2f}"
    )
    ax2.text(
        0.02,
        0.98,
        metrics_text,
        transform=ax2.transAxes,
        fontsize=10,
        verticalalignment="top",
        bbox={"boxstyle": "round", "facecolor": "lightyellow", "alpha": 0.8},
    )

    ax2.set_ylabel("USD", fontsize=11)
    ax2.legend(loc="upper right", fontsize=9)
    ax2.grid(True, alpha=0.3)
    ax2.set_title("Fee Accrual vs Impermanent Loss", fontsize=12)

    # ==========================================================================
    # Bottom Panel: Net LP PnL
    # ==========================================================================

    # Color the line based on positive/negative
    positive_mask = [p >= 0 for p in net_pnl]

    # Plot net PnL line
    ax3.plot(
        timestamps,
        net_pnl,
        linewidth=2,
        color="#2196F3",
        label="Net LP PnL (Fees - IL)",
    )

    # Fill positive area green, negative area red
    ax3.fill_between(
        timestamps,
        0,
        net_pnl,
        where=positive_mask,
        alpha=0.3,
        color="#4CAF50",
        interpolate=True,
    )
    ax3.fill_between(
        timestamps,
        0,
        net_pnl,
        where=[not p for p in positive_mask],
        alpha=0.3,
        color="#F44336",
        interpolate=True,
    )

    # Zero line
    ax3.axhline(y=0, color="gray", linestyle="-", linewidth=1, alpha=0.7)

    # Final PnL annotation
    final_pnl = net_pnl[-1]
    pnl_color = "#4CAF50" if final_pnl >= 0 else "#F44336"
    pnl_text = f"Final Net PnL: ${final_pnl:,.2f}"
    ax3.text(
        0.02,
        0.98 if final_pnl >= 0 else 0.05,
        pnl_text,
        transform=ax3.transAxes,
        fontsize=11,
        fontweight="bold",
        verticalalignment="top" if final_pnl >= 0 else "bottom",
        color=pnl_color,
        bbox={"boxstyle": "round", "facecolor": "white", "alpha": 0.8},
    )

    ax3.set_ylabel("USD", fontsize=11)
    ax3.set_xlabel("Date", fontsize=11)
    ax3.legend(loc="upper right", fontsize=9)
    ax3.grid(True, alpha=0.3)
    ax3.set_title("Net LP PnL (Fees - Impermanent Loss)", fontsize=12)

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


class TestLPBacktestVisual:
    """Visual tests for LP strategy backtest."""

    @pytest.mark.asyncio
    async def test_lp_strategy_30_day_backtest(self) -> None:
        """Run 30-day LP backtest and generate visualization.

        This test:
        1. Creates a mock data provider with price movement across LP range
        2. Runs an LP strategy that provides liquidity in ETH/USDC
        3. Generates a chart showing price with range boundaries
        4. Highlights in-range (green) vs out-of-range (red) periods
        5. Saves the chart to tests/visual/output/lp_backtest.png

        The chart can be visually inspected to verify:
        - Range boundaries are correctly displayed
        - In-range periods are shaded green
        - Out-of-range periods are shaded red
        - Range entry/exit events are marked
        """
        # Setup
        start_time = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
        end_time = start_time + timedelta(days=30)

        range_lower = Decimal("2800")
        range_upper = Decimal("3200")

        data_provider = LPTestDataProvider(
            start_time=start_time,
            base_eth_price=Decimal("3000"),
            range_lower=range_lower,
            range_upper=range_upper,
        )

        config = PnLBacktestConfig(
            start_time=start_time,
            end_time=end_time,
            interval_seconds=3600,  # 1 hour intervals
            initial_capital_usd=Decimal("10000"),
            tokens=["WETH", "USDC"],
            include_gas_costs=True,
        )

        strategy = LPStrategy(
            range_lower=range_lower,
            range_upper=range_upper,
            initial_eth=Decimal("1"),
            initial_usdc=Decimal("3000"),
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

        # Count range events
        enter_events = [e for e in strategy.range_events if e.event_type == "ENTER_RANGE"]
        exit_above = [e for e in strategy.range_events if e.event_type == "EXIT_ABOVE"]
        exit_below = [e for e in strategy.range_events if e.event_type == "EXIT_BELOW"]

        print("\n=== LP Strategy Backtest Results ===")
        print("Duration: 30 days (hourly intervals)")
        print(f"LP Range: ${range_lower} - ${range_upper}")
        print(f"Total range events: {len(strategy.range_events)}")
        print(f"  Enter range: {len(enter_events)}")
        print(f"  Exit above: {len(exit_above)}")
        print(f"  Exit below: {len(exit_below)}")
        print(f"Final portfolio value: ${result.final_capital_usd:.2f}")

        # Calculate in-range time
        prices = [data_provider.get_price_at_index(i) for i in range(720)]
        in_range_hours = sum(1 for p in prices if range_lower <= p <= range_upper)
        in_range_pct = (in_range_hours / 720) * 100
        print(f"Time in range: {in_range_pct:.1f}%")

        # Generate visualization
        output_path = OUTPUT_DIR / "lp_backtest.png"
        chart_generated = generate_lp_backtest_chart(
            data_provider=data_provider,
            strategy=strategy,
            start_time=start_time,
            output_path=output_path,
        )

        assert chart_generated, "Failed to generate chart"
        assert output_path.exists(), f"Chart file not found: {output_path}"

        print(f"\nVisualization saved to: {output_path}")
        print("=====================================\n")

        # Verify we had some range boundary crossings
        assert len(strategy.range_events) > 0, "Expected at least one range event"

        # Verify range events are correctly classified
        for event in strategy.range_events:
            if event.event_type == "ENTER_RANGE":
                assert range_lower <= event.price <= range_upper, (
                    f"Enter event price {event.price} should be in range"
                )
            elif event.event_type == "EXIT_ABOVE":
                assert event.price > range_upper, (
                    f"Exit above event price {event.price} should be above {range_upper}"
                )
            elif event.event_type == "EXIT_BELOW":
                assert event.price < range_lower, (
                    f"Exit below event price {event.price} should be below {range_lower}"
                )

    @pytest.mark.asyncio
    async def test_lp_strategy_complete_visualization(self) -> None:
        """Generate complete LP backtest visualization with fee accrual and IL.

        This test creates a 3-panel visualization:
        1. Top panel: Position range chart with price and boundaries
        2. Middle panel: Fee accrual (cumulative) and IL over time
        3. Bottom panel: Net LP PnL (fees - IL)

        The chart can be visually inspected to verify:
        - Fees accumulate only during in-range periods
        - IL increases with price deviation from entry
        - Net PnL shows the combined effect of fees vs IL
        """
        # Setup
        start_time = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
        end_time = start_time + timedelta(days=30)

        range_lower = Decimal("2800")
        range_upper = Decimal("3200")
        entry_price = Decimal("3000")  # Mid-range entry
        initial_value_usd = Decimal("6000")  # ~1 ETH + 3000 USDC

        data_provider = LPTestDataProvider(
            start_time=start_time,
            base_eth_price=entry_price,
            range_lower=range_lower,
            range_upper=range_upper,
        )

        config = PnLBacktestConfig(
            start_time=start_time,
            end_time=end_time,
            interval_seconds=3600,
            initial_capital_usd=Decimal("10000"),
            tokens=["WETH", "USDC"],
            include_gas_costs=True,
        )

        strategy = LPStrategy(
            range_lower=range_lower,
            range_upper=range_upper,
            initial_eth=Decimal("1"),
            initial_usdc=Decimal("3000"),
        )

        backtester = PnLBacktester(
            data_provider=data_provider,
            fee_models={"default": DefaultFeeModel()},
            slippage_models={"default": DefaultSlippageModel()},
        )

        # Run backtest to populate strategy range events
        result = await backtester.backtest(strategy, config)

        assert result.success, f"Backtest failed: {result.error}"

        # Calculate LP metrics over time
        lp_metrics = calculate_lp_metrics_over_time(
            data_provider=data_provider,
            start_time=start_time,
            entry_price=entry_price,
            initial_value_usd=initial_value_usd,
            range_lower=range_lower,
            range_upper=range_upper,
            fee_tier=Decimal("0.003"),  # 0.3% fee tier
            liquidity=Decimal("1000000"),
        )

        # Print metrics summary
        print("\n=== LP Complete Backtest Analysis ===")
        print("Duration: 30 days (hourly intervals)")
        print(f"LP Range: ${range_lower} - ${range_upper}")
        print(f"Entry Price: ${entry_price}")
        print(f"Initial Position Value: ${initial_value_usd}")

        # Calculate in-range hours
        in_range_hours = sum(1 for m in lp_metrics if m.is_in_range)
        in_range_pct = (in_range_hours / len(lp_metrics)) * 100
        print(f"Time in range: {in_range_pct:.1f}%")

        # Final metrics
        final_metrics = lp_metrics[-1]
        print(f"\nFinal Cumulative Fees: ${final_metrics.cumulative_fees_usd:.2f}")
        print(f"Final Impermanent Loss: ${final_metrics.impermanent_loss_usd:.2f}")
        print(f"Final Net LP PnL: ${final_metrics.net_pnl_usd:.2f}")

        # Find peak and trough of net PnL
        max_pnl = max(m.net_pnl_usd for m in lp_metrics)
        min_pnl = min(m.net_pnl_usd for m in lp_metrics)
        print(f"\nPeak Net PnL: ${max_pnl:.2f}")
        print(f"Trough Net PnL: ${min_pnl:.2f}")

        # Generate complete visualization
        output_path = OUTPUT_DIR / "lp_backtest_complete.png"
        chart_generated = generate_lp_complete_chart(
            data_provider=data_provider,
            strategy=strategy,
            lp_metrics=lp_metrics,
            start_time=start_time,
            output_path=output_path,
        )

        assert chart_generated, "Failed to generate complete chart"
        assert output_path.exists(), f"Chart file not found: {output_path}"

        print(f"\nComplete visualization saved to: {output_path}")
        print("=====================================\n")

        # Verify metrics calculations
        assert len(lp_metrics) == 720, "Expected 720 hourly data points"

        # Verify fees only accumulate when in range
        prev_fees = Decimal("0")
        for m in lp_metrics:
            if m.is_in_range:
                # Fees should increase or stay same
                assert m.cumulative_fees_usd >= prev_fees, (
                    "Cumulative fees should not decrease"
                )
            else:
                # When out of range, fees should stay the same
                assert m.cumulative_fees_usd == prev_fees, (
                    f"Fees should not accumulate out of range: "
                    f"prev={prev_fees}, current={m.cumulative_fees_usd}"
                )
            prev_fees = m.cumulative_fees_usd

        # Verify IL is always non-negative
        for m in lp_metrics:
            assert m.impermanent_loss_usd >= Decimal("0"), (
                f"IL should be non-negative: {m.impermanent_loss_usd}"
            )

        # Verify net PnL calculation
        for m in lp_metrics:
            expected_net = m.cumulative_fees_usd - m.impermanent_loss_usd
            assert abs(m.net_pnl_usd - expected_net) < Decimal("0.01"), (
                f"Net PnL mismatch: expected {expected_net}, got {m.net_pnl_usd}"
            )
