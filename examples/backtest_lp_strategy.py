#!/usr/bin/env python3
"""LP (Liquidity Provider) Strategy Backtest Example.

This example demonstrates backtesting a Uniswap V3 concentrated liquidity
strategy using the Almanak SDK's PnL backtesting engine.

Strategy Logic:
- Open LP position with concentrated range around entry price
- Track fee accrual (only when price is in range)
- Track impermanent loss as price moves
- Compare net LP PnL vs HODL baseline

Output:
- Console: LP-specific metrics and comparison
- Charts: 3-panel visualization with range tracking, fees/IL, and net PnL

Usage:
    python examples/backtest_lp_strategy.py

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

from almanak.framework.backtesting.pnl.calculators.impermanent_loss import (
    ImpermanentLossCalculator,
)
from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
from almanak.framework.backtesting.pnl.engine import (
    DefaultFeeModel,
    DefaultSlippageModel,
    PnLBacktester,
)
from examples.common.data_providers import LPRangeDataProvider

# Try to import matplotlib for visualization
try:
    import matplotlib.patches as mpatches
    import matplotlib.pyplot as plt
    from matplotlib.dates import DateFormatter

    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False

# =============================================================================
# Configuration
# =============================================================================

OUTPUT_DIR = Path(__file__).parent / "output"

# LP Position parameters
BASE_PRICE = Decimal("3000")
RANGE_LOWER = Decimal("2800")  # ~6.67% below entry
RANGE_UPPER = Decimal("3200")  # ~6.67% above entry
INITIAL_ETH = Decimal("1")
INITIAL_USDC = Decimal("3000")
INITIAL_POSITION_VALUE = Decimal("6000")  # 1 ETH + 3000 USDC

# Pool parameters
FEE_TIER = Decimal("0.003")  # 0.3% fee tier (Uniswap V3)
POOL_LIQUIDITY = Decimal("1000000")  # Position liquidity

# Backtest parameters
START_TIME = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
END_TIME = START_TIME + timedelta(days=30)
INTERVAL_SECONDS = 3600  # Hourly


# =============================================================================
# LP Metrics Data Classes
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


@dataclass
class RangeEvent:
    """Record of a range status change."""

    timestamp: datetime
    price: Decimal
    is_in_range: bool
    event_type: str  # "ENTER_RANGE", "EXIT_ABOVE", "EXIT_BELOW"


@dataclass
class RangePeriod:
    """A period of in-range or out-of-range status."""

    start_idx: int
    end_idx: int
    is_in_range: bool


@dataclass
class LPBacktestSummary:
    """Summary of LP backtest results."""

    total_fees_earned_usd: Decimal
    impermanent_loss_usd: Decimal
    net_lp_pnl_usd: Decimal
    time_in_range_pct: Decimal
    rebalance_count: int
    fee_apy: Decimal
    il_to_fees_ratio: Decimal
    hodl_pnl_usd: Decimal
    lp_vs_hodl_usd: Decimal


# =============================================================================
# LP Strategy
# =============================================================================


@dataclass
class MockLPOpenIntent:
    """Mock LP open intent."""

    intent_type: str = "LP_OPEN"
    pool: str = "ETH/USDC"
    amount0: Decimal = Decimal("1")
    amount1: Decimal = Decimal("3000")
    range_lower: Decimal = Decimal("2800")
    range_upper: Decimal = Decimal("3200")
    protocol: str = "uniswap_v3"


@dataclass
class MockHoldIntent:
    """Mock hold intent (no action)."""

    intent_type: str = "HOLD"
    reason: str = "Waiting"


class ConcentratedLPStrategy:
    """Uniswap V3 concentrated liquidity strategy.

    Opens an LP position with a defined price range and holds it,
    tracking range status and fee accrual over time.
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
        return "concentrated_lp_demo"

    @property
    def range_lower(self) -> Decimal:
        return self._range_lower

    @property
    def range_upper(self) -> Decimal:
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

        self._current_timestamp = getattr(market, "timestamp", None) or getattr(market, "_timestamp", None)
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
# LP Metrics Calculation
# =============================================================================


def calculate_lp_metrics_over_time(
    data_provider: LPRangeDataProvider,
    start_time: datetime,
    entry_price: Decimal,
    initial_value_usd: Decimal,
    range_lower: Decimal,
    range_upper: Decimal,
    fee_tier: Decimal = Decimal("0.003"),
    liquidity: Decimal = Decimal("1000000"),
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

    # Use wide tick range for IL calculation
    tick_lower = -50000
    tick_upper = 50000

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


def calculate_hodl_pnl(
    data_provider: LPRangeDataProvider,
    entry_price: Decimal,
    initial_eth: Decimal,
    initial_usdc: Decimal,
) -> Decimal:
    """Calculate PnL from simply holding the initial assets.

    Args:
        data_provider: Provider with price data
        entry_price: Price at position entry
        initial_eth: Initial ETH amount
        initial_usdc: Initial USDC amount

    Returns:
        HODL PnL in USD
    """
    # Initial value
    initial_value = initial_eth * entry_price + initial_usdc

    # Final value (USDC stays same, ETH at final price)
    final_price = data_provider.get_price_at_index(719)  # Last hour
    final_value = initial_eth * final_price + initial_usdc

    return final_value - initial_value


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
            periods.append(RangePeriod(start_idx=start_idx, end_idx=i - 1, is_in_range=current_in_range))
            start_idx = i
            current_in_range = in_range

    periods.append(RangePeriod(start_idx=start_idx, end_idx=len(prices) - 1, is_in_range=current_in_range))

    return periods


def generate_lp_complete_chart(
    data_provider: LPRangeDataProvider,
    strategy: ConcentratedLPStrategy,
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
    if not MATPLOTLIB_AVAILABLE:
        print("matplotlib not installed. Run: uv add matplotlib")
        return False

    # Prepare data
    num_hours = 720
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
    fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(16, 14), height_ratios=[2, 1.5, 1], sharex=True)
    fig.suptitle(
        "LP Position Backtest - ETH/USDC (30 Days) - Complete Analysis",
        fontsize=14,
        fontweight="bold",
    )

    # ==========================================================================
    # Top Panel: Price Chart with Range
    # ==========================================================================
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

    ax1.plot(timestamps, prices_float, linewidth=2, color="#2196F3", label="ETH Price", zorder=3)

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

    ax1.fill_between(
        timestamps,
        [range_lower] * len(timestamps),
        [range_upper] * len(timestamps),
        alpha=0.05,
        color="green",
        zorder=0,
    )

    # Add markers for range events
    for event in strategy.range_events:
        if event.event_type == "ENTER_RANGE":
            marker, color = "^", "green"
        elif event.event_type == "EXIT_ABOVE":
            marker, color = "v", "red"
        else:
            marker, color = "v", "orange"

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
    in_range_hours = sum(period.end_idx - period.start_idx + 1 for period in range_periods if period.is_in_range)
    in_range_pct = (in_range_hours / num_hours) * 100

    ax1.text(
        0.02,
        0.98,
        f"In-Range: {in_range_pct:.1f}%\nRange Events: {len(strategy.range_events)}",
        transform=ax1.transAxes,
        fontsize=10,
        verticalalignment="top",
        bbox={"boxstyle": "round", "facecolor": "wheat", "alpha": 0.8},
    )

    handles, labels = ax1.get_legend_handles_labels()
    in_range_patch = mpatches.Patch(color="green", alpha=0.3, label="In-Range")
    out_range_patch = mpatches.Patch(color="red", alpha=0.2, label="Out-of-Range")
    handles.extend([in_range_patch, out_range_patch])
    ax1.legend(handles=handles, loc="upper right", fontsize=9)

    ax1.set_ylabel("ETH Price (USD)", fontsize=11)
    ax1.grid(True, alpha=0.3)
    ax1.set_title("Position Range Chart", fontsize=12)

    y_margin = (range_upper - range_lower) * 0.5
    ax1.set_ylim(range_lower - y_margin, range_upper + y_margin)

    # ==========================================================================
    # Middle Panel: Fee Accrual and IL
    # ==========================================================================
    ax2.plot(timestamps, fees_usd, linewidth=2, color="#4CAF50", label="Cumulative Fees")
    ax2.fill_between(timestamps, 0, fees_usd, alpha=0.2, color="#4CAF50")

    ax2.plot(timestamps, il_usd, linewidth=2, color="#F44336", label="Impermanent Loss")
    ax2.fill_between(timestamps, 0, il_usd, alpha=0.2, color="#F44336")

    ax2.axhline(y=0, color="gray", linestyle="-", linewidth=0.5, alpha=0.5)

    final_fees = fees_usd[-1]
    final_il = il_usd[-1]
    ax2.text(
        0.02,
        0.98,
        f"Final Fees: ${final_fees:,.2f}\nFinal IL: ${final_il:,.2f}",
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
    positive_mask = [p >= 0 for p in net_pnl]

    ax3.plot(timestamps, net_pnl, linewidth=2, color="#2196F3", label="Net LP PnL (Fees - IL)")

    ax3.fill_between(timestamps, 0, net_pnl, where=positive_mask, alpha=0.3, color="#4CAF50", interpolate=True)
    ax3.fill_between(
        timestamps, 0, net_pnl, where=[not p for p in positive_mask], alpha=0.3, color="#F44336", interpolate=True
    )

    ax3.axhline(y=0, color="gray", linestyle="-", linewidth=1, alpha=0.7)

    final_pnl = net_pnl[-1]
    pnl_color = "#4CAF50" if final_pnl >= 0 else "#F44336"
    ax3.text(
        0.02,
        0.98 if final_pnl >= 0 else 0.05,
        f"Final Net PnL: ${final_pnl:,.2f}",
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

    ax3.xaxis.set_major_formatter(DateFormatter("%m/%d"))
    plt.xticks(rotation=45, ha="right")

    plt.tight_layout()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)

    print(f"Chart saved to: {output_path}")
    return True


# =============================================================================
# Main Execution
# =============================================================================


async def run_backtest() -> None:
    """Run the LP strategy backtest and generate visualizations."""
    print("\n" + "=" * 70)
    print("       CONCENTRATED LP STRATEGY BACKTEST")
    print("=" * 70)
    print("\nConfiguration:")
    print(f"  Period: {START_TIME.date()} to {END_TIME.date()} (30 days)")
    print(f"  Entry Price: ${BASE_PRICE}")
    print(f"  LP Range: ${RANGE_LOWER} - ${RANGE_UPPER}")
    print(f"  Range Width: {float((RANGE_UPPER - RANGE_LOWER) / BASE_PRICE * 100):.1f}%")
    print(f"  Initial Position: {INITIAL_ETH} ETH + {INITIAL_USDC} USDC")
    print(f"  Fee Tier: {float(FEE_TIER) * 100:.1f}%")

    # Create data provider with range-crossing price patterns
    data_provider = LPRangeDataProvider(
        start_time=START_TIME,
        base_price=BASE_PRICE,
        range_lower=RANGE_LOWER,
        range_upper=RANGE_UPPER,
    )

    # Create strategy
    strategy = ConcentratedLPStrategy(
        range_lower=RANGE_LOWER,
        range_upper=RANGE_UPPER,
        initial_eth=INITIAL_ETH,
        initial_usdc=INITIAL_USDC,
    )

    # Configure backtest
    config = PnLBacktestConfig(
        start_time=START_TIME,
        end_time=END_TIME,
        interval_seconds=INTERVAL_SECONDS,
        initial_capital_usd=INITIAL_POSITION_VALUE,
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

    # Run backtest to populate strategy range events
    result = await backtester.backtest(strategy, config)

    if not result.success:
        print(f"Backtest failed: {result.error}")
        return

    print("Backtest completed successfully!")

    # Calculate LP metrics over time
    lp_metrics = calculate_lp_metrics_over_time(
        data_provider=data_provider,
        start_time=START_TIME,
        entry_price=BASE_PRICE,
        initial_value_usd=INITIAL_POSITION_VALUE,
        range_lower=RANGE_LOWER,
        range_upper=RANGE_UPPER,
        fee_tier=FEE_TIER,
        liquidity=POOL_LIQUIDITY,
    )

    # Calculate HODL PnL for comparison
    hodl_pnl = calculate_hodl_pnl(
        data_provider=data_provider,
        entry_price=BASE_PRICE,
        initial_eth=INITIAL_ETH,
        initial_usdc=INITIAL_USDC,
    )

    # Compile summary
    final_metrics = lp_metrics[-1]
    in_range_hours = sum(1 for m in lp_metrics if m.is_in_range)
    in_range_pct = Decimal(str(in_range_hours / len(lp_metrics) * 100))

    # Calculate fee APY (annualized)
    days = 30
    daily_fee_rate = final_metrics.cumulative_fees_usd / INITIAL_POSITION_VALUE / days
    fee_apy = daily_fee_rate * 365

    # IL to fees ratio
    il_to_fees = (
        final_metrics.impermanent_loss_usd / final_metrics.cumulative_fees_usd
        if final_metrics.cumulative_fees_usd > 0
        else Decimal("0")
    )

    # LP vs HODL
    lp_vs_hodl = final_metrics.net_pnl_usd - hodl_pnl

    summary = LPBacktestSummary(
        total_fees_earned_usd=final_metrics.cumulative_fees_usd,
        impermanent_loss_usd=final_metrics.impermanent_loss_usd,
        net_lp_pnl_usd=final_metrics.net_pnl_usd,
        time_in_range_pct=in_range_pct,
        rebalance_count=len(strategy.range_events),
        fee_apy=fee_apy,
        il_to_fees_ratio=il_to_fees,
        hodl_pnl_usd=hodl_pnl,
        lp_vs_hodl_usd=lp_vs_hodl,
    )

    # Print results
    print("\n" + "=" * 60)
    print("             LP BACKTEST RESULTS")
    print("=" * 60)

    print("\n  Fee Metrics:")
    print(f"    Total Fees Earned: ${summary.total_fees_earned_usd:,.2f}")
    print(f"    Fee APY: {float(summary.fee_apy) * 100:.1f}%")

    print("\n  Impermanent Loss:")
    print(f"    Total IL: ${summary.impermanent_loss_usd:,.2f}")
    print(f"    IL / Fees Ratio: {float(summary.il_to_fees_ratio):.2f}")

    print("\n  Net Performance:")
    print(f"    Net LP PnL: ${summary.net_lp_pnl_usd:,.2f}")
    print(f"    Time in Range: {float(summary.time_in_range_pct):.1f}%")
    print(f"    Range Events: {summary.rebalance_count}")

    print("\n  Comparison with HODL:")
    print(f"    HODL PnL: ${summary.hodl_pnl_usd:,.2f}")
    print(f"    LP vs HODL: ${summary.lp_vs_hodl_usd:,.2f}")
    outperformed = "OUTPERFORMED" if summary.lp_vs_hodl_usd > 0 else "UNDERPERFORMED"
    print(f"    Status: LP {outperformed} HODL")

    print("\n" + "=" * 60)

    # Generate visualization
    print("\nGenerating visualization...")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    chart_path = OUTPUT_DIR / "lp_strategy_complete.png"
    success = generate_lp_complete_chart(
        data_provider=data_provider,
        strategy=strategy,
        lp_metrics=lp_metrics,
        start_time=START_TIME,
        output_path=chart_path,
    )

    if success:
        print(f"\nVisualization saved to: {chart_path}")
    else:
        print("\nFailed to generate visualization (matplotlib may not be installed)")

    # Print methodology notes
    print("\n" + "-" * 70)
    print("METHODOLOGY & ASSUMPTIONS")
    print("-" * 70)
    print("""
LP Backtest Methodology:

1. Fee Accrual Model:
   - Assumes $10M daily pool volume
   - Position represents 1% of pool liquidity
   - Fees accrue only when price is in range
   - Hourly fee = (daily_volume / 24) * fee_tier * liquidity_share

2. Impermanent Loss Calculation:
   - Uses Uniswap V3 concentrated IL formula
   - IL = position_value * IL_percentage
   - IL increases as price moves away from entry

3. HODL Baseline:
   - Simply holding initial 1 ETH + 3000 USDC
   - No rebalancing or trading
   - Final value = ETH_amount * final_price + USDC_amount

4. Key Insights:
   - LP profits when fees > IL
   - High in-range time = more fee accrual
   - Concentrated ranges = higher fees but higher IL risk
   - Wide ranges = lower fees but more stable
    """)

    print("\n" + "=" * 70)
    print("                    BACKTEST COMPLETE")
    print("=" * 70 + "\n")


def main() -> None:
    """Entry point for the example."""
    asyncio.run(run_backtest())


if __name__ == "__main__":
    main()
