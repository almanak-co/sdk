#!/usr/bin/env python3
"""Looping/Leverage Strategy Backtest Example.

This example demonstrates backtesting a leveraged yield farming strategy
using the Almanak SDK's PnL backtesting engine.

Strategy Logic:
- Supply wstETH as collateral
- Borrow USDC at target LTV (75%)
- Swap borrowed USDC back to wstETH
- Repeat to achieve target leverage (~3x)
- Monitor health factor, deleverage if HF drops below threshold

Output:
- Console: Leverage and health factor metrics
- Charts: 3-panel visualization with leverage, health factor, and yield

Usage:
    python examples/backtest_looping_strategy.py

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
from examples.common.data_providers import LendingDataProvider

# Try to import matplotlib for visualization
try:
    import matplotlib.pyplot as plt
    from matplotlib.dates import DateFormatter

    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False

# =============================================================================
# Configuration
# =============================================================================

OUTPUT_DIR = Path(__file__).parent / "output"

# Looping strategy parameters
INITIAL_COLLATERAL_USD = Decimal("10000")  # Starting capital
TARGET_LOOPS = 3  # Number of leverage loops
TARGET_LTV = Decimal("0.75")  # Borrow at 75% LTV
LIQUIDATION_LTV = Decimal("0.85")  # Liquidation threshold
MIN_HEALTH_FACTOR = Decimal("1.5")  # Deleverage if HF drops below this
WSTETH_TO_ETH_RATIO = Decimal("1.15")  # wstETH trades at ~1.15x ETH

# Interest rate parameters (annual)
SUPPLY_APY = Decimal("0.04")  # 4% supply APY
BORROW_APY = Decimal("0.06")  # 6% borrow APY

# Backtest parameters
START_TIME = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
END_TIME = START_TIME + timedelta(days=30)
INTERVAL_SECONDS = 3600  # Hourly


# =============================================================================
# Looping Strategy Data Classes
# =============================================================================


@dataclass
class PositionState:
    """Current state of the leveraged position."""

    collateral_usd: Decimal
    borrowed_usd: Decimal
    leverage_ratio: Decimal
    health_factor: Decimal
    supply_interest_earned: Decimal
    borrow_interest_paid: Decimal
    net_yield: Decimal
    is_liquidated: bool = False


@dataclass
class LeverageMetricsPoint:
    """A point in the leverage metrics time series."""

    timestamp: datetime
    price: Decimal
    leverage_ratio: Decimal
    health_factor: Decimal
    collateral_usd: Decimal
    borrowed_usd: Decimal
    supply_interest_earned: Decimal
    borrow_interest_paid: Decimal
    net_yield: Decimal
    is_in_danger_zone: bool  # HF < MIN_HEALTH_FACTOR


@dataclass
class LeverageBacktestSummary:
    """Summary of leverage backtest results."""

    final_leverage_ratio: Decimal
    min_health_factor: Decimal
    health_factor_warnings: int
    liquidations_count: int
    total_supply_interest: Decimal
    total_borrow_interest: Decimal
    net_yield_usd: Decimal
    yield_apy: Decimal
    max_drawdown_pct: Decimal


# =============================================================================
# Looping Strategy
# =============================================================================


@dataclass
class MockSupplyIntent:
    """Mock supply intent for lending protocol."""

    intent_type: str = "SUPPLY"
    token: str = "wstETH"
    amount: Decimal = Decimal("0")
    protocol: str = "morpho_blue"


@dataclass
class MockBorrowIntent:
    """Mock borrow intent for lending protocol."""

    intent_type: str = "BORROW"
    token: str = "USDC"
    amount: Decimal = Decimal("0")
    protocol: str = "morpho_blue"


@dataclass
class MockRepayIntent:
    """Mock repay intent for deleveraging."""

    intent_type: str = "REPAY"
    token: str = "USDC"
    amount: Decimal = Decimal("0")
    protocol: str = "morpho_blue"


@dataclass
class MockHoldIntent:
    """Mock hold intent (no action)."""

    intent_type: str = "HOLD"
    reason: str = "Waiting"


class LoopingYieldStrategy:
    """Leveraged yield farming strategy via recursive borrowing.

    Achieves target leverage by:
    1. Supply collateral
    2. Borrow against it
    3. Swap borrowed tokens back to collateral
    4. Repeat until target leverage reached

    Monitors health factor and deleverages if necessary.
    """

    def __init__(
        self,
        initial_collateral_usd: Decimal = Decimal("10000"),
        target_loops: int = 3,
        target_ltv: Decimal = Decimal("0.75"),
        liquidation_ltv: Decimal = Decimal("0.85"),
        min_health_factor: Decimal = Decimal("1.5"),
        supply_apy: Decimal = Decimal("0.04"),
        borrow_apy: Decimal = Decimal("0.06"),
    ):
        """Initialize looping strategy.

        Args:
            initial_collateral_usd: Starting capital in USD
            target_loops: Number of leverage iterations
            target_ltv: Loan-to-value ratio for borrowing
            liquidation_ltv: LTV at which position gets liquidated
            min_health_factor: Trigger deleverage if HF below this
            supply_apy: Annual supply interest rate
            borrow_apy: Annual borrow interest rate
        """
        self._initial_collateral = initial_collateral_usd
        self._target_loops = target_loops
        self._target_ltv = target_ltv
        self._liquidation_ltv = liquidation_ltv
        self._min_health_factor = min_health_factor
        self._supply_apy = supply_apy
        self._borrow_apy = borrow_apy

        # Position state
        self._loops_completed = 0
        self._collateral_usd = Decimal("0")
        self._borrowed_usd = Decimal("0")
        self._supply_interest = Decimal("0")
        self._borrow_interest = Decimal("0")
        self._is_building = True  # Building leverage vs maintaining
        self._is_liquidated = False

        # Metrics tracking
        self.metrics_history: list[LeverageMetricsPoint] = []
        self._health_factor_warnings = 0
        self._min_health_factor_seen = Decimal("999")

    @property
    def strategy_id(self) -> str:
        return "looping_yield_demo"

    def _calculate_health_factor(self, collateral_usd: Decimal, borrowed_usd: Decimal) -> Decimal:
        """Calculate health factor.

        HF = (collateral_value * liquidation_threshold) / borrow_value
        HF > 1 = safe, HF < 1 = liquidation
        """
        if borrowed_usd == 0:
            return Decimal("999")
        return (collateral_usd * self._liquidation_ltv) / borrowed_usd

    def _calculate_leverage_ratio(self, collateral_usd: Decimal, borrowed_usd: Decimal) -> Decimal:
        """Calculate effective leverage ratio.

        Leverage = total_exposure / equity
        where equity = collateral - borrowed
        """
        equity = collateral_usd - borrowed_usd
        if equity <= 0:
            return Decimal("999")  # Over-leveraged
        return collateral_usd / equity

    def _accrue_interest(self, hours: int = 1) -> None:
        """Accrue interest for the given period."""
        hourly_supply_rate = self._supply_apy / Decimal("8760")  # Hours per year
        hourly_borrow_rate = self._borrow_apy / Decimal("8760")

        supply_interest = self._collateral_usd * hourly_supply_rate * hours
        borrow_interest = self._borrowed_usd * hourly_borrow_rate * hours

        self._supply_interest += supply_interest
        self._borrow_interest += borrow_interest

        # Interest compounds into position
        self._collateral_usd += supply_interest
        self._borrowed_usd += borrow_interest

    def decide(self, market: Any) -> MockSupplyIntent | MockBorrowIntent | MockRepayIntent | MockHoldIntent:
        """Decide on looping strategy action.

        Args:
            market: MarketSnapshot with price() method and timestamp

        Returns:
            Intent based on current position state
        """
        # Extract price and timestamp from market snapshot
        try:
            price = market.price("WSTETH")
        except (ValueError, AttributeError):
            try:
                price = market.price("WETH")
                price = price * WSTETH_TO_ETH_RATIO
            except (ValueError, AttributeError):
                return MockHoldIntent(reason="No price available")

        timestamp = getattr(market, "timestamp", None) or getattr(market, "_timestamp", None)
        if timestamp is None:
            return MockHoldIntent(reason="No timestamp")

        # Accrue interest
        self._accrue_interest(hours=1)

        # Calculate current health factor
        health_factor = self._calculate_health_factor(self._collateral_usd, self._borrowed_usd)
        leverage_ratio = self._calculate_leverage_ratio(self._collateral_usd, self._borrowed_usd)

        # Track minimum health factor
        if health_factor < self._min_health_factor_seen and health_factor < Decimal("999"):
            self._min_health_factor_seen = health_factor

        # Check for danger zone
        is_in_danger_zone = health_factor < self._min_health_factor

        if is_in_danger_zone and not self._is_liquidated:
            self._health_factor_warnings += 1

        # Record metrics
        self.metrics_history.append(
            LeverageMetricsPoint(
                timestamp=timestamp,
                price=price,
                leverage_ratio=leverage_ratio,
                health_factor=health_factor,
                collateral_usd=self._collateral_usd,
                borrowed_usd=self._borrowed_usd,
                supply_interest_earned=self._supply_interest,
                borrow_interest_paid=self._borrow_interest,
                net_yield=self._supply_interest - self._borrow_interest,
                is_in_danger_zone=is_in_danger_zone,
            )
        )

        # Check for liquidation
        if health_factor < Decimal("1") and not self._is_liquidated:
            self._is_liquidated = True
            return MockHoldIntent(reason="LIQUIDATED")

        # If in danger zone, deleverage
        if is_in_danger_zone and not self._is_liquidated and self._borrowed_usd > 0:
            # Repay 20% of borrowed amount to improve HF
            repay_amount = self._borrowed_usd * Decimal("0.2")
            self._borrowed_usd -= repay_amount
            self._collateral_usd -= repay_amount  # Sell collateral to repay
            return MockRepayIntent(token="USDC", amount=repay_amount)

        # Build leverage if not done
        if self._is_building and self._loops_completed < self._target_loops:
            if self._collateral_usd == 0:
                # First loop: supply initial collateral
                self._collateral_usd = self._initial_collateral
                self._loops_completed += 1
                return MockSupplyIntent(token="wstETH", amount=self._initial_collateral)
            else:
                # Subsequent loops: borrow and re-supply
                borrow_amount = self._collateral_usd * self._target_ltv
                self._borrowed_usd += borrow_amount
                self._collateral_usd += borrow_amount  # Re-supply borrowed
                self._loops_completed += 1

                if self._loops_completed >= self._target_loops:
                    self._is_building = False

                return MockBorrowIntent(token="USDC", amount=borrow_amount)

        return MockHoldIntent(reason=f"Maintaining position, HF={float(health_factor):.2f}")


def generate_leverage_chart(
    metrics_history: list[LeverageMetricsPoint],
    start_time: datetime,
    output_path: Path,
    min_hf_threshold: float = 1.5,
) -> bool:
    """Generate complete leverage backtest visualization with 3 panels.

    Creates a figure showing:
    - Top panel: Leverage ratio over time
    - Middle panel: Health factor with warning threshold
    - Bottom panel: Yield (interest earned vs paid)

    Args:
        metrics_history: Pre-calculated leverage metrics over time
        start_time: Start time of the backtest
        output_path: Path to save the PNG file
        min_hf_threshold: Health factor warning threshold

    Returns:
        True if chart generated successfully, False otherwise
    """
    if not MATPLOTLIB_AVAILABLE:
        print("matplotlib not installed. Run: uv add matplotlib")
        return False

    # Extract data
    timestamps = [m.timestamp for m in metrics_history]
    leverage_ratios = [float(m.leverage_ratio) if m.leverage_ratio < 100 else 10 for m in metrics_history]
    health_factors = [float(m.health_factor) if m.health_factor < 100 else 10 for m in metrics_history]
    supply_interest = [float(m.supply_interest_earned) for m in metrics_history]
    borrow_interest = [float(m.borrow_interest_paid) for m in metrics_history]
    net_yield = [float(m.net_yield) for m in metrics_history]
    danger_zones = [m.is_in_danger_zone for m in metrics_history]

    # Create figure with 3 subplots
    fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(16, 14), height_ratios=[1.5, 1.5, 1], sharex=True)
    fig.suptitle(
        "Leveraged Yield Strategy Backtest - Morpho Blue (30 Days)",
        fontsize=14,
        fontweight="bold",
    )

    # ==========================================================================
    # Top Panel: Leverage Ratio
    # ==========================================================================
    ax1.plot(timestamps, leverage_ratios, linewidth=2, color="#2196F3", label="Leverage Ratio")
    ax1.fill_between(timestamps, 1, leverage_ratios, alpha=0.2, color="#2196F3")

    # Mark target leverage line
    ax1.axhline(y=3, color="#4CAF50", linestyle="--", linewidth=1.5, label="Target (3x)")
    ax1.axhline(y=1, color="gray", linestyle="-", linewidth=0.5, alpha=0.5)

    final_leverage = leverage_ratios[-1] if leverage_ratios else 1
    ax1.text(
        0.02,
        0.98,
        f"Final Leverage: {final_leverage:.2f}x",
        transform=ax1.transAxes,
        fontsize=10,
        verticalalignment="top",
        bbox={"boxstyle": "round", "facecolor": "wheat", "alpha": 0.8},
    )

    ax1.set_ylabel("Leverage Ratio (x)", fontsize=11)
    ax1.legend(loc="upper right", fontsize=9)
    ax1.grid(True, alpha=0.3)
    ax1.set_title("Leverage Ratio Over Time", fontsize=12)
    ax1.set_ylim(0, max(max(leverage_ratios) * 1.2, 4))

    # ==========================================================================
    # Middle Panel: Health Factor
    # ==========================================================================
    ax2.plot(timestamps, health_factors, linewidth=2, color="#9C27B0", label="Health Factor")

    # Fill danger zones
    for i, in_danger in enumerate(danger_zones):
        if in_danger and i > 0:
            ax2.axvspan(timestamps[i - 1], timestamps[i], alpha=0.3, color="red", zorder=1)

    # Warning and liquidation thresholds
    ax2.axhline(y=min_hf_threshold, color="#FF9800", linestyle="--", linewidth=2, label=f"Warning ({min_hf_threshold})")
    ax2.axhline(y=1.0, color="#F44336", linestyle="-", linewidth=2, label="Liquidation (1.0)")

    # Fill safe zone
    ax2.fill_between(
        timestamps, min_hf_threshold, [max(health_factors) * 1.1] * len(timestamps), alpha=0.1, color="green"
    )
    ax2.fill_between(timestamps, 1.0, min_hf_threshold, alpha=0.1, color="orange")
    ax2.fill_between(timestamps, 0, 1.0, alpha=0.1, color="red")

    min_hf = min(health_factors) if health_factors else 10
    ax2.text(
        0.02,
        0.98,
        f"Min HF: {min_hf:.2f}",
        transform=ax2.transAxes,
        fontsize=10,
        verticalalignment="top",
        bbox={"boxstyle": "round", "facecolor": "lightyellow", "alpha": 0.8},
    )

    ax2.set_ylabel("Health Factor", fontsize=11)
    ax2.legend(loc="upper right", fontsize=9)
    ax2.grid(True, alpha=0.3)
    ax2.set_title("Health Factor Monitoring", fontsize=12)
    ax2.set_ylim(0.5, min(max(health_factors) * 1.2, 5))

    # ==========================================================================
    # Bottom Panel: Yield (Interest)
    # ==========================================================================
    ax3.plot(timestamps, supply_interest, linewidth=2, color="#4CAF50", label="Supply Interest Earned")
    ax3.plot(timestamps, borrow_interest, linewidth=2, color="#F44336", label="Borrow Interest Paid")
    ax3.plot(timestamps, net_yield, linewidth=2, color="#2196F3", linestyle="--", label="Net Yield")

    ax3.fill_between(timestamps, 0, supply_interest, alpha=0.2, color="#4CAF50")
    ax3.fill_between(timestamps, 0, borrow_interest, alpha=0.2, color="#F44336")

    ax3.axhline(y=0, color="gray", linestyle="-", linewidth=0.5, alpha=0.5)

    final_net = net_yield[-1] if net_yield else 0
    final_supply = supply_interest[-1] if supply_interest else 0
    final_borrow = borrow_interest[-1] if borrow_interest else 0
    ax3.text(
        0.02,
        0.98,
        f"Supply: +${final_supply:.2f}\nBorrow: -${final_borrow:.2f}\nNet: ${final_net:+.2f}",
        transform=ax3.transAxes,
        fontsize=10,
        verticalalignment="top",
        bbox={"boxstyle": "round", "facecolor": "white", "alpha": 0.8},
    )

    ax3.set_ylabel("USD", fontsize=11)
    ax3.set_xlabel("Date", fontsize=11)
    ax3.legend(loc="upper right", fontsize=9)
    ax3.grid(True, alpha=0.3)
    ax3.set_title("Interest: Earned vs Paid", fontsize=12)

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
    """Run the looping strategy backtest and generate visualizations."""
    print("\n" + "=" * 70)
    print("       LEVERAGED LOOPING STRATEGY BACKTEST")
    print("=" * 70)
    print("\nConfiguration:")
    print(f"  Period: {START_TIME.date()} to {END_TIME.date()} (30 days)")
    print(f"  Initial Capital: ${INITIAL_COLLATERAL_USD:,}")
    print(f"  Target Loops: {TARGET_LOOPS}")
    print(f"  Target LTV: {float(TARGET_LTV) * 100:.0f}%")
    print(f"  Liquidation LTV: {float(LIQUIDATION_LTV) * 100:.0f}%")
    print(f"  Min Health Factor: {float(MIN_HEALTH_FACTOR):.1f}")
    print(f"  Supply APY: {float(SUPPLY_APY) * 100:.1f}%")
    print(f"  Borrow APY: {float(BORROW_APY) * 100:.1f}%")

    # Create data provider with volatility for HF testing
    data_provider = LendingDataProvider(
        start_time=START_TIME,
        base_price=Decimal("3000"),
    )

    # Create strategy
    strategy = LoopingYieldStrategy(
        initial_collateral_usd=INITIAL_COLLATERAL_USD,
        target_loops=TARGET_LOOPS,
        target_ltv=TARGET_LTV,
        liquidation_ltv=LIQUIDATION_LTV,
        min_health_factor=MIN_HEALTH_FACTOR,
        supply_apy=SUPPLY_APY,
        borrow_apy=BORROW_APY,
    )

    # Configure backtest
    config = PnLBacktestConfig(
        start_time=START_TIME,
        end_time=END_TIME,
        interval_seconds=INTERVAL_SECONDS,
        initial_capital_usd=INITIAL_COLLATERAL_USD,
        tokens=["WSTETH", "WETH", "USDC"],
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

    if not result.success:
        print(f"Backtest failed: {result.error}")
        return

    print("Backtest completed successfully!")

    # Compile summary from metrics history
    if not strategy.metrics_history:
        print("No metrics recorded")
        return

    final_metrics = strategy.metrics_history[-1]

    # Calculate summary statistics
    health_factors = [float(m.health_factor) for m in strategy.metrics_history if m.health_factor < 100]
    min_hf = min(health_factors) if health_factors else Decimal("0")

    # Calculate yield APY
    days = 30
    initial_value = INITIAL_COLLATERAL_USD
    net_yield = final_metrics.net_yield
    daily_yield_rate = net_yield / initial_value / days if initial_value > 0 else Decimal("0")
    yield_apy = daily_yield_rate * 365

    # Calculate max drawdown (from collateral perspective)
    collateral_values = [float(m.collateral_usd) for m in strategy.metrics_history]
    peak = collateral_values[0]
    max_dd = 0.0
    for val in collateral_values:
        if val > peak:
            peak = val
        dd = (val - peak) / peak * 100 if peak > 0 else 0
        if dd < max_dd:
            max_dd = dd

    summary = LeverageBacktestSummary(
        final_leverage_ratio=final_metrics.leverage_ratio,
        min_health_factor=Decimal(str(min_hf)),
        health_factor_warnings=strategy._health_factor_warnings,
        liquidations_count=1 if strategy._is_liquidated else 0,
        total_supply_interest=final_metrics.supply_interest_earned,
        total_borrow_interest=final_metrics.borrow_interest_paid,
        net_yield_usd=final_metrics.net_yield,
        yield_apy=yield_apy,
        max_drawdown_pct=Decimal(str(max_dd)),
    )

    # Print results
    print("\n" + "=" * 60)
    print("          LOOPING STRATEGY RESULTS")
    print("=" * 60)

    print("\n  Leverage Metrics:")
    print(f"    Final Leverage: {float(summary.final_leverage_ratio):.2f}x")
    print(f"    Loops Completed: {strategy._loops_completed}")

    print("\n  Risk Metrics:")
    print(f"    Min Health Factor: {float(summary.min_health_factor):.2f}")
    print(f"    HF Warnings: {summary.health_factor_warnings}")
    print(f"    Liquidations: {summary.liquidations_count}")
    print(f"    Max Drawdown: {float(summary.max_drawdown_pct):.2f}%")

    print("\n  Yield Metrics:")
    print(f"    Supply Interest Earned: ${float(summary.total_supply_interest):,.2f}")
    print(f"    Borrow Interest Paid: ${float(summary.total_borrow_interest):,.2f}")
    print(f"    Net Yield: ${float(summary.net_yield_usd):,.2f}")
    print(f"    Yield APY: {float(summary.yield_apy) * 100:.1f}%")

    print("\n  Position State:")
    print(f"    Final Collateral: ${float(final_metrics.collateral_usd):,.2f}")
    print(f"    Final Borrowed: ${float(final_metrics.borrowed_usd):,.2f}")
    status = "LIQUIDATED" if strategy._is_liquidated else "ACTIVE"
    print(f"    Status: {status}")

    print("\n" + "=" * 60)

    # Generate visualization
    print("\nGenerating visualization...")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    chart_path = OUTPUT_DIR / "looping_strategy_complete.png"
    success = generate_leverage_chart(
        metrics_history=strategy.metrics_history,
        start_time=START_TIME,
        output_path=chart_path,
        min_hf_threshold=float(MIN_HEALTH_FACTOR),
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
Looping Strategy Methodology:

1. Leverage Building:
   - Start with $10,000 collateral
   - Borrow at 75% LTV and re-supply
   - After 3 loops: ~3x leverage
   - Effective formula: L = 1 / (1 - LTV^n) where n = loops

2. Health Factor Calculation:
   - HF = (collateral * liquidation_threshold) / borrowed
   - HF > 1.5: Safe zone (green)
   - HF 1.0-1.5: Warning zone (orange)
   - HF < 1.0: Liquidation (red)

3. Interest Accrual:
   - Supply APY: 4% (hourly compound)
   - Borrow APY: 6% (hourly compound)
   - Net spread is negative (paying more than earning)
   - But leverage amplifies exposure to price appreciation

4. Risk Management:
   - Monitor HF every hour
   - Deleverage if HF drops below 1.5
   - Repay 20% of borrowed to improve HF

5. Key Insights:
   - Higher leverage = higher yield but lower HF
   - Price volatility directly impacts HF
   - Net interest spread is typically negative
   - Profit comes from collateral price appreciation
    """)

    print("\n" + "=" * 70)
    print("                    BACKTEST COMPLETE")
    print("=" * 70 + "\n")


def main() -> None:
    """Entry point for the example."""
    asyncio.run(run_backtest())


if __name__ == "__main__":
    main()
