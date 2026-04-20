"""Crisis backtest runner for stress-testing strategies during crisis periods.

This module provides functions for running backtests specifically during
historical crisis periods. It uses the CrisisScenario definitions to
configure the backtest date range automatically.

Key Components:
    - run_crisis_backtest: Async function to run backtest during a crisis scenario
    - run_crisis_backtest_sync: Synchronous wrapper for run_crisis_backtest
    - CrisisBacktestConfig: Configuration combining scenario with backtest settings

Example:
    from almanak.framework.backtesting.scenarios import (
        BLACK_THURSDAY,
        run_crisis_backtest,
    )
    from almanak.framework.backtesting.pnl import PnLBacktester

    # Run backtest during Black Thursday
    result = await run_crisis_backtest(
        strategy=my_strategy,
        scenario=BLACK_THURSDAY,
        backtester=backtester,
        initial_capital_usd=Decimal("10000"),
    )

    # Or with custom scenario
    custom = CrisisScenario(
        name="svb_collapse",
        start_date=datetime(2023, 3, 10),
        end_date=datetime(2023, 3, 15),
        description="Silicon Valley Bank collapse",
    )
    result = await run_crisis_backtest(strategy, custom, backtester)
"""

import asyncio
import logging
import os
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from almanak.framework.backtesting.models import BacktestResult, CrisisMetrics
from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
from almanak.framework.backtesting.pnl.engine import BacktestableStrategy, PnLBacktester
from almanak.framework.backtesting.scenarios.crisis import CrisisScenario

logger = logging.getLogger(__name__)

# CoinGecko free tier only provides 365 days of historical data
_COINGECKO_FREE_TIER_MAX_DAYS = 365


@dataclass
class CrisisBacktestConfig:
    """Configuration for a crisis backtest.

    This config combines a CrisisScenario with backtest-specific settings.
    The start_time and end_time are automatically set from the scenario.

    Attributes:
        scenario: The crisis scenario defining the date range
        initial_capital_usd: Starting capital for the backtest
        interval_seconds: Time between simulation ticks (default: 3600 = 1 hour)
        chain: Blockchain to simulate (default: 'arbitrum')
        tokens: List of tokens to track (default: ['WETH', 'USDC'])
        fee_model: Fee model to use (default: 'realistic')
        slippage_model: Slippage model to use (default: 'realistic')
        include_gas_costs: Whether to include gas costs (default: True)
        gas_price_gwei: Gas price for cost calculation (default: 50 for crisis periods)
        mev_simulation_enabled: Enable MEV cost simulation (default: True for crisis)
        extra_config: Additional configuration options passed to PnLBacktestConfig

    Example:
        config = CrisisBacktestConfig(
            scenario=BLACK_THURSDAY,
            initial_capital_usd=Decimal("100000"),
            tokens=["WETH", "USDC", "WBTC"],
        )
    """

    scenario: CrisisScenario
    initial_capital_usd: Decimal = Decimal("10000")
    interval_seconds: int = 3600
    chain: str = "arbitrum"
    tokens: list[str] = field(default_factory=lambda: ["WETH", "USDC"])
    fee_model: str = "realistic"
    slippage_model: str = "realistic"
    include_gas_costs: bool = True
    gas_price_gwei: Decimal = Decimal("50")  # Higher default for crisis periods
    mev_simulation_enabled: bool = True  # Enable MEV by default for crisis tests
    extra_config: dict[str, Any] = field(default_factory=dict)

    def to_pnl_config(self) -> PnLBacktestConfig:
        """Convert to a PnLBacktestConfig using the scenario date range.

        The start_time is extended backwards by the scenario's warmup_days
        to allow indicator-based strategies (RSI, MACD, etc.) to compute
        their values before the crisis window begins.

        Returns:
            PnLBacktestConfig with warmup-extended start and scenario end
        """
        return PnLBacktestConfig(
            start_time=self.scenario.warmup_start_date,
            end_time=self.scenario.end_date,
            interval_seconds=self.interval_seconds,
            initial_capital_usd=self.initial_capital_usd,
            fee_model=self.fee_model,
            slippage_model=self.slippage_model,
            include_gas_costs=self.include_gas_costs,
            gas_price_gwei=self.gas_price_gwei,
            chain=self.chain,
            tokens=self.tokens,
            mev_simulation_enabled=self.mev_simulation_enabled,
            **self.extra_config,
        )

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary.

        Returns:
            Dictionary with config data
        """
        return {
            "scenario": self.scenario.to_dict(),
            "initial_capital_usd": str(self.initial_capital_usd),
            "interval_seconds": self.interval_seconds,
            "chain": self.chain,
            "tokens": self.tokens,
            "fee_model": self.fee_model,
            "slippage_model": self.slippage_model,
            "include_gas_costs": self.include_gas_costs,
            "gas_price_gwei": str(self.gas_price_gwei),
            "mev_simulation_enabled": self.mev_simulation_enabled,
            "extra_config": self.extra_config,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "CrisisBacktestConfig":
        """Deserialize from dictionary.

        Args:
            data: Dictionary with serialized config

        Returns:
            CrisisBacktestConfig instance
        """
        return cls(
            scenario=CrisisScenario.from_dict(data["scenario"]),
            initial_capital_usd=Decimal(data.get("initial_capital_usd", "10000")),
            interval_seconds=data.get("interval_seconds", 3600),
            chain=data.get("chain", "arbitrum"),
            tokens=data.get("tokens", ["WETH", "USDC"]),
            fee_model=data.get("fee_model", "realistic"),
            slippage_model=data.get("slippage_model", "realistic"),
            include_gas_costs=data.get("include_gas_costs", True),
            gas_price_gwei=Decimal(data.get("gas_price_gwei", "50")),
            mev_simulation_enabled=data.get("mev_simulation_enabled", True),
            extra_config=data.get("extra_config", {}),
        )


@dataclass
class CrisisBacktestResult:
    """Result from a crisis backtest with scenario context.

    This extends the standard BacktestResult with crisis-specific metadata
    including the scenario that was tested and crisis-specific metrics.

    Attributes:
        result: The underlying BacktestResult from the backtest
        scenario: The crisis scenario that was tested
        crisis_metrics: Additional metrics specific to crisis analysis
    """

    result: BacktestResult
    scenario: CrisisScenario
    crisis_metrics: dict[str, Any] = field(default_factory=dict)

    @property
    def scenario_name(self) -> str:
        """Get the name of the crisis scenario."""
        return self.scenario.name

    @property
    def scenario_duration_days(self) -> int:
        """Get the duration of the crisis in days."""
        return self.scenario.duration_days

    @property
    def max_drawdown_during_crisis(self) -> Decimal:
        """Get the maximum drawdown that occurred during the crisis."""
        return self.result.metrics.max_drawdown_pct

    @property
    def total_return_during_crisis(self) -> Decimal:
        """Get the total return during the crisis period."""
        return self.result.metrics.total_return_pct

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary.

        Returns:
            Dictionary with result data including top-level success/error fields
            for consistency with PnL backtest output.
        """
        return {
            "success": self.result.success,
            "error": self.result.error,
            "result": self.result.to_dict(),
            "scenario": self.scenario.to_dict(),
            "crisis_metrics": self.crisis_metrics,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "CrisisBacktestResult":
        """Deserialize from dictionary.

        Args:
            data: Dictionary with serialized result

        Returns:
            CrisisBacktestResult instance
        """
        return cls(
            result=BacktestResult.from_dict(data["result"]),
            scenario=CrisisScenario.from_dict(data["scenario"]),
            crisis_metrics=data.get("crisis_metrics", {}),
        )

    def summary(self) -> str:
        """Generate a human-readable summary of the crisis backtest.

        Returns:
            Formatted string with key metrics
        """
        return (
            f"Crisis Backtest: {self.scenario.name}\n"
            f"Period: {self.scenario.start_date.strftime('%Y-%m-%d')} to "
            f"{self.scenario.end_date.strftime('%Y-%m-%d')} ({self.scenario_duration_days} days)\n"
            f"Description: {self.scenario.description[:100]}...\n"
            f"\n"
            f"Performance:\n"
            f"  Total Return: {self.total_return_during_crisis * 100:.2f}%\n"
            f"  Max Drawdown: {self.max_drawdown_during_crisis * 100:.2f}%\n"
            f"  Sharpe Ratio: {self.result.metrics.sharpe_ratio:.3f}\n"
            f"  Total Trades: {self.result.metrics.total_trades}\n"
            f"\n"
            f"Strategy: {self.result.strategy_id}"
        )


class CrisisScenarioDateRangeError(Exception):
    """Raised when a crisis scenario's dates exceed CoinGecko free tier limits."""


def _is_coingecko_provider(backtester: PnLBacktester) -> bool:
    """Check if the backtester uses a CoinGecko-based data provider."""
    provider = getattr(backtester, "data_provider", None)
    if provider is None:
        return False
    type_name = type(provider).__name__
    return "coingecko" in type_name.lower()


def _validate_scenario_date_range(scenario: CrisisScenario, backtester: PnLBacktester) -> None:
    """Validate that scenario dates are within CoinGecko data availability.

    CoinGecko free tier only provides 365 days of historical data.
    Predefined crisis scenarios (2020-2022) always exceed this limit.
    Skips validation if: (a) the backtester uses a non-CoinGecko data provider,
    or (b) the user has COINGECKO_API_KEY set (pro tier has no limit).

    Uses warmup_start_date (not start_date) since the backtest will fetch
    data from the warmup start.

    Raises:
        CrisisScenarioDateRangeError: If dates exceed free tier limit and no API key
    """
    if not _is_coingecko_provider(backtester):
        return

    has_api_key = bool(os.environ.get("COINGECKO_API_KEY", ""))
    if has_api_key:
        return

    now = datetime.now(UTC)
    # Use warmup_start_date since data fetching begins from there
    effective_start = scenario.warmup_start_date
    start = effective_start.replace(tzinfo=UTC) if effective_start.tzinfo is None else effective_start.astimezone(UTC)
    days_ago = (now.date() - start.date()).days

    if days_ago > _COINGECKO_FREE_TIER_MAX_DAYS:
        raise CrisisScenarioDateRangeError(
            f"Crisis scenario '{scenario.name}' requires data from "
            f"{effective_start.strftime('%Y-%m-%d')} ({days_ago} days ago, "
            f"including {scenario.warmup_days}-day warmup), "
            f"but CoinGecko free tier only provides {_COINGECKO_FREE_TIER_MAX_DAYS} days "
            f"of historical data. To use predefined crisis scenarios, either:\n"
            f"  1. Set COINGECKO_API_KEY environment variable (pro tier has no date limit)\n"
            f"  2. Create a custom scenario with dates within the last "
            f"{_COINGECKO_FREE_TIER_MAX_DAYS} days"
        )


async def run_crisis_backtest(
    strategy: BacktestableStrategy,
    scenario: CrisisScenario,
    backtester: PnLBacktester,
    initial_capital_usd: Decimal = Decimal("10000"),
    interval_seconds: int = 3600,
    chain: str = "arbitrum",
    tokens: list[str] | None = None,
    fee_model: str = "realistic",
    slippage_model: str = "realistic",
    include_gas_costs: bool = True,
    gas_price_gwei: Decimal = Decimal("50"),
    mev_simulation_enabled: bool = True,
    config: CrisisBacktestConfig | None = None,
    **extra_config: Any,
) -> CrisisBacktestResult:
    """Run a backtest during a specific crisis scenario period.

    This function configures a PnL backtest to run specifically during the
    date range defined by a CrisisScenario. It's useful for stress-testing
    strategies against historical market crises.

    Args:
        strategy: Strategy to backtest (must implement BacktestableStrategy)
        scenario: CrisisScenario defining the crisis period (or custom scenario)
        backtester: PnLBacktester instance to use for the backtest
        initial_capital_usd: Starting capital in USD (default: $10,000)
        interval_seconds: Time between simulation ticks (default: 3600 = 1 hour)
        chain: Blockchain to simulate (default: 'arbitrum')
        tokens: List of tokens to track (default: ['WETH', 'USDC'])
        fee_model: Fee model to use (default: 'realistic')
        slippage_model: Slippage model to use (default: 'realistic')
        include_gas_costs: Include gas costs in calculations (default: True)
        gas_price_gwei: Gas price for cost calculation (default: 50 gwei)
        mev_simulation_enabled: Enable MEV cost simulation (default: True)
        config: Optional CrisisBacktestConfig (overrides other parameters)
        **extra_config: Additional config options passed to PnLBacktestConfig

    Returns:
        CrisisBacktestResult with backtest results and scenario context

    Example:
        # Using a predefined scenario
        from almanak.framework.backtesting.scenarios import BLACK_THURSDAY

        result = await run_crisis_backtest(
            strategy=my_strategy,
            scenario=BLACK_THURSDAY,
            backtester=backtester,
            initial_capital_usd=Decimal("100000"),
        )

        # Using a custom scenario
        custom = CrisisScenario(
            name="svb_collapse",
            start_date=datetime(2023, 3, 10),
            end_date=datetime(2023, 3, 15),
            description="Silicon Valley Bank collapse",
        )
        result = await run_crisis_backtest(
            strategy=my_strategy,
            scenario=custom,
            backtester=backtester,
        )
    """
    if tokens is None:
        tokens = ["WETH", "USDC"]

    # Validate date range against CoinGecko free tier limit
    _validate_scenario_date_range(scenario, backtester)

    # Use provided config or create one from parameters
    if config is not None:
        crisis_config = config
    else:
        crisis_config = CrisisBacktestConfig(
            scenario=scenario,
            initial_capital_usd=initial_capital_usd,
            interval_seconds=interval_seconds,
            chain=chain,
            tokens=tokens,
            fee_model=fee_model,
            slippage_model=slippage_model,
            include_gas_costs=include_gas_costs,
            gas_price_gwei=gas_price_gwei,
            mev_simulation_enabled=mev_simulation_enabled,
            extra_config=extra_config,
        )

    warmup_info = ""
    if scenario.warmup_days > 0:
        warmup_info = f", warmup: {scenario.warmup_days} days from {scenario.warmup_start_date.strftime('%Y-%m-%d')}"
    logger.info(
        f"Starting crisis backtest for scenario '{scenario.name}' "
        f"from {scenario.start_date.strftime('%Y-%m-%d')} to "
        f"{scenario.end_date.strftime('%Y-%m-%d')} ({scenario.duration_days} days{warmup_info})"
    )

    # Convert to PnL config with scenario dates
    pnl_config = crisis_config.to_pnl_config()

    # Run the backtest (includes warmup period)
    result = await backtester.backtest(strategy, pnl_config)

    # Trim equity curve to crisis window only BEFORE computing metrics (VIB-176)
    # This must happen before build_crisis_metrics() so metrics reflect
    # only the crisis window, not the warmup period.
    if scenario.warmup_days > 0 and result.equity_curve:
        crisis_start = scenario.start_date
        # Normalize to UTC-aware for safe comparison with equity curve timestamps
        if crisis_start.tzinfo is None:
            crisis_start = crisis_start.replace(tzinfo=UTC)
        result.equity_curve = [
            p
            for p in result.equity_curve
            if (p.timestamp.replace(tzinfo=UTC) if p.timestamp.tzinfo is None else p.timestamp) >= crisis_start
        ]

    # Build CrisisMetrics and attach to BacktestResult
    crisis_metrics_obj = build_crisis_metrics(result, scenario)
    result.crisis_results = crisis_metrics_obj

    # Also return legacy dict format for CrisisBacktestResult.crisis_metrics
    crisis_metrics = crisis_metrics_obj.to_dict()

    logger.info(
        f"Crisis backtest completed for '{scenario.name}': "
        f"Return={result.metrics.total_return_pct * 100:.2f}%, "
        f"Max DD={result.metrics.max_drawdown_pct * 100:.2f}%, "
        f"Recovery={crisis_metrics_obj.recovery_pct * 100:.1f}%"
    )

    return CrisisBacktestResult(
        result=result,
        scenario=scenario,
        crisis_metrics=crisis_metrics,
    )


def run_crisis_backtest_sync(
    strategy: BacktestableStrategy,
    scenario: CrisisScenario,
    backtester: PnLBacktester,
    initial_capital_usd: Decimal = Decimal("10000"),
    interval_seconds: int = 3600,
    chain: str = "arbitrum",
    tokens: list[str] | None = None,
    fee_model: str = "realistic",
    slippage_model: str = "realistic",
    include_gas_costs: bool = True,
    gas_price_gwei: Decimal = Decimal("50"),
    mev_simulation_enabled: bool = True,
    config: CrisisBacktestConfig | None = None,
    **extra_config: Any,
) -> CrisisBacktestResult:
    """Synchronous wrapper for run_crisis_backtest.

    This function runs the async run_crisis_backtest in a new event loop.
    Use this when calling from synchronous code.

    Args:
        strategy: Strategy to backtest (must implement BacktestableStrategy)
        scenario: CrisisScenario defining the crisis period
        backtester: PnLBacktester instance to use
        initial_capital_usd: Starting capital in USD
        interval_seconds: Time between simulation ticks
        chain: Blockchain to simulate
        tokens: List of tokens to track
        fee_model: Fee model to use
        slippage_model: Slippage model to use
        include_gas_costs: Include gas costs in calculations
        gas_price_gwei: Gas price for cost calculation
        mev_simulation_enabled: Enable MEV cost simulation
        config: Optional CrisisBacktestConfig
        **extra_config: Additional config options

    Returns:
        CrisisBacktestResult with backtest results and scenario context

    Example:
        result = run_crisis_backtest_sync(
            strategy=my_strategy,
            scenario=TERRA_COLLAPSE,
            backtester=backtester,
        )
    """
    return asyncio.run(
        run_crisis_backtest(
            strategy=strategy,
            scenario=scenario,
            backtester=backtester,
            initial_capital_usd=initial_capital_usd,
            interval_seconds=interval_seconds,
            chain=chain,
            tokens=tokens,
            fee_model=fee_model,
            slippage_model=slippage_model,
            include_gas_costs=include_gas_costs,
            gas_price_gwei=gas_price_gwei,
            mev_simulation_enabled=mev_simulation_enabled,
            config=config,
            **extra_config,
        )
    )


def _calculate_crisis_metrics(
    result: BacktestResult,
    scenario: CrisisScenario,
) -> dict[str, Any]:
    """Calculate crisis-specific metrics from backtest results (legacy dict format).

    This function computes additional metrics that are particularly
    relevant for analyzing strategy performance during crisis periods.

    Args:
        result: BacktestResult from the crisis backtest
        scenario: The crisis scenario that was tested

    Returns:
        Dictionary with crisis-specific metrics
    """
    # Build and use the CrisisMetrics object for calculations
    crisis_metrics = build_crisis_metrics(result, scenario)
    return crisis_metrics.to_dict()


def build_crisis_metrics(
    result: BacktestResult,
    scenario: CrisisScenario,
    normal_period_result: BacktestResult | None = None,
) -> CrisisMetrics:
    """Build a CrisisMetrics object from backtest results.

    This function computes detailed crisis-specific metrics including drawdown
    analysis, recovery time, and optional comparison to normal period performance.

    Args:
        result: BacktestResult from the crisis backtest
        scenario: The crisis scenario that was tested
        normal_period_result: Optional BacktestResult from a normal (non-crisis) period
            for comparison. If provided, adds normal_period_comparison metrics.

    Returns:
        CrisisMetrics object with detailed crisis analysis
    """
    # Initialize with basic scenario info
    max_drawdown_pct = result.metrics.max_drawdown_pct
    drawdown_start: datetime | None = None
    drawdown_trough: datetime | None = None
    days_to_trough = 0
    recovery_time_days: int | None = None
    recovery_pct = Decimal("0")

    # Calculate detailed drawdown and recovery metrics from equity curve
    if result.equity_curve and len(result.equity_curve) > 1:
        # Find peak (highest point before any significant decline)
        values = [p.value_usd for p in result.equity_curve]
        timestamps = [p.timestamp for p in result.equity_curve]

        # Find the running maximum (peak before each point)
        running_max = values[0]
        running_max_idx = 0
        max_dd = Decimal("0")
        peak_idx = 0
        trough_idx = 0
        trough_value = values[0]

        for i, val in enumerate(values):
            if val > running_max:
                running_max = val
                running_max_idx = i
            # Calculate drawdown from current running max
            if running_max > Decimal("0"):
                dd = (running_max - val) / running_max
                if dd > max_dd:
                    max_dd = dd
                    peak_idx = running_max_idx
                    trough_idx = i
                    trough_value = val

        # Set drawdown timestamps
        if max_dd > Decimal("0"):
            max_drawdown_pct = max_dd
            drawdown_start = timestamps[peak_idx]
            drawdown_trough = timestamps[trough_idx]
            days_to_trough = (drawdown_trough - drawdown_start).days

        # Calculate recovery after trough
        if trough_idx < len(values) - 1:
            peak_value_at_dd = values[peak_idx]
            post_trough_values = values[trough_idx:]
            post_trough_timestamps = timestamps[trough_idx:]

            # Check if recovered to previous peak
            for i, val in enumerate(post_trough_values):
                if val >= peak_value_at_dd:
                    # Found recovery
                    recovery_time_days = (post_trough_timestamps[i] - drawdown_trough).days if drawdown_trough else 0
                    recovery_pct = Decimal("1")  # 100% recovery
                    break
            else:
                # Didn't fully recover - calculate partial recovery percentage
                if peak_value_at_dd > trough_value:
                    max_post_trough = max(post_trough_values)
                    recovery_pct = (max_post_trough - trough_value) / (peak_value_at_dd - trough_value)

    # Calculate total costs
    total_costs_usd = (
        result.metrics.total_fees_usd
        + result.metrics.total_slippage_usd
        + result.metrics.total_gas_usd
        + result.metrics.total_mev_cost_usd
    )

    # Build normal period comparison if provided
    normal_period_comparison: dict[str, Any] = {}
    if normal_period_result:
        normal_period_comparison = compare_crisis_to_normal(result, normal_period_result)

    return CrisisMetrics(
        scenario_name=scenario.name,
        scenario_start=scenario.start_date,
        scenario_end=scenario.end_date,
        scenario_duration_days=scenario.duration_days,
        max_drawdown_pct=max_drawdown_pct,
        drawdown_start=drawdown_start,
        drawdown_trough=drawdown_trough,
        days_to_trough=days_to_trough,
        recovery_time_days=recovery_time_days,
        recovery_pct=recovery_pct,
        total_return_pct=result.metrics.total_return_pct,
        volatility=result.metrics.volatility,
        sharpe_ratio=result.metrics.sharpe_ratio,
        total_trades=result.metrics.total_trades,
        winning_trades=result.metrics.winning_trades,
        losing_trades=result.metrics.losing_trades,
        win_rate=result.metrics.win_rate,
        total_costs_usd=total_costs_usd,
        normal_period_comparison=normal_period_comparison,
    )


def compare_crisis_to_normal(
    crisis_result: BacktestResult,
    normal_result: BacktestResult,
) -> dict[str, Any]:
    """Compare crisis period metrics to normal period metrics.

    This function calculates the relative performance between a crisis period
    and a normal (non-crisis) period, enabling analysis of how the strategy
    behaves under stress vs normal market conditions.

    Args:
        crisis_result: BacktestResult from the crisis period backtest
        normal_result: BacktestResult from a normal (non-crisis) period backtest

    Returns:
        Dictionary with comparison metrics:
            - return_diff_pct: Crisis return - Normal return (negative = underperformed)
            - volatility_ratio: Crisis volatility / Normal volatility (>1 = more volatile)
            - drawdown_ratio: Crisis max DD / Normal max DD (>1 = worse drawdowns)
            - sharpe_diff: Crisis Sharpe - Normal Sharpe
            - win_rate_diff: Crisis win rate - Normal win rate
            - cost_ratio: Crisis total costs / Normal total costs

    Example:
        comparison = compare_crisis_to_normal(crisis_result, normal_result)
        if comparison["return_diff_pct"] < -0.1:
            print("Strategy underperformed by >10% during crisis")
    """
    comparison: dict[str, Any] = {}

    # Return difference (crisis return - normal return)
    return_diff = crisis_result.metrics.total_return_pct - normal_result.metrics.total_return_pct
    comparison["return_diff_pct"] = str(return_diff)

    # Volatility ratio (higher = more volatile during crisis)
    if normal_result.metrics.volatility > Decimal("0"):
        vol_ratio = crisis_result.metrics.volatility / normal_result.metrics.volatility
        comparison["volatility_ratio"] = str(vol_ratio)
    else:
        comparison["volatility_ratio"] = "1"

    # Drawdown ratio (higher = worse drawdowns during crisis)
    if normal_result.metrics.max_drawdown_pct > Decimal("0"):
        dd_ratio = crisis_result.metrics.max_drawdown_pct / normal_result.metrics.max_drawdown_pct
        comparison["drawdown_ratio"] = str(dd_ratio)
    else:
        comparison["drawdown_ratio"] = "1"

    # Sharpe difference
    sharpe_diff = crisis_result.metrics.sharpe_ratio - normal_result.metrics.sharpe_ratio
    comparison["sharpe_diff"] = str(sharpe_diff)

    # Win rate difference
    win_rate_diff = crisis_result.metrics.win_rate - normal_result.metrics.win_rate
    comparison["win_rate_diff"] = str(win_rate_diff)

    # Cost ratio
    crisis_costs = (
        crisis_result.metrics.total_fees_usd
        + crisis_result.metrics.total_slippage_usd
        + crisis_result.metrics.total_gas_usd
        + crisis_result.metrics.total_mev_cost_usd
    )
    normal_costs = (
        normal_result.metrics.total_fees_usd
        + normal_result.metrics.total_slippage_usd
        + normal_result.metrics.total_gas_usd
        + normal_result.metrics.total_mev_cost_usd
    )
    if normal_costs > Decimal("0"):
        cost_ratio = crisis_costs / normal_costs
        comparison["cost_ratio"] = str(cost_ratio)
    else:
        comparison["cost_ratio"] = "1"

    # Trade frequency comparison
    if normal_result.metrics.total_trades > 0:
        trade_ratio = Decimal(crisis_result.metrics.total_trades) / Decimal(normal_result.metrics.total_trades)
        comparison["trade_frequency_ratio"] = str(trade_ratio)
    else:
        comparison["trade_frequency_ratio"] = "1"

    # Summary assessment
    comparison["stress_resilience_score"] = str(_calculate_stress_resilience(crisis_result, normal_result))

    return comparison


def _calculate_stress_resilience(
    crisis_result: BacktestResult,
    normal_result: BacktestResult,
) -> Decimal:
    """Calculate a stress resilience score (0-100) based on crisis vs normal performance.

    Higher scores indicate better resilience to crisis conditions.

    Args:
        crisis_result: BacktestResult from crisis period
        normal_result: BacktestResult from normal period

    Returns:
        Decimal score from 0-100 indicating stress resilience
    """
    score = Decimal("50")  # Start at neutral

    # Return preservation (max +/-25 points)
    # If crisis return >= normal return, add points; if worse, subtract
    if normal_result.metrics.total_return_pct != Decimal("0"):
        return_ratio = crisis_result.metrics.total_return_pct / abs(normal_result.metrics.total_return_pct)
        return_adjustment = min(Decimal("25"), max(Decimal("-25"), return_ratio * Decimal("25")))
        score += return_adjustment

    # Drawdown control (max +/-15 points)
    # If crisis drawdown <= normal drawdown, good; if worse, bad
    if normal_result.metrics.max_drawdown_pct > Decimal("0"):
        dd_ratio = crisis_result.metrics.max_drawdown_pct / normal_result.metrics.max_drawdown_pct
        # Lower ratio = better (smaller drawdown during crisis)
        dd_adjustment = Decimal("15") * (Decimal("1") - min(Decimal("2"), dd_ratio)) / Decimal("2")
        score += dd_adjustment

    # Sharpe preservation (max +/-10 points)
    if normal_result.metrics.sharpe_ratio != Decimal("0"):
        sharpe_ratio = crisis_result.metrics.sharpe_ratio / abs(normal_result.metrics.sharpe_ratio)
        sharpe_adjustment = min(Decimal("10"), max(Decimal("-10"), sharpe_ratio * Decimal("10")))
        score += sharpe_adjustment

    # Clamp to 0-100 range
    return max(Decimal("0"), min(Decimal("100"), score))


async def run_multiple_crisis_backtests(
    strategy: BacktestableStrategy,
    scenarios: list[CrisisScenario],
    backtester: PnLBacktester,
    initial_capital_usd: Decimal = Decimal("10000"),
    **config_kwargs: Any,
) -> list[CrisisBacktestResult]:
    """Run backtests across multiple crisis scenarios.

    This function runs the same strategy through multiple historical crisis
    scenarios and returns results for each, enabling comparison of strategy
    performance across different types of market stress.

    Args:
        strategy: Strategy to backtest
        scenarios: List of CrisisScenario to test
        backtester: PnLBacktester instance to use
        initial_capital_usd: Starting capital for each backtest
        **config_kwargs: Additional config options passed to each backtest

    Returns:
        List of CrisisBacktestResult, one for each scenario

    Example:
        from almanak.framework.backtesting.scenarios import (
            BLACK_THURSDAY, TERRA_COLLAPSE, FTX_COLLAPSE
        )

        results = await run_multiple_crisis_backtests(
            strategy=my_strategy,
            scenarios=[BLACK_THURSDAY, TERRA_COLLAPSE, FTX_COLLAPSE],
            backtester=backtester,
            initial_capital_usd=Decimal("100000"),
        )

        for result in results:
            print(f"{result.scenario_name}: {result.total_return_during_crisis*100:.1f}%")
    """
    results: list[CrisisBacktestResult] = []

    for scenario in scenarios:
        logger.info(f"Running crisis backtest for scenario: {scenario.name}")
        result = await run_crisis_backtest(
            strategy=strategy,
            scenario=scenario,
            backtester=backtester,
            initial_capital_usd=initial_capital_usd,
            **config_kwargs,
        )
        results.append(result)

    return results


def run_multiple_crisis_backtests_sync(
    strategy: BacktestableStrategy,
    scenarios: list[CrisisScenario],
    backtester: PnLBacktester,
    initial_capital_usd: Decimal = Decimal("10000"),
    **config_kwargs: Any,
) -> list[CrisisBacktestResult]:
    """Synchronous wrapper for run_multiple_crisis_backtests.

    Args:
        strategy: Strategy to backtest
        scenarios: List of CrisisScenario to test
        backtester: PnLBacktester instance to use
        initial_capital_usd: Starting capital for each backtest
        **config_kwargs: Additional config options

    Returns:
        List of CrisisBacktestResult, one for each scenario
    """
    return asyncio.run(
        run_multiple_crisis_backtests(
            strategy=strategy,
            scenarios=scenarios,
            backtester=backtester,
            initial_capital_usd=initial_capital_usd,
            **config_kwargs,
        )
    )


__all__ = [
    "CrisisBacktestConfig",
    "CrisisBacktestResult",
    "CrisisScenarioDateRangeError",
    "build_crisis_metrics",
    "compare_crisis_to_normal",
    "run_crisis_backtest",
    "run_crisis_backtest_sync",
    "run_multiple_crisis_backtests",
    "run_multiple_crisis_backtests_sync",
]
