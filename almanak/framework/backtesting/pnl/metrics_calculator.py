"""Metrics calculation for PnL backtesting.

Provides standalone functions for calculating backtest performance metrics
including returns, volatility, Sharpe ratio, Sortino ratio, max drawdown,
and gas price summaries.

These are pure math functions that operate on equity curves, trade records,
and configuration parameters.

Extracted from pnl/engine.py for module size management.
"""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from almanak.framework.backtesting.models import (
    BacktestMetrics,
    GasPriceSummary,
    TradeRecord,
)
from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
from almanak.framework.backtesting.pnl.portfolio import SimulatedPortfolio


@dataclass(frozen=True)
class _TradeStatistics:
    """Win/loss aggregates over the trades that realized PnL (VIB-5083)."""

    win_rate: Decimal
    profit_factor: Decimal
    winning_trades: int
    losing_trades: int
    trades_with_realized_pnl: int
    failed_trades: int
    avg_trade_pnl: Decimal
    largest_win: Decimal
    largest_loss: Decimal
    avg_win: Decimal
    avg_loss: Decimal


def _mean(values: list[Decimal]) -> Decimal:
    """Mean of ``values``, or ``Decimal("0")`` for an empty list."""
    if not values:
        return Decimal("0")
    return sum(values, Decimal("0")) / Decimal(str(len(values)))


def _compute_trade_statistics(trades: list[TradeRecord]) -> _TradeStatistics:
    """Win/loss statistics over the trades that realized PnL (VIB-5083).

    Performance stats operate ONLY on trades that actually realized PnL:
    rejected fills (``success=False``) and opening / inventory-building
    trades (``pnl_usd=None``) carry no win/loss signal. Empty != Zero -- an
    unknown PnL must never be miscounted as a loss, which is exactly what
    degraded ``win_rate`` to 0 and produced negative "wins" before this fix.
    ``failed_trades`` is reported separately so rejected fills never inflate
    the win/loss denominator.
    """
    failed_count = sum(1 for t in trades if not t.success)
    # realized_net_pnl() is gated by has_realized_pnl, so this list is all
    # non-None Decimals.
    realized_pnls = [t.realized_net_pnl() for t in trades if t.has_realized_pnl]
    winning_pnls = [p for p in realized_pnls if p > Decimal("0")]
    losing_pnls = [p for p in realized_pnls if p <= Decimal("0")]

    win_rate = Decimal("0")
    if realized_pnls:
        win_rate = Decimal(str(len(winning_pnls))) / Decimal(str(len(realized_pnls)))

    gross_profit = sum(winning_pnls, Decimal("0"))
    gross_loss = abs(sum(losing_pnls, Decimal("0")))
    # profit_factor is gross_profit / gross_loss. When gross_loss == 0 the
    # ratio is mathematically undefined (an all-profit run divides by zero).
    # We report 0 here as a documented limitation: BacktestMetrics.profit_factor
    # is a non-Optional Decimal consumed by the JSON serializer (_decimal_str),
    # the text report (``{...:.2f}``), and from_dict, so widening it to
    # Optional/Infinity would ripple through all three. A 0 profit_factor on a
    # run with gross_profit > 0 and zero losses should be read as "undefined
    # (no losses)", not "no profit" -- the win_rate (1.0) and gross_profit
    # disambiguate it. (VIB-5083, CodeRabbit.)
    profit_factor = gross_profit / gross_loss if gross_loss > Decimal("0") else Decimal("0")

    return _TradeStatistics(
        win_rate=win_rate,
        profit_factor=profit_factor,
        winning_trades=len(winning_pnls),
        losing_trades=len(losing_pnls),
        trades_with_realized_pnl=len(realized_pnls),
        failed_trades=failed_count,
        avg_trade_pnl=_mean(realized_pnls),
        # Directional extrema: the largest WIN comes from winning trades only
        # and the largest LOSS from losing trades only. Taking max/min over the
        # undirected realized list reported a negative "largest win" on an
        # all-loss run (and a positive "largest loss" on an all-win run); the
        # defaults make an empty side collapse to 0 (VIB-5083, CodeRabbit).
        largest_win=max(winning_pnls, default=Decimal("0")),
        largest_loss=min(losing_pnls, default=Decimal("0")),
        avg_win=_mean(winning_pnls),
        avg_loss=_mean(losing_pnls),
    )


def decimal_sqrt(n: Decimal) -> Decimal:
    """Calculate square root of a Decimal using Newton's method.

    Standard library math.sqrt doesn't support Decimal, so we use
    Newton's iterative method for arbitrary precision.

    Args:
        n: Non-negative Decimal value

    Returns:
        Square root approximation

    Raises:
        ValueError: If n is negative
    """
    if n < Decimal("0"):
        raise ValueError("Cannot compute sqrt of negative number")
    if n == Decimal("0"):
        return Decimal("0")

    # Initial guess
    x = n
    # Newton's method: x_new = (x + n/x) / 2
    for _ in range(50):  # Max iterations
        x_new = (x + n / x) / Decimal("2")
        if abs(x_new - x) < Decimal("1e-28"):
            break
        x = x_new
    return x


def calculate_returns(values: list[Decimal]) -> list[Decimal]:
    """Calculate period-over-period returns from equity values.

    Args:
        values: List of equity values over time

    Returns:
        List of returns where returns[i] = (values[i+1] - values[i]) / values[i]
    """
    if len(values) < 2:
        return []

    returns: list[Decimal] = []
    for i in range(1, len(values)):
        if values[i - 1] > Decimal("0"):
            ret = (values[i] - values[i - 1]) / values[i - 1]
            returns.append(ret)
    return returns


def calculate_volatility(
    returns: list[Decimal],
    trading_days: Decimal,
) -> Decimal:
    """Calculate annualized volatility from returns.

    Volatility is the annualized standard deviation of returns:
    volatility = std_dev(returns) * sqrt(trading_days)

    Args:
        returns: List of period returns
        trading_days: Number of trading days per year (365 for crypto, 252 for stocks)

    Returns:
        Annualized volatility as a decimal (0.2 = 20%)
    """
    if len(returns) < 2:
        return Decimal("0")

    # Calculate mean
    n = Decimal(str(len(returns)))
    mean = sum(returns, Decimal("0")) / n

    # Calculate variance (sample variance with n-1)
    squared_diffs = sum((r - mean) ** 2 for r in returns)
    variance = squared_diffs / (n - Decimal("1"))

    # Standard deviation
    std_dev = decimal_sqrt(variance)

    # Annualize
    return std_dev * decimal_sqrt(trading_days)


def calculate_sharpe_ratio(
    returns: list[Decimal],
    volatility: Decimal,
    risk_free_rate: Decimal,
    trading_days: Decimal,
) -> Decimal:
    """Calculate the Sharpe ratio.

    Sharpe ratio = (annualized_return - risk_free_rate) / volatility

    Args:
        returns: List of period returns
        volatility: Annualized volatility
        risk_free_rate: Annual risk-free rate from config
        trading_days: Number of trading days per year

    Returns:
        Sharpe ratio (risk-adjusted return)
    """
    if volatility == Decimal("0") or not returns:
        return Decimal("0")

    # Calculate annualized mean return
    n = Decimal(str(len(returns)))
    mean_return = sum(returns, Decimal("0")) / n
    annualized_return = mean_return * trading_days

    # Sharpe = (return - risk_free_rate) / volatility
    return (annualized_return - risk_free_rate) / volatility


def calculate_sortino_ratio(
    returns: list[Decimal],
    risk_free_rate: Decimal,
    trading_days: Decimal,
) -> Decimal:
    """Calculate the Sortino ratio (downside deviation based).

    Sortino ratio uses only negative returns for the denominator,
    penalizing only downside volatility rather than all volatility.

    Sortino = (annualized_return - risk_free_rate) / downside_deviation

    Args:
        returns: List of period returns
        risk_free_rate: Annual risk-free rate
        trading_days: Number of trading days per year

    Returns:
        Sortino ratio
    """
    if len(returns) < 2:
        return Decimal("0")

    # Get negative returns for downside deviation
    negative_returns = [r for r in returns if r < Decimal("0")]
    if not negative_returns:
        # No negative returns means infinite Sortino (capped at 0 for safety)
        return Decimal("0")

    # Calculate downside deviation
    # Using the semi-deviation: sqrt(sum(min(r, 0)^2) / n)
    n = Decimal(str(len(returns)))
    downside_variance = sum(r**2 for r in negative_returns) / n
    downside_dev = decimal_sqrt(downside_variance)

    if downside_dev == Decimal("0"):
        return Decimal("0")

    # Annualize
    annualized_downside = downside_dev * decimal_sqrt(trading_days)

    # Calculate annualized return
    mean_return = sum(returns, Decimal("0")) / n
    annualized_return = mean_return * trading_days

    return (annualized_return - risk_free_rate) / annualized_downside


def calculate_max_drawdown(values: list[Decimal]) -> Decimal:
    """Calculate maximum drawdown from an equity curve.

    Maximum drawdown is the largest peak-to-trough decline:
    max_dd = max((peak - trough) / peak) for all peaks and subsequent troughs

    Args:
        values: List of equity values over time

    Returns:
        Maximum drawdown as a decimal (0.1 = 10% drawdown)
    """
    if len(values) < 2:
        return Decimal("0")

    max_drawdown = Decimal("0")
    peak = values[0]

    for value in values:
        if value > peak:
            peak = value
        elif peak > Decimal("0"):
            drawdown = (peak - value) / peak
            if drawdown > max_drawdown:
                max_drawdown = drawdown

    return max_drawdown


def compute_cagr(total_return: Decimal, timestamps: list[datetime]) -> Decimal:
    """Compound annual growth rate (CAGR) from a total return and a time span.

    Shared by the USD metrics path (:func:`calculate_metrics`) and the
    numeraire reporting projection (``backtesting.numeraire``) so the two can
    never drift — both feed a ratio ``total_return`` (0.10 == 10%) and the
    equity-curve timestamps, and get back a ratio.

    Returns ``Decimal("0")`` when fewer than two timestamps or a non-positive
    span make a rate undefined. Caps at ``-1`` (-100%) when the portfolio lost
    >= 100%: the base ``(1 + total_return)`` is then ``<= 0`` and the
    non-integer exponentiation would be undefined.
    """
    if len(timestamps) < 2:
        return Decimal("0")
    duration_days = (timestamps[-1] - timestamps[0]).total_seconds() / (24 * 3600)
    if duration_days <= 0:
        return Decimal("0")
    years = Decimal(str(duration_days)) / Decimal("365")
    if years <= 0:
        return Decimal("0")
    if total_return <= Decimal("-1"):
        return Decimal("-1")
    return (Decimal("1") + total_return) ** (Decimal("1") / years) - Decimal("1")


def create_gas_price_summary(
    trades: list[TradeRecord],
) -> GasPriceSummary | None:
    """Create gas price summary from trade records.

    Calculates summary statistics for gas prices used during the backtest.
    This method uses the gas_price_gwei values from trades, which are
    always populated regardless of the track_gas_prices config setting.

    Args:
        trades: List of trade records from the backtest

    Returns:
        GasPriceSummary with min, max, mean, std of gas prices, or None if no trades
    """
    gas_prices = [t.gas_price_gwei for t in trades if t.gas_price_gwei is not None]
    if not gas_prices:
        return None

    # Calculate statistics
    min_gwei = min(gas_prices)
    max_gwei = max(gas_prices)
    mean_gwei = sum(gas_prices, Decimal("0")) / Decimal(len(gas_prices))

    # Calculate standard deviation
    if len(gas_prices) > 1:
        variance = sum((g - mean_gwei) ** 2 for g in gas_prices) / Decimal(len(gas_prices))
        std_gwei = decimal_sqrt(variance)
    else:
        std_gwei = Decimal("0")

    # Build source breakdown from trade metadata
    source_counts: dict[str, int] = {}
    for t in trades:
        if t.gas_price_gwei is not None:
            # Get source from metadata if available
            source = t.metadata.get("gas_price_source", "unknown") if t.metadata else "unknown"
            source_counts[source] = source_counts.get(source, 0) + 1

    return GasPriceSummary(
        min_gwei=min_gwei,
        max_gwei=max_gwei,
        mean_gwei=mean_gwei,
        std_gwei=std_gwei,
        source_breakdown=source_counts,
        total_records=len(gas_prices),
    )


def calculate_metrics(
    portfolio: SimulatedPortfolio,
    trades: list[TradeRecord],
    config: PnLBacktestConfig,
) -> BacktestMetrics:
    """Calculate comprehensive backtest metrics from portfolio and trades.

    This function consolidates metric calculations from the portfolio's equity
    curve and trade records, applying configuration settings such as:
    - Risk-free rate for Sharpe ratio calculation
    - Trading days per year for annualization

    The metrics calculated include:
    - PnL metrics: total_pnl_usd, net_pnl_usd, total_return_pct, annualized_return_pct
    - Risk metrics: sharpe_ratio, sortino_ratio, max_drawdown_pct, volatility, calmar_ratio
    - Trade metrics: win_rate, profit_factor, total_trades, winning_trades, losing_trades
    - Cost metrics: total_fees_usd, total_slippage_usd, total_gas_usd
    - Trade stats: avg_trade_pnl_usd, largest_win_usd, largest_loss_usd, avg_win_usd, avg_loss_usd

    Args:
        portfolio: SimulatedPortfolio with equity curve and trades
        trades: List of TradeRecord from the backtest
        config: PnLBacktestConfig with risk_free_rate and trading_days_per_year

    Returns:
        BacktestMetrics with all calculated performance metrics
    """
    if not portfolio.equity_curve:
        return BacktestMetrics()

    # Extract values for calculations
    equity_values = [p.value_usd for p in portfolio.equity_curve]
    timestamps = [p.timestamp for p in portfolio.equity_curve]

    # Initial and final values
    initial_value = equity_values[0] if equity_values else config.initial_capital_usd
    final_value = equity_values[-1] if equity_values else config.initial_capital_usd

    # Total PnL (before costs - costs are tracked separately)
    total_pnl = final_value - initial_value

    # Execution costs from trades
    total_fees = sum((t.fee_usd for t in trades), Decimal("0"))
    total_slippage = sum((t.slippage_usd for t in trades), Decimal("0"))
    total_gas = sum((t.gas_cost_usd for t in trades), Decimal("0"))

    # MEV costs from trades (only non-None values)
    total_mev = sum(
        (t.estimated_mev_cost_usd for t in trades if t.estimated_mev_cost_usd is not None),
        Decimal("0"),
    )

    # Gas price statistics from trades
    gas_prices = [t.gas_price_gwei for t in trades if t.gas_price_gwei is not None]
    avg_gas_price = Decimal("0")
    max_gas_price = Decimal("0")
    if gas_prices:
        avg_gas_price = sum(gas_prices, Decimal("0")) / Decimal(str(len(gas_prices)))
        max_gas_price = max(gas_prices)

    # Net PnL (same as total since costs are already reflected in equity)
    # The equity curve already accounts for costs deducted during execution
    net_pnl = total_pnl

    # Total return percentage
    total_return = Decimal("0")
    if initial_value > Decimal("0"):
        total_return = (final_value - initial_value) / initial_value

    # Calculate annualized return (CAGR) -- shared helper, see compute_cagr.
    annualized_return_value = compute_cagr(total_return, timestamps)

    # Calculate returns series for risk metrics
    returns = calculate_returns(equity_values)

    # Trading days per year from config (crypto = 365, stocks = 252)
    trading_days = Decimal(str(config.trading_days_per_year))

    # Volatility (annualized standard deviation of returns)
    volatility = calculate_volatility(returns, trading_days)

    # Sharpe ratio with risk-free rate from config
    sharpe = calculate_sharpe_ratio(
        returns=returns,
        volatility=volatility,
        risk_free_rate=config.risk_free_rate,
        trading_days=trading_days,
    )

    # Sortino ratio (downside risk-adjusted return)
    sortino = calculate_sortino_ratio(
        returns=returns,
        risk_free_rate=config.risk_free_rate,
        trading_days=trading_days,
    )

    # Maximum drawdown
    max_drawdown = calculate_max_drawdown(equity_values)

    # Calmar ratio (annualized return / max drawdown)
    calmar = Decimal("0")
    if max_drawdown > Decimal("0"):
        calmar = annualized_return_value / max_drawdown

    # Trade statistics (VIB-5083) -- see _compute_trade_statistics.
    stats = _compute_trade_statistics(trades)

    # Position-derived metrics (LP fee accrual, perp funding, lending interest,
    # health/margin extrema, realized/unrealized PnL). The engine result is
    # assembled from THIS function (engine._calculate_metrics ->
    # _engine_helpers.finalize_backtest_result), so without this the engine
    # result silently reported total_fees_earned_usd=0 / fees_by_pool={} on
    # every LP backtest even when fees accrued and were credited into equity at
    # close -- a reporting/KPI bug, not a value bug (VIB-5079 v1.1 reporting).
    # Sourced from the same SimulatedPortfolio helper get_metrics() uses so the
    # two metric paths can never drift apart again.
    pos = portfolio.aggregate_position_metrics()

    # VIB-2915: `total_return_pct` and `annualized_return_pct` are stored as actual
    # percentages (e.g. 10 for 10%), not decimal ratios. Local `total_return`/`annualized_return`
    # are kept as ratios to preserve the calmar/sharpe/sortino chain that divides by
    # `max_drawdown_pct` (still a ratio in this module).
    return BacktestMetrics(
        total_pnl_usd=total_pnl,
        net_pnl_usd=net_pnl,
        sharpe_ratio=sharpe,
        max_drawdown_pct=max_drawdown,
        win_rate=stats.win_rate,
        # Successful trades only -- failed fills are reported as failed_trades
        # and excluded from the performance denominator (VIB-5083, CodeRabbit).
        total_trades=len(trades) - stats.failed_trades,
        profit_factor=stats.profit_factor,
        total_return_pct=total_return * Decimal("100"),
        annualized_return_pct=annualized_return_value * Decimal("100"),
        total_fees_usd=total_fees,
        total_slippage_usd=total_slippage,
        total_gas_usd=total_gas,
        winning_trades=stats.winning_trades,
        losing_trades=stats.losing_trades,
        trades_with_realized_pnl=stats.trades_with_realized_pnl,
        failed_trades=stats.failed_trades,
        avg_trade_pnl_usd=stats.avg_trade_pnl,
        largest_win_usd=stats.largest_win,
        largest_loss_usd=stats.largest_loss,
        avg_win_usd=stats.avg_win,
        avg_loss_usd=stats.avg_loss,
        volatility=volatility,
        sortino_ratio=sortino,
        calmar_ratio=calmar,
        avg_gas_price_gwei=avg_gas_price,
        max_gas_price_gwei=max_gas_price,
        total_gas_cost_usd=total_gas,
        total_mev_cost_usd=total_mev,
        # Position-derived block -- see aggregate_position_metrics above.
        total_fees_earned_usd=pos.total_fees_earned_usd,
        fees_by_pool=pos.fees_by_pool,
        lp_fee_confidence_breakdown=pos.lp_fee_confidence_breakdown,
        total_funding_paid=pos.total_funding_paid,
        total_funding_received=pos.total_funding_received,
        total_interest_earned=pos.total_interest_earned,
        total_interest_paid=pos.total_interest_paid,
        max_margin_utilization=pos.max_margin_utilization,
        min_health_factor=pos.min_health_factor,
        health_factor_warnings=pos.health_factor_warnings,
        realized_pnl=pos.realized_pnl,
        unrealized_pnl=pos.unrealized_pnl,
        liquidations_count=pos.liquidations_count,
        liquidation_losses_usd=pos.liquidation_losses_usd,
    )
