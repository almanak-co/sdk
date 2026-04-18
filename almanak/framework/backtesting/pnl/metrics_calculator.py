"""Metrics calculation for PnL backtesting.

Provides standalone functions for calculating backtest performance metrics
including returns, volatility, Sharpe ratio, Sortino ratio, max drawdown,
and gas price summaries.

These are pure math functions that operate on equity curves, trade records,
and configuration parameters.

Extracted from pnl/engine.py for module size management.
"""

from decimal import Decimal

from almanak.framework.backtesting.models import (
    BacktestMetrics,
    GasPriceSummary,
    TradeRecord,
)
from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
from almanak.framework.backtesting.pnl.portfolio import SimulatedPortfolio


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

    # Calculate annualized return
    annualized_return = Decimal("0")
    if len(timestamps) >= 2:
        duration_days = (timestamps[-1] - timestamps[0]).total_seconds() / (24 * 3600)
        if duration_days > 0:
            years = Decimal(str(duration_days)) / Decimal("365")
            if years > 0:
                # Compound annual growth rate (CAGR)
                # (1 + total_return) ^ (1/years) - 1
                if total_return <= Decimal("-1"):
                    # Portfolio lost >= 100% (e.g. gas costs exceed principal).
                    # The base (1 + total_return) is <= 0, so exponentiation is
                    # undefined for non-integer exponents. Cap at -100%.
                    annualized_return = Decimal("-1")
                else:
                    annualized_return = (Decimal("1") + total_return) ** (Decimal("1") / years) - Decimal("1")

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
        calmar = annualized_return / max_drawdown

    # Trade statistics
    winning_trades = [t for t in trades if t.net_pnl_usd > Decimal("0")]
    losing_trades = [t for t in trades if t.net_pnl_usd <= Decimal("0")]

    # Win rate
    win_rate = Decimal("0")
    if trades:
        win_rate = Decimal(str(len(winning_trades))) / Decimal(str(len(trades)))

    # Profit factor (gross profit / gross loss)
    gross_profit = sum((t.net_pnl_usd for t in winning_trades), Decimal("0"))
    gross_loss_sum = sum((t.net_pnl_usd for t in losing_trades), Decimal("0"))
    gross_loss = abs(gross_loss_sum)
    profit_factor = Decimal("0")
    if gross_loss > Decimal("0"):
        profit_factor = gross_profit / gross_loss

    # Average trade PnL
    avg_trade_pnl = Decimal("0")
    if trades:
        total_trade_pnl = sum((t.net_pnl_usd for t in trades), Decimal("0"))
        avg_trade_pnl = total_trade_pnl / Decimal(str(len(trades)))

    # Largest win and loss
    trade_pnls = [t.net_pnl_usd for t in trades]
    largest_win = max(trade_pnls, default=Decimal("0"))
    largest_loss = min(trade_pnls, default=Decimal("0"))

    # Average win and loss
    avg_win = Decimal("0")
    if winning_trades:
        winning_pnl_sum = sum((t.net_pnl_usd for t in winning_trades), Decimal("0"))
        avg_win = winning_pnl_sum / Decimal(str(len(winning_trades)))

    avg_loss = Decimal("0")
    if losing_trades:
        losing_pnl_sum = sum((t.net_pnl_usd for t in losing_trades), Decimal("0"))
        avg_loss = losing_pnl_sum / Decimal(str(len(losing_trades)))

    # VIB-2915: `total_return_pct` and `annualized_return_pct` are stored as actual
    # percentages (e.g. 10 for 10%), not decimal ratios. Local `total_return`/`annualized_return`
    # are kept as ratios to preserve the calmar/sharpe/sortino chain that divides by
    # `max_drawdown_pct` (still a ratio in this module).
    return BacktestMetrics(
        total_pnl_usd=total_pnl,
        net_pnl_usd=net_pnl,
        sharpe_ratio=sharpe,
        max_drawdown_pct=max_drawdown,
        win_rate=win_rate,
        total_trades=len(trades),
        profit_factor=profit_factor,
        total_return_pct=total_return * Decimal("100"),
        annualized_return_pct=annualized_return * Decimal("100"),
        total_fees_usd=total_fees,
        total_slippage_usd=total_slippage,
        total_gas_usd=total_gas,
        winning_trades=len(winning_trades),
        losing_trades=len(losing_trades),
        avg_trade_pnl_usd=avg_trade_pnl,
        largest_win_usd=largest_win,
        largest_loss_usd=largest_loss,
        avg_win_usd=avg_win,
        avg_loss_usd=avg_loss,
        volatility=volatility,
        sortino_ratio=sortino,
        calmar_ratio=calmar,
        avg_gas_price_gwei=avg_gas_price,
        max_gas_price_gwei=max_gas_price,
        total_gas_cost_usd=total_gas,
        total_mev_cost_usd=total_mev,
    )
