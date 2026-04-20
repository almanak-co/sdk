"""Metrics calculation for paper trading.

Provides standalone functions for calculating paper trading performance
metrics. These are simplified versions of the PnL metrics, tuned for
paper trading's hourly execution cadence.

Note: While similar to pnl/metrics_calculator.py, the paper trading
versions differ in annualization assumptions (hourly vs daily) and
available metrics (no sortino, no gas price summary).

Extracted from paper/engine.py for module size management.
"""

from decimal import Decimal

from almanak.framework.backtesting.pnl.metrics_calculator import (
    calculate_max_drawdown,
    calculate_returns,
    decimal_sqrt,
)


def calculate_volatility(returns: list[Decimal]) -> Decimal:
    """Calculate volatility (standard deviation) of returns.

    Unlike the PnL version, this does not annualize -- annualization
    is handled by the caller in calculate_sharpe_ratio.

    Args:
        returns: List of period returns

    Returns:
        Standard deviation of returns
    """
    if len(returns) < 2:
        return Decimal("0")

    n = Decimal(str(len(returns)))
    mean = sum(returns, Decimal("0")) / n
    squared_diffs = sum((r - mean) ** 2 for r in returns)
    variance = squared_diffs / (n - Decimal("1"))

    return decimal_sqrt(variance)


def calculate_sharpe_ratio(
    returns: list[Decimal],
    volatility: Decimal,
) -> Decimal:
    """Calculate Sharpe ratio for paper trading.

    Annualizes assuming hourly returns (8760 hours per year).

    Args:
        returns: List of period returns
        volatility: Non-annualized volatility (std dev of returns)

    Returns:
        Annualized Sharpe ratio
    """
    if volatility == Decimal("0") or not returns:
        return Decimal("0")

    n = Decimal(str(len(returns)))
    mean_return = sum(returns, Decimal("0")) / n

    # Annualize (assuming hourly returns for paper trading)
    annualized_return = mean_return * Decimal("8760")  # Hours per year
    annualized_vol = volatility * decimal_sqrt(Decimal("8760"))

    if annualized_vol == Decimal("0"):
        return Decimal("0")

    return annualized_return / annualized_vol


# Re-export shared functions from pnl.metrics_calculator
__all__ = [
    "calculate_max_drawdown",
    "calculate_returns",
    "calculate_sharpe_ratio",
    "calculate_volatility",
    "decimal_sqrt",
]
