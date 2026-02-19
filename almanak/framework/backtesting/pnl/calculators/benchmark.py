"""Benchmark performance calculators for portfolio analysis.

This module provides calculators for comparing strategy performance against
benchmarks, including:

- Information Ratio (IR): Risk-adjusted excess return vs benchmark
- Beta: Sensitivity to benchmark movements
- Alpha: Excess return beyond what beta would predict

These metrics help evaluate whether a strategy adds value compared to
passive investment alternatives.

Example:
    from almanak.framework.backtesting.pnl.calculators.benchmark import (
        BenchmarkCalculator,
        calculate_information_ratio,
        calculate_beta,
        calculate_alpha,
    )

    # Using the calculator class
    calc = BenchmarkCalculator()

    # Calculate IR
    ir = calc.calculate_information_ratio(
        strategy_returns=[Decimal("0.01"), Decimal("0.02"), Decimal("-0.005")],
        benchmark_returns=[Decimal("0.005"), Decimal("0.015"), Decimal("-0.002")],
    )

    # Calculate Beta
    beta = calc.calculate_beta(
        strategy_returns=[Decimal("0.01"), Decimal("0.02"), Decimal("-0.005")],
        benchmark_returns=[Decimal("0.005"), Decimal("0.015"), Decimal("-0.002")],
    )

    # Calculate Alpha (annualized)
    alpha = calc.calculate_alpha(
        strategy_return=Decimal("0.15"),  # 15% total return
        benchmark_return=Decimal("0.10"),  # 10% benchmark return
        beta=Decimal("1.2"),
        risk_free_rate=Decimal("0.05"),  # 5% risk-free rate
    )

References:
    - Information Ratio: https://www.investopedia.com/terms/i/informationratio.asp
    - Beta: https://www.investopedia.com/terms/b/beta.asp
    - Jensen's Alpha: https://www.investopedia.com/terms/j/jensensmeasure.asp
"""

from dataclasses import dataclass
from decimal import Decimal


@dataclass
class BenchmarkCalculator:
    """Calculator for benchmark comparison metrics.

    Provides methods to calculate key benchmark comparison metrics:
    - Information Ratio: Measures risk-adjusted excess return
    - Beta: Measures portfolio sensitivity to benchmark
    - Alpha: Measures excess return beyond beta-adjusted benchmark

    Attributes:
        annualization_factor: Number of periods per year for annualization.
            Default is 252 (trading days). Use 365 for daily data including
            weekends, 52 for weekly, 12 for monthly.

    Example:
        calc = BenchmarkCalculator(annualization_factor=252)

        # Calculate all benchmark metrics
        ir = calc.calculate_information_ratio(strategy_rets, benchmark_rets)
        beta = calc.calculate_beta(strategy_rets, benchmark_rets)
        alpha = calc.calculate_alpha(
            total_strategy_return,
            total_benchmark_return,
            beta,
            risk_free_rate,
        )
    """

    annualization_factor: int = 252

    def calculate_information_ratio(
        self,
        strategy_returns: list[Decimal],
        benchmark_returns: list[Decimal],
    ) -> Decimal:
        """Calculate the Information Ratio (IR).

        The Information Ratio measures the risk-adjusted excess return of a
        strategy relative to a benchmark. It is the ratio of the average
        excess return (alpha) to the tracking error (standard deviation of
        excess returns).

        IR = Mean(Strategy Return - Benchmark Return) / Std(Strategy Return - Benchmark Return)

        A higher IR indicates better risk-adjusted performance relative to
        the benchmark:
        - IR > 0.5: Good
        - IR > 0.75: Very Good
        - IR > 1.0: Exceptional

        Args:
            strategy_returns: List of strategy period returns (e.g., daily)
                as decimals (0.01 = 1%)
            benchmark_returns: List of benchmark period returns, same length
                and periods as strategy_returns

        Returns:
            Annualized Information Ratio. Returns Decimal("0") if:
            - Lists have different lengths
            - Fewer than 2 data points
            - Zero tracking error

        Example:
            # Daily returns for 5 days
            strategy = [Decimal("0.01"), Decimal("0.02"), Decimal("-0.01"),
                       Decimal("0.005"), Decimal("0.015")]
            benchmark = [Decimal("0.005"), Decimal("0.015"), Decimal("-0.005"),
                        Decimal("0.003"), Decimal("0.010")]
            ir = calc.calculate_information_ratio(strategy, benchmark)
        """
        # Validate inputs
        if len(strategy_returns) != len(benchmark_returns):
            return Decimal("0")

        if len(strategy_returns) < 2:
            return Decimal("0")

        # Calculate excess returns (strategy - benchmark)
        excess_returns = [s - b for s, b in zip(strategy_returns, benchmark_returns, strict=True)]

        # Calculate mean excess return
        mean_excess = sum(excess_returns) / Decimal(str(len(excess_returns)))

        # Calculate tracking error (std dev of excess returns)
        n = len(excess_returns)
        variance = sum((r - mean_excess) ** 2 for r in excess_returns) / Decimal(str(n - 1))
        tracking_error = _decimal_sqrt(variance)

        if tracking_error == 0:
            return Decimal("0")

        # Annualize: IR = (mean_excess * sqrt(ann_factor)) / tracking_error
        # Or equivalently: (mean_excess / tracking_error) * sqrt(ann_factor)
        annualized_ir = (mean_excess / tracking_error) * _decimal_sqrt(Decimal(str(self.annualization_factor)))

        return annualized_ir

    def calculate_beta(
        self,
        strategy_returns: list[Decimal],
        benchmark_returns: list[Decimal],
    ) -> Decimal:
        """Calculate portfolio Beta.

        Beta measures the sensitivity of the strategy's returns to benchmark
        movements. It is the covariance of strategy and benchmark returns
        divided by the variance of benchmark returns.

        Beta = Cov(Strategy, Benchmark) / Var(Benchmark)

        Interpretation:
        - Beta = 1.0: Strategy moves exactly with the benchmark
        - Beta > 1.0: Strategy is more volatile than benchmark
        - Beta < 1.0: Strategy is less volatile than benchmark
        - Beta < 0: Strategy moves inversely to benchmark (rare)

        Args:
            strategy_returns: List of strategy period returns as decimals
            benchmark_returns: List of benchmark period returns, same length

        Returns:
            Beta coefficient. Returns Decimal("0") if:
            - Lists have different lengths
            - Fewer than 2 data points
            - Zero benchmark variance

        Example:
            strategy = [Decimal("0.02"), Decimal("-0.01"), Decimal("0.03")]
            benchmark = [Decimal("0.01"), Decimal("-0.005"), Decimal("0.015")]
            beta = calc.calculate_beta(strategy, benchmark)
            # If strategy moves 2x as much as benchmark, beta ≈ 2.0
        """
        # Validate inputs
        if len(strategy_returns) != len(benchmark_returns):
            return Decimal("0")

        if len(strategy_returns) < 2:
            return Decimal("0")

        n = len(strategy_returns)

        # Calculate means
        strategy_mean = sum(strategy_returns) / Decimal(str(n))
        benchmark_mean = sum(benchmark_returns) / Decimal(str(n))

        # Calculate covariance (Cov = E[(X - μX)(Y - μY)])
        covariance = sum(
            (s - strategy_mean) * (b - benchmark_mean) for s, b in zip(strategy_returns, benchmark_returns, strict=True)
        ) / Decimal(str(n - 1))

        # Calculate benchmark variance
        benchmark_variance = sum((b - benchmark_mean) ** 2 for b in benchmark_returns) / Decimal(str(n - 1))

        if benchmark_variance == 0:
            return Decimal("0")

        # Beta = Cov(strategy, benchmark) / Var(benchmark)
        beta = covariance / benchmark_variance

        return beta

    def calculate_alpha(
        self,
        strategy_return: Decimal,
        benchmark_return: Decimal,
        beta: Decimal,
        risk_free_rate: Decimal = Decimal("0"),
    ) -> Decimal:
        """Calculate Jensen's Alpha.

        Alpha measures the excess return of a strategy beyond what would be
        predicted by its beta (market sensitivity). It represents the
        manager's skill in generating returns.

        Alpha = Strategy Return - [Risk-Free Rate + Beta * (Benchmark Return - Risk-Free Rate)]

        Or equivalently:
        Alpha = (Strategy Return - Risk-Free Rate) - Beta * (Benchmark Return - Risk-Free Rate)

        Interpretation:
        - Alpha > 0: Strategy outperformed beta-adjusted benchmark
        - Alpha < 0: Strategy underperformed beta-adjusted benchmark
        - Alpha = 0: Strategy performed as expected given its beta

        Args:
            strategy_return: Total strategy return over the period as decimal
                (0.15 = 15% return)
            benchmark_return: Total benchmark return over the same period
            beta: The portfolio beta (from calculate_beta)
            risk_free_rate: Risk-free rate for the period (default 0).
                Use treasury rate or similar (0.05 = 5%)

        Returns:
            Alpha as a decimal representing excess return.

        Example:
            # Strategy returned 20%, benchmark 12%, beta 1.5, risk-free 5%
            alpha = calc.calculate_alpha(
                strategy_return=Decimal("0.20"),
                benchmark_return=Decimal("0.12"),
                beta=Decimal("1.5"),
                risk_free_rate=Decimal("0.05"),
            )
            # Expected return = 0.05 + 1.5 * (0.12 - 0.05) = 0.155
            # Alpha = 0.20 - 0.155 = 0.045 (4.5% excess return)
        """
        # Expected return based on CAPM
        # E[R] = Rf + Beta * (Rm - Rf)
        # Where:
        #   Rf = risk-free rate
        #   Rm = benchmark (market) return
        #   Beta = portfolio beta
        expected_return = risk_free_rate + beta * (benchmark_return - risk_free_rate)

        # Alpha = Actual Return - Expected Return
        alpha = strategy_return - expected_return

        return alpha


def _decimal_sqrt(n: Decimal, precision: int = 28) -> Decimal:
    """Calculate square root of a Decimal using Newton's method.

    Args:
        n: Non-negative Decimal to find square root of
        precision: Number of decimal places for convergence

    Returns:
        Square root as Decimal
    """
    if n < 0:
        raise ValueError("Cannot calculate square root of negative number")
    if n == 0:
        return Decimal("0")

    # Newton's method: x_{n+1} = (x_n + n/x_n) / 2
    x = n
    two = Decimal("2")

    for _ in range(100):
        x_next = (x + n / x) / two
        if abs(x_next - x) < Decimal(f"1e-{precision}"):
            break
        x = x_next

    return x


# Convenience functions for direct use without instantiating the class


def calculate_information_ratio(
    strategy_returns: list[Decimal],
    benchmark_returns: list[Decimal],
    annualization_factor: int = 252,
) -> Decimal:
    """Calculate the Information Ratio (convenience function).

    See BenchmarkCalculator.calculate_information_ratio for full documentation.

    Args:
        strategy_returns: List of strategy period returns
        benchmark_returns: List of benchmark period returns
        annualization_factor: Periods per year (252 for daily trading days)

    Returns:
        Annualized Information Ratio
    """
    calc = BenchmarkCalculator(annualization_factor=annualization_factor)
    return calc.calculate_information_ratio(strategy_returns, benchmark_returns)


def calculate_beta(
    strategy_returns: list[Decimal],
    benchmark_returns: list[Decimal],
) -> Decimal:
    """Calculate portfolio Beta (convenience function).

    See BenchmarkCalculator.calculate_beta for full documentation.

    Args:
        strategy_returns: List of strategy period returns
        benchmark_returns: List of benchmark period returns

    Returns:
        Beta coefficient
    """
    calc = BenchmarkCalculator()
    return calc.calculate_beta(strategy_returns, benchmark_returns)


def calculate_alpha(
    strategy_return: Decimal,
    benchmark_return: Decimal,
    beta: Decimal,
    risk_free_rate: Decimal = Decimal("0"),
) -> Decimal:
    """Calculate Jensen's Alpha (convenience function).

    See BenchmarkCalculator.calculate_alpha for full documentation.

    Args:
        strategy_return: Total strategy return
        benchmark_return: Total benchmark return
        beta: Portfolio beta
        risk_free_rate: Risk-free rate (default 0)

    Returns:
        Alpha (excess return)
    """
    calc = BenchmarkCalculator()
    return calc.calculate_alpha(strategy_return, benchmark_return, beta, risk_free_rate)


__all__ = [
    "BenchmarkCalculator",
    "calculate_information_ratio",
    "calculate_beta",
    "calculate_alpha",
]
