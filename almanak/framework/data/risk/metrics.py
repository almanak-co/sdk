"""Portfolio risk metrics with explicit conventions.

Provides Sharpe ratio, Sortino ratio, Value-at-Risk (VaR), Conditional VaR
(CVaR), and drawdown calculations. All results carry a RiskConventions
dataclass so that return interval, risk-free rate, annualization factor,
and observation count are always explicit and unambiguous.

Example:
    from almanak.framework.data.risk.metrics import PortfolioRiskCalculator

    calc = PortfolioRiskCalculator()
    risk = calc.portfolio_risk(pnl_series, total_value_usd=Decimal("100000"))
    print(f"Sharpe: {risk.sharpe_ratio:.2f}")
    print(f"VaR 95%: ${risk.var_95}")
    print(f"Max drawdown: {risk.max_drawdown:.2%}")
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum

from almanak.framework.data.interfaces import InsufficientDataError

logger = logging.getLogger(__name__)

# Minimum observations required for valid risk metric estimation.
MIN_OBSERVATIONS = 30

# Annualization factors: periods per year for each return interval.
_PERIODS_PER_YEAR: dict[str, int] = {
    "1m": 525_600,
    "5m": 105_120,
    "15m": 35_040,
    "1h": 8_760,
    "4h": 2_190,
    "1d": 365,
    "1w": 52,
}


class VaRMethod(Enum):
    """Value-at-Risk calculation method."""

    PARAMETRIC = "parametric"
    HISTORICAL = "historical"
    CORNISH_FISHER = "cornish_fisher"


@dataclass(frozen=True)
class RiskConventions:
    """Explicit conventions making risk metric calculations unambiguous.

    Every PortfolioRisk result carries this dataclass so callers always
    know exactly how the metrics were computed.

    Attributes:
        return_interval: Periodicity of the return series (e.g. "1d", "1h").
        risk_free_rate: Risk-free rate per period as a decimal (e.g. 0.0 for 0%).
        annualization_factor: sqrt(periods_per_year) used for annualizing.
        sample_count: Number of return observations used.
        window_start: Timestamp of the first observation.
        window_end: Timestamp of the last observation.
    """

    return_interval: str
    risk_free_rate: Decimal
    annualization_factor: float
    sample_count: int
    window_start: datetime
    window_end: datetime


@dataclass(frozen=True)
class PortfolioRisk:
    """Portfolio risk metrics with explicit conventions.

    All ratio fields (sharpe, sortino, drawdown, beta) are floats.
    All monetary fields (var, cvar, total_value_usd) are Decimal.

    Attributes:
        total_value_usd: Current portfolio value in USD.
        sharpe_ratio: Annualized Sharpe ratio (excess return / std dev).
        sortino_ratio: Annualized Sortino ratio (excess return / downside dev).
        max_drawdown: Maximum peak-to-trough decline as a fraction (0.0 - 1.0).
        current_drawdown: Current decline from high-water mark as a fraction.
        var_95: Value-at-Risk at 95% confidence (1-period loss amount in USD).
        cvar_95: Conditional VaR at 95% (expected loss beyond VaR, in USD).
        var_method: Method used for VaR calculation.
        beta_to_eth: Beta to ETH (correlation * vol_portfolio / vol_eth). None if not computed.
        beta_to_btc: Beta to BTC. None if not computed.
        correlation_matrix: Pairwise correlations. Empty dict if not computed.
        conventions: Explicit conventions used for the calculations.
    """

    total_value_usd: Decimal
    sharpe_ratio: float
    sortino_ratio: float
    max_drawdown: float
    current_drawdown: float
    var_95: Decimal
    cvar_95: Decimal
    var_method: str
    beta_to_eth: float | None
    beta_to_btc: float | None
    correlation_matrix: dict[str, dict[str, float]]
    conventions: RiskConventions


@dataclass(frozen=True)
class RollingSharpeEntry:
    """Single point in a rolling Sharpe time series.

    Attributes:
        timestamp: End of the rolling window.
        sharpe: Annualized Sharpe ratio for this window.
        sample_count: Number of observations in the window.
    """

    timestamp: datetime
    sharpe: float
    sample_count: int


@dataclass(frozen=True)
class RollingSharpeResult:
    """Rolling Sharpe ratio time series.

    Attributes:
        entries: List of RollingSharpeEntry.
        window_days: Rolling window length in days.
        return_interval: Return periodicity used.
        risk_free_rate: Risk-free rate per period.
    """

    entries: list[RollingSharpeEntry]
    window_days: int
    return_interval: str
    risk_free_rate: Decimal


class PortfolioRiskCalculator:
    """Computes portfolio risk metrics from PnL or return series.

    All calculations use explicit conventions (return interval, risk-free rate,
    annualization) to ensure results are unambiguous and comparable.

    Supports three VaR methods:
    - **parametric**: Assumes normal distribution. VaR = -z * sigma * value.
    - **historical**: Percentile-based from empirical return distribution.
    - **cornish_fisher**: Adjusts z-score for skewness and kurtosis.
    """

    def portfolio_risk(
        self,
        pnl_series: list[float],
        total_value_usd: Decimal,
        return_interval: str = "1d",
        risk_free_rate: Decimal = Decimal("0"),
        var_method: VaRMethod = VaRMethod.PARAMETRIC,
        timestamps: list[datetime] | None = None,
        benchmark_eth_returns: list[float] | None = None,
        benchmark_btc_returns: list[float] | None = None,
    ) -> PortfolioRisk:
        """Calculate portfolio risk metrics from a PnL series.

        The pnl_series should contain periodic returns as fractions
        (e.g. 0.01 = 1% gain, -0.02 = 2% loss).

        Args:
            pnl_series: List of periodic returns (fractions, not percentages).
            total_value_usd: Current portfolio value in USD.
            return_interval: Periodicity of the returns (1d, 1h, etc.).
            risk_free_rate: Risk-free rate per period as a decimal.
            var_method: VaR calculation method.
            timestamps: Optional timestamps for each return (for conventions).
            benchmark_eth_returns: Optional ETH returns for beta calculation.
            benchmark_btc_returns: Optional BTC returns for beta calculation.

        Returns:
            PortfolioRisk with all metrics and explicit conventions.

        Raises:
            InsufficientDataError: If fewer than 30 observations.
            ValueError: If return_interval is unsupported.
        """
        if return_interval not in _PERIODS_PER_YEAR:
            raise ValueError(
                f"Unsupported return_interval '{return_interval}'. Supported: {sorted(_PERIODS_PER_YEAR.keys())}"
            )

        n = len(pnl_series)
        if n < MIN_OBSERVATIONS:
            raise InsufficientDataError(
                required=MIN_OBSERVATIONS,
                available=n,
                indicator="portfolio_risk",
            )

        periods_per_year = _PERIODS_PER_YEAR[return_interval]
        ann_factor = math.sqrt(periods_per_year)
        rf = float(risk_free_rate)

        # Excess returns
        excess = [r - rf for r in pnl_series]

        # Sharpe ratio
        sharpe = self._sharpe(excess, ann_factor)

        # Sortino ratio
        sortino = self._sortino(excess, rf, ann_factor)

        # Drawdown
        max_dd, current_dd = self._drawdown(pnl_series)

        # VaR and CVaR
        var_95, cvar_95 = self._var_cvar(pnl_series, total_value_usd, var_method)

        # Beta calculations
        beta_eth = self._beta(pnl_series, benchmark_eth_returns) if benchmark_eth_returns else None
        beta_btc = self._beta(pnl_series, benchmark_btc_returns) if benchmark_btc_returns else None

        # Build conventions
        now = datetime.now()
        window_start = timestamps[0] if timestamps and len(timestamps) > 0 else now
        window_end = timestamps[-1] if timestamps and len(timestamps) > 0 else now

        conventions = RiskConventions(
            return_interval=return_interval,
            risk_free_rate=risk_free_rate,
            annualization_factor=ann_factor,
            sample_count=n,
            window_start=window_start,
            window_end=window_end,
        )

        return PortfolioRisk(
            total_value_usd=total_value_usd,
            sharpe_ratio=sharpe,
            sortino_ratio=sortino,
            max_drawdown=max_dd,
            current_drawdown=current_dd,
            var_95=var_95,
            cvar_95=cvar_95,
            var_method=var_method.value,
            beta_to_eth=beta_eth,
            beta_to_btc=beta_btc,
            correlation_matrix={},
            conventions=conventions,
        )

    def rolling_sharpe(
        self,
        pnl_series: list[float],
        window_days: int = 30,
        return_interval: str = "1d",
        risk_free_rate: Decimal = Decimal("0"),
        timestamps: list[datetime] | None = None,
    ) -> RollingSharpeResult:
        """Compute rolling Sharpe ratio over the PnL series.

        Args:
            pnl_series: List of periodic returns.
            window_days: Rolling window in days.
            return_interval: Periodicity of the returns.
            risk_free_rate: Risk-free rate per period.
            timestamps: Optional timestamps aligned with pnl_series.

        Returns:
            RollingSharpeResult with time series of Sharpe ratios.

        Raises:
            InsufficientDataError: If fewer than 30 observations total.
            ValueError: If return_interval is unsupported.
        """
        if return_interval not in _PERIODS_PER_YEAR:
            raise ValueError(
                f"Unsupported return_interval '{return_interval}'. Supported: {sorted(_PERIODS_PER_YEAR.keys())}"
            )

        n = len(pnl_series)
        if n < MIN_OBSERVATIONS:
            raise InsufficientDataError(
                required=MIN_OBSERVATIONS,
                available=n,
                indicator="rolling_sharpe",
            )

        periods_per_year = _PERIODS_PER_YEAR[return_interval]
        ann_factor = math.sqrt(periods_per_year)
        rf = float(risk_free_rate)

        # Convert window_days to number of periods.
        hours_per_period = {
            "1m": 1 / 60,
            "5m": 5 / 60,
            "15m": 0.25,
            "1h": 1.0,
            "4h": 4.0,
            "1d": 24.0,
            "1w": 168.0,
        }
        periods_per_window = max(int(window_days * 24 / hours_per_period[return_interval]), MIN_OBSERVATIONS)

        entries: list[RollingSharpeEntry] = []
        now = datetime.now()

        for end_idx in range(periods_per_window, n + 1):
            start_idx = end_idx - periods_per_window
            window = pnl_series[start_idx:end_idx]
            excess = [r - rf for r in window]
            sharpe = self._sharpe(excess, ann_factor)

            ts = timestamps[end_idx - 1] if timestamps and end_idx - 1 < len(timestamps) else now

            entries.append(
                RollingSharpeEntry(
                    timestamp=ts,
                    sharpe=sharpe,
                    sample_count=len(window),
                )
            )

        return RollingSharpeResult(
            entries=entries,
            window_days=window_days,
            return_interval=return_interval,
            risk_free_rate=risk_free_rate,
        )

    # -----------------------------------------------------------------
    # Private helpers
    # -----------------------------------------------------------------

    def _sharpe(self, excess_returns: list[float], ann_factor: float) -> float:
        """Compute annualized Sharpe ratio from excess returns."""
        n = len(excess_returns)
        if n < 2:
            return 0.0
        mean = sum(excess_returns) / n
        variance = sum((r - mean) ** 2 for r in excess_returns) / (n - 1)
        std = math.sqrt(variance)
        if std < 1e-15:
            return 0.0
        return (mean / std) * ann_factor

    def _sortino(self, excess_returns: list[float], rf: float, ann_factor: float) -> float:
        """Compute annualized Sortino ratio.

        Uses downside deviation: std dev of returns below risk-free rate.
        """
        n = len(excess_returns)
        if n < 2:
            return 0.0
        mean_excess = sum(excess_returns) / n

        # Downside squared deviations (only negative excess returns).
        downside_sq = [r**2 for r in excess_returns if r < 0]
        if not downside_sq:
            # No downside observations: infinite Sortino, cap at a large value.
            return float("inf") if mean_excess > 0 else 0.0

        downside_variance = sum(downside_sq) / n  # Full sample denominator
        downside_dev = math.sqrt(downside_variance)
        if downside_dev < 1e-15:
            return 0.0
        return (mean_excess / downside_dev) * ann_factor

    def _drawdown(self, returns: list[float]) -> tuple[float, float]:
        """Compute max drawdown and current drawdown from returns series.

        Builds a cumulative wealth index (starting at 1.0), tracks high-water
        mark, and reports drawdowns as fractions (0.0 = no drawdown, 1.0 = total loss).

        Returns:
            (max_drawdown, current_drawdown) as positive fractions.
        """
        if not returns:
            return 0.0, 0.0

        cumulative = 1.0
        high_water = 1.0
        max_dd = 0.0

        for r in returns:
            cumulative *= 1.0 + r
            if cumulative > high_water:
                high_water = cumulative
            dd = (high_water - cumulative) / high_water if high_water > 0 else 0.0
            if dd > max_dd:
                max_dd = dd

        current_dd = (high_water - cumulative) / high_water if high_water > 0 else 0.0
        return max_dd, current_dd

    def _var_cvar(
        self,
        returns: list[float],
        total_value: Decimal,
        method: VaRMethod,
    ) -> tuple[Decimal, Decimal]:
        """Compute 1-period 95% VaR and CVaR.

        VaR is reported as a positive Decimal representing the loss amount.
        CVaR (expected shortfall) is the average loss beyond VaR.

        Returns:
            (var_95, cvar_95) as positive Decimal amounts.
        """
        n = len(returns)
        if n < 2:
            return Decimal("0"), Decimal("0")

        sorted_returns = sorted(returns)
        value = float(total_value)

        if method == VaRMethod.HISTORICAL:
            # 5th percentile index (lower tail).
            idx = max(int(n * 0.05), 0)
            var_return = -sorted_returns[idx]
            # CVaR: average of returns at or below the 5th percentile.
            tail = sorted_returns[: idx + 1]
            cvar_return = -sum(tail) / len(tail) if tail else var_return

        elif method == VaRMethod.CORNISH_FISHER:
            mean = sum(returns) / n
            variance = sum((r - mean) ** 2 for r in returns) / (n - 1)
            std = math.sqrt(variance)
            skew = self._skewness(returns, mean, std, n)
            kurt = self._excess_kurtosis(returns, mean, std, n)

            # Cornish-Fisher expansion: adjusted z-score.
            z = 1.6449  # 95% quantile of standard normal
            z_cf = z + (z**2 - 1) * skew / 6 + (z**3 - 3 * z) * kurt / 24 - (2 * z**3 - 5 * z) * skew**2 / 36

            var_return = -(mean - z_cf * std)
            # CVaR approximation: use parametric formula with adjusted z.
            cvar_z = self._parametric_cvar_z(z_cf)
            cvar_return = -(mean - cvar_z * std)

        else:
            # Parametric (normal distribution assumption).
            mean = sum(returns) / n
            variance = sum((r - mean) ** 2 for r in returns) / (n - 1)
            std = math.sqrt(variance)

            z = 1.6449  # 95% quantile
            var_return = -(mean - z * std)
            # CVaR under normality: E[X | X < -VaR] = mean - std * phi(z) / (1 - Phi(z))
            # phi(z) / (1 - alpha) where alpha = 0.05
            cvar_z = self._parametric_cvar_z(z)
            cvar_return = -(mean - cvar_z * std)

        # Ensure non-negative (loss amounts are positive).
        var_amount = Decimal(str(max(var_return, 0.0) * value))
        cvar_amount = Decimal(str(max(cvar_return, 0.0) * value))

        return var_amount, cvar_amount

    def _parametric_cvar_z(self, z: float) -> float:
        """Compute the CVaR z-equivalent under normality.

        CVaR z-factor = phi(z) / alpha where alpha = 0.05 and phi is the
        standard normal PDF.
        """
        alpha = 0.05
        phi_z = math.exp(-0.5 * z**2) / math.sqrt(2 * math.pi)
        return phi_z / alpha

    def _skewness(self, returns: list[float], mean: float, std: float, n: int) -> float:
        """Compute sample skewness."""
        if std < 1e-15 or n < 3:
            return 0.0
        m3 = sum((r - mean) ** 3 for r in returns) / n
        return m3 / (std**3)

    def _excess_kurtosis(self, returns: list[float], mean: float, std: float, n: int) -> float:
        """Compute sample excess kurtosis (kurtosis - 3)."""
        if std < 1e-15 or n < 4:
            return 0.0
        m4 = sum((r - mean) ** 4 for r in returns) / n
        return m4 / (std**4) - 3.0

    def _beta(self, portfolio_returns: list[float], benchmark_returns: list[float]) -> float | None:
        """Compute beta of portfolio returns against benchmark returns.

        Beta = Cov(portfolio, benchmark) / Var(benchmark).
        Returns None if series lengths don't match.
        """
        n = min(len(portfolio_returns), len(benchmark_returns))
        if n < MIN_OBSERVATIONS:
            return None

        p = portfolio_returns[-n:]
        b = benchmark_returns[-n:]

        p_mean = sum(p) / n
        b_mean = sum(b) / n

        cov = sum((p[i] - p_mean) * (b[i] - b_mean) for i in range(n)) / (n - 1)
        b_var = sum((b[i] - b_mean) ** 2 for i in range(n)) / (n - 1)

        if b_var < 1e-15:
            return None

        return cov / b_var
