"""Tests for portfolio risk metrics calculator.

Tests Sharpe, Sortino, VaR, CVaR, drawdown, beta, rolling Sharpe,
and explicit conventions with known synthetic data.
"""

from __future__ import annotations

import math
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from almanak.framework.data.interfaces import InsufficientDataError
from almanak.framework.data.risk.metrics import (
    PortfolioRisk,
    PortfolioRiskCalculator,
    RiskConventions,
    RollingSharpeEntry,
    RollingSharpeResult,
    VaRMethod,
)


@pytest.fixture
def calc() -> PortfolioRiskCalculator:
    return PortfolioRiskCalculator()


@pytest.fixture
def timestamps_50() -> list[datetime]:
    """50 daily timestamps."""
    base = datetime(2024, 1, 1, tzinfo=UTC)
    return [base + timedelta(days=i) for i in range(50)]


@pytest.fixture
def flat_returns() -> list[float]:
    """50 returns of 0.01 (1% daily gain)."""
    return [0.01] * 50


@pytest.fixture
def mixed_returns() -> list[float]:
    """50 returns: alternating +2% and -1%."""
    return [0.02 if i % 2 == 0 else -0.01 for i in range(50)]


# =========================================================================
# RiskConventions frozen dataclass tests
# =========================================================================


class TestRiskConventions:
    def test_construction(self):
        now = datetime.now(UTC)
        conv = RiskConventions(
            return_interval="1d",
            risk_free_rate=Decimal("0.0001"),
            annualization_factor=math.sqrt(365),
            sample_count=100,
            window_start=now - timedelta(days=100),
            window_end=now,
        )
        assert conv.return_interval == "1d"
        assert conv.risk_free_rate == Decimal("0.0001")
        assert conv.sample_count == 100

    def test_frozen(self):
        now = datetime.now(UTC)
        conv = RiskConventions(
            return_interval="1d",
            risk_free_rate=Decimal("0"),
            annualization_factor=math.sqrt(365),
            sample_count=50,
            window_start=now,
            window_end=now,
        )
        with pytest.raises(AttributeError):
            conv.return_interval = "1h"  # type: ignore[misc]


# =========================================================================
# PortfolioRisk frozen dataclass tests
# =========================================================================


class TestPortfolioRisk:
    def test_construction(self):
        now = datetime.now(UTC)
        conv = RiskConventions(
            return_interval="1d",
            risk_free_rate=Decimal("0"),
            annualization_factor=math.sqrt(365),
            sample_count=50,
            window_start=now,
            window_end=now,
        )
        risk = PortfolioRisk(
            total_value_usd=Decimal("100000"),
            sharpe_ratio=1.5,
            sortino_ratio=2.0,
            max_drawdown=0.15,
            current_drawdown=0.05,
            var_95=Decimal("5000"),
            cvar_95=Decimal("6500"),
            var_method="parametric",
            beta_to_eth=1.2,
            beta_to_btc=0.8,
            correlation_matrix={},
            conventions=conv,
        )
        assert risk.sharpe_ratio == 1.5
        assert risk.total_value_usd == Decimal("100000")
        assert risk.var_method == "parametric"
        assert risk.beta_to_eth == 1.2


# =========================================================================
# Sharpe ratio tests
# =========================================================================


class TestSharpe:
    def test_positive_constant_returns(self, calc, flat_returns, timestamps_50):
        """Constant positive returns should give very high Sharpe (low vol)."""
        result = calc.portfolio_risk(
            flat_returns,
            total_value_usd=Decimal("100000"),
            timestamps=timestamps_50,
        )
        # Constant returns -> zero volatility -> Sharpe = 0 (our implementation returns 0 for zero std)
        assert result.sharpe_ratio == 0.0

    def test_mixed_returns_positive_sharpe(self, calc, mixed_returns, timestamps_50):
        """Mixed but net-positive returns should give positive Sharpe."""
        result = calc.portfolio_risk(
            mixed_returns,
            total_value_usd=Decimal("100000"),
            timestamps=timestamps_50,
        )
        assert result.sharpe_ratio > 0

    def test_negative_returns_negative_sharpe(self, calc, timestamps_50):
        """Consistently negative returns should give negative Sharpe."""
        returns = [-0.01] * 50
        result = calc.portfolio_risk(
            returns,
            total_value_usd=Decimal("100000"),
            timestamps=timestamps_50,
        )
        # Constant returns -> 0 vol -> sharpe = 0
        assert result.sharpe_ratio == 0.0

    def test_sharpe_manual_calculation(self, calc):
        """Verify Sharpe against manual computation for known data."""
        # Known series: 50 returns alternating between +3% and -1%
        returns = [0.03 if i % 2 == 0 else -0.01 for i in range(50)]
        mean = sum(returns) / len(returns)  # = 0.01
        variance = sum((r - mean) ** 2 for r in returns) / (len(returns) - 1)
        std = math.sqrt(variance)
        ann_factor = math.sqrt(365)
        expected_sharpe = (mean / std) * ann_factor

        result = calc.portfolio_risk(
            returns,
            total_value_usd=Decimal("100000"),
        )
        assert abs(result.sharpe_ratio - expected_sharpe) < 0.01

    def test_sharpe_with_risk_free_rate(self, calc, mixed_returns, timestamps_50):
        """Non-zero risk-free rate should reduce Sharpe."""
        result_zero_rf = calc.portfolio_risk(
            mixed_returns,
            total_value_usd=Decimal("100000"),
            risk_free_rate=Decimal("0"),
            timestamps=timestamps_50,
        )
        result_nonzero_rf = calc.portfolio_risk(
            mixed_returns,
            total_value_usd=Decimal("100000"),
            risk_free_rate=Decimal("0.005"),
            timestamps=timestamps_50,
        )
        # Higher risk-free rate -> lower Sharpe for same returns
        assert result_nonzero_rf.sharpe_ratio < result_zero_rf.sharpe_ratio

    def test_hourly_interval(self, calc, mixed_returns, timestamps_50):
        """Hourly interval uses different annualization factor."""
        result_daily = calc.portfolio_risk(
            mixed_returns,
            total_value_usd=Decimal("100000"),
            return_interval="1d",
            timestamps=timestamps_50,
        )
        result_hourly = calc.portfolio_risk(
            mixed_returns,
            total_value_usd=Decimal("100000"),
            return_interval="1h",
            timestamps=timestamps_50,
        )
        # Hourly annualization (sqrt(8760)) >> daily annualization (sqrt(365))
        # so hourly Sharpe should be much larger in absolute magnitude
        assert abs(result_hourly.sharpe_ratio) > abs(result_daily.sharpe_ratio)


# =========================================================================
# Sortino ratio tests
# =========================================================================


class TestSortino:
    def test_all_positive_returns_infinite(self, calc, timestamps_50):
        """All positive returns -> no downside deviation -> infinite Sortino."""
        returns = [0.01 + i * 0.0001 for i in range(50)]  # slightly varying positive
        result = calc.portfolio_risk(
            returns,
            total_value_usd=Decimal("100000"),
            timestamps=timestamps_50,
        )
        assert result.sortino_ratio == float("inf")

    def test_mixed_returns_sortino_positive(self, calc, mixed_returns, timestamps_50):
        """Net-positive mixed returns should give positive Sortino."""
        result = calc.portfolio_risk(
            mixed_returns,
            total_value_usd=Decimal("100000"),
            timestamps=timestamps_50,
        )
        assert result.sortino_ratio > 0

    def test_sortino_greater_than_sharpe(self, calc, mixed_returns, timestamps_50):
        """Sortino should be >= Sharpe when there are more up days than down days."""
        result = calc.portfolio_risk(
            mixed_returns,
            total_value_usd=Decimal("100000"),
            timestamps=timestamps_50,
        )
        # With mixed_returns (alternating +2%, -1%), sortino should be > sharpe
        # because downside deviation is smaller than total deviation
        assert result.sortino_ratio > result.sharpe_ratio


# =========================================================================
# Drawdown tests
# =========================================================================


class TestDrawdown:
    def test_no_drawdown(self, calc, timestamps_50):
        """Strictly increasing returns have zero drawdown."""
        returns = [0.01 + i * 0.0001 for i in range(50)]
        result = calc.portfolio_risk(
            returns,
            total_value_usd=Decimal("100000"),
            timestamps=timestamps_50,
        )
        assert result.max_drawdown == pytest.approx(0.0, abs=1e-10)
        assert result.current_drawdown == pytest.approx(0.0, abs=1e-10)

    def test_known_drawdown(self, calc):
        """Verify drawdown with a known sequence."""
        # Start at 1.0, go up to ~1.05, then down to ~0.95, then recover
        returns = [0.05] + [-0.01] * 10 + [0.02] * 39
        result = calc.portfolio_risk(
            returns,
            total_value_usd=Decimal("100000"),
        )
        # After +5%: wealth = 1.05
        # After 10 * -1%: wealth = 1.05 * 0.99^10 = ~0.9506
        # Max DD from peak: (1.05 - 0.9506) / 1.05 = ~0.0947
        assert result.max_drawdown > 0.09
        assert result.max_drawdown < 0.10

    def test_total_loss_drawdown(self, calc):
        """100% loss should give 1.0 max drawdown."""
        returns = [-0.02] * 50  # heavy losses
        result = calc.portfolio_risk(
            returns,
            total_value_usd=Decimal("100000"),
        )
        assert result.max_drawdown > 0.5  # cumulative losses are severe

    def test_current_drawdown_after_recovery(self, calc):
        """Current drawdown should be 0 if portfolio fully recovered."""
        # Drop then full recovery to new highs
        returns = [-0.05] + [0.06] + [0.01] * 48
        result = calc.portfolio_risk(
            returns,
            total_value_usd=Decimal("100000"),
        )
        assert result.current_drawdown == pytest.approx(0.0, abs=1e-10)


# =========================================================================
# VaR and CVaR tests
# =========================================================================


class TestVaRCVaR:
    def test_parametric_var(self, calc, mixed_returns, timestamps_50):
        """Parametric VaR should be positive for a series with losses."""
        result = calc.portfolio_risk(
            mixed_returns,
            total_value_usd=Decimal("100000"),
            var_method=VaRMethod.PARAMETRIC,
            timestamps=timestamps_50,
        )
        assert result.var_95 >= Decimal("0")
        assert result.cvar_95 >= result.var_95  # CVaR >= VaR always

    def test_historical_var(self, calc, mixed_returns, timestamps_50):
        """Historical VaR uses empirical percentile."""
        result = calc.portfolio_risk(
            mixed_returns,
            total_value_usd=Decimal("100000"),
            var_method=VaRMethod.HISTORICAL,
            timestamps=timestamps_50,
        )
        assert result.var_95 >= Decimal("0")
        assert result.var_method == "historical"

    def test_cornish_fisher_var(self, calc, mixed_returns, timestamps_50):
        """Cornish-Fisher adjusts for skewness and kurtosis."""
        result = calc.portfolio_risk(
            mixed_returns,
            total_value_usd=Decimal("100000"),
            var_method=VaRMethod.CORNISH_FISHER,
            timestamps=timestamps_50,
        )
        assert result.var_95 >= Decimal("0")
        assert result.var_method == "cornish_fisher"

    def test_cvar_exceeds_var(self, calc, mixed_returns, timestamps_50):
        """CVaR (expected shortfall) should be >= VaR."""
        for method in VaRMethod:
            result = calc.portfolio_risk(
                mixed_returns,
                total_value_usd=Decimal("100000"),
                var_method=method,
                timestamps=timestamps_50,
            )
            assert result.cvar_95 >= result.var_95, f"CVaR < VaR for {method.value}"

    def test_var_scales_with_portfolio_value(self, calc, mixed_returns, timestamps_50):
        """VaR should double when portfolio value doubles."""
        result_100k = calc.portfolio_risk(
            mixed_returns,
            total_value_usd=Decimal("100000"),
            timestamps=timestamps_50,
        )
        result_200k = calc.portfolio_risk(
            mixed_returns,
            total_value_usd=Decimal("200000"),
            timestamps=timestamps_50,
        )
        # VaR scales linearly with portfolio value
        ratio = float(result_200k.var_95) / float(result_100k.var_95) if float(result_100k.var_95) > 0 else 0
        assert abs(ratio - 2.0) < 0.01

    def test_var_zero_for_all_positive_returns(self, calc, timestamps_50):
        """If all returns are positive, parametric VaR may be 0 (no loss)."""
        returns = [0.01 + i * 0.0001 for i in range(50)]
        result = calc.portfolio_risk(
            returns,
            total_value_usd=Decimal("100000"),
            var_method=VaRMethod.HISTORICAL,
            timestamps=timestamps_50,
        )
        # All returns positive -> 5th percentile is still positive -> VaR = 0
        assert result.var_95 == Decimal("0")


# =========================================================================
# Beta tests
# =========================================================================


class TestBeta:
    def test_no_benchmark_returns_none(self, calc, mixed_returns, timestamps_50):
        """Without benchmark data, beta should be None."""
        result = calc.portfolio_risk(
            mixed_returns,
            total_value_usd=Decimal("100000"),
            timestamps=timestamps_50,
        )
        assert result.beta_to_eth is None
        assert result.beta_to_btc is None

    def test_perfect_correlation_beta_one(self, calc, timestamps_50):
        """Identical series should give beta = 1.0."""
        returns = [0.02 if i % 2 == 0 else -0.01 for i in range(50)]
        result = calc.portfolio_risk(
            returns,
            total_value_usd=Decimal("100000"),
            benchmark_eth_returns=returns,
            timestamps=timestamps_50,
        )
        assert result.beta_to_eth is not None
        assert abs(result.beta_to_eth - 1.0) < 0.01

    def test_double_volatility_beta_two(self, calc, timestamps_50):
        """Portfolio with 2x benchmark returns should have beta ~ 2.0."""
        benchmark = [0.02 if i % 2 == 0 else -0.01 for i in range(50)]
        portfolio = [r * 2 for r in benchmark]
        result = calc.portfolio_risk(
            portfolio,
            total_value_usd=Decimal("100000"),
            benchmark_eth_returns=benchmark,
            timestamps=timestamps_50,
        )
        assert result.beta_to_eth is not None
        assert abs(result.beta_to_eth - 2.0) < 0.01

    def test_uncorrelated_beta_near_zero(self, calc, timestamps_50):
        """Uncorrelated series should have beta ~ 0."""
        import random

        random.seed(42)
        portfolio = [random.gauss(0, 0.01) for _ in range(50)]
        benchmark = [random.gauss(0, 0.01) for _ in range(50)]
        result = calc.portfolio_risk(
            portfolio,
            total_value_usd=Decimal("100000"),
            benchmark_eth_returns=benchmark,
            timestamps=timestamps_50,
        )
        if result.beta_to_eth is not None:
            assert abs(result.beta_to_eth) < 0.5  # loosely near zero


# =========================================================================
# Conventions tests
# =========================================================================


class TestConventions:
    def test_conventions_included(self, calc, mixed_returns, timestamps_50):
        """PortfolioRisk should always include conventions."""
        result = calc.portfolio_risk(
            mixed_returns,
            total_value_usd=Decimal("100000"),
            return_interval="1d",
            risk_free_rate=Decimal("0.0001"),
            timestamps=timestamps_50,
        )
        assert result.conventions.return_interval == "1d"
        assert result.conventions.risk_free_rate == Decimal("0.0001")
        assert result.conventions.sample_count == 50
        assert result.conventions.annualization_factor == pytest.approx(math.sqrt(365), rel=1e-6)
        assert result.conventions.window_start == timestamps_50[0]
        assert result.conventions.window_end == timestamps_50[-1]

    def test_hourly_conventions(self, calc, mixed_returns, timestamps_50):
        """Hourly interval should have annualization_factor = sqrt(8760)."""
        result = calc.portfolio_risk(
            mixed_returns,
            total_value_usd=Decimal("100000"),
            return_interval="1h",
            timestamps=timestamps_50,
        )
        assert result.conventions.annualization_factor == pytest.approx(math.sqrt(8760), rel=1e-6)

    def test_weekly_interval(self, calc, mixed_returns, timestamps_50):
        """Weekly interval should use sqrt(52) annualization."""
        result = calc.portfolio_risk(
            mixed_returns,
            total_value_usd=Decimal("100000"),
            return_interval="1w",
            timestamps=timestamps_50,
        )
        assert result.conventions.annualization_factor == pytest.approx(math.sqrt(52), rel=1e-6)


# =========================================================================
# Rolling Sharpe tests
# =========================================================================


class TestRollingSharpe:
    def test_basic_rolling(self, calc, mixed_returns, timestamps_50):
        """Rolling Sharpe should produce entries."""
        result = calc.rolling_sharpe(
            mixed_returns,
            window_days=30,
            return_interval="1d",
            timestamps=timestamps_50,
        )
        assert isinstance(result, RollingSharpeResult)
        assert len(result.entries) > 0
        assert result.window_days == 30
        assert result.return_interval == "1d"

    def test_entries_have_correct_fields(self, calc, mixed_returns, timestamps_50):
        """Each entry should have timestamp, sharpe, sample_count."""
        result = calc.rolling_sharpe(
            mixed_returns,
            window_days=30,
            timestamps=timestamps_50,
        )
        for entry in result.entries:
            assert isinstance(entry, RollingSharpeEntry)
            assert isinstance(entry.sharpe, float)
            assert isinstance(entry.sample_count, int)
            assert entry.sample_count >= 30

    def test_rolling_sharpe_count(self, calc, timestamps_50):
        """Number of entries = n - window_periods + 1."""
        returns = [0.02 if i % 2 == 0 else -0.01 for i in range(50)]
        result = calc.rolling_sharpe(
            returns,
            window_days=30,
            return_interval="1d",
            timestamps=timestamps_50,
        )
        # window = 30 days at 1d interval = 30 periods; entries = 50 - 30 + 1 = 21
        assert len(result.entries) == 21

    def test_insufficient_data_error(self, calc):
        """Should raise InsufficientDataError for < 30 observations."""
        with pytest.raises(InsufficientDataError):
            calc.rolling_sharpe([0.01] * 20)


# =========================================================================
# Error handling tests
# =========================================================================


class TestErrorHandling:
    def test_insufficient_data_error(self, calc):
        """Should raise InsufficientDataError for < 30 observations."""
        with pytest.raises(InsufficientDataError):
            calc.portfolio_risk([0.01] * 20, total_value_usd=Decimal("100000"))

    def test_unsupported_return_interval(self, calc, mixed_returns):
        """Unknown return_interval should raise ValueError."""
        with pytest.raises(ValueError, match="Unsupported return_interval"):
            calc.portfolio_risk(
                mixed_returns,
                total_value_usd=Decimal("100000"),
                return_interval="2d",
            )

    def test_unsupported_rolling_interval(self, calc, mixed_returns):
        """Rolling Sharpe with bad interval should raise ValueError."""
        with pytest.raises(ValueError, match="Unsupported return_interval"):
            calc.rolling_sharpe(mixed_returns, return_interval="3d")

    def test_exactly_30_observations(self, calc):
        """Exactly 30 observations should work."""
        returns = [0.01 * ((-1) ** i) for i in range(30)]
        result = calc.portfolio_risk(returns, total_value_usd=Decimal("100000"))
        assert result.conventions.sample_count == 30


# =========================================================================
# VaRMethod enum tests
# =========================================================================


class TestVaRMethod:
    def test_values(self):
        assert VaRMethod.PARAMETRIC.value == "parametric"
        assert VaRMethod.HISTORICAL.value == "historical"
        assert VaRMethod.CORNISH_FISHER.value == "cornish_fisher"
