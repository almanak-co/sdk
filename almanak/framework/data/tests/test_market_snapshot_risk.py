"""Tests for MarketSnapshot portfolio_risk() and rolling_sharpe() integration."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from almanak.framework.data.market_snapshot import (
    MarketSnapshot,
    PortfolioRiskUnavailableError,
    RollingSharpeUnavailableError,
)
from almanak.framework.data.models import DataClassification, DataEnvelope
from almanak.framework.data.risk.metrics import (
    PortfolioRisk,
    PortfolioRiskCalculator,
    RollingSharpeResult,
)


@pytest.fixture
def risk_calculator() -> PortfolioRiskCalculator:
    return PortfolioRiskCalculator()


@pytest.fixture
def snapshot(risk_calculator) -> MarketSnapshot:
    return MarketSnapshot(
        chain="arbitrum",
        wallet_address="0x" + "a" * 40,
        risk_calculator=risk_calculator,
    )


@pytest.fixture
def timestamps_50() -> list[datetime]:
    base = datetime(2024, 1, 1, tzinfo=UTC)
    return [base + timedelta(days=i) for i in range(50)]


@pytest.fixture
def mixed_returns() -> list[float]:
    return [0.02 if i % 2 == 0 else -0.01 for i in range(50)]


# =========================================================================
# portfolio_risk() integration tests
# =========================================================================


class TestPortfolioRiskIntegration:
    def test_returns_data_envelope(self, snapshot, mixed_returns, timestamps_50):
        """portfolio_risk() should return DataEnvelope[PortfolioRisk]."""
        result = snapshot.portfolio_risk(
            pnl_series=mixed_returns,
            total_value_usd=Decimal("100000"),
            timestamps=timestamps_50,
        )
        assert isinstance(result, DataEnvelope)
        assert isinstance(result.value, PortfolioRisk)

    def test_informational_classification(self, snapshot, mixed_returns, timestamps_50):
        """Should have INFORMATIONAL classification."""
        result = snapshot.portfolio_risk(
            pnl_series=mixed_returns,
            total_value_usd=Decimal("100000"),
            timestamps=timestamps_50,
        )
        assert result.classification == DataClassification.INFORMATIONAL

    def test_meta_source_computed(self, snapshot, mixed_returns, timestamps_50):
        """Meta source should be 'computed'."""
        result = snapshot.portfolio_risk(
            pnl_series=mixed_returns,
            total_value_usd=Decimal("100000"),
            timestamps=timestamps_50,
        )
        assert result.meta.source == "computed"
        assert result.meta.finality == "off_chain"
        assert result.meta.confidence == 1.0

    def test_transparent_delegation(self, snapshot, mixed_returns, timestamps_50):
        """DataEnvelope should transparently delegate to PortfolioRisk."""
        result = snapshot.portfolio_risk(
            pnl_series=mixed_returns,
            total_value_usd=Decimal("100000"),
            timestamps=timestamps_50,
        )
        # These should delegate to result.value.*
        assert isinstance(result.sharpe_ratio, float)
        assert isinstance(result.sortino_ratio, float)
        assert isinstance(result.max_drawdown, float)
        assert isinstance(result.var_95, Decimal)

    def test_default_total_value(self, snapshot, mixed_returns, timestamps_50):
        """Omitting total_value_usd should default to Decimal('0')."""
        result = snapshot.portfolio_risk(
            pnl_series=mixed_returns,
            timestamps=timestamps_50,
        )
        assert result.value.total_value_usd == Decimal("0")

    def test_var_method_parametric(self, snapshot, mixed_returns, timestamps_50):
        """Should accept 'parametric' as var_method string."""
        result = snapshot.portfolio_risk(
            pnl_series=mixed_returns,
            total_value_usd=Decimal("100000"),
            var_method="parametric",
            timestamps=timestamps_50,
        )
        assert result.value.var_method == "parametric"

    def test_var_method_historical(self, snapshot, mixed_returns, timestamps_50):
        """Should accept 'historical' as var_method string."""
        result = snapshot.portfolio_risk(
            pnl_series=mixed_returns,
            total_value_usd=Decimal("100000"),
            var_method="historical",
            timestamps=timestamps_50,
        )
        assert result.value.var_method == "historical"

    def test_var_method_cornish_fisher(self, snapshot, mixed_returns, timestamps_50):
        """Should accept 'cornish_fisher' as var_method string."""
        result = snapshot.portfolio_risk(
            pnl_series=mixed_returns,
            total_value_usd=Decimal("100000"),
            var_method="cornish_fisher",
            timestamps=timestamps_50,
        )
        assert result.value.var_method == "cornish_fisher"

    def test_invalid_var_method_error(self, snapshot, mixed_returns, timestamps_50):
        """Invalid var_method should raise PortfolioRiskUnavailableError."""
        with pytest.raises(PortfolioRiskUnavailableError):
            snapshot.portfolio_risk(
                pnl_series=mixed_returns,
                total_value_usd=Decimal("100000"),
                var_method="bogus",
                timestamps=timestamps_50,
            )

    def test_no_calculator_raises_value_error(self, mixed_returns, timestamps_50):
        """Should raise ValueError if no risk calculator configured."""
        snap = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0x" + "a" * 40,
        )
        with pytest.raises(ValueError, match="No risk calculator configured"):
            snap.portfolio_risk(
                pnl_series=mixed_returns,
                total_value_usd=Decimal("100000"),
                timestamps=timestamps_50,
            )

    def test_insufficient_data_wrapped(self, snapshot):
        """InsufficientDataError should be wrapped in PortfolioRiskUnavailableError."""
        with pytest.raises(PortfolioRiskUnavailableError):
            snapshot.portfolio_risk(
                pnl_series=[0.01] * 10,
                total_value_usd=Decimal("100000"),
            )

    def test_with_benchmark_returns(self, snapshot, mixed_returns, timestamps_50):
        """Should compute beta when benchmark data is provided."""
        benchmark = [0.015 if i % 2 == 0 else -0.005 for i in range(50)]
        result = snapshot.portfolio_risk(
            pnl_series=mixed_returns,
            total_value_usd=Decimal("100000"),
            benchmark_eth_returns=benchmark,
            timestamps=timestamps_50,
        )
        assert result.value.beta_to_eth is not None


# =========================================================================
# rolling_sharpe() integration tests
# =========================================================================


class TestRollingSharpeIntegration:
    def test_returns_data_envelope(self, snapshot, mixed_returns, timestamps_50):
        """rolling_sharpe() should return DataEnvelope[RollingSharpeResult]."""
        result = snapshot.rolling_sharpe(
            pnl_series=mixed_returns,
            window_days=30,
            timestamps=timestamps_50,
        )
        assert isinstance(result, DataEnvelope)
        assert isinstance(result.value, RollingSharpeResult)

    def test_informational_classification(self, snapshot, mixed_returns, timestamps_50):
        """Should have INFORMATIONAL classification."""
        result = snapshot.rolling_sharpe(
            pnl_series=mixed_returns,
            window_days=30,
            timestamps=timestamps_50,
        )
        assert result.classification == DataClassification.INFORMATIONAL

    def test_meta_source_computed(self, snapshot, mixed_returns, timestamps_50):
        """Meta source should be 'computed'."""
        result = snapshot.rolling_sharpe(
            pnl_series=mixed_returns,
            window_days=30,
            timestamps=timestamps_50,
        )
        assert result.meta.source == "computed"

    def test_entries_present(self, snapshot, mixed_returns, timestamps_50):
        """Should have at least one rolling entry."""
        result = snapshot.rolling_sharpe(
            pnl_series=mixed_returns,
            window_days=30,
            timestamps=timestamps_50,
        )
        assert len(result.value.entries) > 0

    def test_no_calculator_raises_value_error(self, mixed_returns, timestamps_50):
        """Should raise ValueError if no risk calculator configured."""
        snap = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0x" + "a" * 40,
        )
        with pytest.raises(ValueError, match="No risk calculator configured"):
            snap.rolling_sharpe(
                pnl_series=mixed_returns,
                window_days=30,
                timestamps=timestamps_50,
            )

    def test_insufficient_data_wrapped(self, snapshot):
        """InsufficientDataError should be wrapped in RollingSharpeUnavailableError."""
        with pytest.raises(RollingSharpeUnavailableError):
            snapshot.rolling_sharpe(pnl_series=[0.01] * 10)
