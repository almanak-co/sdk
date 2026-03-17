"""Tests for OperatorCard portfolio value wiring (VIB-1268).

Validates that StrategyRunner._query_portfolio_value() returns real portfolio
data when available, and falls back to (Decimal("0"), Decimal("0")) on failure.
"""

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.framework.runner.strategy_runner import StrategyRunner


class TestQueryPortfolioValue:
    """Test StrategyRunner._query_portfolio_value() helper."""

    @pytest.fixture
    def runner(self):
        """Create a minimal StrategyRunner for testing."""
        return StrategyRunner(
            price_oracle=MagicMock(),
            balance_provider=MagicMock(),
            execution_orchestrator=MagicMock(),
            state_manager=MagicMock(),
        )

    def test_returns_real_value_from_portfolio_snapshot(self, runner):
        """When strategy has get_portfolio_snapshot(), returns real values."""
        strategy = MagicMock()
        snapshot = MagicMock()
        snapshot.total_value_usd = Decimal("5000.00")
        snapshot.available_cash_usd = Decimal("1200.50")
        strategy.get_portfolio_snapshot.return_value = snapshot

        total, available = runner._query_portfolio_value(strategy)

        assert total == Decimal("5000.00")
        assert available == Decimal("1200.50")

    def test_falls_back_on_exception(self, runner):
        """When get_portfolio_snapshot() raises, falls back to zero."""
        strategy = MagicMock()
        strategy.get_portfolio_snapshot.side_effect = RuntimeError("RPC timeout")

        total, available = runner._query_portfolio_value(strategy)

        assert total == Decimal("0")
        assert available == Decimal("0")

    def test_falls_back_when_no_method(self, runner):
        """When strategy has no get_portfolio_snapshot(), falls back to zero."""
        strategy = MagicMock(spec=[])  # spec=[] means no attributes

        total, available = runner._query_portfolio_value(strategy)

        assert total == Decimal("0")
        assert available == Decimal("0")

    def test_handles_missing_attributes_gracefully(self, runner):
        """When snapshot lacks expected attributes, falls back to zero per attr."""
        strategy = MagicMock()
        snapshot = MagicMock(spec=[])  # No attributes
        strategy.get_portfolio_snapshot.return_value = snapshot

        total, available = runner._query_portfolio_value(strategy)

        assert total == Decimal("0")
        assert available == Decimal("0")

    def test_normalizes_none_values_to_zero(self, runner):
        """When snapshot attributes are None, normalizes to Decimal("0")."""
        strategy = MagicMock()
        snapshot = MagicMock()
        snapshot.total_value_usd = None
        snapshot.available_cash_usd = None
        strategy.get_portfolio_snapshot.return_value = snapshot

        total, available = runner._query_portfolio_value(strategy)

        assert total == Decimal("0")
        assert available == Decimal("0")

    def test_normalizes_string_values_to_decimal(self, runner):
        """When snapshot returns string values, converts to Decimal."""
        strategy = MagicMock()
        snapshot = MagicMock()
        snapshot.total_value_usd = "3500.25"
        snapshot.available_cash_usd = "800.10"
        strategy.get_portfolio_snapshot.return_value = snapshot

        total, available = runner._query_portfolio_value(strategy)

        assert total == Decimal("3500.25")
        assert available == Decimal("800.10")
