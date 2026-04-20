"""Test that CAGR calculation handles portfolios that lose >100%."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
import pytest

from almanak.framework.backtesting.models import BacktestMetrics, EquityPoint, TradeRecord
from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
from almanak.framework.backtesting.pnl.engine import PnLBacktester
from almanak.framework.backtesting.pnl.portfolio import SimulatedPortfolio
from almanak.framework.intents.vocabulary import IntentType


@pytest.fixture
def backtester():
    """Create a PnLBacktester with mocked dependencies."""
    bt = PnLBacktester.__new__(PnLBacktester)
    return bt


@pytest.fixture
def config():
    """Minimal config for metric calculation."""
    return PnLBacktestConfig(
        start_time=datetime(2024, 1, 1, tzinfo=UTC),
        end_time=datetime(2024, 2, 1, tzinfo=UTC),
        initial_capital_usd=Decimal("10000"),
    )


def _make_portfolio(initial: Decimal, final: Decimal, days: int = 30) -> SimulatedPortfolio:
    """Create a portfolio with a simple equity curve."""
    start = datetime(2024, 1, 1, tzinfo=UTC)
    portfolio = SimulatedPortfolio(initial_capital_usd=initial)
    portfolio.equity_curve = [
        EquityPoint(timestamp=start, value_usd=initial),
        EquityPoint(timestamp=start + timedelta(days=days), value_usd=final),
    ]
    return portfolio


class TestCAGRCrash:
    """Verify CAGR handles edge cases without crashing."""

    def test_portfolio_loses_more_than_100_pct(self, backtester, config):
        """Portfolio going negative (gas > principal) should not crash."""
        # Portfolio lost >100%: $10K -> -$2K
        portfolio = _make_portfolio(Decimal("10000"), Decimal("-2000"))
        metrics = backtester._calculate_metrics(portfolio, [], config)

        assert isinstance(metrics, BacktestMetrics)
        assert metrics.annualized_return_pct == Decimal("-1")

    def test_portfolio_loses_exactly_100_pct(self, backtester, config):
        """Portfolio going to zero should cap at -100%."""
        portfolio = _make_portfolio(Decimal("10000"), Decimal("0"))
        metrics = backtester._calculate_metrics(portfolio, [], config)

        assert metrics.annualized_return_pct == Decimal("-1")

    def test_normal_loss_unaffected(self, backtester, config):
        """Normal loss (<100%) should calculate CAGR normally."""
        portfolio = _make_portfolio(Decimal("10000"), Decimal("8000"))
        metrics = backtester._calculate_metrics(portfolio, [], config)

        # Should be a negative return but not -1
        assert metrics.annualized_return_pct < Decimal("0")
        assert metrics.annualized_return_pct > Decimal("-1")

    def test_normal_gain_unaffected(self, backtester, config):
        """Normal gain should calculate CAGR normally."""
        portfolio = _make_portfolio(Decimal("10000"), Decimal("12000"))
        metrics = backtester._calculate_metrics(portfolio, [], config)

        assert metrics.annualized_return_pct > Decimal("0")

    def test_portfolio_with_trades_losing_more_than_100_pct(self, backtester, config):
        """Portfolio with trades that lost >100% should not crash."""
        portfolio = _make_portfolio(Decimal("10000"), Decimal("-2223"))
        trade = TradeRecord(
            timestamp=datetime(2024, 1, 15, tzinfo=UTC),
            intent_type=IntentType.SWAP,
            executed_price=Decimal("2000"),
            fee_usd=Decimal("5"),
            slippage_usd=Decimal("2"),
            gas_cost_usd=Decimal("19"),
            pnl_usd=Decimal("-100"),
            success=True,
        )
        metrics = backtester._calculate_metrics(portfolio, [trade], config)

        assert isinstance(metrics, BacktestMetrics)
        assert metrics.annualized_return_pct == Decimal("-1")
        assert metrics.total_gas_usd == Decimal("19")
