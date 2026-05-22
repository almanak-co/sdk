"""Regression tests for `metrics_calculator.calculate_metrics`.

VIB-2915: `total_return_pct` and `annualized_return_pct` must be stored as actual
percentages (e.g. 10 for 10%) rather than decimal ratios (0.1). Before this fix,
a 4882% actual return was being reported as 48.82% — the raw ratio stored in the
`_pct` field and formatted with a literal `%` sign.
"""

from datetime import datetime, timedelta
from decimal import Decimal

from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
from almanak.framework.backtesting.pnl.metrics_calculator import calculate_metrics
from almanak.framework.backtesting.pnl.portfolio import EquityPoint, SimulatedPortfolio


def _make_portfolio(points: list[tuple[datetime, Decimal]]) -> SimulatedPortfolio:
    portfolio = SimulatedPortfolio(initial_capital_usd=points[0][1])
    portfolio.equity_curve = [EquityPoint(timestamp=ts, value_usd=v) for ts, v in points]
    return portfolio


def _make_config(initial_capital: Decimal = Decimal("10000")) -> PnLBacktestConfig:
    return PnLBacktestConfig(
        start_time=datetime(2024, 1, 1),
        end_time=datetime(2024, 12, 31),
        initial_capital_usd=initial_capital,
    )


class TestVIB2915ReturnPercentage:
    """`total_return_pct` / `annualized_return_pct` must be actual percentages."""

    def test_10pct_return_reports_as_10(self) -> None:
        t0 = datetime(2024, 1, 1)
        portfolio = _make_portfolio(
            [
                (t0, Decimal("10000")),
                (t0 + timedelta(days=365), Decimal("11000")),
            ]
        )

        metrics = calculate_metrics(portfolio, trades=[], config=_make_config())

        assert metrics.total_return_pct == Decimal("10"), "10% return should be stored as 10, not 0.1"

    def test_4882pct_return_matches_ticket_example(self) -> None:
        """Reproduces the original VIB-2915 report: $10K -> $498.23K should read ~4882%, not 48.82%."""
        t0 = datetime(2024, 1, 1)
        portfolio = _make_portfolio(
            [
                (t0, Decimal("10000")),
                (t0 + timedelta(days=30), Decimal("498230")),
            ]
        )

        metrics = calculate_metrics(portfolio, trades=[], config=_make_config())

        assert metrics.total_return_pct == Decimal("4882.30")

    def test_loss_reports_as_negative_percentage(self) -> None:
        t0 = datetime(2024, 1, 1)
        portfolio = _make_portfolio(
            [
                (t0, Decimal("10000")),
                (t0 + timedelta(days=30), Decimal("7500")),
            ]
        )

        metrics = calculate_metrics(portfolio, trades=[], config=_make_config())

        assert metrics.total_return_pct == Decimal("-25")

    def test_annualized_return_matches_cagr_as_percentage(self) -> None:
        """10% return over 365 days must annualize to 10% exactly (CAGR identity)."""
        t0 = datetime(2024, 1, 1)
        portfolio = _make_portfolio(
            [
                (t0, Decimal("10000")),
                (t0 + timedelta(days=365), Decimal("11000")),
            ]
        )

        metrics = calculate_metrics(portfolio, trades=[], config=_make_config())

        # The CAGR over exactly one year equals the total return.
        assert abs(metrics.annualized_return_pct - Decimal("10")) < Decimal("0.01")


class TestVIB2915SchemaMigration:
    """Legacy backtest artifacts (pre-VIB-2915) stored returns as ratios.

    `BacktestResult.from_dict()` must upgrade them to whole percentages when
    `metrics.schema_version` is absent or < BacktestMetrics.SCHEMA_VERSION.
    """

    def _legacy_payload(self, total_return_ratio: str, annualized_ratio: str) -> dict:
        return {
            "engine": "pnl",
            "deployment_id": "legacy",
            "start_time": "2024-01-01T00:00:00+00:00",
            "end_time": "2024-12-31T00:00:00+00:00",
            "initial_capital_usd": "10000",
            "final_capital_usd": "11000",
            "chain": "arbitrum",
            "config": {},
            "metrics": {
                # schema_version intentionally omitted -> v1 (ratios)
                "total_return_pct": total_return_ratio,
                "annualized_return_pct": annualized_ratio,
            },
            "trades": [],
            "equity_curve": [],
            "errors": [],
        }

    def test_legacy_ratio_is_migrated_to_percentage(self) -> None:
        from almanak.framework.backtesting.models import BacktestResult

        result = BacktestResult.from_dict(self._legacy_payload("0.10", "0.12"))

        assert result.metrics.total_return_pct == Decimal("10.00")
        assert result.metrics.annualized_return_pct == Decimal("12.00")

    def test_current_schema_is_not_migrated(self) -> None:
        from almanak.framework.backtesting.models import BacktestMetrics, BacktestResult

        payload = self._legacy_payload("10", "12")
        payload["metrics"]["schema_version"] = BacktestMetrics.SCHEMA_VERSION
        result = BacktestResult.from_dict(payload)

        # Values already in percentage form - must not be multiplied again.
        assert result.metrics.total_return_pct == Decimal("10")
        assert result.metrics.annualized_return_pct == Decimal("12")

    def test_roundtrip_preserves_percentages(self) -> None:
        from almanak.framework.backtesting.models import BacktestResult

        payload = self._legacy_payload("10", "12")
        # Simulate a fresh v2 write by including schema_version.
        from almanak.framework.backtesting.models import BacktestMetrics

        payload["metrics"]["schema_version"] = BacktestMetrics.SCHEMA_VERSION
        first = BacktestResult.from_dict(payload)
        second = BacktestResult.from_dict({**payload, "metrics": first.metrics.to_dict()})

        assert second.metrics.total_return_pct == first.metrics.total_return_pct
        assert second.metrics.annualized_return_pct == first.metrics.annualized_return_pct
