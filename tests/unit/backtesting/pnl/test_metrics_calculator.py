"""Regression tests for `metrics_calculator.calculate_metrics`.

VIB-2915: `total_return_pct` and `annualized_return_pct` must be stored as actual
percentages (e.g. 10 for 10%) rather than decimal ratios (0.1). Before this fix,
a 4882% actual return was being reported as 48.82% — the raw ratio stored in the
`_pct` field and formatted with a literal `%` sign.
"""
from tests.backtesting_funding import pnl_token_funding as _pnl_token_funding

from datetime import datetime, timedelta
from decimal import Decimal

from almanak.framework.backtesting.models import IntentType, LendingLiquidationEvent, LiquidationEvent, TradeRecord
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
        token_funding=_pnl_token_funding(initial_capital),
    )


def _trade_record(
    *,
    timestamp: datetime,
    pnl_usd: Decimal | None,
    success: bool = True,
    fee_usd: Decimal = Decimal("0"),
    slippage_usd: Decimal = Decimal("0"),
    gas_cost_usd: Decimal = Decimal("0"),
    gas_price_gwei: Decimal | None = None,
    estimated_mev_cost_usd: Decimal | None = None,
) -> TradeRecord:
    return TradeRecord(
        timestamp=timestamp,
        intent_type=IntentType.SWAP,
        executed_price=Decimal("1"),
        fee_usd=fee_usd,
        slippage_usd=slippage_usd,
        gas_cost_usd=gas_cost_usd,
        pnl_usd=pnl_usd,
        success=success,
        gas_price_gwei=gas_price_gwei,
        estimated_mev_cost_usd=estimated_mev_cost_usd,
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
            "initial_portfolio_value_usd": "10000",
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


class TestNetPnlEqualsEquityCurvePnl:
    """net_pnl_usd == total_pnl_usd == equity-curve PnL (VIB-5079).

    Execution costs (gas, venue fee/slippage, SWAP fee/slippage netted into
    tokens_in) are debited from the portfolio during execution, so the equity
    curve is already net of them. ``net_pnl_usd`` must therefore equal the
    equity-curve PnL; re-subtracting the cost columns would double-count
    every cost. The cost columns are an informational breakdown only.
    """

    def test_net_pnl_is_not_reduced_by_cost_columns(self) -> None:
        from almanak.framework.backtesting.models import IntentType, TradeRecord

        t0 = datetime(2024, 1, 1)
        portfolio = _make_portfolio(
            [
                (t0, Decimal("10000")),
                (t0 + timedelta(days=30), Decimal("10100")),
            ]
        )
        trades = [
            TradeRecord(
                timestamp=t0,
                intent_type=IntentType.SWAP,
                executed_price=Decimal("3000"),
                fee_usd=Decimal("30"),
                slippage_usd=Decimal("10"),
                gas_cost_usd=Decimal("5"),
                pnl_usd=Decimal("0"),
                success=True,
            ),
        ]

        metrics = calculate_metrics(portfolio, trades=trades, config=_make_config())

        assert metrics.total_pnl_usd == Decimal("100")
        assert metrics.net_pnl_usd == metrics.total_pnl_usd
        assert metrics.total_fees_usd == Decimal("30")
        assert metrics.total_slippage_usd == Decimal("10")
        assert metrics.total_gas_usd == Decimal("5")


class TestTradeStatisticsBoundaries:
    """Characterize trade-stat denominator and cost-column boundaries."""

    def test_breakeven_trade_is_measured_realized_pnl(self) -> None:
        t0 = datetime(2024, 1, 1)
        portfolio = _make_portfolio(
            [
                (t0, Decimal("10000")),
                (t0 + timedelta(days=1), Decimal("10000")),
            ]
        )
        trades = [_trade_record(timestamp=t0, pnl_usd=Decimal("0"))]

        metrics = calculate_metrics(portfolio, trades=trades, config=_make_config())

        assert metrics.trades_with_realized_pnl == 1
        assert metrics.winning_trades == 0
        assert metrics.losing_trades == 1
        assert metrics.win_rate == Decimal("0")
        assert metrics.avg_trade_pnl_usd == Decimal("0")

    def test_failed_trade_costs_are_reported_but_excluded_from_performance_denominator(self) -> None:
        t0 = datetime(2024, 1, 1)
        portfolio = _make_portfolio(
            [
                (t0, Decimal("10000")),
                (t0 + timedelta(days=1), Decimal("10010")),
            ]
        )
        trades = [
            _trade_record(timestamp=t0, pnl_usd=Decimal("10"), gas_price_gwei=Decimal("20")),
            _trade_record(
                timestamp=t0 + timedelta(hours=1),
                pnl_usd=Decimal("-999"),
                success=False,
                fee_usd=Decimal("2"),
                slippage_usd=Decimal("3"),
                gas_cost_usd=Decimal("4"),
                gas_price_gwei=Decimal("40"),
                estimated_mev_cost_usd=Decimal("5"),
            ),
        ]

        metrics = calculate_metrics(portfolio, trades=trades, config=_make_config())

        assert metrics.total_trades == 1
        assert metrics.failed_trades == 1
        assert metrics.trades_with_realized_pnl == 1
        assert metrics.total_fees_usd == Decimal("2")
        assert metrics.total_slippage_usd == Decimal("3")
        assert metrics.total_gas_usd == Decimal("4")
        assert metrics.total_mev_cost_usd == Decimal("5")
        assert metrics.avg_gas_price_gwei == Decimal("30")
        assert metrics.max_gas_price_gwei == Decimal("40")


class TestPositionDerivedMetricsParity:
    """The engine-result path and the portfolio-native path must agree (VIB-5079).

    ``calculate_metrics`` (the path the engine *result* uses) and
    ``SimulatedPortfolio.get_metrics`` now source the position-derived block from
    one shared helper (``aggregate_position_metrics``). These pin that the two
    agree -- the regression that motivated the helper was them silently
    diverging, with ``calculate_metrics`` reporting zeros.
    """

    def test_liquidation_metrics_aggregated_and_match_get_metrics(self) -> None:
        """liquidations_count / liquidation_losses_usd are aggregated in BOTH paths.

        Before the shared helper, neither path populated these (they only ever
        round-tripped through from_dict), so every result reported zero
        liquidations. Perp loss is the explicit LiquidationEvent.loss_usd; lending
        loss is collateral_seized - debt_repaid (the liquidation-penalty bonus).
        """
        t0 = datetime(2024, 1, 1)
        portfolio = _make_portfolio(
            [
                (t0, Decimal("10000")),
                (t0 + timedelta(days=30), Decimal("9000")),
            ]
        )
        portfolio._perp_liquidations.append(
            LiquidationEvent(
                timestamp=t0,
                position_id="perp-1",
                price=Decimal("2000"),
                loss_usd=Decimal("250.50"),
            )
        )
        portfolio._lending_liquidations.append(
            LendingLiquidationEvent(
                timestamp=t0,
                position_id="lend-1",
                health_factor=Decimal("0.95"),
                collateral_seized=Decimal("1050"),  # debt_repaid * (1 + penalty)
                debt_repaid=Decimal("1000"),
                penalty=Decimal("0.05"),
            )
        )
        # Borrower's loss == the penalty bonus == collateral_seized - debt_repaid.
        expected_losses = Decimal("250.50") + (Decimal("1050") - Decimal("1000"))

        metrics = calculate_metrics(portfolio, trades=[], config=_make_config())

        assert metrics.liquidations_count == 2
        assert metrics.liquidation_losses_usd == expected_losses

        # Parity: the portfolio-native path reports the same (shared helper).
        native = portfolio.get_metrics()
        assert native.liquidations_count == metrics.liquidations_count
        assert native.liquidation_losses_usd == metrics.liquidation_losses_usd
