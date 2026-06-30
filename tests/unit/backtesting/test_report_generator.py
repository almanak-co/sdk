"""Unit tests for backtest HTML report generation."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import patch

from almanak.framework.backtesting.models import (
    BacktestEngine,
    BacktestMetrics,
    BacktestResult,
    IntentType,
    TradeRecord,
)
from almanak.framework.backtesting.report_generator import (
    _prepare_metrics_dict,
    generate_report,
)

ETH_ATTRIBUTION_KEY = "arbitrum:0x123400000000000000000000000000000000abcd"


def _result_with_trades() -> BacktestResult:
    metrics = BacktestMetrics(
        total_pnl_usd=Decimal("1500.25"),
        net_pnl_usd=Decimal("1234.56"),
        sharpe_ratio=Decimal("1.75"),
        max_drawdown_pct=Decimal("0.05"),
        win_rate=Decimal("0.5"),
        total_trades=2,
        profit_factor=Decimal("2.5"),
        total_return_pct=Decimal("12.345"),
        annualized_return_pct=Decimal("25.5"),
        total_fees_usd=Decimal("10"),
        total_slippage_usd=Decimal("2"),
        total_gas_usd=Decimal("3"),
        winning_trades=1,
        losing_trades=1,
        avg_trade_pnl_usd=Decimal("750.125"),
        largest_win_usd=Decimal("1500.25"),
        largest_loss_usd=Decimal("-5"),
        avg_win_usd=Decimal("1500.25"),
        avg_loss_usd=Decimal("-5"),
        volatility=Decimal("0.2"),
        sortino_ratio=Decimal("2.1"),
        calmar_ratio=Decimal("3.2"),
        total_fees_earned_usd=Decimal("42"),
        fees_by_pool={"ETH/USDC": Decimal("42")},
        total_funding_paid=Decimal("1.5"),
        total_funding_received=Decimal("2.5"),
        liquidations_count=1,
        liquidation_losses_usd=Decimal("4"),
        max_margin_utilization=Decimal("0.7"),
        total_interest_earned=Decimal("6"),
        total_interest_paid=Decimal("1"),
        min_health_factor=Decimal("1.2"),
        health_factor_warnings=2,
        avg_gas_price_gwei=Decimal("18"),
        max_gas_price_gwei=Decimal("25"),
        total_gas_cost_usd=Decimal("3"),
        total_mev_cost_usd=Decimal("0.5"),
        total_leverage=Decimal("1.4"),
        max_net_delta={ETH_ATTRIBUTION_KEY: Decimal("0.75")},
        max_net_delta_display_labels={ETH_ATTRIBUTION_KEY: "ETH"},
        correlation_risk=None,
        liquidation_cascade_risk=Decimal("0.2"),
        pnl_by_protocol={"uniswap_v3": Decimal("1500.25")},
        pnl_by_intent_type={"SWAP": Decimal("1500.25")},
        pnl_by_asset={ETH_ATTRIBUTION_KEY: Decimal("1500.25")},
        pnl_by_asset_display_labels={ETH_ATTRIBUTION_KEY: "ETH"},
        realized_pnl=Decimal("1000"),
        unrealized_pnl=Decimal("500.25"),
    )
    trades = [
        TradeRecord(
            timestamp=datetime(2024, 1, 1, 12, 0, tzinfo=UTC),
            intent_type=IntentType.SWAP,
            executed_price=Decimal("2000"),
            fee_usd=Decimal("1"),
            slippage_usd=Decimal("0.5"),
            gas_cost_usd=Decimal("2"),
            pnl_usd=Decimal("25"),
            success=True,
            amount_usd=Decimal("1000"),
            protocol="uniswap_v3",
            tokens=["ETH", "USDC"],
        ),
        TradeRecord(
            timestamp=datetime(2024, 1, 1, 13, 0, tzinfo=UTC),
            intent_type=IntentType.LP_OPEN,
            executed_price=Decimal("1"),
            fee_usd=Decimal("1"),
            slippage_usd=Decimal("0"),
            gas_cost_usd=Decimal("1"),
            pnl_usd=None,
            success=True,
            amount_usd=Decimal("500"),
            protocol="uniswap_v3",
            tokens=["ETH", "USDC"],
        ),
    ]
    return BacktestResult(
        engine=BacktestEngine.PNL,
        deployment_id="demo-report",
        start_time=datetime(2024, 1, 1, tzinfo=UTC),
        end_time=datetime(2024, 1, 2, tzinfo=UTC),
        metrics=metrics,
        trades=trades,
        initial_capital_usd=Decimal("10000"),
        final_capital_usd=Decimal("11500.25"),
        chain="arbitrum",
        run_started_at=datetime(2024, 1, 2, 1, 0, tzinfo=UTC),
        run_ended_at=datetime(2024, 1, 2, 1, 1, tzinfo=UTC),
        run_duration_seconds=60,
        config={"interval_seconds": 3600},
    )


class TestPrepareMetricsDict:
    def test_serializes_decimal_metrics_and_optional_none(self) -> None:
        metrics = _prepare_metrics_dict(_result_with_trades())

        assert metrics["net_pnl_usd"] == "1234.56"
        assert metrics["total_execution_cost_usd"] == "15"
        assert metrics["fees_by_pool"] == {"ETH/USDC": "42"}
        assert metrics["max_net_delta"] == {ETH_ATTRIBUTION_KEY: "0.75"}
        assert metrics["max_net_delta_display_labels"] == {ETH_ATTRIBUTION_KEY: "ETH"}
        assert metrics["correlation_risk"] is None
        assert metrics["pnl_by_protocol"] == {"uniswap_v3": "1500.25"}
        assert metrics["pnl_by_asset"] == {ETH_ATTRIBUTION_KEY: "1500.25"}
        assert metrics["pnl_by_asset_display_labels"] == {ETH_ATTRIBUTION_KEY: "ETH"}


class TestGenerateReport:
    def test_writes_html_with_supplied_charts_and_attribution(self, tmp_path) -> None:
        output_path = tmp_path / "report.html"

        report = generate_report(
            _result_with_trades(),
            output_path=output_path,
            equity_chart_html="<div id='equity-chart'>equity</div>",
            pnl_histogram_html="<div id='pnl-chart'>pnl</div>",
            drawdown_chart_html="<div id='drawdown-chart'>drawdown</div>",
            attribution_charts={"by_protocol": "<div id='protocol-chart'>protocol</div>"},
            auto_generate_charts=False,
        )

        assert report.success is True
        assert report.file_path == output_path
        assert output_path.read_text(encoding="utf-8") == report.html_content
        assert "demo-report" in report.html_content
        assert "<div id='equity-chart'>equity</div>" in report.html_content
        assert "<div id='protocol-chart'>protocol</div>" in report.html_content

    def test_asset_table_disambiguates_duplicate_display_labels(self) -> None:
        result = _result_with_trades()
        base_usdc_key = "base:0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
        arb_usdc_key = "arbitrum:0xaf88d065e77c8cc2239327c5edb3a432268e5831"
        result.metrics.pnl_by_asset = {
            base_usdc_key: Decimal("12.34"),
            arb_usdc_key: Decimal("56.78"),
        }
        result.metrics.pnl_by_asset_display_labels = {
            base_usdc_key: "USDC",
            arb_usdc_key: "USDC",
        }

        report = generate_report(
            result,
            attribution_charts={"by_asset": "<div>asset-chart</div>"},
            auto_generate_charts=False,
        )

        assert report.success is True
        assert f">USDC ({base_usdc_key})</td>" in report.html_content
        assert f">USDC ({arb_usdc_key})</td>" in report.html_content

    def test_unrealized_trade_pnl_renders_dash_not_zero(self) -> None:
        report = generate_report(
            _result_with_trades(),
            auto_generate_charts=False,
        )

        assert report.success is True
        assert "<td class=\"neutral\" data-value=\"\">" in report.html_content
        assert "\n                                -\n" in report.html_content

    def test_auto_generates_missing_charts(self) -> None:
        result = _result_with_trades()

        with (
            patch(
                "almanak.framework.backtesting.visualization.generate_equity_chart_html",
                return_value="<div>equity-auto</div>",
            ) as equity,
            patch(
                "almanak.framework.backtesting.visualization.generate_pnl_distribution_html",
                return_value="<div>pnl-auto</div>",
            ) as pnl,
            patch(
                "almanak.framework.backtesting.visualization.generate_drawdown_chart_html",
                return_value="<div>drawdown-auto</div>",
            ) as drawdown,
            patch(
                "almanak.framework.backtesting.visualization.generate_attribution_charts_html",
                return_value={"by_asset": "<div>asset-auto</div>"},
            ) as attribution,
        ):
            report = generate_report(result)

        assert report.success is True
        assert "<div>equity-auto</div>" in report.html_content
        assert "<div>asset-auto</div>" in report.html_content
        equity.assert_called_once_with(result)
        pnl.assert_called_once_with(result)
        drawdown.assert_called_once_with(result)
        attribution.assert_called_once_with(result)

    def test_returns_failure_result_when_template_setup_fails(self) -> None:
        with patch("almanak.framework.backtesting.report_generator.Environment", side_effect=RuntimeError("boom")):
            report = generate_report(_result_with_trades(), auto_generate_charts=False)

        assert report.success is False
        assert report.error == "boom"
        assert report.html_content == ""
