"""Report generator for backtest results.

This module provides functions to generate HTML reports from backtest results
using Jinja2 templates. The reports include executive summaries, detailed metrics,
charts, trade logs, and configuration information.

Example:
    from almanak.framework.backtesting.report_generator import generate_report

    # Generate HTML report
    result = generate_report(backtest_result, output_path="report.html")
    if result.success:
        print(f"Report saved to: {result.file_path}")

    # Generate report with custom charts
    result = generate_report(
        backtest_result,
        equity_chart_html=equity_curve_html,
        pnl_histogram_html=histogram_html,
    )
"""

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

try:
    from jinja2 import Environment, FileSystemLoader
except ImportError as e:
    raise ImportError("Jinja2 is required for report generation. Install it with: pip install jinja2") from e

from almanak.framework.backtesting.templates import TEMPLATE_DIR

if TYPE_CHECKING:
    from almanak.framework.backtesting.models import BacktestResult

logger = logging.getLogger(__name__)


@dataclass
class ReportResult:
    """Result of report generation.

    Attributes:
        success: Whether report generation succeeded
        file_path: Path to the generated report (if saved to file)
        html_content: Generated HTML content
        error: Error message if generation failed
    """

    success: bool
    file_path: Path | None = None
    html_content: str = ""
    error: str | None = None


def _prepare_metrics_dict(result: "BacktestResult") -> dict[str, Any]:
    """Prepare metrics dictionary for template rendering.

    Converts BacktestMetrics to a dictionary with appropriate types for
    Jinja2 template rendering, ensuring all values can be formatted correctly.

    Args:
        result: BacktestResult containing metrics

    Returns:
        Dictionary with metrics values suitable for template rendering
    """
    metrics = result.metrics

    # Convert all Decimal values to strings that can be parsed as floats
    return {
        "total_pnl_usd": str(metrics.total_pnl_usd),
        "net_pnl_usd": str(metrics.net_pnl_usd),
        "sharpe_ratio": str(metrics.sharpe_ratio),
        "max_drawdown_pct": str(metrics.max_drawdown_pct),
        "win_rate": str(metrics.win_rate),
        "total_trades": metrics.total_trades,
        "profit_factor": str(metrics.profit_factor),
        "total_return_pct": str(metrics.total_return_pct),
        "annualized_return_pct": str(metrics.annualized_return_pct),
        "total_fees_usd": str(metrics.total_fees_usd),
        "total_slippage_usd": str(metrics.total_slippage_usd),
        "total_gas_usd": str(metrics.total_gas_usd),
        "total_execution_cost_usd": str(metrics.total_execution_cost_usd),
        "winning_trades": metrics.winning_trades,
        "losing_trades": metrics.losing_trades,
        "avg_trade_pnl_usd": str(metrics.avg_trade_pnl_usd),
        "largest_win_usd": str(metrics.largest_win_usd),
        "largest_loss_usd": str(metrics.largest_loss_usd),
        "avg_win_usd": str(metrics.avg_win_usd),
        "avg_loss_usd": str(metrics.avg_loss_usd),
        "volatility": str(metrics.volatility),
        "sortino_ratio": str(metrics.sortino_ratio),
        "calmar_ratio": str(metrics.calmar_ratio),
        "total_fees_earned_usd": str(metrics.total_fees_earned_usd),
        "fees_by_pool": {k: str(v) for k, v in metrics.fees_by_pool.items()},
        "total_funding_paid": str(metrics.total_funding_paid),
        "total_funding_received": str(metrics.total_funding_received),
        "liquidations_count": metrics.liquidations_count,
        "liquidation_losses_usd": str(metrics.liquidation_losses_usd),
        "max_margin_utilization": str(metrics.max_margin_utilization),
        "total_interest_earned": str(metrics.total_interest_earned),
        "total_interest_paid": str(metrics.total_interest_paid),
        "min_health_factor": str(metrics.min_health_factor),
        "health_factor_warnings": metrics.health_factor_warnings,
        "avg_gas_price_gwei": str(metrics.avg_gas_price_gwei),
        "max_gas_price_gwei": str(metrics.max_gas_price_gwei),
        "total_gas_cost_usd": str(metrics.total_gas_cost_usd),
        "total_mev_cost_usd": str(metrics.total_mev_cost_usd),
        "total_leverage": str(metrics.total_leverage),
        "max_net_delta": {k: str(v) for k, v in metrics.max_net_delta.items()},
        "correlation_risk": str(metrics.correlation_risk) if metrics.correlation_risk is not None else None,
        "liquidation_cascade_risk": str(metrics.liquidation_cascade_risk),
        "pnl_by_protocol": {k: str(v) for k, v in metrics.pnl_by_protocol.items()},
        "pnl_by_intent_type": {k: str(v) for k, v in metrics.pnl_by_intent_type.items()},
        "pnl_by_asset": {k: str(v) for k, v in metrics.pnl_by_asset.items()},
        "realized_pnl": str(metrics.realized_pnl),
        "unrealized_pnl": str(metrics.unrealized_pnl),
    }


def _prepare_trades_list(result: "BacktestResult") -> list[dict[str, Any]]:
    """Prepare trades list for template rendering.

    Converts TradeRecord objects to dictionaries suitable for Jinja2 template
    rendering, extracting key fields and converting types as needed.

    Args:
        result: BacktestResult containing trades

    Returns:
        List of trade dictionaries for template rendering
    """
    trades = []

    for trade in result.trades:
        trades.append(
            {
                "timestamp": trade.timestamp,
                "intent_type": trade.intent_type,
                "protocol": trade.protocol,
                "tokens": trade.tokens,
                "amount_usd": str(trade.amount_usd),
                "executed_price": str(trade.executed_price) if trade.executed_price else "0",
                "fee_usd": str(trade.fee_usd),
                "slippage_usd": str(trade.slippage_usd),
                "gas_cost_usd": str(trade.gas_cost_usd),
                "pnl_usd": str(trade.pnl_usd),
                "success": trade.success,
            }
        )

    return trades


def _prepare_result_dict(result: "BacktestResult") -> dict[str, Any]:
    """Prepare result dictionary for template rendering.

    Extracts key fields from BacktestResult for template rendering,
    converting types as needed.

    Args:
        result: BacktestResult to prepare

    Returns:
        Dictionary with result values for template rendering
    """
    return {
        "strategy_id": result.strategy_id,
        "engine": result.engine,
        "chain": result.chain if result.chain else "unknown",
        "start_time": result.start_time,
        "end_time": result.end_time,
        "simulation_duration_days": float(result.simulation_duration_days),
        "initial_capital_usd": str(result.initial_capital_usd),
        "final_capital_usd": str(result.final_capital_usd),
        "config": result.config if result.config else {},
        "run_started_at": result.run_started_at,
        "run_ended_at": result.run_ended_at,
        "run_duration_seconds": float(result.run_duration_seconds) if result.run_duration_seconds else 0,
        "error": result.error,
    }


def generate_report(
    result: "BacktestResult",
    output_path: str | Path | None = None,
    equity_chart_html: str | None = None,
    pnl_histogram_html: str | None = None,
    drawdown_chart_html: str | None = None,
    attribution_charts: dict[str, str] | None = None,
    auto_generate_charts: bool = True,
) -> ReportResult:
    """Generate an HTML report from backtest results.

    Creates a comprehensive HTML report using the Jinja2 template, including
    executive summary, detailed metrics, trade log, and optional charts.

    Args:
        result: BacktestResult to generate report from
        output_path: Optional path to save the report. If None, returns HTML content only.
        equity_chart_html: Optional HTML string for equity curve chart
        pnl_histogram_html: Optional HTML string for PnL histogram chart
        drawdown_chart_html: Optional HTML string for drawdown chart
        attribution_charts: Optional dict with attribution chart HTML strings:
            - "by_protocol": Chart showing PnL by protocol
            - "by_intent_type": Chart showing PnL by intent type
            - "by_asset": Chart showing PnL by asset
        auto_generate_charts: If True (default), automatically generate charts if not provided

    Returns:
        ReportResult with success status, file path (if saved), and HTML content

    Example:
        >>> result = generate_report(backtest_result, "report.html")
        >>> if result.success:
        ...     print(f"Report saved to {result.file_path}")
    """
    try:
        # Auto-generate charts if not provided and auto_generate_charts is True
        if auto_generate_charts:
            from almanak.framework.backtesting.visualization import (
                generate_attribution_charts_html,
                generate_drawdown_chart_html,
                generate_equity_chart_html,
                generate_pnl_distribution_html,
            )

            if equity_chart_html is None:
                equity_chart_html = generate_equity_chart_html(result)
            if pnl_histogram_html is None:
                pnl_histogram_html = generate_pnl_distribution_html(result)
            if drawdown_chart_html is None:
                drawdown_chart_html = generate_drawdown_chart_html(result)
            if attribution_charts is None:
                attribution_charts = generate_attribution_charts_html(result)

        # Set up Jinja2 environment
        env = Environment(
            loader=FileSystemLoader(str(TEMPLATE_DIR)),
            autoescape=True,
        )

        # Load the template
        template = env.get_template("report.html")

        # Prepare template variables
        metrics_dict = _prepare_metrics_dict(result)

        # Extract attribution charts (default to empty dict if None)
        attr_charts = attribution_charts or {}

        template_vars = {
            "result": _prepare_result_dict(result),
            "metrics": metrics_dict,
            "trades": _prepare_trades_list(result),
            "equity_chart_html": equity_chart_html,
            "pnl_histogram_html": pnl_histogram_html,
            "drawdown_chart_html": drawdown_chart_html,
            # Attribution charts
            "attribution_by_protocol": attr_charts.get("by_protocol", ""),
            "attribution_by_intent_type": attr_charts.get("by_intent_type", ""),
            "attribution_by_asset": attr_charts.get("by_asset", ""),
            # Attribution data for tables
            "pnl_by_protocol": metrics_dict.get("pnl_by_protocol", {}),
            "pnl_by_intent_type": metrics_dict.get("pnl_by_intent_type", {}),
            "pnl_by_asset": metrics_dict.get("pnl_by_asset", {}),
            "generated_at": datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC"),
        }

        # Render the template
        html_content = template.render(**template_vars)

        # Save to file if path provided
        file_path = None
        if output_path:
            file_path = Path(output_path)
            file_path.write_text(html_content, encoding="utf-8")
            logger.info(f"Report saved to: {file_path}")

        return ReportResult(
            success=True,
            file_path=file_path,
            html_content=html_content,
        )

    except Exception as e:
        logger.error(f"Failed to generate report: {e}")
        return ReportResult(
            success=False,
            error=str(e),
        )


def generate_report_from_json(
    json_path: str | Path,
    output_path: str | Path | None = None,
) -> ReportResult:
    """Generate an HTML report from a saved backtest JSON file.

    Loads a BacktestResult from a JSON file and generates an HTML report.

    Args:
        json_path: Path to the JSON file containing BacktestResult
        output_path: Optional path to save the report. If None, uses the same
            directory as the JSON file with .html extension.

    Returns:
        ReportResult with success status, file path, and HTML content

    Example:
        >>> result = generate_report_from_json("backtest_results.json")
        >>> if result.success:
        ...     print(f"Report saved to {result.file_path}")
    """
    import json

    from almanak.framework.backtesting.models import BacktestResult

    try:
        json_path = Path(json_path)

        # Load the JSON file
        with open(json_path, encoding="utf-8") as f:
            data = json.load(f)

        # Deserialize to BacktestResult
        result = BacktestResult.from_dict(data)

        # Determine output path
        if output_path is None:
            output_path = json_path.with_suffix(".html")

        # Generate the report
        return generate_report(result, output_path=output_path)

    except Exception as e:
        logger.error(f"Failed to generate report from JSON: {e}")
        return ReportResult(
            success=False,
            error=str(e),
        )


__all__ = [
    "ReportResult",
    "generate_report",
    "generate_report_from_json",
]
