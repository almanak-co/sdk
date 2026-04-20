"""Backtest Dashboard - Streamlit application for exploring backtest results.

This module provides the main Streamlit application for interactive
exploration of backtest results from the PnL Backtester and Paper Trader.

Usage:
    streamlit run almanak/framework/backtesting/dashboard/app.py
"""

from __future__ import annotations

import json
import sys
from decimal import Decimal
from pathlib import Path
from typing import Any, Literal

import plotly.graph_objects as go
import streamlit as st

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from almanak.framework.backtesting.models import BacktestResult, TradeRecord  # noqa: E402

# Page configuration
PAGE_TITLE = "Backtest Dashboard"
PAGE_ICON = "📊"
LAYOUT: Literal["centered", "wide"] = "wide"

# Color palette for multiple backtests
CHART_COLORS = [
    "#1f77b4",  # blue
    "#ff7f0e",  # orange
    "#2ca02c",  # green
    "#d62728",  # red
    "#9467bd",  # purple
    "#8c564b",  # brown
    "#e377c2",  # pink
    "#7f7f7f",  # gray
]


def load_backtest_result(data: dict[str, Any]) -> BacktestResult | None:
    """Load BacktestResult from dictionary data.

    Args:
        data: Dictionary containing serialized BacktestResult

    Returns:
        BacktestResult instance or None if loading fails
    """
    try:
        return BacktestResult.from_dict(data)
    except Exception as e:
        st.error(f"Failed to parse backtest result: {e}")
        return None


def format_currency(value: Decimal | float | str | None) -> str:
    """Format a value as currency.

    Args:
        value: The value to format

    Returns:
        Formatted currency string
    """
    if value is None:
        return "-"
    try:
        num_value = float(value) if not isinstance(value, float) else value
        if abs(num_value) >= 1_000_000:
            return f"${num_value / 1_000_000:,.2f}M"
        if abs(num_value) >= 1_000:
            return f"${num_value / 1_000:,.2f}K"
        return f"${num_value:,.2f}"
    except (ValueError, TypeError):
        return "-"


def format_percentage(value: Decimal | float | str | None) -> str:
    """Format a value as percentage.

    Args:
        value: The value to format (as decimal, e.g., 0.05 = 5%)

    Returns:
        Formatted percentage string
    """
    if value is None:
        return "-"
    try:
        num_value = float(value) if not isinstance(value, float) else value
        return f"{num_value * 100:.2f}%"
    except (ValueError, TypeError):
        return "-"


def render_equity_curve(
    results: dict[str, BacktestResult],
    selected_results: list[str],
    normalize: bool = False,
) -> None:
    """Render interactive equity curve with zoom/pan support.

    Args:
        results: Dictionary of backtest results keyed by name
        selected_results: List of result names to display
        normalize: If True, normalize values to percentage returns
    """
    if not selected_results:
        st.info("Select at least one backtest to display the equity curve.")
        return

    fig = go.Figure()

    for idx, result_name in enumerate(selected_results):
        result = results.get(result_name)
        if result is None or not result.equity_curve:
            continue

        color = CHART_COLORS[idx % len(CHART_COLORS)]

        # Extract timestamps and values
        timestamps = [p.timestamp for p in result.equity_curve]
        values = [float(p.value_usd) for p in result.equity_curve]

        if normalize and values:
            # Normalize to percentage returns from initial value
            initial = values[0] if values[0] != 0 else 1
            values = [(v / initial - 1) * 100 for v in values]
            y_label = "Return (%)"
        else:
            y_label = "Portfolio Value (USD)"

        fig.add_trace(
            go.Scatter(
                x=timestamps,
                y=values,
                mode="lines",
                name=result_name,
                line={"color": color, "width": 2},
                hovertemplate=(
                    f"<b>{result_name}</b><br>"
                    + "Date: %{x|%Y-%m-%d %H:%M}<br>"
                    + ("Return: %{y:.2f}%<br>" if normalize else "Value: $%{y:,.2f}<br>")
                    + "<extra></extra>"
                ),
            )
        )

    fig.update_layout(
        title={
            "text": "Equity Curve" + (" (Normalized)" if normalize else ""),
            "x": 0.5,
            "xanchor": "center",
        },
        xaxis={
            "title": "Date",
            "rangeslider": {"visible": True},
            "rangeselector": {
                "buttons": [
                    {"count": 1, "label": "1D", "step": "day", "stepmode": "backward"},
                    {"count": 7, "label": "1W", "step": "day", "stepmode": "backward"},
                    {"count": 1, "label": "1M", "step": "month", "stepmode": "backward"},
                    {"count": 3, "label": "3M", "step": "month", "stepmode": "backward"},
                    {"step": "all", "label": "All"},
                ]
            },
        },
        yaxis={
            "title": y_label,
            "tickformat": ".2f%" if normalize else "$,.0f",
        },
        hovermode="x unified",
        legend={
            "orientation": "h",
            "yanchor": "bottom",
            "y": 1.02,
            "xanchor": "right",
            "x": 1,
        },
        height=500,
        template="plotly_dark",
    )

    # Enable zoom/pan
    fig.update_xaxes(fixedrange=False)
    fig.update_yaxes(fixedrange=False)

    st.plotly_chart(fig, use_container_width=True)


def render_comparison_metrics(results: dict[str, BacktestResult], selected_results: list[str]) -> None:
    """Render comparison table of key metrics across multiple backtests.

    Args:
        results: Dictionary of backtest results keyed by name
        selected_results: List of result names to compare
    """
    if len(selected_results) < 2:
        return

    st.subheader("Metrics Comparison")

    # Build comparison data
    comparison_data = []
    for result_name in selected_results:
        result = results.get(result_name)
        if result is None or result.metrics is None:
            continue

        metrics = result.metrics
        comparison_data.append(
            {
                "Backtest": result_name,
                "Net PnL": format_currency(metrics.net_pnl_usd),
                "Return": format_percentage(metrics.total_return_pct),
                "Sharpe": f"{float(metrics.sharpe_ratio):.3f}" if metrics.sharpe_ratio else "-",
                "Sortino": f"{float(metrics.sortino_ratio):.3f}" if metrics.sortino_ratio else "-",
                "Max DD": format_percentage(metrics.max_drawdown_pct),
                "Win Rate": format_percentage(metrics.win_rate),
                "Total Trades": str(metrics.total_trades),
                "Profit Factor": f"{float(metrics.profit_factor):.2f}" if metrics.profit_factor else "-",
            }
        )

    if comparison_data:
        st.dataframe(comparison_data, use_container_width=True)


def render_summary_metrics(result: BacktestResult) -> None:
    """Render the summary metrics section.

    Args:
        result: BacktestResult to display metrics for
    """
    metrics = result.metrics
    if metrics is None:
        st.warning("No metrics available in this backtest result.")
        return

    st.subheader("Performance Summary")

    # Row 1: Key performance metrics
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        pnl_value = float(metrics.net_pnl_usd) if metrics.net_pnl_usd else 0
        delta_color: Literal["normal", "inverse"] = "normal" if pnl_value >= 0 else "inverse"
        st.metric(
            label="Net PnL",
            value=format_currency(metrics.net_pnl_usd),
            delta=format_percentage(metrics.total_return_pct),
            delta_color=delta_color,
        )

    with col2:
        return_pct = float(metrics.total_return_pct) if metrics.total_return_pct else 0
        st.metric(
            label="Total Return",
            value=format_percentage(metrics.total_return_pct),
            delta=f"{return_pct * 100:.1f}% total" if return_pct else None,
            delta_color="normal" if return_pct >= 0 else "inverse",
        )

    with col3:
        sharpe = float(metrics.sharpe_ratio) if metrics.sharpe_ratio else 0
        st.metric(
            label="Sharpe Ratio",
            value=f"{sharpe:.3f}",
            help="Risk-adjusted return metric. >1 is good, >2 is excellent.",
        )

    with col4:
        drawdown = float(metrics.max_drawdown_pct) if metrics.max_drawdown_pct else 0
        st.metric(
            label="Max Drawdown",
            value=format_percentage(metrics.max_drawdown_pct),
            delta=f"{abs(drawdown) * 100:.1f}% peak decline",
            delta_color="inverse",
        )

    # Row 2: Trading statistics
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            label="Total Trades",
            value=str(metrics.total_trades),
        )

    with col2:
        win_rate = float(metrics.win_rate) if metrics.win_rate else 0
        st.metric(
            label="Win Rate",
            value=format_percentage(metrics.win_rate),
            delta="above 50%" if win_rate > 0.5 else "below 50%",
            delta_color="normal" if win_rate >= 0.5 else "inverse",
        )

    with col3:
        st.metric(
            label="Winning Trades",
            value=str(metrics.winning_trades),
        )

    with col4:
        st.metric(
            label="Losing Trades",
            value=str(metrics.losing_trades),
        )

    # Row 3: Capital and costs
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            label="Initial Capital",
            value=format_currency(result.initial_capital_usd),
        )

    with col2:
        st.metric(
            label="Final Capital",
            value=format_currency(result.final_capital_usd),
        )

    with col3:
        st.metric(
            label="Total Fees",
            value=format_currency(metrics.total_fees_usd),
        )

    with col4:
        st.metric(
            label="Total Slippage",
            value=format_currency(metrics.total_slippage_usd),
        )


def render_risk_metrics(result: BacktestResult) -> None:
    """Render the risk metrics section.

    Args:
        result: BacktestResult to display risk metrics for
    """
    metrics = result.metrics
    if metrics is None:
        return

    st.subheader("Risk Metrics")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        sortino = float(metrics.sortino_ratio) if metrics.sortino_ratio else 0
        st.metric(
            label="Sortino Ratio",
            value=f"{sortino:.3f}",
            help="Downside risk-adjusted return. Higher is better.",
        )

    with col2:
        calmar = float(metrics.calmar_ratio) if metrics.calmar_ratio else 0
        st.metric(
            label="Calmar Ratio",
            value=f"{calmar:.3f}",
            help="Return / Max Drawdown. Higher is better.",
        )

    with col3:
        st.metric(
            label="Volatility (Ann.)",
            value=format_percentage(metrics.volatility),
            help="Annualized standard deviation of returns.",
        )

    with col4:
        profit_factor = float(metrics.profit_factor) if metrics.profit_factor else 0
        st.metric(
            label="Profit Factor",
            value=f"{profit_factor:.2f}",
            help="Gross profit / Gross loss. >1 means profitable.",
        )


def render_backtest_info(result: BacktestResult) -> None:
    """Render backtest information section.

    Args:
        result: BacktestResult to display info for
    """
    st.subheader("Backtest Information")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Strategy Details**")
        info_data = {
            "Strategy ID": result.strategy_id,
            "Engine": result.engine.value if result.engine else "-",
            "Chain": result.chain or "-",
            "Duration": f"{result.simulation_duration_days:.1f} days",
        }
        for key, value in info_data.items():
            st.text(f"{key}: {value}")

    with col2:
        st.markdown("**Time Range**")
        time_data = {
            "Start Time": result.start_time.strftime("%Y-%m-%d %H:%M:%S") if result.start_time else "-",
            "End Time": result.end_time.strftime("%Y-%m-%d %H:%M:%S") if result.end_time else "-",
            "Run Started": result.run_started_at.strftime("%Y-%m-%d %H:%M:%S") if result.run_started_at else "-",
            "Run Duration": f"{result.run_duration_seconds:.2f}s" if result.run_duration_seconds else "-",
        }
        for key, value in time_data.items():
            st.text(f"{key}: {value}")


def render_trade_explorer(result: BacktestResult) -> None:
    """Render trade explorer with filtering by date, type, and PnL.

    Args:
        result: BacktestResult to display trades for
    """
    st.subheader("Trade Explorer")

    if not result.trades:
        st.info("No trades recorded in this backtest.")
        return

    # Get all unique intent types from trades
    all_intent_types = sorted({t.intent_type.value for t in result.trades})

    # Filter controls
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        # Date range filter
        min_date = min(t.timestamp.date() for t in result.trades)
        max_date = max(t.timestamp.date() for t in result.trades)
        date_range = st.date_input(
            "Date Range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date,
            help="Filter trades by date range",
        )

    with col2:
        # Intent type filter
        selected_types = st.multiselect(
            "Trade Type",
            options=all_intent_types,
            default=all_intent_types,
            help="Filter by trade type",
        )

    with col3:
        # PnL filter
        pnl_filter = st.selectbox(
            "PnL Filter",
            options=["All", "Profitable", "Losing", "Zero/No PnL"],
            index=0,
            help="Filter by trade outcome",
        )

    with col4:
        # Success filter
        success_filter = st.selectbox(
            "Status",
            options=["All", "Successful", "Failed"],
            index=0,
            help="Filter by execution status",
        )

    # Apply filters
    filtered_trades: list[TradeRecord] = []
    for trade in result.trades:
        # Date filter
        if isinstance(date_range, tuple) and len(date_range) == 2:
            start_date, end_date = date_range
            if not (start_date <= trade.timestamp.date() <= end_date):
                continue

        # Type filter
        if trade.intent_type.value not in selected_types:
            continue

        # PnL filter
        pnl_value = float(trade.pnl_usd)
        if pnl_filter == "Profitable" and pnl_value <= 0:
            continue
        if pnl_filter == "Losing" and pnl_value >= 0:
            continue
        if pnl_filter == "Zero/No PnL" and pnl_value != 0:
            continue

        # Success filter
        if success_filter == "Successful" and not trade.success:
            continue
        if success_filter == "Failed" and trade.success:
            continue

        filtered_trades.append(trade)

    # Display stats
    st.markdown(f"**Showing {len(filtered_trades)} of {len(result.trades)} trades**")

    # Build trade table data
    if filtered_trades:
        table_data = []
        for trade in filtered_trades:
            pnl = float(trade.pnl_usd)
            net_pnl = float(trade.net_pnl_usd)
            table_data.append(
                {
                    "Timestamp": trade.timestamp.strftime("%Y-%m-%d %H:%M"),
                    "Type": trade.intent_type.value,
                    "Tokens": ", ".join(trade.tokens) if trade.tokens else "-",
                    "Protocol": trade.protocol or "-",
                    "Amount": format_currency(trade.amount_usd),
                    "PnL": format_currency(pnl),
                    "Net PnL": format_currency(net_pnl),
                    "Fee": format_currency(trade.fee_usd),
                    "Gas": format_currency(trade.gas_cost_usd),
                    "Status": "✓" if trade.success else "✗",
                }
            )

        st.dataframe(table_data, use_container_width=True, height=400)

        # Trade summary stats for filtered trades
        st.markdown("**Filtered Trade Summary**")
        total_pnl = sum(float(t.pnl_usd) for t in filtered_trades)
        total_fees = sum(float(t.fee_usd) for t in filtered_trades)
        total_gas = sum(float(t.gas_cost_usd) for t in filtered_trades)
        winning_trades = sum(1 for t in filtered_trades if float(t.pnl_usd) > 0)
        losing_trades = sum(1 for t in filtered_trades if float(t.pnl_usd) < 0)

        col1, col2, col3, col4, col5 = st.columns(5)
        with col1:
            st.metric("Total PnL", format_currency(total_pnl))
        with col2:
            st.metric("Total Fees", format_currency(total_fees))
        with col3:
            st.metric("Total Gas", format_currency(total_gas))
        with col4:
            st.metric("Winning", str(winning_trades))
        with col5:
            st.metric("Losing", str(losing_trades))
    else:
        st.warning("No trades match the current filter criteria.")


def render_position_breakdown(result: BacktestResult) -> None:
    """Render position breakdown over time visualization.

    Args:
        result: BacktestResult to display position breakdown for
    """
    st.subheader("Position Breakdown Over Time")

    if not result.equity_curve:
        st.info("No equity curve data available.")
        return

    # Group trades by type to show position distribution
    trades_by_type: dict[str, list[TradeRecord]] = {}
    for trade in result.trades:
        trade_type = trade.intent_type.value
        if trade_type not in trades_by_type:
            trades_by_type[trade_type] = []
        trades_by_type[trade_type].append(trade)

    if not trades_by_type:
        st.info("No trade data available for position breakdown.")
        return

    # Create cumulative PnL by trade type over time
    fig = go.Figure()

    # Track cumulative PnL by type
    for idx, (trade_type, trades) in enumerate(sorted(trades_by_type.items())):
        # Sort trades by timestamp
        sorted_trades = sorted(trades, key=lambda t: t.timestamp)

        # Build cumulative PnL series
        timestamps = []
        cumulative_pnl = []
        running_total = 0.0

        for trade in sorted_trades:
            running_total += float(trade.pnl_usd)
            timestamps.append(trade.timestamp)
            cumulative_pnl.append(running_total)

        color = CHART_COLORS[idx % len(CHART_COLORS)]

        fig.add_trace(
            go.Scatter(
                x=timestamps,
                y=cumulative_pnl,
                mode="lines+markers",
                name=trade_type,
                line={"color": color, "width": 2},
                marker={"size": 6},
                hovertemplate=(
                    f"<b>{trade_type}</b><br>"
                    + "Date: %{x|%Y-%m-%d %H:%M}<br>"
                    + "Cumulative PnL: $%{y:,.2f}<br>"
                    + "<extra></extra>"
                ),
            )
        )

    fig.update_layout(
        title={
            "text": "Cumulative PnL by Trade Type",
            "x": 0.5,
            "xanchor": "center",
        },
        xaxis={"title": "Date"},
        yaxis={"title": "Cumulative PnL (USD)", "tickformat": "$,.0f"},
        hovermode="x unified",
        legend={
            "orientation": "h",
            "yanchor": "bottom",
            "y": 1.02,
            "xanchor": "right",
            "x": 1,
        },
        height=400,
        template="plotly_dark",
    )

    st.plotly_chart(fig, use_container_width=True)

    # Position type breakdown summary
    st.markdown("**Position Type Summary**")

    summary_data = []
    for trade_type, trades in sorted(trades_by_type.items()):
        total_pnl = sum(float(t.pnl_usd) for t in trades)
        total_trades = len(trades)
        successful = sum(1 for t in trades if t.success)
        total_fees = sum(float(t.fee_usd) for t in trades)

        summary_data.append(
            {
                "Type": trade_type,
                "Trades": total_trades,
                "Successful": successful,
                "Total PnL": format_currency(total_pnl),
                "Total Fees": format_currency(total_fees),
                "Avg PnL/Trade": format_currency(total_pnl / total_trades if total_trades else 0),
            }
        )

    st.dataframe(summary_data, use_container_width=True)


def render_enhanced_metrics_comparison(
    results: dict[str, BacktestResult],
    selected_results: list[str],
) -> None:
    """Render enhanced metrics comparison view with more details.

    Args:
        results: Dictionary of backtest results keyed by name
        selected_results: List of result names to compare
    """
    if len(selected_results) < 2:
        st.info("Select at least 2 backtests to compare metrics.")
        return

    st.subheader("Detailed Metrics Comparison")

    # Create tabs for different metric categories
    tab1, tab2, tab3, tab4 = st.tabs(["Performance", "Risk", "Trading", "Execution Costs"])

    with tab1:
        # Performance metrics comparison
        perf_data = []
        for result_name in selected_results:
            result = results.get(result_name)
            if result is None or result.metrics is None:
                continue
            m = result.metrics
            perf_data.append(
                {
                    "Backtest": result_name,
                    "Net PnL": format_currency(m.net_pnl_usd),
                    "Total Return": format_percentage(m.total_return_pct),
                    "Annualized Return": format_percentage(m.annualized_return_pct),
                    "Initial Capital": format_currency(result.initial_capital_usd),
                    "Final Capital": format_currency(result.final_capital_usd),
                }
            )
        if perf_data:
            st.dataframe(perf_data, use_container_width=True)

    with tab2:
        # Risk metrics comparison
        risk_data = []
        for result_name in selected_results:
            result = results.get(result_name)
            if result is None or result.metrics is None:
                continue
            m = result.metrics
            risk_data.append(
                {
                    "Backtest": result_name,
                    "Sharpe": f"{float(m.sharpe_ratio):.3f}" if m.sharpe_ratio else "-",
                    "Sortino": f"{float(m.sortino_ratio):.3f}" if m.sortino_ratio else "-",
                    "Calmar": f"{float(m.calmar_ratio):.3f}" if m.calmar_ratio else "-",
                    "Max Drawdown": format_percentage(m.max_drawdown_pct),
                    "Volatility": format_percentage(m.volatility),
                    "Profit Factor": f"{float(m.profit_factor):.2f}" if m.profit_factor else "-",
                }
            )
        if risk_data:
            st.dataframe(risk_data, use_container_width=True)

    with tab3:
        # Trading statistics comparison
        trade_data = []
        for result_name in selected_results:
            result = results.get(result_name)
            if result is None or result.metrics is None:
                continue
            m = result.metrics
            trade_data.append(
                {
                    "Backtest": result_name,
                    "Total Trades": m.total_trades,
                    "Winning": m.winning_trades,
                    "Losing": m.losing_trades,
                    "Win Rate": format_percentage(m.win_rate),
                    "Avg Trade": format_currency(m.avg_trade_pnl_usd),
                    "Largest Win": format_currency(m.largest_win_usd),
                    "Largest Loss": format_currency(m.largest_loss_usd),
                }
            )
        if trade_data:
            st.dataframe(trade_data, use_container_width=True)

    with tab4:
        # Execution costs comparison
        cost_data = []
        for result_name in selected_results:
            result = results.get(result_name)
            if result is None or result.metrics is None:
                continue
            m = result.metrics
            total_cost = float(m.total_fees_usd) + float(m.total_slippage_usd) + float(m.total_gas_usd)
            cost_data.append(
                {
                    "Backtest": result_name,
                    "Total Fees": format_currency(m.total_fees_usd),
                    "Total Slippage": format_currency(m.total_slippage_usd),
                    "Total Gas": format_currency(m.total_gas_usd),
                    "Total Costs": format_currency(total_cost),
                    "Cost/Trade": format_currency(total_cost / m.total_trades if m.total_trades > 0 else 0),
                }
            )
        if cost_data:
            st.dataframe(cost_data, use_container_width=True)

    # Radar chart for visual comparison
    st.markdown("**Performance Radar Chart**")

    fig = go.Figure()

    categories = ["Return", "Sharpe", "Win Rate", "Profit Factor", "Volatility"]

    for idx, result_name in enumerate(selected_results[:4]):  # Limit to 4 for clarity
        result = results.get(result_name)
        if result is None or result.metrics is None:
            continue

        m = result.metrics

        # Normalize values to 0-1 scale for radar chart
        values = [
            min(1, max(-1, float(m.total_return_pct) * 5)),  # -20% to 20% scaled
            min(1, max(0, float(m.sharpe_ratio) / 3)),  # 0-3 scaled
            float(m.win_rate),  # 0-1 already
            min(1, max(0, float(m.profit_factor) / 3)),  # 0-3 scaled
            1 - min(1, float(m.volatility) * 2),  # Lower volatility is better
        ]
        # Close the radar shape
        values.append(values[0])
        cats = categories + [categories[0]]

        color = CHART_COLORS[idx % len(CHART_COLORS)]

        fig.add_trace(
            go.Scatterpolar(
                r=values,
                theta=cats,
                fill="toself",
                name=result_name,
                line={"color": color},
                opacity=0.6,
            )
        )

    fig.update_layout(
        polar={
            "radialaxis": {
                "visible": True,
                "range": [0, 1],
            }
        },
        showlegend=True,
        height=400,
        template="plotly_dark",
    )

    st.plotly_chart(fig, use_container_width=True)


def main() -> None:
    """Main dashboard application entry point."""
    st.set_page_config(
        page_title=PAGE_TITLE,
        page_icon=PAGE_ICON,
        layout=LAYOUT,
        initial_sidebar_state="expanded",
    )

    # Custom CSS for dark theme consistency
    st.markdown(
        """
        <style>
        .main > div {
            padding-top: 2rem;
        }
        .stMetric {
            background-color: rgba(28, 131, 225, 0.1);
            padding: 1rem;
            border-radius: 0.5rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    # Initialize session state for backtest results
    if "backtest_results" not in st.session_state:
        st.session_state["backtest_results"] = {}

    # Header
    st.title(f"{PAGE_ICON} {PAGE_TITLE}")
    st.markdown("Upload backtest JSON results to explore performance metrics and compare strategies.")

    # Sidebar - File upload and controls
    with st.sidebar:
        st.header("Load Results")

        uploaded_files = st.file_uploader(
            "Upload Backtest JSON",
            type=["json"],
            help="Upload one or more JSON files containing BacktestResult data",
            accept_multiple_files=True,
        )

        # Process uploaded files
        if uploaded_files:
            for uploaded_file in uploaded_files:
                file_name = uploaded_file.name
                # Skip if already loaded
                if file_name in st.session_state["backtest_results"]:
                    continue

                try:
                    data = json.load(uploaded_file)
                    result = load_backtest_result(data)
                    if result is not None:
                        # Use strategy_id as display name, fallback to filename
                        display_name = result.strategy_id or file_name.replace(".json", "")
                        # Ensure unique names
                        base_name = display_name
                        counter = 1
                        while display_name in st.session_state["backtest_results"]:
                            display_name = f"{base_name}_{counter}"
                            counter += 1
                        st.session_state["backtest_results"][display_name] = result
                        st.success(f"Loaded: {display_name}")
                except json.JSONDecodeError as e:
                    st.error(f"Invalid JSON in {file_name}: {e}")

        # Show loaded backtests
        if st.session_state["backtest_results"]:
            st.divider()
            st.markdown("### Loaded Backtests")

            for name in list(st.session_state["backtest_results"].keys()):
                col1, col2 = st.columns([3, 1])
                with col1:
                    st.text(f"📈 {name}")
                with col2:
                    if st.button("🗑️", key=f"remove_{name}", help=f"Remove {name}"):
                        del st.session_state["backtest_results"][name]
                        st.rerun()

            if st.button("Clear All", key="clear_all"):
                st.session_state["backtest_results"] = {}
                st.rerun()

        st.divider()
        st.markdown("### About")
        st.markdown(
            """
            This dashboard allows you to explore and compare backtest results
            from the Almanak SDK backtesting engines:
            - **PnL Backtester**: Historical price simulation
            - **Paper Trader**: Real-time fork execution

            **Features:**
            - Upload multiple backtests for comparison
            - Interactive equity curve with zoom/pan
            - Side-by-side metrics comparison
            """
        )

    # Main content area
    results = st.session_state["backtest_results"]

    if results:
        # Parameter selector for comparing multiple backtests
        st.subheader("Select Backtests to Compare")

        result_names = list(results.keys())

        # Multi-select for backtest comparison
        selected_results = st.multiselect(
            "Select backtests",
            options=result_names,
            default=result_names[: min(3, len(result_names))],  # Default to first 3
            help="Select one or more backtests to display and compare",
        )

        if selected_results:
            # Chart controls
            col1, col2 = st.columns([1, 4])
            with col1:
                normalize_chart = st.checkbox(
                    "Normalize Returns",
                    value=False,
                    help="Show percentage returns instead of absolute values",
                )

            # Render interactive equity curve
            st.divider()
            render_equity_curve(results, selected_results, normalize=normalize_chart)

            # Render comparison metrics if multiple selected
            if len(selected_results) > 1:
                st.divider()
                render_comparison_metrics(results, selected_results)

            # Render detailed metrics for primary selection
            st.divider()
            primary_result = results.get(selected_results[0])
            if primary_result:
                st.markdown(f"### Detailed Metrics: **{selected_results[0]}**")

                # Allow switching primary result
                if len(selected_results) > 1:
                    primary_selection = st.selectbox(
                        "Show details for:",
                        options=selected_results,
                        index=0,
                    )
                    primary_result = results.get(primary_selection)

                if primary_result:
                    render_summary_metrics(primary_result)

                    st.divider()
                    render_risk_metrics(primary_result)

                    st.divider()
                    render_backtest_info(primary_result)

                    st.divider()
                    render_trade_explorer(primary_result)

                    st.divider()
                    render_position_breakdown(primary_result)

            # Enhanced metrics comparison (always show if 2+ selected)
            if len(selected_results) >= 2:
                st.divider()
                render_enhanced_metrics_comparison(results, selected_results)

    else:
        # No file uploaded - show instructions
        st.info(
            """
            **Getting Started**

            1. Run a backtest using the Almanak SDK
            2. Save the result to JSON using `result.to_json(path)`
            3. Upload one or more JSON files using the sidebar

            The dashboard will display:
            - **Interactive equity curve** with zoom/pan and range selector
            - **Trade explorer** with filtering by date, type, and PnL
            - **Position breakdown** over time visualization
            - **Metrics comparison** for multiple backtests (with radar chart)
            - Performance metrics (PnL, returns, Sharpe ratio)
            - Risk metrics (drawdown, volatility, Sortino ratio)
            - Trading statistics (win rate, trade counts)
            """
        )

        # Show example structure
        with st.expander("Expected JSON Structure"):
            st.code(
                """
{
    "strategy_id": "my_strategy",
    "engine": "pnl",
    "chain": "arbitrum",
    "start_time": "2024-01-01T00:00:00",
    "end_time": "2024-01-31T23:59:59",
    "initial_capital_usd": "10000",
    "final_capital_usd": "10500",
    "metrics": {
        "net_pnl_usd": "500",
        "total_return_pct": "0.05",
        "sharpe_ratio": "1.5",
        "max_drawdown_pct": "0.03",
        ...
    },
    "trades": [...],
    "equity_curve": [...]
}
                """,
                language="json",
            )


if __name__ == "__main__":
    main()
