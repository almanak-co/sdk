"""Portfolio plots for strategy dashboards.

This module provides visualization components for portfolio-level metrics including:
- Portfolio value over time
- PnL waterfall charts
- Asset allocation breakdown
- Trade history visualization

These plots are useful for any strategy type to track overall performance.

Example:
    from almanak.framework.dashboard.plots.portfolio_plots import (
        plot_portfolio_value_over_time,
        plot_pnl_waterfall,
        plot_asset_allocation,
    )

    # Portfolio value
    fig = plot_portfolio_value_over_time(
        value_data=portfolio_df,
        benchmark_data=eth_hold_df,
    )
    st.plotly_chart(fig)
"""

from dataclasses import dataclass
from datetime import datetime

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from almanak.framework.dashboard.plots.base import (
    PlotConfig,
    apply_theme,
    create_empty_figure,
    format_usd,
    get_default_config,
)


@dataclass
class TradeRecord:
    """Record of a single trade.

    Attributes:
        timestamp: When the trade occurred
        trade_type: Type of trade (BUY, SELL, SWAP, etc.)
        asset: Asset traded
        amount: Amount traded
        price: Price at execution
        value_usd: Trade value in USD
        pnl: Profit/loss from this trade
        fees: Fees paid
    """

    timestamp: datetime
    trade_type: str
    asset: str
    amount: float
    price: float
    value_usd: float
    pnl: float | None = None
    fees: float | None = None


def plot_portfolio_value_over_time(
    value_data: pd.DataFrame | list[dict],
    benchmark_data: pd.DataFrame | list[dict] | None = None,
    time_column: str = "timestamp",
    value_column: str = "value",
    benchmark_column: str = "value",
    show_drawdown: bool = True,
    normalize: bool = False,
    title: str = "Portfolio Value Over Time",
    benchmark_label: str = "Benchmark",
    config: PlotConfig | None = None,
) -> go.Figure:
    """Plot portfolio value over time with optional benchmark.

    Args:
        value_data: DataFrame or list with portfolio value history
            Expected columns: timestamp, value
        benchmark_data: Optional benchmark data for comparison
        time_column: Name of time column
        value_column: Name of value column
        benchmark_column: Name of benchmark value column
        show_drawdown: If True, highlight drawdown periods
        normalize: If True, show percentage returns instead of absolute values
        title: Chart title
        benchmark_label: Label for benchmark line
        config: Plot configuration

    Returns:
        Plotly figure with portfolio value chart
    """
    config = config or get_default_config()
    colors = config.colors

    # Convert to DataFrame if necessary
    if isinstance(value_data, list):
        if not value_data:
            return create_empty_figure("No portfolio data", config)
        df = pd.DataFrame(value_data)
    else:
        df = value_data.copy()

    if df.empty:
        return create_empty_figure("No portfolio data", config)

    # Normalize column names
    time_col = time_column if time_column in df.columns else "timestamp"
    val_col = value_column if value_column in df.columns else "value"

    # Handle alternative column names
    if time_col not in df.columns:
        for alt in ["time", "Time", "date", "Date", "Timestamp"]:
            if alt in df.columns:
                time_col = alt
                break

    if val_col not in df.columns:
        for alt in ["total_value", "portfolio_value", "Value"]:
            if alt in df.columns:
                val_col = alt
                break

    if time_col not in df.columns or val_col not in df.columns:
        return create_empty_figure("Invalid portfolio data format", config)

    # Ensure datetime
    if not pd.api.types.is_datetime64_any_dtype(df[time_col]):
        df[time_col] = pd.to_datetime(df[time_col])

    df = df.sort_values(time_col)

    # Normalize to percentage if requested
    if normalize:
        initial_value = df[val_col].iloc[0]
        if initial_value == 0:
            # Cannot normalize from zero starting value
            plot_col = val_col
            y_title = "Portfolio Value (USD)"
        else:
            df["normalized"] = (df[val_col] / initial_value - 1) * 100
            plot_col = "normalized"
            y_title = "Return (%)"
    else:
        plot_col = val_col
        y_title = "Portfolio Value (USD)"

    # Create figure
    fig = go.Figure()

    # Add portfolio value line
    fig.add_trace(
        go.Scatter(
            x=df[time_col],
            y=df[plot_col],
            mode="lines",
            name="Portfolio",
            line={"color": colors.primary, "width": config.line_width},
            fill="tozeroy" if normalize else None,
            fillcolor=f"rgba({int(colors.primary[1:3], 16)}, {int(colors.primary[3:5], 16)}, {int(colors.primary[5:7], 16)}, 0.1)"
            if normalize
            else None,
        )
    )

    # Add benchmark if provided
    if benchmark_data is not None:
        if isinstance(benchmark_data, list):
            bench_df = pd.DataFrame(benchmark_data)
        else:
            bench_df = benchmark_data.copy()

        if not bench_df.empty:
            bench_time_col = time_column if time_column in bench_df.columns else time_col
            bench_val_col = benchmark_column if benchmark_column in bench_df.columns else val_col

            if bench_time_col in bench_df.columns and bench_val_col in bench_df.columns:
                if not pd.api.types.is_datetime64_any_dtype(bench_df[bench_time_col]):
                    bench_df[bench_time_col] = pd.to_datetime(bench_df[bench_time_col])

                bench_df = bench_df.sort_values(bench_time_col)

                if normalize and plot_col == "normalized":
                    initial_bench = bench_df[bench_val_col].iloc[0]
                    if initial_bench != 0:
                        bench_df["normalized"] = (bench_df[bench_val_col] / initial_bench - 1) * 100
                        bench_plot_col = "normalized"
                    else:
                        bench_plot_col = bench_val_col
                else:
                    bench_plot_col = bench_val_col

                fig.add_trace(
                    go.Scatter(
                        x=bench_df[bench_time_col],
                        y=bench_df[bench_plot_col],
                        mode="lines",
                        name=benchmark_label,
                        line={"color": colors.neutral, "width": config.line_width, "dash": "dash"},
                    )
                )

    # Add drawdown highlighting
    if show_drawdown and not normalize:
        running_max = df[val_col].expanding().max()
        # Guard against division by zero
        drawdown = (df[val_col] - running_max) / running_max.replace(0, float("nan"))
        drawdown = drawdown.fillna(0)

        # Find drawdown periods (more than 5% drawdown)
        in_drawdown = drawdown < -0.05
        if in_drawdown.any():
            # Add shaded regions for drawdowns
            drawdown_start = None
            for _i, (is_dd, time, _val) in enumerate(zip(in_drawdown, df[time_col], df[val_col], strict=False)):
                if is_dd and drawdown_start is None:
                    drawdown_start = time
                elif not is_dd and drawdown_start is not None:
                    fig.add_vrect(
                        x0=drawdown_start,
                        x1=time,
                        fillcolor=colors.danger,
                        opacity=0.1,
                        layer="below",
                        line_width=0,
                    )
                    drawdown_start = None

            # Handle ongoing drawdown
            if drawdown_start is not None:
                fig.add_vrect(
                    x0=drawdown_start,
                    x1=df[time_col].iloc[-1],
                    fillcolor=colors.danger,
                    opacity=0.1,
                    layer="below",
                    line_width=0,
                )

    # Add zero line for normalized view
    if normalize:
        fig.add_hline(y=0, line_dash="dash", line_color=colors.neutral)

    fig.update_layout(
        title={"text": title, "font": {"size": config.title_font_size}},
        xaxis_title="Date",
        yaxis_title=y_title,
        xaxis={"rangeslider": {"visible": True}},
        hovermode="x unified",
    )

    return apply_theme(fig, config)


def plot_pnl_waterfall(
    pnl_components: dict[str, float],
    title: str = "PnL Breakdown",
    config: PlotConfig | None = None,
) -> go.Figure:
    """Plot PnL waterfall chart showing contribution of each component.

    Args:
        pnl_components: Dictionary of component name -> PnL value
            Example: {"Trading": 1000, "Fees": -50, "IL": -200, "Rewards": 100}
        title: Chart title
        config: Plot configuration

    Returns:
        Plotly figure with waterfall chart
    """
    config = config or get_default_config()
    colors = config.colors

    if not pnl_components:
        return create_empty_figure("No PnL data", config)

    # Prepare data for waterfall
    labels = list(pnl_components.keys()) + ["Total"]
    values = list(pnl_components.values())
    total = sum(values)

    # Determine measure types
    measures = ["relative"] * len(pnl_components) + ["total"]

    # Determine colors
    bar_colors = []
    for v in values:
        if v >= 0:
            bar_colors.append(colors.profit)
        else:
            bar_colors.append(colors.loss)
    bar_colors.append(colors.primary if total >= 0 else colors.loss)

    fig = go.Figure(
        go.Waterfall(
            name="PnL",
            orientation="v",
            measure=measures,
            x=labels,
            y=values + [total],
            text=[format_usd(v) for v in values] + [format_usd(total)],
            textposition="outside",
            connector={"line": {"color": colors.neutral}},
            increasing={"marker": {"color": colors.profit}},
            decreasing={"marker": {"color": colors.loss}},
            totals={"marker": {"color": colors.primary if total >= 0 else colors.loss}},
        )
    )

    fig.update_layout(
        title={"text": title, "font": {"size": config.title_font_size}},
        yaxis_title="PnL (USD)",
        showlegend=False,
    )

    return apply_theme(fig, config)


def plot_asset_allocation(
    assets: dict[str, float],
    title: str = "Asset Allocation",
    show_percentages: bool = True,
    config: PlotConfig | None = None,
) -> go.Figure:
    """Plot asset allocation as a treemap or pie chart.

    Args:
        assets: Dictionary of asset symbol -> value in USD
        title: Chart title
        show_percentages: Whether to show percentage labels
        config: Plot configuration

    Returns:
        Plotly figure with asset allocation
    """
    config = config or get_default_config()
    colors = config.colors

    if not assets:
        return create_empty_figure("No asset data", config)

    # Sort by value descending
    sorted_assets = dict(sorted(assets.items(), key=lambda x: x[1], reverse=True))

    labels = list(sorted_assets.keys())
    values = list(sorted_assets.values())

    # Generate colors
    asset_colors = [
        colors.primary,
        colors.secondary,
        colors.accent,
        colors.success,
        colors.warning,
        colors.info,
        "#E91E63",
        "#00BCD4",
    ]
    pie_colors = [asset_colors[i % len(asset_colors)] for i in range(len(labels))]

    # Create treemap
    fig = go.Figure(
        go.Treemap(
            labels=labels,
            parents=[""] * len(labels),
            values=values,
            textinfo="label+value+percent entry" if show_percentages else "label+value",
            marker={"colors": pie_colors},
            hovertemplate="<b>%{label}</b><br>Value: %{value:$,.2f}<br>Share: %{percentEntry:.1%}<extra></extra>",
        )
    )

    fig.update_layout(
        title={"text": title, "font": {"size": config.title_font_size}},
    )

    return apply_theme(fig, config)


def plot_trade_history(
    trades: pd.DataFrame | list[TradeRecord] | list[dict],
    time_column: str = "timestamp",
    pnl_column: str = "pnl",
    show_cumulative: bool = True,
    title: str = "Trade History",
    config: PlotConfig | None = None,
) -> go.Figure:
    """Plot trade history with PnL per trade.

    Args:
        trades: DataFrame or list with trade history
        time_column: Name of time column
        pnl_column: Name of PnL column
        show_cumulative: Whether to show cumulative PnL line
        title: Chart title
        config: Plot configuration

    Returns:
        Plotly figure with trade history
    """
    config = config or get_default_config()
    colors = config.colors

    # Convert to DataFrame
    if isinstance(trades, list):
        if not trades:
            return create_empty_figure("No trade data", config)
        if isinstance(trades[0], TradeRecord):
            trade_records = [t for t in trades if isinstance(t, TradeRecord)]
            df = pd.DataFrame(
                [
                    {
                        "timestamp": t.timestamp,
                        "trade_type": t.trade_type,
                        "asset": t.asset,
                        "amount": t.amount,
                        "value_usd": t.value_usd,
                        "pnl": t.pnl,
                    }
                    for t in trade_records
                ]
            )
        else:
            df = pd.DataFrame(trades)
    else:
        df = trades.copy()

    if df.empty:
        return create_empty_figure("No trade data", config)

    # Normalize column names
    time_col = time_column if time_column in df.columns else "timestamp"
    pnl_col = pnl_column if pnl_column in df.columns else "pnl"

    if time_col not in df.columns:
        return create_empty_figure("Invalid trade data format", config)

    # Ensure datetime
    if not pd.api.types.is_datetime64_any_dtype(df[time_col]):
        df[time_col] = pd.to_datetime(df[time_col])

    df = df.sort_values(time_col)

    # Create subplots if showing cumulative
    if show_cumulative and pnl_col in df.columns:
        fig = make_subplots(
            rows=2,
            cols=1,
            shared_xaxes=True,
            vertical_spacing=0.05,
            subplot_titles=("Trade PnL", "Cumulative PnL"),
            row_heights=[0.5, 0.5],
        )

        # Trade PnL bars
        bar_colors = [colors.profit if p >= 0 else colors.loss for p in df[pnl_col]]
        fig.add_trace(
            go.Bar(
                x=df[time_col],
                y=df[pnl_col],
                marker_color=bar_colors,
                name="Trade PnL",
            ),
            row=1,
            col=1,
        )

        # Cumulative line
        df["cumulative_pnl"] = df[pnl_col].cumsum()
        fig.add_trace(
            go.Scatter(
                x=df[time_col],
                y=df["cumulative_pnl"],
                mode="lines",
                name="Cumulative PnL",
                line={"color": colors.primary, "width": config.line_width},
            ),
            row=2,
            col=1,
        )

        # Zero lines
        fig.add_hline(y=0, line_dash="dash", line_color=colors.neutral, row=1, col=1)
        fig.add_hline(y=0, line_dash="dash", line_color=colors.neutral, row=2, col=1)

        fig.update_xaxes(title_text="Date", row=2, col=1)
        fig.update_yaxes(title_text="PnL (USD)", row=1, col=1)
        fig.update_yaxes(title_text="Cumulative PnL (USD)", row=2, col=1)

        fig.update_layout(
            height=config.height + 200,
        )
    else:
        # Simple bar chart
        fig = go.Figure()

        if pnl_col in df.columns:
            bar_colors = [colors.profit if p >= 0 else colors.loss for p in df[pnl_col]]
            fig.add_trace(
                go.Bar(
                    x=df[time_col],
                    y=df[pnl_col],
                    marker_color=bar_colors,
                    name="Trade PnL",
                )
            )
            fig.add_hline(y=0, line_dash="dash", line_color=colors.neutral)
        else:
            # Just show trade count over time
            fig.add_trace(
                go.Histogram(
                    x=df[time_col],
                    name="Trade Count",
                    marker_color=colors.primary,
                )
            )

        fig.update_layout(
            xaxis_title="Date",
            yaxis_title="PnL (USD)" if pnl_col in df.columns else "Trade Count",
        )

    fig.update_layout(
        title={"text": title, "font": {"size": config.title_font_size}},
    )

    return apply_theme(fig, config)


def plot_daily_pnl_heatmap(
    pnl_data: pd.DataFrame | list[dict],
    time_column: str = "timestamp",
    pnl_column: str = "pnl",
    title: str = "Daily PnL Heatmap",
    config: PlotConfig | None = None,
) -> go.Figure:
    """Plot daily PnL as a calendar heatmap.

    Args:
        pnl_data: DataFrame or list with daily PnL data
        time_column: Name of time column
        pnl_column: Name of PnL column
        title: Chart title
        config: Plot configuration

    Returns:
        Plotly figure with heatmap
    """
    config = config or get_default_config()
    colors = config.colors

    # Convert to DataFrame
    if isinstance(pnl_data, list):
        if not pnl_data:
            return create_empty_figure("No PnL data", config)
        df = pd.DataFrame(pnl_data)
    else:
        df = pnl_data.copy()

    if df.empty:
        return create_empty_figure("No PnL data", config)

    # Normalize column names
    time_col = time_column if time_column in df.columns else "timestamp"
    pnl_col = pnl_column if pnl_column in df.columns else "pnl"

    if time_col not in df.columns or pnl_col not in df.columns:
        return create_empty_figure("Invalid PnL data format", config)

    # Ensure datetime
    if not pd.api.types.is_datetime64_any_dtype(df[time_col]):
        df[time_col] = pd.to_datetime(df[time_col])

    # Aggregate to daily
    df["date"] = df[time_col].dt.date
    daily_pnl = df.groupby("date")[pnl_col].sum().reset_index()
    daily_pnl["date"] = pd.to_datetime(daily_pnl["date"])

    # Extract week and day of week
    daily_pnl["week"] = daily_pnl["date"].dt.isocalendar().week
    daily_pnl["year"] = daily_pnl["date"].dt.year
    daily_pnl["day_of_week"] = daily_pnl["date"].dt.dayofweek
    daily_pnl["week_label"] = daily_pnl["year"].astype(str) + "-W" + daily_pnl["week"].astype(str).str.zfill(2)

    # Create pivot table
    pivot = daily_pnl.pivot_table(
        values=pnl_col,
        index="day_of_week",
        columns="week_label",
        aggfunc="sum",
    )

    # Day names
    day_names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

    fig = go.Figure(
        go.Heatmap(
            z=pivot.values,
            x=pivot.columns,
            y=[day_names[i] for i in pivot.index],
            colorscale=[
                [0, colors.loss],
                [0.5, "white"],
                [1, colors.profit],
            ],
            zmid=0,
            text=[[format_usd(v) if pd.notna(v) else "" for v in row] for row in pivot.values],
            texttemplate="%{text}",
            hovertemplate="Week: %{x}<br>Day: %{y}<br>PnL: %{z:$,.2f}<extra></extra>",
        )
    )

    fig.update_layout(
        title={"text": title, "font": {"size": config.title_font_size}},
        xaxis_title="Week",
        yaxis_title="Day",
    )

    return apply_theme(fig, config)
