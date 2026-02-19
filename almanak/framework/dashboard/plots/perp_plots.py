"""Perpetual futures plots for derivatives strategy dashboards.

This module provides visualization components for perpetual trading strategies including:
- Position dashboard with entry, current, and liquidation prices
- Funding rate history
- Leverage gauge
- Liquidation level indicators

These plots are designed for perpetual protocols like GMX V2 and Hyperliquid.

Example:
    from almanak.framework.dashboard.plots.perp_plots import (
        plot_perp_position_dashboard,
        plot_funding_rate_history,
        plot_leverage_gauge,
    )

    # Position dashboard
    fig = plot_perp_position_dashboard(
        entry_price=2000,
        current_price=2100,
        liquidation_price=1600,
        is_long=True,
        size_usd=10000,
    )
    st.plotly_chart(fig)
"""

from dataclasses import dataclass

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from almanak.framework.dashboard.plots.base import (
    PlotConfig,
    apply_theme,
    create_empty_figure,
    format_price,
    format_usd,
    get_default_config,
    hex_to_rgba,
)


@dataclass
class PerpPosition:
    """Perpetual futures position data.

    Attributes:
        market: Market identifier (e.g., "ETH/USD")
        is_long: True for long, False for short
        entry_price: Price at position entry
        current_price: Current market price
        liquidation_price: Price at which liquidation occurs
        size_usd: Position size in USD
        collateral_usd: Collateral amount in USD
        leverage: Current leverage
        unrealized_pnl: Unrealized profit/loss in USD
        funding_paid: Total funding paid/received
    """

    market: str
    is_long: bool
    entry_price: float
    current_price: float
    liquidation_price: float
    size_usd: float
    collateral_usd: float
    leverage: float
    unrealized_pnl: float
    funding_paid: float | None = None


def plot_perp_position_dashboard(
    entry_price: float,
    current_price: float,
    liquidation_price: float,
    is_long: bool,
    size_usd: float,
    leverage: float = 1.0,
    market: str = "",
    collateral_usd: float | None = None,
    unrealized_pnl: float | None = None,
    title: str = "Position Overview",
    config: PlotConfig | None = None,
) -> go.Figure:
    """Plot a comprehensive perpetual position dashboard.

    Shows entry price, current price, liquidation price, and key metrics
    on a single visualization.

    Args:
        entry_price: Price at position entry
        current_price: Current market price
        liquidation_price: Price at which liquidation occurs
        is_long: True for long position, False for short
        size_usd: Position size in USD
        leverage: Current leverage
        market: Market identifier for display
        collateral_usd: Collateral amount in USD
        unrealized_pnl: Unrealized PnL in USD
        title: Chart title
        config: Plot configuration

    Returns:
        Plotly figure with position dashboard
    """
    config = config or get_default_config()
    colors = config.colors

    # Determine price range for visualization
    prices = [entry_price, current_price, liquidation_price]
    min_price = min(prices) * 0.95
    max_price = max(prices) * 1.05

    # Determine position status (guard against division by zero)
    if is_long:
        direction = "LONG"
        direction_color = colors.success
        pnl_pct = ((current_price - entry_price) / entry_price * leverage * 100) if entry_price != 0 else 0
        liq_distance = ((current_price - liquidation_price) / current_price * 100) if current_price != 0 else 0
    else:
        direction = "SHORT"
        direction_color = colors.danger
        pnl_pct = ((entry_price - current_price) / entry_price * leverage * 100) if entry_price != 0 else 0
        liq_distance = ((liquidation_price - current_price) / current_price * 100) if current_price != 0 else 0

    # Create figure
    fig = go.Figure()

    # Background gradient showing profit/loss zones
    if is_long:
        # Long: green above entry, red below
        fig.add_shape(
            type="rect",
            x0=0,
            x1=1,
            y0=entry_price,
            y1=max_price,
            xref="paper",
            fillcolor=colors.success,
            opacity=0.1,
            line_width=0,
        )
        fig.add_shape(
            type="rect",
            x0=0,
            x1=1,
            y0=liquidation_price,
            y1=entry_price,
            xref="paper",
            fillcolor=colors.danger,
            opacity=0.1,
            line_width=0,
        )
    else:
        # Short: green below entry, red above
        fig.add_shape(
            type="rect",
            x0=0,
            x1=1,
            y0=min_price,
            y1=entry_price,
            xref="paper",
            fillcolor=colors.success,
            opacity=0.1,
            line_width=0,
        )
        fig.add_shape(
            type="rect",
            x0=0,
            x1=1,
            y0=entry_price,
            y1=liquidation_price,
            xref="paper",
            fillcolor=colors.danger,
            opacity=0.1,
            line_width=0,
        )

    # Liquidation zone
    fig.add_shape(
        type="rect",
        x0=0,
        x1=1,
        y0=liquidation_price - (max_price - min_price) * 0.02 if is_long else liquidation_price,
        y1=liquidation_price if is_long else liquidation_price + (max_price - min_price) * 0.02,
        xref="paper",
        fillcolor=colors.critical,
        opacity=0.3,
        line_width=0,
    )

    # Entry price line
    fig.add_hline(
        y=entry_price,
        line_dash="dash",
        line_color=colors.primary,
        annotation_text=f"Entry: {format_price(entry_price)}",
        annotation_position="left",
    )

    # Current price line
    current_color = colors.success if pnl_pct >= 0 else colors.danger
    fig.add_hline(
        y=current_price,
        line_dash="solid",
        line_color=current_color,
        line_width=3,
        annotation_text=f"Current: {format_price(current_price)}",
        annotation_position="right",
    )

    # Liquidation price line
    fig.add_hline(
        y=liquidation_price,
        line_dash="dot",
        line_color=colors.critical,
        annotation_text=f"Liquidation: {format_price(liquidation_price)}",
        annotation_position="left" if is_long else "right",
    )

    # Add position marker
    fig.add_trace(
        go.Scatter(
            x=[0.5],
            y=[current_price],
            mode="markers",
            marker={
                "symbol": "diamond",
                "size": 20,
                "color": current_color,
                "line": {"width": 2, "color": "white"},
            },
            name="Current Position",
            showlegend=False,
        )
    )

    # Build title with market info
    full_title = title
    if market:
        full_title = f"{title} - {market}"
    full_title += f"<br><span style='font-size:14px;color:{direction_color}'>{direction} {leverage:.1f}x</span>"

    # Add metrics annotation
    metrics_text = (
        f"<b>Size:</b> {format_usd(size_usd)}<br><b>Leverage:</b> {leverage:.1f}x<br><b>PnL:</b> {pnl_pct:+.2f}%"
    )
    if unrealized_pnl is not None:
        metrics_text += f" ({format_usd(unrealized_pnl)})"
    metrics_text += f"<br><b>Liq. Distance:</b> {liq_distance:.1f}%"

    fig.add_annotation(
        x=0.02,
        y=0.98,
        xref="paper",
        yref="paper",
        text=metrics_text,
        showarrow=False,
        font={"size": 12},
        align="left",
        bgcolor="rgba(0,0,0,0.5)",
        borderpad=10,
    )

    fig.update_layout(
        title={"text": full_title, "font": {"size": config.title_font_size}},
        xaxis={"visible": False},
        yaxis={
            "range": [min_price, max_price],
            "title": "Price",
            "side": "right",
        },
        height=400,
    )

    return apply_theme(fig, config)


def plot_funding_rate_history(
    funding_data: pd.DataFrame | list[dict],
    time_column: str = "timestamp",
    rate_column: str = "funding_rate",
    show_cumulative: bool = True,
    title: str = "Funding Rate History",
    config: PlotConfig | None = None,
) -> go.Figure:
    """Plot funding rate history over time.

    Args:
        funding_data: DataFrame or list with funding rate history
            Expected columns: timestamp, funding_rate
        time_column: Name of time column
        rate_column: Name of funding rate column
        show_cumulative: Whether to show cumulative funding
        title: Chart title
        config: Plot configuration

    Returns:
        Plotly figure with funding rate history
    """
    config = config or get_default_config()
    colors = config.colors

    # Convert to DataFrame
    if isinstance(funding_data, list):
        if not funding_data:
            return create_empty_figure("No funding data", config)
        df = pd.DataFrame(funding_data)
    else:
        df = funding_data.copy()

    if df.empty:
        return create_empty_figure("No funding data", config)

    # Normalize column names
    time_col = time_column if time_column in df.columns else "timestamp"
    rate_col = rate_column if rate_column in df.columns else "funding_rate"

    if time_col not in df.columns or rate_col not in df.columns:
        return create_empty_figure("Invalid funding data format", config)

    # Ensure datetime
    if not pd.api.types.is_datetime64_any_dtype(df[time_col]):
        df[time_col] = pd.to_datetime(df[time_col])

    df = df.sort_values(time_col)

    # Create figure with subplots if showing cumulative
    if show_cumulative:
        fig = make_subplots(
            rows=2,
            cols=1,
            shared_xaxes=True,
            vertical_spacing=0.05,
            subplot_titles=("Funding Rate", "Cumulative Funding"),
            row_heights=[0.5, 0.5],
        )

        # Funding rate bars
        bar_colors = [colors.success if r >= 0 else colors.danger for r in df[rate_col]]
        fig.add_trace(
            go.Bar(
                x=df[time_col],
                y=df[rate_col] * 100,  # Convert to percentage
                marker_color=bar_colors,
                name="Funding Rate",
            ),
            row=1,
            col=1,
        )

        # Cumulative funding
        df["cumulative"] = df[rate_col].cumsum() * 100
        fig.add_trace(
            go.Scatter(
                x=df[time_col],
                y=df["cumulative"],
                mode="lines",
                name="Cumulative",
                line={"color": colors.primary, "width": config.line_width},
            ),
            row=2,
            col=1,
        )

        # Zero lines
        fig.add_hline(y=0, line_dash="dash", line_color=colors.neutral, row=1, col=1)
        fig.add_hline(y=0, line_dash="dash", line_color=colors.neutral, row=2, col=1)

        fig.update_yaxes(title_text="Rate (%)", row=1, col=1)
        fig.update_yaxes(title_text="Cumulative (%)", row=2, col=1)
        fig.update_xaxes(title_text="Time", row=2, col=1)

        fig.update_layout(height=config.height + 200)
    else:
        fig = go.Figure()

        # Funding rate bars
        bar_colors = [colors.success if r >= 0 else colors.danger for r in df[rate_col]]
        fig.add_trace(
            go.Bar(
                x=df[time_col],
                y=df[rate_col] * 100,
                marker_color=bar_colors,
                name="Funding Rate",
            )
        )

        fig.add_hline(y=0, line_dash="dash", line_color=colors.neutral)

        fig.update_layout(
            xaxis_title="Time",
            yaxis_title="Funding Rate (%)",
        )

    fig.update_layout(
        title={"text": title, "font": {"size": config.title_font_size}},
    )

    return apply_theme(fig, config)


def plot_liquidation_levels(
    positions: list[PerpPosition] | list[dict],
    current_price: float,
    title: str = "Liquidation Levels",
    config: PlotConfig | None = None,
) -> go.Figure:
    """Plot liquidation levels for multiple positions.

    Args:
        positions: List of positions with liquidation prices
        current_price: Current market price
        title: Chart title
        config: Plot configuration

    Returns:
        Plotly figure with liquidation levels
    """
    config = config or get_default_config()
    colors = config.colors

    if not positions:
        return create_empty_figure("No position data", config)

    # Convert dicts to PerpPosition if needed
    processed = []
    for pos in positions:
        if isinstance(pos, dict):
            processed.append(
                PerpPosition(
                    market=pos.get("market", ""),
                    is_long=pos.get("is_long", True),
                    entry_price=pos.get("entry_price", 0),
                    current_price=pos.get("current_price", current_price),
                    liquidation_price=pos.get("liquidation_price", 0),
                    size_usd=pos.get("size_usd", 0),
                    collateral_usd=pos.get("collateral_usd", 0),
                    leverage=pos.get("leverage", 1),
                    unrealized_pnl=pos.get("unrealized_pnl", 0),
                )
            )
        else:
            processed.append(pos)

    fig = go.Figure()

    # Determine y-axis range
    all_prices = [current_price] + [p.liquidation_price for p in processed] + [p.entry_price for p in processed]
    y_min = min(all_prices) * 0.9
    y_max = max(all_prices) * 1.1

    # Current price line
    fig.add_hline(
        y=current_price,
        line_dash="solid",
        line_color=colors.primary,
        line_width=2,
        annotation_text=f"Current: {format_price(current_price)}",
    )

    # Add each position
    for i, pos in enumerate(processed):
        x_pos = i + 1
        liq_color = colors.danger if pos.is_long else colors.success
        direction = "L" if pos.is_long else "S"

        # Liquidation marker
        fig.add_trace(
            go.Scatter(
                x=[x_pos],
                y=[pos.liquidation_price],
                mode="markers+text",
                marker={
                    "symbol": "x",
                    "size": 15,
                    "color": colors.critical,
                    "line": {"width": 2},
                },
                text=[f"{direction}: {format_price(pos.liquidation_price)}"],
                textposition="middle right",
                name=f"Liq {pos.market}",
                showlegend=False,
            )
        )

        # Entry marker
        fig.add_trace(
            go.Scatter(
                x=[x_pos],
                y=[pos.entry_price],
                mode="markers",
                marker={
                    "symbol": "circle",
                    "size": 10,
                    "color": colors.primary,
                },
                name=f"Entry {pos.market}",
                showlegend=False,
            )
        )

        # Line connecting entry to liquidation
        fig.add_trace(
            go.Scatter(
                x=[x_pos, x_pos],
                y=[pos.entry_price, pos.liquidation_price],
                mode="lines",
                line={"color": liq_color, "width": 2, "dash": "dot"},
                showlegend=False,
            )
        )

    # X-axis labels
    fig.update_xaxes(
        tickmode="array",
        tickvals=list(range(1, len(processed) + 1)),
        ticktext=[f"{p.market}<br>{format_usd(p.size_usd)}" for p in processed],
    )

    fig.update_layout(
        title={"text": title, "font": {"size": config.title_font_size}},
        yaxis_title="Price",
        yaxis={"range": [y_min, y_max]},
        xaxis_title="Positions",
    )

    return apply_theme(fig, config)


def plot_leverage_gauge(
    current_leverage: float,
    max_leverage: float,
    safe_leverage: float | None = None,
    title: str = "Leverage",
    config: PlotConfig | None = None,
) -> go.Figure:
    """Plot a leverage gauge.

    Args:
        current_leverage: Current leverage value
        max_leverage: Maximum allowed leverage
        safe_leverage: Recommended safe leverage threshold
        title: Chart title
        config: Plot configuration

    Returns:
        Plotly figure with leverage gauge
    """
    config = config or get_default_config()
    colors = config.colors

    if safe_leverage is None:
        safe_leverage = max_leverage * 0.5

    # Determine color based on leverage
    if current_leverage >= max_leverage * 0.9:
        bar_color = colors.critical
        status = "MAX"
    elif current_leverage >= safe_leverage:
        bar_color = colors.caution
        status = "HIGH"
    else:
        bar_color = colors.healthy
        status = "SAFE"

    fig = go.Figure(
        go.Indicator(
            mode="gauge+number+delta",
            value=current_leverage,
            number={"suffix": "x", "font": {"size": 36}},
            delta={"reference": safe_leverage, "relative": False, "suffix": "x"},
            title={"text": f"{title}<br><span style='font-size:14px;color:{bar_color}'>{status}</span>"},
            gauge={
                "axis": {"range": [0, max_leverage], "ticksuffix": "x"},
                "bar": {"color": bar_color, "thickness": 0.75},
                "steps": [
                    {"range": [0, safe_leverage], "color": hex_to_rgba(colors.healthy, 0.2)},  # Safe zone
                    {
                        "range": [safe_leverage, max_leverage * 0.9],
                        "color": hex_to_rgba(colors.caution, 0.2),
                    },  # Caution zone
                    {
                        "range": [max_leverage * 0.9, max_leverage],
                        "color": hex_to_rgba(colors.critical, 0.2),
                    },  # Critical zone
                ],
                "threshold": {
                    "line": {"color": colors.danger, "width": 4},
                    "thickness": 0.75,
                    "value": max_leverage,
                },
            },
        )
    )

    fig.update_layout(
        height=250,
        margin={"t": 80, "b": 20, "l": 30, "r": 30},
    )

    return apply_theme(fig, config)


def plot_pnl_by_market(
    pnl_data: dict[str, float],
    title: str = "PnL by Market",
    config: PlotConfig | None = None,
) -> go.Figure:
    """Plot PnL breakdown by market.

    Args:
        pnl_data: Dictionary of market -> PnL value
        title: Chart title
        config: Plot configuration

    Returns:
        Plotly figure with PnL by market
    """
    config = config or get_default_config()
    colors = config.colors

    if not pnl_data:
        return create_empty_figure("No PnL data", config)

    # Sort by absolute PnL
    sorted_data = dict(sorted(pnl_data.items(), key=lambda x: abs(x[1]), reverse=True))

    markets = list(sorted_data.keys())
    pnls = list(sorted_data.values())

    bar_colors = [colors.profit if p >= 0 else colors.loss for p in pnls]

    fig = go.Figure(
        go.Bar(
            x=markets,
            y=pnls,
            marker_color=bar_colors,
            text=[format_usd(p) for p in pnls],
            textposition="outside",
        )
    )

    fig.add_hline(y=0, line_dash="dash", line_color=colors.neutral)

    fig.update_layout(
        title={"text": title, "font": {"size": config.title_font_size}},
        xaxis_title="Market",
        yaxis_title="PnL (USD)",
    )

    return apply_theme(fig, config)
