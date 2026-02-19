"""Lending protocol plots for DeFi strategy dashboards.

This module provides visualization components for lending strategies including:
- Health factor gauge with liquidation warning
- Loan-to-Value (LTV) ratio visualization
- Collateral breakdown by asset
- Borrow utilization charts
- Lending rates comparison across protocols

These plots are designed for lending protocols like Aave V3, Morpho Blue,
Compound V3, and Spark.

Example:
    from almanak.framework.dashboard.plots.lending_plots import (
        plot_health_factor_gauge,
        plot_ltv_ratio,
        plot_collateral_breakdown,
    )

    # Health factor gauge
    fig = plot_health_factor_gauge(
        health_factor=1.85,
        liquidation_threshold=1.0,
    )
    st.plotly_chart(fig)

    # Collateral breakdown
    fig = plot_collateral_breakdown(
        assets={"WETH": 10000, "USDC": 5000, "WBTC": 3000},
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
    format_usd,
    get_default_config,
    hex_to_rgba,
)


@dataclass
class LendingPosition:
    """Data for a lending position.

    Attributes:
        protocol: Protocol name (e.g., "Aave V3", "Morpho Blue")
        collateral_value_usd: Total collateral value in USD
        debt_value_usd: Total debt value in USD
        health_factor: Current health factor
        ltv: Current loan-to-value ratio
        max_ltv: Maximum allowed LTV
        liquidation_ltv: LTV threshold for liquidation
        supply_apy: Current supply APY
        borrow_apy: Current borrow APY
    """

    protocol: str
    collateral_value_usd: float
    debt_value_usd: float
    health_factor: float
    ltv: float
    max_ltv: float
    liquidation_ltv: float
    supply_apy: float | None = None
    borrow_apy: float | None = None


def plot_health_factor_gauge(
    health_factor: float,
    liquidation_threshold: float = 1.0,
    safe_threshold: float = 1.5,
    max_display: float = 5.0,
    title: str = "Health Factor",
    show_zones: bool = True,
    config: PlotConfig | None = None,
) -> go.Figure:
    """Plot a health factor gauge with liquidation warning zones.

    Creates a gauge visualization showing:
    - Current health factor value
    - Color-coded zones (critical, caution, healthy)
    - Liquidation threshold marker

    Args:
        health_factor: Current health factor value
        liquidation_threshold: Threshold below which liquidation occurs (default 1.0)
        safe_threshold: Threshold above which position is considered safe (default 1.5)
        max_display: Maximum value to display on gauge (default 5.0)
        title: Chart title
        show_zones: Whether to show colored threshold zones
        config: Plot configuration

    Returns:
        Plotly figure with health factor gauge
    """
    config = config or get_default_config()
    colors = config.colors

    # Clamp health factor for display
    display_value = min(health_factor, max_display)

    # Determine color based on health factor
    if health_factor < liquidation_threshold:
        bar_color = colors.critical
        status = "LIQUIDATION RISK"
    elif health_factor < safe_threshold:
        bar_color = colors.caution
        status = "CAUTION"
    else:
        bar_color = colors.healthy
        status = "HEALTHY"

    # Create gauge
    fig = go.Figure(
        go.Indicator(
            mode="gauge+number",
            value=display_value,
            number={"suffix": "" if health_factor <= max_display else "+", "font": {"size": 40}},
            title={"text": f"{title}<br><span style='font-size:14px;color:{bar_color}'>{status}</span>"},
            gauge={
                "axis": {"range": [0, max_display], "tickwidth": 1},
                "bar": {"color": bar_color, "thickness": 0.75},
                "bgcolor": "rgba(0,0,0,0)",
                "borderwidth": 0,
                "steps": [
                    {"range": [0, liquidation_threshold], "color": hex_to_rgba(colors.critical, 0.2)},  # Critical zone
                    {
                        "range": [liquidation_threshold, safe_threshold],
                        "color": hex_to_rgba(colors.caution, 0.2),
                    },  # Caution zone
                    {"range": [safe_threshold, max_display], "color": hex_to_rgba(colors.healthy, 0.2)},  # Healthy zone
                ]
                if show_zones
                else [],
                "threshold": {
                    "line": {"color": colors.danger, "width": 4},
                    "thickness": 0.75,
                    "value": liquidation_threshold,
                },
            },
        )
    )

    fig.update_layout(
        height=300,
        margin={"t": 80, "b": 20, "l": 30, "r": 30},
    )

    return apply_theme(fig, config)


def plot_ltv_ratio(
    current_ltv: float,
    max_ltv: float,
    liquidation_ltv: float,
    title: str = "Loan-to-Value Ratio",
    config: PlotConfig | None = None,
) -> go.Figure:
    """Plot LTV ratio with threshold markers.

    Creates a horizontal bar showing:
    - Current LTV position
    - Max LTV threshold
    - Liquidation LTV threshold
    - Remaining capacity

    Args:
        current_ltv: Current LTV ratio (0-1)
        max_ltv: Maximum allowed LTV (0-1)
        liquidation_ltv: Liquidation threshold LTV (0-1)
        title: Chart title
        config: Plot configuration

    Returns:
        Plotly figure with LTV visualization
    """
    config = config or get_default_config()
    colors = config.colors

    # Determine color based on LTV
    if current_ltv >= liquidation_ltv:
        bar_color = colors.critical
        status = "LIQUIDATION"
    elif current_ltv >= max_ltv:
        bar_color = colors.danger
        status = "ABOVE MAX"
    elif current_ltv >= max_ltv * 0.9:
        bar_color = colors.caution
        status = "NEAR MAX"
    else:
        bar_color = colors.healthy
        status = "SAFE"

    fig = go.Figure()

    # Background bar (full range)
    fig.add_trace(
        go.Bar(
            x=[1],
            y=["LTV"],
            orientation="h",
            marker_color=colors.neutral,
            opacity=0.2,
            showlegend=False,
            hoverinfo="skip",
        )
    )

    # Current LTV bar
    fig.add_trace(
        go.Bar(
            x=[current_ltv],
            y=["LTV"],
            orientation="h",
            marker_color=bar_color,
            name=f"Current: {current_ltv * 100:.1f}%",
            hovertemplate=f"Current LTV: {current_ltv * 100:.1f}%<extra></extra>",
        )
    )

    # Add threshold lines
    fig.add_vline(
        x=max_ltv,
        line_dash="dash",
        line_color=colors.warning,
        annotation_text=f"Max LTV ({max_ltv * 100:.0f}%)",
        annotation_position="top",
    )
    fig.add_vline(
        x=liquidation_ltv,
        line_dash="solid",
        line_color=colors.danger,
        annotation_text=f"Liquidation ({liquidation_ltv * 100:.0f}%)",
        annotation_position="bottom",
    )

    # Status annotation
    fig.add_annotation(
        x=current_ltv / 2,
        y="LTV",
        text=f"<b>{status}</b>",
        showarrow=False,
        font={"size": 14, "color": "white"},
    )

    fig.update_layout(
        title={"text": title, "font": {"size": config.title_font_size}},
        xaxis={"range": [0, 1], "tickformat": ".0%", "title": "LTV Ratio"},
        yaxis={"visible": False},
        height=200,
        margin={"t": 60, "b": 60, "l": 20, "r": 20},
        showlegend=True,
        legend={"orientation": "h", "y": -0.3},
    )

    return apply_theme(fig, config)


def plot_collateral_breakdown(
    assets: dict[str, float],
    title: str = "Collateral Breakdown",
    show_values: bool = True,
    value_format: str = "usd",
    config: PlotConfig | None = None,
) -> go.Figure:
    """Plot collateral breakdown by asset as a pie chart.

    Args:
        assets: Dictionary of asset symbol -> value in USD
        title: Chart title
        show_values: Whether to show value labels
        value_format: Format for values ("usd" or "pct")
        config: Plot configuration

    Returns:
        Plotly figure with collateral pie chart
    """
    config = config or get_default_config()
    colors = config.colors

    if not assets:
        return create_empty_figure("No collateral data", config)

    # Sort by value descending
    sorted_assets = dict(sorted(assets.items(), key=lambda x: x[1], reverse=True))

    labels = list(sorted_assets.keys())
    values = list(sorted_assets.values())
    total = sum(values)

    # Generate colors
    asset_colors = [
        colors.primary,
        colors.secondary,
        colors.accent,
        colors.success,
        colors.warning,
        colors.info,
    ]
    pie_colors = [asset_colors[i % len(asset_colors)] for i in range(len(labels))]

    # Format text
    if show_values:
        if value_format == "usd":
            text_info = "label+value"
            hover_template = "%{label}: %{value:$,.2f} (%{percent})<extra></extra>"
        else:
            text_info = "label+percent"
            hover_template = "%{label}: %{percent} (%{value:$,.2f})<extra></extra>"
    else:
        text_info = "label"
        hover_template = "%{label}: %{value:$,.2f} (%{percent})<extra></extra>"

    fig = go.Figure(
        go.Pie(
            labels=labels,
            values=values,
            textinfo=text_info,
            hovertemplate=hover_template,
            marker={"colors": pie_colors},
            hole=0.4,  # Donut chart
        )
    )

    # Add total in center
    fig.add_annotation(
        text=f"<b>Total</b><br>{format_usd(total)}",
        x=0.5,
        y=0.5,
        font={"size": 16},
        showarrow=False,
    )

    fig.update_layout(
        title={"text": title, "font": {"size": config.title_font_size}},
        height=400,
    )

    return apply_theme(fig, config)


def plot_borrow_utilization(
    borrowed: float,
    available: float,
    asset_symbol: str = "",
    title: str = "Borrow Utilization",
    config: PlotConfig | None = None,
) -> go.Figure:
    """Plot borrow utilization as a progress bar.

    Args:
        borrowed: Amount currently borrowed
        available: Amount available to borrow
        asset_symbol: Asset symbol for display
        title: Chart title
        config: Plot configuration

    Returns:
        Plotly figure with utilization bar
    """
    config = config or get_default_config()
    colors = config.colors

    total = borrowed + available
    if total == 0:
        return create_empty_figure("No borrow capacity", config)

    utilization = borrowed / total

    # Determine color
    if utilization >= 0.9:
        bar_color = colors.danger
    elif utilization >= 0.7:
        bar_color = colors.caution
    else:
        bar_color = colors.healthy

    fig = go.Figure()

    # Background bar
    fig.add_trace(
        go.Bar(
            x=[1],
            y=["Utilization"],
            orientation="h",
            marker_color=colors.neutral,
            opacity=0.2,
            showlegend=False,
            hoverinfo="skip",
        )
    )

    # Utilization bar
    fig.add_trace(
        go.Bar(
            x=[utilization],
            y=["Utilization"],
            orientation="h",
            marker_color=bar_color,
            name="Borrowed",
            hovertemplate=f"Borrowed: {format_usd(borrowed)}<br>"
            f"Available: {format_usd(available)}<br>"
            f"Utilization: {utilization * 100:.1f}%<extra></extra>",
        )
    )

    # Add labels
    asset_label = f" ({asset_symbol})" if asset_symbol else ""
    fig.add_annotation(
        x=utilization / 2,
        y="Utilization",
        text=f"<b>{utilization * 100:.1f}% Used</b>",
        showarrow=False,
        font={"size": 14, "color": "white"},
    )

    full_title = f"{title}{asset_label}"

    fig.update_layout(
        title={"text": full_title, "font": {"size": config.title_font_size}},
        xaxis={"range": [0, 1], "tickformat": ".0%"},
        yaxis={"visible": False},
        height=150,
        margin={"t": 60, "b": 20, "l": 20, "r": 20},
    )

    return apply_theme(fig, config)


def plot_lending_rates_comparison(
    protocols: list[str],
    supply_rates: list[float],
    borrow_rates: list[float],
    asset_symbol: str = "",
    title: str = "Lending Rates Comparison",
    config: PlotConfig | None = None,
) -> go.Figure:
    """Plot lending rates comparison across protocols.

    Creates a grouped bar chart comparing supply and borrow rates
    across different lending protocols.

    Args:
        protocols: List of protocol names
        supply_rates: List of supply APY values (as decimals, e.g., 0.05 = 5%)
        borrow_rates: List of borrow APY values (as decimals)
        asset_symbol: Asset symbol for display
        title: Chart title
        config: Plot configuration

    Returns:
        Plotly figure with rates comparison
    """
    config = config or get_default_config()
    colors = config.colors

    if not protocols:
        return create_empty_figure("No protocol data", config)

    # Validate that all lists have the same length
    if len(protocols) != len(supply_rates) or len(protocols) != len(borrow_rates):
        return create_empty_figure("Mismatched data lengths", config)

    fig = go.Figure()

    # Supply rates
    fig.add_trace(
        go.Bar(
            name="Supply APY",
            x=protocols,
            y=[r * 100 for r in supply_rates],  # Convert to percentage
            marker_color=colors.success,
            text=[f"{r * 100:.2f}%" for r in supply_rates],
            textposition="outside",
        )
    )

    # Borrow rates
    fig.add_trace(
        go.Bar(
            name="Borrow APY",
            x=protocols,
            y=[r * 100 for r in borrow_rates],  # Convert to percentage
            marker_color=colors.danger,
            text=[f"{r * 100:.2f}%" for r in borrow_rates],
            textposition="outside",
        )
    )

    asset_label = f" - {asset_symbol}" if asset_symbol else ""
    full_title = f"{title}{asset_label}"

    fig.update_layout(
        title={"text": full_title, "font": {"size": config.title_font_size}},
        xaxis_title="Protocol",
        yaxis_title="APY (%)",
        barmode="group",
        legend={"orientation": "h", "y": 1.1},
    )

    return apply_theme(fig, config)


def plot_lending_position_summary(
    position: LendingPosition,
    title: str = "Lending Position Summary",
    config: PlotConfig | None = None,
) -> go.Figure:
    """Plot a comprehensive lending position summary.

    Creates a multi-panel visualization showing:
    - Health factor gauge
    - LTV bar
    - Collateral vs debt comparison
    - APY information

    Args:
        position: LendingPosition data
        title: Chart title
        config: Plot configuration

    Returns:
        Plotly figure with position summary
    """
    config = config or get_default_config()
    colors = config.colors

    # Create subplots
    fig = make_subplots(
        rows=2,
        cols=2,
        specs=[
            [{"type": "indicator"}, {"type": "bar"}],
            [{"type": "bar", "colspan": 2}, None],
        ],
        subplot_titles=("Health Factor", "LTV Ratio", "Collateral vs Debt"),
        vertical_spacing=0.15,
        horizontal_spacing=0.1,
    )

    # Health factor gauge
    hf_color = (
        colors.healthy
        if position.health_factor >= 1.5
        else (colors.caution if position.health_factor >= 1.0 else colors.critical)
    )

    fig.add_trace(
        go.Indicator(
            mode="gauge+number",
            value=min(position.health_factor, 5),
            gauge={
                "axis": {"range": [0, 5]},
                "bar": {"color": hf_color},
                "threshold": {
                    "line": {"color": colors.danger, "width": 2},
                    "thickness": 0.75,
                    "value": 1.0,
                },
            },
        ),
        row=1,
        col=1,
    )

    # LTV bar
    fig.add_trace(
        go.Bar(
            x=[position.ltv * 100],
            y=["LTV"],
            orientation="h",
            marker_color=colors.primary,
            name="Current LTV",
            showlegend=False,
        ),
        row=1,
        col=2,
    )

    # Add max LTV line
    fig.add_vline(
        x=position.max_ltv * 100,
        line_dash="dash",
        line_color=colors.warning,
        row=1,
        col=2,
    )

    # Collateral vs Debt
    fig.add_trace(
        go.Bar(
            x=["Collateral", "Debt"],
            y=[position.collateral_value_usd, position.debt_value_usd],
            marker_color=[colors.success, colors.danger],
            text=[format_usd(position.collateral_value_usd), format_usd(position.debt_value_usd)],
            textposition="outside",
            showlegend=False,
        ),
        row=2,
        col=1,
    )

    fig.update_layout(
        title={"text": f"{title} - {position.protocol}", "font": {"size": config.title_font_size}},
        height=500,
    )

    # Update axes
    fig.update_xaxes(range=[0, 100], ticksuffix="%", row=1, col=2)

    return apply_theme(fig, config)


def plot_health_factor_history(
    health_factors: pd.DataFrame | list[dict],
    time_column: str = "timestamp",
    hf_column: str = "health_factor",
    liquidation_threshold: float = 1.0,
    safe_threshold: float = 1.5,
    title: str = "Health Factor History",
    config: PlotConfig | None = None,
) -> go.Figure:
    """Plot health factor over time with threshold zones.

    Args:
        health_factors: DataFrame or list with health factor history
            Expected columns: timestamp, health_factor
        time_column: Name of time column
        hf_column: Name of health factor column
        liquidation_threshold: Liquidation threshold (default 1.0)
        safe_threshold: Safe threshold (default 1.5)
        title: Chart title
        config: Plot configuration

    Returns:
        Plotly figure with health factor history
    """
    config = config or get_default_config()
    colors = config.colors

    # Convert to DataFrame if necessary
    if isinstance(health_factors, list):
        if not health_factors:
            return create_empty_figure("No health factor data", config)
        df = pd.DataFrame(health_factors)
    else:
        df = health_factors.copy()

    if df.empty:
        return create_empty_figure("No health factor data", config)

    # Normalize column names
    time_col = time_column if time_column in df.columns else "timestamp"
    hf_col = hf_column if hf_column in df.columns else "health_factor"

    if time_col not in df.columns or hf_col not in df.columns:
        return create_empty_figure("Invalid health factor data format", config)

    # Ensure datetime
    if not pd.api.types.is_datetime64_any_dtype(df[time_col]):
        df[time_col] = pd.to_datetime(df[time_col])

    df = df.sort_values(time_col)

    fig = go.Figure()

    # Add health factor line
    fig.add_trace(
        go.Scatter(
            x=df[time_col],
            y=df[hf_col],
            mode="lines",
            name="Health Factor",
            line={"color": colors.primary, "width": config.line_width},
        )
    )

    # Add threshold lines
    fig.add_hline(
        y=liquidation_threshold,
        line_dash="solid",
        line_color=colors.danger,
        annotation_text="Liquidation",
    )
    fig.add_hline(
        y=safe_threshold,
        line_dash="dash",
        line_color=colors.success,
        annotation_text="Safe",
    )

    # Add danger zone shading
    y_max = max(df[hf_col].max() * 1.1, safe_threshold + 1)
    fig.add_hrect(
        y0=0,
        y1=liquidation_threshold,
        fillcolor=colors.critical,
        opacity=0.1,
        annotation_text="Liquidation Zone",
    )
    fig.add_hrect(
        y0=liquidation_threshold,
        y1=safe_threshold,
        fillcolor=colors.caution,
        opacity=0.1,
    )

    fig.update_layout(
        title={"text": title, "font": {"size": config.title_font_size}},
        xaxis_title="Time",
        yaxis_title="Health Factor",
        yaxis={"range": [0, y_max]},
    )

    return apply_theme(fig, config)
