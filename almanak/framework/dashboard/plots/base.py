"""Base utilities and configuration for dashboard plots.

This module provides shared configuration, theming, and utility functions
used across all plot types.
"""

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any

import plotly.graph_objects as go

# Default font sizes
PLOT_TITLE_FONT_SIZE = 18
PLOT_AXIS_FONT_SIZE = 12
PLOT_LEGEND_FONT_SIZE = 10


class ChartTheme(Enum):
    """Available chart themes."""

    DARK = "plotly_dark"
    LIGHT = "plotly_white"
    ALMANAK = "almanak"  # Custom Almanak theme


@dataclass
class PlotColors:
    """Color palette for plots.

    Provides consistent colors across all visualizations.
    """

    # Primary colors
    primary: str = "#3498DB"  # Blue
    secondary: str = "#9B59B6"  # Purple
    accent: str = "#1ABC9C"  # Teal

    # Semantic colors
    success: str = "#27AE60"  # Green
    warning: str = "#F39C12"  # Orange
    danger: str = "#E74C3C"  # Red
    info: str = "#3498DB"  # Blue

    # Position colors
    in_range: str = "#3498DB"  # Blue - position in range
    out_of_range: str = "#283AFF"  # Dark blue - out of range
    active_tick: str = "#F39C12"  # Orange - current market tick
    position_fill: str = "#27AE60"  # Green - position rectangle

    # Trading colors
    buy: str = "#27AE60"  # Green
    sell: str = "#E74C3C"  # Red
    profit: str = "#27AE60"  # Green
    loss: str = "#E74C3C"  # Red

    # Neutral colors
    neutral: str = "#7F8C8D"  # Gray
    grid: str = "#95A5A6"  # Light gray
    background: str = "rgba(0,0,0,0)"  # Transparent

    # Lending colors
    healthy: str = "#27AE60"  # Green - healthy health factor
    caution: str = "#F39C12"  # Orange - caution zone
    critical: str = "#E74C3C"  # Red - critical/liquidation risk


@dataclass
class PlotConfig:
    """Configuration for plot styling and behavior.

    Attributes:
        theme: Chart theme (dark, light, or almanak)
        colors: Color palette to use
        title_font_size: Font size for chart titles
        axis_font_size: Font size for axis labels
        legend_font_size: Font size for legend text
        height: Default chart height in pixels
        width: Default chart width in pixels (None = auto)
        show_grid: Whether to show grid lines
        show_legend: Whether to show legend
        interactive: Whether to enable interactive features (zoom, pan)
        line_width: Default line width for traces
        marker_size: Default marker size
    """

    theme: ChartTheme = ChartTheme.DARK
    colors: PlotColors = field(default_factory=PlotColors)
    title_font_size: int = PLOT_TITLE_FONT_SIZE
    axis_font_size: int = PLOT_AXIS_FONT_SIZE
    legend_font_size: int = PLOT_LEGEND_FONT_SIZE
    height: int = 500
    width: int | None = None
    show_grid: bool = True
    show_legend: bool = True
    interactive: bool = True
    line_width: float = 2.0
    marker_size: int = 10


@dataclass
class PlotResult:
    """Result of a plot generation operation.

    Attributes:
        figure: The generated Plotly figure
        success: Whether the plot was generated successfully
        error: Error message if generation failed
        metadata: Additional metadata about the plot
    """

    figure: go.Figure | None
    success: bool
    error: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


def get_default_config() -> PlotConfig:
    """Get the default plot configuration."""
    return PlotConfig()


def apply_theme(fig: go.Figure, config: PlotConfig) -> go.Figure:
    """Apply theme and styling to a Plotly figure.

    Args:
        fig: The Plotly figure to style
        config: Plot configuration

    Returns:
        The styled figure
    """
    template = config.theme.value if config.theme != ChartTheme.ALMANAK else "plotly_dark"

    layout_updates = {
        "template": template,
        "plot_bgcolor": config.colors.background,
        "paper_bgcolor": config.colors.background,
        "font": {"size": config.axis_font_size},
        "title": {"font": {"size": config.title_font_size}},
        "showlegend": config.show_legend,
        "hovermode": "x unified" if config.interactive else "closest",
    }

    if config.height:
        layout_updates["height"] = config.height
    if config.width:
        layout_updates["width"] = config.width

    fig.update_layout(**layout_updates)

    # Update axes
    fig.update_xaxes(showgrid=config.show_grid)
    fig.update_yaxes(showgrid=config.show_grid)

    return fig


def format_price(price: float | Decimal, decimals: int = 4) -> str:
    """Format a price value for display.

    Args:
        price: The price value
        decimals: Number of decimal places

    Returns:
        Formatted price string
    """
    if isinstance(price, Decimal):
        price = float(price)
    if abs(price) < 0.0001:
        return f"{price:.8f}"
    elif abs(price) < 1:
        return f"{price:.6f}"
    elif abs(price) < 1000:
        return f"{price:.{decimals}f}"
    else:
        return f"{price:,.2f}"


def format_datetime(dt: datetime | None) -> str:
    """Format a datetime for display.

    Args:
        dt: The datetime object

    Returns:
        Formatted datetime string
    """
    if dt is None:
        return "N/A"
    return dt.strftime("%Y-%m-%d %H:%M")


def format_percentage(value: float, decimals: int = 2) -> str:
    """Format a percentage value for display.

    Args:
        value: The percentage value (0.05 = 5%)
        decimals: Number of decimal places

    Returns:
        Formatted percentage string
    """
    return f"{value * 100:.{decimals}f}%"


def format_usd(value: float | Decimal) -> str:
    """Format a USD value for display.

    Args:
        value: The USD value

    Returns:
        Formatted USD string
    """
    if isinstance(value, Decimal):
        value = float(value)
    if abs(value) >= 1_000_000:
        return f"${value / 1_000_000:.2f}M"
    elif abs(value) >= 1_000:
        return f"${value / 1_000:.2f}K"
    else:
        return f"${value:.2f}"


def create_empty_figure(message: str = "No data available", config: PlotConfig | None = None) -> go.Figure:
    """Create an empty figure with a message.

    Args:
        message: Message to display
        config: Plot configuration

    Returns:
        Empty Plotly figure with message
    """
    config = config or get_default_config()
    fig = go.Figure()

    fig.add_annotation(
        text=message,
        xref="paper",
        yref="paper",
        x=0.5,
        y=0.5,
        showarrow=False,
        font={"size": 16, "color": config.colors.neutral},
    )

    fig.update_layout(
        xaxis={"visible": False},
        yaxis={"visible": False},
    )

    return apply_theme(fig, config)


def hex_to_rgba(hex_color: str, alpha: float = 0.2) -> str:
    """Convert hex color to rgba string for Plotly compatibility.

    Plotly doesn't support hex colors with alpha channel (#RRGGBBAA format),
    so this converts hex colors to rgba() format.

    Args:
        hex_color: Hex color string (e.g., "#E74C3C" or "E74C3C")
        alpha: Alpha value between 0 and 1 (default: 0.2)

    Returns:
        rgba() color string (e.g., "rgba(231, 76, 60, 0.2)")
    """
    hex_color = hex_color.lstrip("#")
    if len(hex_color) == 6:
        r = int(hex_color[0:2], 16)
        g = int(hex_color[2:4], 16)
        b = int(hex_color[4:6], 16)
        return f"rgba({r}, {g}, {b}, {alpha})"
    else:
        # Fallback for invalid hex
        return f"rgba(128, 128, 128, {alpha})"
