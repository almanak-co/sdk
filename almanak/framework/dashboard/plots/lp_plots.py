"""Liquidity Provider (LP) plots for DEX strategy dashboards.

This module provides visualization components for LP strategies including:
- Liquidity distribution across price ticks
- Position lifecycle visualization over time
- Impermanent loss tracking
- Fee accumulation charts
- Position range status indicators

These plots are designed for concentrated liquidity protocols like
Uniswap V3, PancakeSwap V3, TraderJoe V2, and Aerodrome.

Example:
    from almanak.framework.dashboard.plots.lp_plots import (
        plot_liquidity_distribution,
        plot_positions_over_time,
    )

    # Liquidity distribution
    fig = plot_liquidity_distribution(
        tick_data=tick_df,
        current_tick=pool.active_tick,
        position_bounds=(lower_tick, upper_tick),
        token_pair="ETH/USDC",
    )
    st.plotly_chart(fig)

    # Position history
    fig = plot_positions_over_time(
        positions=position_history,
        price_data=price_df,
        invert_prices=False,
    )
    st.plotly_chart(fig)
"""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

import pandas as pd
import plotly.graph_objects as go

from almanak.framework.dashboard.plots.base import (
    PlotConfig,
    apply_theme,
    create_empty_figure,
    format_datetime,
    format_price,
    get_default_config,
)


@dataclass
class TickData:
    """Processed tick data for liquidity distribution.

    Attributes:
        tick_idx: The tick index
        liquidity_active: Active liquidity at this tick
        price0: Price in terms of token0
        price1: Price in terms of token1 (inverse)
    """

    tick_idx: int
    liquidity_active: Decimal
    price0: float
    price1: float


@dataclass
class PositionData:
    """Position data for visualization.

    Attributes:
        position_id: Unique identifier for the position
        date_start: When the position was opened
        date_end: When the position was closed (None if still open)
        bound_tick_lower: Lower tick bound
        bound_tick_upper: Upper tick bound
        bound_price_lower: Lower price bound
        bound_price_upper: Upper price bound
        token0_amount: Amount of token0 deposited
        token1_amount: Amount of token1 deposited
        fees_collected: Total fees collected
        is_active: Whether the position is currently active
    """

    position_id: str = ""
    date_start: datetime | None = None
    date_end: datetime | None = None
    bound_tick_lower: int = 0
    bound_tick_upper: int = 0
    bound_price_lower: float = 0.0
    bound_price_upper: float = 0.0
    token0_amount: Decimal | None = None
    token1_amount: Decimal | None = None
    fees_collected: Decimal | None = None
    is_active: bool = True


def plot_liquidity_distribution(
    tick_data: pd.DataFrame | list[TickData],
    current_tick: int,
    position_bounds: tuple[int, int] | None = None,
    token_pair: str = "",
    fee_tier: str = "",
    invert_prices: bool = False,
    auto_zoom: bool = True,
    zoom_threshold: float = 0.05,
    simple: bool = False,
    config: PlotConfig | None = None,
) -> go.Figure:
    """Plot liquidity distribution across price ticks.

    Creates a bar chart showing liquidity at different price levels,
    with color coding for:
    - Active/current tick (orange)
    - Ticks within position bounds (blue)
    - Ticks outside position bounds (dark blue)

    Args:
        tick_data: DataFrame or list with tick liquidity data.
            Expected columns: tick_idx, liquidity_active, price0, price1
        current_tick: The current active tick in the pool
        position_bounds: Tuple of (lower_tick, upper_tick) for position highlighting
        token_pair: Token pair name for display (e.g., "ETH/USDC")
        fee_tier: Fee tier string for display (e.g., "0.30%")
        invert_prices: If True, show prices in terms of token1
        auto_zoom: If True, filter out low-liquidity ticks for better visualization
        zoom_threshold: Threshold for auto-zoom (0.05 = 5% of max liquidity)
        simple: If True, hide axes and labels for compact display
        config: Plot configuration

    Returns:
        Plotly figure with the liquidity distribution bar chart
    """
    config = config or get_default_config()
    colors = config.colors

    # Convert to DataFrame if necessary
    if isinstance(tick_data, list):
        if not tick_data:
            return create_empty_figure("No tick data available", config)
        df = pd.DataFrame(
            [
                {
                    "tick_idx": t.tick_idx,
                    "liquidity_active": float(t.liquidity_active),
                    "price0": t.price0,
                    "price1": t.price1,
                }
                for t in tick_data
            ]
        )
    else:
        df = tick_data.copy()

    if df.empty:
        return create_empty_figure("No tick data available", config)

    # Ensure liquidity column is numeric
    df["liquidity_active"] = pd.to_numeric(df["liquidity_active"], errors="coerce")

    # Select price column based on inversion
    price_col = "price1" if invert_prices else "price0"

    # Auto-zoom: filter low-liquidity ticks
    if auto_zoom:
        max_liquidity = df["liquidity_active"].max()
        threshold = zoom_threshold * max_liquidity
        df = df[df["liquidity_active"] >= threshold].copy()

    if df.empty:
        return create_empty_figure("No significant liquidity in range", config)

    # Convert price to string for categorical x-axis
    df[price_col] = df[price_col].astype(float).apply(lambda x: format_price(x, 4))

    # Determine bar colors based on position and active tick
    def get_bar_color(tick_idx: int) -> str:
        if tick_idx == current_tick:
            return colors.active_tick  # Orange for active tick
        if position_bounds is not None:
            lower, upper = position_bounds
            if lower <= tick_idx <= upper:
                return colors.in_range  # Blue for in-range
        return colors.out_of_range  # Dark blue for out-of-range

    bar_colors = [get_bar_color(idx) for idx in df["tick_idx"]]

    # Create figure
    fig = go.Figure()

    # Main liquidity bars
    hover_text = df.apply(
        lambda row: (
            f"Price (token0): {row['price0']}<br>"
            f"Price (token1): {row['price1']}<br>"
            f"Tick: {row['tick_idx']}<br>"
            f"Liquidity: {row['liquidity_active']:,.0f}"
        ),
        axis=1,
    )

    fig.add_trace(
        go.Bar(
            x=df[price_col],
            y=df["liquidity_active"],
            marker_color=bar_colors,
            hovertext=hover_text,
            hoverinfo="text",
            name=f"{token_pair} {fee_tier}".strip(),
        )
    )

    # Add legend entries for colors
    if not simple:
        fig.add_trace(
            go.Bar(
                x=[None],
                y=[None],
                marker_color=colors.in_range,
                name="Position Range",
                showlegend=True,
            )
        )
        fig.add_trace(
            go.Bar(
                x=[None],
                y=[None],
                marker_color=colors.active_tick,
                name="Current Price",
                showlegend=True,
            )
        )

    # Title
    title = "Liquidity Distribution"
    if token_pair:
        title = f"{title} - {token_pair}"
    if fee_tier:
        title = f"{title} ({fee_tier})"

    # Layout
    fig.update_layout(
        title={"text": title, "font": {"size": config.title_font_size}},
        showlegend=not simple,
        bargap=0.1,
    )

    # Axis configuration
    if simple:
        fig.update_xaxes(showgrid=False, showticklabels=False, title="")
        fig.update_yaxes(showgrid=False, showticklabels=False, title="")
    else:
        fig.update_xaxes(title="Price", showgrid=False)
        fig.update_yaxes(title="Total Value Locked (TVL)", autorange=True)

    return apply_theme(fig, config)


def plot_positions_over_time(
    positions: list[PositionData] | list[dict],
    price_data: pd.DataFrame,
    price_column: str = "price",
    time_column: str = "timestamp",
    invert_prices: bool = False,
    show_price_bounds: bool = True,
    price_bounds_ratio: float | None = None,
    title: str = "Positions Over Time",
    config: PlotConfig | None = None,
) -> go.Figure:
    """Plot position history over time with price movement.

    Creates a time series chart showing:
    - Price line (or price model)
    - Rectangular overlays for each position's price range and duration
    - Optional price bounds lines within positions

    Args:
        positions: List of position data (PositionData objects or dicts)
        price_data: DataFrame with price history
            Expected columns: timestamp/time, price/close
        price_column: Name of the price column in price_data
        time_column: Name of the time column in price_data
        invert_prices: If True, show inverted prices (1/price)
        show_price_bounds: If True, show position rectangles
        price_bounds_ratio: If set, draw dashed lines at this ratio within position bounds
        title: Chart title
        config: Plot configuration

    Returns:
        Plotly figure with positions over time
    """
    config = config or get_default_config()
    colors = config.colors

    if price_data.empty:
        return create_empty_figure("No price data available", config)

    # Normalize price data
    df = price_data.copy()

    # Handle various column names
    time_candidates = [time_column, "timestamp", "time", "Timestamp", "Time", "date", "Date"]
    price_candidates = [price_column, "price", "close", "Price", "Close", "model"]

    time_col = None
    for col in time_candidates:
        if col in df.columns:
            time_col = col
            break

    price_col = None
    for col in price_candidates:
        if col in df.columns:
            price_col = col
            break

    if time_col is None or price_col is None:
        return create_empty_figure("Invalid price data format", config)

    # Ensure datetime type
    if not pd.api.types.is_datetime64_any_dtype(df[time_col]):
        df[time_col] = pd.to_datetime(df[time_col])

    df = df.sort_values(time_col)

    # Invert prices if requested
    if invert_prices:
        df[price_col] = 1.0 / df[price_col]

    # Create figure
    fig = go.Figure()

    # Add price line
    fig.add_trace(
        go.Scatter(
            x=df[time_col],
            y=df[price_col],
            mode="lines",
            name="Price",
            line={"color": colors.primary, "width": config.line_width},
        )
    )

    # Determine chart bounds for clipping positions
    # Normalize tz-awareness: strip tzinfo from timestamps so comparisons with
    # tz-naive position dates (date_start/date_end) don't raise TypeError.
    ts = df[time_col]
    if hasattr(ts.dt, "tz") and ts.dt.tz is not None:
        ts = ts.dt.tz_convert("UTC").dt.tz_localize(None)
    x_min = ts.min()
    x_max = ts.max()

    # Process positions
    if positions:
        # Convert dicts to PositionData if necessary
        processed_positions = []
        for pos in positions:
            if isinstance(pos, dict):
                raw_start = pos.get("date_start", pos.get("dateStart"))
                if raw_start is None:
                    continue
                processed_positions.append(
                    PositionData(
                        position_id=str(pos.get("position_id", pos.get("id", ""))),
                        date_start=raw_start,
                        date_end=pos.get("date_end", pos.get("dateEnd")),
                        bound_tick_lower=pos.get("bound_tick_lower", 0),
                        bound_tick_upper=pos.get("bound_tick_upper", 0),
                        bound_price_lower=pos.get("bound_price_lower", 0),
                        bound_price_upper=pos.get("bound_price_upper", 0),
                        is_active=pos.get("is_active", pos.get("date_end") is None),
                    )
                )
            else:
                processed_positions.append(pos)

        # Add legend entry for positions
        fig.add_trace(
            go.Scatter(
                x=[None],
                y=[None],
                mode="lines",
                name="Position Range",
                line={"color": colors.position_fill},
            )
        )

        for pos in processed_positions:
            # Get position bounds
            date_start = pos.date_start
            date_end = pos.date_end or x_max

            # Skip positions without a start date
            if date_start is None:
                continue

            # Clip to chart bounds
            if isinstance(date_start, str):
                date_start = pd.to_datetime(date_start)
            if isinstance(date_end, str):
                date_end = pd.to_datetime(date_end)

            # Normalize tz-awareness: strip tzinfo so naive/aware datetimes can be compared
            if hasattr(date_start, "tzinfo") and date_start.tzinfo is not None:
                date_start = date_start.replace(tzinfo=None)
            if hasattr(date_end, "tzinfo") and date_end.tzinfo is not None:
                date_end = date_end.replace(tzinfo=None)

            if date_end < x_min or date_start > x_max:
                continue  # Position outside chart range

            date_start = max(date_start, x_min)
            date_end = min(date_end, x_max)

            # Get price bounds
            lower_price = pos.bound_price_lower
            upper_price = pos.bound_price_upper

            if invert_prices and lower_price and upper_price:
                # When inverting: lower becomes 1/upper, upper becomes 1/lower
                lower_price, upper_price = 1.0 / upper_price, 1.0 / lower_price

            if not lower_price or not upper_price:
                continue

            # Add position rectangle
            if show_price_bounds:
                fig.add_shape(
                    type="rect",
                    x0=date_start,
                    x1=date_end,
                    y0=lower_price,
                    y1=upper_price,
                    fillcolor=colors.position_fill,
                    opacity=0.3,
                    line_width=0,
                )

                # Add price bounds lines if configured
                if price_bounds_ratio is not None:
                    center = lower_price + (upper_price - lower_price) / 2
                    line1 = center + (upper_price - center) * price_bounds_ratio
                    line2 = center - (center - lower_price) * price_bounds_ratio

                    for line_y in [line1, line2]:
                        fig.add_shape(
                            type="line",
                            x0=date_start,
                            x1=date_end,
                            y0=line_y,
                            y1=line_y,
                            line={"color": colors.neutral, "width": 1, "dash": "dot"},
                        )

            # Create hover text
            hover_text = (
                f"<b>Position {pos.position_id}</b><br>"
                f"Start: {format_datetime(date_start)}<br>"
                f"End: {format_datetime(date_end)}<br>"
                f"Price Lower: {format_price(lower_price)}<br>"
                f"Price Upper: {format_price(upper_price)}<br>"
                f"Tick Lower: {pos.bound_tick_lower}<br>"
                f"Tick Upper: {pos.bound_tick_upper}"
            )

            # Add invisible trace for hover
            x_corners = [date_start, date_end, date_end, date_start]
            y_corners = [lower_price, lower_price, upper_price, upper_price]
            fig.add_trace(
                go.Scatter(
                    x=x_corners,
                    y=y_corners,
                    mode="lines",
                    text=[hover_text] * 4,
                    hoverinfo="text",
                    showlegend=False,
                    opacity=0,
                    line={"color": "rgba(0,0,0,0)"},
                )
            )

    # Layout
    fig.update_layout(
        title={"text": title, "font": {"size": config.title_font_size}},
        xaxis_title="Date",
        yaxis_title="Price",
        xaxis={"rangeslider": {"visible": True}},
    )

    return apply_theme(fig, config)


def plot_impermanent_loss(
    il_data: pd.DataFrame | list[dict],
    time_column: str = "timestamp",
    il_column: str = "impermanent_loss",
    show_cumulative: bool = True,
    title: str = "Impermanent Loss Over Time",
    config: PlotConfig | None = None,
) -> go.Figure:
    """Plot impermanent loss over time.

    Args:
        il_data: DataFrame or list with IL data
            Expected columns: timestamp, impermanent_loss
        time_column: Name of the time column
        il_column: Name of the IL column
        show_cumulative: If True, show cumulative IL
        title: Chart title
        config: Plot configuration

    Returns:
        Plotly figure with IL over time
    """
    config = config or get_default_config()
    colors = config.colors

    # Convert to DataFrame if necessary
    if isinstance(il_data, list):
        if not il_data:
            return create_empty_figure("No IL data available", config)
        df = pd.DataFrame(il_data)
    else:
        df = il_data.copy()

    if df.empty:
        return create_empty_figure("No IL data available", config)

    # Normalize column names
    time_col = time_column if time_column in df.columns else "timestamp"
    il_col = il_column if il_column in df.columns else "impermanent_loss"

    if time_col not in df.columns or il_col not in df.columns:
        return create_empty_figure("Invalid IL data format", config)

    # Ensure datetime
    if not pd.api.types.is_datetime64_any_dtype(df[time_col]):
        df[time_col] = pd.to_datetime(df[time_col])

    df = df.sort_values(time_col)

    # Create figure
    fig = go.Figure()

    # Add IL line
    fig.add_trace(
        go.Scatter(
            x=df[time_col],
            y=df[il_col],
            mode="lines",
            name="Impermanent Loss",
            line={"color": colors.danger, "width": config.line_width},
            fill="tozeroy",
            fillcolor=f"rgba({int(colors.danger[1:3], 16)}, {int(colors.danger[3:5], 16)}, {int(colors.danger[5:7], 16)}, 0.1)",
        )
    )

    # Add zero line
    fig.add_hline(y=0, line_dash="dash", line_color=colors.neutral)

    # Add cumulative if requested
    if show_cumulative and len(df) > 1:
        df["cumulative_il"] = df[il_col].cumsum()
        fig.add_trace(
            go.Scatter(
                x=df[time_col],
                y=df["cumulative_il"],
                mode="lines",
                name="Cumulative IL",
                line={"color": colors.warning, "width": config.line_width, "dash": "dot"},
            )
        )

    fig.update_layout(
        title={"text": title, "font": {"size": config.title_font_size}},
        xaxis_title="Time",
        yaxis_title="Impermanent Loss (%)",
        yaxis={"tickformat": ".2%"},
    )

    return apply_theme(fig, config)


def plot_fee_accumulation(
    fee_data: pd.DataFrame | list[dict],
    time_column: str = "timestamp",
    fee_column: str = "fees",
    show_cumulative: bool = True,
    fee_unit: str = "USD",
    title: str = "Fee Accumulation",
    config: PlotConfig | None = None,
) -> go.Figure:
    """Plot fee accumulation over time.

    Args:
        fee_data: DataFrame or list with fee data
            Expected columns: timestamp, fees
        time_column: Name of the time column
        fee_column: Name of the fee column
        show_cumulative: If True, show cumulative fees (default)
        fee_unit: Unit for fee display (USD, ETH, etc.)
        title: Chart title
        config: Plot configuration

    Returns:
        Plotly figure with fee accumulation
    """
    config = config or get_default_config()
    colors = config.colors

    # Convert to DataFrame if necessary
    if isinstance(fee_data, list):
        if not fee_data:
            return create_empty_figure("No fee data available", config)
        df = pd.DataFrame(fee_data)
    else:
        df = fee_data.copy()

    if df.empty:
        return create_empty_figure("No fee data available", config)

    # Normalize column names
    time_col = time_column if time_column in df.columns else "timestamp"
    fee_col = fee_column if fee_column in df.columns else "fees"

    if time_col not in df.columns or fee_col not in df.columns:
        return create_empty_figure("Invalid fee data format", config)

    # Ensure datetime
    if not pd.api.types.is_datetime64_any_dtype(df[time_col]):
        df[time_col] = pd.to_datetime(df[time_col])

    df = df.sort_values(time_col)

    # Create figure
    fig = go.Figure()

    if show_cumulative:
        df["cumulative_fees"] = df[fee_col].cumsum()
        fig.add_trace(
            go.Scatter(
                x=df[time_col],
                y=df["cumulative_fees"],
                mode="lines",
                name="Cumulative Fees",
                line={"color": colors.success, "width": config.line_width},
                fill="tozeroy",
                fillcolor=f"rgba({int(colors.success[1:3], 16)}, {int(colors.success[3:5], 16)}, {int(colors.success[5:7], 16)}, 0.1)",
            )
        )
    else:
        fig.add_trace(
            go.Bar(
                x=df[time_col],
                y=df[fee_col],
                name="Fees",
                marker_color=colors.success,
            )
        )

    fig.update_layout(
        title={"text": title, "font": {"size": config.title_font_size}},
        xaxis_title="Time",
        yaxis_title=f"Fees ({fee_unit})",
    )

    return apply_theme(fig, config)


def plot_position_range_status(
    current_price: float,
    lower_bound: float,
    upper_bound: float,
    token_pair: str = "",
    invert_prices: bool = False,
    title: str = "Position Range Status",
    config: PlotConfig | None = None,
) -> go.Figure:
    """Plot a visual indicator of current price within position range.

    Creates a horizontal bar showing:
    - Full range (lower to upper bound)
    - Current price position
    - Visual indication of in-range vs out-of-range

    Args:
        current_price: Current market price
        lower_bound: Lower price bound of position
        upper_bound: Upper price bound of position
        token_pair: Token pair name for display
        invert_prices: If True, show inverted prices
        title: Chart title
        config: Plot configuration

    Returns:
        Plotly figure with range status indicator
    """
    config = config or get_default_config()
    colors = config.colors

    if invert_prices:
        current_price = 1.0 / current_price
        lower_bound, upper_bound = 1.0 / upper_bound, 1.0 / lower_bound

    # Determine if in range
    in_range = lower_bound <= current_price <= upper_bound

    # Calculate position percentage
    range_width = upper_bound - lower_bound
    if range_width > 0:
        position_pct = (current_price - lower_bound) / range_width
    else:
        position_pct = 0.5

    # Clamp to 0-1 for display
    display_pct = max(0, min(1, position_pct))

    # Create figure
    fig = go.Figure()

    # Range bar (background)
    fig.add_trace(
        go.Bar(
            x=[1],
            y=["Range"],
            orientation="h",
            marker_color=colors.in_range if in_range else colors.out_of_range,
            opacity=0.3,
            showlegend=False,
            hoverinfo="skip",
        )
    )

    # Current position indicator
    fig.add_trace(
        go.Scatter(
            x=[display_pct],
            y=["Range"],
            mode="markers",
            marker={
                "symbol": "diamond",
                "size": 20,
                "color": colors.active_tick,
                "line": {"width": 2, "color": "white"},
            },
            name="Current Price",
            hovertemplate=f"Current: {format_price(current_price)}<br>"
            f"{'In Range' if in_range else 'Out of Range'}<extra></extra>",
        )
    )

    # Add annotations for bounds
    fig.add_annotation(
        x=0,
        y="Range",
        text=f"Lower<br>{format_price(lower_bound)}",
        showarrow=False,
        xanchor="right",
        xshift=-10,
    )
    fig.add_annotation(
        x=1,
        y="Range",
        text=f"Upper<br>{format_price(upper_bound)}",
        showarrow=False,
        xanchor="left",
        xshift=10,
    )

    # Status annotation
    status_text = "IN RANGE" if in_range else "OUT OF RANGE"
    status_color = colors.success if in_range else colors.danger
    fig.add_annotation(
        x=0.5,
        y="Range",
        text=f"<b>{status_text}</b>",
        showarrow=False,
        yshift=30,
        font={"size": 16, "color": status_color},
    )

    # Title with token pair
    full_title = title
    if token_pair:
        full_title = f"{title} - {token_pair}"

    fig.update_layout(
        title={"text": full_title, "font": {"size": config.title_font_size}},
        xaxis={"visible": False, "range": [-0.1, 1.1]},
        yaxis={"visible": False},
        height=150,
        margin={"l": 100, "r": 100, "t": 60, "b": 20},
    )

    return apply_theme(fig, config)
