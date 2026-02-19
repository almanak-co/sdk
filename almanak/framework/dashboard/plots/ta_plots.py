"""Technical Analysis (TA) plots for trading strategy dashboards.

This module provides visualization components for TA-based strategies including:
- Price charts with buy/sell signals
- RSI indicator with overbought/oversold zones
- MACD indicator with histogram
- Stochastic oscillator
- Bollinger Bands
- Performance metrics visualization

These plots are designed for any strategy using technical indicators
for trade signal generation.

Example:
    from almanak.framework.dashboard.plots.ta_plots import (
        plot_price_with_signals,
        plot_rsi_indicator,
        plot_macd_indicator,
        calculate_ta_metrics,
    )

    # Price with signals
    fig = plot_price_with_signals(
        price_data=price_df,
        buy_signals=buy_df,
        sell_signals=sell_df,
    )
    st.plotly_chart(fig)

    # RSI indicator
    fig = plot_rsi_indicator(
        rsi_data=rsi_series,
        time_index=price_df.index,
        overbought=70,
        oversold=30,
    )
    st.plotly_chart(fig)
"""

from dataclasses import dataclass

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from almanak.framework.dashboard.plots.base import (
    PlotConfig,
    apply_theme,
    create_empty_figure,
    get_default_config,
)


@dataclass
class TAMetrics:
    """Technical analysis performance metrics.

    Attributes:
        total_trades: Total number of trades
        win_rate: Percentage of winning trades (0-100)
        avg_win: Average profit on winning trades
        avg_loss: Average loss on losing trades (positive value)
        profit_factor: Ratio of gross profit to gross loss
        max_drawdown: Maximum drawdown percentage
        sharpe_ratio: Annualized Sharpe ratio
        total_pnl: Total profit/loss
        best_trade: Best single trade return
        worst_trade: Worst single trade return
    """

    total_trades: int = 0
    win_rate: float = 0.0
    avg_win: float = 0.0
    avg_loss: float = 0.0
    profit_factor: float = 0.0
    max_drawdown: float = 0.0
    sharpe_ratio: float = 0.0
    total_pnl: float = 0.0
    best_trade: float = 0.0
    worst_trade: float = 0.0


def plot_price_with_signals(
    price_data: pd.DataFrame,
    buy_signals: pd.DataFrame | None = None,
    sell_signals: pd.DataFrame | None = None,
    indicators: dict[str, pd.Series] | None = None,
    time_column: str = "time",
    price_column: str = "price",
    title: str = "Price Chart with Signals",
    config: PlotConfig | None = None,
) -> go.Figure:
    """Plot price data with buy/sell signals and optional indicators.

    Args:
        price_data: DataFrame with price data
            Expected columns: time, price (or configured names)
        buy_signals: DataFrame with buy signal points
            Expected columns: time, price
        sell_signals: DataFrame with sell signal points
            Expected columns: time, price
        indicators: Dict of indicator name -> pandas Series to overlay
        time_column: Name of time column in price_data
        price_column: Name of price column in price_data
        title: Chart title
        config: Plot configuration

    Returns:
        Plotly figure with price and signals
    """
    config = config or get_default_config()
    colors = config.colors

    if price_data.empty:
        return create_empty_figure("No price data available", config)

    # Normalize column names
    time_col = time_column if time_column in price_data.columns else "time"
    price_col = price_column if price_column in price_data.columns else "price"

    # Handle alternative column names
    if time_col not in price_data.columns:
        for alt in ["timestamp", "Timestamp", "Time", "date", "Date"]:
            if alt in price_data.columns:
                time_col = alt
                break
    if price_col not in price_data.columns:
        for alt in ["close", "Close", "Price"]:
            if alt in price_data.columns:
                price_col = alt
                break

    if time_col not in price_data.columns or price_col not in price_data.columns:
        return create_empty_figure("Invalid price data format", config)

    fig = go.Figure()

    # Add price line
    fig.add_trace(
        go.Scatter(
            x=price_data[time_col],
            y=price_data[price_col],
            mode="lines",
            name="Price",
            line={"color": colors.primary, "width": config.line_width},
        )
    )

    # Add indicators if provided
    if indicators:
        indicator_colors = ["#9B59B6", "#E67E22", "#1ABC9C", "#F39C12"]
        for i, (name, data) in enumerate(indicators.items()):
            color = indicator_colors[i % len(indicator_colors)]
            fig.add_trace(
                go.Scatter(
                    x=price_data[time_col],
                    y=data,
                    mode="lines",
                    name=name,
                    line={"color": color, "width": config.line_width},
                )
            )

    # Add buy signals
    if buy_signals is not None and not buy_signals.empty:
        buy_time = buy_signals.get(time_col, buy_signals.get("time"))
        buy_price = buy_signals.get(price_col, buy_signals.get("price"))
        if buy_time is not None and buy_price is not None:
            fig.add_trace(
                go.Scatter(
                    x=buy_time,
                    y=buy_price,
                    mode="markers",
                    name="Buy Signal",
                    marker={
                        "symbol": "triangle-up",
                        "size": 15,
                        "color": colors.buy,
                        "line": {"width": 2, "color": "#1E8449"},
                    },
                )
            )

    # Add sell signals
    if sell_signals is not None and not sell_signals.empty:
        sell_time = sell_signals.get(time_col, sell_signals.get("time"))
        sell_price = sell_signals.get(price_col, sell_signals.get("price"))
        if sell_time is not None and sell_price is not None:
            fig.add_trace(
                go.Scatter(
                    x=sell_time,
                    y=sell_price,
                    mode="markers",
                    name="Sell Signal",
                    marker={
                        "symbol": "triangle-down",
                        "size": 15,
                        "color": colors.sell,
                        "line": {"width": 2, "color": "#C0392B"},
                    },
                )
            )

    fig.update_layout(
        title={"text": title, "font": {"size": config.title_font_size}},
        xaxis_title="Time",
        yaxis_title="Price",
        hovermode="x unified",
    )

    return apply_theme(fig, config)


def plot_rsi_indicator(
    rsi_data: pd.Series | list,
    time_index: pd.DatetimeIndex | pd.Series | list,
    overbought: float = 70,
    oversold: float = 30,
    current_value: float | None = None,
    title: str = "RSI Indicator",
    config: PlotConfig | None = None,
) -> go.Figure:
    """Plot RSI indicator with overbought/oversold zones.

    Args:
        rsi_data: Series or list of RSI values
        time_index: DateTime index for x-axis
        overbought: Overbought threshold (default 70)
        oversold: Oversold threshold (default 30)
        current_value: Current RSI value to highlight
        title: Chart title
        config: Plot configuration

    Returns:
        Plotly figure with RSI indicator
    """
    config = config or get_default_config()
    colors = config.colors

    if isinstance(rsi_data, list):
        rsi_data = pd.Series(rsi_data)

    if len(rsi_data) == 0:
        return create_empty_figure("No RSI data available", config)

    fig = go.Figure()

    # Add RSI line
    fig.add_trace(
        go.Scatter(
            x=time_index,
            y=rsi_data,
            mode="lines",
            name="RSI",
            line={"color": colors.secondary, "width": config.line_width},
        )
    )

    # Add overbought/oversold lines
    fig.add_hline(
        y=overbought,
        line_dash="dash",
        line_color=colors.danger,
        annotation_text=f"Overbought ({overbought})",
        annotation_position="bottom right",
    )
    fig.add_hline(
        y=oversold,
        line_dash="dash",
        line_color=colors.success,
        annotation_text=f"Oversold ({oversold})",
        annotation_position="top right",
    )

    # Add neutral line
    fig.add_hline(y=50, line_dash="dot", line_color=colors.neutral, line_width=1)

    # Add shaded zones
    fig.add_hrect(y0=overbought, y1=100, fillcolor=colors.danger, opacity=0.1)
    fig.add_hrect(y0=0, y1=oversold, fillcolor=colors.success, opacity=0.1)

    # Highlight current value if provided
    if current_value is not None:
        # Determine color based on zone
        if current_value >= overbought:
            current_color = colors.danger
            zone_text = "OVERBOUGHT"
        elif current_value <= oversold:
            current_color = colors.success
            zone_text = "OVERSOLD"
        else:
            current_color = colors.neutral
            zone_text = "NEUTRAL"

        fig.add_annotation(
            x=1.02,
            xref="paper",
            y=current_value,
            text=f"<b>{current_value:.1f}</b><br>{zone_text}",
            showarrow=False,
            font={"color": current_color, "size": 12},
            xanchor="left",
        )

    fig.update_layout(
        title={"text": title, "font": {"size": config.title_font_size}},
        xaxis_title="Time",
        yaxis_title="RSI Value",
        yaxis={"range": [0, 100]},
    )

    return apply_theme(fig, config)


def plot_macd_indicator(
    macd: pd.Series | list,
    macd_signal: pd.Series | list,
    macd_hist: pd.Series | list,
    time_index: pd.DatetimeIndex | pd.Series | list,
    title: str = "MACD Indicator",
    config: PlotConfig | None = None,
) -> go.Figure:
    """Plot MACD indicator with signal line and histogram.

    Args:
        macd: MACD line values
        macd_signal: Signal line values
        macd_hist: Histogram values (MACD - Signal)
        time_index: DateTime index for x-axis
        title: Chart title
        config: Plot configuration

    Returns:
        Plotly figure with MACD indicator
    """
    config = config or get_default_config()
    colors = config.colors

    if isinstance(macd, list):
        macd = pd.Series(macd)
    if isinstance(macd_signal, list):
        macd_signal = pd.Series(macd_signal)
    if isinstance(macd_hist, list):
        macd_hist = pd.Series(macd_hist)

    if len(macd) == 0:
        return create_empty_figure("No MACD data available", config)

    # Create subplots
    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.05,
        subplot_titles=("MACD & Signal", "Histogram"),
        row_heights=[0.6, 0.4],
    )

    # MACD and Signal lines
    fig.add_trace(
        go.Scatter(
            x=time_index,
            y=macd,
            mode="lines",
            name="MACD",
            line={"color": colors.primary, "width": config.line_width},
        ),
        row=1,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=time_index,
            y=macd_signal,
            mode="lines",
            name="Signal",
            line={"color": colors.danger, "width": config.line_width},
        ),
        row=1,
        col=1,
    )

    # Histogram with color based on sign
    hist_colors = [colors.success if val >= 0 else colors.danger for val in macd_hist]
    fig.add_trace(
        go.Bar(
            x=time_index,
            y=macd_hist,
            name="Histogram",
            marker_color=hist_colors,
        ),
        row=2,
        col=1,
    )

    # Add zero lines
    fig.add_hline(y=0, line_color=colors.neutral, row=1, col=1)
    fig.add_hline(y=0, line_color=colors.neutral, row=2, col=1)

    fig.update_xaxes(title_text="Time", row=2, col=1)
    fig.update_yaxes(title_text="MACD", row=1, col=1)
    fig.update_yaxes(title_text="Histogram", row=2, col=1)

    fig.update_layout(
        title={"text": title, "font": {"size": config.title_font_size}},
        showlegend=True,
        height=config.height + 200,  # Extra height for subplots
    )

    return apply_theme(fig, config)


def plot_stochastic_indicator(
    stoch_k: pd.Series | list,
    stoch_d: pd.Series | list,
    time_index: pd.DatetimeIndex | pd.Series | list,
    overbought: float = 80,
    oversold: float = 20,
    signal_high: float = 70,
    signal_low: float = 30,
    title: str = "Stochastic Oscillator",
    config: PlotConfig | None = None,
) -> go.Figure:
    """Plot Stochastic Oscillator with %K and %D lines.

    Args:
        stoch_k: %K line values
        stoch_d: %D line values
        time_index: DateTime index for x-axis
        overbought: Overbought zone threshold (default 80)
        oversold: Oversold zone threshold (default 20)
        signal_high: Upper signal threshold (default 70)
        signal_low: Lower signal threshold (default 30)
        title: Chart title
        config: Plot configuration

    Returns:
        Plotly figure with Stochastic Oscillator
    """
    config = config or get_default_config()
    colors = config.colors

    if isinstance(stoch_k, list):
        stoch_k = pd.Series(stoch_k)
    if isinstance(stoch_d, list):
        stoch_d = pd.Series(stoch_d)

    if len(stoch_k) == 0:
        return create_empty_figure("No Stochastic data available", config)

    fig = go.Figure()

    # Add %K and %D lines
    fig.add_trace(
        go.Scatter(
            x=time_index,
            y=stoch_k,
            mode="lines",
            name="%K",
            line={"color": colors.primary, "width": config.line_width},
        )
    )
    fig.add_trace(
        go.Scatter(
            x=time_index,
            y=stoch_d,
            mode="lines",
            name="%D",
            line={"color": colors.warning, "width": config.line_width},
        )
    )

    # Add overbought/oversold zones
    fig.add_hline(
        y=overbought,
        line_dash="dash",
        line_color=colors.danger,
        annotation_text=f"Overbought ({overbought})",
    )
    fig.add_hline(
        y=oversold,
        line_dash="dash",
        line_color=colors.success,
        annotation_text=f"Oversold ({oversold})",
    )

    # Add signal thresholds
    fig.add_hline(y=signal_high, line_dash="dot", line_color=colors.danger, line_width=1)
    fig.add_hline(y=signal_low, line_dash="dot", line_color=colors.success, line_width=1)

    # Add shaded zones
    fig.add_hrect(y0=overbought, y1=100, fillcolor=colors.danger, opacity=0.1)
    fig.add_hrect(y0=0, y1=oversold, fillcolor=colors.success, opacity=0.1)

    fig.update_layout(
        title={"text": title, "font": {"size": config.title_font_size}},
        xaxis_title="Time",
        yaxis_title="Stochastic Value",
        yaxis={"range": [0, 100]},
    )

    return apply_theme(fig, config)


def plot_bollinger_bands(
    price_data: pd.Series | list,
    upper_band: pd.Series | list,
    middle_band: pd.Series | list,
    lower_band: pd.Series | list,
    time_index: pd.DatetimeIndex | pd.Series | list,
    title: str = "Bollinger Bands",
    config: PlotConfig | None = None,
) -> go.Figure:
    """Plot price with Bollinger Bands.

    Args:
        price_data: Price series
        upper_band: Upper band values
        middle_band: Middle band (SMA) values
        lower_band: Lower band values
        time_index: DateTime index for x-axis
        title: Chart title
        config: Plot configuration

    Returns:
        Plotly figure with Bollinger Bands
    """
    config = config or get_default_config()
    colors = config.colors

    if isinstance(price_data, list):
        price_data = pd.Series(price_data)

    if len(price_data) == 0:
        return create_empty_figure("No price data available", config)

    fig = go.Figure()

    # Add upper band first (for fill)
    fig.add_trace(
        go.Scatter(
            x=time_index,
            y=upper_band,
            mode="lines",
            name="Upper Band",
            line={"color": colors.neutral, "width": 1, "dash": "dash"},
        )
    )

    # Add lower band with fill to upper
    fig.add_trace(
        go.Scatter(
            x=time_index,
            y=lower_band,
            mode="lines",
            name="Lower Band",
            line={"color": colors.neutral, "width": 1, "dash": "dash"},
            fill="tonexty",
            fillcolor="rgba(128, 128, 128, 0.2)",
        )
    )

    # Add middle band (SMA)
    fig.add_trace(
        go.Scatter(
            x=time_index,
            y=middle_band,
            mode="lines",
            name="Middle Band (SMA)",
            line={"color": colors.warning, "width": 1},
        )
    )

    # Add price line on top
    fig.add_trace(
        go.Scatter(
            x=time_index,
            y=price_data,
            mode="lines",
            name="Price",
            line={"color": colors.primary, "width": config.line_width},
        )
    )

    fig.update_layout(
        title={"text": title, "font": {"size": config.title_font_size}},
        xaxis_title="Time",
        yaxis_title="Price",
        hovermode="x unified",
    )

    return apply_theme(fig, config)


def plot_ta_performance_metrics(
    trades_df: pd.DataFrame,
    balance_df: pd.DataFrame | None = None,
    time_column: str = "time",
    pnl_column: str = "pnl",
    balance_column: str = "total_value",
    title: str = "Strategy Performance",
    config: PlotConfig | None = None,
) -> go.Figure:
    """Plot comprehensive performance metrics for TA strategy.

    Args:
        trades_df: DataFrame with trade history
            Expected columns: time, pnl
        balance_df: Optional DataFrame with balance history
            Expected columns: time, total_value
        time_column: Name of time column
        pnl_column: Name of PnL column
        balance_column: Name of balance column
        title: Chart title
        config: Plot configuration

    Returns:
        Plotly figure with performance subplots
    """
    config = config or get_default_config()
    colors = config.colors

    # Create subplots
    has_balance = balance_df is not None and not balance_df.empty
    rows = 2 if has_balance else 1
    subplot_titles = ["Cumulative P&L", "Portfolio Balance"] if has_balance else ["Cumulative P&L"]

    fig = make_subplots(
        rows=rows,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.05,
        subplot_titles=subplot_titles,
    )

    # Calculate cumulative P&L from trades
    if not trades_df.empty and pnl_column in trades_df.columns:
        time_col = time_column if time_column in trades_df.columns else "time"
        df = trades_df.sort_values(time_col).copy()
        df["cumulative_pnl"] = df[pnl_column].cumsum()

        fig.add_trace(
            go.Scatter(
                x=df[time_col],
                y=df["cumulative_pnl"],
                mode="lines+markers",
                name="Cumulative P&L",
                line={"color": colors.success, "width": config.line_width},
                marker={"size": 6},
            ),
            row=1,
            col=1,
        )

        # Add zero line
        fig.add_hline(y=0, line_dash="dash", line_color=colors.neutral, row=1, col=1)

    # Add balance history if provided
    if has_balance and balance_df is not None:
        time_col = time_column if time_column in balance_df.columns else "time"
        bal_col = balance_column if balance_column in balance_df.columns else "total_value"

        if time_col in balance_df.columns and bal_col in balance_df.columns:
            fig.add_trace(
                go.Scatter(
                    x=balance_df[time_col],
                    y=balance_df[bal_col],
                    mode="lines",
                    name="Portfolio Value",
                    line={"color": colors.primary, "width": config.line_width},
                ),
                row=2,
                col=1,
            )

    fig.update_xaxes(title_text="Time", row=rows, col=1)
    fig.update_yaxes(title_text="P&L", row=1, col=1)
    if has_balance:
        fig.update_yaxes(title_text="Balance", row=2, col=1)

    fig.update_layout(
        title={"text": title, "font": {"size": config.title_font_size}},
        showlegend=True,
        height=config.height + 200 if has_balance else config.height,
    )

    return apply_theme(fig, config)


def calculate_ta_metrics(
    trades_df: pd.DataFrame,
    pnl_column: str = "pnl",
    returns_column: str | None = None,
) -> TAMetrics:
    """Calculate standard TA performance metrics.

    Args:
        trades_df: DataFrame with trade history
            Expected columns: pnl (or configured column)
        pnl_column: Name of PnL column
        returns_column: Name of returns column (for Sharpe calculation)

    Returns:
        TAMetrics dataclass with calculated metrics
    """
    metrics = TAMetrics()

    if trades_df.empty or pnl_column not in trades_df.columns:
        return metrics

    pnl = trades_df[pnl_column]

    # Basic metrics
    metrics.total_trades = len(trades_df)
    metrics.total_pnl = float(pnl.sum())

    winning_trades = pnl[pnl > 0]
    losing_trades = pnl[pnl < 0]

    if metrics.total_trades > 0:
        metrics.win_rate = (len(winning_trades) / metrics.total_trades) * 100

    if not winning_trades.empty:
        metrics.avg_win = float(winning_trades.mean())
        metrics.best_trade = float(winning_trades.max())

    if not losing_trades.empty:
        metrics.avg_loss = float(abs(losing_trades.mean()))
        metrics.worst_trade = float(losing_trades.min())

    # Profit factor
    gross_profit = winning_trades.sum() if not winning_trades.empty else 0
    gross_loss = abs(losing_trades.sum()) if not losing_trades.empty else 0
    if gross_loss > 0:
        metrics.profit_factor = float(gross_profit / gross_loss)

    # Max drawdown
    cumulative_pnl = pnl.cumsum()
    running_max = cumulative_pnl.expanding().max()
    drawdown = (cumulative_pnl - running_max) / running_max.replace(0, 1)
    metrics.max_drawdown = float(abs(drawdown.min()) * 100)

    # Sharpe ratio (simplified)
    if metrics.total_trades > 1:
        if returns_column and returns_column in trades_df.columns:
            returns = trades_df[returns_column]
        else:
            # Calculate returns from PnL
            returns = pnl.pct_change().dropna()

        if len(returns) > 0 and returns.std() != 0:
            metrics.sharpe_ratio = float((returns.mean() / returns.std()) * np.sqrt(252))

    return metrics


def plot_cci_indicator(
    cci_data: pd.Series | list,
    time_index: pd.DatetimeIndex | pd.Series | list,
    overbought: float = 100,
    oversold: float = -100,
    title: str = "CCI Indicator",
    config: PlotConfig | None = None,
) -> go.Figure:
    """Plot Commodity Channel Index (CCI) indicator.

    Args:
        cci_data: CCI values
        time_index: DateTime index for x-axis
        overbought: Overbought threshold (default 100)
        oversold: Oversold threshold (default -100)
        title: Chart title
        config: Plot configuration

    Returns:
        Plotly figure with CCI indicator
    """
    config = config or get_default_config()
    colors = config.colors

    if isinstance(cci_data, list):
        cci_data = pd.Series(cci_data)

    if len(cci_data) == 0:
        return create_empty_figure("No CCI data available", config)

    fig = go.Figure()

    # Add CCI line
    fig.add_trace(
        go.Scatter(
            x=time_index,
            y=cci_data,
            mode="lines",
            name="CCI",
            line={"color": colors.secondary, "width": config.line_width},
        )
    )

    # Add threshold lines
    fig.add_hline(
        y=overbought, line_dash="dash", line_color=colors.danger, annotation_text=f"Overbought ({overbought})"
    )
    fig.add_hline(y=oversold, line_dash="dash", line_color=colors.success, annotation_text=f"Oversold ({oversold})")
    fig.add_hline(y=0, line_dash="dot", line_color=colors.neutral)

    # Add shaded zones
    fig.add_hrect(y0=overbought, y1=cci_data.max() + 50, fillcolor=colors.danger, opacity=0.1)
    fig.add_hrect(y0=cci_data.min() - 50, y1=oversold, fillcolor=colors.success, opacity=0.1)

    fig.update_layout(
        title={"text": title, "font": {"size": config.title_font_size}},
        xaxis_title="Time",
        yaxis_title="CCI Value",
    )

    return apply_theme(fig, config)


def plot_atr_indicator(
    atr_data: pd.Series | list,
    time_index: pd.DatetimeIndex | pd.Series | list,
    title: str = "ATR (Average True Range)",
    config: PlotConfig | None = None,
) -> go.Figure:
    """Plot Average True Range (ATR) indicator.

    Args:
        atr_data: ATR values
        time_index: DateTime index for x-axis
        title: Chart title
        config: Plot configuration

    Returns:
        Plotly figure with ATR indicator
    """
    config = config or get_default_config()
    colors = config.colors

    if isinstance(atr_data, list):
        atr_data = pd.Series(atr_data)

    if len(atr_data) == 0:
        return create_empty_figure("No ATR data available", config)

    fig = go.Figure()

    # Add ATR line with fill
    fig.add_trace(
        go.Scatter(
            x=time_index,
            y=atr_data,
            mode="lines",
            name="ATR",
            line={"color": colors.warning, "width": config.line_width},
            fill="tozeroy",
            fillcolor=f"rgba({int(colors.warning[1:3], 16)}, {int(colors.warning[3:5], 16)}, {int(colors.warning[5:7], 16)}, 0.1)",
        )
    )

    # Add average line
    avg_atr = atr_data.mean()
    fig.add_hline(y=avg_atr, line_dash="dash", line_color=colors.neutral, annotation_text=f"Avg: {avg_atr:.4f}")

    fig.update_layout(
        title={"text": title, "font": {"size": config.title_font_size}},
        xaxis_title="Time",
        yaxis_title="ATR Value",
    )

    return apply_theme(fig, config)


def plot_obv_indicator(
    obv_data: pd.Series | list,
    price_data: pd.Series | list,
    time_index: pd.DatetimeIndex | pd.Series | list,
    title: str = "On-Balance Volume (OBV)",
    config: PlotConfig | None = None,
) -> go.Figure:
    """Plot On-Balance Volume (OBV) with price comparison.

    Args:
        obv_data: OBV values
        price_data: Price values for comparison
        time_index: DateTime index for x-axis
        title: Chart title
        config: Plot configuration

    Returns:
        Plotly figure with OBV and price
    """
    config = config or get_default_config()
    colors = config.colors

    if isinstance(obv_data, list):
        obv_data = pd.Series(obv_data)
    if isinstance(price_data, list):
        price_data = pd.Series(price_data)

    if len(obv_data) == 0:
        return create_empty_figure("No OBV data available", config)

    # Create subplots
    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.05,
        subplot_titles=("Price", "OBV"),
        row_heights=[0.5, 0.5],
    )

    # Add price
    fig.add_trace(
        go.Scatter(
            x=time_index,
            y=price_data,
            mode="lines",
            name="Price",
            line={"color": colors.primary, "width": config.line_width},
        ),
        row=1,
        col=1,
    )

    # Add OBV
    fig.add_trace(
        go.Scatter(
            x=time_index,
            y=obv_data,
            mode="lines",
            name="OBV",
            line={"color": colors.success, "width": config.line_width},
        ),
        row=2,
        col=1,
    )

    fig.update_xaxes(title_text="Time", row=2, col=1)
    fig.update_yaxes(title_text="Price", row=1, col=1)
    fig.update_yaxes(title_text="OBV", row=2, col=1)

    fig.update_layout(
        title={"text": title, "font": {"size": config.title_font_size}},
        height=config.height + 200,
    )

    return apply_theme(fig, config)
