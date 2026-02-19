"""
Technical Analysis (TA) Dashboard Template.

Reusable template for creating dashboards for indicator-based strategies.
Supports any TA indicator with configurable signal logic and visualization.

Usage:
    from almanak.framework.dashboard.templates import TADashboardConfig, render_ta_dashboard

    config = TADashboardConfig(
        indicator_name="RSI",
        indicator_period=14,
        upper_threshold=70,
        lower_threshold=30,
        signal_type="reversion",  # or "momentum"
    )

    def render_custom_dashboard(strategy_id, strategy_config, api_client, session_state):
        render_ta_dashboard(strategy_id, strategy_config, session_state, config)
"""

from collections.abc import Callable
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Literal

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

from almanak.framework.dashboard.plots import plot_price_with_signals
from almanak.framework.dashboard.plots.base import get_default_config


@dataclass
class TADashboardConfig:
    """Configuration for a TA dashboard.

    Attributes:
        indicator_name: Name of the indicator (e.g., "RSI", "MACD", "CCI")
        indicator_period: Primary period for the indicator
        secondary_periods: Additional periods (e.g., signal line for MACD)
        upper_threshold: Upper threshold for signals (overbought/bullish)
        lower_threshold: Lower threshold for signals (oversold/bearish)
        signal_type: Type of signal logic - "reversion" or "momentum"
        value_format: Format string for displaying indicator value (e.g., "{:.1f}", "{:+.2f}")
        value_suffix: Suffix for indicator value (e.g., "%", " bps")
        show_progress_bar: Whether to show a progress bar for the indicator
        progress_range: (min, max) range for progress bar normalization
        custom_signal_fn: Optional custom function for signal determination
        chain: Default chain name
        protocol: Default protocol name
        base_token: Default base token
        quote_token: Default quote token
    """

    indicator_name: str
    indicator_period: int = 14
    secondary_periods: list[int] = field(default_factory=list)
    upper_threshold: float | None = None
    lower_threshold: float | None = None
    signal_type: Literal["reversion", "momentum"] = "reversion"
    value_format: str = "{:.1f}"
    value_suffix: str = ""
    show_progress_bar: bool = False
    progress_range: tuple[float, float] = (0, 100)
    custom_signal_fn: Callable[[dict[str, Any]], str] | None = None
    chain: str = "Arbitrum"
    protocol: str = "Uniswap V3"
    base_token: str = "WETH"
    quote_token: str = "USDC"


def render_ta_dashboard(
    strategy_id: str,
    strategy_config: dict[str, Any],
    session_state: dict[str, Any],
    config: TADashboardConfig,
) -> None:
    """Render a technical analysis dashboard using the provided configuration.

    Args:
        strategy_id: The strategy identifier
        strategy_config: Strategy configuration dictionary
        session_state: Current session state with indicator values
        config: TADashboardConfig for this dashboard
    """
    st.title(f"{config.indicator_name} Strategy Dashboard")

    # Extract config overrides
    base_token = strategy_config.get("base_token", config.base_token)
    quote_token = strategy_config.get("quote_token", config.quote_token)
    chain = strategy_config.get("chain", config.chain)
    protocol = strategy_config.get("protocol", config.protocol)
    period = strategy_config.get(f"{config.indicator_name.lower()}_period", config.indicator_period)

    st.markdown(f"**Strategy ID:** `{strategy_id}`")
    st.markdown(f"**Pair:** {base_token}/{quote_token}")
    st.markdown(f"**Chain:** {chain} | **Protocol:** {protocol}")

    st.divider()

    # Indicator section
    _render_indicator_section(session_state, strategy_config, config, period)

    st.divider()

    # Charts section - Price with signals and indicator
    _render_charts_section(session_state, strategy_config, config, period)

    st.divider()

    # Signal status
    _render_signal_status(session_state, strategy_config, config)

    st.divider()

    # Position section
    st.subheader("Current Position")
    _render_position(session_state, base_token, quote_token)

    st.divider()

    # Performance section
    st.subheader("Performance")
    _render_performance(session_state)


def _render_indicator_section(
    session_state: dict[str, Any],
    strategy_config: dict[str, Any],
    config: TADashboardConfig,
    period: int,
) -> None:
    """Render the indicator display section."""
    st.subheader(f"{config.indicator_name}({period})")

    # Get primary indicator value
    indicator_key = config.indicator_name.lower()
    indicator_value = float(session_state.get(f"{indicator_key}_value", session_state.get(indicator_key, 50)))

    # Create columns based on whether we have thresholds
    if config.upper_threshold is not None and config.lower_threshold is not None:
        col1, col2, col3 = st.columns(3)
        with col1:
            formatted_value = config.value_format.format(indicator_value) + config.value_suffix
            st.metric(config.indicator_name, formatted_value)
        with col2:
            st.metric("Upper", f"{config.upper_threshold}{config.value_suffix}")
        with col3:
            st.metric("Lower", f"{config.lower_threshold}{config.value_suffix}")
    else:
        col1, col2 = st.columns(2)
        with col1:
            formatted_value = config.value_format.format(indicator_value) + config.value_suffix
            st.metric(config.indicator_name, formatted_value)
        with col2:
            st.metric("Period", str(period))

    # Secondary indicator values (e.g., signal line for MACD)
    if config.secondary_periods:
        secondary_cols = st.columns(len(config.secondary_periods) + 1)
        for i, sec_period in enumerate(config.secondary_periods):
            with secondary_cols[i]:
                key = f"{indicator_key}_signal_{sec_period}"
                alt_key = f"{indicator_key}_{sec_period}"
                value = float(session_state.get(key, session_state.get(alt_key, 0)))
                formatted = config.value_format.format(value) + config.value_suffix
                st.metric(f"Signal({sec_period})", formatted)

    # Progress bar visualization
    if config.show_progress_bar:
        min_val, max_val = config.progress_range
        normalized = (indicator_value - min_val) / (max_val - min_val)
        normalized = max(0, min(1, normalized))
        st.progress(normalized, text=f"{config.indicator_name}: {config.value_format.format(indicator_value)}")


def _render_charts_section(
    session_state: dict[str, Any],
    strategy_config: dict[str, Any],
    config: TADashboardConfig,
    period: int,
) -> None:
    """Render price and indicator charts with buy/sell signals."""
    st.subheader("Price & Indicator Charts")

    # Get price history
    price_history = session_state.get("price_history")
    if price_history is None or (isinstance(price_history, pd.DataFrame) and price_history.empty):
        st.info("Price history data not available")
        return

    # Convert to DataFrame if it's a list
    if isinstance(price_history, list):
        price_df = pd.DataFrame(price_history, columns=["time", "price"])
    else:
        price_df = price_history.copy()

    # Ensure time column is datetime
    if "time" in price_df.columns:
        if not pd.api.types.is_datetime64_any_dtype(price_df["time"]):
            price_df["time"] = pd.to_datetime(price_df["time"])
    elif "timestamp" in price_df.columns:
        price_df = price_df.rename(columns={"timestamp": "time"})
        if not pd.api.types.is_datetime64_any_dtype(price_df["time"]):
            price_df["time"] = pd.to_datetime(price_df["time"])
    else:
        st.warning("Price data missing time column")
        return

    # Get buy/sell signals
    buy_signals = session_state.get("buy_signals")
    sell_signals = session_state.get("sell_signals")

    # Convert signals to DataFrame if they're lists
    buy_df = None
    sell_df = None

    if buy_signals:
        if isinstance(buy_signals, list):
            buy_df = pd.DataFrame(buy_signals, columns=["time", "price"])
        elif isinstance(buy_signals, pd.DataFrame):
            buy_df = buy_signals.copy()
        if buy_df is not None and "time" in buy_df.columns:
            if not pd.api.types.is_datetime64_any_dtype(buy_df["time"]):
                buy_df["time"] = pd.to_datetime(buy_df["time"])

    if sell_signals:
        if isinstance(sell_signals, list):
            sell_df = pd.DataFrame(sell_signals, columns=["time", "price"])
        elif isinstance(sell_signals, pd.DataFrame):
            sell_df = sell_signals.copy()
        if sell_df is not None and "time" in sell_df.columns:
            if not pd.api.types.is_datetime64_any_dtype(sell_df["time"]):
                sell_df["time"] = pd.to_datetime(sell_df["time"])

    # Get indicator data
    indicator_key = config.indicator_name.lower()
    indicator_data = session_state.get(f"{indicator_key}_data") or session_state.get(f"{indicator_key}_history")

    # For RSI specifically, create combined subplot
    if config.indicator_name.upper() == "RSI" and indicator_data:
        # Create subplot: price on top, RSI on bottom
        fig = make_subplots(
            rows=2,
            cols=1,
            shared_xaxes=True,
            vertical_spacing=0.05,
            row_heights=[0.7, 0.3],
            subplot_titles=("Price with Buy/Sell Signals", f"{config.indicator_name} Indicator"),
        )

        config_plot = get_default_config()
        colors = config_plot.colors

        # Add price line
        fig.add_trace(
            go.Scatter(
                x=price_df["time"],
                y=price_df["price"],
                mode="lines",
                name="Price",
                line={"color": colors.primary, "width": 2},
            ),
            row=1,
            col=1,
        )

        # Add buy signals (green triangles up)
        if buy_df is not None and not buy_df.empty:
            for _, signal in buy_df.iterrows():
                signal_time = signal["time"]
                signal_price = signal.get(
                    "price",
                    price_df.loc[price_df["time"] == signal_time, "price"].values[0]
                    if len(price_df.loc[price_df["time"] == signal_time]) > 0
                    else price_df["price"].iloc[-1],
                )
                fig.add_trace(
                    go.Scatter(
                        x=[signal_time],
                        y=[signal_price],
                        mode="markers",
                        name="Buy",
                        marker={
                            "symbol": "triangle-up",
                            "size": 15,
                            "color": colors.success,
                            "line": {"color": "white", "width": 1},
                        },
                        showlegend=False,
                    ),
                    row=1,
                    col=1,
                )

        # Add sell signals (red triangles down)
        if sell_df is not None and not sell_df.empty:
            for _, signal in sell_df.iterrows():
                signal_time = signal["time"]
                signal_price = signal.get(
                    "price",
                    price_df.loc[price_df["time"] == signal_time, "price"].values[0]
                    if len(price_df.loc[price_df["time"] == signal_time]) > 0
                    else price_df["price"].iloc[-1],
                )
                fig.add_trace(
                    go.Scatter(
                        x=[signal_time],
                        y=[signal_price],
                        mode="markers",
                        name="Sell",
                        marker={
                            "symbol": "triangle-down",
                            "size": 15,
                            "color": colors.danger,
                            "line": {"color": "white", "width": 1},
                        },
                        showlegend=False,
                    ),
                    row=1,
                    col=1,
                )

        # Add RSI indicator
        if isinstance(indicator_data, list):
            # Convert list of tuples to Series
            rsi_times = [item[0] for item in indicator_data]
            rsi_values = [item[1] for item in indicator_data]
            rsi_series = pd.Series(rsi_values, index=pd.to_datetime(rsi_times))
        else:
            rsi_series = indicator_data

        overbought = config.upper_threshold or 70
        oversold = config.lower_threshold or 30

        fig.add_trace(
            go.Scatter(
                x=rsi_series.index,
                y=rsi_series.values,
                mode="lines",
                name="RSI",
                line={"color": colors.secondary, "width": 2},
            ),
            row=2,
            col=1,
        )

        # Add RSI zones
        fig.add_hrect(
            y0=0,
            y1=oversold,
            fillcolor=colors.success,
            opacity=0.1,
            layer="below",
            line_width=0,
            row=2,
            col=1,
        )
        fig.add_hrect(
            y0=overbought,
            y1=100,
            fillcolor=colors.danger,
            opacity=0.1,
            layer="below",
            line_width=0,
            row=2,
            col=1,
        )

        # Add reference lines
        fig.add_hline(y=oversold, line_dash="dash", line_color=colors.success, row=2, col=1)
        fig.add_hline(y=50, line_dash="dash", line_color=colors.neutral, row=2, col=1)
        fig.add_hline(y=overbought, line_dash="dash", line_color=colors.danger, row=2, col=1)

        # Update layout
        fig.update_xaxes(title_text="Time", row=2, col=1)
        fig.update_yaxes(title_text="Price", row=1, col=1)
        fig.update_yaxes(title_text="RSI", range=[0, 100], row=2, col=1)
        fig.update_layout(
            height=800,
            hovermode="x unified",
            showlegend=True,
        )

        st.plotly_chart(fig, use_container_width=True)

    else:
        # For other indicators, use separate charts
        # Price chart with signals
        fig_price = plot_price_with_signals(
            price_data=price_df,
            buy_signals=buy_df,
            sell_signals=sell_df,
            title="Price with Buy/Sell Signals",
        )
        st.plotly_chart(fig_price, use_container_width=True)

        # Indicator chart if available (for non-RSI indicators)
        # Note: RSI with indicator_data is handled in the if branch above (line 244),
        # so this else branch only handles non-RSI indicators
        if indicator_data:
            if isinstance(indicator_data, list):
                indicator_times = [item[0] for item in indicator_data]
                indicator_values = [item[1] for item in indicator_data]
                indicator_series = pd.Series(indicator_values, index=pd.to_datetime(indicator_times))
            else:
                indicator_series = indicator_data

            # Generic indicator line chart for non-RSI indicators
            fig_indicator = go.Figure()
            fig_indicator.add_trace(
                go.Scatter(
                    x=indicator_series.index,
                    y=indicator_series.values,
                    mode="lines",
                    name=config.indicator_name,
                    line={"color": "#1f77b4", "width": 2},
                )
            )
            fig_indicator.update_layout(
                title=f"{config.indicator_name} Indicator",
                xaxis_title="Time",
                yaxis_title=config.indicator_name,
                height=300,
            )
            st.plotly_chart(fig_indicator, use_container_width=True)


def _render_signal_status(
    session_state: dict[str, Any],
    strategy_config: dict[str, Any],
    config: TADashboardConfig,
) -> None:
    """Render the signal status section."""
    st.subheader("Signal Status")

    # Get indicator value
    indicator_key = config.indicator_name.lower()
    indicator_value = float(session_state.get(f"{indicator_key}_value", session_state.get(indicator_key, 50)))

    # Use custom signal function if provided
    if config.custom_signal_fn is not None:
        signal = config.custom_signal_fn(session_state)
        if "buy" in signal.lower() or "bullish" in signal.lower():
            st.success(signal)
        elif "sell" in signal.lower() or "bearish" in signal.lower():
            st.error(signal)
        else:
            st.info(signal)
        return

    # Default signal logic
    if config.upper_threshold is not None and config.lower_threshold is not None:
        if config.signal_type == "reversion":
            # Reversion: buy on low values, sell on high values
            if indicator_value < config.lower_threshold:
                st.success(
                    f"BUY SIGNAL: {config.indicator_name} ({indicator_value:.1f}) < {config.lower_threshold} (Oversold)"
                )
            elif indicator_value > config.upper_threshold:
                st.error(
                    f"SELL SIGNAL: {config.indicator_name} ({indicator_value:.1f}) > {config.upper_threshold} (Overbought)"
                )
            else:
                st.info(f"NEUTRAL: {config.indicator_name} ({indicator_value:.1f}) in normal range")
        else:
            # Momentum: buy on high values, sell on low values
            if indicator_value > config.upper_threshold:
                st.success(
                    f"BUY SIGNAL: {config.indicator_name} ({indicator_value:.1f}) > {config.upper_threshold} (Strong momentum)"
                )
            elif indicator_value < config.lower_threshold:
                st.error(
                    f"SELL SIGNAL: {config.indicator_name} ({indicator_value:.1f}) < {config.lower_threshold} (Weak momentum)"
                )
            else:
                st.info(f"NEUTRAL: {config.indicator_name} ({indicator_value:.1f}) in normal range")
    else:
        # No thresholds - just display value
        st.info(f"Current {config.indicator_name}: {indicator_value:.1f}")


def _render_position(
    session_state: dict[str, Any],
    base_token: str,
    quote_token: str,
) -> None:
    """Render the current position section."""
    base_balance = Decimal(str(session_state.get("base_balance", "0")))
    quote_balance = Decimal(str(session_state.get("quote_balance", "0")))

    # Get price from session state or use default
    base_price = Decimal(str(session_state.get("base_price", "1")))
    total_value = base_balance * base_price + quote_balance

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric(f"{base_token}", f"{float(base_balance):.4f}")
    with col2:
        st.metric(f"{quote_token}", f"${float(quote_balance):,.2f}")
    with col3:
        st.metric("Total", f"${float(total_value):,.2f}")


def _render_performance(session_state: dict[str, Any]) -> None:
    """Render the performance metrics section."""
    pnl = Decimal(str(session_state.get("total_pnl", "0")))
    trades = session_state.get("total_trades", 0)
    win_rate = Decimal(str(session_state.get("win_rate", "50")))

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("PnL", f"${float(pnl):+,.2f}")
    with col2:
        st.metric("Trades", str(trades))
    with col3:
        st.metric("Win Rate", f"{float(win_rate):.0f}%")


# Pre-configured templates for common indicators


def get_rsi_config(period: int = 14, overbought: float = 70, oversold: float = 30) -> TADashboardConfig:
    """Get pre-configured RSI dashboard config."""
    return TADashboardConfig(
        indicator_name="RSI",
        indicator_period=period,
        upper_threshold=overbought,
        lower_threshold=oversold,
        signal_type="reversion",
        value_suffix="",
        show_progress_bar=True,
        progress_range=(0, 100),
    )


def get_macd_config(fast: int = 12, slow: int = 26, signal: int = 9) -> TADashboardConfig:
    """Get pre-configured MACD dashboard config."""
    return TADashboardConfig(
        indicator_name="MACD",
        indicator_period=fast,
        secondary_periods=[slow, signal],
        signal_type="momentum",
        value_format="{:+.2f}",
        show_progress_bar=False,
    )


def get_cci_config(period: int = 20, overbought: float = 100, oversold: float = -100) -> TADashboardConfig:
    """Get pre-configured CCI dashboard config."""
    return TADashboardConfig(
        indicator_name="CCI",
        indicator_period=period,
        upper_threshold=overbought,
        lower_threshold=oversold,
        signal_type="reversion",
        value_format="{:+.1f}",
        show_progress_bar=True,
        progress_range=(-200, 200),
    )


def get_stochastic_config(
    fast_k: int = 14, slow_k: int = 3, slow_d: int = 3, overbought: float = 80, oversold: float = 20
) -> TADashboardConfig:
    """Get pre-configured Stochastic dashboard config."""
    return TADashboardConfig(
        indicator_name="Stochastic",
        indicator_period=fast_k,
        secondary_periods=[slow_k, slow_d],
        upper_threshold=overbought,
        lower_threshold=oversold,
        signal_type="reversion",
        value_suffix="%",
        show_progress_bar=True,
        progress_range=(0, 100),
    )


def get_atr_config(period: int = 14) -> TADashboardConfig:
    """Get pre-configured ATR dashboard config."""
    return TADashboardConfig(
        indicator_name="ATR",
        indicator_period=period,
        signal_type="momentum",
        value_format="${:.2f}",
        show_progress_bar=False,
    )


def get_adx_config(period: int = 14, trend_threshold: float = 25) -> TADashboardConfig:
    """Get pre-configured ADX dashboard config."""
    return TADashboardConfig(
        indicator_name="ADX",
        indicator_period=period,
        lower_threshold=trend_threshold,
        signal_type="momentum",
        show_progress_bar=True,
        progress_range=(0, 100),
    )


def get_bollinger_config(period: int = 20, std_dev: float = 2.0) -> TADashboardConfig:
    """Get pre-configured Bollinger Bands dashboard config."""
    return TADashboardConfig(
        indicator_name="Bollinger",
        indicator_period=period,
        secondary_periods=[int(std_dev * 10)],  # Encode std_dev
        signal_type="reversion",
        value_format="${:.2f}",
        show_progress_bar=False,
    )
