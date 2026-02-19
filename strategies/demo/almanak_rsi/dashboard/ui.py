"""
ALMANAK RSI Strategy Dashboard.

Custom dashboard showing:
- RSI indicator over time with buy/sell zones and signal markers
- Price chart with signal markers
- Performance metrics (PnL, trades, position state)
- Trade history

Uses the Almanak dashboard framework for consistent Plotly visualizations.
"""

from decimal import Decimal
from typing import Any

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

from almanak.framework.dashboard.plots.base import get_default_config


def render_custom_dashboard(
    strategy_id: str,
    strategy_config: dict[str, Any],
    api_client: Any,
    session_state: dict[str, Any],
) -> None:
    """Render the ALMANAK RSI custom dashboard.

    Shows:
    - Strategy info header
    - RSI chart with buy/sell signal markers overlaid
    - Price chart with buy/sell markers
    - Current RSI gauge
    - Performance metrics
    - Trade history
    """
    st.title("ALMANAK RSI Strategy Dashboard")

    # Extract config values
    base_token = strategy_config.get("base_token", "ALMANAK")
    quote_token = strategy_config.get("quote_token", "USDC")
    pool_address = strategy_config.get("pool_address", "0xbDbC386...")
    rsi_oversold = float(strategy_config.get("rsi_oversold", 30))
    rsi_overbought = float(strategy_config.get("rsi_overbought", 70))
    rsi_period = strategy_config.get("rsi_period", 14)
    cooldown_hours = strategy_config.get("cooldown_hours", 1)

    # Strategy info header
    st.markdown(f"**Strategy ID:** `{strategy_id}`")
    st.markdown(f"**Trading Pair:** {base_token}/{quote_token}")
    st.markdown(f"**Pool:** `{pool_address[:16]}...{pool_address[-8:]}`")
    st.markdown("**DEX:** Uniswap V3")
    st.markdown("**Chain:** Base")
    st.markdown("**Data Source:** CoinGecko DEX (GeckoTerminal)")

    st.divider()

    # Main visualization: RSI + Price with signals
    st.subheader("RSI Indicator with Buy/Sell Signals")
    _render_rsi_chart_with_signals(
        session_state,
        rsi_period,
        rsi_oversold,
        rsi_overbought,
        base_token,
    )

    st.divider()

    # Current RSI status
    st.subheader("Current RSI Status")
    _render_rsi_gauge(session_state, rsi_period, rsi_oversold, rsi_overbought, base_token)

    st.divider()

    # Performance Metrics section
    st.subheader("Key Performance Metrics")
    _render_performance_metrics(session_state, strategy_config)

    st.divider()

    # Current Position section
    st.subheader("Current Position State")
    _render_current_position(session_state, base_token, quote_token, cooldown_hours)

    st.divider()

    # Trade History section
    st.subheader("Recent Trades")
    _render_trade_history(api_client, strategy_id)


def _render_rsi_chart_with_signals(
    session_state: dict[str, Any],
    rsi_period: int,
    rsi_oversold: float,
    rsi_overbought: float,
    base_token: str,
) -> None:
    """Render RSI chart over time with buy/sell signal markers overlaid.

    This is the main visualization showing:
    - RSI line over time (blue)
    - Horizontal threshold lines (overbought=red dashed, oversold=green dashed)
    - Shaded zones (green below oversold, red above overbought)
    - BUY signals as green triangles pointing UP
    - SELL signals as red triangles pointing DOWN
    """
    price_history = session_state.get("price_history", [])
    signal_history = session_state.get("signal_history", [])

    if not price_history:
        st.info(
            "No RSI data yet. The chart will appear after the strategy collects data. "
            "RSI requires multiple price points to calculate."
        )
        return

    try:
        # Get theme colors from framework
        config = get_default_config()
        colors = config.colors

        # Convert price history to DataFrame
        df = pd.DataFrame(price_history)
        df["timestamp"] = pd.to_datetime(df["timestamp"])

        # Convert signals to DataFrames
        buy_signals = []
        sell_signals = []
        for sig in signal_history:
            sig_time = pd.to_datetime(sig["timestamp"])
            sig_rsi = float(sig["rsi"])
            if sig["signal"] == "BUY":
                buy_signals.append({"timestamp": sig_time, "rsi": sig_rsi})
            else:
                sell_signals.append({"timestamp": sig_time, "rsi": sig_rsi})

        buy_df = pd.DataFrame(buy_signals) if buy_signals else pd.DataFrame()
        sell_df = pd.DataFrame(sell_signals) if sell_signals else pd.DataFrame()

        # Create subplot: Price on top, RSI on bottom
        fig = make_subplots(
            rows=2,
            cols=1,
            shared_xaxes=True,
            vertical_spacing=0.08,
            row_heights=[0.5, 0.5],
            subplot_titles=(f"{base_token} Price with Signals", f"RSI({rsi_period}) with Signals"),
        )

        # =====================================================================
        # ROW 1: PRICE CHART WITH BUY/SELL SIGNALS
        # =====================================================================

        # Price line
        fig.add_trace(
            go.Scatter(
                x=df["timestamp"],
                y=df["price"],
                mode="lines",
                name="Price",
                line={"color": colors.primary, "width": 2},
                hovertemplate="Price: $%{y:.4f}<extra></extra>",
            ),
            row=1,
            col=1,
        )

        # Buy signals on price chart (find corresponding price)
        if not buy_df.empty:
            buy_prices = []
            for _, sig in buy_df.iterrows():
                # Find closest price to signal timestamp
                idx = (df["timestamp"] - sig["timestamp"]).abs().argmin()
                buy_prices.append(df.iloc[idx]["price"])

            fig.add_trace(
                go.Scatter(
                    x=buy_df["timestamp"],
                    y=buy_prices,
                    mode="markers",
                    name="Buy Signal",
                    marker={
                        "symbol": "triangle-up",
                        "size": 15,
                        "color": colors.success,
                        "line": {"width": 2, "color": "#1E8449"},
                    },
                    hovertemplate="BUY @ $%{y:.4f}<extra></extra>",
                ),
                row=1,
                col=1,
            )

        # Sell signals on price chart
        if not sell_df.empty:
            sell_prices = []
            for _, sig in sell_df.iterrows():
                idx = (df["timestamp"] - sig["timestamp"]).abs().argmin()
                sell_prices.append(df.iloc[idx]["price"])

            fig.add_trace(
                go.Scatter(
                    x=sell_df["timestamp"],
                    y=sell_prices,
                    mode="markers",
                    name="Sell Signal",
                    marker={
                        "symbol": "triangle-down",
                        "size": 15,
                        "color": colors.danger,
                        "line": {"width": 2, "color": "#C0392B"},
                    },
                    hovertemplate="SELL @ $%{y:.4f}<extra></extra>",
                ),
                row=1,
                col=1,
            )

        # =====================================================================
        # ROW 2: RSI CHART WITH BUY/SELL SIGNALS
        # =====================================================================

        # RSI line
        fig.add_trace(
            go.Scatter(
                x=df["timestamp"],
                y=df["rsi"],
                mode="lines",
                name="RSI",
                line={"color": colors.secondary, "width": 2},
                hovertemplate="RSI: %{y:.1f}<extra></extra>",
            ),
            row=2,
            col=1,
        )

        # Add overbought threshold line
        fig.add_hline(
            y=rsi_overbought,
            line_dash="dash",
            line_color=colors.danger,
            line_width=1,
            annotation_text=f"Overbought ({rsi_overbought})",
            annotation_position="top right",
            row=2,
            col=1,
        )

        # Add oversold threshold line
        fig.add_hline(
            y=rsi_oversold,
            line_dash="dash",
            line_color=colors.success,
            line_width=1,
            annotation_text=f"Oversold ({rsi_oversold})",
            annotation_position="bottom right",
            row=2,
            col=1,
        )

        # Add neutral line at 50
        fig.add_hline(
            y=50,
            line_dash="dot",
            line_color=colors.neutral,
            line_width=1,
            row=2,
            col=1,
        )

        # Add shaded zones for overbought/oversold
        fig.add_hrect(
            y0=rsi_overbought,
            y1=100,
            fillcolor=colors.danger,
            opacity=0.1,
            line_width=0,
            row=2,
            col=1,
        )
        fig.add_hrect(
            y0=0,
            y1=rsi_oversold,
            fillcolor=colors.success,
            opacity=0.1,
            line_width=0,
            row=2,
            col=1,
        )

        # Buy signals on RSI chart (green triangles UP)
        if not buy_df.empty:
            fig.add_trace(
                go.Scatter(
                    x=buy_df["timestamp"],
                    y=buy_df["rsi"],
                    mode="markers",
                    name="Buy (RSI)",
                    marker={
                        "symbol": "triangle-up",
                        "size": 15,
                        "color": colors.success,
                        "line": {"width": 2, "color": "#1E8449"},
                    },
                    hovertemplate="BUY @ RSI %{y:.1f}<extra></extra>",
                    showlegend=False,
                ),
                row=2,
                col=1,
            )

        # Sell signals on RSI chart (red triangles DOWN)
        if not sell_df.empty:
            fig.add_trace(
                go.Scatter(
                    x=sell_df["timestamp"],
                    y=sell_df["rsi"],
                    mode="markers",
                    name="Sell (RSI)",
                    marker={
                        "symbol": "triangle-down",
                        "size": 15,
                        "color": colors.danger,
                        "line": {"width": 2, "color": "#C0392B"},
                    },
                    hovertemplate="SELL @ RSI %{y:.1f}<extra></extra>",
                    showlegend=False,
                ),
                row=2,
                col=1,
            )

        # Update layout
        fig.update_xaxes(title_text="Time", row=2, col=1)
        fig.update_yaxes(title_text="Price (USD)", row=1, col=1)
        fig.update_yaxes(title_text="RSI", range=[0, 100], row=2, col=1)

        fig.update_layout(
            height=700,
            hovermode="x unified",
            showlegend=True,
            legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "xanchor": "right", "x": 1},
            template="plotly_dark",
        )

        st.plotly_chart(fig, use_container_width=True)

        # Show signal summary
        if signal_history:
            st.caption(f"Total signals: {len(buy_signals)} buys, {len(sell_signals)} sells")

    except ImportError:
        st.warning("Plotly required for charts. Install with: pip install plotly")
        _render_fallback_chart(session_state)
    except Exception as e:
        st.error(f"Error rendering chart: {e}")
        _render_fallback_chart(session_state)


def _render_fallback_chart(session_state: dict[str, Any]) -> None:
    """Render basic fallback chart using st.line_chart."""
    price_history = session_state.get("price_history", [])
    if not price_history:
        return

    try:
        df = pd.DataFrame(price_history)
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df.set_index("timestamp")

        tab1, tab2 = st.tabs(["Price", "RSI"])
        with tab1:
            st.line_chart(df["price"], use_container_width=True)
        with tab2:
            st.line_chart(df["rsi"], use_container_width=True)
            st.caption("Oversold: < 30 | Overbought: > 70")
    except Exception:
        st.info("Unable to render chart")


def _render_rsi_gauge(
    session_state: dict[str, Any],
    rsi_period: int,
    rsi_oversold: float,
    rsi_overbought: float,
    base_token: str,
) -> None:
    """Render current RSI value with gauge and zone indicator."""
    current_rsi = float(session_state.get("current_rsi", 50))

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            f"RSI({rsi_period})",
            f"{current_rsi:.1f}",
            help="Current RSI value (0-100)",
        )

    with col2:
        st.metric(
            "Oversold Level",
            f"< {rsi_oversold:.0f}",
            help="RSI below this triggers BUY signal",
        )

    with col3:
        st.metric(
            "Overbought Level",
            f"> {rsi_overbought:.0f}",
            help="RSI above this triggers SELL signal",
        )

    # Zone indicator with color
    if current_rsi <= rsi_oversold:
        st.success(f"OVERSOLD ZONE - Buy {base_token} signal active")
    elif current_rsi >= rsi_overbought:
        st.error(f"OVERBOUGHT ZONE - Sell {base_token} signal active")
    else:
        st.info("NEUTRAL ZONE - Holding position")

    # RSI progress bar visualization
    col1, col2 = st.columns([3, 1])
    with col1:
        st.progress(current_rsi / 100)
    with col2:
        st.markdown(f"**{current_rsi:.0f}/100**")


def _render_performance_metrics(
    session_state: dict[str, Any],
    strategy_config: dict[str, Any],
) -> None:
    """Render key performance metrics."""
    total_pnl_usd = Decimal(str(session_state.get("total_pnl_usd", "0")))
    total_pnl_eth = Decimal(str(session_state.get("total_pnl_eth", "0")))
    initial_value = Decimal(str(strategy_config.get("initial_capital_usdc", "20")))
    current_value = Decimal(str(session_state.get("current_value_usd", initial_value)))
    total_trades = session_state.get("trade_count", 0)

    # Calculate PnL percentage
    if initial_value > 0:
        pnl_pct = ((current_value - initial_value) / initial_value) * Decimal("100")
    else:
        pnl_pct = Decimal("0")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        pnl_sign = "+" if total_pnl_usd >= 0 else "-"
        st.metric(
            "Net P&L (USD)",
            f"{pnl_sign}${float(abs(total_pnl_usd)):,.2f}",
            delta=f"{pnl_sign}{float(abs(total_pnl_usd)):,.2f}" if total_pnl_usd != 0 else None,
            help="Total profit/loss in USD",
        )

    with col2:
        pnl_sign = "+" if total_pnl_eth >= 0 else "-"
        st.metric(
            "Net P&L (ETH)",
            f"{pnl_sign}{float(abs(total_pnl_eth)):.6f}",
            help="Total profit/loss in ETH",
        )

    with col3:
        pnl_sign = "+" if pnl_pct >= 0 else ""
        st.metric(
            "Net P&L (%)",
            f"{pnl_sign}{float(pnl_pct):.2f}%",
            delta=f"{float(pnl_pct):.2f}%" if pnl_pct != 0 else None,
            help="Total return percentage",
        )

    with col4:
        st.metric(
            "Total Trades",
            str(total_trades),
            help="Number of executed trades",
        )

    # Portfolio value
    st.markdown(f"**Current Portfolio Value:** ${float(current_value):,.2f}")
    st.markdown(f"**Initial Capital:** ${float(initial_value):,.2f}")


def _render_current_position(
    session_state: dict[str, Any],
    base_token: str,
    quote_token: str,
    cooldown_hours: int,
) -> None:
    """Render current position state."""
    initialized = session_state.get("initialized", False)
    can_trade = session_state.get("can_trade", True)
    cooldown_remaining = session_state.get("cooldown_remaining_min", 0)
    base_balance = Decimal(str(session_state.get("base_balance", "0")))
    quote_balance = Decimal(str(session_state.get("quote_balance", "0")))
    base_price = Decimal(str(session_state.get("base_price", "0")))

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            f"{base_token} Balance",
            f"{float(base_balance):,.4f}",
            help=f"Current {base_token} holdings",
        )

    with col2:
        st.metric(
            f"{quote_token} Balance",
            f"${float(quote_balance):,.2f}",
            help=f"Current {quote_token} holdings",
        )

    with col3:
        total_value = (base_balance * base_price) + quote_balance
        st.metric(
            "Total Value",
            f"${float(total_value):,.2f}",
            help="Total portfolio value in USD",
        )

    # Status indicators
    col1, col2 = st.columns(2)

    with col1:
        if initialized:
            st.success("Strategy Initialized")
        else:
            st.warning("Awaiting Initialization (first buy pending)")

    with col2:
        if can_trade:
            st.success("Ready to Trade")
        else:
            st.warning(f"Cooldown Active ({cooldown_remaining:.0f} min remaining)")


def _render_trade_history(api_client: Any, strategy_id: str) -> None:
    """Render recent trade history."""
    trades = []

    if api_client:
        try:
            events = api_client.get_timeline(strategy_id, limit=20)
            trades = [e for e in events if e.get("event_type") in ["SWAP", "swap", "INTENT_EXECUTED"]]
        except Exception as exc:
            st.warning(f"Unable to load trade history: {exc}")
            return

    if trades:
        for trade in trades[:10]:
            timestamp = str(trade.get("timestamp", "N/A"))
            details = trade.get("details", {}) or {}
            from_token = str(details.get("from_token", "?"))
            to_token = str(details.get("to_token", "?"))
            amount = details.get("amount", "?")

            # Determine trade type
            if "USDC" in from_token or "usdc" in from_token.lower():
                emoji = "📈"
                trade_type = "BUY"
            else:
                emoji = "📉"
                trade_type = "SELL"

            ts_display = timestamp[:19] if len(timestamp) > 19 else timestamp
            st.markdown(
                f"- {emoji} **{trade_type}** `{ts_display}`: "
                f"{from_token} -> {to_token} (amount: {amount})"
            )
    else:
        st.info("No trades executed yet. Trades will appear here after RSI signals are triggered.")
