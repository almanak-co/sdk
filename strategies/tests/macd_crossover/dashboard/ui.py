"""
MACD Crossover Strategy Dashboard.

Custom dashboard showing MACD line, signal line, histogram,
crossover status, current position, and PnL metrics.
"""

from decimal import Decimal
from typing import Any

import streamlit as st


def render_custom_dashboard(
    strategy_id: str,
    strategy_config: dict[str, Any],
    api_client: Any,
    session_state: dict[str, Any],
) -> None:
    """Render the MACD Crossover custom dashboard."""
    st.title("MACD Crossover Strategy Dashboard")

    # Extract config
    macd_fast = strategy_config.get("macd_fast", 12)
    macd_slow = strategy_config.get("macd_slow", 26)
    macd_signal = strategy_config.get("macd_signal", 9)
    base_token = strategy_config.get("base_token", "ARB")
    quote_token = strategy_config.get("quote_token", "USDC")

    st.markdown(f"**Strategy ID:** `{strategy_id}`")
    st.markdown(f"**Pair:** {base_token}/{quote_token}")
    st.markdown("**Chain:** Arbitrum | **Protocol:** Uniswap V3")

    st.divider()

    # MACD Indicator
    st.subheader(f"MACD({macd_fast},{macd_slow},{macd_signal})")
    macd_line = Decimal(str(session_state.get("macd_line", "0")))
    signal_line = Decimal(str(session_state.get("signal_line", "0")))
    histogram = macd_line - signal_line

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("MACD Line", f"{float(macd_line):.4f}")
    with col2:
        st.metric("Signal Line", f"{float(signal_line):.4f}")
    with col3:
        delta_color = "normal" if histogram >= 0 else "inverse"
        st.metric("Histogram", f"{float(histogram):+.4f}", delta_color=delta_color)

    if histogram > 0:
        st.success("Crossover: BULLISH - Buy signal")
    elif histogram < 0:
        st.error("Crossover: BEARISH - Sell signal")
    else:
        st.info("Crossover: NEUTRAL")

    st.divider()

    # Position
    st.subheader("Current Position")
    _render_position(session_state, base_token, quote_token)

    st.divider()

    # PnL
    st.subheader("Performance")
    _render_pnl(session_state)


def _render_position(session_state: dict[str, Any], base_token: str, quote_token: str) -> None:
    base_balance = Decimal(str(session_state.get("base_balance", "0")))
    quote_balance = Decimal(str(session_state.get("quote_balance", "0")))
    base_price = Decimal("1.2")  # ARB price
    total_value = base_balance * base_price + quote_balance

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric(f"{base_token}", f"{float(base_balance):.2f}")
    with col2:
        st.metric(f"{quote_token}", f"${float(quote_balance):,.2f}")
    with col3:
        st.metric("Total", f"${float(total_value):,.2f}")


def _render_pnl(session_state: dict[str, Any]) -> None:
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
