"""
ADX Trend Filter Strategy Dashboard.

Custom dashboard showing ADX value, DI+ and DI-, trend strength,
current position, and PnL metrics.
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
    """Render the ADX Trend Filter custom dashboard."""
    st.title("ADX Trend Filter Strategy Dashboard")

    # Extract config
    adx_period = strategy_config.get("adx_period", 14)
    trend_threshold = strategy_config.get("trend_threshold", 25)
    base_token = strategy_config.get("base_token", "LINK")
    quote_token = strategy_config.get("quote_token", "USDC")

    st.markdown(f"**Strategy ID:** `{strategy_id}`")
    st.markdown(f"**Pair:** {base_token}/{quote_token}")
    st.markdown("**Chain:** Arbitrum | **Protocol:** Uniswap V3")

    st.divider()

    # ADX Indicator
    st.subheader(f"ADX({adx_period})")
    adx = Decimal(str(session_state.get("adx", "20")))
    plus_di = Decimal(str(session_state.get("plus_di", "25")))
    minus_di = Decimal(str(session_state.get("minus_di", "15")))

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("ADX", f"{float(adx):.1f}")
    with col2:
        st.metric("+DI", f"{float(plus_di):.1f}")
    with col3:
        st.metric("-DI", f"{float(minus_di):.1f}")

    # Trend strength
    if adx < Decimal(str(trend_threshold)):
        st.info(f"Trend: WEAK (ADX < {trend_threshold}) - No trade")
    elif plus_di > minus_di:
        st.success("Trend: STRONG UPTREND (+DI > -DI) - Buy signal")
    else:
        st.error("Trend: STRONG DOWNTREND (-DI > +DI) - Sell signal")

    col4, col5 = st.columns(2)
    with col4:
        st.metric("Trend Threshold", str(trend_threshold))
    with col5:
        strength = "Strong" if adx >= Decimal(str(trend_threshold)) else "Weak"
        st.metric("Trend Strength", strength)

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
    base_price = Decimal("14")  # LINK price
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
