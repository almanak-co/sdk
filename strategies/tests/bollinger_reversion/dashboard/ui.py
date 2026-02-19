"""
Bollinger Bands Reversion Strategy Dashboard.

Custom dashboard showing upper/middle/lower bands, price position,
band width, current position, and PnL metrics.
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
    """Render the Bollinger Reversion custom dashboard."""
    st.title("Bollinger Bands Reversion Dashboard")

    # Extract config
    bb_period = strategy_config.get("bb_period", 20)
    bb_std_dev = strategy_config.get("bb_std_dev", 2.0)
    base_token = strategy_config.get("base_token", "WETH")
    quote_token = strategy_config.get("quote_token", "USDC")

    st.markdown(f"**Strategy ID:** `{strategy_id}`")
    st.markdown(f"**Pair:** {base_token}/{quote_token}")
    st.markdown("**Chain:** Arbitrum | **Protocol:** Uniswap V3")

    st.divider()

    # Bollinger Bands
    st.subheader(f"Bollinger Bands({bb_period}, {bb_std_dev})")
    upper = Decimal(str(session_state.get("upper_band", "3600")))
    middle = Decimal(str(session_state.get("middle_band", "3400")))
    lower = Decimal(str(session_state.get("lower_band", "3200")))
    price = Decimal(str(session_state.get("current_price", "3400")))

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Upper Band", f"${float(upper):,.0f}")
    with col2:
        st.metric("Middle (SMA)", f"${float(middle):,.0f}")
    with col3:
        st.metric("Lower Band", f"${float(lower):,.0f}")

    col4, col5 = st.columns(2)
    with col4:
        st.metric("Current Price", f"${float(price):,.0f}")
    with col5:
        band_width = (upper - lower) / middle * Decimal("100")
        st.metric("Band Width", f"{float(band_width):.1f}%")

    if price <= lower:
        st.success("Zone: AT LOWER BAND - Buy signal (oversold)")
    elif price >= upper:
        st.error("Zone: AT UPPER BAND - Sell signal (overbought)")
    else:
        st.info("Zone: WITHIN BANDS - Hold")

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
    base_price = Decimal("3400")
    total_value = base_balance * base_price + quote_balance

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric(f"{base_token}", f"{float(base_balance):.4f}")
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
