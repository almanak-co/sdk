"""
OBV Divergence Strategy Dashboard.

Custom dashboard showing On-Balance Volume, signal line,
volume momentum, and PnL metrics.
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
    """Render the OBV Divergence custom dashboard."""
    st.title("OBV Divergence Strategy Dashboard")

    # Extract config
    signal_period = strategy_config.get("obv_signal_period", 21)
    base_token = strategy_config.get("base_token", "WETH")
    quote_token = strategy_config.get("quote_token", "USDC")

    st.markdown(f"**Strategy ID:** `{strategy_id}`")
    st.markdown(f"**Pair:** {base_token}/{quote_token}")
    st.markdown("**Chain:** Arbitrum | **Protocol:** Uniswap V3")

    st.divider()

    # OBV Indicator
    st.subheader(f"On-Balance Volume (Signal: SMA {signal_period})")

    obv = float(session_state.get("obv", 1000000))
    signal = float(session_state.get("obv_signal", 950000))

    col1, col2 = st.columns(2)
    with col1:
        st.metric("OBV", f"{obv:,.0f}")
    with col2:
        st.metric("Signal Line", f"{signal:,.0f}")

    # OBV vs Signal difference
    obv_diff = obv - signal
    obv_diff_pct = (obv_diff / signal) * 100 if signal != 0 else 0

    col3, col4 = st.columns(2)
    with col3:
        st.metric("OBV - Signal", f"{obv_diff:+,.0f}")
    with col4:
        st.metric("Divergence %", f"{obv_diff_pct:+.2f}%")

    st.divider()

    # Momentum status
    st.subheader("Volume Momentum")
    if obv > signal:
        st.success(f"BULLISH: OBV ({obv:,.0f}) > Signal ({signal:,.0f}) - Buying pressure dominant")
    elif obv < signal:
        st.error(f"BEARISH: OBV ({obv:,.0f}) < Signal ({signal:,.0f}) - Selling pressure dominant")
    else:
        st.info("OBV at Signal - Neutral momentum")

    # Volume trend strength
    abs_diff_pct = abs(obv_diff_pct)
    if abs_diff_pct > 5:
        st.warning("Strong volume divergence detected!")
    elif abs_diff_pct > 2:
        st.info("Moderate volume divergence")
    else:
        st.info("Weak volume divergence - wait for confirmation")

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
    base_price = Decimal("3400")  # WETH price
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
