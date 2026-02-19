"""
ATR Volatility Gate Strategy Dashboard.

Custom dashboard showing ATR value, ATR percentage, volatility gate status,
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
    """Render the ATR Volatility Gate custom dashboard."""
    st.title("ATR Volatility Gate Strategy Dashboard")

    # Extract config
    atr_period = strategy_config.get("atr_period", 14)
    max_volatility = Decimal(str(strategy_config.get("max_volatility_threshold", "5")))
    base_token = strategy_config.get("base_token", "WETH")
    quote_token = strategy_config.get("quote_token", "USDC")

    st.markdown(f"**Strategy ID:** `{strategy_id}`")
    st.markdown(f"**Pair:** {base_token}/{quote_token}")
    st.markdown("**Chain:** Arbitrum | **Protocol:** Uniswap V3")

    st.divider()

    # ATR Indicator
    st.subheader(f"ATR({atr_period})")
    atr_value = Decimal(str(session_state.get("atr_value", "100")))
    atr_pct = Decimal(str(session_state.get("atr_pct", "3")))

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("ATR Value", f"${float(atr_value):.2f}")
    with col2:
        st.metric("ATR %", f"{float(atr_pct):.2f}%")
    with col3:
        st.metric("Max Volatility", f"{float(max_volatility):.1f}%")

    # Volatility gate status
    if atr_pct <= max_volatility:
        st.success(f"Gate: OPEN - Volatility acceptable ({float(atr_pct):.1f}% < {float(max_volatility):.1f}%)")
    else:
        st.error(f"Gate: CLOSED - Volatility too high ({float(atr_pct):.1f}% > {float(max_volatility):.1f}%)")

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
