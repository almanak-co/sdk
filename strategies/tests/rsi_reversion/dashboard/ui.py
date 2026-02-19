"""
RSI Reversion Strategy Dashboard.

Custom dashboard showing RSI indicator, thresholds, zone indicator,
current position, signal history, and PnL metrics.
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
    """Render the RSI Reversion custom dashboard."""
    st.title("RSI Reversion Strategy Dashboard")

    # Extract config
    rsi_period = strategy_config.get("rsi_period", 14)
    rsi_oversold = Decimal(str(strategy_config.get("rsi_oversold", "30")))
    rsi_overbought = Decimal(str(strategy_config.get("rsi_overbought", "70")))
    base_token = strategy_config.get("base_token", "WETH")
    quote_token = strategy_config.get("quote_token", "USDC")

    st.markdown(f"**Strategy ID:** `{strategy_id}`")
    st.markdown(f"**Pair:** {base_token}/{quote_token}")
    st.markdown("**Chain:** Arbitrum | **Protocol:** Uniswap V3")

    st.divider()

    # RSI Indicator
    st.subheader("RSI Indicator")
    current_rsi = Decimal(str(session_state.get("current_rsi", "50")))

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric(f"RSI({rsi_period})", f"{float(current_rsi):.1f}")
    with col2:
        st.metric("Oversold", f"< {float(rsi_oversold):.0f}")
    with col3:
        st.metric("Overbought", f"> {float(rsi_overbought):.0f}")

    if current_rsi <= rsi_oversold:
        st.success("Zone: OVERSOLD - Buy signal")
    elif current_rsi >= rsi_overbought:
        st.error("Zone: OVERBOUGHT - Sell signal")
    else:
        st.info("Zone: NEUTRAL - Hold")

    st.progress(float(current_rsi) / 100)

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
