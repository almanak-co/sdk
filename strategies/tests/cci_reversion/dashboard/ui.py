"""
CCI Reversion Strategy Dashboard.

Custom dashboard showing CCI value, oversold/overbought levels,
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
    """Render the CCI Reversion custom dashboard."""
    st.title("CCI Reversion Strategy Dashboard")

    # Extract config
    cci_period = strategy_config.get("cci_period", 20)
    upper_level = strategy_config.get("cci_upper_level", 100)
    lower_level = strategy_config.get("cci_lower_level", -100)
    base_token = strategy_config.get("base_token", "ARB")
    quote_token = strategy_config.get("quote_token", "USDC")

    st.markdown(f"**Strategy ID:** `{strategy_id}`")
    st.markdown(f"**Pair:** {base_token}/{quote_token}")
    st.markdown("**Chain:** Arbitrum | **Protocol:** Uniswap V3")

    st.divider()

    # CCI Indicator
    st.subheader(f"CCI({cci_period})")
    cci_value = float(session_state.get("cci_value", 0))

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("CCI Value", f"{cci_value:.1f}")
    with col2:
        st.metric("Upper Level", str(upper_level))
    with col3:
        st.metric("Lower Level", str(lower_level))

    # Zone status
    if cci_value < lower_level:
        st.success(f"Zone: OVERSOLD - Buy opportunity (CCI < {lower_level})")
    elif cci_value > upper_level:
        st.error(f"Zone: OVERBOUGHT - Sell opportunity (CCI > {upper_level})")
    else:
        st.info(f"Zone: NEUTRAL - No signal ({lower_level} < CCI < {upper_level})")

    # CCI progress visualization
    normalized_cci = (cci_value + 200) / 400  # Normalize to 0-1 range
    normalized_cci = max(0, min(1, normalized_cci))
    st.progress(normalized_cci, text=f"CCI Position: {cci_value:.1f}")

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
