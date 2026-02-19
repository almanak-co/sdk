"""
Stochastic Reversion Strategy Dashboard.

Custom dashboard showing %K and %D lines, overbought/oversold zones,
crossover signals, and PnL metrics.
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
    """Render the Stochastic Reversion custom dashboard."""
    st.title("Stochastic Reversion Strategy Dashboard")

    # Extract config
    fast_k = strategy_config.get("stoch_fast_k", 14)
    slow_k = strategy_config.get("stoch_slow_k", 3)
    slow_d = strategy_config.get("stoch_slow_d", 3)
    overbought = strategy_config.get("overbought", 80)
    oversold = strategy_config.get("oversold", 20)
    base_token = strategy_config.get("base_token", "ARB")
    quote_token = strategy_config.get("quote_token", "USDC")

    st.markdown(f"**Strategy ID:** `{strategy_id}`")
    st.markdown(f"**Pair:** {base_token}/{quote_token}")
    st.markdown("**Chain:** Arbitrum | **Protocol:** Uniswap V3")

    st.divider()

    # Stochastic Indicator
    st.subheader(f"Stochastic Oscillator ({fast_k}, {slow_k}, {slow_d})")

    percent_k = float(session_state.get("percent_k", 50))
    percent_d = float(session_state.get("percent_d", 48))

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("%K", f"{percent_k:.1f}")
    with col2:
        st.metric("%D", f"{percent_d:.1f}")
    with col3:
        st.metric("Overbought", str(overbought))
    with col4:
        st.metric("Oversold", str(oversold))

    # Progress bars for K and D
    st.markdown("**%K Position:**")
    st.progress(percent_k / 100, text=f"%K: {percent_k:.1f}")
    st.markdown("**%D Position:**")
    st.progress(percent_d / 100, text=f"%D: {percent_d:.1f}")

    st.divider()

    # Zone and signal status
    st.subheader("Signal Status")

    # Determine zone
    in_oversold = percent_k < oversold or percent_d < oversold
    in_overbought = percent_k > overbought or percent_d > overbought
    k_above_d = percent_k > percent_d

    if in_oversold:
        if k_above_d:
            st.success(f"BUY SIGNAL: %K ({percent_k:.1f}) > %D ({percent_d:.1f}) in OVERSOLD zone")
        else:
            st.warning("OVERSOLD zone - waiting for %K to cross above %D")
    elif in_overbought:
        if not k_above_d:
            st.error(f"SELL SIGNAL: %K ({percent_k:.1f}) < %D ({percent_d:.1f}) in OVERBOUGHT zone")
        else:
            st.warning("OVERBOUGHT zone - waiting for %K to cross below %D")
    else:
        st.info(f"NEUTRAL zone - %K={percent_k:.1f}, %D={percent_d:.1f}")

    # K/D relationship
    if k_above_d:
        st.info("%K above %D - Bullish momentum")
    else:
        st.info("%K below %D - Bearish momentum")

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
