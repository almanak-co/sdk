"""
Ichimoku Crossover Strategy Dashboard.

Custom dashboard showing Ichimoku Cloud components (Tenkan, Kijun, Senkou spans),
cloud position, crossover signals, and PnL metrics.
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
    """Render the Ichimoku Crossover custom dashboard."""
    st.title("Ichimoku Crossover Strategy Dashboard")

    # Extract config
    tenkan_period = strategy_config.get("tenkan_period", 9)
    kijun_period = strategy_config.get("kijun_period", 26)
    senkou_b_period = strategy_config.get("senkou_b_period", 52)
    base_token = strategy_config.get("base_token", "LINK")
    quote_token = strategy_config.get("quote_token", "USDC")

    st.markdown(f"**Strategy ID:** `{strategy_id}`")
    st.markdown(f"**Pair:** {base_token}/{quote_token}")
    st.markdown("**Chain:** Arbitrum | **Protocol:** Uniswap V3")

    st.divider()

    # Ichimoku Indicator
    st.subheader(f"Ichimoku Cloud ({tenkan_period}/{kijun_period}/{senkou_b_period})")

    tenkan = Decimal(str(session_state.get("tenkan_sen", "14.5")))
    kijun = Decimal(str(session_state.get("kijun_sen", "14.2")))
    senkou_a = Decimal(str(session_state.get("senkou_span_a", "14.35")))
    senkou_b = Decimal(str(session_state.get("senkou_span_b", "14.0")))
    current_price = Decimal(str(session_state.get("current_price", "14.3")))

    col1, col2 = st.columns(2)
    with col1:
        st.metric("Tenkan-sen", f"${float(tenkan):.4f}")
        st.metric("Senkou Span A", f"${float(senkou_a):.4f}")
    with col2:
        st.metric("Kijun-sen", f"${float(kijun):.4f}")
        st.metric("Senkou Span B", f"${float(senkou_b):.4f}")

    st.divider()

    # Crossover status
    st.subheader("Signal Status")
    if tenkan > kijun:
        st.success("Tenkan > Kijun: BULLISH crossover - Buy signal")
    elif tenkan < kijun:
        st.error("Tenkan < Kijun: BEARISH crossover - Sell signal")
    else:
        st.info("Tenkan = Kijun: No crossover")

    # Cloud position
    cloud_top = max(senkou_a, senkou_b)
    cloud_bottom = min(senkou_a, senkou_b)

    if current_price > cloud_top:
        st.success(f"Price ABOVE cloud (${float(current_price):.2f} > ${float(cloud_top):.2f}) - Bullish trend")
    elif current_price < cloud_bottom:
        st.error(f"Price BELOW cloud (${float(current_price):.2f} < ${float(cloud_bottom):.2f}) - Bearish trend")
    else:
        st.warning(f"Price IN cloud (${float(cloud_bottom):.2f} - ${float(cloud_top):.2f}) - Consolidation")

    # Cloud color
    if senkou_a > senkou_b:
        st.info("Cloud: GREEN (Senkou A > Senkou B) - Bullish momentum")
    else:
        st.warning("Cloud: RED (Senkou A < Senkou B) - Bearish momentum")

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
