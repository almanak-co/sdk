"""
MA Crossover Strategy Dashboard.

Custom dashboard showing short and long moving averages,
crossover status (golden/death cross), and PnL metrics.
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
    """Render the MA Crossover custom dashboard."""
    st.title("MA Crossover Strategy Dashboard")

    # Extract config
    sma_short = strategy_config.get("sma_short", 9)
    sma_long = strategy_config.get("sma_long", 21)
    base_token = strategy_config.get("base_token", "LINK")
    quote_token = strategy_config.get("quote_token", "USDC")

    st.markdown(f"**Strategy ID:** `{strategy_id}`")
    st.markdown(f"**Pair:** {base_token}/{quote_token}")
    st.markdown("**Chain:** Arbitrum | **Protocol:** Uniswap V3")

    st.divider()

    # MA Indicator
    st.subheader(f"Moving Averages (SMA {sma_short} / {sma_long})")

    short_ma = Decimal(str(session_state.get("short_ma", "14.5")))
    long_ma = Decimal(str(session_state.get("long_ma", "14.2")))
    current_price = Decimal(str(session_state.get("current_price", "14.3")))

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric(f"SMA({sma_short})", f"${float(short_ma):.4f}")
    with col2:
        st.metric(f"SMA({sma_long})", f"${float(long_ma):.4f}")
    with col3:
        st.metric("Current Price", f"${float(current_price):.4f}")

    # Crossover status
    ma_diff = short_ma - long_ma
    ma_diff_pct = (ma_diff / long_ma) * 100 if long_ma != 0 else Decimal("0")

    col4, col5 = st.columns(2)
    with col4:
        st.metric("MA Spread", f"${float(ma_diff):.4f}")
    with col5:
        st.metric("Spread %", f"{float(ma_diff_pct):+.2f}%")

    st.divider()

    # Signal status
    st.subheader("Signal Status")
    if short_ma > long_ma:
        st.success(f"GOLDEN CROSS: SMA({sma_short}) > SMA({sma_long}) - Bullish trend, Buy signal")
    elif short_ma < long_ma:
        st.error(f"DEATH CROSS: SMA({sma_short}) < SMA({sma_long}) - Bearish trend, Sell signal")
    else:
        st.info("MAs Equal - No crossover signal")

    # Price vs MAs
    if current_price > short_ma and current_price > long_ma:
        st.info("Price above both MAs - Strong bullish momentum")
    elif current_price < short_ma and current_price < long_ma:
        st.warning("Price below both MAs - Strong bearish momentum")
    else:
        st.info("Price between MAs - Potential reversal zone")

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
