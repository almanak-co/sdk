"""
Enso RSI Strategy Dashboard.

Custom dashboard showing RSI indicator value, thresholds,
current position, trade history, and cumulative PnL.
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
    """Render the Enso RSI custom dashboard.

    Shows:
    - Current RSI value with color coding
    - RSI thresholds (overbought/oversold)
    - Current position (token held)
    - Recent trade history
    - Cumulative PnL
    """
    st.title("Enso RSI Strategy Dashboard")

    # Extract config values
    base_token = strategy_config.get("base_token", "WETH")
    quote_token = strategy_config.get("quote_token", "USDC")
    rsi_oversold = Decimal(str(strategy_config.get("rsi_oversold", "30")))
    rsi_overbought = Decimal(str(strategy_config.get("rsi_overbought", "70")))
    rsi_period = strategy_config.get("rsi_period", 14)

    # Strategy info header
    st.markdown(f"**Strategy ID:** `{strategy_id}`")
    st.markdown(f"**Trading Pair:** {base_token}/{quote_token}")
    st.markdown("**Aggregator:** Enso")
    st.markdown("**Indicator:** RSI Mean Reversion")

    st.divider()

    # RSI Indicator section
    st.subheader("RSI Indicator")
    _render_rsi_indicator(session_state, rsi_period, rsi_oversold, rsi_overbought, base_token)

    st.divider()

    # Current Position section
    st.subheader("Current Position")
    _render_current_position(session_state, base_token, quote_token)

    st.divider()

    # Trade History section
    st.subheader("Recent Trades")
    _render_trade_history(api_client, strategy_id)

    st.divider()

    # PnL section
    st.subheader("Performance")
    _render_pnl(session_state)


def _render_rsi_indicator(
    session_state: dict[str, Any],
    rsi_period: int,
    rsi_oversold: Decimal,
    rsi_overbought: Decimal,
    base_token: str = "WETH",
) -> None:
    """Render RSI indicator with color coding."""
    current_rsi = Decimal(str(session_state.get("current_rsi", "50")))

    col1, col2, col3 = st.columns(3)

    with col1:
        # RSI value with color
        st.metric(
            f"RSI({rsi_period})",
            f"{float(current_rsi):.1f}",
            help="Current RSI value",
        )

    with col2:
        st.metric(
            "Oversold Level",
            f"{float(rsi_oversold):.0f}",
            help="RSI below this = buy signal",
        )

    with col3:
        st.metric(
            "Overbought Level",
            f"{float(rsi_overbought):.0f}",
            help="RSI above this = sell signal",
        )

    # RSI zone indicator
    if current_rsi <= rsi_oversold:
        st.success(f"Zone: OVERSOLD - Buy opportunity (RSI={float(current_rsi):.1f})")
    elif current_rsi >= rsi_overbought:
        st.error(f"Zone: OVERBOUGHT - Sell opportunity (RSI={float(current_rsi):.1f})")
    else:
        st.info(f"Zone: NEUTRAL - No signal (RSI={float(current_rsi):.1f})")

    # RSI explanation
    st.markdown("**RSI Strategy Logic:**")
    st.markdown(
        f"""
        - RSI < {float(rsi_oversold):.0f}: Buy {base_token} (oversold)
        - RSI > {float(rsi_overbought):.0f}: Sell {base_token} (overbought)
        - Between: Hold current position
        """
    )


def _render_current_position(session_state: dict[str, Any], base_token: str, quote_token: str) -> None:
    """Render current position details."""
    base_balance = Decimal(str(session_state.get("base_balance", "0")))
    quote_balance = Decimal(str(session_state.get("quote_balance", "0")))
    base_price = Decimal(str(session_state.get("base_price", "3400")))

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            f"{base_token} Balance",
            f"{float(base_balance):.4f}",
            help=f"Current {base_token} holdings",
        )

    with col2:
        st.metric(
            f"{quote_token} Balance",
            f"${float(quote_balance):,.2f}",
            help=f"Current {quote_token} holdings",
        )

    with col3:
        total_value = base_balance * base_price + quote_balance
        st.metric(
            "Total Value",
            f"${float(total_value):,.2f}",
            help="Total portfolio value in USD",
        )

    # Position allocation
    if total_value > 0:
        base_allocation = (base_balance * base_price / total_value) * Decimal("100")
        quote_allocation = (quote_balance / total_value) * Decimal("100")
        st.markdown(
            f"**Allocation:** {float(base_allocation):.1f}% {base_token} / {float(quote_allocation):.1f}% {quote_token}"
        )


def _render_trade_history(api_client: Any, strategy_id: str) -> None:
    """Render recent trade history."""
    trades = []
    if api_client:
        try:
            events = api_client.get_timeline(strategy_id, limit=10)
            trades = [e for e in events if e.get("event_type") in ["SWAP", "swap"]]
        except Exception:
            pass

    if trades:
        for trade in trades[:5]:
            timestamp = trade.get("timestamp", "N/A")
            details = trade.get("details", {})
            from_token = details.get("from_token", "?")
            to_token = details.get("to_token", "?")
            amount = details.get("amount", "?")
            st.markdown(f"- `{timestamp[:19]}` {from_token} -> {to_token} ({amount})")
    else:
        st.info("No recent trades. Strategy will execute when RSI signals trigger.")


def _render_pnl(session_state: dict[str, Any]) -> None:
    """Render cumulative PnL metrics."""
    total_pnl = Decimal(str(session_state.get("total_pnl", "0")))
    total_trades = session_state.get("total_trades", 0)
    win_rate = Decimal(str(session_state.get("win_rate", "50")))

    col1, col2, col3 = st.columns(3)

    with col1:
        pnl_color = "normal" if total_pnl >= 0 else "inverse"
        st.metric(
            "Cumulative PnL",
            f"${float(total_pnl):+,.2f}",
            delta_color=pnl_color,
            help="Total profit/loss",
        )

    with col2:
        st.metric(
            "Total Trades",
            str(total_trades),
            help="Number of trades executed",
        )

    with col3:
        st.metric(
            "Win Rate",
            f"{float(win_rate):.0f}%",
            help="Percentage of profitable trades",
        )
