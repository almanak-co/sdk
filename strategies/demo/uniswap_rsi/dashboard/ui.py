"""
Uniswap RSI Strategy Dashboard.

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
    """Render the Uniswap RSI custom dashboard.

    Shows:
    - Current RSI value with color coding
    - RSI thresholds
    - Current position
    - Recent trade history
    - Cumulative PnL
    """
    st.title("Uniswap RSI Strategy Dashboard")

    # Extract config values
    base_token = strategy_config.get("base_token", "WETH")
    quote_token = strategy_config.get("quote_token", "USDC")
    rsi_oversold = Decimal(str(strategy_config.get("rsi_oversold", "30")))
    rsi_overbought = Decimal(str(strategy_config.get("rsi_overbought", "70")))
    rsi_period = strategy_config.get("rsi_period", 14)

    # Strategy info header
    st.markdown(f"**Strategy ID:** `{strategy_id}`")
    st.markdown(f"**Trading Pair:** {base_token}/{quote_token}")
    st.markdown("**DEX:** Uniswap V3")
    st.markdown("**Chain:** Arbitrum")
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
    base_token: str,
) -> None:
    """Render RSI indicator with color coding."""
    current_rsi = Decimal(str(session_state.get("current_rsi", "50")))

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            f"RSI({rsi_period})",
            f"{float(current_rsi):.1f}",
            help="Current RSI value (0-100)",
        )

    with col2:
        st.metric(
            "Oversold Level",
            f"{float(rsi_oversold):.0f}",
            help="RSI below this triggers buy",
        )

    with col3:
        st.metric(
            "Overbought Level",
            f"{float(rsi_overbought):.0f}",
            help="RSI above this triggers sell",
        )

    # Zone indicator
    if current_rsi <= rsi_oversold:
        st.success(f"OVERSOLD ZONE - Buy {base_token} signal")
    elif current_rsi >= rsi_overbought:
        st.error(f"OVERBOUGHT ZONE - Sell {base_token} signal")
    else:
        st.info("NEUTRAL ZONE - Hold position")

    # RSI gauge visualization
    col1, col2 = st.columns([3, 1])
    with col1:
        st.progress(float(current_rsi) / 100)
    with col2:
        st.markdown(f"**{float(current_rsi):.0f}/100**")


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
            help="Total portfolio value",
        )

    # Allocation breakdown
    if total_value > 0:
        base_pct = (base_balance * base_price / total_value) * Decimal("100")
        quote_pct = (quote_balance / total_value) * Decimal("100")
        st.markdown(f"**Allocation:** {float(base_pct):.0f}% {base_token} | {float(quote_pct):.0f}% {quote_token}")


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
            details.get("amount", "?")
            st.markdown(f"- `{timestamp[:19] if len(timestamp) > 19 else timestamp}` {from_token} -> {to_token}")
    else:
        st.info("No recent trades. Strategy executes when RSI crosses thresholds.")


def _render_pnl(session_state: dict[str, Any]) -> None:
    """Render PnL metrics."""
    total_pnl = Decimal(str(session_state.get("total_pnl", "0")))
    total_trades = session_state.get("total_trades", 0)
    win_rate = Decimal(str(session_state.get("win_rate", "50")))
    avg_trade = total_pnl / Decimal(str(total_trades)) if total_trades > 0 else Decimal("0")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        pnl_delta = "+" if total_pnl >= 0 else ""
        st.metric(
            "Total PnL",
            f"{pnl_delta}${float(abs(total_pnl)):,.2f}",
            help="Cumulative profit/loss",
        )

    with col2:
        st.metric(
            "Total Trades",
            str(total_trades),
            help="Number of completed trades",
        )

    with col3:
        st.metric(
            "Win Rate",
            f"{float(win_rate):.0f}%",
            help="Percentage of profitable trades",
        )

    with col4:
        st.metric(
            "Avg Trade",
            f"${float(avg_trade):+,.2f}",
            help="Average profit per trade",
        )

    # Performance summary
    if total_pnl > 0:
        st.success(f"Strategy is profitable: +${float(total_pnl):,.2f}")
    elif total_pnl < 0:
        st.warning(f"Strategy is at a loss: ${float(total_pnl):,.2f}")
    else:
        st.info("No trades executed yet or breakeven.")
