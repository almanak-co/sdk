"""
GMX Perps Strategy Dashboard.

Custom dashboard showing position size, direction, entry price,
unrealized PnL, leverage, liquidation price, and funding rate.
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
    """Render the GMX Perps custom dashboard.

    Shows:
    - Position size and direction (long/short)
    - Entry price
    - Current price
    - Unrealized PnL with color coding
    - Leverage ratio
    - Liquidation price
    - Funding rate (positive/negative)
    """
    st.title("GMX Perps Strategy Dashboard")

    # Extract config values
    market = strategy_config.get("market", "ETH/USD")
    direction = strategy_config.get("direction", "long")
    leverage = Decimal(str(strategy_config.get("leverage", "5")))
    position_size_usd = Decimal(str(strategy_config.get("position_size_usd", "1000")))

    # Strategy info header
    st.markdown(f"**Strategy ID:** `{strategy_id}`")
    st.markdown(f"**Market:** {market}")
    st.markdown("**Protocol:** GMX V2")
    st.markdown("**Chain:** Arbitrum")

    # Direction badge
    if direction.lower() == "long":
        st.success(f"Direction: LONG {market}")
    else:
        st.error(f"Direction: SHORT {market}")

    st.divider()

    # Position Overview section
    st.subheader("Position Overview")
    _render_position_overview(session_state, market, direction, leverage, position_size_usd)

    st.divider()

    # PnL Section
    st.subheader("Profit & Loss")
    _render_pnl(session_state, direction)

    st.divider()

    # Risk Metrics section
    st.subheader("Risk Metrics")
    _render_risk_metrics(session_state, direction, leverage)

    st.divider()

    # Funding & Fees section
    st.subheader("Funding & Fees")
    _render_funding_fees(session_state, direction)


def _render_position_overview(
    session_state: dict[str, Any],
    market: str,
    direction: str,
    leverage: Decimal,
    position_size_usd: Decimal,
) -> None:
    """Render position overview with size and prices."""
    has_position = session_state.get("has_position", False)
    entry_price = Decimal(str(session_state.get("entry_price", "3300")))
    current_price = Decimal(str(session_state.get("current_price", "3400")))

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            "Position Status",
            "Open" if has_position else "Closed",
            help="Whether there is an active perpetual position",
        )

    with col2:
        st.metric(
            "Position Size",
            f"${float(position_size_usd):,.0f}",
            help="Total position size in USD",
        )

    with col3:
        collateral = position_size_usd / leverage
        st.metric(
            "Collateral",
            f"${float(collateral):,.0f}",
            help="Margin deposited",
        )

    col4, col5, col6 = st.columns(3)

    with col4:
        st.metric(
            "Entry Price",
            f"${float(entry_price):,.2f}",
            help="Average entry price",
        )

    with col5:
        st.metric(
            "Current Price",
            f"${float(current_price):,.2f}",
            help="Current market price",
        )

    with col6:
        st.metric(
            "Leverage",
            f"{float(leverage):.1f}x",
            help="Position leverage",
        )


def _render_pnl(session_state: dict[str, Any], direction: str) -> None:
    """Render unrealized PnL with color coding."""
    entry_price = Decimal(str(session_state.get("entry_price", "3300")))
    current_price = Decimal(str(session_state.get("current_price", "3400")))
    position_size_usd = Decimal(str(session_state.get("position_size_usd", "1000")))

    # Calculate PnL
    if direction.lower() == "long":
        price_change_pct = (current_price - entry_price) / entry_price * Decimal("100")
    else:
        price_change_pct = (entry_price - current_price) / entry_price * Decimal("100")

    unrealized_pnl = position_size_usd * price_change_pct / Decimal("100")
    pnl_pct = price_change_pct

    col1, col2, col3 = st.columns(3)

    with col1:
        # PnL amount
        pnl_color = "normal" if unrealized_pnl >= 0 else "inverse"
        st.metric(
            "Unrealized PnL",
            f"${float(unrealized_pnl):+,.2f}",
            delta=f"{float(pnl_pct):+.2f}%",
            delta_color=pnl_color,
            help="Unrealized profit/loss",
        )

    with col2:
        # Price change
        price_delta = current_price - entry_price
        st.metric(
            "Price Change",
            f"${float(price_delta):+,.2f}",
            help="Change from entry price",
        )

    with col3:
        # ROI
        collateral = position_size_usd / Decimal(str(session_state.get("leverage", "5")))
        roi = unrealized_pnl / collateral * Decimal("100") if collateral > 0 else Decimal("0")
        st.metric(
            "ROI on Collateral",
            f"{float(roi):+.1f}%",
            help="Return on invested collateral",
        )

    # PnL visualization
    if unrealized_pnl > 0:
        st.success(f"Position is profitable: +${float(unrealized_pnl):,.2f}")
    elif unrealized_pnl < 0:
        st.error(f"Position is at a loss: ${float(unrealized_pnl):,.2f}")
    else:
        st.info("Position is at breakeven")


def _render_risk_metrics(session_state: dict[str, Any], direction: str, leverage: Decimal) -> None:
    """Render liquidation price and risk indicators."""
    entry_price = Decimal(str(session_state.get("entry_price", "3300")))
    current_price = Decimal(str(session_state.get("current_price", "3400")))

    # Calculate liquidation price (simplified)
    # For long: liq_price = entry * (1 - 1/leverage + maintenance_margin)
    # For short: liq_price = entry * (1 + 1/leverage - maintenance_margin)
    maintenance_margin = Decimal("0.01")  # 1%

    if direction.lower() == "long":
        liq_price = entry_price * (Decimal("1") - Decimal("1") / leverage + maintenance_margin)
        distance_to_liq = (current_price - liq_price) / current_price * Decimal("100")
    else:
        liq_price = entry_price * (Decimal("1") + Decimal("1") / leverage - maintenance_margin)
        distance_to_liq = (liq_price - current_price) / current_price * Decimal("100")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            "Liquidation Price",
            f"${float(liq_price):,.2f}",
            help="Price at which position gets liquidated",
        )

    with col2:
        st.metric(
            "Distance to Liq",
            f"{float(distance_to_liq):.1f}%",
            help="Price movement needed to trigger liquidation",
        )

    with col3:
        # Risk level indicator
        if distance_to_liq > Decimal("20"):
            st.success("Risk: Low")
        elif distance_to_liq > Decimal("10"):
            st.info("Risk: Moderate")
        elif distance_to_liq > Decimal("5"):
            st.warning("Risk: High")
        else:
            st.error("Risk: Critical")

    # Risk warnings
    if distance_to_liq < Decimal("10"):
        st.warning(
            f"Position is within {float(distance_to_liq):.1f}% of liquidation. Consider reducing leverage or adding collateral."
        )


def _render_funding_fees(session_state: dict[str, Any], direction: str) -> None:
    """Render funding rate and accumulated fees."""
    # GMX V2 uses borrowing fees instead of funding rates
    borrow_fee_rate = Decimal("0.01")  # 0.01% per hour (example)
    hourly_fee = Decimal(str(session_state.get("position_size_usd", "1000"))) * borrow_fee_rate / Decimal("100")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            "Borrow Fee Rate",
            f"{float(borrow_fee_rate):.3f}%/hr",
            help="Hourly borrowing fee rate",
        )

    with col2:
        st.metric(
            "Hourly Cost",
            f"${float(hourly_fee):.4f}",
            help="Hourly borrowing fee cost",
        )

    with col3:
        daily_cost = hourly_fee * Decimal("24")
        st.metric(
            "Daily Cost",
            f"${float(daily_cost):.2f}",
            help="Daily borrowing fee cost",
        )

    st.markdown("**GMX V2 Fee Structure:**")
    st.markdown(
        """
        - **Open/Close Fee:** 0.05% - 0.07% of position size
        - **Borrowing Fee:** Variable rate based on utilization
        - **Price Impact:** Depends on position size vs pool liquidity
        - **Keeper Fee:** ~$0.35 for execution
        """
    )

    st.caption(
        "Note: GMX V2 uses borrowing fees instead of funding rates. Fees accrue continuously while position is open."
    )
