"""
Aave Borrow Strategy Dashboard.

Custom dashboard showing collateral value, borrowed amount,
health factor, interest rate, and liquidation risk.
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
    """Render the Aave Borrow custom dashboard.

    Shows:
    - Collateral value (USD)
    - Borrowed amount and asset
    - Health factor with color coding
    - Current borrow APY
    - Liquidation threshold
    """
    st.title("Aave Borrow Strategy Dashboard")

    # Extract config values with defaults
    collateral_token = strategy_config.get("collateral_token", "WETH")
    borrow_token = strategy_config.get("borrow_token", "USDC")
    collateral_amount = Decimal(str(strategy_config.get("collateral_amount", "1")))
    target_ltv_pct = Decimal(str(strategy_config.get("target_ltv_pct", "60")))

    # Strategy info header
    st.markdown(f"**Strategy ID:** `{strategy_id}`")
    st.markdown(f"**Collateral:** {collateral_token}")
    st.markdown(f"**Borrow Asset:** {borrow_token}")
    st.markdown("**Chain:** Arbitrum")
    st.markdown("**Protocol:** Aave V3")

    st.divider()

    # Position Overview section
    st.subheader("Position Overview")
    _render_position_overview(session_state, collateral_token, borrow_token, collateral_amount, target_ltv_pct)

    st.divider()

    # Health Factor section
    st.subheader("Health Factor")
    _render_health_factor(session_state)

    st.divider()

    # Interest Rates section
    st.subheader("Interest Rates")
    _render_interest_rates(session_state, borrow_token)

    st.divider()

    # Liquidation Risk section
    st.subheader("Liquidation Risk")
    _render_liquidation_risk(session_state, collateral_token)


def _render_position_overview(
    session_state: dict[str, Any],
    collateral_token: str,
    borrow_token: str,
    collateral_amount: Decimal,
    target_ltv_pct: Decimal,
) -> None:
    """Render position overview with collateral and borrow details."""
    has_position = session_state.get("has_position", False)
    session_state.get("collateral_value_usd", "0")
    session_state.get("borrowed_amount", "0")

    # Estimate collateral value (would be from oracle in production)
    eth_price = Decimal("3400")  # Placeholder
    estimated_collateral_usd = collateral_amount * eth_price if collateral_token == "WETH" else collateral_amount

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            "Position Status",
            "Active" if has_position else "Inactive",
            help="Whether there is an active borrow position",
        )

    with col2:
        st.metric(
            f"{collateral_token} Collateral",
            f"{float(collateral_amount):.4f}",
            help=f"Amount of {collateral_token} deposited as collateral",
        )

    with col3:
        st.metric(
            "Collateral Value",
            f"${float(estimated_collateral_usd):,.2f}",
            help="USD value of collateral",
        )

    # Borrow details
    col4, col5, col6 = st.columns(3)

    estimated_borrow = estimated_collateral_usd * target_ltv_pct / Decimal("100")

    with col4:
        st.metric(
            f"{borrow_token} Borrowed",
            f"{float(estimated_borrow):,.2f}" if has_position else "0.00",
            help=f"Amount of {borrow_token} borrowed",
        )

    with col5:
        st.metric(
            "Target LTV",
            f"{float(target_ltv_pct):.0f}%",
            help="Target Loan-to-Value ratio",
        )

    with col6:
        actual_ltv = (
            (estimated_borrow / estimated_collateral_usd * Decimal("100"))
            if estimated_collateral_usd > 0
            else Decimal("0")
        )
        st.metric(
            "Current LTV",
            f"{float(actual_ltv):.1f}%",
            help="Current Loan-to-Value ratio",
        )


def _render_health_factor(session_state: dict[str, Any]) -> None:
    """Render health factor with color-coded indicator."""
    health_factor = Decimal(str(session_state.get("health_factor", "2.5")))

    col1, col2, col3 = st.columns(3)

    with col1:
        # Health factor value
        st.metric(
            "Health Factor",
            f"{float(health_factor):.2f}",
            help="Health factor below 1.0 triggers liquidation",
        )

    with col2:
        # Health status indicator
        if health_factor >= Decimal("2.0"):
            st.success("Status: Safe")
        elif health_factor >= Decimal("1.5"):
            st.info("Status: Moderate")
        elif health_factor >= Decimal("1.2"):
            st.warning("Status: At Risk")
        else:
            st.error("Status: Danger - Near Liquidation")

    with col3:
        # Buffer to liquidation
        buffer_pct = max(Decimal("0"), (health_factor - Decimal("1")) * Decimal("100"))
        st.metric(
            "Safety Buffer",
            f"{float(buffer_pct):.1f}%",
            help="Buffer before liquidation threshold",
        )

    # Health factor explanation
    st.markdown("**Health Factor Guide:**")
    st.markdown(
        """
        - **> 2.0:** Very safe position
        - **1.5 - 2.0:** Moderate risk
        - **1.2 - 1.5:** Consider adding collateral or repaying
        - **< 1.2:** High risk - take action immediately
        - **< 1.0:** Position can be liquidated
        """
    )


def _render_interest_rates(session_state: dict[str, Any], borrow_token: str) -> None:
    """Render current borrow APY and interest accrued."""
    borrow_apy = Decimal(str(session_state.get("borrow_apy", "5.5")))
    interest_accrued = Decimal(str(session_state.get("interest_accrued", "0")))

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            "Borrow APY",
            f"{float(borrow_apy):.2f}%",
            help=f"Current interest rate for borrowing {borrow_token}",
        )

    with col2:
        st.metric(
            "Interest Accrued",
            f"${float(interest_accrued):.4f}",
            help="Interest accumulated on the borrow position",
        )

    with col3:
        # Variable vs stable rate
        st.metric(
            "Rate Type",
            "Variable",
            help="Variable rates fluctuate with market demand",
        )

    st.caption(f"Note: {borrow_token} borrow rates vary based on utilization. Higher utilization = higher rates.")


def _render_liquidation_risk(session_state: dict[str, Any], collateral_token: str) -> None:
    """Render liquidation threshold and price levels."""
    liquidation_threshold = Decimal("82.5")  # WETH on Aave V3 Arbitrum
    current_price = Decimal("3400")  # Placeholder
    health_factor = Decimal(str(session_state.get("health_factor", "2.5")))

    # Calculate liquidation price
    # HF = (Collateral * LT) / Debt
    # At HF=1: Collateral * LT = Debt
    # Liquidation_Price = Current_Price / Health_Factor
    liquidation_price = current_price / health_factor if health_factor > 0 else Decimal("0")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            "Liquidation Threshold",
            f"{float(liquidation_threshold):.1f}%",
            help=f"Max LTV before liquidation for {collateral_token}",
        )

    with col2:
        st.metric(
            f"Current {collateral_token} Price",
            f"${float(current_price):,.0f}",
            help="Current market price",
        )

    with col3:
        st.metric(
            "Liquidation Price",
            f"${float(liquidation_price):,.0f}",
            help=f"{collateral_token} price at which position gets liquidated",
        )

    # Price drop tolerance
    price_drop_tolerance = ((current_price - liquidation_price) / current_price) * Decimal("100")
    st.info(f"{collateral_token} price can drop {float(price_drop_tolerance):.1f}% before liquidation")

    st.markdown("**Liquidation Prevention Tips:**")
    st.markdown(
        """
        - Monitor health factor regularly
        - Add collateral if health factor drops below 1.5
        - Repay part of the loan to reduce risk
        - Set up alerts for price movements
        """
    )
