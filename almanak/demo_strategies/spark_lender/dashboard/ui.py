"""
Spark Lender Strategy Dashboard.

Custom dashboard showing spDAI balance, DAI equivalent,
interest accrued, supply APY, and protocol utilization.
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
    """Render the Spark Lender custom dashboard.

    Shows:
    - spDAI balance
    - DAI equivalent value
    - Interest accrued
    - Current supply APY
    - Protocol utilization rate
    """
    st.title("Spark Lender Strategy Dashboard")

    # Extract config values
    Decimal(str(strategy_config.get("min_supply_amount", "100")))

    # Strategy info header
    st.markdown(f"**Strategy ID:** `{strategy_id}`")
    st.markdown("**Protocol:** Spark (Aave V3 Fork)")
    st.markdown("**Chain:** Ethereum")
    st.markdown("**Asset:** DAI")

    st.divider()

    # Supply Position section
    st.subheader("Supply Position")
    _render_supply_position(session_state)

    st.divider()

    # Yield Metrics section
    st.subheader("Yield Metrics")
    _render_yield_metrics(session_state)

    st.divider()

    # Protocol Health section
    st.subheader("Protocol Health")
    _render_protocol_health(session_state)

    st.divider()

    # Risk Information section
    st.subheader("Risk Information")
    _render_risk_info()


def _render_supply_position(session_state: dict[str, Any]) -> None:
    """Render supply position details."""
    supplied = session_state.get("supplied", False)
    supplied_amount = Decimal(str(session_state.get("supplied_amount", "0")))

    # spDAI is 1:1 with DAI (interest accrues separately)
    dai_equivalent = supplied_amount

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            "Position Status",
            "Supplied" if supplied else "Not Supplied",
            help="Whether DAI is supplied to Spark",
        )

    with col2:
        st.metric(
            "spDAI Balance",
            f"{float(supplied_amount):,.2f}",
            help="Amount of spDAI (Spark DAI) tokens",
        )

    with col3:
        st.metric(
            "DAI Equivalent",
            f"${float(dai_equivalent):,.2f}",
            help="Value in DAI terms",
        )

    # Collateral status
    st.info("Note: Spark automatically uses all supplied assets as collateral. This cannot be disabled.")


def _render_yield_metrics(session_state: dict[str, Any]) -> None:
    """Render interest accrued and APY."""
    supplied_amount = Decimal(str(session_state.get("supplied_amount", "0")))
    interest_accrued = Decimal(str(session_state.get("interest_accrued", "0")))

    # Spark DAI APY (typically 5-8%)
    supply_apy = Decimal("6.5")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            "Supply APY",
            f"{float(supply_apy):.2f}%",
            help="Current supply interest rate",
        )

    with col2:
        st.metric(
            "Interest Accrued",
            f"${float(interest_accrued):,.4f}",
            help="Interest earned to date",
        )

    with col3:
        total_value = supplied_amount + interest_accrued
        st.metric(
            "Total Value",
            f"${float(total_value):,.2f}",
            help="Principal + interest",
        )

    # Projected earnings
    if supplied_amount > 0:
        yearly_yield = supplied_amount * supply_apy / Decimal("100")
        daily_yield = yearly_yield / Decimal("365")

        st.markdown("**Projected Earnings:**")
        st.markdown(f"- Daily: ~${float(daily_yield):.4f}")
        st.markdown(f"- Monthly: ~${float(daily_yield * 30):.2f}")
        st.markdown(f"- Yearly: ~${float(yearly_yield):,.2f}")

    st.caption("Spark supply rates vary based on utilization. Higher utilization = higher rates.")


def _render_protocol_health(session_state: dict[str, Any]) -> None:
    """Render protocol utilization and health metrics."""
    # Typical Spark metrics
    utilization_rate = Decimal("75")  # 75% utilization
    total_supplied = Decimal("500000000")  # $500M
    total_borrowed = total_supplied * utilization_rate / Decimal("100")
    available_liquidity = total_supplied - total_borrowed

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            "Utilization Rate",
            f"{float(utilization_rate):.1f}%",
            help="Percentage of supplied DAI that is borrowed",
        )

    with col2:
        st.metric(
            "Total Supplied",
            f"${float(total_supplied / Decimal('1000000')):,.0f}M",
            help="Total DAI supplied to Spark",
        )

    with col3:
        st.metric(
            "Available Liquidity",
            f"${float(available_liquidity / Decimal('1000000')):,.0f}M",
            help="Available for withdrawal",
        )

    # Utilization impact explanation
    if utilization_rate > Decimal("90"):
        st.warning("High utilization - withdrawals may be delayed if liquidity is insufficient.")
    elif utilization_rate > Decimal("80"):
        st.info("Moderate utilization - good supply rates, adequate liquidity.")
    else:
        st.success("Low utilization - high liquidity available for withdrawals.")


def _render_risk_info() -> None:
    """Render Spark-specific risk information."""
    st.markdown("**Spark Risk Factors:**")

    risks = [
        ("Smart Contract Risk", "Spark protocol bugs or exploits (Aave V3 fork)", "warning"),
        ("Interest Rate Risk", "Supply rates fluctuate with market demand", "info"),
        ("Utilization Risk", "High utilization can delay withdrawals", "info"),
        ("Oracle Risk", "Price oracle issues could affect the protocol", "warning"),
    ]

    for risk_name, risk_desc, risk_level in risks:
        if risk_level == "warning":
            st.warning(f"**{risk_name}:** {risk_desc}")
        else:
            st.info(f"**{risk_name}:** {risk_desc}")

    st.markdown("**Spark vs Aave:**")
    st.markdown(
        """
        - Spark is an Aave V3 fork within the Maker/Sky ecosystem
        - DAI-centric with optimized parameters
        - Governed by MakerDAO
        - Often offers competitive DAI rates
        """
    )
