"""
Ethena Yield Strategy Dashboard.

Custom dashboard showing sUSDe balance, USDe equivalent,
yield accrued, current APY, and cooldown status.
"""

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import streamlit as st


def render_custom_dashboard(
    strategy_id: str,
    strategy_config: dict[str, Any],
    api_client: Any,
    session_state: dict[str, Any],
) -> None:
    """Render the Ethena Yield custom dashboard.

    Shows:
    - sUSDe balance
    - USDe equivalent value
    - Yield accrued (sUSDe value growth)
    - Current APY
    - Cooldown status (not started / active with countdown / ready to claim)
    """
    st.title("Ethena Yield Strategy Dashboard")

    # Extract config values
    min_stake_amount = Decimal(str(strategy_config.get("min_stake_amount", "100")))
    swap_usdc_to_usde = strategy_config.get("swap_usdc_to_usde", False)

    # Strategy info header
    st.markdown(f"**Strategy ID:** `{strategy_id}`")
    st.markdown("**Protocol:** Ethena")
    st.markdown("**Chain:** Ethereum")
    st.markdown(f"**Min Stake Amount:** {float(min_stake_amount):,.0f} USDe")

    if swap_usdc_to_usde:
        st.info("USDC -> USDe swap enabled")

    st.divider()

    # Staking Position section
    st.subheader("Staking Position")
    _render_staking_position(session_state)

    st.divider()

    # Yield Metrics section
    st.subheader("Yield Metrics")
    _render_yield_metrics(session_state)

    st.divider()

    # Cooldown Status section
    st.subheader("Cooldown Status")
    _render_cooldown_status(session_state)

    st.divider()

    # Risk Information section
    st.subheader("Risk Information")
    _render_risk_info()


def _render_staking_position(session_state: dict[str, Any]) -> None:
    """Render sUSDe staking position details."""
    staked = session_state.get("staked", False)
    staked_amount = Decimal(str(session_state.get("staked_amount", "0")))

    # sUSDe exchange rate (increases over time as yield accrues)
    # In production, would query actual rate from sUSDe contract
    susde_rate = Decimal("1.05")  # 1 sUSDe = 1.05 USDe (5% yield accrued)
    usde_equivalent = staked_amount * susde_rate

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            "Position Status",
            "Staked" if staked else "Not Staked",
            help="Whether USDe is staked for sUSDe",
        )

    with col2:
        st.metric(
            "sUSDe Balance",
            f"{float(staked_amount):,.2f}",
            help="Amount of sUSDe tokens held",
        )

    with col3:
        st.metric(
            "USDe Equivalent",
            f"${float(usde_equivalent):,.2f}",
            help="Current value in USDe",
        )

    # Exchange rate info
    col4, col5 = st.columns(2)

    with col4:
        st.metric(
            "sUSDe/USDe Rate",
            f"{float(susde_rate):.4f}",
            help="Current sUSDe to USDe exchange rate",
        )

    with col5:
        st.metric(
            "Position Value",
            f"${float(usde_equivalent):,.2f}",
            help="Total position value in USD",
        )


def _render_yield_metrics(session_state: dict[str, Any]) -> None:
    """Render yield accrued and APY."""
    staked_amount = Decimal(str(session_state.get("staked_amount", "0")))

    # Ethena typically offers 15-25% APY from funding rate arbitrage
    current_apy = Decimal("18.5")  # Placeholder
    trailing_7d_apy = Decimal("19.2")

    # Calculate yield based on rate appreciation
    susde_rate = Decimal("1.05")
    yield_accrued = staked_amount * (susde_rate - Decimal("1"))

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            "Current APY",
            f"{float(current_apy):.1f}%",
            help="Current annualized yield",
        )

    with col2:
        st.metric(
            "7-Day APY",
            f"{float(trailing_7d_apy):.1f}%",
            help="Trailing 7-day annualized yield",
        )

    with col3:
        st.metric(
            "Yield Accrued",
            f"${float(yield_accrued):,.2f}",
            help="Total yield earned to date",
        )

    # Projected earnings
    if staked_amount > 0:
        yearly_yield = staked_amount * current_apy / Decimal("100")
        daily_yield = yearly_yield / Decimal("365")

        st.markdown("**Projected Earnings:**")
        st.markdown(f"- Daily: ~${float(daily_yield):.2f}")
        st.markdown(f"- Monthly: ~${float(daily_yield * 30):,.2f}")
        st.markdown(f"- Yearly: ~${float(yearly_yield):,.2f}")

    st.caption("APY varies based on perpetual futures funding rates. High positive funding = higher yields.")


def _render_cooldown_status(session_state: dict[str, Any]) -> None:
    """Render cooldown status for unstaking."""
    cooldown_status = session_state.get("cooldown_status", "not_started")
    cooldown_end_time = session_state.get("cooldown_end_time")

    col1, col2, col3 = st.columns(3)

    with col1:
        status_display = {
            "not_started": "Not Started",
            "active": "In Progress",
            "ready": "Ready to Claim",
        }.get(cooldown_status, cooldown_status)

        st.metric(
            "Cooldown Status",
            status_display,
            help="Status of unstaking cooldown",
        )

    with col2:
        st.metric(
            "Cooldown Period",
            "7 days",
            help="Time required to unstake sUSDe",
        )

    with col3:
        if cooldown_status == "active" and cooldown_end_time:
            try:
                end_time = datetime.fromisoformat(cooldown_end_time.replace("Z", "+00:00"))
                now = datetime.now(UTC)
                remaining = end_time - now
                days_remaining = max(0, remaining.days)
                hours_remaining = max(0, remaining.seconds // 3600)
                st.metric(
                    "Time Remaining",
                    f"{days_remaining}d {hours_remaining}h",
                    help="Time until cooldown completes",
                )
            except Exception:
                st.metric("Time Remaining", "N/A")
        elif cooldown_status == "ready":
            st.success("Ready to Claim!")
        else:
            st.metric("Time Remaining", "N/A")

    # Cooldown explanation
    if cooldown_status == "not_started":
        st.info("No cooldown initiated. To unstake, initiate cooldown and wait 7 days.")
    elif cooldown_status == "active":
        st.warning("Cooldown in progress. Cannot claim until complete.")
    elif cooldown_status == "ready":
        st.success("Cooldown complete! You can now claim your USDe.")


def _render_risk_info() -> None:
    """Render Ethena-specific risk information."""
    st.markdown("**Ethena Risk Factors:**")

    risks = [
        ("Smart Contract Risk", "Ethena protocol bugs or exploits", "warning"),
        ("Depeg Risk", "USDe may trade below $1 in extreme conditions", "warning"),
        ("Funding Rate Risk", "Negative funding rates reduce yield", "info"),
        ("Cooldown Period", "7-day wait required to unstake", "info"),
        ("Custodial Risk", "Ethena relies on centralized exchanges for hedging", "warning"),
    ]

    for risk_name, risk_desc, risk_level in risks:
        if risk_level == "warning":
            st.warning(f"**{risk_name}:** {risk_desc}")
        else:
            st.info(f"**{risk_name}:** {risk_desc}")

    st.markdown(
        """
        **How Ethena Works:**
        - USDe is minted against delta-neutral ETH positions
        - Yield comes from perpetual futures funding rate arbitrage
        - sUSDe is a yield-bearing wrapper that appreciates over time
        - During positive funding (most of the time), longs pay shorts
        - Ethena captures this yield and passes it to sUSDe holders
        """
    )
