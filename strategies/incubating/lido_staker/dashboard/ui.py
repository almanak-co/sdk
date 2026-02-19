"""
Lido Staker Strategy Dashboard.

Custom dashboard showing stETH/wstETH balance, ETH equivalent,
staking yield, APY, withdrawal queue status, and peg status.
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
    """Render the Lido Staker custom dashboard.

    Shows:
    - stETH/wstETH balance
    - ETH equivalent value
    - Staking yield accrued
    - Current APY
    - Withdrawal queue status (if any pending)
    - stETH/ETH peg status
    """
    st.title("Lido Staker Strategy Dashboard")

    # Extract config values
    Decimal(str(strategy_config.get("min_stake_amount", "0.1")))
    receive_wrapped = strategy_config.get("receive_wrapped", True)
    output_token = "wstETH" if receive_wrapped else "stETH"

    # Strategy info header
    st.markdown(f"**Strategy ID:** `{strategy_id}`")
    st.markdown("**Protocol:** Lido")
    st.markdown("**Chain:** Ethereum")
    st.markdown(f"**Output Token:** {output_token}")

    st.divider()

    # Staking Position section
    st.subheader("Staking Position")
    _render_staking_position(session_state, output_token)

    st.divider()

    # Yield Metrics section
    st.subheader("Yield Metrics")
    _render_yield_metrics(session_state, output_token)

    st.divider()

    # Withdrawal Status section
    st.subheader("Withdrawal Queue")
    _render_withdrawal_status(session_state)

    st.divider()

    # Peg Status section
    st.subheader("stETH/ETH Peg Status")
    _render_peg_status(session_state)


def _render_staking_position(session_state: dict[str, Any], output_token: str) -> None:
    """Render stETH/wstETH staking position details."""
    staked = session_state.get("staked", False)
    staked_amount = Decimal(str(session_state.get("staked_amount", "0")))

    # ETH price for USD value
    eth_price = Decimal("3400")  # Placeholder

    # wstETH rate (wstETH appreciates relative to stETH)
    wsteth_rate = Decimal("1.15") if output_token == "wstETH" else Decimal("1.0")
    eth_equivalent = staked_amount * wsteth_rate
    usd_value = eth_equivalent * eth_price

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            "Position Status",
            "Staked" if staked else "Not Staked",
            help="Whether ETH is staked with Lido",
        )

    with col2:
        st.metric(
            f"{output_token} Balance",
            f"{float(staked_amount):.4f}",
            help=f"Amount of {output_token} tokens held",
        )

    with col3:
        st.metric(
            "ETH Equivalent",
            f"{float(eth_equivalent):.4f}",
            help="Value in ETH terms",
        )

    col4, col5 = st.columns(2)

    with col4:
        st.metric(
            "USD Value",
            f"${float(usd_value):,.2f}",
            help="Current USD value of position",
        )

    with col5:
        if output_token == "wstETH":
            st.metric(
                "wstETH/stETH Rate",
                f"{float(wsteth_rate):.4f}",
                help="1 wstETH = X stETH",
            )


def _render_yield_metrics(session_state: dict[str, Any], output_token: str) -> None:
    """Render staking yield and APY."""
    staked_amount = Decimal(str(session_state.get("staked_amount", "0")))
    eth_price = Decimal("3400")

    # Lido staking APY (typically 3-5%)
    current_apy = Decimal("4.2")

    # For wstETH, yield is reflected in the rate appreciation
    # For stETH, balance increases daily (rebasing)
    yield_explanation = "wstETH rate appreciation" if output_token == "wstETH" else "daily balance rebase"

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            "Current APY",
            f"{float(current_apy):.2f}%",
            help="Current Lido staking APY",
        )

    with col2:
        st.metric(
            "7-Day APY",
            f"{float(current_apy + Decimal('0.1')):.2f}%",
            help="Trailing 7-day APY",
        )

    with col3:
        st.metric(
            "Yield Type",
            yield_explanation.title(),
            help=f"How yield is distributed for {output_token}",
        )

    # Projected earnings
    if staked_amount > 0:
        yearly_yield_eth = staked_amount * current_apy / Decimal("100")
        daily_yield_eth = yearly_yield_eth / Decimal("365")

        st.markdown("**Projected Earnings:**")
        st.markdown(f"- Daily: ~{float(daily_yield_eth):.6f} ETH (${float(daily_yield_eth * eth_price):.2f})")
        st.markdown(
            f"- Monthly: ~{float(daily_yield_eth * 30):.4f} ETH (${float(daily_yield_eth * 30 * eth_price):.2f})"
        )
        st.markdown(f"- Yearly: ~{float(yearly_yield_eth):.4f} ETH (${float(yearly_yield_eth * eth_price):,.2f})")

    st.caption("Lido staking rewards come from Ethereum consensus layer rewards. APY varies based on network activity.")


def _render_withdrawal_status(session_state: dict[str, Any]) -> None:
    """Render Lido withdrawal queue status."""
    withdrawal_pending = session_state.get("withdrawal_pending", False)
    withdrawal_request_id = session_state.get("withdrawal_request_id")
    withdrawal_ready = session_state.get("withdrawal_ready", False)

    col1, col2, col3 = st.columns(3)

    with col1:
        if withdrawal_pending:
            status = "Ready" if withdrawal_ready else "Pending"
        else:
            status = "None"
        st.metric(
            "Withdrawal Status",
            status,
            help="Status of withdrawal requests",
        )

    with col2:
        st.metric(
            "Queue Time",
            "3-5 days",
            help="Typical withdrawal queue duration",
        )

    with col3:
        if withdrawal_request_id:
            st.metric(
                "Request ID",
                str(withdrawal_request_id)[:8] + "...",
                help="Withdrawal request NFT ID",
            )
        else:
            st.metric("Request ID", "N/A")

    # Withdrawal explanation
    if withdrawal_ready:
        st.success("Withdrawal is ready! You can claim your ETH.")
    elif withdrawal_pending:
        st.warning("Withdrawal is pending. Please wait for the queue to process.")
    else:
        st.info("No active withdrawal request. To withdraw, initiate via Lido's withdrawal queue.")

    st.markdown("**Lido Withdrawal Process:**")
    st.markdown(
        """
        1. Request withdrawal (burns stETH/wstETH, receives NFT)
        2. Wait in queue (3-5 days typically)
        3. Claim ETH when ready
        """
    )


def _render_peg_status(session_state: dict[str, Any]) -> None:
    """Render stETH/ETH peg status."""
    # In production, would query actual DEX prices
    steth_eth_rate = Decimal("0.9995")  # stETH trading at slight discount
    peg_deviation = abs(Decimal("1") - steth_eth_rate) * Decimal("100")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            "stETH/ETH Rate",
            f"{float(steth_eth_rate):.4f}",
            help="Current stETH price in ETH",
        )

    with col2:
        st.metric(
            "Peg Deviation",
            f"{float(peg_deviation):.2f}%",
            help="Deviation from 1:1 peg",
        )

    with col3:
        if peg_deviation < Decimal("0.1"):
            st.success("Peg: Excellent")
        elif peg_deviation < Decimal("0.5"):
            st.info("Peg: Good")
        elif peg_deviation < Decimal("1.0"):
            st.warning("Peg: Fair")
        else:
            st.error("Peg: Depegged")

    st.caption(
        "stETH typically trades at a slight discount due to withdrawal queue. Large depegs may indicate market stress."
    )
