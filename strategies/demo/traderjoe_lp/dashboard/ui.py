"""
TraderJoe LP Strategy Dashboard.

Custom dashboard showing LP token IDs, underlying token amounts,
bin distribution, active bin status, fees earned, and estimated APR.
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
    """Render the TraderJoe LP custom dashboard.

    Shows:
    - LP token IDs
    - Underlying token amounts
    - Bin distribution visualization
    - Active bin status
    - Fees earned
    - Estimated APR
    """
    st.title("TraderJoe LP Strategy Dashboard")

    # Extract config values
    pool = strategy_config.get("pool", "AVAX/USDC")
    bin_step = strategy_config.get("bin_step", 20)
    pool_parts = pool.split("/")
    token0 = pool_parts[0] if len(pool_parts) > 0 else "AVAX"
    token1 = pool_parts[1] if len(pool_parts) > 1 else "USDC"

    # Strategy info header
    st.markdown(f"**Strategy ID:** `{strategy_id}`")
    st.markdown(f"**Pool:** {token0}/{token1}")
    st.markdown("**Protocol:** TraderJoe V2 (Liquidity Book)")
    st.markdown("**Chain:** Avalanche")
    st.markdown(f"**Bin Step:** {bin_step} bps")

    st.divider()

    # LP Position section
    st.subheader("LP Position")
    _render_lp_position(session_state, token0, token1)

    st.divider()

    # Bin Distribution section
    st.subheader("Bin Distribution")
    _render_bin_distribution(session_state, token0, token1)

    st.divider()

    # Active Bin section
    st.subheader("Active Bin Status")
    _render_active_bin(session_state, bin_step)

    st.divider()

    # Fees & APR section
    st.subheader("Fees & APR")
    _render_fees_apr(session_state)


def _render_lp_position(session_state: dict[str, Any], token0: str, token1: str) -> None:
    """Render LP position details."""
    has_position = session_state.get("has_position", False)
    position_ids = session_state.get("position_ids", [])
    amount0 = Decimal(str(session_state.get("amount0", "0")))
    amount1 = Decimal(str(session_state.get("amount1", "0")))

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            "Position Status",
            "Active" if has_position else "Inactive",
            help="Whether there is an active LP position",
        )

    with col2:
        st.metric(
            f"{token0} Amount",
            f"{float(amount0):.4f}",
            help=f"Amount of {token0} in LP",
        )

    with col3:
        st.metric(
            f"{token1} Amount",
            f"{float(amount1):.2f}",
            help=f"Amount of {token1} in LP",
        )

    # Position IDs
    if position_ids:
        st.caption(
            f"Position IDs: {', '.join(str(p) for p in position_ids[:5])}{'...' if len(position_ids) > 5 else ''}"
        )

    # Total value estimate
    token0_price = Decimal("40") if token0 == "AVAX" else Decimal("3400")  # AVAX price
    total_value = amount0 * token0_price + amount1
    st.metric(
        "Total Position Value",
        f"${float(total_value):,.2f}",
        help="Estimated USD value",
    )


def _render_bin_distribution(session_state: dict[str, Any], token0: str, token1: str) -> None:
    """Render bin distribution visualization."""
    bins = session_state.get("bins", [])
    num_bins = session_state.get("num_bins", 0)
    lower_bin = session_state.get("lower_bin", 0)
    upper_bin = session_state.get("upper_bin", 0)

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            "Number of Bins",
            str(num_bins),
            help="Total bins with liquidity",
        )

    with col2:
        st.metric(
            "Lower Bin ID",
            str(lower_bin),
            help="Lowest bin with liquidity",
        )

    with col3:
        st.metric(
            "Upper Bin ID",
            str(upper_bin),
            help="Highest bin with liquidity",
        )

    # Bin distribution explanation
    st.markdown("**Liquidity Book Bins:**")
    st.markdown(
        """
        - TraderJoe V2 uses discrete price bins (Liquidity Book)
        - Each bin represents a specific price point
        - Only the active bin earns fees
        - Liquidity can be distributed across multiple bins
        """
    )

    # Placeholder for bin visualization
    if bins:
        st.info(f"Liquidity distributed across {num_bins} bins from {lower_bin} to {upper_bin}")
    else:
        st.info("No bin data available. Position may not be active.")


def _render_active_bin(session_state: dict[str, Any], bin_step: int) -> None:
    """Render active bin status."""
    active_bin = session_state.get("active_bin", 0)
    in_range = session_state.get("in_range", False)
    lower_bin = session_state.get("lower_bin", 0)
    upper_bin = session_state.get("upper_bin", 0)

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            "Active Bin ID",
            str(active_bin),
            help="Current trading bin (where fees are earned)",
        )

    with col2:
        st.metric(
            "Bin Step",
            f"{bin_step} bps",
            help="Price increment per bin (0.2% = 20 bps)",
        )

    with col3:
        if in_range:
            st.success("In Range")
        else:
            st.warning("Out of Range")

    # Range status explanation
    if in_range:
        st.success(f"Active bin ({active_bin}) is within your range ({lower_bin}-{upper_bin}). Earning fees.")
    else:
        st.warning(f"Active bin ({active_bin}) is outside your range ({lower_bin}-{upper_bin}). Not earning fees.")
        st.info("Consider rebalancing to bring position back in range.")


def _render_fees_apr(session_state: dict[str, Any]) -> None:
    """Render fees earned and APR estimate."""
    fees_earned_0 = Decimal(str(session_state.get("fees_earned_0", "0")))
    fees_earned_1 = Decimal(str(session_state.get("fees_earned_1", "0")))

    # Typical TraderJoe APRs
    fee_apr = Decimal("25")  # Concentrated liquidity can have higher APR

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            "Token0 Fees",
            f"{float(fees_earned_0):.6f}",
            help="Unclaimed fees in token0",
        )

    with col2:
        st.metric(
            "Token1 Fees",
            f"{float(fees_earned_1):.4f}",
            help="Unclaimed fees in token1",
        )

    with col3:
        st.metric(
            "Est. APR",
            f"~{float(fee_apr):.0f}%",
            help="Estimated APR (varies with volume)",
        )

    st.caption(
        "APR depends on trading volume and position range. Tighter ranges = higher APR when in range, but higher IL risk."
    )
