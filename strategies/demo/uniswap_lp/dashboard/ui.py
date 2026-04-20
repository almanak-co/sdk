"""
Uniswap LP Strategy Dashboard.

Custom dashboard showing NFT position ID, underlying token amounts,
price range, in-range status, fees earned, and estimated APR.
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
    """Render the Uniswap LP custom dashboard.

    Shows:
    - NFT position ID
    - Underlying token amounts
    - Price range (lower/upper tick)
    - Current price vs range (in-range/out-of-range)
    - Fees earned (uncollected)
    - Estimated APR
    """
    st.title("Uniswap V3 LP Strategy Dashboard")

    # Extract config values
    pool = strategy_config.get("pool", "WETH/USDC")
    fee_tier = strategy_config.get("fee_tier", 3000)  # 0.3%
    pool_parts = pool.split("/")
    token0 = pool_parts[0] if len(pool_parts) > 0 else "WETH"
    token1 = pool_parts[1] if len(pool_parts) > 1 else "USDC"

    # Strategy info header
    st.markdown(f"**Strategy ID:** `{strategy_id}`")
    st.markdown(f"**Pool:** {token0}/{token1}")
    st.markdown("**Protocol:** Uniswap V3")
    st.markdown("**Chain:** Arbitrum")
    st.markdown(f"**Fee Tier:** {fee_tier / 10000}%")

    st.divider()

    # Position Overview section
    st.subheader("Position Overview")
    _render_position_overview(session_state, token0, token1)

    st.divider()

    # Price Range section
    st.subheader("Price Range")
    _render_price_range(session_state, token0, token1)

    st.divider()

    # Fees section
    st.subheader("Uncollected Fees")
    _render_fees(session_state, token0, token1)

    st.divider()

    # APR section
    st.subheader("APR Estimate")
    _render_apr(session_state, fee_tier)


def _render_position_overview(session_state: dict[str, Any], token0: str, token1: str) -> None:
    """Render position overview with NFT ID and token amounts."""
    has_position = session_state.get("has_position", False)
    position_id = session_state.get("position_id", "N/A")
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
            help=f"Amount of {token0} in position",
        )

    with col3:
        st.metric(
            f"{token1} Amount",
            f"{float(amount1):.2f}",
            help=f"Amount of {token1} in position",
        )

    # NFT Position ID
    if position_id != "N/A":
        st.caption(f"NFT Position ID: {position_id}")

    # Total value
    token0_price = Decimal("3400") if token0 == "WETH" else Decimal("1")
    total_value = amount0 * token0_price + amount1
    st.metric(
        "Total Position Value",
        f"${float(total_value):,.2f}",
        help="Estimated USD value",
    )


def _render_price_range(session_state: dict[str, Any], token0: str, token1: str) -> None:
    """Render price range and in-range status."""
    lower_price = Decimal(str(session_state.get("lower_price", "3000")))
    upper_price = Decimal(str(session_state.get("upper_price", "3800")))
    current_price = Decimal(str(session_state.get("current_price", "3400")))
    in_range = session_state.get("in_range", True)

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            "Lower Price",
            f"${float(lower_price):,.0f}",
            help=f"Lower bound ({token0}/{token1})",
        )

    with col2:
        st.metric(
            "Current Price",
            f"${float(current_price):,.0f}",
            help=f"Current {token0} price",
        )

    with col3:
        st.metric(
            "Upper Price",
            f"${float(upper_price):,.0f}",
            help=f"Upper bound ({token0}/{token1})",
        )

    # Range width
    range_width = (upper_price - lower_price) / current_price * Decimal("100")
    st.markdown(f"**Range Width:** {float(range_width):.1f}% of current price")

    # In-range status with visual indicator
    if in_range:
        # Calculate position within range
        position_in_range = (current_price - lower_price) / (upper_price - lower_price) * Decimal("100")
        st.success(f"IN RANGE - Price at {float(position_in_range):.0f}% of range")
        st.progress(float(position_in_range) / 100)
    else:
        if current_price < lower_price:
            st.error("OUT OF RANGE - Price below lower bound (100% in token1)")
        else:
            st.error("OUT OF RANGE - Price above upper bound (100% in token0)")
        st.warning("Position is not earning fees. Consider rebalancing.")


def _render_fees(session_state: dict[str, Any], token0: str, token1: str) -> None:
    """Render uncollected fees."""
    fees0 = Decimal(str(session_state.get("fees_token0", "0")))
    fees1 = Decimal(str(session_state.get("fees_token1", "0")))

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            f"{token0} Fees",
            f"{float(fees0):.6f}",
            help=f"Uncollected {token0} fees",
        )

    with col2:
        st.metric(
            f"{token1} Fees",
            f"{float(fees1):.4f}",
            help=f"Uncollected {token1} fees",
        )

    with col3:
        token0_price = Decimal("3400") if token0 == "WETH" else Decimal("1")
        total_fees_usd = fees0 * token0_price + fees1
        st.metric(
            "Total Fees (USD)",
            f"${float(total_fees_usd):,.2f}",
            help="Total uncollected fees in USD",
        )

    st.caption("Fees accumulate continuously. Collect via Uniswap interface or by closing position.")


def _render_apr(session_state: dict[str, Any], fee_tier: int) -> None:
    """Render APR estimate."""
    # APR varies significantly based on range width and volume
    # Tighter ranges = higher APR when in range
    range_factor = Decimal("2.0")  # Assume 2x concentration vs full range
    base_apr = Decimal("10") * range_factor  # ~20% for concentrated position

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            "Fee Tier",
            f"{fee_tier / 10000}%",
            help="Trading fee per swap",
        )

    with col2:
        st.metric(
            "Est. APR (in range)",
            f"~{float(base_apr):.0f}%",
            help="Estimated APR when position is in range",
        )

    with col3:
        # Liquidity utilization (time in range)
        time_in_range = Decimal("85")  # Placeholder
        effective_apr = base_apr * time_in_range / Decimal("100")
        st.metric(
            "Effective APR",
            f"~{float(effective_apr):.0f}%",
            help="APR adjusted for time in range",
        )

    st.markdown("**APR Factors:**")
    st.markdown(
        """
        - Narrower range = higher APR (when in range)
        - Time in range affects realized yield
        - Higher fee tier pools = more fees, less volume
        - Impermanent loss can offset fee gains
        """
    )
