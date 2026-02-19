"""
Aerodrome LP Strategy Dashboard.

Custom dashboard showing LP token balance, underlying token amounts,
trading fees earned, pool TVL, and estimated APR.
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
    """Render the Aerodrome LP custom dashboard.

    Shows:
    - LP token balance
    - Underlying token amounts
    - Trading fees earned
    - Pool TVL
    - Estimated APR
    """
    st.title("Aerodrome LP Strategy Dashboard")

    # Extract config values
    pool = strategy_config.get("pool", "WETH/USDC")
    stable = strategy_config.get("stable", False)
    pool_parts = pool.split("/")
    token0 = pool_parts[0] if len(pool_parts) > 0 else "WETH"
    token1 = pool_parts[1] if len(pool_parts) > 1 else "USDC"

    # Strategy info header
    st.markdown(f"**Strategy ID:** `{strategy_id}`")
    st.markdown(f"**Pool:** {token0}/{token1}")
    st.markdown("**Protocol:** Aerodrome")
    st.markdown("**Chain:** Base")

    if stable:
        st.success("Stable Pool (x^3*y curve)")
    else:
        st.info("Volatile Pool (x*y=k curve)")

    st.divider()

    # LP Position section
    st.subheader("LP Position")
    _render_lp_position(session_state, token0, token1)

    st.divider()

    # Trading Fees section
    st.subheader("Trading Fees")
    _render_trading_fees(session_state)

    st.divider()

    # Pool Metrics section
    st.subheader("Pool Metrics")
    _render_pool_metrics(token0, token1, stable)

    st.divider()

    # APR Estimate section
    st.subheader("APR Estimate")
    _render_apr_estimate(session_state, stable)


def _render_lp_position(session_state: dict[str, Any], token0: str, token1: str) -> None:
    """Render LP position details."""
    has_position = session_state.get("has_position", False)
    Decimal(str(session_state.get("lp_balance", "0")))
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

    # Estimate total value
    # Assuming token1 is a stablecoin and token0 price
    token0_price = Decimal("3400") if token0 == "WETH" else Decimal("1")
    total_value = amount0 * token0_price + amount1

    st.metric(
        "Total Position Value",
        f"${float(total_value):,.2f}",
        help="Estimated USD value of LP position",
    )


def _render_trading_fees(session_state: dict[str, Any]) -> None:
    """Render trading fees earned."""
    fees_earned_0 = Decimal(str(session_state.get("fees_earned_0", "0")))
    fees_earned_1 = Decimal(str(session_state.get("fees_earned_1", "0")))

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            "Unclaimed Fees (Token0)",
            f"{float(fees_earned_0):.6f}",
            help="Trading fees earned in token0",
        )

    with col2:
        st.metric(
            "Unclaimed Fees (Token1)",
            f"{float(fees_earned_1):.4f}",
            help="Trading fees earned in token1",
        )

    with col3:
        # Estimate total fees in USD
        token0_price = Decimal("3400")
        total_fees_usd = fees_earned_0 * token0_price + fees_earned_1
        st.metric(
            "Total Fees (USD)",
            f"${float(total_fees_usd):,.2f}",
            help="Estimated USD value of unclaimed fees",
        )

    st.caption("Fees accumulate from trading activity. Claim fees by closing/reopening position or via Aerodrome UI.")


def _render_pool_metrics(token0: str, token1: str, stable: bool) -> None:
    """Render pool TVL and volume metrics."""
    # Placeholder metrics
    pool_tvl = Decimal("10000000")  # $10M
    daily_volume = Decimal("500000")  # $500K

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            "Pool TVL",
            f"${float(pool_tvl / Decimal('1000000')):,.1f}M",
            help="Total value locked in pool",
        )

    with col2:
        st.metric(
            "24h Volume",
            f"${float(daily_volume / Decimal('1000')):,.0f}K",
            help="Trading volume in last 24 hours",
        )

    with col3:
        fee_tier = "0.01%" if stable else "0.30%"
        st.metric(
            "Fee Tier",
            fee_tier,
            help="Trading fee percentage",
        )


def _render_apr_estimate(session_state: dict[str, Any], stable: bool) -> None:
    """Render APR estimates."""
    # Typical Aerodrome APRs
    trading_apr = Decimal("5") if stable else Decimal("15")
    emission_apr = Decimal("10")  # AERO emissions
    total_apr = trading_apr + emission_apr

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            "Trading Fee APR",
            f"~{float(trading_apr):.1f}%",
            help="APR from trading fees",
        )

    with col2:
        st.metric(
            "AERO Emission APR",
            f"~{float(emission_apr):.1f}%",
            help="APR from AERO rewards (if staked)",
        )

    with col3:
        st.metric(
            "Total APR",
            f"~{float(total_apr):.1f}%",
            help="Combined APR",
        )

    st.caption(
        "APR varies based on trading volume, TVL, and AERO emissions. Stake LP tokens in gauges to earn AERO rewards."
    )
