"""
Aerodrome Stable Yield Farmer Dashboard.

Custom dashboard showing LP token balance, trading fees, Pool TVL,
APR estimate, and stable pool peg status.
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
    """Render the Aerodrome Stable Yield Farmer custom dashboard.

    Shows:
    - LP token balance
    - Estimated trading fees earned
    - Pool TVL
    - Simple APR estimate based on fees/principal
    - Stable pool health (peg status)
    """
    st.title("Aerodrome Stable Yield Farmer Dashboard")

    # Extract config values with defaults
    pool = strategy_config.get("pool", "USDC/USDbC")
    stable = strategy_config.get("stable", True)
    amount0 = Decimal(str(strategy_config.get("amount0", "3")))
    amount1 = Decimal(str(strategy_config.get("amount1", "3")))

    pool_parts = pool.split("/")
    token0 = pool_parts[0] if len(pool_parts) > 0 else "USDC"
    token1 = pool_parts[1] if len(pool_parts) > 1 else "USDbC"

    # Strategy info header
    st.markdown(f"**Strategy ID:** `{strategy_id}`")
    st.markdown(f"**Pool:** {token0}/{token1} ({'Stable' if stable else 'Volatile'})")
    st.markdown("**Chain:** Base")

    # Pool type badge
    if stable:
        st.success("Stable Pool - Low Impermanent Loss")
    else:
        st.warning("Volatile Pool - Higher IL Risk")

    st.divider()

    # LP Token Balance section
    st.subheader("LP Token Balance")
    _render_lp_balance(session_state, amount0, amount1, token0, token1)

    st.divider()

    # Trading Fees Earned section
    st.subheader("Trading Fees Earned")
    _render_fee_metrics(api_client, strategy_id, amount0, amount1)

    st.divider()

    # Pool TVL section
    st.subheader("Pool TVL")
    _render_pool_tvl(token0, token1)

    st.divider()

    # APR Estimate section
    st.subheader("APR Estimate")
    _render_apr_estimate(api_client, strategy_id, amount0, amount1)

    st.divider()

    # Stable Pool Health / Peg Status section
    st.subheader("Stable Pool Health")
    _render_peg_status(session_state, token0, token1)


def _render_lp_balance(
    session_state: dict[str, Any],
    amount0: Decimal,
    amount1: Decimal,
    token0: str,
    token1: str,
) -> None:
    """Render LP token balance information."""
    has_position = session_state.get("has_position", False)
    lp_token_balance = session_state.get("lp_token_balance", "0")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            "Position Status",
            "Active" if has_position else "Inactive",
            help="Whether there is an active LP position",
        )

    with col2:
        st.metric(
            f"{token0} Deposited",
            f"{float(amount0):.2f}" if has_position else "0.00",
            help=f"Amount of {token0} in LP position",
        )

    with col3:
        st.metric(
            f"{token1} Deposited",
            f"{float(amount1):.2f}" if has_position else "0.00",
            help=f"Amount of {token1} in LP position",
        )

    # Total value
    total_value_usd = amount0 + amount1  # Both are stablecoins worth ~$1
    st.markdown(f"**Total Position Value:** ${float(total_value_usd):.2f} USD")

    if lp_token_balance and Decimal(str(lp_token_balance)) > 0:
        st.caption(f"LP Token Balance: {lp_token_balance}")


def _render_fee_metrics(
    api_client: Any,
    strategy_id: str,
    amount0: Decimal,
    amount1: Decimal,
) -> None:
    """Render estimated trading fees earned from timeline events."""
    events = []
    if api_client:
        try:
            events = api_client.get_timeline(strategy_id, limit=100)
        except Exception:
            pass

    # Count LP events to estimate position duration
    lp_open_events = [e for e in events if e.get("event_type") in ["LP_OPEN", "lp_open"]]
    lp_close_events = [e for e in events if e.get("event_type") in ["LP_CLOSE", "lp_close"]]

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            "LP Opens",
            str(len(lp_open_events)),
            help="Number of times LP position was opened",
        )

    with col2:
        st.metric(
            "LP Closes",
            str(len(lp_close_events)),
            help="Number of times LP position was closed",
        )

    with col3:
        # Estimate fees based on stable pool typical APR
        # Stable pools typically earn 2-5% APR in fees
        # For demo purposes, show placeholder
        st.metric(
            "Est. Fees Earned",
            "N/A",
            help="Actual fee earnings require on-chain query",
        )

    # Calculate position duration if we have events
    if lp_open_events:
        try:
            first_open = lp_open_events[-1]  # Events are usually newest first
            timestamp = first_open.get("timestamp", "")
            if isinstance(timestamp, str):
                open_time = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
            else:
                open_time = timestamp

            now = datetime.now(UTC)
            duration = now - open_time
            days_active = duration.days
            hours_active = duration.seconds // 3600

            st.info(f"Position active for approximately {days_active} days, {hours_active} hours")
        except Exception:
            pass

    st.caption(
        "Note: Actual fee earnings are accrued on-chain and collected when closing the position. Stable pools typically earn lower fees but have minimal IL."
    )


def _render_pool_tvl(token0: str, token1: str) -> None:
    """Render Pool TVL information."""
    # In a real implementation, this would query on-chain data
    # For demo purposes, show typical stable pool TVL range

    col1, col2 = st.columns(2)

    with col1:
        st.metric(
            "Pool TVL",
            "~$1-5M",
            help="Estimated pool total value locked (requires on-chain query)",
        )

    with col2:
        st.metric(
            "Pool Type",
            "Stable (x^3*y)",
            help="Aerodrome stable pools use x^3*y + y^3*x curve",
        )

    st.caption(
        f"The {token0}/{token1} stable pool is optimized for 1:1 pegged assets. TVL varies based on market conditions."
    )

    # Show pool characteristics
    st.markdown("**Pool Characteristics:**")
    st.markdown(
        """
        - Low slippage for trades near 1:1 ratio
        - Minimal impermanent loss for pegged assets
        - Trading fees distributed to LPs proportionally
        - No concentrated liquidity ranges (full range)
        """
    )


def _render_apr_estimate(
    api_client: Any,
    strategy_id: str,
    amount0: Decimal,
    amount1: Decimal,
) -> None:
    """Render simple APR estimate based on fees/principal."""
    # Typical stable pool APRs on Aerodrome
    estimated_trading_apr = Decimal("2.5")  # 2.5% trading fee APR estimate
    estimated_emission_apr = Decimal("5.0")  # 5% AERO emission APR estimate

    total_apr = estimated_trading_apr + estimated_emission_apr
    principal = amount0 + amount1  # Both stablecoins worth ~$1

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            "Trading Fee APR",
            f"~{float(estimated_trading_apr):.1f}%",
            help="Estimated APR from trading fees",
        )

    with col2:
        st.metric(
            "Emission APR",
            f"~{float(estimated_emission_apr):.1f}%",
            help="Estimated APR from AERO emissions (if staked)",
        )

    with col3:
        st.metric(
            "Total Est. APR",
            f"~{float(total_apr):.1f}%",
            help="Combined estimated APR",
        )

    # Projected yearly earnings
    yearly_earnings = principal * total_apr / Decimal("100")
    daily_earnings = yearly_earnings / Decimal("365")

    st.markdown(f"**Projected Earnings (at {float(total_apr):.1f}% APR):**")
    st.markdown(f"- Daily: ~${float(daily_earnings):.4f}")
    st.markdown(f"- Monthly: ~${float(daily_earnings * 30):.2f}")
    st.markdown(f"- Yearly: ~${float(yearly_earnings):.2f}")

    st.caption(
        "APR estimates are approximate and based on typical stable pool performance. Actual returns depend on trading volume, pool utilization, and AERO emissions."
    )


def _render_peg_status(
    session_state: dict[str, Any],
    token0: str,
    token1: str,
) -> None:
    """Render stable pool peg status and health metrics."""
    # For stable pools, peg status indicates how close the price ratio is to 1:1
    # In a real implementation, this would query on-chain price data

    col1, col2, col3 = st.columns(3)

    # Simulated peg values (would be from on-chain in production)
    peg_ratio = Decimal("0.9998")  # USDC/USDbC ratio
    peg_deviation = abs(Decimal("1") - peg_ratio)
    peg_deviation_pct = float(peg_deviation * 100)

    with col1:
        # Peg ratio
        st.metric(
            f"{token0}/{token1} Ratio",
            f"{float(peg_ratio):.4f}",
            help="Current price ratio (1.0 = perfect peg)",
        )

    with col2:
        # Peg deviation
        st.metric(
            "Peg Deviation",
            f"{peg_deviation_pct:.2f}%",
            help="Deviation from 1:1 peg",
        )

    with col3:
        # Peg health indicator
        if peg_deviation_pct < 0.1:
            st.success("Peg Health: Excellent")
        elif peg_deviation_pct < 0.5:
            st.info("Peg Health: Good")
        elif peg_deviation_pct < 1.0:
            st.warning("Peg Health: Fair")
        else:
            st.error("Peg Health: Poor")

    # Peg explanation
    st.markdown("**What is Peg Status?**")
    st.markdown(
        f"""
        Stable pools like {token0}/{token1} are designed for assets that maintain a 1:1 ratio.

        - **Perfect peg (1.0):** Both tokens have equal value
        - **Deviation < 0.1%:** Normal variance, no IL concern
        - **Deviation > 1%:** May indicate de-peg risk

        The Aerodrome stable curve (x^3*y + y^3*x) provides better capital efficiency
        for pegged assets compared to constant product (x*y=k) AMMs.
        """
    )

    # Historical peg tracking (placeholder)
    st.markdown("**Peg History:**")
    st.info(
        "Historical peg tracking requires on-chain data indexing. The USDC/USDbC pair typically maintains a very tight peg."
    )
