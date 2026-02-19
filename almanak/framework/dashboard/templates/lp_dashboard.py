"""
Liquidity Provider (LP) Dashboard Template.

Reusable template for creating dashboards for LP strategies on concentrated
liquidity protocols like Uniswap V3, PancakeSwap V3, TraderJoe V2, and Aerodrome.

Usage:
    from almanak.framework.dashboard.templates import LPDashboardConfig, render_lp_dashboard

    config = LPDashboardConfig(
        protocol="uniswap_v3",
        token0="WETH",
        token1="USDC",
        fee_tier="0.30%",
    )

    def render_custom_dashboard(strategy_id, strategy_config, api_client, session_state):
        render_lp_dashboard(strategy_id, strategy_config, session_state, config)
"""

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import streamlit as st

from almanak.framework.dashboard.plots import (
    plot_fee_accumulation,
    plot_impermanent_loss,
    plot_liquidity_distribution,
    plot_position_range_status,
    plot_positions_over_time,
)


@dataclass
class LPDashboardConfig:
    """Configuration for an LP dashboard.

    Attributes:
        protocol: Protocol name (e.g., "uniswap_v3", "aerodrome", "traderjoe_v2")
        token0: First token symbol
        token1: Second token symbol
        fee_tier: Fee tier display string (e.g., "0.30%")
        chain: Chain name
        show_liquidity_distribution: Whether to show liquidity distribution chart
        show_position_history: Whether to show position history chart
        show_impermanent_loss: Whether to show IL tracking
        show_fee_accumulation: Whether to show fee accumulation chart
        invert_prices: Whether to invert price display
        position_bounds_ratio: Ratio for position bounds lines (None to disable)
    """

    protocol: str = "uniswap_v3"
    token0: str = "WETH"
    token1: str = "USDC"
    fee_tier: str = "0.30%"
    chain: str = "arbitrum"
    show_liquidity_distribution: bool = True
    show_position_history: bool = True
    show_impermanent_loss: bool = True
    show_fee_accumulation: bool = True
    invert_prices: bool = False
    position_bounds_ratio: float | None = 0.8


def render_lp_dashboard(
    strategy_id: str,
    strategy_config: dict[str, Any],
    session_state: dict[str, Any],
    config: LPDashboardConfig,
) -> None:
    """Render an LP strategy dashboard using the provided configuration.

    Args:
        strategy_id: The strategy identifier
        strategy_config: Strategy configuration dictionary
        session_state: Current session state with position data
        config: LPDashboardConfig for this dashboard
    """
    # Extract config overrides
    token0 = strategy_config.get("token0", config.token0)
    token1 = strategy_config.get("token1", config.token1)
    chain = strategy_config.get("chain", config.chain)
    protocol = strategy_config.get("protocol", config.protocol)
    fee_tier = strategy_config.get("fee_tier", config.fee_tier)

    st.title(f"{protocol.replace('_', ' ').title()} LP Dashboard")

    st.markdown(f"**Strategy ID:** `{strategy_id}`")
    st.markdown(f"**Pool:** {token0}/{token1} ({fee_tier})")
    st.markdown(f"**Chain:** {chain.title()}")

    st.divider()

    # Position Status Section
    st.subheader("Position Status")
    _render_position_status(session_state, config)

    st.divider()

    # Helper to check for non-empty data (handles DataFrames, lists, and other types)
    def _has_data(data: object) -> bool:
        if data is None:
            return False
        if hasattr(data, "empty"):  # pandas DataFrame/Series
            return not data.empty
        if hasattr(data, "__len__"):  # lists, tuples, etc.
            return len(data) > 0
        return True

    # Position Range Status
    current_price = session_state.get("current_price")
    lower_price = session_state.get("lower_price")
    upper_price = session_state.get("upper_price")
    if current_price is not None and lower_price is not None and upper_price is not None:
        fig = plot_position_range_status(
            current_price=float(current_price),
            lower_bound=float(lower_price),
            upper_bound=float(upper_price),
            token_pair=f"{token0}/{token1}",
            invert_prices=config.invert_prices,
        )
        st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # Liquidity Distribution
    tick_data = session_state.get("tick_data")
    lower_tick = session_state.get("lower_tick")
    upper_tick = session_state.get("upper_tick")
    position_bounds = (lower_tick, upper_tick) if lower_tick is not None and upper_tick is not None else None

    if config.show_liquidity_distribution and _has_data(tick_data):
        st.subheader("Liquidity Distribution")
        fig = plot_liquidity_distribution(
            tick_data=tick_data,
            current_tick=session_state.get("current_tick", 0),
            position_bounds=position_bounds,
            token_pair=f"{token0}/{token1}",
            fee_tier=fee_tier,
            invert_prices=config.invert_prices,
        )
        st.plotly_chart(fig, use_container_width=True)
    elif config.show_liquidity_distribution:
        st.info("Liquidity distribution data not available")

    st.divider()

    # Position History
    if config.show_position_history:
        st.subheader("Position History")
        position_history = session_state.get("position_history")
        price_history = session_state.get("price_history")
        if _has_data(position_history) and _has_data(price_history) and position_history is not None:
            fig = plot_positions_over_time(
                positions=position_history,
                price_data=price_history,
                invert_prices=config.invert_prices,
                price_bounds_ratio=config.position_bounds_ratio,
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Position history data not available")

    st.divider()

    # Metrics Section
    col1, col2 = st.columns(2)

    with col1:
        # Fee Accumulation
        fee_history = session_state.get("fee_history")
        if config.show_fee_accumulation and _has_data(fee_history):
            st.subheader("Fee Accumulation")
            fig = plot_fee_accumulation(
                fee_data=fee_history,
                show_cumulative=True,
                fee_unit="USD",
            )
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        # Impermanent Loss
        il_history = session_state.get("il_history")
        if config.show_impermanent_loss and _has_data(il_history):
            st.subheader("Impermanent Loss")
            fig = plot_impermanent_loss(
                il_data=il_history,
                show_cumulative=True,
            )
            st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # Performance Summary
    st.subheader("Performance Summary")
    _render_performance_summary(session_state)


def _render_position_status(
    session_state: dict[str, Any],
    config: LPDashboardConfig,
) -> None:
    """Render the position status section."""
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        position_id = session_state.get("position_id", "N/A")
        st.metric("Position ID", str(position_id)[:10] + "..." if len(str(position_id)) > 10 else position_id)

    with col2:
        is_active = session_state.get("is_active", False)
        status = "Active" if is_active else "Inactive"
        st.metric("Status", status)

    with col3:
        in_range = session_state.get("in_range", None)
        if in_range is not None:
            range_status = "In Range" if in_range else "Out of Range"
            st.metric("Range Status", range_status)
        else:
            st.metric("Range Status", "Unknown")

    with col4:
        current_price = session_state.get("current_price")
        if current_price is not None:
            st.metric("Current Price", f"${float(current_price):,.4f}")
        else:
            st.metric("Current Price", "N/A")

    # Second row of metrics
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        lower_price = session_state.get("lower_price")
        if lower_price is not None:
            st.metric("Lower Bound", f"${float(lower_price):,.4f}")
        else:
            st.metric("Lower Bound", "N/A")

    with col2:
        upper_price = session_state.get("upper_price")
        if upper_price is not None:
            st.metric("Upper Bound", f"${float(upper_price):,.4f}")
        else:
            st.metric("Upper Bound", "N/A")

    with col3:
        token0_amount = session_state.get("token0_amount", 0)
        st.metric(config.token0, f"{float(token0_amount):.4f}")

    with col4:
        token1_amount = session_state.get("token1_amount", 0)
        st.metric(config.token1, f"{float(token1_amount):,.2f}")


def _render_performance_summary(
    session_state: dict[str, Any],
) -> None:
    """Render the performance summary section."""
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        total_fees = Decimal(str(session_state.get("total_fees_usd", "0")))
        st.metric("Total Fees", f"${float(total_fees):,.2f}")

    with col2:
        il = Decimal(str(session_state.get("impermanent_loss_pct", "0")))
        st.metric("Impermanent Loss", f"{float(il):+.2f}%")

    with col3:
        net_pnl = Decimal(str(session_state.get("net_pnl_usd", "0")))
        st.metric("Net PnL", f"${float(net_pnl):+,.2f}")

    with col4:
        position_value = Decimal(str(session_state.get("position_value_usd", "0")))
        st.metric("Position Value", f"${float(position_value):,.2f}")


# Pre-configured templates for common LP protocols


def get_uniswap_v3_config(
    token0: str = "WETH",
    token1: str = "USDC",
    fee_tier: str = "0.30%",
    chain: str = "arbitrum",
) -> LPDashboardConfig:
    """Get pre-configured Uniswap V3 LP dashboard config."""
    return LPDashboardConfig(
        protocol="uniswap_v3",
        token0=token0,
        token1=token1,
        fee_tier=fee_tier,
        chain=chain,
    )


def get_aerodrome_config(
    token0: str = "WETH",
    token1: str = "USDC",
    pool_type: str = "volatile",
    chain: str = "base",
) -> LPDashboardConfig:
    """Get pre-configured Aerodrome LP dashboard config."""
    return LPDashboardConfig(
        protocol="aerodrome",
        token0=token0,
        token1=token1,
        fee_tier=pool_type,
        chain=chain,
    )


def get_traderjoe_v2_config(
    token0: str = "WAVAX",
    token1: str = "USDC",
    bin_step: str = "20",
    chain: str = "avalanche",
) -> LPDashboardConfig:
    """Get pre-configured TraderJoe V2 LP dashboard config."""
    return LPDashboardConfig(
        protocol="traderjoe_v2",
        token0=token0,
        token1=token1,
        fee_tier=f"Bin Step {bin_step}",
        chain=chain,
    )


def get_pancakeswap_v3_config(
    token0: str = "WBNB",
    token1: str = "USDT",
    fee_tier: str = "0.25%",
    chain: str = "bsc",
) -> LPDashboardConfig:
    """Get pre-configured PancakeSwap V3 LP dashboard config."""
    return LPDashboardConfig(
        protocol="pancakeswap_v3",
        token0=token0,
        token1=token1,
        fee_tier=fee_tier,
        chain=chain,
    )
