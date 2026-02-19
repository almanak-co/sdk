"""
Lending Protocol Dashboard Template.

Reusable template for creating dashboards for lending strategies on protocols
like Aave V3, Morpho Blue, Compound V3, and Spark.

Usage:
    from almanak.framework.dashboard.templates import LendingDashboardConfig, render_lending_dashboard

    config = LendingDashboardConfig(
        protocol="aave_v3",
        collateral_token="WETH",
        borrow_token="USDC",
    )

    def render_custom_dashboard(strategy_id, strategy_config, api_client, session_state):
        render_lending_dashboard(strategy_id, strategy_config, session_state, config)
"""

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import streamlit as st

from almanak.framework.dashboard.plots import (
    plot_borrow_utilization,
    plot_collateral_breakdown,
    plot_health_factor_gauge,
    plot_lending_rates_comparison,
    plot_ltv_ratio,
)


@dataclass
class LendingDashboardConfig:
    """Configuration for a lending dashboard.

    Attributes:
        protocol: Protocol name (e.g., "aave_v3", "morpho_blue", "compound_v3")
        collateral_token: Primary collateral token symbol
        borrow_token: Primary borrow token symbol
        chain: Chain name
        liquidation_threshold: Health factor threshold for liquidation (default 1.0)
        safe_threshold: Health factor threshold considered safe (default 1.5)
        max_ltv: Maximum LTV ratio (default 0.8)
        liquidation_ltv: LTV at which liquidation occurs (default 0.85)
        show_health_factor: Whether to show health factor gauge
        show_ltv: Whether to show LTV ratio visualization
        show_collateral_breakdown: Whether to show collateral breakdown
        show_rate_comparison: Whether to show rate comparison (for multi-protocol)
    """

    protocol: str = "aave_v3"
    collateral_token: str = "WETH"
    borrow_token: str = "USDC"
    chain: str = "arbitrum"
    liquidation_threshold: float = 1.0
    safe_threshold: float = 1.5
    max_ltv: float = 0.8
    liquidation_ltv: float = 0.85
    show_health_factor: bool = True
    show_ltv: bool = True
    show_collateral_breakdown: bool = True
    show_rate_comparison: bool = False


def render_lending_dashboard(
    strategy_id: str,
    strategy_config: dict[str, Any],
    session_state: dict[str, Any],
    config: LendingDashboardConfig,
) -> None:
    """Render a lending strategy dashboard using the provided configuration.

    Args:
        strategy_id: The strategy identifier
        strategy_config: Strategy configuration dictionary
        session_state: Current session state with position data
        config: LendingDashboardConfig for this dashboard
    """
    # Extract config overrides
    collateral_token = strategy_config.get("collateral_token", config.collateral_token)
    borrow_token = strategy_config.get("borrow_token", config.borrow_token)
    chain = strategy_config.get("chain", config.chain)
    protocol = strategy_config.get("protocol", config.protocol)

    st.title(f"{protocol.replace('_', ' ').title()} Lending Dashboard")

    st.markdown(f"**Strategy ID:** `{strategy_id}`")
    st.markdown(f"**Collateral:** {collateral_token} | **Borrow:** {borrow_token}")
    st.markdown(f"**Chain:** {chain.title()}")

    st.divider()

    # Health Factor and LTV Section
    col1, col2 = st.columns(2)

    with col1:
        if config.show_health_factor:
            health_factor = float(session_state.get("health_factor", 2.0))
            fig = plot_health_factor_gauge(
                health_factor=health_factor,
                liquidation_threshold=config.liquidation_threshold,
                safe_threshold=config.safe_threshold,
            )
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        if config.show_ltv:
            current_ltv = float(session_state.get("ltv", 0.5))
            fig = plot_ltv_ratio(
                current_ltv=current_ltv,
                max_ltv=config.max_ltv,
                liquidation_ltv=config.liquidation_ltv,
            )
            st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # Position Details
    st.subheader("Position Details")
    _render_position_details(session_state, collateral_token, borrow_token)

    st.divider()

    # Collateral Breakdown
    if config.show_collateral_breakdown:
        st.subheader("Collateral Breakdown")
        collateral_assets = session_state.get("collateral_assets", {})
        if collateral_assets:
            fig = plot_collateral_breakdown(
                assets=collateral_assets,
                show_values=True,
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            # Show single collateral
            collateral_value = float(session_state.get("collateral_value_usd", 0))
            if collateral_value > 0:
                fig = plot_collateral_breakdown(
                    assets={collateral_token: collateral_value},
                    show_values=True,
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No collateral data available")

    st.divider()

    # Borrow Utilization
    st.subheader("Borrow Utilization")
    borrowed = float(session_state.get("borrowed_value_usd", 0))
    available = float(session_state.get("available_to_borrow_usd", 0))
    if borrowed > 0 or available > 0:
        fig = plot_borrow_utilization(
            borrowed=borrowed,
            available=available,
            asset_symbol=borrow_token,
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No borrow utilization data available")

    st.divider()

    # Rate Comparison (optional)
    if config.show_rate_comparison and session_state.get("rate_comparison"):
        st.subheader("Rate Comparison")
        rate_data = session_state["rate_comparison"]
        fig = plot_lending_rates_comparison(
            protocols=rate_data.get("protocols", []),
            supply_rates=rate_data.get("supply_rates", []),
            borrow_rates=rate_data.get("borrow_rates", []),
            asset_symbol=borrow_token,
        )
        st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # Performance Summary
    st.subheader("Performance Summary")
    _render_performance_summary(session_state)


def _render_position_details(
    session_state: dict[str, Any],
    collateral_token: str,
    borrow_token: str,
) -> None:
    """Render the position details section."""
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        collateral_amount = Decimal(str(session_state.get("collateral_amount", "0")))
        st.metric(f"Collateral ({collateral_token})", f"{float(collateral_amount):.4f}")

    with col2:
        collateral_value = Decimal(str(session_state.get("collateral_value_usd", "0")))
        st.metric("Collateral Value", f"${float(collateral_value):,.2f}")

    with col3:
        borrowed_amount = Decimal(str(session_state.get("borrowed_amount", "0")))
        st.metric(f"Borrowed ({borrow_token})", f"{float(borrowed_amount):,.2f}")

    with col4:
        borrowed_value = Decimal(str(session_state.get("borrowed_value_usd", "0")))
        st.metric("Borrowed Value", f"${float(borrowed_value):,.2f}")

    # Second row
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        supply_apy = Decimal(str(session_state.get("supply_apy", "0")))
        st.metric("Supply APY", f"{float(supply_apy) * 100:.2f}%")

    with col2:
        borrow_apy = Decimal(str(session_state.get("borrow_apy", "0")))
        st.metric("Borrow APY", f"{float(borrow_apy) * 100:.2f}%")

    with col3:
        net_apy = Decimal(str(session_state.get("net_apy", "0")))
        st.metric("Net APY", f"{float(net_apy) * 100:+.2f}%")

    with col4:
        leverage = Decimal(str(session_state.get("leverage", "1")))
        st.metric("Leverage", f"{float(leverage):.2f}x")


def _render_performance_summary(session_state: dict[str, Any]) -> None:
    """Render the performance summary section."""
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        interest_earned = Decimal(str(session_state.get("interest_earned_usd", "0")))
        st.metric("Interest Earned", f"${float(interest_earned):,.2f}")

    with col2:
        interest_paid = Decimal(str(session_state.get("interest_paid_usd", "0")))
        st.metric("Interest Paid", f"${float(interest_paid):,.2f}")

    with col3:
        net_interest = Decimal(str(session_state.get("net_interest_usd", "0")))
        st.metric("Net Interest", f"${float(net_interest):+,.2f}")

    with col4:
        total_pnl = Decimal(str(session_state.get("total_pnl_usd", "0")))
        st.metric("Total PnL", f"${float(total_pnl):+,.2f}")


# Pre-configured templates for common lending protocols


def get_aave_v3_config(
    collateral_token: str = "WETH",
    borrow_token: str = "USDC",
    chain: str = "arbitrum",
) -> LendingDashboardConfig:
    """Get pre-configured Aave V3 lending dashboard config."""
    return LendingDashboardConfig(
        protocol="aave_v3",
        collateral_token=collateral_token,
        borrow_token=borrow_token,
        chain=chain,
        max_ltv=0.80,
        liquidation_ltv=0.825,
    )


def get_morpho_blue_config(
    collateral_token: str = "wstETH",
    borrow_token: str = "USDC",
    chain: str = "ethereum",
) -> LendingDashboardConfig:
    """Get pre-configured Morpho Blue lending dashboard config."""
    return LendingDashboardConfig(
        protocol="morpho_blue",
        collateral_token=collateral_token,
        borrow_token=borrow_token,
        chain=chain,
        max_ltv=0.77,
        liquidation_ltv=0.80,
    )


def get_compound_v3_config(
    collateral_token: str = "WETH",
    borrow_token: str = "USDC",
    chain: str = "ethereum",
) -> LendingDashboardConfig:
    """Get pre-configured Compound V3 lending dashboard config."""
    return LendingDashboardConfig(
        protocol="compound_v3",
        collateral_token=collateral_token,
        borrow_token=borrow_token,
        chain=chain,
        max_ltv=0.83,
        liquidation_ltv=0.90,
    )


def get_spark_config(
    collateral_token: str = "WETH",
    borrow_token: str = "DAI",
    chain: str = "ethereum",
) -> LendingDashboardConfig:
    """Get pre-configured Spark lending dashboard config."""
    return LendingDashboardConfig(
        protocol="spark",
        collateral_token=collateral_token,
        borrow_token=borrow_token,
        chain=chain,
        max_ltv=0.80,
        liquidation_ltv=0.825,
    )
