"""
Perpetual Futures Dashboard Template.

Reusable template for creating dashboards for perpetual trading strategies
on protocols like GMX V2 and Hyperliquid.

Usage:
    from almanak.framework.dashboard.templates import PerpDashboardConfig, render_perp_dashboard

    config = PerpDashboardConfig(
        protocol="gmx_v2",
        market="ETH/USD",
        collateral_token="WETH",
    )

    def render_custom_dashboard(strategy_id, strategy_config, api_client, session_state):
        render_perp_dashboard(strategy_id, strategy_config, session_state, config)
"""

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import streamlit as st

from almanak.framework.dashboard.plots import (
    plot_funding_rate_history,
    plot_leverage_gauge,
    plot_liquidation_levels,
    plot_perp_position_dashboard,
)


@dataclass
class PerpDashboardConfig:
    """Configuration for a perpetual futures dashboard.

    Attributes:
        protocol: Protocol name (e.g., "gmx_v2", "hyperliquid")
        market: Market identifier (e.g., "ETH/USD", "BTC/USD")
        collateral_token: Collateral token symbol
        chain: Chain name
        max_leverage: Maximum allowed leverage
        safe_leverage: Recommended safe leverage
        show_position_dashboard: Whether to show the main position dashboard
        show_funding_history: Whether to show funding rate history
        show_leverage_gauge: Whether to show leverage gauge
        show_liquidation_levels: Whether to show liquidation levels
    """

    protocol: str = "gmx_v2"
    market: str = "ETH/USD"
    collateral_token: str = "WETH"
    chain: str = "arbitrum"
    max_leverage: float = 50.0
    safe_leverage: float = 10.0
    show_position_dashboard: bool = True
    show_funding_history: bool = True
    show_leverage_gauge: bool = True
    show_liquidation_levels: bool = True


def render_perp_dashboard(
    strategy_id: str,
    strategy_config: dict[str, Any],
    session_state: dict[str, Any],
    config: PerpDashboardConfig,
) -> None:
    """Render a perpetual futures strategy dashboard using the provided configuration.

    Args:
        strategy_id: The strategy identifier
        strategy_config: Strategy configuration dictionary
        session_state: Current session state with position data
        config: PerpDashboardConfig for this dashboard
    """
    # Extract config overrides
    market = strategy_config.get("market", config.market)
    collateral_token = strategy_config.get("collateral_token", config.collateral_token)
    chain = strategy_config.get("chain", config.chain)
    protocol = strategy_config.get("protocol", config.protocol)
    max_leverage = strategy_config.get("max_leverage", config.max_leverage)

    st.title(f"{protocol.replace('_', ' ').title()} Perpetuals Dashboard")

    st.markdown(f"**Strategy ID:** `{strategy_id}`")
    st.markdown(f"**Market:** {market}")
    st.markdown(f"**Collateral:** {collateral_token} | **Chain:** {chain.title()}")

    st.divider()

    # Position Overview
    if config.show_position_dashboard and session_state.get("has_position"):
        st.subheader("Position Overview")

        entry_price = float(session_state.get("entry_price", 0))
        current_price = float(session_state.get("current_price", 0))
        liquidation_price = float(session_state.get("liquidation_price", 0))
        is_long = session_state.get("is_long", True)
        size_usd = float(session_state.get("size_usd", 0))
        leverage = float(session_state.get("leverage", 1))
        collateral_usd = float(session_state.get("collateral_usd", 0))
        unrealized_pnl = float(session_state.get("unrealized_pnl", 0))

        if entry_price > 0 and current_price > 0:
            fig = plot_perp_position_dashboard(
                entry_price=entry_price,
                current_price=current_price,
                liquidation_price=liquidation_price,
                is_long=is_long,
                size_usd=size_usd,
                leverage=leverage,
                market=market,
                collateral_usd=collateral_usd,
                unrealized_pnl=unrealized_pnl,
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Position data not available")
    elif config.show_position_dashboard:
        st.info("No active position")

    st.divider()

    # Position Metrics and Leverage
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Position Metrics")
        _render_position_metrics(session_state)

    with col2:
        if config.show_leverage_gauge:
            st.subheader("Leverage")
            leverage = float(session_state.get("leverage", 1))
            fig = plot_leverage_gauge(
                current_leverage=leverage,
                max_leverage=max_leverage,
                safe_leverage=config.safe_leverage,
            )
            st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # Funding Rate History
    if config.show_funding_history:
        st.subheader("Funding Rate History")
        funding_history = session_state.get("funding_history")
        # Handle DataFrames (which raise ValueError on truthiness check) and lists
        has_funding = funding_history is not None and (
            (hasattr(funding_history, "empty") and not funding_history.empty)
            or (hasattr(funding_history, "__len__") and len(funding_history) > 0)
        )
        if has_funding:
            fig = plot_funding_rate_history(
                funding_data=funding_history,
                show_cumulative=True,
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Funding rate history not available")

    st.divider()

    # Multiple Positions / Liquidation Levels
    if config.show_liquidation_levels:
        positions = session_state.get("positions", [])
        if positions:
            st.subheader("Liquidation Levels")
            current_price = float(session_state.get("current_price", 0))
            fig = plot_liquidation_levels(
                positions=positions,
                current_price=current_price,
            )
            st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # Performance Summary
    st.subheader("Performance Summary")
    _render_performance_summary(session_state)


def _render_position_metrics(
    session_state: dict[str, Any],
) -> None:
    """Render the position metrics section."""
    col1, col2 = st.columns(2)

    with col1:
        direction = "LONG" if session_state.get("is_long", True) else "SHORT"
        direction_color = "green" if session_state.get("is_long", True) else "red"
        st.markdown(f"**Direction:** :{direction_color}[{direction}]")

        size_usd = Decimal(str(session_state.get("size_usd", "0")))
        st.metric("Size", f"${float(size_usd):,.2f}")

        collateral_usd = Decimal(str(session_state.get("collateral_usd", "0")))
        st.metric("Collateral", f"${float(collateral_usd):,.2f}")

    with col2:
        entry_price = Decimal(str(session_state.get("entry_price", "0")))
        st.metric("Entry Price", f"${float(entry_price):,.2f}")

        current_price = Decimal(str(session_state.get("current_price", "0")))
        st.metric("Current Price", f"${float(current_price):,.2f}")

        liquidation_price = Decimal(str(session_state.get("liquidation_price", "0")))
        st.metric("Liquidation Price", f"${float(liquidation_price):,.2f}")


def _render_performance_summary(session_state: dict[str, Any]) -> None:
    """Render the performance summary section."""
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        unrealized_pnl = Decimal(str(session_state.get("unrealized_pnl", "0")))
        st.metric("Unrealized PnL", f"${float(unrealized_pnl):+,.2f}")

    with col2:
        realized_pnl = Decimal(str(session_state.get("realized_pnl", "0")))
        st.metric("Realized PnL", f"${float(realized_pnl):+,.2f}")

    with col3:
        funding_paid = Decimal(str(session_state.get("funding_paid", "0")))
        st.metric("Funding Paid", f"${float(funding_paid):+,.2f}")

    with col4:
        total_pnl = Decimal(str(session_state.get("total_pnl", "0")))
        st.metric("Total PnL", f"${float(total_pnl):+,.2f}")

    # Second row
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        win_rate = Decimal(str(session_state.get("win_rate", "0")))
        st.metric("Win Rate", f"{float(win_rate):.1f}%")

    with col2:
        total_trades = session_state.get("total_trades", 0)
        st.metric("Total Trades", str(total_trades))

    with col3:
        avg_hold_time = session_state.get("avg_hold_time_hours", 0)
        st.metric("Avg Hold Time", f"{avg_hold_time:.1f}h")

    with col4:
        max_drawdown = Decimal(str(session_state.get("max_drawdown_pct", "0")))
        st.metric("Max Drawdown", f"{float(max_drawdown):.2f}%")


# Pre-configured templates for common perpetual protocols


def get_gmx_v2_config(
    market: str = "ETH/USD",
    collateral_token: str = "WETH",
    chain: str = "arbitrum",
) -> PerpDashboardConfig:
    """Get pre-configured GMX V2 perpetuals dashboard config."""
    return PerpDashboardConfig(
        protocol="gmx_v2",
        market=market,
        collateral_token=collateral_token,
        chain=chain,
        max_leverage=100.0,
        safe_leverage=10.0,
    )


def get_hyperliquid_config(
    market: str = "ETH",
    collateral_token: str = "USDC",
    chain: str = "hyperliquid",
) -> PerpDashboardConfig:
    """Get pre-configured Hyperliquid perpetuals dashboard config."""
    return PerpDashboardConfig(
        protocol="hyperliquid",
        market=market,
        collateral_token=collateral_token,
        chain=chain,
        max_leverage=50.0,
        safe_leverage=5.0,
    )
