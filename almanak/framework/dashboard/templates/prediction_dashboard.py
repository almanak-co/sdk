"""
Prediction Market Dashboard Template.

Reusable template for creating dashboards for prediction market strategies
on platforms like Polymarket.

Usage:
    from almanak.framework.dashboard.templates import PredictionDashboardConfig, render_prediction_dashboard

    config = PredictionDashboardConfig(
        protocol="polymarket",
    )

    def render_custom_dashboard(strategy_id, strategy_config, api_client, session_state):
        render_prediction_dashboard(strategy_id, strategy_config, session_state, config)
"""

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import streamlit as st

from almanak.framework.dashboard.plots import (
    plot_market_outcomes,
    plot_prediction_position,
    plot_probability_over_time,
)
from almanak.framework.dashboard.plots.prediction_plots import (
    plot_arbitrage_opportunity,
    plot_prediction_pnl_breakdown,
)


@dataclass
class PredictionDashboardConfig:
    """Configuration for a prediction market dashboard.

    Attributes:
        protocol: Protocol name (e.g., "polymarket")
        chain: Chain name
        show_position_overview: Whether to show position overview
        show_probability_chart: Whether to show probability over time
        show_market_outcomes: Whether to show market outcomes comparison
        show_arbitrage_analysis: Whether to show arbitrage opportunity analysis
        show_pnl_breakdown: Whether to show PnL breakdown
    """

    protocol: str = "polymarket"
    chain: str = "polygon"
    show_position_overview: bool = True
    show_probability_chart: bool = True
    show_market_outcomes: bool = True
    show_arbitrage_analysis: bool = True
    show_pnl_breakdown: bool = True


def render_prediction_dashboard(
    strategy_id: str,
    strategy_config: dict[str, Any],
    session_state: dict[str, Any],
    config: PredictionDashboardConfig,
) -> None:
    """Render a prediction market strategy dashboard using the provided configuration.

    Args:
        strategy_id: The strategy identifier
        strategy_config: Strategy configuration dictionary
        session_state: Current session state with position data
        config: PredictionDashboardConfig for this dashboard
    """
    # Extract config overrides
    protocol = strategy_config.get("protocol", config.protocol)
    chain = strategy_config.get("chain", config.chain)

    st.title(f"{protocol.replace('_', ' ').title()} Dashboard")

    st.markdown(f"**Strategy ID:** `{strategy_id}`")
    st.markdown(f"**Chain:** {chain.title()}")

    st.divider()

    # Active Market Info
    market_question = session_state.get("market_question", "")
    market_id = session_state.get("market_id", "")

    if market_question:
        st.subheader("Active Market")
        st.markdown(f"**Question:** {market_question}")
        if market_id:
            st.markdown(f"**Market ID:** `{market_id}`")

    st.divider()

    # Position Overview
    if config.show_position_overview:
        st.subheader("Position Overview")

        yes_shares = float(session_state.get("yes_shares", 0))
        no_shares = float(session_state.get("no_shares", 0))
        yes_price = float(session_state.get("yes_price", 0.5))
        no_price = float(session_state.get("no_price", 0.5))
        cost_basis = float(session_state.get("cost_basis", 0))

        if yes_shares > 0 or no_shares > 0:
            fig = plot_prediction_position(
                yes_shares=yes_shares,
                no_shares=no_shares,
                yes_price=yes_price,
                no_price=no_price,
                cost_basis=cost_basis if cost_basis > 0 else None,
                market_question=market_question,
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No active position in this market")

    st.divider()

    # Position Details
    st.subheader("Position Details")
    _render_position_details(session_state)

    st.divider()

    # Arbitrage Analysis
    if config.show_arbitrage_analysis:
        st.subheader("Arbitrage Analysis")

        yes_price = float(session_state.get("yes_price", 0.5))
        no_price = float(session_state.get("no_price", 0.5))

        if yes_price > 0 and no_price > 0:
            fig = plot_arbitrage_opportunity(
                yes_price=yes_price,
                no_price=no_price,
                market_question=market_question,
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Market price data not available")

    st.divider()

    # Probability Over Time
    if config.show_probability_chart:
        st.subheader("Probability Over Time")

        probability_history = session_state.get("probability_history")
        # Handle DataFrames (which raise ValueError on truthiness check) and lists
        has_probability = probability_history is not None and (
            (hasattr(probability_history, "empty") and not probability_history.empty)
            or (hasattr(probability_history, "__len__") and len(probability_history) > 0)
        )
        if has_probability:
            fig = plot_probability_over_time(
                probability_data=probability_history,
                show_both=True,
                market_question=market_question,
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Probability history not available")

    st.divider()

    # Market Outcomes (Multiple Markets)
    if config.show_market_outcomes:
        markets = session_state.get("tracked_markets", [])
        if markets:
            st.subheader("Tracked Markets")
            fig = plot_market_outcomes(
                markets=markets,
                sort_by="probability",
            )
            st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # PnL Breakdown
    if config.show_pnl_breakdown:
        trades = session_state.get("trade_history", [])
        if trades:
            st.subheader("Trade History")
            fig = plot_prediction_pnl_breakdown(
                trades=trades,
            )
            st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # Performance Summary
    st.subheader("Performance Summary")
    _render_performance_summary(session_state)


def _render_position_details(session_state: dict[str, Any]) -> None:
    """Render the position details section."""
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        yes_shares = Decimal(str(session_state.get("yes_shares", "0")))
        st.metric("YES Shares", f"{float(yes_shares):,.2f}")

    with col2:
        no_shares = Decimal(str(session_state.get("no_shares", "0")))
        st.metric("NO Shares", f"{float(no_shares):,.2f}")

    with col3:
        yes_price = Decimal(str(session_state.get("yes_price", "0.5")))
        st.metric("YES Price", f"${float(yes_price):.4f}")

    with col4:
        no_price = Decimal(str(session_state.get("no_price", "0.5")))
        st.metric("NO Price", f"${float(no_price):.4f}")

    # Second row
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        cost_basis = Decimal(str(session_state.get("cost_basis", "0")))
        st.metric("Cost Basis", f"${float(cost_basis):,.2f}")

    with col2:
        current_value = Decimal(str(session_state.get("current_value", "0")))
        st.metric("Current Value", f"${float(current_value):,.2f}")

    with col3:
        unrealized_pnl = Decimal(str(session_state.get("unrealized_pnl", "0")))
        st.metric("Unrealized PnL", f"${float(unrealized_pnl):+,.2f}")

    with col4:
        # Calculate potential payout
        yes_shares_f = float(session_state.get("yes_shares", 0))
        no_shares_f = float(session_state.get("no_shares", 0))
        max_payout = max(yes_shares_f, no_shares_f)  # Potential $1 per share
        st.metric("Max Payout", f"${max_payout:,.2f}")


def _render_performance_summary(session_state: dict[str, Any]) -> None:
    """Render the performance summary section."""
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        realized_pnl = Decimal(str(session_state.get("realized_pnl", "0")))
        st.metric("Realized PnL", f"${float(realized_pnl):+,.2f}")

    with col2:
        total_invested = Decimal(str(session_state.get("total_invested", "0")))
        st.metric("Total Invested", f"${float(total_invested):,.2f}")

    with col3:
        roi = Decimal(str(session_state.get("roi_pct", "0")))
        st.metric("ROI", f"{float(roi):+.2f}%")

    with col4:
        markets_traded = session_state.get("markets_traded", 0)
        st.metric("Markets Traded", str(markets_traded))

    # Second row
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        win_rate = Decimal(str(session_state.get("win_rate", "0")))
        st.metric("Win Rate", f"{float(win_rate):.1f}%")

    with col2:
        avg_position_size = Decimal(str(session_state.get("avg_position_size", "0")))
        st.metric("Avg Position Size", f"${float(avg_position_size):,.2f}")

    with col3:
        arbitrage_profits = Decimal(str(session_state.get("arbitrage_profits", "0")))
        st.metric("Arbitrage Profits", f"${float(arbitrage_profits):+,.2f}")

    with col4:
        total_fees_paid = Decimal(str(session_state.get("total_fees_paid", "0")))
        st.metric("Fees Paid", f"${float(total_fees_paid):,.2f}")


# Pre-configured templates


def get_polymarket_config() -> PredictionDashboardConfig:
    """Get pre-configured Polymarket dashboard config."""
    return PredictionDashboardConfig(
        protocol="polymarket",
        chain="polygon",
    )


def get_polymarket_arbitrage_config() -> PredictionDashboardConfig:
    """Get pre-configured Polymarket arbitrage dashboard config."""
    return PredictionDashboardConfig(
        protocol="polymarket",
        chain="polygon",
        show_arbitrage_analysis=True,
        show_market_outcomes=True,
    )
