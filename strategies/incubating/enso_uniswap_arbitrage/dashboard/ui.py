"""
Enso Uniswap Arbitrage Strategy Dashboard.

Custom dashboard showing arbitrage opportunities detected,
execution success rate, profit per trade, cumulative profit, and gas costs.
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
    """Render the Enso Uniswap Arbitrage custom dashboard.

    Shows:
    - Arbitrage opportunities detected
    - Execution success rate
    - Profit per trade
    - Cumulative profit
    - Gas costs
    """
    st.title("Enso Uniswap Arbitrage Dashboard")

    # Extract config values
    min_profit_threshold = Decimal(str(strategy_config.get("min_profit_threshold", "10")))

    # Strategy info header
    st.markdown(f"**Strategy ID:** `{strategy_id}`")
    st.markdown("**Aggregator:** Enso")
    st.markdown("**DEX:** Uniswap V3")
    st.markdown(f"**Min Profit Threshold:** ${float(min_profit_threshold):.2f}")

    st.divider()

    # Opportunity Detection section
    st.subheader("Opportunity Detection")
    _render_opportunity_detection(session_state)

    st.divider()

    # Execution Stats section
    st.subheader("Execution Statistics")
    _render_execution_stats(session_state)

    st.divider()

    # Profit Metrics section
    st.subheader("Profit Metrics")
    _render_profit_metrics(session_state)

    st.divider()

    # Gas Analysis section
    st.subheader("Gas Analysis")
    _render_gas_analysis(session_state)


def _render_opportunity_detection(session_state: dict[str, Any]) -> None:
    """Render arbitrage opportunity detection stats."""
    opportunities_detected = session_state.get("opportunities_detected", 0)
    opportunities_executed = session_state.get("opportunities_executed", 0)
    last_opportunity_time = session_state.get("last_opportunity_time", "N/A")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            "Opportunities Detected",
            str(opportunities_detected),
            help="Total arbitrage opportunities found",
        )

    with col2:
        st.metric(
            "Opportunities Executed",
            str(opportunities_executed),
            help="Successfully executed arbitrages",
        )

    with col3:
        conversion_rate = (opportunities_executed / opportunities_detected * 100) if opportunities_detected > 0 else 0
        st.metric(
            "Conversion Rate",
            f"{conversion_rate:.1f}%",
            help="Percentage of detected opportunities executed",
        )

    st.caption(f"Last opportunity detected: {last_opportunity_time}")


def _render_execution_stats(session_state: dict[str, Any]) -> None:
    """Render execution success/failure stats."""
    successful_trades = session_state.get("successful_trades", 0)
    failed_trades = session_state.get("failed_trades", 0)
    total_trades = successful_trades + failed_trades

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            "Successful Trades",
            str(successful_trades),
            help="Trades that completed successfully",
        )

    with col2:
        st.metric(
            "Failed Trades",
            str(failed_trades),
            help="Trades that reverted or failed",
        )

    with col3:
        success_rate = (successful_trades / total_trades * 100) if total_trades > 0 else 0
        st.metric(
            "Success Rate",
            f"{success_rate:.1f}%",
            help="Percentage of successful executions",
        )

    # Failure reasons
    if failed_trades > 0:
        st.markdown("**Common Failure Reasons:**")
        st.markdown(
            """
            - Price moved before execution (MEV)
            - Insufficient liquidity
            - Gas price spike
            - Slippage exceeded
            """
        )


def _render_profit_metrics(session_state: dict[str, Any]) -> None:
    """Render profit per trade and cumulative profit."""
    cumulative_profit = Decimal(str(session_state.get("cumulative_profit", "0")))
    total_trades = session_state.get("successful_trades", 0)
    best_trade = Decimal(str(session_state.get("best_trade_profit", "0")))
    worst_trade = Decimal(str(session_state.get("worst_trade_profit", "0")))

    avg_profit = cumulative_profit / Decimal(str(total_trades)) if total_trades > 0 else Decimal("0")

    col1, col2, col3 = st.columns(3)

    with col1:
        pnl_color = "normal" if cumulative_profit >= 0 else "inverse"
        st.metric(
            "Cumulative Profit",
            f"${float(cumulative_profit):+,.2f}",
            delta_color=pnl_color,
            help="Total profit from all trades",
        )

    with col2:
        st.metric(
            "Avg Profit/Trade",
            f"${float(avg_profit):+,.2f}",
            help="Average profit per successful trade",
        )

    with col3:
        st.metric(
            "Best Trade",
            f"${float(best_trade):+,.2f}",
            help="Highest single trade profit",
        )

    # Profit breakdown
    if total_trades > 0:
        st.markdown("**Profit Distribution:**")
        st.markdown(f"- Best trade: ${float(best_trade):+,.2f}")
        st.markdown(f"- Worst trade: ${float(worst_trade):+,.2f}")
        st.markdown(f"- Average: ${float(avg_profit):+,.2f}")


def _render_gas_analysis(session_state: dict[str, Any]) -> None:
    """Render gas costs analysis."""
    total_gas_spent = Decimal(str(session_state.get("total_gas_spent", "0")))
    avg_gas_per_trade = Decimal(str(session_state.get("avg_gas_per_trade", "0")))
    cumulative_profit = Decimal(str(session_state.get("cumulative_profit", "0")))

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            "Total Gas Spent",
            f"${float(total_gas_spent):,.2f}",
            help="Total gas costs in USD",
        )

    with col2:
        st.metric(
            "Avg Gas/Trade",
            f"${float(avg_gas_per_trade):.2f}",
            help="Average gas cost per trade",
        )

    with col3:
        net_profit = cumulative_profit - total_gas_spent
        st.metric(
            "Net Profit (after gas)",
            f"${float(net_profit):+,.2f}",
            help="Profit minus gas costs",
        )

    # Gas efficiency
    if cumulative_profit > 0:
        gas_ratio = total_gas_spent / cumulative_profit * Decimal("100")
        st.markdown(f"**Gas/Profit Ratio:** {float(gas_ratio):.1f}% of profits spent on gas")

        if gas_ratio > Decimal("50"):
            st.warning("High gas costs are significantly reducing profitability.")
        elif gas_ratio > Decimal("25"):
            st.info("Gas costs are moderate.")
        else:
            st.success("Gas costs are well optimized.")
