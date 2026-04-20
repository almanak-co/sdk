"""Strategy detail page for the Almanak Operator Dashboard.

Displays detailed information about a single strategy.
Wires action buttons to real API endpoints.
"""

import logging
from decimal import Decimal
from typing import Any

import requests
import streamlit as st

from almanak.framework.dashboard.components import render_operator_card
from almanak.framework.dashboard.config import API_BASE_URL, API_TIMEOUT, check_system_health
from almanak.framework.dashboard.data_source import execute_strategy_action
from almanak.framework.dashboard.models import Strategy, StrategyStatus
from almanak.framework.dashboard.plots.lending_plots import plot_health_factor_gauge
from almanak.framework.dashboard.plots.lp_plots import plot_position_range_status
from almanak.framework.dashboard.plots.perp_plots import plot_leverage_gauge
from almanak.framework.dashboard.plots.portfolio_plots import plot_portfolio_value_over_time
from almanak.framework.dashboard.plots.ta_plots import plot_price_with_signals
from almanak.framework.dashboard.theme import get_chain_color, get_chain_health_color, get_status_color
from almanak.framework.dashboard.utils import (
    format_bridge_progress,
    format_chain_badge,
    format_timeline_summary,
    format_usd,
    get_chain_health_icon,
    get_chain_icon,
    get_status_icon,
    get_timeline_event_icon,
)

logger = logging.getLogger(__name__)


def _detect_strategy_profile(strategy: Strategy) -> str:
    """Infer strategy profile for default chart selection."""
    protocol = (strategy.protocol or "").lower()
    event_types = {e.event_type.value for e in strategy.timeline_events}

    if strategy.position and strategy.position.lp_positions:
        return "LP"
    if {"LP_OPEN", "LP_CLOSE"} & event_types:
        return "LP"

    if strategy.position and strategy.position.health_factor is not None:
        return "LENDING"
    if {"BORROW", "REPAY"} & event_types:
        return "LENDING"
    if any(name in protocol for name in {"aave", "morpho", "compound", "spark"}):
        return "LENDING"

    if strategy.position and strategy.position.leverage is not None:
        return "PERPS"
    if any(name in protocol for name in {"gmx", "perp", "hyperliquid"}):
        return "PERPS"

    return "TA"


def _coerce_float(value: object) -> float | None:
    """Try converting a value to float."""
    if value is None:
        return None
    if isinstance(value, int | float):
        return float(value)
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return None


def call_strategy_action(strategy_id: str, action: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    """Call a strategy action API endpoint.

    Args:
        strategy_id: The strategy ID
        action: The action name (pause, resume, bump-gas, cancel-tx)
        payload: Optional request payload

    Returns:
        API response as dict with success/error info
    """
    normalized_action = action.strip().lower()

    # Prefer gateway-native control path for pause/resume.
    if normalized_action in {"pause", "resume"}:
        gateway_action = "PAUSE" if normalized_action == "pause" else "RESUME"
        raw_reason = payload.get("reason") if payload else None
        reason: str = (
            raw_reason
            if isinstance(raw_reason, str)
            else f"{gateway_action.title()} requested from dashboard detail page"
        )
        try:
            success = execute_strategy_action(strategy_id, gateway_action, reason)
            if success:
                return {"success": True, "message": f"{gateway_action.title()} request submitted"}
            return {"success": False, "error": f"Gateway rejected {gateway_action.title()} request"}
        except Exception as e:
            logger.exception("Gateway action call failed")
            return {"success": False, "error": str(e)}

    # Fallback to REST API for non-migrated actions.
    url = f"{API_BASE_URL}/api/strategies/{strategy_id}/{action}"
    headers = {"Content-Type": "application/json", "X-API-Key": "demo-key"}

    try:
        response = requests.post(
            url,
            json=payload or {},
            headers=headers,
            timeout=API_TIMEOUT,
        )

        if response.status_code == 200:
            return response.json()
        elif response.status_code == 404:
            return {"success": False, "error": f"Strategy {strategy_id} not found"}
        elif response.status_code == 400:
            error_detail = response.json().get("detail", "Bad request")
            return {"success": False, "error": error_detail}
        else:
            return {"success": False, "error": f"API error: {response.status_code}"}

    except requests.exceptions.ConnectionError:
        return {
            "success": False,
            "error": "Cannot connect to API server. Make sure the API is running.",
            "connection_error": True,
        }
    except requests.exceptions.Timeout:
        return {"success": False, "error": "API request timed out"}
    except Exception as e:
        logger.exception(f"Action API call failed: {e}")
        return {"success": False, "error": str(e)}


def render_pnl_chart(strategy: Strategy) -> None:
    """Render the PnL chart for a strategy."""
    if not strategy.pnl_history:
        st.info("No PnL history available.")
        return

    import pandas as pd

    chart_df = pd.DataFrame(
        {
            "timestamp": [p.timestamp for p in strategy.pnl_history],
            "value": [float(p.value_usd) for p in strategy.pnl_history],
            "pnl": [float(p.pnl_usd) for p in strategy.pnl_history],
        }
    )

    tab1, tab2 = st.tabs(["Portfolio Value", "PnL"])

    with tab1:
        value_fig = plot_portfolio_value_over_time(
            chart_df,
            time_column="timestamp",
            value_column="value",
            title="Portfolio Value Over Time",
            show_drawdown=True,
        )
        st.plotly_chart(value_fig, use_container_width=True)

    with tab2:
        pnl_fig = plot_portfolio_value_over_time(
            chart_df,
            time_column="timestamp",
            value_column="pnl",
            title="PnL Over Time",
            show_drawdown=False,
        )
        st.plotly_chart(pnl_fig, use_container_width=True)


def render_profile_charts(strategy: Strategy) -> None:
    """Render baseline strategy-type chart pack."""
    profile = _detect_strategy_profile(strategy)
    st.markdown(f"### Strategy Insights ({profile})")

    price_points: list[dict[str, object]] = []
    buy_signals: list[dict[str, object]] = []
    sell_signals: list[dict[str, object]] = []
    for event in sorted(strategy.timeline_events, key=lambda e: e.timestamp):
        details = event.details or {}
        price_value = (
            _coerce_float(details.get("price"))
            or _coerce_float(details.get("market_price"))
            or _coerce_float(details.get("current_price"))
        )
        if price_value is None:
            continue
        price_points.append({"time": event.timestamp, "price": price_value})
        signal = str(details.get("signal", "")).upper().strip()
        if signal == "BUY":
            buy_signals.append({"time": event.timestamp, "price": price_value})
        elif signal == "SELL":
            sell_signals.append({"time": event.timestamp, "price": price_value})

    if len(price_points) >= 2:
        import pandas as pd

        st.plotly_chart(
            plot_price_with_signals(
                pd.DataFrame(price_points),
                buy_signals=pd.DataFrame(buy_signals) if buy_signals else None,
                sell_signals=pd.DataFrame(sell_signals) if sell_signals else None,
                title="Observed Market Price and Signals",
            ),
            use_container_width=True,
        )
    else:
        st.caption("No gateway-backed price history available yet for this strategy.")

    if profile == "LP" and strategy.position and strategy.position.lp_positions:
        lp = strategy.position.lp_positions[0]
        st.plotly_chart(
            plot_position_range_status(
                current_price=float(lp.current_price),
                lower_bound=float(lp.range_lower),
                upper_bound=float(lp.range_upper),
                token_pair=lp.pool,
                title=f"LP Range Status ({lp.pool})",
            ),
            use_container_width=True,
        )
        return

    if profile == "LENDING" and strategy.position and strategy.position.health_factor is not None:
        st.plotly_chart(
            plot_health_factor_gauge(
                health_factor=float(strategy.position.health_factor),
                title="Lending Health Factor",
            ),
            use_container_width=True,
        )
        return

    if profile == "PERPS" and strategy.position and strategy.position.leverage is not None:
        st.plotly_chart(
            plot_leverage_gauge(
                current_leverage=float(strategy.position.leverage),
                max_leverage=max(float(strategy.position.leverage) * 2.0, 2.0),
                title="Leverage",
            ),
            use_container_width=True,
        )


def render_position_summary(strategy: Strategy) -> None:
    """Render the position summary for a strategy."""
    position = strategy.position
    if not position:
        st.info("No position data available.")
        return

    # Token balances
    st.markdown("### Token Balances")
    if position.token_balances:
        balance_data = []
        for tb in position.token_balances:
            balance_data.append(
                {
                    "Token": tb.symbol,
                    "Balance": f"{tb.balance:,.4f}",
                    "Value (USD)": format_usd(tb.value_usd),
                }
            )
        st.dataframe(balance_data, use_container_width=True, hide_index=True)
    else:
        st.info("No token balances.")

    # LP positions
    if position.lp_positions:
        st.markdown("### LP Positions")
        for lp in position.lp_positions:
            range_status = "In Range" if lp.in_range else "Out of Range"
            range_icon = "\U0001f7e2" if lp.in_range else "\U0001f534"
            st.markdown(
                f"""
                <div style="
                    background-color: #1e1e1e;
                    border: 1px solid #333;
                    border-radius: 8px;
                    padding: 1rem;
                    margin-bottom: 0.5rem;
                ">
                    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.5rem;">
                        <strong>{lp.pool}</strong>
                        <span>{range_icon} {range_status}</span>
                    </div>
                    <div style="color: #888; font-size: 0.9rem;">
                        <div>Liquidity: {format_usd(lp.liquidity_usd)}</div>
                        <div>Range: ${lp.range_lower:,.2f} - ${lp.range_upper:,.2f}</div>
                        <div>Current Price: ${lp.current_price:,.2f}</div>
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

    # Health metrics (for lending strategies)
    if position.health_factor is not None or position.leverage is not None:
        st.markdown("### Health Metrics")
        col1, col2 = st.columns(2)
        with col1:
            if position.health_factor is not None:
                st.metric("Health Factor", f"{position.health_factor:.2f}")
        with col2:
            if position.leverage is not None:
                st.metric("Leverage", f"{position.leverage:.1f}x")


def get_explorer_url(chain: str, tx_hash: str) -> str:
    """Get block explorer URL for a transaction hash."""
    explorers = {
        "arbitrum": "https://arbiscan.io/tx/",
        "ethereum": "https://etherscan.io/tx/",
        "base": "https://basescan.org/tx/",
        "optimism": "https://optimistic.etherscan.io/tx/",
        "polygon": "https://polygonscan.com/tx/",
        "avalanche": "https://snowtrace.io/tx/",
    }
    base_url = explorers.get(chain.lower(), "https://etherscan.io/tx/")
    # Ensure tx_hash has 0x prefix
    if not tx_hash.startswith("0x"):
        tx_hash = f"0x{tx_hash}"
    return f"{base_url}{tx_hash}"


def render_timeline_events(strategy: Strategy, limit: int = 10) -> None:
    """Render recent timeline events grouped by intent with expandable TX details."""
    events = strategy.timeline_events

    if not events:
        st.info("No recent events.")
        return

    st.markdown("### Recent Activity")

    # Group events by correlation_id (intent)
    intents: dict[str, dict] = {}
    ungrouped_events = []

    for event in events:
        correlation_id = event.details.get("correlation_id") if event.details else None
        if correlation_id:
            if correlation_id not in intents:
                intents[correlation_id] = {
                    "intent_description": event.details.get("intent_description", "Unknown Intent"),
                    "events": [],
                    "status": None,
                    "timestamp": event.timestamp,
                    "tx_count": event.details.get("tx_count", 0),
                }
            intents[correlation_id]["events"].append(event)
            # Track the final status (EXECUTION_SUCCESS or EXECUTION_FAILED)
            exec_event = event.details.get("execution_event", "")
            if exec_event in ("EXECUTION_SUCCESS", "EXECUTION_FAILED"):
                intents[correlation_id]["status"] = exec_event
            # Use earliest timestamp for sorting
            if event.timestamp < intents[correlation_id]["timestamp"]:
                intents[correlation_id]["timestamp"] = event.timestamp
        else:
            ungrouped_events.append(event)

    # Sort intents by most recent first (use the latest event timestamp)
    sorted_intents = sorted(intents.items(), key=lambda x: max(e.timestamp for e in x[1]["events"]), reverse=True)[
        :limit
    ]

    # Render each intent as a collapsible section
    for _correlation_id, intent_data in sorted_intents:
        intent_desc = intent_data["intent_description"]
        status = intent_data["status"]
        tx_count = intent_data["tx_count"] or len([e for e in intent_data["events"] if e.details.get("tx_hash")])
        intent_events = sorted(intent_data["events"], key=lambda e: e.timestamp, reverse=True)
        latest_time = intent_events[0].timestamp.strftime("%Y-%m-%d %H:%M:%S")

        # Determine status icon and color
        if status == "EXECUTION_SUCCESS":
            status_icon = "✓"
            status_color = "#00c853"
            status_text = "Completed"
        elif status == "EXECUTION_FAILED":
            status_icon = "✗"
            status_color = "#f44336"
            status_text = "Failed"
        else:
            # Still in progress
            status_icon = "⏳"
            status_color = "#ff9800"
            status_text = "In Progress"

        # Intent header with status
        st.markdown(
            f"""<div style="
                background-color: #1e1e1e;
                border: 1px solid #333;
                border-left: 4px solid {status_color};
                border-radius: 8px;
                padding: 0.75rem 1rem;
                margin-bottom: 0.25rem;
            ">
                <div style="display: flex; justify-content: space-between; align-items: center;">
                    <span style="font-weight: 500;">{status_icon} {intent_desc}</span>
                    <span style="color: {status_color}; font-size: 0.85rem;">{status_text}</span>
                </div>
                <div style="color: #888; font-size: 0.8rem; margin-top: 0.25rem;">
                    {latest_time} · {tx_count} transaction(s)
                </div>
            </div>""",
            unsafe_allow_html=True,
        )

        # Expandable TX details
        with st.expander("View transaction details", expanded=False):
            for event in intent_events:
                exec_event = event.details.get("execution_event", "") if event.details else ""
                tx_hash = event.details.get("tx_hash", "") if event.details else ""
                time_str = event.timestamp.strftime("%H:%M:%S")

                # Skip the summary events, show only TX-level events
                if exec_event in ("TX_SENT", "TX_CONFIRMED", "TX_FAILED", "TX_REVERTED"):
                    if exec_event == "TX_CONFIRMED":
                        icon = "✓"
                        color = "#00c853"
                        block = event.details.get("block_number", "")
                        gas = event.details.get("gas_used", "")
                        detail = f"Block {block:,}" if block else ""
                        if gas:
                            detail += f" · Gas: {gas:,}"
                    elif exec_event == "TX_SENT":
                        icon = "→"
                        color = "#2196f3"
                        detail = "Submitted to mempool"
                    elif exec_event in ("TX_FAILED", "TX_REVERTED"):
                        icon = "✗"
                        color = "#f44336"
                        detail = event.details.get("error", "Transaction failed")
                    else:
                        icon = "•"
                        color = "#888"
                        detail = ""

                    tx_short = tx_hash[:10] + "..." if tx_hash else ""
                    # Get chain from event for explorer link
                    chain = getattr(event, "chain", None) or strategy.chain or "arbitrum"
                    explorer_url = get_explorer_url(chain, tx_hash) if tx_hash else ""

                    # Make TX hash a clickable link
                    if tx_hash and explorer_url:
                        tx_display = f'<a href="{explorer_url}" target="_blank" style="background: #2a2a2a; padding: 0.1rem 0.3rem; border-radius: 4px; font-size: 0.8rem; font-family: monospace; color: #58a6ff; text-decoration: none;">{tx_short}</a>'
                    else:
                        tx_display = f'<code style="background: #2a2a2a; padding: 0.1rem 0.3rem; border-radius: 4px; font-size: 0.8rem;">{tx_short}</code>'

                    st.markdown(
                        f"""<div style="
                            padding: 0.5rem 0;
                            border-bottom: 1px solid #333;
                            font-size: 0.9rem;
                        ">
                            <span style="color: {color};">{icon}</span>
                            {tx_display}
                            <span style="color: #888; margin-left: 0.5rem;">{time_str}</span>
                            <span style="color: #666; margin-left: 0.5rem;">{detail}</span>
                        </div>""",
                        unsafe_allow_html=True,
                    )

    # Show any ungrouped events (legacy or missing correlation_id)
    if ungrouped_events and not sorted_intents:
        for event in ungrouped_events[:limit]:
            icon = get_timeline_event_icon(event.event_type)
            time_str = event.timestamp.strftime("%Y-%m-%d %H:%M:%S")
            summary = format_timeline_summary(event.event_type, event.description, event.details or {})
            st.markdown(
                f'<div style="background-color: #1e1e1e; border-radius: 8px; padding: 0.75rem 1rem; margin-bottom: 0.5rem;">'
                f"{icon} {time_str} - {summary}"
                f"</div>",
                unsafe_allow_html=True,
            )


def render_chain_health_indicators(strategy: Strategy) -> None:
    """Render chain health indicators for multi-chain strategies."""
    if not strategy.chain_health:
        return

    st.markdown("#### Chain Health")
    cols = st.columns(len(strategy.chain_health))

    for idx, (chain, health) in enumerate(strategy.chain_health.items()):
        chain_color = get_chain_color(chain)
        get_chain_health_color(health.status)
        health_icon = get_chain_health_icon(health.status)
        chain_icon = get_chain_icon(chain)

        with cols[idx]:
            # Build health details
            details_parts = []
            if health.rpc_latency_ms is not None:
                details_parts.append(f"Latency: {health.rpc_latency_ms}ms")
            if health.gas_price_gwei is not None:
                details_parts.append(f"Gas: {health.gas_price_gwei:.1f} gwei")
            if health.block_number is not None:
                details_parts.append(f"Block: {health.block_number:,}")
            details_str = " | ".join(details_parts) if details_parts else ""

            st.markdown(
                f"""
                <div style="
                    background-color: {chain_color}11;
                    border: 1px solid {chain_color}44;
                    border-radius: 8px;
                    padding: 0.5rem;
                ">
                    <div style="display: flex; align-items: center; gap: 0.5rem;">
                        <span>{chain_icon}</span>
                        <span style="color: {chain_color}; font-weight: bold; text-transform: uppercase;">
                            {chain}
                        </span>
                        <span style="margin-left: auto;">{health_icon}</span>
                    </div>
                    <div style="font-size: 0.75rem; color: #888; margin-top: 0.25rem;">
                        {details_str}
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )


def render_bridge_transfers(strategy: Strategy) -> None:
    """Render active and recent bridge transfers."""
    if not strategy.bridge_transfers:
        return

    st.markdown("### Bridge Transfers")

    # Separate active and completed transfers
    active_transfers = [t for t in strategy.bridge_transfers if t.status == "IN_FLIGHT"]
    completed_transfers = [t for t in strategy.bridge_transfers if t.status != "IN_FLIGHT"]

    if active_transfers:
        st.markdown("#### In Progress")
        for transfer in active_transfers:
            st.markdown(
                f"""
                <div style="
                    background-color: #1e1e1e;
                    border: 1px solid #333;
                    border-radius: 8px;
                    padding: 1rem;
                    margin-bottom: 0.5rem;
                ">
                    <div style="display: flex; justify-content: space-between; align-items: center;">
                        <strong>{transfer.amount} {transfer.token}</strong>
                        <span style="color: #888; font-size: 0.85rem;">via {transfer.bridge_protocol or "Bridge"}</span>
                    </div>
                    {format_bridge_progress(transfer.from_chain, transfer.to_chain, transfer.status, 50)}
                    <div style="font-size: 0.8rem; color: #888; margin-top: 0.25rem;">
                        Started: {transfer.initiated_at.strftime("%H:%M:%S")} | Fee: {format_usd(transfer.fee_usd)}
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

    if completed_transfers:
        with st.expander(f"Completed Transfers ({len(completed_transfers)})"):
            for transfer in completed_transfers[:5]:  # Show last 5
                status_icon = "\u2705" if transfer.status == "COMPLETED" else "\u274c"
                st.markdown(
                    f"""
                    <div style="
                        padding: 0.5rem;
                        border-bottom: 1px solid #333;
                    ">
                        {status_icon} {transfer.amount} {transfer.token} |
                        {transfer.from_chain.upper()} → {transfer.to_chain.upper()} |
                        Fee: {format_usd(transfer.fee_usd)}
                    </div>
                    """,
                    unsafe_allow_html=True,
                )


def render_multi_chain_position_summary(strategy: Strategy) -> None:
    """Render position summary with per-chain tabs for multi-chain strategies."""
    if not strategy.is_multi_chain or not strategy.positions_by_chain:
        # Fall back to standard position summary
        render_position_summary(strategy)
        return

    st.markdown("### Positions by Chain")

    # Create tabs for each chain
    chain_tabs = st.tabs([chain.upper() for chain in strategy.chains])

    for idx, chain in enumerate(strategy.chains):
        with chain_tabs[idx]:
            chain_position = strategy.positions_by_chain.get(chain)
            if not chain_position:
                st.info(f"No positions on {chain}")
                continue

            chain_color = get_chain_color(chain)

            # Per-chain value and P&L
            chain_pnl = strategy.pnl_by_chain.get(chain, Decimal("0"))
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Value", format_usd(chain_position.total_value_usd))
            with col2:
                pnl_delta = f"{'+' if chain_pnl >= 0 else ''}{chain_pnl:,.2f}"
                st.metric("24h PnL", format_usd(abs(chain_pnl)), delta=pnl_delta)

            # Token balances
            if chain_position.token_balances:
                st.markdown("**Token Balances**")
                balance_data = []
                for tb in chain_position.token_balances:
                    balance_data.append(
                        {
                            "Token": tb.symbol,
                            "Balance": f"{tb.balance:,.4f}",
                            "Value (USD)": format_usd(tb.value_usd),
                        }
                    )
                st.dataframe(balance_data, use_container_width=True, hide_index=True)

            # LP positions
            if chain_position.lp_positions:
                st.markdown("**LP Positions**")
                for lp in chain_position.lp_positions:
                    range_status = "In Range" if lp.in_range else "Out of Range"
                    range_icon = "\U0001f7e2" if lp.in_range else "\U0001f534"
                    st.markdown(
                        f"""
                        <div style="
                            background-color: {chain_color}11;
                            border: 1px solid {chain_color}44;
                            border-radius: 8px;
                            padding: 0.75rem;
                            margin-bottom: 0.5rem;
                        ">
                            <div style="display: flex; justify-content: space-between;">
                                <strong>{lp.pool}</strong>
                                <span>{range_icon} {range_status}</span>
                            </div>
                            <div style="color: #888; font-size: 0.9rem;">
                                Liquidity: {format_usd(lp.liquidity_usd)} | Range: ${lp.range_lower:,.2f} - ${lp.range_upper:,.2f}
                            </div>
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )

            # Health metrics
            if chain_position.health_factor is not None or chain_position.leverage is not None:
                col1, col2 = st.columns(2)
                with col1:
                    if chain_position.health_factor is not None:
                        st.metric("Health Factor", f"{chain_position.health_factor:.2f}")
                with col2:
                    if chain_position.leverage is not None:
                        st.metric("Leverage", f"{chain_position.leverage:.1f}x")


def page(strategies: list[Strategy]) -> None:
    """Render the strategy detail page.

    Args:
        strategies: List of all strategy data objects
    """
    # Get strategy ID from query params
    strategy_id = st.query_params.get("strategy_id")

    if not strategy_id:
        st.info("👈 Please select a strategy from the sidebar to view details.")
        st.markdown("### Or select a strategy here:")
        if strategies:
            strategy_names = [f"{s.name} ({s.id[:12]}...)" for s in strategies]
            selected_idx = st.selectbox(
                "Choose a strategy",
                range(len(strategy_names)),
                format_func=lambda x: strategy_names[x],
                key="detail_strategy_selector",
            )
            if st.button("View Details", use_container_width=True):
                st.query_params["strategy_id"] = strategies[selected_idx].id
                st.rerun()
        else:
            st.warning("No strategies found. Make sure you have strategies running or check your state database.")
            if st.button("Go to Overview"):
                st.query_params["page"] = "overview"
        return

    strategy = next((s for s in strategies if s.id == strategy_id), None)

    if not strategy:
        st.error(f"Strategy {strategy_id} not found.")
        if st.button("Go to Overview"):
            st.query_params["page"] = "overview"
        return

    # Enrich with full details (timeline, position) from gateway
    from almanak.framework.dashboard.data_source import GatewayConnectionError, get_strategy_details

    try:
        detailed = get_strategy_details(strategy_id)
        if detailed is not None:
            strategy = detailed
    except GatewayConnectionError:
        st.warning("Gateway unavailable - showing cached strategy data")

    # Back button
    if st.button("← Back to Overview"):
        st.query_params["page"] = "overview"
        if "strategy_id" in st.query_params:
            del st.query_params["strategy_id"]

    # Ensure we have a strategy object
    if not strategy:
        st.error("Strategy object is None - this should not happen")
        return

    # Header with status
    status_icon = get_status_icon(strategy.status)
    status_color = get_status_color(strategy.status)

    st.markdown(
        f"""
        <div style="display: flex; align-items: center; gap: 1rem; margin-bottom: 1rem;">
            <h2 style="margin: 0;">{strategy.name}</h2>
            <span style="
                background-color: {status_color}22;
                color: {status_color};
                padding: 0.25rem 0.75rem;
                border-radius: 16px;
                font-weight: bold;
            ">{status_icon} {strategy.status.value}</span>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # Strategy info row - show chain badges for multi-chain
    if strategy.is_multi_chain and strategy.chains:
        # Multi-chain info display
        chain_badges_html = ""
        for chain in strategy.chains:
            chain_color = get_chain_color(chain)
            chain_badges_html += format_chain_badge(chain, chain_color)

        st.markdown(
            f"""
            <div style="display: flex; align-items: center; gap: 1rem; margin-bottom: 0.5rem;">
                <strong>Chains:</strong> {chain_badges_html}
            </div>
            """,
            unsafe_allow_html=True,
        )

        # Chain health indicators
        render_chain_health_indicators(strategy)

        col1, col2 = st.columns(2)
        with col1:
            st.markdown(f"**Protocols:** {strategy.protocol}")
        with col2:
            if strategy.last_action_at:
                st.markdown(f"**Last Action:** {strategy.last_action_at.strftime('%Y-%m-%d %H:%M')}")
    else:
        # Single-chain info
        col1, col2, col3 = st.columns(3)
        with col1:
            chain_color = get_chain_color(strategy.chain)
            chain_badge = format_chain_badge(strategy.chain, chain_color)
            st.markdown(f"**Chain:** {chain_badge}", unsafe_allow_html=True)
        with col2:
            st.markdown(f"**Protocol:** {strategy.protocol}")
        with col3:
            if strategy.last_action_at:
                st.markdown(f"**Last Action:** {strategy.last_action_at.strftime('%Y-%m-%d %H:%M')}")

    st.divider()

    # Key metrics - include bridge fees for multi-chain
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        # Show value confidence indicator
        confidence = getattr(strategy, "value_confidence", None)
        if confidence and confidence != "HIGH":
            confidence_icons = {
                "ESTIMATED": "⚠️",
                "STALE": "⏰",
                "UNAVAILABLE": "❓",
            }
            confidence_icon = confidence_icons.get(confidence, "")
            st.metric(
                "Total Value",
                format_usd(strategy.total_value_usd),
                help=f"Value confidence: {confidence} {confidence_icon}",
            )
        else:
            st.metric("Total Value", format_usd(strategy.total_value_usd))
    with col2:
        # Net PnL includes bridge fees
        net_pnl = strategy.pnl_24h_usd - strategy.bridge_fees_usd
        pnl_delta = f"{'+' if net_pnl >= 0 else ''}{net_pnl:,.2f}"
        st.metric(
            "24h PnL (Net)",
            format_usd(abs(net_pnl)),
            delta=pnl_delta,
            help=f"Includes ${strategy.bridge_fees_usd:,.2f} in bridge fees" if strategy.bridge_fees_usd > 0 else None,
        )
    with col3:
        if strategy.position and strategy.position.total_lp_value_usd > 0:
            st.metric("LP Value", format_usd(strategy.position.total_lp_value_usd))
        elif strategy.position and strategy.position.health_factor:
            st.metric("Health Factor", f"{strategy.position.health_factor:.2f}")
        else:
            st.metric("Positions", "N/A")
    with col4:
        if strategy.pnl_history:
            # Calculate 7d PnL
            pnl_7d = strategy.pnl_history[-1].pnl_usd
            st.metric("7d PnL", format_usd(abs(pnl_7d)), delta=f"{'+' if pnl_7d >= 0 else ''}{pnl_7d:,.2f}")

    st.divider()

    # Operator Card - show prominently if strategy needs attention
    if strategy.attention_required and strategy.operator_card:
        st.markdown("## Operator Alert")
        render_operator_card(strategy.operator_card, strategy.name)
        st.divider()

    # Action buttons row
    st.markdown("### Actions")

    # Check system health to determine which buttons should be enabled
    health = check_system_health()
    can_pause_resume = health.can_execute("pause_resume")
    can_bump_gas = health.can_execute("bump_gas")
    health.can_execute("execute_teardown")

    # Show warning if CLI isn't running
    if not health.cli_running:
        st.info(
            "**CLI Not Running** - Some actions are disabled. "
            "Start the strategy runner CLI to enable Pause/Resume, Bump Gas, and Execute Teardown.",
            icon="ℹ️",
        )

    action_col1, action_col2, action_col3, action_col4, action_col5 = st.columns(5)

    # Initialize action result state
    action_result_key = f"action_result_{strategy.id}"

    with action_col1:
        if strategy.status == StrategyStatus.RUNNING:
            if st.button("⏸️ Pause", use_container_width=True, disabled=not can_pause_resume):
                with st.spinner(f"Pausing {strategy.name}..."):
                    result = call_strategy_action(strategy.id, "pause")
                st.session_state[action_result_key] = result
                st.rerun()
        else:
            if st.button("▶️ Resume", use_container_width=True, disabled=not can_pause_resume):
                with st.spinner(f"Resuming {strategy.name}..."):
                    result = call_strategy_action(strategy.id, "resume")
                st.session_state[action_result_key] = result
                st.rerun()

    with action_col2:
        if st.button("⚙️ Config", use_container_width=True):
            st.query_params["page"] = "config"
            st.rerun()

    with action_col3:
        if st.button("🔄 Refresh", use_container_width=True):
            st.toast("Refreshing strategy data...")
            st.rerun()

    with action_col4:
        if strategy.status == StrategyStatus.STUCK:
            # Show gas bump dialog
            if st.button("⛽ Bump Gas", use_container_width=True, disabled=not can_bump_gas):
                st.session_state[f"show_gas_dialog_{strategy.id}"] = True
                st.rerun()

    with action_col5:
        # Close Strategy button - preview always available, execution requires CLI
        if st.button("🚪 Close Strategy", use_container_width=True, type="secondary"):
            st.query_params["page"] = "teardown"
            st.query_params["strategy_id"] = strategy.id
            st.rerun()

    # Show action result feedback
    if action_result_key in st.session_state:
        result = st.session_state[action_result_key]
        if result.get("success"):
            st.success(result.get("message", "Action completed successfully"))
        else:
            error_msg = result.get("error", "Action failed")
            if result.get("connection_error"):
                st.warning(f"API not available: {error_msg}")
            else:
                st.error(error_msg)
        # Clear result after showing
        del st.session_state[action_result_key]

    # Gas bump dialog
    gas_dialog_key = f"show_gas_dialog_{strategy.id}"
    if st.session_state.get(gas_dialog_key):
        st.markdown("---")
        st.markdown("#### Bump Gas Price")
        st.caption("Enter a higher gas price to speed up the pending transaction")

        col1, col2, col3 = st.columns([2, 1, 1])
        with col1:
            new_gas_price = st.number_input(
                "New Gas Price (Gwei)",
                min_value=0.1,
                max_value=1000.0,
                value=1.0,
                step=0.1,
                key=f"gas_price_input_{strategy.id}",
            )
        with col2:
            if st.button("Submit", key=f"submit_gas_{strategy.id}"):
                with st.spinner("Bumping gas price..."):
                    result = call_strategy_action(strategy.id, "bump-gas", {"gas_price_gwei": new_gas_price})
                st.session_state[action_result_key] = result
                st.session_state[gas_dialog_key] = False
                st.rerun()
        with col3:
            if st.button("Cancel", key=f"cancel_gas_{strategy.id}"):
                st.session_state[gas_dialog_key] = False
                st.rerun()

    st.divider()

    # Navigation buttons
    st.markdown("### Navigation")
    nav_col1, nav_col2, nav_col3, nav_col4 = st.columns(4)

    with nav_col1:
        if st.button("View Full Timeline", use_container_width=True):
            st.query_params["page"] = "timeline"

    with nav_col2:
        if st.button("Backtest", use_container_width=True):
            st.toast("Backtest will be available in US-027")

    with nav_col3:
        if st.button("View Logs", use_container_width=True):
            st.toast("Logs view coming soon...")

    with nav_col4:
        if st.button("View Config", use_container_width=True):
            st.query_params["page"] = "config"

    st.divider()

    # Main content area - two columns
    left_col, right_col = st.columns([2, 1])

    with left_col:
        # PnL Chart
        st.markdown("### Portfolio Performance (7 days)")
        try:
            render_pnl_chart(strategy)
        except Exception as e:
            st.error(f"Error rendering PnL chart: {e}")
            import traceback

            st.code(traceback.format_exc())

        try:
            render_profile_charts(strategy)
        except Exception as e:
            st.error(f"Error rendering strategy insights: {e}")
            import traceback

            st.code(traceback.format_exc())

    with right_col:
        # Position Summary - use multi-chain view if applicable
        try:
            if strategy.is_multi_chain:
                render_multi_chain_position_summary(strategy)
            else:
                render_position_summary(strategy)
        except Exception as e:
            st.error(f"Error rendering position summary: {e}")
            import traceback

            st.code(traceback.format_exc())

    st.divider()

    # Bridge Transfers section for multi-chain strategies
    if strategy.is_multi_chain and strategy.bridge_transfers:
        render_bridge_transfers(strategy)
        st.divider()

    # Timeline Events
    try:
        render_timeline_events(strategy, limit=10)
    except Exception as e:
        st.error(f"Error rendering timeline events: {e}")
        import traceback

        st.code(traceback.format_exc())
