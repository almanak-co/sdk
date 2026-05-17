"""Strategy detail page for the Almanak Operator Dashboard.

Displays detailed information about a single strategy.
Wires action buttons to real API endpoints.
"""

import html
import logging
import os
from decimal import Decimal, InvalidOperation
from typing import Any

import requests
import streamlit as st

from almanak.config.framework import framework_config_from_env
from almanak.framework.dashboard.components import render_operator_card
from almanak.framework.dashboard.config import API_BASE_URL, API_TIMEOUT, check_system_health
from almanak.framework.dashboard.data_source import execute_strategy_action
from almanak.framework.dashboard.models import Strategy
from almanak.framework.dashboard.pages._detail_render import (
    group_events_by_intent,
    status_badge,
    tx_display_fields,
)
from almanak.framework.dashboard.plots.lending_plots import plot_health_factor_gauge
from almanak.framework.dashboard.plots.lp_plots import (
    plot_position_range_status,
    plot_positions_over_time,
)
from almanak.framework.dashboard.plots.perp_plots import plot_leverage_gauge
from almanak.framework.dashboard.plots.portfolio_plots import plot_portfolio_value_over_time
from almanak.framework.dashboard.plots.ta_plots import plot_price_with_signals
from almanak.framework.dashboard.theme import get_chain_color, get_chain_health_color
from almanak.framework.dashboard.utils import (
    format_bridge_progress,
    format_timeline_summary,
    format_usd,
    get_chain_health_icon,
    get_chain_icon,
    get_timeline_event_icon,
    maybe_auto_select_strategy,
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


def _format_usd_str(raw: object) -> str:
    """Render a possibly-stringified USD amount as ``$1,234.56`` for table cells.

    Falls back to the raw stringified value on parse failure so we never
    blank out unexpected data; returns ``""`` for empty / None.
    """
    if raw is None or raw == "":
        return ""
    try:
        return format_usd(Decimal(str(raw)))
    except (InvalidOperation, ValueError):
        return str(raw)


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
    dashboard_secret = framework_config_from_env().dashboard_api_key
    if dashboard_secret is None:
        logger.error(
            "ALMANAK_DASHBOARD_API_KEY is not set. "
            "Refusing to call the strategy action REST API with an unauthenticated request."
        )
        return {
            "success": False,
            "error": (
                "ALMANAK_DASHBOARD_API_KEY environment variable is not set. "
                "Set it to a valid API key before invoking dashboard actions."
            ),
        }

    url = f"{API_BASE_URL}/api/strategies/{strategy_id}/{action}"
    headers = {"Content-Type": "application/json", "X-API-Key": dashboard_secret.get_secret_value()}

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


def render_paper_session_detail(strategy: Strategy) -> None:
    """Render paper trading session detail view."""
    pm = strategy.paper_metrics
    if pm is None:
        st.info("No paper trading metrics available.")
        return

    # Session summary
    st.markdown("### Session Summary")
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Ticks", pm.tick_count)
    with col2:
        rate = f"{pm.success_rate * 100:.1f}%" if pm.total_decisions > 0 else "N/A"
        st.metric("Success Rate", rate)
    with col3:
        st.metric("Simulated PnL", format_usd(pm.simulated_pnl_usd))
    with col4:
        st.metric("Gas Cost", format_usd(pm.total_gas_cost_usd))

    col5, col6, col7, col8 = st.columns(4)
    with col5:
        st.metric("Successes", pm.success_count)
    with col6:
        st.metric("Holds", pm.hold_count)
    with col7:
        st.metric("Errors", pm.error_count)
    with col8:
        st.metric("Trades/Hour", f"{pm.trades_per_hour:.1f}")

    st.divider()

    # Equity curve
    if pm.equity_curve:
        st.markdown("### Equity Curve")
        import pandas as pd

        eq_df = pd.DataFrame(
            {
                "timestamp": [pt.timestamp for pt in pm.equity_curve],
                "value_usd": [float(pt.value_usd) for pt in pm.equity_curve],
            }
        )
        eq_df = eq_df.set_index("timestamp")
        st.line_chart(eq_df, y="value_usd", use_container_width=True)
        st.divider()

    # Error breakdown
    if pm.error_breakdown:
        st.markdown("### Error Breakdown")
        import pandas as pd

        error_data = [
            {"Error Type": etype.replace("_", " ").title(), "Count": count}
            for etype, count in sorted(pm.error_breakdown.items(), key=lambda x: -x[1])
        ]
        st.dataframe(error_data, use_container_width=True, hide_index=True)
        st.divider()

    # Health telemetry
    if pm.tick_count > 0:
        st.markdown("### Health Telemetry")
        col1, col2, col3 = st.columns(3)
        with col1:
            fork_pct = pm.ticks_with_fork / pm.tick_count * 100
            st.metric("Fork Usage", f"{fork_pct:.0f}%", help="Ticks with active Anvil fork")
        with col2:
            ind_pct = pm.ticks_with_indicators / pm.tick_count * 100
            st.metric("Indicator Availability", f"{ind_pct:.0f}%", help="Ticks with market indicators")
        with col3:
            act_pct = pm.ticks_with_action / pm.tick_count * 100
            st.metric("Action Rate", f"{act_pct:.0f}%", help="Ticks that produced a trade")

    # Pre-flight status
    if pm.anvil_result:
        st.markdown("### Pre-flight Status")
        result_icons = {"SUCCESS": "Pass", "FAIL": "Fail", "HOLD": "Hold"}
        result_colors = {"SUCCESS": "#00c853", "FAIL": "#f44336", "HOLD": "#ffc107"}
        result_label = result_icons.get(pm.anvil_result.upper(), html.escape(pm.anvil_result))
        result_color = result_colors.get(pm.anvil_result.upper(), "#9e9e9e")
        st.markdown(
            f'<span style="color: {result_color}; font-weight: bold;">Anvil Test: {result_label}</span>',
            unsafe_allow_html=True,
        )

    # Promotion readiness
    st.divider()
    st.markdown("### Deployment Readiness")
    criteria = [
        ("Tick count >= 50", pm.tick_count >= 50, f"{pm.tick_count}/50"),
        ("At least 1 trade", pm.success_count >= 1, f"{pm.success_count} trades"),
        (
            "Success rate >= 80%",
            pm.success_rate >= Decimal("0.80") if pm.total_decisions > 0 else False,
            f"{pm.success_rate * 100:.0f}%" if pm.total_decisions > 0 else "N/A (no trades)",
        ),
        (
            "Error rate < 5%",
            pm.error_rate < Decimal("0.05") if pm.total_decisions > 0 else False,
            f"{pm.error_rate * 100:.1f}%" if pm.total_decisions > 0 else "N/A (no trades)",
        ),
        ("Session age > 1 hour", pm.session_age_hours > Decimal("1"), f"{pm.session_age_hours:.1f}h"),
    ]

    for label, passed, value in criteria:
        icon = "+" if passed else "-"
        color = "#00c853" if passed else "#f44336"
        st.markdown(
            f'<div style="color: {color}; margin-bottom: 0.25rem;">[{icon}] {label} ({value})</div>',
            unsafe_allow_html=True,
        )

    if pm.is_promotion_ready:
        st.success(
            "This paper session meets all readiness criteria. "
            "To deploy to mainnet, run: `almanak strat run -d <strategy_dir>`"
        )
    else:
        st.info("Paper session does not yet meet all deployment criteria.")


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
    from almanak.framework.dashboard.models import TimelineEventType

    events = strategy.timeline_events

    if not events:
        st.info("No recent events.")
        return

    st.markdown("### Recent Activity")

    # The runner emits two classes of low-signal *successful* events that
    # an operator rarely cares about and which trivially saturate the
    # limited window:
    #
    #   * STATE_CHANGE — one per ``decide()`` call (``started`` +
    #     ``returned HoldIntent`` pairs during long HOLD phases).
    #   * TRANSACTION_SUBMITTED / TRANSACTION_CONFIRMED — one per on-chain
    #     TX, duplicating the higher-level intent rows
    #     (``SWAP executed``, ``Opened LP position …``) that already
    #     convey the outcome.
    #
    # Hide both classes by default; the toggle flips them back on for
    # low-level debugging.
    #
    # TRANSACTION_FAILED / TRANSACTION_REVERTED are deliberately NOT in
    # this set — failure events are exactly what an operator needs to
    # see by default, and the orchestrator maps
    # ``ExecutionStatus.EXECUTION_FAILED`` onto TRANSACTION_FAILED as the
    # final completion event for a failed intent.
    _LOW_LEVEL = {
        TimelineEventType.STATE_CHANGE,
        TimelineEventType.TRANSACTION_SUBMITTED,
        TimelineEventType.TRANSACTION_CONFIRMED,
    }
    show_low_level = st.toggle(
        "Show low-level events (state machine + per-TX confirmations)",
        value=False,
        key=f"activity_show_low_level_{strategy.id}",
        help=(
            "When off, STATE_CHANGE rows (decide() started, returned "
            "HoldIntent) and successful per-TX TRANSACTION_* rows are "
            "hidden so the window fills with meaningful intent landings. "
            "Failures (TRANSACTION_FAILED / TRANSACTION_REVERTED) remain "
            "visible regardless. Flip on for low-level debugging."
        ),
    )
    if not show_low_level:
        events = [e for e in events if e.event_type not in _LOW_LEVEL]
        if not events:
            st.info(
                "No high-level events in the current window. "
                "Toggle on **Show low-level events** to see the underlying "
                "state-machine and per-TX rows."
            )
            return

    # Pure grouping + status-derivation logic lives in ``_detail_render`` so
    # this function only owns the Streamlit HTML emission.
    intent_groups, ungrouped_events = group_events_by_intent(events)
    sorted_intents = intent_groups[:limit]

    # Render each intent as a collapsible section
    for group in sorted_intents:
        intent_desc = group.intent_description
        badge = status_badge(group.status)
        tx_count = group.tx_count
        intent_events = sorted(group.events, key=lambda e: e.timestamp, reverse=True)
        latest_time = intent_events[0].timestamp.strftime("%Y-%m-%d %H:%M:%S")

        # Intent header with status
        st.markdown(
            f"""<div style="
                background-color: #1e1e1e;
                border: 1px solid #333;
                border-left: 4px solid {badge.color};
                border-radius: 8px;
                padding: 0.75rem 1rem;
                margin-bottom: 0.25rem;
            ">
                <div style="display: flex; justify-content: space-between; align-items: center;">
                    <span style="font-weight: 500;">{badge.icon} {intent_desc}</span>
                    <span style="color: {badge.color}; font-size: 0.85rem;">{badge.text}</span>
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
                fields = tx_display_fields(event)
                if fields is None:
                    continue  # Skip summary events; show only TX-level events.

                tx_hash = event.details.get("tx_hash", "") if event.details else ""
                time_str = event.timestamp.strftime("%H:%M:%S")
                tx_short = tx_hash[:10] + "..." if tx_hash else ""
                # Resolve chain for the explorer link in per-event priority
                # order (#1733). Multi-chain strategies produce events on
                # different chains, so falling through to ``strategy.chain``
                # too early sends the operator to the wrong block explorer.
                #
                # Priority:
                #   1. ``event.chain`` - typed top-level field, populated by
                #      the gateway timeline service for multi-chain events.
                #   2. ``event.details["chain"]`` - legacy / custom events
                #      that stuffed the chain into the free-form details bag.
                #   3. ``strategy.chain`` - single-chain strategies where
                #      every event is on the primary chain.
                #   4. ``"arbitrum"`` - last-resort fallback kept for
                #      backwards compatibility with existing renderer output.
                event_details_chain = (event.details or {}).get("chain") if event.details else None
                chain = getattr(event, "chain", None) or event_details_chain or strategy.chain or "arbitrum"
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
                        <span style="color: {fields.color};">{fields.icon}</span>
                        {tx_display}
                        <span style="color: #888; margin-left: 0.5rem;">{time_str}</span>
                        <span style="color: #666; margin-left: 0.5rem;">{fields.detail}</span>
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


def render_position_lifecycle(strategy: Strategy) -> None:
    """Render position lifecycle events with PnL attribution.

    Reads position events from the local SQLite store (no gateway gRPC path yet).
    Shows a table of all position events and per-position PnL breakdown for
    closed positions.
    """
    import asyncio
    import json

    from almanak.framework.dashboard.export import export_positions

    # Try to read position events from the local SQLite store
    events: list[dict] = []
    try:
        from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore

        db_path = _find_state_db(strategy.id)
        if not db_path:
            return  # No local DB found — position events not available

        config = SQLiteConfig(db_path=db_path)
        store = SQLiteStore(config)

        async def _fetch_position_events() -> list[dict]:
            """Run the store lifecycle + fetch in a single event loop.

            Previously used ``asyncio.get_event_loop().run_until_complete(...)``
            three separate times, which is deprecated under Python 3.10+ (will
            raise ``DeprecationWarning`` and eventually ``RuntimeError`` once
            there is no running loop in the current thread). Wrapping the
            sequence in a single coroutine and dispatching through
            ``asyncio.run`` (#1712) makes lifecycle cleanup deterministic
            and removes the three-loop-spin-up overhead on every render.
            """
            await store.initialize()
            try:
                return await store.get_position_events(strategy.id, limit=200)
            finally:
                await store.close()

        events = asyncio.run(_fetch_position_events())
    except Exception:
        return  # Silently skip if SQLite not available

    if not events:
        return

    # token_id → strategy-stamped handle (e.g. "leg_narrow") for multi-position
    # strategies that name their legs via ``registry_handle``.
    handles = _fetch_registry_handles(db_path, strategy.id)

    st.markdown("### Position Lifecycle")

    # Summary metrics
    open_count = sum(1 for e in events if e.get("event_type") == "OPEN")
    close_count = sum(1 for e in events if e.get("event_type") == "CLOSE")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Positions Opened", open_count)
    with col2:
        st.metric("Positions Closed", close_count)
    with col3:
        st.metric("Total Events", len(events))

    # Events table
    show_alias = any(handles.values())
    table_data = []
    for evt in events:
        position_id = str(evt.get("position_id", ""))
        tx_hash = str(evt.get("tx_hash", "") or "")
        evt_chain = str(evt.get("chain", "") or "")
        row: dict[str, Any] = {
            "Time": evt.get("timestamp", "")[:19],
            "Type": evt.get("event_type", ""),
            "Position": evt.get("position_type", ""),
            "ID": position_id[:12],
        }
        if show_alias:
            row["Alias"] = handles.get(position_id, "")
        row.update(
            {
                "Protocol": evt.get("protocol", ""),
                "Value (USD)": _format_usd_str(evt.get("value_usd")),
                "TX": get_explorer_url(evt_chain, tx_hash) if tx_hash else "",
            }
        )
        table_data.append(row)

    st.dataframe(
        table_data,
        use_container_width=True,
        hide_index=True,
        column_config={
            "TX": st.column_config.LinkColumn(
                "TX",
                display_text=r".*/tx/(0x[a-fA-F0-9]{8})",
                help="Open transaction in block explorer",
            ),
        },
    )

    # PnL attribution for closed positions
    closed_with_attr = [e for e in events if e.get("event_type") == "CLOSE" and e.get("attribution_json", "{}") != "{}"]
    if closed_with_attr:
        st.markdown("#### PnL Attribution (Closed Positions)")
        attr_data = []
        for evt in closed_with_attr:
            try:
                attr = json.loads(evt.get("attribution_json", "{}"))
                position_id = str(evt.get("position_id", ""))
                attr_row: dict[str, Any] = {"Position": position_id[:12]}
                if show_alias:
                    attr_row["Alias"] = handles.get(position_id, "")
                attr_row.update(
                    {
                        "Type": attr.get("position_type", ""),
                        # Empty != Zero: missing attribution fields render blank
                        # rather than $0.00 so partial / legacy payloads do not
                        # display as measured zeroes (AGENTS.md §Accounting).
                        "Net PnL": _format_usd_str(attr.get("net_pnl_usd")),
                        "Price PnL": _format_usd_str(attr.get("price_pnl_usd")),
                        "Fee PnL": _format_usd_str(attr.get("fee_pnl_usd")),
                        "Gas": _format_usd_str(attr.get("gas_usd")),
                        "Version": f"v{attr.get('version', '?')}",
                    }
                )
                attr_data.append(attr_row)
            except (json.JSONDecodeError, TypeError):
                continue

        if attr_data:
            st.dataframe(attr_data, use_container_width=True, hide_index=True)

    # Export button
    csv_bytes = export_positions(events, fmt="csv")
    if csv_bytes:
        st.download_button(
            label="Export Position Events (CSV)",
            data=csv_bytes,
            file_name=f"position_events_{strategy.id}.csv",
            mime="text/csv",
        )


def render_positions_summary(strategy: Strategy) -> None:
    """Render a Positions table aggregated from ``position_registry``.

    One row per position (handle + physical_identity_hash) with its status,
    open/close times, NFT/position id, and tx hashes. Defaults to *all*
    positions so a torn-down strategy still shows what it ever held; an
    "Open only" toggle filters to live positions for the live-monitoring case.

    Distinct from ``render_position_lifecycle`` (which lists raw OPEN/CLOSE
    *events* — one row per event, two per round-trip): this table collapses
    each round-trip to a single row keyed by the registry handle.
    """
    db_path = _find_state_db(strategy.id)
    if not db_path:
        return

    rows = _fetch_positions_registry(db_path, strategy.id)
    if not rows:
        return

    st.markdown("### Positions")
    show_open_only = st.toggle(
        "Show open only",
        value=False,
        key=f"positions_open_only_{strategy.id}",
        help="Hide closed and reorg-invalidated rows.",
    )
    visible = [r for r in rows if r["status"] == "open"] if show_open_only else rows
    if not visible:
        st.info("No open positions." if show_open_only else "No positions on record.")
        return

    table_data = [
        {
            "Alias": r["handle"] or "",
            "Chain": r["chain"],
            "Type": r["primitive"],
            "Status": r["status"],
            "Opened": (r["opened_at"] or "")[:19],
            "Closed": (r["closed_at"] or "")[:19],
            "Position ID": r["position_id"][:18],
            "Open TX": get_explorer_url(r["chain"], r["opened_tx"]) if r["opened_tx"] else "",
            "Close TX": get_explorer_url(r["chain"], r["closed_tx"]) if r["closed_tx"] else "",
        }
        for r in visible
    ]
    st.dataframe(
        table_data,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Open TX": st.column_config.LinkColumn(
                "Open TX",
                display_text=r".*/tx/(0x[a-fA-F0-9]{8})",
                help="Open transaction in block explorer",
            ),
            "Close TX": st.column_config.LinkColumn(
                "Close TX",
                display_text=r".*/tx/(0x[a-fA-F0-9]{8})",
                help="Open transaction in block explorer",
            ),
        },
    )


def _fetch_positions_registry(db_path: str, deployment_id: str) -> list[dict[str, Any]]:
    """Read ``position_registry`` rows for a deployment, decorated for display.

    Each returned dict carries the original registry columns plus a flattened
    ``position_id`` (NFT token id when present in the JSON payload, else the
    physical-identity hash) and a synthesised ``opened_at`` / ``closed_at``
    pulled from the latest matching ``position_events`` row — the registry
    itself records block numbers, not wall-clock times.
    """
    import json
    import sqlite3

    out: list[dict[str, Any]] = []
    try:
        with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True) as conn:
            conn.row_factory = sqlite3.Row
            # ``status ASC`` would alphabetically rank ``closed`` above ``open`` —
            # an operator monitoring live risk wants open rows pinned to the top.
            # The CASE expression also pushes ``reorg_invalidated`` last so
            # rare-but-noisy reorg rows don't crowd the actionable view.
            registry_rows = conn.execute(
                "SELECT chain, primitive, physical_identity_hash, handle, status, "
                "payload, opened_tx, closed_tx FROM position_registry "
                "WHERE deployment_id = ? "
                "ORDER BY CASE status "
                "WHEN 'open' THEN 0 WHEN 'closed' THEN 1 ELSE 2 END ASC, "
                "(opened_at_block IS NULL) ASC, opened_at_block ASC",
                (deployment_id,),
            ).fetchall()
            # SNAPSHOT / COLLECT_FEES rows can be 100s per long-running strategy
            # and only OPEN/CLOSE carry the timestamps this function consults —
            # filtering at the SQL boundary keeps memory + scan cost flat.
            event_rows = conn.execute(
                "SELECT position_id, event_type, timestamp FROM position_events "
                "WHERE deployment_id = ? AND event_type IN ('OPEN', 'CLOSE') "
                "ORDER BY timestamp ASC",
                (deployment_id,),
            ).fetchall()
    except sqlite3.Error:
        return []

    # position-key → (first_open_ts, last_close_ts) so each registry row can
    # surface a wall-clock window without a per-row subquery. We key by every
    # identifier that a position_event might use — NFT token_id when present,
    # else the registry's physical_identity_hash for non-NFT primitives.
    timings: dict[str, dict[str, str]] = {}
    for ev in event_rows:
        pid = str(ev["position_id"] or "")
        if not pid:
            continue
        slot = timings.setdefault(pid, {})
        if ev["event_type"] == "OPEN":
            slot.setdefault("opened_at", ev["timestamp"])
        elif ev["event_type"] == "CLOSE":
            slot["closed_at"] = ev["timestamp"]

    for row in registry_rows:
        try:
            payload = json.loads(row["payload"] or "{}")
        except (json.JSONDecodeError, TypeError):
            payload = {}
        token_id = str(payload.get("token_id") or payload.get("position_id") or "")
        # Match the position_id fallback used a few lines below so the timing
        # lookup and the displayed identifier always reference the same key.
        # Non-NFT primitives don't write token_id to the payload and key their
        # events by ``physical_identity_hash`` instead.
        lookup_id = token_id or row["physical_identity_hash"]
        slot = timings.get(lookup_id, {})
        out.append(
            {
                "chain": row["chain"],
                "primitive": row["primitive"],
                "physical_identity_hash": row["physical_identity_hash"],
                "handle": row["handle"],
                "status": row["status"],
                "opened_tx": row["opened_tx"],
                "closed_tx": row["closed_tx"],
                "position_id": lookup_id,
                "opened_at": slot.get("opened_at"),
                "closed_at": slot.get("closed_at"),
            }
        )
    return out


def render_lp_position_history(strategy: Strategy) -> None:
    """Render LP position range overlaid on the pool's price history.

    Sources data directly from local SQLite — ``position_events`` for the
    OPEN/CLOSE timestamps and tick range, and ``portfolio_snapshots`` for
    the per-snapshot token prices. Silently no-ops when there are no LP
    events, no snapshots, or the strategy isn't running locally (hosted
    deployments don't have a SQLite path to read from).

    The plot itself (``plot_positions_over_time``) is already used by the
    LP dashboard template (Blueprint 23); this surfaces the same chart on
    the framework-default detail page so strategies without a custom
    ``dashboard/ui.py`` get the same view.
    """
    db_path = _find_state_db(strategy.id)
    if not db_path:
        return

    lp_event_rows, snapshot_rows = _read_lp_history_rows(db_path, strategy.id)
    if not lp_event_rows or not snapshot_rows:
        return

    # Default page plots one pool at a time — multi-pool strategies fall
    # back to the first pool. Callers wanting more should write a custom
    # dashboard via ``dashboard/ui.py``.
    first = lp_event_rows[0]
    token0_sym = (first["token0"] or "").upper()
    token1_sym = (first["token1"] or "").upper()
    if not token0_sym or not token1_sym:
        return

    # Filter events to the chosen pool — without this, multi-pool strategies
    # would overlay rectangles from other pools onto this pool's price line
    # with incorrect decimals (audit finding: P2 Codex + Important Claude).
    pool_event_rows = [
        r
        for r in lp_event_rows
        if (r["token0"] or "").upper() == token0_sym and (r["token1"] or "").upper() == token1_sym
    ]
    if not pool_event_rows:
        return

    price_df, decimals0, decimals1 = _build_lp_price_series(snapshot_rows, token0_sym, token1_sym)
    if price_df.empty or decimals0 is None or decimals1 is None:
        return

    handles = _fetch_registry_handles(db_path, strategy.id)
    positions = _build_lp_position_entries(pool_event_rows, decimals0, decimals1, handles)
    if not positions:
        return

    st.markdown("### Position Range History")
    fig = plot_positions_over_time(
        positions=positions,
        price_data=price_df,
        title=f"{token0_sym}/{token1_sym} price + position ranges",
    )
    st.plotly_chart(fig, use_container_width=True)


def _read_lp_history_rows(db_path: str, deployment_id: str) -> tuple[list[Any], list[Any]]:
    """Fetch the two SQL row sets ``render_lp_position_history`` needs."""
    import sqlite3

    try:
        with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True) as conn:
            conn.row_factory = sqlite3.Row
            # ``_build_lp_position_entries`` only inspects OPEN + CLOSE rows;
            # SNAPSHOT / COLLECT_FEES events would be scanned and discarded.
            # Filter at the SQL boundary so the dashboard stays responsive on
            # strategies with hundreds of intra-position snapshots.
            lp_event_rows = conn.execute(
                "SELECT position_id, event_type, timestamp, tick_lower, tick_upper, "
                "token0, token1, chain "
                "FROM position_events "
                "WHERE deployment_id = ? AND position_type = 'LP' "
                "AND event_type IN ('OPEN', 'CLOSE') "
                "ORDER BY timestamp ASC",
                (deployment_id,),
            ).fetchall()
            snapshot_rows = conn.execute(
                "SELECT timestamp, token_prices_json FROM portfolio_snapshots "
                "WHERE deployment_id = ? ORDER BY timestamp ASC",
                (deployment_id,),
            ).fetchall()
    except sqlite3.Error:
        return [], []
    return list(lp_event_rows), list(snapshot_rows)


def _build_lp_price_series(
    snapshot_rows: list[Any],
    token0_sym: str,
    token1_sym: str,
) -> tuple["Any", int | None, int | None]:
    """Build the (timestamp, price) DataFrame + token decimals from snapshots.

    Price is ``price(token0) / price(token1)`` so the y-axis carries the
    same units as Uniswap V3's pool price (e.g. USDC per WETH for the
    WETH/USDC pool).
    """
    import json

    import pandas as pd

    decimals0: int | None = None
    decimals1: int | None = None
    price_rows: list[dict] = []
    for snap in snapshot_rows:
        try:
            prices = json.loads(snap["token_prices_json"] or "{}")
        except (json.JSONDecodeError, TypeError):
            continue
        price0_dec: Decimal | None = None
        price1_dec: Decimal | None = None
        for meta in prices.values():
            sym = (meta.get("symbol") or "").upper()
            if sym == token0_sym:
                price0_dec = Decimal(str(meta.get("price_usd", "0")))
                if decimals0 is None:
                    decimals0 = int(meta.get("decimals", 18))
            elif sym == token1_sym:
                price1_dec = Decimal(str(meta.get("price_usd", "0")))
                if decimals1 is None:
                    decimals1 = int(meta.get("decimals", 18))
        if price0_dec is None or price1_dec is None or price1_dec == 0:
            continue
        price_rows.append(
            {
                "timestamp": pd.to_datetime(snap["timestamp"]),
                "price": float(price0_dec / price1_dec),
            }
        )
    return pd.DataFrame(price_rows), decimals0, decimals1


def _build_lp_position_entries(
    lp_event_rows: list[Any],
    decimals0: int,
    decimals1: int,
    handles: dict[str, str],
) -> list[dict[str, Any]]:
    """Group LP OPEN/CLOSE event rows into ``plot_positions_over_time`` dicts."""
    from math import log

    import pandas as pd

    by_position: dict[str, dict[str, Any]] = {}
    for row in lp_event_rows:
        pid = str(row["position_id"] or "")
        if not pid:
            continue
        entry = by_position.setdefault(
            pid,
            {"position_id": pid, "tick_lower": None, "tick_upper": None, "date_start": None, "date_end": None},
        )
        if row["event_type"] == "OPEN":
            entry["date_start"] = row["timestamp"]
            entry["tick_lower"] = row["tick_lower"]
            entry["tick_upper"] = row["tick_upper"]
        elif row["event_type"] == "CLOSE":
            entry["date_end"] = row["timestamp"]

    log1_0001 = log(1.0001)
    # Use Decimal-typed adjustment so pools where token0 has fewer decimals
    # than token1 (e.g. USDC/WETH on Ethereum: decimals0=6, decimals1=18) don't
    # turn ``decimal_adjust`` into a float and crash the ``Decimal × float``
    # multiplication below. Audit finding: Important Claude.
    exponent = decimals0 - decimals1
    decimal_adjust = Decimal(10) ** exponent if exponent >= 0 else Decimal(1) / (Decimal(10) ** (-exponent))
    positions: list[dict[str, Any]] = []
    for entry in by_position.values():
        tick_lower = entry["tick_lower"]
        tick_upper = entry["tick_upper"]
        if entry["date_start"] is None or tick_lower is None or tick_upper is None:
            continue
        bound_price_lower = float(_safe_exp(tick_lower * log1_0001) * decimal_adjust)
        bound_price_upper = float(_safe_exp(tick_upper * log1_0001) * decimal_adjust)
        date_end_raw = entry["date_end"]
        date_end = pd.to_datetime(date_end_raw) if date_end_raw else None
        alias = handles.get(entry["position_id"], "")
        position_label = f"{alias} ({entry['position_id'][:6]})" if alias else entry["position_id"]
        positions.append(
            {
                "position_id": position_label,
                "date_start": pd.to_datetime(entry["date_start"]),
                "date_end": date_end,
                "bound_tick_lower": tick_lower,
                "bound_tick_upper": tick_upper,
                "bound_price_lower": bound_price_lower,
                "bound_price_upper": bound_price_upper,
                "is_active": date_end is None,
            }
        )
    return positions


def _safe_exp(x: float) -> Decimal:
    """``Decimal(exp(x))`` with overflow / underflow guarded.

    Tick→price conversion uses ``1.0001 ** tick``; extreme ticks (Solana
    Whirlpool ±443636, Uniswap V3 ±887272) blow past ``float`` range. We
    fall back to ``0`` / ``inf`` rather than raising — the rectangle just
    won't be drawn at that bound.
    """
    from math import exp as math_exp

    try:
        return Decimal(str(math_exp(x)))
    except OverflowError:
        return Decimal("inf") if x > 0 else Decimal("0")


def _fetch_registry_handles(db_path: str, deployment_id: str) -> dict[str, str]:
    """Return a {position-key → handle} map from ``position_registry``.

    The strategy-stamped ``registry_handle`` (e.g. ``leg_narrow`` /
    ``leg_wide`` on multi-position fixtures) lives on the registry row, not
    on ``position_events``. To resolve a handle for any row in
    ``position_events`` we have to be ready for either key shape that
    primitives in the wild use:

    * **NFT-backed primitives** (Uniswap V3 LP, Pendle LP, …) write the NFT
      ``token_id`` to the registry payload AND to ``position_events.position_id``.
    * **Non-NFT primitives** (Aave V3 lending, perps, future CDP/vault) have
      no token_id; ``position_events.position_id`` matches the registry's
      ``physical_identity_hash`` instead.

    Emit BOTH keys whenever they exist so renderers consuming events from
    either family resolve the alias without per-primitive special-casing.

    Missing handles, malformed payloads, or a missing DB silently yield an
    empty map — table renderers treat no-alias as "single-leg" and omit the
    column.
    """
    import json
    import sqlite3

    out: dict[str, str] = {}
    try:
        with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True) as conn:
            conn.row_factory = sqlite3.Row
            # Sort so ``closed`` rows are read AFTER ``open`` ones — for a
            # handle re-bound to a new position the surfaced alias should
            # reflect the most recent registry write, not whichever row the
            # SQLite engine happened to return first.
            cursor = conn.execute(
                "SELECT handle, payload, physical_identity_hash FROM position_registry "
                "WHERE deployment_id = ? AND handle IS NOT NULL AND handle != '' "
                "ORDER BY CASE status WHEN 'open' THEN 0 WHEN 'closed' THEN 1 ELSE 2 END ASC",
                (deployment_id,),
            )
            for row in cursor:
                handle = row["handle"]
                if not handle:
                    continue
                try:
                    payload = json.loads(row["payload"] or "{}")
                except (json.JSONDecodeError, TypeError):
                    payload = {}
                token_id = payload.get("token_id") or payload.get("position_id")
                if token_id:
                    out[str(token_id)] = handle
                # Always also key by physical_identity_hash so non-NFT
                # primitives (or any consumer that joins by hash) resolve.
                phys = row["physical_identity_hash"]
                if phys:
                    out[str(phys)] = handle
    except sqlite3.Error:
        return {}
    return out


def _find_state_db(strategy_id: str) -> str | None:
    """Find the SQLite state DB for a strategy.

    Resolution order (deterministic-first, issue #1713 + VIB-3761):

    1. ``almanak.framework.local_paths.local_db_path()`` — the canonical
       resolver shared by every other module that needs the local DB.
       Honours ``ALMANAK_STATE_DB`` (operator override),
       ``ALMANAK_STRATEGY_FOLDER`` (set by ``almanak strat run``),
       ``ALMANAK_GATEWAY_DB_PATH``, and falls back to the per-user
       utility directory. The cwd-relative ``./almanak_state.db`` legacy
       default is removed (April 29 silent-failure root cause).
    2. Deployment-id lookup: ``~/.almanak/state/<strategy_id>/state.db`` and
       ``~/.almanak/state/<base_name>/state.db``.
    3. Legacy flat locations: ``~/.almanak/state/state.db`` and
       ``./.almanak/state.db``.

    When more than one candidate outside of (1)-(2) exists, a warning is
    logged identifying every match; the first hit is still returned so the
    dashboard stays usable, but the operator is alerted to the ambiguity
    instead of the old code silently picking one path.
    """

    # 1. Canonical local-DB resolver — single source of truth across the SDK.
    try:
        from almanak.framework.local_paths import LocalPathError, local_db_path

        canonical = local_db_path()
        if canonical.exists():
            return str(canonical)
    except LocalPathError:
        # Hosted mode — dashboard reads from Postgres, not SQLite.
        return None

    # 2. Deterministic deployment_id lookup. If the caller passed a full
    #    deployment id (``StrategyName:abc123``) we prefer an exact match on
    #    that id over anything derived from the base strategy name.
    home = os.path.expanduser("~")
    deployment_candidates: list[str] = [
        os.path.join(home, ".almanak", "state", strategy_id, "state.db"),
    ]
    if ":" in strategy_id:
        base_name = strategy_id.split(":", 1)[0]
        deployment_candidates.append(
            os.path.join(home, ".almanak", "state", base_name, "state.db"),
        )

    for path in deployment_candidates:
        if os.path.exists(path):
            return path

    # 4. Pattern-search fallback - legacy flat locations. If more than one
    #    of these exists we have no way to know which one belongs to the
    #    strategy being viewed; warn loudly and return the first match so the
    #    operator can investigate instead of silently picking a winner.
    fallback_candidates = [
        os.path.join(home, ".almanak", "state", "state.db"),
        os.path.join(".", ".almanak", "state.db"),
    ]
    matches = [p for p in fallback_candidates if os.path.exists(p)]
    if len(matches) > 1:
        logger.warning(
            "Multiple legacy state DB candidates matched for strategy %s: %s. "
            "Returning the first match (%s). Set ALMANAK_STATE_DB to disambiguate "
            "or migrate the DB to ~/.almanak/state/<strategy_id>/state.db.",
            strategy_id,
            matches,
            matches[0],
        )
    if matches:
        return matches[0]

    return None


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


def page(strategies: list[Strategy]) -> None:  # noqa: C901
    """Render the strategy detail page.

    Args:
        strategies: List of all strategy data objects
    """
    # Get strategy ID from query params
    strategy_id = st.query_params.get("strategy_id")

    if not strategy_id:
        maybe_auto_select_strategy(strategies)
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

    # Header, chain info, and key metrics (extracted to _detail_header for
    # testability; see Phase 5b plan).
    from almanak.framework.dashboard.pages._detail_header import (
        render_chain_info_row,
        render_quant_header,
        render_strategy_header,
    )

    render_strategy_header(strategy)
    render_chain_info_row(strategy)

    st.divider()

    # Paper trading sessions get a dedicated detail view
    if strategy.execution_mode == "paper":
        render_paper_session_detail(strategy)
        return

    # Senior-Quant header — money / risk / audit. VIB-3969: three
    # focused gateway RPCs replace the legacy ``GetQuantHeader`` god-
    # object. We fire them concurrently from a single ThreadPool because
    # the three handlers do disjoint Postgres reads and the latency wins
    # are real (PnL is hot-read, audit is heavy server-side compute).
    # Each slice degrades independently — a missing audit fetch should
    # not blank the eyeball card.
    from concurrent.futures import ThreadPoolExecutor

    from almanak.framework.dashboard.data_source import (
        get_audit_posture,
        get_cost_stack,
        get_pnl_summary,
    )
    from almanak.framework.dashboard.gateway_client import (
        AuditPosture,
        CostStackInfo,
        PnLSummary,
    )

    # Each future is collected in its own try block so one failed slice
    # does not discard already-computed siblings (CodeRabbit). The
    # data_source layer already converts gateway outages into ``None``
    # returns, so reaching the broad ``except`` here means an unexpected
    # programming error (bad payload, stub mismatch) — surface it but
    # keep rendering the slices we did manage to fetch.
    pnl_slice: PnLSummary | None = None
    cost_slice: CostStackInfo | None = None
    audit_slice: AuditPosture | None = None
    with ThreadPoolExecutor(max_workers=3) as ex:
        f_pnl = ex.submit(get_pnl_summary, strategy.id)
        f_cost = ex.submit(get_cost_stack, strategy.id)
        f_audit = ex.submit(get_audit_posture, strategy.id)
        try:
            pnl_slice = f_pnl.result()
        except Exception as exc:  # noqa: BLE001 — page must remain renderable
            st.error(f"Quant pnl slice unavailable: {exc}")
        try:
            cost_slice = f_cost.result()
        except Exception as exc:  # noqa: BLE001
            st.error(f"Quant cost slice unavailable: {exc}")
        try:
            audit_slice = f_audit.result()
        except Exception as exc:  # noqa: BLE001
            st.error(f"Quant audit slice unavailable: {exc}")

    render_quant_header(strategy, pnl_slice, cost_slice, audit_slice)

    st.divider()

    # Operator Card - show prominently if strategy needs attention
    if strategy.attention_required and strategy.operator_card:
        st.markdown("## Operator Alert")
        render_operator_card(strategy.operator_card, strategy.name)
        st.divider()

    # Action buttons, result feedback, and gas-bump dialog (extracted to
    # _detail_actions for testability; see Phase 5a plan).
    from almanak.framework.dashboard.pages._detail_actions import (
        handle_action_result,
        render_action_row,
        render_gas_bump_dialog,
    )

    health = check_system_health()
    render_action_row(strategy, health)
    handle_action_result(strategy.id)
    render_gas_bump_dialog(strategy.id)

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

    # Main content area (two-column layout) + bridge / lifecycle / timeline
    # lower-stack are extracted to _detail_content for testability and to
    # collapse the duplicated try/except+traceback boilerplate; see Phase 5c
    # plan.
    from almanak.framework.dashboard.pages._detail_content import (
        render_bridge_and_lifecycle,
        render_main_content_columns,
    )

    render_main_content_columns(strategy)

    st.divider()

    render_bridge_and_lifecycle(strategy)

    # Reconciliation surface (VIB-4548): net-new audit tab + operator
    # controls backed by the Phase 1 RPCs. Both surfaces degrade to a
    # single st.info if the gateway connection fails, so always safe to
    # invoke. Lazy import keeps the dashboard package streamlit-free at
    # the gateway sidecar (see VIB-4048 / __init__ docstring).
    from almanak.framework.dashboard.sections_operator import (
        render_reconciliation_operator_panel,
    )
    from almanak.framework.dashboard.sections_reconciliation import (
        render_reconciliation_report_section,
    )
    from almanak.framework.dashboard.service_client import (
        DashboardClientError,
        get_dashboard_service_client,
        get_operator_dashboard_service_client,
    )

    # Render the report and the operator panel independently so a failure on
    # one (read-side gateway down, operator client unreachable, ...) does not
    # blank both surfaces, and so the second section's connect() does not
    # block page render after the first has already degraded.
    _dash_read = get_dashboard_service_client()
    try:
        if not _dash_read.is_connected:
            _dash_read.connect()
        render_reconciliation_report_section(strategy.id, _dash_read)
    except DashboardClientError as exc:
        st.info(f"Reconciliation report temporarily unavailable: {exc}")

    _dash_op = get_operator_dashboard_service_client()
    try:
        if not _dash_op.is_connected:
            _dash_op.connect()
        render_reconciliation_operator_panel(strategy.id, _dash_op)
    except DashboardClientError as exc:
        st.info(f"Reconciliation operator panel temporarily unavailable: {exc}")
