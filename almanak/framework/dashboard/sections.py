"""Section helpers callable from inside ``render_custom_dashboard()``.

Strategy authors own every pixel of their custom dashboard, but a few
sections are generic enough across DeFi primitives that every dashboard
should embed them: the PnL eyeball card at the top, the cost-stack
breakdown above the trade tape, and the trade tape at the bottom. This
module is the home for those shared section building blocks.

Distinct from ``almanak/framework/dashboard/pages/`` which contains
*operator-console* page renderers invoked by the multi-strategy
dashboard router (``app.py``). Pages render full pages with their own
chrome; sections render embeddable blocks with just a divider and a
heading, so they slot cleanly into an author-written
``render_custom_dashboard()``.

Recommended layout — three sections framing the author's primitive-
specific content (VIB-3969)::

    from almanak.framework.dashboard import (
        render_pnl_section,         # top — 5-second eyeball
        render_cost_stack_section,  # bottom — life-to-date costs
        render_trade_tape_section,  # bottom — TX-level audit
    )

    def render_custom_dashboard(deployment_id, strategy_config, api_client, session_state):
        # 1. Title / strategy info
        st.title(...)

        # 2. Eyeball — am I making or losing money?
        render_pnl_section(deployment_id)

        # 3. Strategy-specific content (LP plots / HF gauge / RSI chart / ...)
        # ... author's custom UI here ...

        # 4. Audit — paper trade, transactions, full breakdown
        render_cost_stack_section(deployment_id)
        render_trade_tape_section(deployment_id)

Each helper is intentionally thin (divider + heading + delegate) so the
contract is trivially stable across releases. Each calls exactly one
focused gateway RPC — section authors never pay the cost of fetching
data they don't render.
"""

from __future__ import annotations

import json
import logging
from typing import Any

import streamlit as st

from almanak.framework.dashboard.data_source import (
    GatewayConnectionError,
    get_cost_stack,
    get_pnl_summary,
)
from almanak.framework.dashboard.pages._detail_header import (
    render_cost_stack,
    render_money_trail,
)
from almanak.framework.dashboard.pages.trade_tape import render_trade_tape

logger = logging.getLogger(__name__)


def render_pnl_section(deployment_id: str) -> None:
    """Render the 5-second-eyeball PnL section (VIB-3969).

    Money Trail row: Deployed / NAV / Lifetime PnL / Net APR. The
    standard top-of-dashboard card so an operator answers "am I making
    or losing money?" before scrolling. Backed by the gateway's
    ``GetPnLSummary`` RPC; on RPC failure the section degrades to an
    info banner rather than crashing the page.

    Conventionally placed immediately below the strategy title.

    Args:
        deployment_id: The deployment id (passed straight through from
            ``render_custom_dashboard``'s first positional argument).
    """
    st.divider()
    st.markdown("### PnL")
    try:
        pnl = get_pnl_summary(deployment_id)
    except GatewayConnectionError as exc:
        # VIB-4047: distinguish a genuine disconnect from an UNAUTHENTICATED
        # gateway (managed mainnet session-token mismatch). Both fail LOUD +
        # CLEAN here — a quiet "temporarily unavailable" hid a dashboard that
        # could not read live money for an entire session.
        from almanak.framework.dashboard.error_ui import render_gateway_error

        render_gateway_error(exc, context="PnL", raw=str(exc))
        return
    if pnl is None:
        st.info("No PnL data yet — run a few iterations to populate the snapshot table.")
        return
    # Strategy PnL / APR tiles need the realized-PnL components from the cost
    # stack. Fetch it best-effort: a failed cost RPC degrades those two tiles
    # to "—" rather than blocking the whole PnL row.
    try:
        cost = get_cost_stack(deployment_id)
    except GatewayConnectionError:
        cost = None
    render_money_trail(pnl, cost)


# Preset NAV/PnL chart ranges (VIB-5059 Phase 2). "All" (0 seconds) = full
# lifetime; the others are trailing windows ending "now". The server decimates
# every range to a constant point budget, so even "All" stays bounded.
NAV_RANGE_SECONDS: dict[str, int] = {"24h": 86_400, "7d": 604_800, "30d": 2_592_000, "All": 0}
"""Public preset-range → trailing-window-seconds map. ``"All"`` → ``0`` (open
bound = full lifetime). Single source of truth shared by the NAV selector here
and the TA/LP price-chart templates that follow the same range (VIB-5114)."""

# Backwards-compatible private alias (this module's render code referenced the
# underscored name before the constant was promoted to public for VIB-5114).
_NAV_RANGE_SECONDS = NAV_RANGE_SECONDS
_NAV_RANGE_ORDER = ("24h", "7d", "30d", "All")
_NAV_MAX_POINTS = 1500


def nav_range_session_key(deployment_id: str) -> str:
    """The ``session_state`` key the NAV range selector writes/reads.

    Single source of truth for the cross-section coordination key (VIB-5114):
    the NAV selector (:func:`render_nav_history_section`) persists the operator's
    chosen preset here via the ``st.radio`` ``key=``, and the TA/LP price-chart
    templates read the SAME key so their candle fetch + trade markers follow the
    range the operator picked on the NAV chart. Scoped per ``deployment_id`` so
    two strategies rendered in one Streamlit session never cross-contaminate.
    """
    return f"nav_range_{deployment_id}"


def selected_nav_range_seconds(
    deployment_id: str,
    session_state: dict[str, Any] | None = None,
) -> int | None:
    """Resolve the operator's selected NAV range to trailing-window **seconds**.

    Reads the shared range key (:func:`nav_range_session_key`) from the supplied
    ``session_state`` (the dict the templates already thread through) so the
    price chart follows the range the operator picked on the NAV chart
    (VIB-5114). Returns:

    - the trailing-window seconds (e.g. ``604_800`` for ``"7d"``) when a bounded
      preset is selected;
    - ``0`` when ``"All"`` is selected (open bound = full lifetime — a *measured*
      "no lower bound", distinct from "unset");
    - ``None`` when no range has been selected yet, or the stored value is not a
      known preset — the caller keeps its existing default-window behaviour
      unchanged (Empty != Zero: unset is not "All").

    ``session_state`` defaults to ``None`` rather than reading ``st.session_state``
    implicitly so the resolver stays a pure, unit-testable function; callers that
    want the live Streamlit state pass ``st.session_state`` explicitly.
    """
    state = session_state if session_state is not None else {}
    label = state.get(nav_range_session_key(deployment_id))
    if not isinstance(label, str):
        return None
    return NAV_RANGE_SECONDS.get(label)


@st.cache_data(ttl=300, show_spinner=False)
def _fetch_windowed_nav(deployment_id: str, range_label: str) -> list[dict[str, Any]]:
    """Fetch the decimated NAV/PnL series for a preset range (VIB-5059 P2).

    Cached per ``(deployment_id, range_label)`` with a 300 s TTL — the snapshot
    cadence — so the trailing-window right edge is at most one snapshot stale and
    repeated reruns don't re-hit the gateway. Each preset ends "now", so a flat
    TTL is the correct freshness policy (a fixed past window would be immutable
    and cacheable indefinitely; the presets are not).
    """
    from datetime import UTC, datetime, timedelta

    from almanak.framework.dashboard.data_source import get_dashboard_client

    seconds = _NAV_RANGE_SECONDS[range_label]
    from_ts = 0 if seconds == 0 else int((datetime.now(UTC) - timedelta(seconds=seconds)).timestamp())
    client = get_dashboard_client()
    details = client.get_strategy_details(
        deployment_id,
        include_timeline=False,
        include_pnl_history=True,
        from_ts=from_ts,
        to_ts=0,
        max_points=_NAV_MAX_POINTS,
    )
    points: list[dict[str, Any]] = []
    for entry in details.pnl_history:
        ts = entry.get("timestamp") if isinstance(entry, dict) else getattr(entry, "timestamp", None)
        if ts is None:
            continue
        value = entry.get("value_usd") if isinstance(entry, dict) else getattr(entry, "value_usd", None)
        if not value:
            # Empty != Zero: the gateway already drops unmeasured NAV samples; this
            # is belt-and-braces — skip rather than plot a fake $0 trough. ("0" is
            # truthy, so a measured zero is preserved.)
            continue
        pnl = entry.get("pnl_usd") if isinstance(entry, dict) else getattr(entry, "pnl_usd", None)
        points.append({"timestamp": ts, "value": float(value), "pnl": float(pnl) if pnl else 0.0})
    return points


def render_nav_history_section(deployment_id: str, *, default_range: str = "7d") -> None:
    """Render the windowed NAV / PnL time-travel chart (VIB-5059 Phase 2).

    Preset range buttons (24h / 7d / 30d / All) drive a server-side, decimated
    fetch: the operator can inspect last month's behaviour, not just the most
    recent slice, and a full-lifetime view stays bounded to a constant point
    budget with drawdown spikes preserved. On gateway disconnect or an empty
    series the section degrades to a banner rather than crashing the page.
    """
    # Delegate the Plotly rendering to the omitted ``_detail_header`` shell (same
    # pattern as ``render_money_trail`` / ``render_cost_stack``) so this covered
    # section never imports a coverage-omitted ``plots/*`` figure builder directly
    # (test_coverage_omits_no_callers). The testable data shaping stays here.
    from almanak.framework.dashboard.pages._detail_header import render_nav_history_tabs

    st.divider()
    st.markdown("### NAV / PnL History")
    selected = st.radio(
        "Range",
        _NAV_RANGE_ORDER,
        index=_NAV_RANGE_ORDER.index(default_range) if default_range in _NAV_RANGE_ORDER else 1,
        horizontal=True,
        key=nav_range_session_key(deployment_id),
        label_visibility="collapsed",
    )

    try:
        points = _fetch_windowed_nav(deployment_id, selected)
    except GatewayConnectionError:
        st.info("NAV history temporarily unavailable — the gateway is disconnected.")
        return
    except Exception:
        logger.debug("Windowed NAV history failed for %s (%s)", deployment_id, selected, exc_info=True)
        st.info("NAV history could not be loaded for the selected range.")
        return

    if not points:
        st.caption("No NAV history yet for the selected range.")
        return

    render_nav_history_tabs(points)


def render_cost_stack_section(deployment_id: str, *, heading: str = "### Cost Stack") -> None:
    """Render the life-to-date Cost Stack section (VIB-3969).

    Gas / Fees / Slippage / Earn — generic across primitives (every
    primitive emits these into ``transaction_ledger`` +
    ``accounting_events``). Backed by the gateway's ``GetCostStack``
    RPC; on RPC failure the section degrades to an info banner.

    Conventionally placed at the start of an "Audit" section, just
    above the trade tape.

    Args:
        deployment_id: The deployment id.
        heading: Override the section heading. Pass an empty string to
            suppress the heading entirely (useful when composing inside
            a larger Audit panel that already has its own heading).
    """
    st.divider()
    if heading:
        st.markdown(heading)
    try:
        cost = get_cost_stack(deployment_id)
    except GatewayConnectionError:
        st.info("Cost data temporarily unavailable — the gateway is disconnected.")
        return
    if cost is None:
        st.info("No cost data yet — gas / fees / slippage accumulate as the strategy executes.")
        return
    render_cost_stack(cost)


def render_trade_tape_section(deployment_id: str, *, limit: int = 50) -> None:
    """Render the standard trade-tape section.

    Conventionally placed at the bottom of every
    ``render_custom_dashboard()`` so accounting can be visually QA'd
    locally and on the hosted platform from the same code path. The
    underlying ``render_trade_tape`` reads through the gateway's
    ``DashboardService.GetTradeTape``, which abstracts SQLite (local)
    and Postgres (hosted) — the section travels everywhere the gateway
    does.

    Args:
        deployment_id: The deployment id (passed straight through from
            ``render_custom_dashboard``'s first positional argument).
        limit: Most recent intents to fetch. Defaults to 50.
    """
    st.divider()
    st.markdown("### Trade Tape")
    render_trade_tape(deployment_id, limit=limit)


def render_position_lifecycle_section(
    deployment_id: str,
    api_client: Any,
    *,
    position_types: list[str] | None = None,
    open_only: bool = False,
    limit: int = 200,
    heading: str = "### Position Lifecycle",
) -> None:
    """Hosted-compatible sibling of ``pages.detail.render_position_lifecycle``.

    Renders the lifecycle table (OPEN / CLOSE events with PnL attribution
    for closed positions) using ONLY gateway-mediated reads — no local
    SQLite, no filesystem. The detail-page version exists for the local
    Command Center; this section is the one custom dashboards (including
    AlmanakCode-generated LP scaffolds) embed so the same table renders
    on hosted exactly as it does locally (PR 2 / Problem A2).

    Registry handles (``leg_narrow`` / ``leg_mid`` / ``leg_wide`` on
    multi-position fixtures) are joined client-side from the position
    registry via :func:`get_dashboard_service_client`. The join produces
    an "Alias" column whenever any of the underlying handles is set —
    matches the single-leg / multi-leg behaviour of the detail page.

    Args:
        deployment_id: The deployment id (passed straight through from
            ``render_custom_dashboard``'s first positional argument).
        api_client: Custom-dashboard ``DashboardAPIClient`` (already
            scoped to ``deployment_id``). The section calls
            ``api_client.get_position_events(position_types=...)`` on it.
        position_types: Optional list filter (e.g. ``["LP"]``). ``None``
            (default) returns all position types known to the gateway.
        open_only: Filter out CLOSE rows and any position that has been
            closed. Useful for the live-monitoring case.
        limit: Maximum number of events to render. Defaults to 200 to
            match the detail-page query.
        heading: Section heading override; empty suppresses the heading
            AND the leading ``st.divider()`` so the section composes
            cleanly inside a larger panel that already has its own
            chrome (the lifecycle table can land right under the
            registry table without a doubled-up divider).
    """
    from almanak.framework.dashboard.export import export_positions
    from almanak.framework.dashboard.pages.detail import _format_usd_str, get_explorer_url
    from almanak.framework.dashboard.service_client import get_dashboard_service_client

    if heading:
        st.divider()
        st.markdown(heading)

    if api_client is None:
        st.info("Position lifecycle unavailable — no api_client supplied.")
        return

    try:
        events = api_client.get_position_events(position_types=position_types)
    except Exception:  # noqa: BLE001
        # api_client.get_position_events itself already returns [] on RPC
        # failure (see DashboardAPIClient.get_position_events), so reaching
        # this branch means the client is mis-configured.
        st.info("Position lifecycle temporarily unavailable.")
        return

    if not events:
        st.info("No position events yet — events appear once OPEN/CLOSE intents land on-chain.")
        return

    if open_only:
        events = _filter_open_only_events(events)
        if not events:
            st.info("No open positions right now.")
            return

    # Sort by timestamp DESC before slicing — ``GetPositionEventsFiltered``
    # orders rows by ``(position_id, timestamp, id)`` for backfill
    # determinism, NOT newest-first. Without an explicit sort, a multi-
    # position strategy whose total events exceed ``limit`` would have
    # newer rows for later position_ids silently dropped while older
    # rows for earlier position_ids remained — the rendered "Recent X"
    # table would not be the most-recent X events. Sort is on the
    # timestamp string (ISO 8601 from the proto adapter — lexically
    # sortable). Missing-timestamp rows sort to the bottom.
    events = sorted(events, key=lambda e: e.get("timestamp") or "", reverse=True)
    events = events[:limit]

    handles, show_alias = _resolve_handles_with_visible_failure(deployment_id, get_dashboard_service_client)

    _render_lifecycle_metrics(events)

    _render_lifecycle_events_table(events, handles, show_alias, _format_usd_str, get_explorer_url)

    _render_attribution_subtable(events, handles, show_alias, _format_usd_str)

    csv_bytes = export_positions(events, fmt="csv")
    if csv_bytes:
        st.download_button(
            label="Export Position Events (CSV)",
            data=csv_bytes,
            file_name=f"position_events_{deployment_id}.csv",
            mime="text/csv",
        )


def _render_lifecycle_metrics(events: list[dict[str, Any]]) -> None:
    """Render the three-metric header (Opened / Closed / Total Events).

    Extracted to keep ``render_position_lifecycle_section`` under the
    CRAP cyclomatic-complexity cap; no behavior change.
    """
    open_count = sum(1 for e in events if e.get("event_type") == "OPEN")
    close_count = sum(1 for e in events if e.get("event_type") == "CLOSE")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Positions Opened", open_count)
    with col2:
        st.metric("Positions Closed", close_count)
    with col3:
        st.metric("Total Events", len(events))


def _lifecycle_event_row(
    evt: dict[str, Any],
    handles: dict[str, str],
    show_alias: bool,
    format_usd: Any,
    explorer_url: Any,
) -> dict[str, Any]:
    """Construct one lifecycle table row dict from a position event.

    Extracted so the per-row decisioning (Alias column inclusion, tx-link
    formatting, value-USD formatting) lives in one focused helper rather
    than inside the main render function's body.
    """
    position_id = str(evt.get("position_id", "") or "")
    tx_hash = str(evt.get("tx_hash", "") or "")
    evt_chain = str(evt.get("chain", "") or "")
    timestamp = str(evt.get("timestamp", "") or "")
    row: dict[str, Any] = {
        "Time": timestamp[:19],
        "Type": evt.get("event_type", "") or "",
        "Position": evt.get("position_type", "") or "",
        "ID": position_id[:12],
    }
    if show_alias:
        row["Alias"] = handles.get(position_id, "")
    row["Protocol"] = evt.get("protocol", "") or ""
    row["Value (USD)"] = format_usd(evt.get("value_usd"))
    row["TX"] = explorer_url(evt_chain, tx_hash) if tx_hash else ""
    return row


def _render_lifecycle_events_table(
    events: list[dict[str, Any]],
    handles: dict[str, str],
    show_alias: bool,
    format_usd: Any,
    explorer_url: Any,
) -> None:
    """Render the lifecycle events dataframe.

    Delegates per-row construction to ``_lifecycle_event_row`` so the
    table-shape decisioning is reusable (and so this render function
    stays small enough to stay under the CRAP cap).
    """
    table_data = [_lifecycle_event_row(e, handles, show_alias, format_usd, explorer_url) for e in events]
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


def _attribution_row(
    evt: dict[str, Any],
    handles: dict[str, str],
    show_alias: bool,
    format_usd: Any,
) -> dict[str, Any] | None:
    """Construct one PnL-attribution sub-table row from a CLOSE event.

    Returns ``None`` if the attribution JSON is unparseable so the caller
    can skip the row. Empty != Zero (AGENTS.md §Accounting): missing
    attribution fields render blank rather than $0.00 so partial / legacy
    payloads do not display as measured zeroes.
    """
    try:
        attr = json.loads(evt.get("attribution_json", "{}") or "{}")
    except (json.JSONDecodeError, TypeError):
        return None
    position_id = str(evt.get("position_id", "") or "")
    attr_row: dict[str, Any] = {"Position": position_id[:12]}
    if show_alias:
        attr_row["Alias"] = handles.get(position_id, "")
    attr_row["Type"] = attr.get("position_type", "") or ""
    attr_row["Net PnL"] = format_usd(attr.get("net_pnl_usd"))
    attr_row["Price PnL"] = format_usd(attr.get("price_pnl_usd"))
    attr_row["Fee PnL"] = format_usd(attr.get("fee_pnl_usd"))
    attr_row["Gas"] = format_usd(attr.get("gas_usd"))
    attr_row["Version"] = f"v{attr.get('version', '?')}"
    return attr_row


def _render_attribution_subtable(
    events: list[dict[str, Any]],
    handles: dict[str, str],
    show_alias: bool,
    format_usd: Any,
) -> None:
    """Render the PnL-attribution sub-table for closed positions with non-empty attribution."""
    closed_with_attr = [
        e for e in events if e.get("event_type") == "CLOSE" and (e.get("attribution_json") or "{}") != "{}"
    ]
    if not closed_with_attr:
        return
    rows = [r for r in (_attribution_row(e, handles, show_alias, format_usd) for e in closed_with_attr) if r]
    if not rows:
        return
    st.markdown("#### PnL Attribution (Closed Positions)")
    st.dataframe(rows, use_container_width=True, hide_index=True)


def _resolve_handles_with_visible_failure(
    deployment_id: str,
    client_factory: Any,
) -> tuple[dict[str, str], bool]:
    """Resolve registry handles and emit a visible caption on RPC failure.

    ``_fetch_registry_handles_via_gateway`` returns ``None`` on RPC failure
    and ``{}`` (or a populated map) on success. We distinguish the two so the
    operator can be told "aliases unavailable" rather than silently shown
    "no aliases" — for multi-position fixtures these two states are not
    observationally equivalent and conflating them violates the documented
    alias-enrichment contract (UAT card Trust #7 §B / D3.F4b).

    Returns ``(handles, show_alias)`` so the caller can immediately use both
    without re-checking the success sentinel.
    """
    result = _fetch_registry_handles_via_gateway(deployment_id, client_factory)
    if result is None:
        st.caption("⚠ Position aliases unavailable — registry lookup failed (events shown without leg labels).")
        return {}, False
    return result, any(result.values())


def _filter_open_only_events(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Drop CLOSE rows and any position that has any CLOSE row.

    A position with an OPEN+CLOSE pair is no longer "open" — even the
    OPEN row should not surface in open-only mode.
    """
    closed_ids = {str(e.get("position_id", "") or "") for e in events if e.get("event_type") == "CLOSE"}
    return [
        e for e in events if e.get("event_type") != "CLOSE" and str(e.get("position_id", "") or "") not in closed_ids
    ]


def _fetch_registry_handles_via_gateway(
    deployment_id: str,
    client_factory: Any,
) -> dict[str, str] | None:
    """Build the ``{position-key → handle}`` map by reading the registry RPC.

    Mirrors the SQLite-backed ``_fetch_registry_handles`` in
    ``pages.detail`` but talks to the gateway via
    ``DashboardServiceClient.get_positions``. The map keys on:

    - the NFT ``token_id`` (or ``position_id``) inside the registry
      ``primitive_payload_json``, AND
    - the registry ``physical_identity_hash``

    so both NFT-backed (Uniswap V3 LP, Pendle LP) and non-NFT
    (Aave V3 lending) primitives resolve their handle from
    ``position_events.position_id``.

    **Known hosted-vs-local parity gap**: ``DashboardService.GetPositions``
    currently filters server-side to ``status = 'open'`` only (see
    ``almanak/gateway/services/dashboard_service.py``). The local detail
    page reads the SQLite ``position_registry`` table directly and
    includes ``closed`` rows. Consequence: on hosted, a CLOSE lifecycle
    event for a position that has finished closing renders without its
    alias. Locally, the same fixture renders WITH the alias. Closing
    this gap requires a gateway RPC extension; deferred to a follow-up
    rather than rolled into this PR per the reuse-first rule in
    ``docs/internal/DashboardImprovementsMay19.md`` §A2.

    Returns:
        - ``dict[str, str]`` with the handle map (possibly empty if no
          handles are registered — legitimate "single-leg strategy" case).
        - ``None`` if the registry RPC failed. Callers MUST distinguish
          this from the empty-dict case so the operator can be told
          "aliases unavailable" rather than silently shown "no aliases" —
          the two are not observationally equivalent for multi-position
          fixtures.
    """
    try:
        client = client_factory()
        # Defensive: client_factory() should always return a singleton
        # instance per the get_dashboard_service_client() contract, but
        # alternate factories used by tests / custom embeddings may
        # legitimately return None. Treat that as an RPC failure so the
        # caller emits the visible caption rather than crashing inside
        # the AttributeError catch-all below.
        if client is None:
            logger.warning("Registry handle lookup: client_factory returned None for %s", deployment_id)
            return None
        if not client.is_connected:
            client.connect()
        result = client.get_positions(deployment_id)
    except Exception as exc:  # noqa: BLE001
        # Log at WARNING with traceback so transient gateway failures vs
        # payload-shape regressions are post-incident diagnosable. The
        # caller surfaces a visible caption to the operator regardless.
        logger.warning("Registry handle lookup failed for %s: %s", deployment_id, exc, exc_info=True)
        return None

    # Defensive: ``get_positions`` is typed to return ``GetPositionsResult``
    # with a non-None ``positions`` list, but a malformed RPC payload or a
    # future proto change could surface None here. Treat as "no handles" —
    # this is the legitimate-empty case from the caller's perspective, not
    # a failure, since we did get a response back.
    positions = getattr(result, "positions", None) or []
    out: dict[str, str] = {}
    for entry in positions:
        handle = (entry.handle or "").strip()
        if not handle:
            continue
        try:
            payload = json.loads(entry.primitive_payload_json or "{}")
        except (json.JSONDecodeError, TypeError):
            payload = {}
        token_id = payload.get("token_id") or payload.get("position_id")
        if token_id:
            out[str(token_id)] = handle
        if entry.physical_identity_hash:
            out[str(entry.physical_identity_hash)] = handle
    return out
