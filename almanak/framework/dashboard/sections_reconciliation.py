"""Read-only section helpers backed by the Phase 1 RPCs (VIB-4495 / Phase 3).

This module is the **renderer-facing** entry point for the Phase 1 surface
(``GetPositions`` / ``GetPositionRangeHistory`` / ``GetReconciliationReport``).
Every helper:

* Takes a ``DashboardServiceClient`` (read-only) — never an
  ``OperatorDashboardServiceClient``. Phase 4's CI lint (``test_no_bypass.py``)
  enforces this at import time: any module importing
  ``OperatorDashboardServiceClient`` from this file will fail CI.
* Degrades to an ``st.info`` banner on ``DashboardClientError`` rather than
  crashing the page — a gateway hiccup must not take down the whole tab.
* Renders header → body → audit-pill (cutover state, confidence, freshness)
  so operators can scan trust signals without drilling in.
* Treats payload JSON as opaque; the caller decides how to parse the
  ``primitive_payload_json`` field per primitive.

Sibling module ``sections_operator.py`` carries the corresponding
mutation panels (Preview/Apply/Refresh). They live in a separate file
specifically so the read-only / operator split is grep-able.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING

import streamlit as st

from almanak.framework.dashboard.service_client import (
    CutoverState,
    DashboardClientError,
    DashboardServiceClient,
    PositionConfidence,
    PositionEntry,
    PositionSource,
    PositionStatus,
    ReconciliationSeverity,
)

if TYPE_CHECKING:  # pragma: no cover
    from almanak.framework.dashboard.service_client import (
        GetPositionsResult,
    )


# =============================================================================
# Display-text helpers — kept private so tests pin them via the public
# section renderers, not the formatting layer in isolation.
# =============================================================================


_CUTOVER_LABELS: dict[CutoverState, str] = {
    CutoverState.UNSPECIFIED: "[cutover: unknown]",
    CutoverState.PRE_BACKFILL: "[cutover: pre-backfill]",
    CutoverState.BACKFILL_IN_PROGRESS: "[cutover: backfill in progress]",
    CutoverState.BACKFILL_COMPLETE: "[cutover: backfill complete]",
    CutoverState.REGISTRY_AUTHORITATIVE: "[cutover: registry authoritative]",
}


_CONFIDENCE_LABELS: dict[PositionConfidence, str] = {
    PositionConfidence.UNSPECIFIED: "unknown",
    PositionConfidence.HIGH: "high",
    PositionConfidence.MEDIUM: "medium",
    PositionConfidence.LOW: "low",
    PositionConfidence.DIVERGED: "DIVERGED",
}


_SOURCE_LABELS: dict[PositionSource, str] = {
    PositionSource.UNSPECIFIED: "unknown",
    PositionSource.REGISTRY: "registry",
    PositionSource.SNAPSHOT: "snapshot",
    PositionSource.LEGACY: "legacy",
}


_STATUS_LABELS: dict[PositionStatus, str] = {
    PositionStatus.UNSPECIFIED: "—",
    PositionStatus.OPEN: "open",
    PositionStatus.CLOSED: "closed",
    PositionStatus.REORG_INVALIDATED: "reorg-invalidated",
}


def _cutover_pill(state: CutoverState) -> str:
    return _CUTOVER_LABELS.get(state, _CUTOVER_LABELS[CutoverState.UNSPECIFIED])


def _confidence_label(conf: PositionConfidence) -> str:
    return _CONFIDENCE_LABELS.get(conf, _CONFIDENCE_LABELS[PositionConfidence.UNSPECIFIED])


def _source_label(source: PositionSource) -> str:
    return _SOURCE_LABELS.get(source, _SOURCE_LABELS[PositionSource.UNSPECIFIED])


def _status_label(status: PositionStatus) -> str:
    return _STATUS_LABELS.get(status, _STATUS_LABELS[PositionStatus.UNSPECIFIED])


def _format_value_usd(value: Decimal | None) -> str:
    """Render USD amounts in a way that's stable across small / large values.

    Empty≠Zero: ``None`` is an *unmeasured* value (the gateway emitted an empty
    ``value_usd`` for this row — typically a transient reprice miss surfaced
    honestly with confidence=LOW) and renders as "—", NOT "$0.00". Rendering a
    fabricated "$0.00" here made the Positions table contradict the position's
    real value on every other surface (VIB-5738 cluster). A measured
    ``Decimal("0")`` is preserved as "$0.00".
    """
    if value is None:
        return "—"
    if value == 0:
        return "$0.00"
    return f"${value:,.2f}"


def _format_freshness(iso_or_unix: str | int | None) -> str:
    """Best-effort "as of HH:MM:SS UTC" rendering for the freshness pill."""
    if not iso_or_unix:
        return ""
    if isinstance(iso_or_unix, int):
        try:
            ts = datetime.fromtimestamp(iso_or_unix, tz=UTC)
        except (OSError, ValueError, OverflowError):
            return ""
    else:
        try:
            ts = datetime.fromisoformat(str(iso_or_unix).replace("Z", "+00:00"))
        except ValueError:
            return ""
    return f"as of {ts.strftime('%Y-%m-%d %H:%M:%S')} UTC"


def _position_row(p: PositionEntry) -> dict[str, str]:
    """Map a PositionEntry to a table row keyed by visible column names.

    Returning a plain dict (vs. a dataframe directly) keeps the row shape
    inspectable for tests and lets the caller compose multiple rows into
    a single ``st.dataframe`` call.
    """
    return {
        "Handle": p.handle or p.physical_identity_hash[:12] or "—",
        "Status": _status_label(p.status),
        "Chain": p.chain,
        "Primitive": p.primitive,
        "Value (USD)": _format_value_usd(p.value_usd),
        "Source": _source_label(p.source),
        "Confidence": _confidence_label(p.confidence),
        "Reconciled @ block": str(p.last_reconciled_at_block or "—"),
    }


# =============================================================================
# Public section helpers
# =============================================================================


def render_positions_section(
    deployment_id: str,
    client: DashboardServiceClient,
    *,
    heading: str = "### Positions",
    chain: str = "",
    primitive: str = "",
) -> None:
    """Render the authoritative-lane positions table, grouped by accounting category.

    Each accounting-category group renders under a sub-header that carries
    the cutover-state pill, so operators can tell at a glance whether the
    rows below are registry-backed (post-cutover) or snapshot-derived
    (mid-migration).

    Args:
        deployment_id: Deployment identifier (e.g. ``"aave-avax"``).
        client: Read-only dashboard client. Operator client also accepts;
            see ``sections_operator.py`` for mutation surfaces.
        heading: Section heading. Empty string suppresses the heading
            (useful when composing inside a larger panel).
        chain: Optional gateway-side chain filter.
        primitive: Optional gateway-side primitive filter.
    """
    st.divider()
    if heading:
        st.markdown(heading)

    try:
        result = client.get_positions(
            deployment_id,
            chain=chain,
            primitive=primitive,
        )
    except DashboardClientError as exc:
        # VIB-4047: fail LOUD + CLEAN. An UNAUTHENTICATED GetPositions against a
        # managed mainnet gateway must never render as a quiet blue "temporarily
        # unavailable" that reads as "no activity" while real exposure is live —
        # nor leak the raw _InactiveRpcError repr. render_gateway_error picks the
        # red auth/unreachable banner and hides the raw text behind debug.
        from almanak.framework.dashboard.error_ui import render_gateway_error

        render_gateway_error(exc, context="Positions", raw=str(exc))
        return

    if not result.positions:
        st.info("No positions yet — the registry will populate as the strategy executes.")
        return

    _render_position_groups(result)


def _render_position_groups(result: GetPositionsResult) -> None:
    """Body renderer for ``render_positions_section`` — split for testability."""
    grouped = result.by_accounting_category()
    for category in sorted(grouped.keys()):
        rows = grouped[category]
        cutover = result.cutover_for(category)
        pill = _cutover_pill(cutover.state) if cutover else ""
        freshness = _format_freshness(cutover.last_reconciled_unix_seconds) if cutover else ""

        st.markdown(f"**{category}** {pill} {freshness}".strip())
        st.dataframe(
            [_position_row(p) for p in rows],
            hide_index=True,
            use_container_width=True,
        )


def render_position_range_history_section(
    deployment_id: str,
    client: DashboardServiceClient,
    *,
    chain: str,
    accounting_category: str,
    handle: str = "",
    physical_identity_hash: str = "",
    heading: str = "### Position History",
    from_time: datetime | None = None,
    to_time: datetime | None = None,
) -> None:
    """Render the event timeline for a single position.

    Routes by primitive on the server side: LP/PERP positions come
    from ``position_events``, lending positions come from
    ``accounting_events``. When the primitive has no notion of history
    (swap/prediction-market), the gateway surfaces a stub message that
    we render in place of an empty table.

    Args:
        deployment_id: Deployment identifier.
        client: Read-only dashboard client.
        chain: Chain identifier (required — history is per-chain).
        accounting_category: e.g. ``"LP_UNIV3"`` or ``"AAVE_COLLATERAL"``.
        handle: Renderer-friendly handle. Either this OR ``physical_identity_hash``
            is required; the hash wins when both are supplied (stable key).
        physical_identity_hash: Stable primary key.
        heading: Section heading override.
        from_time: Optional window start.
        to_time: Optional window end.
    """
    st.divider()
    if heading:
        st.markdown(heading)

    if not handle and not physical_identity_hash:
        st.info("Position history requires either a handle or a physical identity hash.")
        return

    try:
        result = client.get_position_range_history(
            deployment_id,
            chain=chain,
            accounting_category=accounting_category,
            handle=handle,
            physical_identity_hash=physical_identity_hash,
            from_time=from_time,
            to_time=to_time,
        )
    except DashboardClientError as exc:
        st.info(f"Position history temporarily unavailable: {exc}")
        return

    if result.stub_message:
        st.info(result.stub_message)
        return

    if not result.entries:
        st.caption("No history events recorded for this position yet.")
        return

    rows = []
    for entry in result.entries:
        rows.append(
            {
                "Timestamp (UTC)": (entry.timestamp.strftime("%Y-%m-%d %H:%M:%S") if entry.timestamp else "—"),
                "Block": entry.block_number or "—",
                "Event": entry.event_type or "—",
                "Source": entry.source_table,
                "Ledger ID": entry.ledger_entry_id or "—",
                "Tx": entry.tx_hash or "—",
            }
        )
    st.dataframe(rows, hide_index=True, use_container_width=True)


def render_reconciliation_report_section(
    deployment_id: str,
    client: DashboardServiceClient,
    *,
    heading: str = "### Reconciliation Report",
) -> None:
    """Render the read-only three-way-diff reconciliation report.

    Server-side 5-second TTL cache means re-rendering this section on
    every page tick is cheap — there's no need for renderer-side
    de-bouncing.

    Args:
        deployment_id: Deployment identifier.
        client: Read-only dashboard client.
        heading: Section heading override.
    """
    st.divider()
    if heading:
        st.markdown(heading)

    # VIB-5942: this report is a position-STRUCTURE three-way diff (does the
    # same physical position appear consistently across ledger, snapshots, and
    # registry). It deliberately does NOT check the PnL value identity — that is
    # the header's "PnL reconciliation (G6)" tile (wallet PnL ≡ Σ component PnL).
    # Both can hold at once: positions can agree structurally (0 findings here)
    # while the PnL identity has a gap (G6 FAIL). The explicit scope label stops
    # the two surfaces from reading as a self-contradiction.
    st.caption(
        "Scope: position **structure** across ledger / snapshots / registry — "
        "not PnL values. PnL value identity lives in the header's "
        "*PnL reconciliation (G6)* tile."
    )

    try:
        report = client.get_reconciliation_report(deployment_id)
    except DashboardClientError as exc:
        st.info(f"Reconciliation report temporarily unavailable: {exc}")
        return

    freshness = _format_freshness(report.as_of)
    if freshness:
        st.caption(freshness)

    col_diverged, col_warn, col_total = st.columns(3)
    col_diverged.metric("Diverged", report.diverged_count)
    col_warn.metric("Warn", report.warn_count)
    col_total.metric("Total findings", len(report.findings))

    if report.findings:
        st.dataframe(
            [_finding_row(f) for f in report.findings],
            hide_index=True,
            use_container_width=True,
        )
    elif report.primitive_stubs:
        # VIB-5942 audit: "no findings" does NOT mean "everything reconciles" when
        # primitives below have no v1 parser — those positions were NOT checked, so
        # a green success banner would overstate coverage. Qualify to the COVERED
        # set and point at the uncovered stubs listed right below.
        st.info(
            "No structural findings among the **covered** positions (ledger ↔ snapshots ↔ "
            "registry agree). Coverage is partial — the primitives listed below have no v1 "
            "parser and were not reconciled. (Position structure only; PnL value identity is "
            "the header's G6 tile.)"
        )
    else:
        st.success(
            "No structural findings — every position appears consistently across "
            "ledger, snapshots, and registry. (Position structure only; PnL value "
            "identity is the header's G6 tile.)"
        )

    if report.primitive_stubs:
        st.markdown("**Coverage stubs** — primitives without a v1 parser (NOT reconciled):")
        for stub in report.primitive_stubs:
            st.caption(f"• {stub.primitive}: {stub.message} ({stub.ticket})")

    if report.reconciliation_id:
        st.caption(f"reconciliation_id = `{report.reconciliation_id}` · block = {report.source_block_number}")


def _finding_row(f) -> dict[str, str]:
    severity = f.severity
    severity_text = severity.value if severity != ReconciliationSeverity.UNSPECIFIED else "—"
    coverage_bits = []
    if f.ledger_has_row:
        coverage_bits.append("L")
    if f.snapshot_has_row:
        coverage_bits.append("S")
    if f.registry_has_row:
        coverage_bits.append("R")
    coverage = "+".join(coverage_bits) if coverage_bits else "—"
    return {
        "Severity": severity_text,
        "Category": f.accounting_category,
        "Position": f.physical_identity_hash[:12] if f.physical_identity_hash else "—",
        "Delta": f.delta or "—",
        "Coverage (L+S+R)": coverage,
        "Suggested action": f.suggested_action or "—",
    }


__all__ = [
    "render_position_range_history_section",
    "render_positions_section",
    "render_reconciliation_report_section",
]
