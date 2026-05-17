"""Operator-only section helpers — mutation surfaces (VIB-4495 / Phase 3).

Every helper in this module type-hints ``OperatorDashboardServiceClient``.
Phase 4's CI lint (``test_no_bypass.py``) scans for forbidden imports
and will FAIL CI if a renderer module imports this file. Operator pages
(the toolbar tab that the operator clicks to trigger reconcile / refresh)
are the only legitimate callers.

The reconciliation triad UI orchestrates the preview→apply contract:

  1. Operator clicks "Preview reconcile" → server computes diff, returns
     ``preview_token`` valid for ~5 min.
  2. UI displays diff buckets (matched / phantom_missing / stranded).
  3. Operator clicks "Apply reconcile" → server consumes token and either
     applies the rebuilt rows (SUCCESS / PARTIAL_SUCCESS) or rejects
     because state drifted (STATE_DRIFT) or the token expired (EXPIRED).
  4. UI feeds the outcome back; on STATE_DRIFT / EXPIRED the operator
     re-clicks Preview.

The preview token + UI selections live in ``st.session_state`` keyed by
``strategy_id`` so two operator tabs (different strategies) don't trample
each other's previews.
"""

from __future__ import annotations

import streamlit as st

from almanak.framework.dashboard.service_client import (
    DashboardClientError,
    OperatorDashboardServiceClient,
)

# Session-state keys — namespaced by strategy_id so different tabs don't collide.
_PREVIEW_TOKEN_KEY = "phase1_reconcile_preview_token::{strategy_id}"
_PREVIEW_RESULT_KEY = "phase1_reconcile_preview_result::{strategy_id}"
_APPLY_OUTCOME_KEY = "phase1_reconcile_apply_outcome::{strategy_id}"
_REFRESH_OUTCOME_KEY = "phase1_reconcile_refresh_outcome::{strategy_id}"


def _key(template: str, strategy_id: str) -> str:
    return template.format(strategy_id=strategy_id)


def render_reconciliation_operator_panel(
    strategy_id: str,
    client: OperatorDashboardServiceClient,
    *,
    heading: str = "### Reconciliation Operator Panel",
) -> None:
    """Render Preview / Apply / Refresh buttons + diff display.

    Self-contained: the panel reads and writes its own ``st.session_state``
    keys. Drop this anywhere inside an operator-only Streamlit page.

    Args:
        strategy_id: Strategy identifier.
        client: Operator dashboard client. Read-only clients cannot
            satisfy the type — that's the point.
        heading: Section heading override.
    """
    st.divider()
    if heading:
        st.markdown(heading)
    st.caption(
        "Preview the three-way-diff (ledger × snapshots × registry), then apply or refresh "
        "from chain. Preview tokens expire after ~5 min and invalidate on any state change."
    )

    col_preview, col_apply, col_refresh = st.columns(3)
    with col_preview:
        preview_clicked = st.button("Preview reconcile", key=f"phase1_preview_btn_{strategy_id}")
    with col_apply:
        # Apply button is enabled iff a preview token exists.
        has_token = bool(st.session_state.get(_key(_PREVIEW_TOKEN_KEY, strategy_id)))
        apply_clicked = st.button(
            "Apply reconcile",
            key=f"phase1_apply_btn_{strategy_id}",
            disabled=not has_token,
            help="Run a preview first to obtain a token." if not has_token else None,
        )
    with col_refresh:
        refresh_clicked = st.button(
            "Refresh registry from chain",
            key=f"phase1_refresh_btn_{strategy_id}",
            help="Force a fresh on-chain read pass for this strategy.",
        )

    if preview_clicked:
        _do_preview(strategy_id, client)
    if apply_clicked:
        _do_apply(strategy_id, client)
    if refresh_clicked:
        _do_refresh(strategy_id, client)

    _render_preview_result(strategy_id)
    _render_apply_outcome(strategy_id)
    _render_refresh_outcome(strategy_id)


# =============================================================================
# Action handlers — keep side effects out of the button rendering loop
# =============================================================================


def _do_preview(strategy_id: str, client: OperatorDashboardServiceClient) -> None:
    try:
        result = client.preview_reconcile(strategy_id)
    except DashboardClientError as exc:
        st.error(f"PreviewReconcile failed: {exc}")
        return
    st.session_state[_key(_PREVIEW_TOKEN_KEY, strategy_id)] = result.preview_token
    st.session_state[_key(_PREVIEW_RESULT_KEY, strategy_id)] = result
    # Clear any stale apply outcome from a previous preview.
    st.session_state.pop(_key(_APPLY_OUTCOME_KEY, strategy_id), None)


def _do_apply(strategy_id: str, client: OperatorDashboardServiceClient) -> None:
    token = st.session_state.get(_key(_PREVIEW_TOKEN_KEY, strategy_id))
    if not token:
        st.error("No preview token in session — click Preview first.")
        return
    try:
        outcome = client.apply_reconcile(strategy_id, token)
    except DashboardClientError as exc:
        st.error(f"ApplyReconcile failed: {exc}")
        return
    st.session_state[_key(_APPLY_OUTCOME_KEY, strategy_id)] = outcome
    # Always invalidate the token after the RPC returns — every terminal
    # outcome (SUCCESS, PARTIAL_SUCCESS, STATE_DRIFT, EXPIRED, NOT_FOUND)
    # marks the token as consumed server-side. Keeping it around would
    # let an operator re-click "Apply" and get a NOT_FOUND on the second
    # attempt, which is just confusing UX.
    st.session_state.pop(_key(_PREVIEW_TOKEN_KEY, strategy_id), None)


def _do_refresh(strategy_id: str, client: OperatorDashboardServiceClient) -> None:
    try:
        outcome = client.refresh_registry_from_chain(strategy_id)
    except DashboardClientError as exc:
        st.error(f"RefreshRegistryFromChain failed: {exc}")
        return
    st.session_state[_key(_REFRESH_OUTCOME_KEY, strategy_id)] = outcome


# =============================================================================
# Result renderers — purely read st.session_state
# =============================================================================


def _render_preview_result(strategy_id: str) -> None:
    result = st.session_state.get(_key(_PREVIEW_RESULT_KEY, strategy_id))
    if result is None:
        return
    st.markdown("#### Preview diff")
    if result.expires_at:
        st.caption(f"Token expires {result.expires_at.strftime('%H:%M:%S UTC')}")
    col_match, col_phantom, col_stranded = st.columns(3)
    col_match.metric("Matched", len(result.matched))
    col_phantom.metric("Phantom missing", len(result.phantom_missing))
    col_stranded.metric("Stranded", len(result.stranded))

    if result.phantom_missing:
        st.markdown("**Phantom-missing (will be inserted on Apply):**")
        st.dataframe(
            [
                {
                    "Category": p.accounting_category,
                    "Position": p.physical_identity_hash[:12],
                    "Primitive": p.primitive,
                    "Opened @ block": p.opened_at_block or "—",
                }
                for p in result.phantom_missing
            ],
            hide_index=True,
            use_container_width=True,
        )

    if result.stranded:
        st.markdown("**Stranded (registry has, chain doesn't — operator inspect):**")
        st.dataframe(
            [
                {
                    "Category": s.accounting_category,
                    "Position": s.physical_identity_hash[:12],
                    "Handle": s.handle or "—",
                    "Confirmed absent @ block": s.confirmed_absent_at_block or "—",
                    "Reason": s.absent_reason or "—",
                }
                for s in result.stranded
            ],
            hide_index=True,
            use_container_width=True,
        )

    if result.primitive_stubs:
        st.caption("Coverage stubs: " + ", ".join(f"{s.primitive} ({s.ticket})" for s in result.primitive_stubs))


def _render_apply_outcome(strategy_id: str) -> None:
    outcome = st.session_state.get(_key(_APPLY_OUTCOME_KEY, strategy_id))
    if outcome is None:
        return
    st.markdown("#### Apply outcome")
    if outcome.is_success:
        st.success(f"SUCCESS — {len(outcome.rebuilt)} row(s) applied.")
    elif outcome.result == "PARTIAL_SUCCESS":
        st.warning(f"PARTIAL_SUCCESS — {len(outcome.rebuilt)} applied, {len(outcome.primitive_errors)} failed.")
    elif outcome.needs_retry:
        st.error(f"{outcome.result} — click Preview to retry. {outcome.detail}")
    else:
        st.error(f"{outcome.result} — {outcome.detail}")

    if outcome.primitive_errors:
        st.dataframe(
            [
                {
                    "Primitive": e.primitive,
                    "Chain": e.chain,
                    "Code": e.code,
                    "Recoverable": "yes" if e.recoverable else "no",
                    "Message": e.message,
                }
                for e in outcome.primitive_errors
            ],
            hide_index=True,
            use_container_width=True,
        )


def _render_refresh_outcome(strategy_id: str) -> None:
    outcome = st.session_state.get(_key(_REFRESH_OUTCOME_KEY, strategy_id))
    if outcome is None:
        return
    st.markdown("#### Refresh-from-chain outcome")
    if outcome.is_success:
        st.success(
            f"SUCCESS — refreshed {outcome.positions_refreshed} position(s), "
            f"emitted {outcome.events_emitted} divergent event(s) "
            f"at block {outcome.source_block_number}."
        )
    elif outcome.result == "RATE_LIMITED":
        st.info(f"RATE_LIMITED — another refresh is already in flight. {outcome.detail}")
    else:
        st.error(f"{outcome.result} — {outcome.detail}")


__all__ = [
    "render_reconciliation_operator_panel",
]
