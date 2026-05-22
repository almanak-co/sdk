"""Tests for the operator-only reconciliation panel (VIB-4495 / Phase 3).

These tests cover the Preview → Apply → Refresh flow:

* Button click dispatches the right ``OperatorDashboardServiceClient`` call.
* Preview stores token + result in ``st.session_state``.
* Apply consumes the token and clears it on success / drift / expiry
  so the next Apply requires a fresh Preview.
* Apply outcomes that ``need_retry`` clear the token; PARTIAL_SUCCESS
  clears the token too (consumed).
* Each handler degrades to ``st.error`` rather than crashing on a
  ``DashboardClientError``.
* ``st.session_state`` keys are namespaced by ``deployment_id`` so two
  open tabs do not collide.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.dashboard import sections_operator as panel
from almanak.framework.dashboard.service_client import (
    ApplyReconcileResult,
    DashboardClientError,
    MatchedPosition,
    PhantomMissingPosition,
    PreviewReconcileResult,
    PrimitiveError,
    RebuiltRow,
    RefreshRegistryResult,
    StrandedRow,
)

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture(autouse=True)
def _clear_session_state():
    """Some Streamlit AppTest harnesses don't clear state between tests."""
    import streamlit as st

    try:
        st.session_state.clear()
    except Exception:  # pragma: no cover
        pass
    yield
    try:
        st.session_state.clear()
    except Exception:  # pragma: no cover
        pass


@pytest.fixture
def fake_operator() -> MagicMock:
    return MagicMock()


def _make_preview(
    *,
    token: str = "tok-1",
    phantom_count: int = 0,
    stranded_count: int = 0,
    matched_count: int = 0,
) -> PreviewReconcileResult:
    return PreviewReconcileResult(
        preview_token=token,
        matched=[
            MatchedPosition(
                physical_identity_hash=f"m{i}",
                primitive="lp",
                accounting_category="LP_UNIV3",
                confirmed_at_block=2000,
            )
            for i in range(matched_count)
        ],
        phantom_missing=[
            PhantomMissingPosition(
                physical_identity_hash=f"p{i}",
                primitive="lp",
                accounting_category="LP_UNIV3",
                semantic_grouping_key="",
                payload_json="",
                opened_at_block=1000,
                opened_tx="",
            )
            for i in range(phantom_count)
        ],
        stranded=[
            StrandedRow(
                physical_identity_hash=f"s{i}",
                primitive="lp",
                accounting_category="LP_UNIV3",
                handle="",
                registry_row_json="",
                confirmed_absent_at_block=3000,
                absent_reason="",
            )
            for i in range(stranded_count)
        ],
        primitive_stubs=[],
        reconciliation_id="recon-1",
        source_block_number=100,
        expires_at_unix_seconds=1747445000,
    )


# =============================================================================
# Public export
# =============================================================================


def test_panel_lazy_export_resolves() -> None:
    import almanak.framework.dashboard as dash_pkg

    assert callable(dash_pkg.render_reconciliation_operator_panel)


# =============================================================================
# Public panel — end-to-end dispatch (covers the public render function
# itself so the project's CRAP gate sees coverage on the dispatch layer,
# not just the extracted handlers).
# =============================================================================


class TestRenderReconciliationOperatorPanel:
    """Public panel render coverage.

    The handlers (`_do_preview` / `_do_apply` / `_do_refresh`) and the result
    renderers each have their own focused tests. These tests pin the dispatch
    layer: heading drawn, three columns laid out, button clicks routed to the
    right handler, and existing session state read for the disabled-state
    logic on the Apply button.
    """

    def _columns(self) -> tuple[MagicMock, MagicMock, MagicMock]:
        return MagicMock(), MagicMock(), MagicMock()

    def test_draws_heading_and_three_columns(self, fake_operator: MagicMock) -> None:
        cols = self._columns()
        with (
            patch.object(panel.st, "divider") as mock_divider,
            patch.object(panel.st, "markdown") as mock_md,
            patch.object(panel.st, "caption"),
            patch.object(panel.st, "columns", return_value=cols) as mock_columns,
            patch.object(panel.st, "button", return_value=False),
        ):
            panel.render_reconciliation_operator_panel("aave-avax", fake_operator)
        mock_divider.assert_called_once_with()
        assert any("Reconciliation Operator Panel" in c.args[0] for c in mock_md.call_args_list)
        mock_columns.assert_called_once_with(3)

    def test_preview_button_dispatches_to_do_preview(self, fake_operator: MagicMock) -> None:
        """First button = Preview. Only Preview is True; the dispatch routes."""
        fake_operator.preview_reconcile.return_value = _make_preview(token="tok-1")
        with (
            patch.object(panel.st, "divider"),
            patch.object(panel.st, "markdown"),
            patch.object(panel.st, "caption"),
            patch.object(panel.st, "columns", return_value=self._columns()),
            patch.object(panel.st, "button", side_effect=[True, False, False]),
        ):
            panel.render_reconciliation_operator_panel("aave-avax", fake_operator)
        fake_operator.preview_reconcile.assert_called_once_with("aave-avax")
        fake_operator.apply_reconcile.assert_not_called()
        fake_operator.refresh_registry_from_chain.assert_not_called()

    def test_apply_button_disabled_when_no_token(self, fake_operator: MagicMock) -> None:
        """Without a preview token in session, the Apply button must be disabled."""
        captured_kwargs: list[dict] = []

        def _record(*args, **kwargs):
            captured_kwargs.append(kwargs)
            return False

        with (
            patch.object(panel.st, "divider"),
            patch.object(panel.st, "markdown"),
            patch.object(panel.st, "caption"),
            patch.object(panel.st, "columns", return_value=self._columns()),
            patch.object(panel.st, "button", side_effect=_record),
        ):
            panel.render_reconciliation_operator_panel("aave-avax", fake_operator)
        # Apply is the second button (after Preview). Its kwargs must carry disabled=True.
        assert captured_kwargs[1].get("disabled") is True

    def test_apply_button_enabled_when_token_in_session(self, fake_operator: MagicMock) -> None:
        import streamlit as st

        st.session_state[panel._key(panel._PREVIEW_TOKEN_KEY, "aave-avax")] = "tok-1"
        captured_kwargs: list[dict] = []

        def _record(*args, **kwargs):
            captured_kwargs.append(kwargs)
            return False

        with (
            patch.object(panel.st, "divider"),
            patch.object(panel.st, "markdown"),
            patch.object(panel.st, "caption"),
            patch.object(panel.st, "columns", return_value=self._columns()),
            patch.object(panel.st, "button", side_effect=_record),
        ):
            panel.render_reconciliation_operator_panel("aave-avax", fake_operator)
        assert captured_kwargs[1].get("disabled") is False

    def test_refresh_button_dispatches_to_refresh_registry(self, fake_operator: MagicMock) -> None:
        """Third button = Refresh."""
        fake_operator.refresh_registry_from_chain.return_value = RefreshRegistryResult(
            result="SUCCESS",
            detail="",
            positions_refreshed=1,
            events_emitted=0,
            source_block_number=100,
            reconciliation_id="r",
        )
        with (
            patch.object(panel.st, "divider"),
            patch.object(panel.st, "markdown"),
            patch.object(panel.st, "caption"),
            patch.object(panel.st, "columns", return_value=self._columns()),
            patch.object(panel.st, "button", side_effect=[False, False, True]),
            patch.object(panel.st, "success"),
        ):
            panel.render_reconciliation_operator_panel("aave-avax", fake_operator)
        fake_operator.refresh_registry_from_chain.assert_called_once_with("aave-avax")
        fake_operator.preview_reconcile.assert_not_called()
        fake_operator.apply_reconcile.assert_not_called()


# =============================================================================
# Preview handler
# =============================================================================


class TestDoPreview:
    def test_stores_token_and_result_in_session(self, fake_operator: MagicMock) -> None:
        preview = _make_preview(token="tok-abc", phantom_count=1)
        fake_operator.preview_reconcile.return_value = preview

        import streamlit as st

        panel._do_preview("aave-avax", fake_operator)

        assert st.session_state[panel._key(panel._PREVIEW_TOKEN_KEY, "aave-avax")] == "tok-abc"
        assert st.session_state[panel._key(panel._PREVIEW_RESULT_KEY, "aave-avax")] is preview
        fake_operator.preview_reconcile.assert_called_once_with("aave-avax")

    def test_clears_stale_apply_outcome(self, fake_operator: MagicMock) -> None:
        """A fresh preview must invalidate the previous Apply outcome
        so the operator doesn't think the new preview already applied."""
        import streamlit as st

        st.session_state[panel._key(panel._APPLY_OUTCOME_KEY, "sid")] = "old-outcome"
        fake_operator.preview_reconcile.return_value = _make_preview()
        panel._do_preview("sid", fake_operator)
        assert panel._key(panel._APPLY_OUTCOME_KEY, "sid") not in st.session_state

    def test_namespaced_by_deployment_id(self, fake_operator: MagicMock) -> None:
        """Two strategies' previews coexist without overwriting each other."""
        import streamlit as st

        fake_operator.preview_reconcile.side_effect = [
            _make_preview(token="tok-a"),
            _make_preview(token="tok-b"),
        ]
        panel._do_preview("strategy-a", fake_operator)
        panel._do_preview("strategy-b", fake_operator)
        assert (
            st.session_state[panel._key(panel._PREVIEW_TOKEN_KEY, "strategy-a")] == "tok-a"
        )
        assert (
            st.session_state[panel._key(panel._PREVIEW_TOKEN_KEY, "strategy-b")] == "tok-b"
        )

    def test_client_error_renders_error_no_session_update(self, fake_operator: MagicMock) -> None:
        import streamlit as st

        fake_operator.preview_reconcile.side_effect = DashboardClientError("dead")
        with patch.object(panel.st, "error") as mock_err:
            panel._do_preview("sid", fake_operator)
        mock_err.assert_called_once()
        assert panel._key(panel._PREVIEW_TOKEN_KEY, "sid") not in st.session_state


# =============================================================================
# Apply handler
# =============================================================================


class TestDoApply:
    def test_no_token_errors_and_short_circuits(self, fake_operator: MagicMock) -> None:
        with patch.object(panel.st, "error") as mock_err:
            panel._do_apply("sid", fake_operator)
        mock_err.assert_called_once()
        fake_operator.apply_reconcile.assert_not_called()

    def test_success_consumes_token(self, fake_operator: MagicMock) -> None:
        import streamlit as st

        st.session_state[panel._key(panel._PREVIEW_TOKEN_KEY, "sid")] = "tok-1"
        fake_operator.apply_reconcile.return_value = ApplyReconcileResult(
            result="SUCCESS",
            detail="",
            rebuilt=[
                RebuiltRow(
                    physical_identity_hash="h1",
                    primitive="lp",
                    accounting_category="LP_UNIV3",
                    source="reconciliation_discovery",
                    last_reconciled_at_block=4000,
                    reconciliation_id="recon-3",
                    registry_row_json="",
                )
            ],
            reconciliation_id="recon-3",
        )
        panel._do_apply("sid", fake_operator)
        fake_operator.apply_reconcile.assert_called_once_with("sid", "tok-1")
        # Token cleared after successful consumption.
        assert panel._key(panel._PREVIEW_TOKEN_KEY, "sid") not in st.session_state
        # Outcome stored.
        assert (
            st.session_state[panel._key(panel._APPLY_OUTCOME_KEY, "sid")].result == "SUCCESS"
        )

    def test_state_drift_clears_token(self, fake_operator: MagicMock) -> None:
        import streamlit as st

        st.session_state[panel._key(panel._PREVIEW_TOKEN_KEY, "sid")] = "tok-1"
        fake_operator.apply_reconcile.return_value = ApplyReconcileResult(
            result="STATE_DRIFT", detail="state moved"
        )
        panel._do_apply("sid", fake_operator)
        # Token cleared — next Apply must re-preview.
        assert panel._key(panel._PREVIEW_TOKEN_KEY, "sid") not in st.session_state

    def test_partial_success_does_not_keep_token(self, fake_operator: MagicMock) -> None:
        """PARTIAL_SUCCESS means token was consumed; re-Apply with same
        token would be a no-op or error. Token must be cleared."""
        import streamlit as st

        st.session_state[panel._key(panel._PREVIEW_TOKEN_KEY, "sid")] = "tok-1"
        fake_operator.apply_reconcile.return_value = ApplyReconcileResult(
            result="PARTIAL_SUCCESS",
            detail="1 of 2 applied",
            rebuilt=[
                RebuiltRow(
                    physical_identity_hash="h1",
                    primitive="lp",
                    accounting_category="LP_UNIV3",
                    source="reconciliation_discovery",
                    last_reconciled_at_block=4000,
                    reconciliation_id="recon-4",
                    registry_row_json="",
                )
            ],
            primitive_errors=[
                PrimitiveError(
                    primitive="lp",
                    chain="avalanche",
                    code="RPC_FANOUT_FAILED",
                    message="timeout",
                    recoverable=True,
                )
            ],
            reconciliation_id="recon-4",
        )
        panel._do_apply("sid", fake_operator)
        assert panel._key(panel._PREVIEW_TOKEN_KEY, "sid") not in st.session_state

    def test_client_error_renders_error_no_outcome_set(self, fake_operator: MagicMock) -> None:
        import streamlit as st

        st.session_state[panel._key(panel._PREVIEW_TOKEN_KEY, "sid")] = "tok-1"
        fake_operator.apply_reconcile.side_effect = DashboardClientError("rpc dead")
        with patch.object(panel.st, "error") as mock_err:
            panel._do_apply("sid", fake_operator)
        mock_err.assert_called_once()
        assert panel._key(panel._APPLY_OUTCOME_KEY, "sid") not in st.session_state


# =============================================================================
# Refresh handler
# =============================================================================


class TestDoRefresh:
    def test_success_stores_outcome(self, fake_operator: MagicMock) -> None:
        import streamlit as st

        fake_operator.refresh_registry_from_chain.return_value = RefreshRegistryResult(
            result="SUCCESS",
            detail="",
            positions_refreshed=5,
            events_emitted=1,
            source_block_number=100,
            reconciliation_id="recon-5",
        )
        panel._do_refresh("sid", fake_operator)
        outcome = st.session_state[panel._key(panel._REFRESH_OUTCOME_KEY, "sid")]
        assert outcome.is_success is True

    def test_rate_limited_stores_outcome(self, fake_operator: MagicMock) -> None:
        import streamlit as st

        fake_operator.refresh_registry_from_chain.return_value = RefreshRegistryResult(
            result="RATE_LIMITED",
            detail="another in flight",
            positions_refreshed=0,
            events_emitted=0,
            source_block_number=0,
            reconciliation_id="",
        )
        panel._do_refresh("sid", fake_operator)
        outcome = st.session_state[panel._key(panel._REFRESH_OUTCOME_KEY, "sid")]
        assert outcome.is_success is False
        assert outcome.result == "RATE_LIMITED"

    def test_client_error_does_not_store_outcome(self, fake_operator: MagicMock) -> None:
        import streamlit as st

        fake_operator.refresh_registry_from_chain.side_effect = DashboardClientError("rpc dead")
        with patch.object(panel.st, "error") as mock_err:
            panel._do_refresh("sid", fake_operator)
        mock_err.assert_called_once()
        assert panel._key(panel._REFRESH_OUTCOME_KEY, "sid") not in st.session_state


# =============================================================================
# Result renderers — pin the visual contract via streamlit-call inspection
# =============================================================================


class TestRenderPreviewResult:
    def test_no_session_state_renders_nothing(self) -> None:
        with (
            patch.object(panel.st, "markdown") as mock_md,
            patch.object(panel.st, "metric"),
            patch.object(panel.st, "columns"),
        ):
            panel._render_preview_result("sid")
        mock_md.assert_not_called()

    def test_renders_metrics_for_three_buckets(self) -> None:
        import streamlit as st

        preview = _make_preview(matched_count=2, phantom_count=1, stranded_count=3)
        st.session_state[panel._key(panel._PREVIEW_RESULT_KEY, "sid")] = preview

        col_match = MagicMock()
        col_phantom = MagicMock()
        col_stranded = MagicMock()
        with (
            patch.object(panel.st, "markdown"),
            patch.object(panel.st, "caption"),
            patch.object(panel.st, "columns", return_value=(col_match, col_phantom, col_stranded)),
            patch.object(panel.st, "dataframe"),
        ):
            panel._render_preview_result("sid")
        col_match.metric.assert_called_once_with("Matched", 2)
        col_phantom.metric.assert_called_once_with("Phantom missing", 1)
        col_stranded.metric.assert_called_once_with("Stranded", 3)


class TestRenderApplyOutcome:
    def test_success_uses_success_banner(self) -> None:
        import streamlit as st

        st.session_state[panel._key(panel._APPLY_OUTCOME_KEY, "sid")] = ApplyReconcileResult(
            result="SUCCESS",
            detail="",
            rebuilt=[
                RebuiltRow(
                    physical_identity_hash="h1",
                    primitive="lp",
                    accounting_category="LP_UNIV3",
                    source="reconciliation_discovery",
                    last_reconciled_at_block=4000,
                    reconciliation_id="r",
                    registry_row_json="",
                )
            ],
        )
        with (
            patch.object(panel.st, "markdown"),
            patch.object(panel.st, "success") as mock_success,
            patch.object(panel.st, "dataframe"),
        ):
            panel._render_apply_outcome("sid")
        mock_success.assert_called_once()

    def test_partial_success_uses_warning(self) -> None:
        import streamlit as st

        st.session_state[panel._key(panel._APPLY_OUTCOME_KEY, "sid")] = ApplyReconcileResult(
            result="PARTIAL_SUCCESS",
            detail="",
            rebuilt=[],
            primitive_errors=[
                PrimitiveError(
                    primitive="lp",
                    chain="avalanche",
                    code="RPC_FANOUT_FAILED",
                    message="t",
                    recoverable=True,
                )
            ],
        )
        with (
            patch.object(panel.st, "markdown"),
            patch.object(panel.st, "warning") as mock_warning,
            patch.object(panel.st, "dataframe"),
        ):
            panel._render_apply_outcome("sid")
        mock_warning.assert_called_once()

    def test_state_drift_uses_error(self) -> None:
        import streamlit as st

        st.session_state[panel._key(panel._APPLY_OUTCOME_KEY, "sid")] = ApplyReconcileResult(
            result="STATE_DRIFT", detail="moved on"
        )
        with (
            patch.object(panel.st, "markdown"),
            patch.object(panel.st, "error") as mock_error,
        ):
            panel._render_apply_outcome("sid")
        mock_error.assert_called_once()


class TestRenderRefreshOutcome:
    def test_success_uses_success_banner(self) -> None:
        import streamlit as st

        st.session_state[panel._key(panel._REFRESH_OUTCOME_KEY, "sid")] = RefreshRegistryResult(
            result="SUCCESS",
            detail="",
            positions_refreshed=3,
            events_emitted=1,
            source_block_number=200,
            reconciliation_id="r",
        )
        with (
            patch.object(panel.st, "markdown"),
            patch.object(panel.st, "success") as mock_success,
        ):
            panel._render_refresh_outcome("sid")
        mock_success.assert_called_once()
        assert "3" in mock_success.call_args.args[0]

    def test_rate_limited_uses_info(self) -> None:
        import streamlit as st

        st.session_state[panel._key(panel._REFRESH_OUTCOME_KEY, "sid")] = RefreshRegistryResult(
            result="RATE_LIMITED",
            detail="another running",
            positions_refreshed=0,
            events_emitted=0,
            source_block_number=0,
            reconciliation_id="",
        )
        with (
            patch.object(panel.st, "markdown"),
            patch.object(panel.st, "info") as mock_info,
        ):
            panel._render_refresh_outcome("sid")
        mock_info.assert_called_once()

    def test_failed_uses_error(self) -> None:
        import streamlit as st

        st.session_state[panel._key(panel._REFRESH_OUTCOME_KEY, "sid")] = RefreshRegistryResult(
            result="FAILED",
            detail="rpc fanout failed",
            positions_refreshed=0,
            events_emitted=0,
            source_block_number=0,
            reconciliation_id="",
        )
        with (
            patch.object(panel.st, "markdown"),
            patch.object(panel.st, "error") as mock_error,
        ):
            panel._render_refresh_outcome("sid")
        mock_error.assert_called_once()
