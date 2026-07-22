"""Tests for the Phase 1-RPC-backed section helpers (VIB-4495 / Phase 3).

These helpers are the renderer surface that strategy authors and the
operator dashboard call from inside Streamlit pages. We test:

* Public exports resolve through the package's lazy ``__getattr__``.
* Each section delegates to the correct ``DashboardServiceClient``
  method and forwards filter / window arguments verbatim.
* Each section degrades to ``st.info`` rather than crashing when the
  client raises ``DashboardClientError``.
* Empty-state branches render a sensible message instead of an empty
  table (which scrolls past the operator without comment).
* The read-only sections never reference the operator client.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.dashboard import sections_reconciliation as sec
from almanak.framework.dashboard.service_client import (
    CutoverState,
    CutoverStateEntry,
    DashboardClientError,
    GetPositionsResult,
    GetRangeHistoryResult,
    PositionConfidence,
    PositionEntry,
    PositionSource,
    PositionStatus,
    PrimitiveCoverageStub,
    RangeHistoryEntry,
    ReconciliationFinding,
    ReconciliationReport,
    ReconciliationSeverity,
)

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def fake_client() -> MagicMock:
    """Mock DashboardServiceClient — every section call site stubs methods on this."""
    return MagicMock()


def _make_position(
    *,
    handle: str = "lp-1",
    accounting_category: str = "LP_UNIV3",
    status: PositionStatus = PositionStatus.OPEN,
    source: PositionSource = PositionSource.REGISTRY,
    confidence: PositionConfidence = PositionConfidence.HIGH,
    cutover_state: CutoverState = CutoverState.BACKFILL_COMPLETE,
    value_usd: Decimal | None = Decimal("100.50"),
) -> PositionEntry:
    return PositionEntry(
        handle=handle,
        physical_identity_hash=f"hash-{handle}",
        deployment_id="dep1",
        chain="avalanche",
        primitive="lp",
        accounting_category=accounting_category,
        status=status,
        opened_at_block=1000,
        closed_at_block=0,
        opened_tx="0xopen",
        closed_tx="",
        value_usd=value_usd,
        value_token0=Decimal("50.00"),
        value_token1=Decimal("50.25"),
        source=source,
        confidence=confidence,
        last_reconciled_at_block=2000,
        cutover_state=cutover_state,
        primitive_payload_json="",
        value_as_of="2026-05-17T00:00:00Z",
    )


# =============================================================================
# Public-API plumbing
# =============================================================================


class TestPublicAPI:
    @pytest.mark.parametrize(
        "name",
        [
            "render_positions_section",
            "render_position_range_history_section",
            "render_reconciliation_report_section",
        ],
    )
    def test_lazy_import_resolves(self, name: str) -> None:
        import almanak.framework.dashboard as dash_pkg

        resolved = getattr(dash_pkg, name)
        assert callable(resolved)

    def test_reconciliation_module_does_not_import_operator_client(self) -> None:
        """The read-only sections module must NOT import OperatorDashboardServiceClient.

        Docstring mentions are fine (they're documentation of the boundary);
        only actual imports would leak operator surfaces into renderer code.
        """
        import inspect

        source = inspect.getsource(sec)
        # Scan only `from`/`import` lines so docstring mentions don't trip the assert.
        import_lines = [line for line in source.splitlines() if line.lstrip().startswith(("from ", "import "))]
        joined = "\n".join(import_lines)
        assert "OperatorDashboardServiceClient" not in joined, (
            "sections_reconciliation.py must not IMPORT OperatorDashboardServiceClient "
            "so Phase 4's CI lint can use it as a renderer-safe import target."
        )


# =============================================================================
# Display helpers — pinned via the public renderers below, but the most
# load-bearing formatters get unit coverage so a label drift is loud.
# =============================================================================


class TestDisplayHelpers:
    @pytest.mark.parametrize(
        "state, expected_label",
        [
            (CutoverState.PRE_BACKFILL, "[cutover: pre-backfill]"),
            (CutoverState.BACKFILL_IN_PROGRESS, "[cutover: backfill in progress]"),
            (CutoverState.BACKFILL_COMPLETE, "[cutover: backfill complete]"),
            (CutoverState.REGISTRY_AUTHORITATIVE, "[cutover: registry authoritative]"),
            (CutoverState.UNSPECIFIED, "[cutover: unknown]"),
        ],
    )
    def test_cutover_pill_labels(self, state: CutoverState, expected_label: str) -> None:
        assert sec._cutover_pill(state) == expected_label

    def test_value_usd_formatting_zero(self) -> None:
        # A *measured* zero renders as an explicit $0.00 (distinct from unmeasured).
        assert sec._format_value_usd(Decimal("0")) == "$0.00"

    def test_value_usd_formatting_unmeasured_is_dash_not_zero(self) -> None:
        # Empty≠Zero: an unmeasured value (None) must render "—", never a
        # fabricated "$0.00" that contradicts the position's real value on the
        # other surfaces (VIB-5738 cluster).
        assert sec._format_value_usd(None) == "—"

    def test_value_usd_formatting_thousands(self) -> None:
        assert sec._format_value_usd(Decimal("12345.6789")) == "$12,345.68"

    def test_position_row_unmeasured_value_renders_dash(self) -> None:
        """An open position the gateway couldn't value (value_usd=None) shows
        "—" in the Value column, not a confident "$0.00"."""
        p = _make_position(value_usd=None)
        row = sec._position_row(p)
        assert row["Value (USD)"] == "—"

    def test_format_freshness_iso(self) -> None:
        out = sec._format_freshness("2026-05-17T01:23:45Z")
        assert "2026-05-17 01:23:45 UTC" in out
        assert out.startswith("as of ")

    def test_format_freshness_unix(self) -> None:
        out = sec._format_freshness(1747444800)
        assert out.startswith("as of ")

    def test_format_freshness_zero_returns_empty(self) -> None:
        assert sec._format_freshness(0) == ""
        assert sec._format_freshness("") == ""

    def test_format_freshness_malformed_returns_empty(self) -> None:
        assert sec._format_freshness("not-an-iso-date") == ""

    def test_position_row_handle_fallback(self) -> None:
        """Empty handle should fall back to a truncated identity hash."""
        p = _make_position(handle="")
        row = sec._position_row(p)
        # Hash is "hash-" (5 chars) — truncated[:12] = "hash-"
        assert row["Handle"] == "hash-"


# =============================================================================
# render_positions_section
# =============================================================================


class TestRenderPositionsSection:
    def test_passes_filters_to_client(self, fake_client: MagicMock) -> None:
        fake_client.get_positions.return_value = GetPositionsResult(
            positions=[_make_position()],
            cutover_states=[],
        )
        with (
            patch.object(sec.st, "divider"),
            patch.object(sec.st, "markdown"),
            patch.object(sec.st, "dataframe"),
        ):
            sec.render_positions_section("sid", fake_client, chain="avalanche", primitive="lp")

        fake_client.get_positions.assert_called_once_with("sid", chain="avalanche", primitive="lp")

    def test_groups_by_accounting_category(self, fake_client: MagicMock) -> None:
        positions = [
            _make_position(handle="lp1", accounting_category="LP_UNIV3"),
            _make_position(handle="lp2", accounting_category="LP_UNIV3"),
            _make_position(handle="aave1", accounting_category="AAVE_COLLATERAL"),
        ]
        fake_client.get_positions.return_value = GetPositionsResult(
            positions=positions,
            cutover_states=[
                CutoverStateEntry(
                    accounting_category="LP_UNIV3",
                    state=CutoverState.BACKFILL_COMPLETE,
                    rows_synthesized=2,
                    rows_skipped_already_present=0,
                    backfill_started_at="",
                    backfill_completed_at="",
                    backfill_reader_version=1,
                    last_reconciled_at_block=5000,
                    last_reconciled_unix_seconds=1747440000,
                )
            ],
        )
        with (
            patch.object(sec.st, "divider"),
            patch.object(sec.st, "markdown") as mock_md,
            patch.object(sec.st, "dataframe") as mock_df,
        ):
            sec.render_positions_section("sid", fake_client)

        # One main heading + one sub-header per category = 1 + 2 = 3
        markdown_calls = [c.args[0] for c in mock_md.call_args_list]
        assert any("### Positions" in s for s in markdown_calls)
        assert any("LP_UNIV3" in s for s in markdown_calls)
        assert any("AAVE_COLLATERAL" in s for s in markdown_calls)
        # The LP_UNIV3 header must carry the cutover pill.
        lp_header = next(s for s in markdown_calls if "LP_UNIV3" in s)
        assert "backfill complete" in lp_header
        # Two dataframes — one per category group.
        assert mock_df.call_count == 2

    def test_empty_state(self, fake_client: MagicMock) -> None:
        fake_client.get_positions.return_value = GetPositionsResult([], [])
        with (
            patch.object(sec.st, "divider"),
            patch.object(sec.st, "markdown"),
            patch.object(sec.st, "info") as mock_info,
            patch.object(sec.st, "dataframe") as mock_df,
        ):
            sec.render_positions_section("sid", fake_client)
        mock_info.assert_called_once()
        mock_df.assert_not_called()

    def test_client_error_fails_loud_and_clean(self, fake_client: MagicMock) -> None:
        # VIB-4047: a GetPositions failure must fail LOUD (a banner, never a
        # silently-empty pane that reads as "no activity") and CLEAN (the raw
        # error text — e.g. a leaked gRPC repr — is never shown to the user).
        fake_client.get_positions.side_effect = DashboardClientError("boom raw grpc detail")
        with (
            patch.object(sec.st, "divider"),
            patch.object(sec.st, "markdown"),
            patch.object(sec.st, "info") as mock_info,
            patch.object(sec.st, "warning") as mock_warning,
            patch.object(sec.st, "error") as mock_error,
            patch.object(sec.st, "dataframe") as mock_df,
        ):
            sec.render_positions_section("sid", fake_client)
        # A loud banner (warning for a generic error, error for auth/unreachable)
        # — not the quiet blue st.info the field bug hid behind.
        assert mock_warning.called or mock_error.called
        mock_info.assert_not_called()
        shown = " ".join(str(c.args[0]) for c in (*mock_warning.call_args_list, *mock_error.call_args_list))
        assert "boom raw grpc detail" not in shown
        mock_df.assert_not_called()

    def test_auth_error_renders_red_banner(self, fake_client: MagicMock) -> None:
        # An UNAUTHENTICATED failure against the strategy's own managed gateway
        # renders the red st.error auth banner — the dangerous silent-empty case.
        fake_client.get_positions.side_effect = DashboardClientError(
            "GetPositions failed: <_InactiveRpcError ... StatusCode.UNAUTHENTICATED ...>"
        )
        with (
            patch.object(sec.st, "divider"),
            patch.object(sec.st, "markdown"),
            patch.object(sec.st, "error") as mock_error,
            patch.object(sec.st, "dataframe") as mock_df,
        ):
            sec.render_positions_section("sid", fake_client)
        mock_error.assert_called_once()
        assert "authenticate" in mock_error.call_args.args[0].lower()
        mock_df.assert_not_called()


# =============================================================================
# render_position_range_history_section
# =============================================================================


class TestRenderPositionRangeHistorySection:
    def test_missing_identifier_short_circuits(self, fake_client: MagicMock) -> None:
        with (
            patch.object(sec.st, "divider"),
            patch.object(sec.st, "markdown"),
            patch.object(sec.st, "info") as mock_info,
        ):
            sec.render_position_range_history_section(
                "sid", fake_client, chain="avalanche", accounting_category="LP_UNIV3"
            )
        mock_info.assert_called_once()
        fake_client.get_position_range_history.assert_not_called()

    def test_stub_message_renders_as_info(self, fake_client: MagicMock) -> None:
        fake_client.get_position_range_history.return_value = GetRangeHistoryResult(
            entries=[], stub_message="No held position; see trade tape."
        )
        with (
            patch.object(sec.st, "divider"),
            patch.object(sec.st, "markdown"),
            patch.object(sec.st, "info") as mock_info,
            patch.object(sec.st, "dataframe") as mock_df,
        ):
            sec.render_position_range_history_section(
                "sid",
                fake_client,
                chain="avalanche",
                accounting_category="SWAP",
                handle="x",
            )
        mock_info.assert_called_once_with("No held position; see trade tape.")
        mock_df.assert_not_called()

    def test_empty_no_stub_renders_caption(self, fake_client: MagicMock) -> None:
        fake_client.get_position_range_history.return_value = GetRangeHistoryResult(entries=[], stub_message="")
        with (
            patch.object(sec.st, "divider"),
            patch.object(sec.st, "markdown"),
            patch.object(sec.st, "caption") as mock_caption,
            patch.object(sec.st, "info") as mock_info,
        ):
            sec.render_position_range_history_section(
                "sid",
                fake_client,
                chain="avalanche",
                accounting_category="LP_UNIV3",
                handle="lp",
            )
        mock_caption.assert_called_once()
        mock_info.assert_not_called()

    def test_renders_entries_into_table(self, fake_client: MagicMock) -> None:
        fake_client.get_position_range_history.return_value = GetRangeHistoryResult(
            entries=[
                RangeHistoryEntry(
                    timestamp_unix_seconds=1747440000,
                    block_number=5000,
                    event_type="OPEN",
                    source_table="position_events",
                    ledger_entry_id="led-1",
                    tx_hash="0xtx1",
                    payload_json="",
                ),
                RangeHistoryEntry(
                    timestamp_unix_seconds=0,
                    block_number=0,
                    event_type="",
                    source_table="position_events",
                    ledger_entry_id="",
                    tx_hash="",
                    payload_json="",
                ),
            ],
            stub_message="",
        )
        with (
            patch.object(sec.st, "divider"),
            patch.object(sec.st, "markdown"),
            patch.object(sec.st, "dataframe") as mock_df,
        ):
            sec.render_position_range_history_section(
                "sid",
                fake_client,
                chain="avalanche",
                accounting_category="LP_UNIV3",
                physical_identity_hash="h1",
            )
        mock_df.assert_called_once()
        rows = mock_df.call_args.args[0]
        assert len(rows) == 2
        # First row gets a formatted timestamp (unix 1747440000 = 2025-05-17);
        # second row has "—" placeholders for missing fields.
        assert rows[0]["Timestamp (UTC)"].startswith("2025-05-17")
        assert rows[1]["Timestamp (UTC)"] == "—"
        assert rows[1]["Block"] == "—"

    def test_forwards_time_window(self, fake_client: MagicMock) -> None:
        fake_client.get_position_range_history.return_value = GetRangeHistoryResult([], "")
        start = datetime(2026, 5, 1, 0, 0, 0, tzinfo=UTC)
        end = datetime(2026, 5, 17, 0, 0, 0, tzinfo=UTC)
        with (
            patch.object(sec.st, "divider"),
            patch.object(sec.st, "markdown"),
            patch.object(sec.st, "caption"),
        ):
            sec.render_position_range_history_section(
                "sid",
                fake_client,
                chain="avalanche",
                accounting_category="LP_UNIV3",
                handle="lp",
                from_time=start,
                to_time=end,
            )
        call = fake_client.get_position_range_history.call_args
        assert call.kwargs["from_time"] == start
        assert call.kwargs["to_time"] == end

    def test_client_error_degrades(self, fake_client: MagicMock) -> None:
        fake_client.get_position_range_history.side_effect = DashboardClientError("rpc dead")
        with (
            patch.object(sec.st, "divider"),
            patch.object(sec.st, "markdown"),
            patch.object(sec.st, "info") as mock_info,
        ):
            sec.render_position_range_history_section(
                "sid",
                fake_client,
                chain="avalanche",
                accounting_category="LP_UNIV3",
                handle="lp",
            )
        mock_info.assert_called_once()
        assert "rpc dead" in mock_info.call_args.args[0]


# =============================================================================
# render_reconciliation_report_section
# =============================================================================


class TestRenderReconciliationReportSection:
    def _report(self, findings=None, stubs=None) -> ReconciliationReport:
        return ReconciliationReport(
            findings=findings or [],
            primitive_stubs=stubs or [],
            reconciliation_id="recon-1",
            source_block_number=12345,
            as_of="2026-05-17T01:00:00Z",
        )

    def test_no_findings_renders_success(self, fake_client: MagicMock) -> None:
        fake_client.get_reconciliation_report.return_value = self._report()
        with (
            patch.object(sec.st, "divider"),
            patch.object(sec.st, "markdown"),
            patch.object(sec.st, "caption"),
            patch.object(sec.st, "columns") as mock_cols,
            patch.object(sec.st, "success") as mock_success,
            patch.object(sec.st, "dataframe") as mock_df,
        ):
            mock_cols.return_value = (MagicMock(), MagicMock(), MagicMock())
            sec.render_reconciliation_report_section("sid", fake_client)
        mock_success.assert_called_once()
        mock_df.assert_not_called()

    def test_renders_scope_label_distinguishing_from_g6(self, fake_client: MagicMock) -> None:
        """VIB-5942: the report must carry an explicit position-STRUCTURE scope
        label so it never reads as a contradiction of the header's PnL-identity
        G6 tile (both can be simultaneously true)."""
        fake_client.get_reconciliation_report.return_value = self._report()
        captions: list[str] = []
        successes: list[str] = []
        with (
            patch.object(sec.st, "divider"),
            patch.object(sec.st, "markdown"),
            patch.object(sec.st, "caption", side_effect=lambda t, *a, **k: captions.append(t)),
            patch.object(sec.st, "columns") as mock_cols,
            patch.object(sec.st, "success", side_effect=lambda t, *a, **k: successes.append(t)),
            patch.object(sec.st, "dataframe"),
        ):
            mock_cols.return_value = (MagicMock(), MagicMock(), MagicMock())
            sec.render_reconciliation_report_section("sid", fake_client)
        # A scope caption naming "structure" and pointing at the G6 tile is present.
        assert any("structure" in c.lower() and "g6" in c.lower() for c in captions), captions
        # The no-findings success text is scoped to structure, not a global "all agree".
        assert successes and "structural" in successes[0].lower()

    def test_findings_render_dataframe_with_severity_column(self, fake_client: MagicMock) -> None:
        findings = [
            ReconciliationFinding(
                accounting_category="LP_UNIV3",
                physical_identity_hash="hashabc12345",
                severity=ReconciliationSeverity.DIVERGED,
                delta="100 vs 95",
                ledger_has_row=True,
                snapshot_has_row=True,
                registry_has_row=True,
                suggested_action="PreviewReconcile",
            )
        ]
        fake_client.get_reconciliation_report.return_value = self._report(findings=findings)
        with (
            patch.object(sec.st, "divider"),
            patch.object(sec.st, "markdown"),
            patch.object(sec.st, "caption"),
            patch.object(sec.st, "columns") as mock_cols,
            patch.object(sec.st, "dataframe") as mock_df,
            patch.object(sec.st, "success"),
        ):
            mock_cols.return_value = (MagicMock(), MagicMock(), MagicMock())
            sec.render_reconciliation_report_section("sid", fake_client)
        mock_df.assert_called_once()
        rendered_rows = mock_df.call_args.args[0]
        assert len(rendered_rows) == 1
        assert rendered_rows[0]["Severity"] == "DIVERGED"
        assert rendered_rows[0]["Coverage (L+S+R)"] == "L+S+R"

    def test_primitive_stubs_render_caption_per_stub(self, fake_client: MagicMock) -> None:
        stubs = [
            PrimitiveCoverageStub(primitive="lending", message="pending", ticket="VIB-4501"),
            PrimitiveCoverageStub(primitive="perp", message="pending", ticket="VIB-4202"),
        ]
        fake_client.get_reconciliation_report.return_value = self._report(stubs=stubs)
        with (
            patch.object(sec.st, "divider"),
            patch.object(sec.st, "markdown"),
            patch.object(sec.st, "caption") as mock_caption,
            patch.object(sec.st, "columns") as mock_cols,
            patch.object(sec.st, "success"),
            patch.object(sec.st, "info"),
        ):
            mock_cols.return_value = (MagicMock(), MagicMock(), MagicMock())
            sec.render_reconciliation_report_section("sid", fake_client)
        # caption called for freshness pill + per stub + reconciliation_id = 4 calls
        # We assert there is at least one caption per stub + one freshness.
        caption_texts = [c.args[0] for c in mock_caption.call_args_list]
        assert any("VIB-4501" in t for t in caption_texts)
        assert any("VIB-4202" in t for t in caption_texts)

    def test_no_findings_with_stubs_qualifies_success_to_covered_set(self, fake_client: MagicMock) -> None:
        """VIB-5942 audit (CodeRabbit): with 0 findings BUT uncovered primitive stubs,
        the banner must NOT claim everything reconciles — coverage is partial. It is
        qualified to the COVERED set (st.info), not an unqualified green st.success,
        and the stub caption calls out that those primitives were NOT reconciled."""
        stubs = [PrimitiveCoverageStub(primitive="perp", message="parser pending", ticket="VIB-5717")]
        fake_client.get_reconciliation_report.return_value = self._report(stubs=stubs)
        infos: list[str] = []
        markdowns: list[str] = []
        with (
            patch.object(sec.st, "divider"),
            patch.object(sec.st, "markdown", side_effect=lambda t, *a, **k: markdowns.append(t)),
            patch.object(sec.st, "caption"),
            patch.object(sec.st, "columns") as mock_cols,
            patch.object(sec.st, "success") as mock_success,
            patch.object(sec.st, "info", side_effect=lambda t, *a, **k: infos.append(t)),
            patch.object(sec.st, "dataframe") as mock_df,
        ):
            mock_cols.return_value = (MagicMock(), MagicMock(), MagicMock())
            sec.render_reconciliation_report_section("sid", fake_client)
        mock_df.assert_not_called()  # no findings
        mock_success.assert_not_called()  # NOT an unqualified green banner
        assert infos and "covered" in infos[0].lower() and "partial" in infos[0].lower()
        # The stubs list explicitly marks the uncovered primitives as NOT reconciled.
        assert any("NOT reconciled" in m for m in markdowns)

    def test_no_findings_no_stubs_stays_full_success(self, fake_client: MagicMock) -> None:
        """Full coverage + 0 findings keeps the unqualified success banner."""
        fake_client.get_reconciliation_report.return_value = self._report()  # no stubs
        with (
            patch.object(sec.st, "divider"),
            patch.object(sec.st, "markdown"),
            patch.object(sec.st, "caption"),
            patch.object(sec.st, "columns") as mock_cols,
            patch.object(sec.st, "success") as mock_success,
            patch.object(sec.st, "info") as mock_info,
        ):
            mock_cols.return_value = (MagicMock(), MagicMock(), MagicMock())
            sec.render_reconciliation_report_section("sid", fake_client)
        mock_success.assert_called_once()
        mock_info.assert_not_called()

    def test_client_error_degrades(self, fake_client: MagicMock) -> None:
        fake_client.get_reconciliation_report.side_effect = DashboardClientError("nope")
        with (
            patch.object(sec.st, "divider"),
            patch.object(sec.st, "markdown"),
            patch.object(sec.st, "info") as mock_info,
            patch.object(sec.st, "columns"),
        ):
            sec.render_reconciliation_report_section("sid", fake_client)
        mock_info.assert_called_once()
        assert "nope" in mock_info.call_args.args[0]
