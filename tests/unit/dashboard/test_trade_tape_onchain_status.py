"""ALM-2759 — trade-tape headline reflects on-chain LANDED status.

``transaction_ledger.success`` is the *framework verdict* ("iteration
completed cleanly: execution + slippage gate + reconciliation"), written
``False`` on the slippage circuit-breaker and reconciliation-failure
paths even when the tx LANDED on-chain. The tape headline ✓/✗ must read
the per-leg receipt status (``sub_transactions[*].status`` /
``all_tx_results[*].success``) instead, and render the framework
downgrade as a distinct "landed but flagged" badge.

These tests exercise the pure resolver ``_resolve_onchain_display_status``
(no Streamlit needed) across the five required cases:

a. clean success                 -> landed True,  no flag
b. hard failure (no leg landed)  -> landed False, no flag
c. slippage-breach landed        -> landed True,  flag "slippage breach"
d. recon-failure landed          -> landed True,  flag "reconciliation downgraded"
e. no per-leg data               -> landed None (defer to framework verdict)
"""

from __future__ import annotations

import json

import pytest

from almanak.framework.dashboard.gateway_client import TradeTapeRow


@pytest.fixture
def make_row():  # type: ignore[no-untyped-def]
    def _make(
        *,
        extracted_data_json: str = "",
        success: bool = True,
        error: str = "",
    ) -> TradeTapeRow:
        return TradeTapeRow(
            id="row-1",
            cycle_id="cyc-1",
            timestamp=None,
            intent_type="SWAP",
            token_in="USDC",
            amount_in="100",
            token_out="WETH",
            amount_out="0.04",
            effective_price="2500",
            slippage_bps=10.0,
            gas_used=300_000,
            gas_usd="2.0",
            tx_hash="0xtail",
            chain="arbitrum",
            protocol="uniswap_v3",
            success=success,
            error=error,
            amount_in_usd="100",
            amount_out_usd="100",
            extracted_data_json=extracted_data_json,
            price_inputs_json="",
            pre_state_json="",
            post_state_json="",
            accounting_payload_json="",
            accounting_event_type="",
            position_key="",
            confidence="HIGH",
            unavailable_reason="",
            schema_version=1,
            formula_version=1,
            matching_policy_version=3,
            position_event_json="",
            position_id="",
            position_event_type="",
        )

    return _make


def _sub_txs(*statuses: str) -> str:
    """Build an ``extracted_data_json`` with a ``sub_transactions`` array."""
    return json.dumps(
        {"sub_transactions": [{"tx_hash": f"0x{i}", "status": s} for i, s in enumerate(statuses)]}
    )


class TestResolveOnchainDisplayStatus:
    def test_a_clean_success_landed_no_flag(self, make_row) -> None:  # type: ignore[no-untyped-def]
        from almanak.framework.dashboard.pages.trade_tape import _resolve_onchain_display_status

        row = make_row(extracted_data_json=_sub_txs("success", "success"), success=True)
        landed, reason = _resolve_onchain_display_status(row)
        assert landed is True
        assert reason is None

    def test_b_hard_failure_no_leg_landed(self, make_row) -> None:  # type: ignore[no-untyped-def]
        from almanak.framework.dashboard.pages.trade_tape import _resolve_onchain_display_status

        # Action reverted (a trailing reset may still succeed); any failed
        # measured leg => not fully landed => red ✗.
        row = make_row(
            extracted_data_json=_sub_txs("success", "failure"),
            success=False,
            error="execution reverted",
        )
        landed, reason = _resolve_onchain_display_status(row)
        assert landed is False
        assert reason is None

    def test_c_slippage_breach_landed_flagged(self, make_row) -> None:  # type: ignore[no-untyped-def]
        from almanak.framework.dashboard.pages.trade_tape import _resolve_onchain_display_status

        # Legs all landed on-chain, but the framework downgraded the
        # iteration on a slippage circuit-breaker => success=False.
        row = make_row(
            extracted_data_json=_sub_txs("success", "success"),
            success=False,
            error="Slippage circuit breaker: actual slippage 320 bps exceeds max 100 bps",
        )
        landed, reason = _resolve_onchain_display_status(row)
        assert landed is True
        assert reason == "slippage breach"

    def test_d_reconciliation_failure_landed_flagged(self, make_row) -> None:  # type: ignore[no-untyped-def]
        from almanak.framework.dashboard.pages.trade_tape import _resolve_onchain_display_status

        row = make_row(
            extracted_data_json=_sub_txs("success"),
            success=False,
            error="Balance reconciliation incident: USDC delta=-5 expected=[0,1]",
        )
        landed, reason = _resolve_onchain_display_status(row)
        assert landed is True
        assert reason == "reconciliation downgraded"

    def test_d2_recon_prefix_classified(self, make_row) -> None:  # type: ignore[no-untyped-def]
        from almanak.framework.dashboard.pages.trade_tape import _resolve_onchain_display_status

        row = make_row(
            extracted_data_json=_sub_txs("success"),
            success=False,
            error="Reconciliation failed for cycle cyc-1",
        )
        _, reason = _resolve_onchain_display_status(row)
        assert reason == "reconciliation downgraded"

    def test_e_no_per_leg_data_defers_to_framework(self, make_row) -> None:  # type: ignore[no-untyped-def]
        from almanak.framework.dashboard.pages.trade_tape import _resolve_onchain_display_status

        # No sub_transactions / all_tx_results at all (older or
        # unmeasured row). Empty != Zero: defer, never invent a verdict.
        row = make_row(extracted_data_json='{"swap_amounts": {}}', success=False, error="boom")
        landed, reason = _resolve_onchain_display_status(row)
        assert landed is None
        assert reason is None

    def test_e2_empty_extracted_data_defers(self, make_row) -> None:  # type: ignore[no-untyped-def]
        from almanak.framework.dashboard.pages.trade_tape import _resolve_onchain_display_status

        row = make_row(extracted_data_json="", success=True)
        landed, reason = _resolve_onchain_display_status(row)
        assert landed is None
        assert reason is None

    def test_unmeasured_legs_only_defers(self, make_row) -> None:  # type: ignore[no-untyped-def]
        from almanak.framework.dashboard.pages.trade_tape import _resolve_onchain_display_status

        # sub_transactions present but legs carry neither status nor
        # success (schema skew) => unmeasured => defer.
        row = make_row(
            extracted_data_json=json.dumps({"sub_transactions": [{"tx_hash": "0xa"}]}),
            success=True,
        )
        landed, reason = _resolve_onchain_display_status(row)
        assert landed is None
        assert reason is None

    def test_falls_back_to_all_tx_results_when_no_sub_transactions(self, make_row) -> None:  # type: ignore[no-untyped-def]
        from almanak.framework.dashboard.pages.trade_tape import _resolve_onchain_display_status

        # Pre-VIB-4087 rows: only the multi-tx ``all_tx_results`` (bool
        # ``success``) is present. The resolver must still read it.
        row = make_row(
            extracted_data_json=json.dumps(
                {"all_tx_results": [{"tx_hash": "0xa", "success": True}, {"tx_hash": "0xb", "success": True}]}
            ),
            success=False,
            error="Slippage breach detected",
        )
        landed, reason = _resolve_onchain_display_status(row)
        assert landed is True
        assert reason == "slippage breach"

    def test_landed_but_no_recognised_reason_buckets_generic(self, make_row) -> None:  # type: ignore[no-untyped-def]
        from almanak.framework.dashboard.pages.trade_tape import _resolve_onchain_display_status

        row = make_row(
            extracted_data_json=_sub_txs("success"),
            success=False,
            error="some other post-execution downgrade",
        )
        landed, reason = _resolve_onchain_display_status(row)
        assert landed is True
        assert reason == "flagged post-execution"


class TestRenderTapeRowMarkers:
    """End-to-end render check: the headline HTML reflects landed status.

    ``_render_tape_row`` writes via ``st.markdown``; we capture the
    emitted HTML to assert the marker colour + badge without a real
    Streamlit runtime.
    """

    @staticmethod
    def _render_html(monkeypatch, row) -> str:  # type: ignore[no-untyped-def]
        import almanak.framework.dashboard.pages.trade_tape as tape

        captured: list[str] = []

        def _fake_markdown(body, *args, **kwargs):  # type: ignore[no-untyped-def]
            captured.append(str(body))

        class _FakeExpander:
            def __enter__(self):  # type: ignore[no-untyped-def]
                return self

            def __exit__(self, *exc):  # type: ignore[no-untyped-def]
                return False

        def _fake_expander(*args, **kwargs):  # type: ignore[no-untyped-def]
            return _FakeExpander()

        def _fake_columns(n, *args, **kwargs):  # type: ignore[no-untyped-def]
            return [_FakeExpander() for _ in range(n)]

        monkeypatch.setattr(tape.st, "markdown", _fake_markdown)
        monkeypatch.setattr(tape.st, "expander", _fake_expander)
        monkeypatch.setattr(tape.st, "columns", _fake_columns)
        tape._render_tape_row(row, show_approvals=False)
        return "\n".join(captured)

    def test_slippage_landed_renders_green_marker_and_amber_badge(self, make_row, monkeypatch) -> None:  # type: ignore[no-untyped-def]
        row = make_row(
            extracted_data_json=_sub_txs("success", "success"),
            success=False,
            error="Slippage circuit breaker: actual slippage 320 bps exceeds 100",
        )
        html = self._render_html(monkeypatch, row)
        # Green ✓ marker (landed), amber flagged chip, NOT the red ⛔.
        assert "#00c853;'>✓" in html
        assert "landed on-chain · flagged: slippage breach" in html
        assert "⛔" not in html

    def test_hard_failure_renders_red_marker_and_error_chip(self, make_row, monkeypatch) -> None:  # type: ignore[no-untyped-def]
        row = make_row(
            extracted_data_json=_sub_txs("success", "failure"),
            success=False,
            error="execution reverted: out of gas",
        )
        html = self._render_html(monkeypatch, row)
        assert "#f44336;'>✗" in html
        assert "⛔" in html
        assert "landed on-chain · flagged" not in html

    def test_clean_success_renders_green_marker_no_badge(self, make_row, monkeypatch) -> None:  # type: ignore[no-untyped-def]
        row = make_row(extracted_data_json=_sub_txs("success", "success"), success=True)
        html = self._render_html(monkeypatch, row)
        assert "#00c853;'>✓" in html
        assert "flagged" not in html
        assert "⛔" not in html

    def test_no_per_leg_data_failure_defers_to_framework_red(self, make_row, monkeypatch) -> None:  # type: ignore[no-untyped-def]
        # Unmeasured row with framework success=False => current behaviour
        # (red ✗ + ⛔ error chip), never a fabricated green ✓.
        row = make_row(extracted_data_json="", success=False, error="boom")
        html = self._render_html(monkeypatch, row)
        assert "#f44336;'>✗" in html
        assert "⛔" in html
