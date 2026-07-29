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
        tx_hash: str = "0xtail",
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
            tx_hash=tx_hash,
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


def _degraded_fields(*, tx_hash: str = "0xtail", legs: str = "") -> dict[str, object]:
    """``make_row`` kwargs for a degraded row, PRODUCED BY THE REAL WRITER.

    ``success``, ``error`` and ``extracted_data_json`` are whatever
    ``classify_ledger_row`` + ``apply_degradation`` actually stamp — the same
    pair the runner's commit path and the gateway's ``SaveLedgerEntry`` both
    call. Nothing here is asserted about the writer; it is run.

    This exists because the controls it replaces paired a ``DEGRADED_PREFIX``
    error with ``extracted_data_json=""``, a combination **no producer can
    emit**: ``apply_degradation`` unconditionally ends with
    ``entry.extracted_data_json = json.dumps(payload)`` and always sets
    ``accounting_degradation`` inside it, and every write boundary routes
    through it. A control pinned to an impossible shape controls nothing — the
    exact defect class that let this PR's own leg-schema regression through CI.

    ``legs=""`` is the no-leg-receipt lane, and it IS reachable: the marker's
    payload is the only thing in ``extracted_data_json`` when the row carried no
    ``sub_transactions`` to begin with — a pre-VIB-4087 row, or one written
    through the gateway RPC by a client that does not emit them.
    """
    from almanak.framework.accounting.ledger_guard import apply_degradation, classify_ledger_row
    from almanak.framework.observability.ledger import LedgerEntry

    entry = LedgerEntry(
        intent_type="SWAP",
        success=True,
        tx_hash=tx_hash,
        amount_in="",
        amount_out="",
        extracted_data_json=legs,
        chain="arbitrum",
        protocol="pancakeswap_v3",
    )
    degradation = classify_ledger_row(entry)
    assert degradation is not None, (
        "fixture precondition: the guard must fire on an unmeasured success row"
    )
    apply_degradation(entry, degradation)
    return {
        "success": entry.success,
        "error": entry.error,
        "extracted_data_json": entry.extracted_data_json,
        "tx_hash": entry.tx_hash,
    }


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


class TestDegradedRowsRenderAsLanded:
    """VIB-6043 leg 2 — a degraded row is landed-but-flagged, never failed.

    The write-time Empty != Zero guard is the THIRD path that writes
    ``transaction_ledger.success=False`` on a tx that really landed (after the
    slippage circuit-breaker and the reconciliation finalizer that ALM-2759
    already handles). It additionally stamps an ``accounting_degraded:`` marker
    on ``error``.

    The lane the guard exists for — a Safe / bundle execution whose parser
    yields neither amounts NOR per-leg receipts — is *precisely* the lane where
    the receipt-based resolver has nothing to measure. Before this fix the
    resolver returned ``(None, None)`` there and the caller fell back to the raw
    ``row.success``, rendering a **red ✗ for a trade that executed** — the same
    misreport ALM-2759 fixed for the slippage lane, on the row shape an operator
    can least easily check by hand.
    """

    @staticmethod
    def _degraded_error(detail: str = "amount_in,amount_out") -> str:
        from almanak.framework.accounting.ledger_guard import DEGRADED_PREFIX

        return f"{DEGRADED_PREFIX}amounts_unmeasured: {detail}"

    def test_degraded_with_no_leg_receipts_is_landed_not_failed(self, make_row) -> None:  # type: ignore[no-untyped-def]
        """THE regression. This is the common shape, not an edge case."""
        from almanak.framework.dashboard.pages.trade_tape import _resolve_onchain_display_status

        row = make_row(**_degraded_fields())
        landed, reason = _resolve_onchain_display_status(row)
        assert landed is True, "a degraded row landed on-chain — rendering it failed misreports an executed trade"
        assert reason == "books degraded (amounts unmeasured)"

    def test_degraded_with_no_leg_data_in_the_payload_is_still_landed(self, make_row) -> None:  # type: ignore[no-untyped-def]
        """The marker lives on ``error``, so no leg data in the payload is fine.

        This used to pass ``extracted_data_json=""``, which is unproducible
        alongside the marker — ``apply_degradation`` always writes a JSON
        payload. The real shape is the marker payload and nothing else, which is
        what ``_degraded_fields(legs="")`` builds by running the writer.
        """
        from almanak.framework.dashboard.pages.trade_tape import (
            _get_sub_transactions,
            _resolve_onchain_display_status,
        )

        row = make_row(**_degraded_fields(legs=""))
        assert "accounting_degradation" in json.loads(row.extracted_data_json)
        assert not _get_sub_transactions(row), "precondition: no leg receipts in the payload"
        landed, reason = _resolve_onchain_display_status(row)
        assert landed is True
        assert reason == "books degraded (amounts unmeasured)"

    def test_degraded_with_successful_leg_receipts_is_landed_and_flagged(self, make_row) -> None:  # type: ignore[no-untyped-def]
        from almanak.framework.dashboard.pages.trade_tape import _resolve_onchain_display_status

        row = make_row(**_degraded_fields(legs=_sub_txs("success")))
        landed, reason = _resolve_onchain_display_status(row)
        assert landed is True
        assert reason == "books degraded (amounts unmeasured)"

    def test_a_measured_failed_leg_outranks_the_degradation_marker(self, make_row) -> None:  # type: ignore[no-untyped-def]
        """The marker must not paint a genuine revert green.

        The guard asserts *the books are degraded*, not *every leg succeeded*.
        A measured failure is direct on-chain evidence and must win — otherwise
        the marker becomes a way to launder a real revert into a green tick,
        which is strictly worse than the bug being fixed.
        """
        from almanak.framework.dashboard.pages.trade_tape import _resolve_onchain_display_status

        row = make_row(**_degraded_fields(legs=_sub_txs("failure")))
        landed, reason = _resolve_onchain_display_status(row)
        assert landed is False, "a measured failed leg must stay red even when the books are degraded"
        assert reason is None

    def test_degraded_reason_is_not_mis_bucketed_as_slippage(self, make_row) -> None:  # type: ignore[no-untyped-def]
        """Marker detail is arbitrary text and must be bucketed before keywords.

        ``LedgerDegradation.detail`` carries free-form diagnostics. If the
        degradation bucket were tested *after* the keyword buckets, a detail
        string that happens to contain "slippage" or "reconciliation" would be
        attributed to the wrong subsystem and send an operator hunting the wrong
        bug.
        """
        from almanak.framework.dashboard.pages.trade_tape import _classify_downgrade_reason

        assert _classify_downgrade_reason(self._degraded_error("slippage guard ran, amounts empty")) == (
            "books degraded (amounts unmeasured)"
        )
        assert _classify_downgrade_reason(self._degraded_error("reconciliation ok, parser silent")) == (
            "books degraded (amounts unmeasured)"
        )

    def test_non_degraded_rows_are_unaffected(self, make_row) -> None:  # type: ignore[no-untyped-def]
        """Anti-vacuity control: the deferral path must still exist.

        Without this, the fix could have been "always return True" and every
        assertion above would still pass while destroying ALM-2759's rule that
        an unknown on-chain status defers rather than inventing a verdict.
        """
        from almanak.framework.dashboard.pages.trade_tape import _resolve_onchain_display_status

        row = make_row(extracted_data_json='{"swap_amounts": {}}', success=False, error="boom")
        landed, reason = _resolve_onchain_display_status(row)
        assert landed is None, "a non-degraded row with no receipts must still defer to the framework verdict"
        assert reason is None


class TestCsvExportDoesNotConflateVerdictWithOnchainStatus:
    """VIB-6043 leg 2 — the CSV's ``tx_success`` column means ON-CHAIN.

    The export has two distinct columns: ``tx_success`` (per-leg on-chain
    receipt status) and ``intent_success`` (the framework verdict). For a
    single-tx intent there is no ``all_tx_results`` array, so a one-leg bundle
    is synthesized — and its ``success`` used to be filled from the raw
    ``row.success``, conflating the two columns.

    A degraded row, or a slippage/reconciliation downgrade, lands on-chain while
    carrying ``success=False``. The export therefore claimed the TRANSACTION
    failed for a trade that executed — in the artifact this exporter's own
    docstring says auditors work from. ``intent_success`` still reports the
    framework verdict, which is what that column is for.
    """

    @staticmethod
    def _rows_for(row):  # type: ignore[no-untyped-def]
        import csv as _csv
        import io

        from almanak.framework.dashboard.pages.trade_tape import _rows_to_csv

        text, _count = _rows_to_csv([row])
        return list(_csv.DictReader(io.StringIO(text)))

    def test_a_degraded_single_tx_row_exports_tx_success_1(self, make_row) -> None:  # type: ignore[no-untyped-def]
        row = make_row(**_degraded_fields())
        out = self._rows_for(row)
        assert len(out) == 1
        assert out[0]["tx_success"] == "1", "the tx landed on-chain; tx_success must not report the framework verdict"
        assert out[0]["intent_success"] == "0", "intent_success is the framework verdict and must stay 0"

    def test_a_genuinely_failed_single_tx_row_still_exports_tx_success_0(self, make_row) -> None:  # type: ignore[no-untyped-def]
        """Anti-vacuity control — the fix must not make every row look landed."""
        row = make_row(extracted_data_json=_sub_txs("failure"), success=False, error="execution reverted")
        out = self._rows_for(row)
        assert out[0]["tx_success"] == "0"
        assert out[0]["intent_success"] == "0"

    def test_a_clean_row_is_unchanged(self, make_row) -> None:  # type: ignore[no-untyped-def]
        row = make_row(extracted_data_json=_sub_txs("success"), success=True, error="")
        out = self._rows_for(row)
        assert out[0]["tx_success"] == "1"
        assert out[0]["intent_success"] == "1"


class TestTheMarkerIsNotOnChainEvidenceOnItsOwn:
    """VIB-6043 leg 2 — `chain_success: True` is asserted, never measured.

    `apply_degradation` stamps the marker without consulting a receipt, so the
    marker alone cannot establish that anything happened on-chain. Two ways that
    bites, both found by adversarial review of #3441 and both fixed:

    * a NO-OP bundle (an `LP_CLOSE` against an already-empty position) has zero
      transactions, `success=True`, `tx_hash=""` and no amounts — so the guard
      stamps the marker for a trade that never existed;
    * a schema-skewed `sub_transactions` array short-circuited the fallback to
      `all_tx_results`, hiding a MEASURED failure behind the marker.
    """

    @staticmethod
    def _degraded() -> str:
        from almanak.framework.accounting.ledger_guard import DEGRADED_PREFIX

        return f"{DEGRADED_PREFIX}amounts_unmeasured: amount_in,amount_out"

    def test_a_no_op_bundle_is_not_rendered_as_landed(self, make_row) -> None:  # type: ignore[no-untyped-def]
        """Nothing was submitted, so nothing landed.

        Green-for-nothing-happened tells an operator "the trade executed, we just
        lost the numbers" for a routine idempotent teardown. That is the same
        misreport class this fix exists to prevent, inverted.
        """
        from almanak.framework.dashboard.pages.trade_tape import _resolve_onchain_display_status

        # no-op: the compiler emitted zero transactions, so there is no hash
        row = make_row(**_degraded_fields(tx_hash=""))
        landed, reason = _resolve_onchain_display_status(row)
        assert landed is not True, "a degraded row with NO tx hash must not render as landed on-chain"
        assert reason is None

    def test_a_real_degraded_row_with_a_tx_hash_is_still_landed(self, make_row) -> None:  # type: ignore[no-untyped-def]
        """Anti-vacuity control — the tx_hash gate must not disable the fix.

        The lane the guard exists for always carries a tx hash, so the gate must
        cost it nothing.
        """
        from almanak.framework.dashboard.pages.trade_tape import _resolve_onchain_display_status

        row = make_row(**_degraded_fields())
        assert row.tx_hash, "fixture precondition: this row carries a tx hash"
        landed, reason = _resolve_onchain_display_status(row)
        assert landed is True
        assert reason == "books degraded (amounts unmeasured)"

    def test_a_measured_failure_in_all_tx_results_outranks_a_skewed_sub_transactions(self, make_row) -> None:  # type: ignore[no-untyped-def]
        """The fallback keys on "no MEASURED status", not on "empty list".

        A non-empty but unmeasured `sub_transactions` used to short-circuit the
        `or`, so `all_tx_results` was never read and a measured on-chain FAILURE
        was painted green by the marker.
        """
        from almanak.framework.dashboard.pages.trade_tape import _resolve_onchain_display_status

        row = make_row(
            **_degraded_fields(
                legs=json.dumps(
                    {
                        "sub_transactions": [{"tx_hash": "0xa"}],  # present but unmeasured
                        "all_tx_results": [{"tx_hash": "0xa", "success": False}],  # MEASURED failure
                    }
                )
            )
        )
        landed, reason = _resolve_onchain_display_status(row)
        assert landed is False, "a measured failure must outrank the marker even from the fallback array"
        assert reason is None

    def test_every_degradation_reason_has_an_operator_label(self) -> None:
        """A new enum member must not silently render as the wrong reason.

        The marker carries the reason token, and the label map is keyed on it.
        Hardcoding one string would mislabel every row of a newly-added reason
        with no test failing.
        """
        from almanak.framework.accounting.ledger_guard import LedgerDegradationReason
        from almanak.framework.dashboard.pages.trade_tape import _DEGRADED_REASON_LABELS

        missing = {r.value for r in LedgerDegradationReason} - set(_DEGRADED_REASON_LABELS)
        assert not missing, f"LedgerDegradationReason members without an operator label: {sorted(missing)}"

    def test_an_unknown_reason_token_degrades_to_an_honest_generic_label(self) -> None:
        """Anti-vacuity control for the map: unknown must not read as a known reason."""
        from almanak.framework.accounting.ledger_guard import DEGRADED_PREFIX
        from almanak.framework.dashboard.pages.trade_tape import _classify_downgrade_reason

        assert _classify_downgrade_reason(f"{DEGRADED_PREFIX}some_future_reason: detail") == "books degraded"


class TestCsvUsesTheMeasuredLegVerdictNotAMissingKey:
    """VIB-6043 leg 2 — the CSV must read the leg schema it is actually given.

    The two leg arrays have DIFFERENT schemas: ``all_tx_results`` entries carry
    ``success``; ``sub_transactions`` entries carry ``status``
    (``"success"``/``"failure"``) and **no ``success`` key at all**. Once the
    export started resolving legs via ``_resolve_legs`` — which returns
    ``sub_transactions`` for essentially every executed row — a
    ``tx.get("success", True)`` read defaulted every leg to ``True``, including
    ones whose receipt says REVERTED.

    Every test below uses a REALISTIC ``sub_transactions`` payload. The earlier
    controls all used ``extracted_data_json=""``, which production cannot
    produce for an executed transaction (``tx_hash`` and ``sub_transactions``
    both derive from ``result.transaction_results``, so they are non-empty
    together) — which is exactly why this regression passed CI. A control that
    pins an impossible shape controls nothing.
    """

    @staticmethod
    def _csv(row):  # type: ignore[no-untyped-def]
        import csv as _csv
        import io

        from almanak.framework.dashboard.pages.trade_tape import _rows_to_csv

        text, _n = _rows_to_csv([row])
        return list(_csv.DictReader(io.StringIO(text)))

    def test_a_reverted_leg_exports_tx_success_0(self, make_row) -> None:  # type: ignore[no-untyped-def]
        """THE regression: a revert must never export as landed."""
        row = make_row(
            extracted_data_json=json.dumps(
                {
                    "sub_transactions": [
                        {"tx_hash": "0xapp", "status": "success"},
                        {"tx_hash": "0xact", "status": "failure"},
                    ]
                }
            ),
            success=False,
            error="execution reverted",
        )
        out = self._csv(row)
        assert [o["tx_success"] for o in out] == ["1", "0"], (
            "sub_transactions carry `status`, not `success`; reading the wrong key defaults a "
            "reverted leg to landed"
        )

    def test_all_successful_legs_export_1(self, make_row) -> None:  # type: ignore[no-untyped-def]
        """Anti-vacuity control — the fix must not mark everything failed."""
        row = make_row(
            extracted_data_json=json.dumps(
                {
                    "sub_transactions": [
                        {"tx_hash": "0xapp", "status": "success"},
                        {"tx_hash": "0xact", "status": "success"},
                    ]
                }
            ),
            success=True,
        )
        assert [o["tx_success"] for o in self._csv(row)] == ["1", "1"]

    def test_an_unmeasured_leg_exports_blank_not_a_claim(self, make_row) -> None:  # type: ignore[no-untyped-def]
        """Empty != Zero in the export: unmeasured is neither landed nor failed."""
        row = make_row(
            extracted_data_json=json.dumps({"sub_transactions": [{"tx_hash": "0xa"}]}),
            success=True,
        )
        assert [o["tx_success"] for o in self._csv(row)] == [""], (
            "a leg with neither status nor success is unmeasured; exporting 1 or 0 is a claim"
        )

    def test_the_no_op_bundle_exports_unmeasured_not_landed_and_not_failed(self, make_row) -> None:  # type: ignore[no-untyped-def]
        """The tx_hash gate applies to the CSV too — but ``0`` is also a lie.

        The headline refused to claim this row landed; the export claimed it
        anyway (``tx_success=1``) — the same defect persisted into the audit
        artifact after being removed from the screen. Both surfaces now share one
        predicate.

        This assertion USED to demand ``"0"``, which swaps one false statement
        for another: ``0`` means "this transaction FAILED", and a no-op did not
        fail — an ``LP_CLOSE`` against an already-empty position is the correct
        idempotent outcome of a teardown with nothing left to close. Nothing
        landed, nothing reverted, no receipt exists to grade. ``""`` is the
        export's existing unmeasured cell and the only honest value; this is the
        same Empty != Zero rule the money columns obey.
        """
        row = make_row(**_degraded_fields(tx_hash=""))
        out = self._csv(row)[0]
        assert out["tx_success"] == "", (
            "a no-op has no on-chain measurement: 1 claims it landed, 0 claims it failed"
        )
        assert out["intent_success"] == "0", "the framework verdict is still a downgrade"

    def test_a_real_degraded_row_with_a_hash_still_exports_as_landed(self, make_row) -> None:  # type: ignore[no-untyped-def]
        """Anti-vacuity control for the gate — it must not disable the fix."""
        row = make_row(**_degraded_fields())
        assert row.tx_hash
        out = self._csv(row)[0]
        assert out["tx_success"] == "1"
        assert out["intent_success"] == "0"


def test_pick_action_tx_reads_status_not_only_success() -> None:
    """``pick_action_tx``'s all-approvals fallback is now fed ``sub_transactions``.

    Same key mismatch one module over: a bare ``tx.get("success", True)`` treats
    every reverted sub-transaction as successful.
    """
    from almanak.framework.dashboard.utils import pick_action_tx

    legs = [
        {"tx_hash": "0xa", "function_selector": "0x095ea7b3", "status": "failure"},
        {"tx_hash": "0xb", "function_selector": "0x095ea7b3", "status": "success"},
    ]
    assert pick_action_tx(legs, "SWAP")["tx_hash"] == "0xb"


def test_a_no_op_of_ANY_intent_type_does_not_export_as_landed(make_row) -> None:  # type: ignore[no-untyped-def]
    """The tx_hash gate must not depend on the intent type being money-slotted.

    The degraded arm only fires for intent types in `REQUIRED_MONEY_SLOTS`
    (SWAP / LP_OPEN / LP_CLOSE). A no-op of any OTHER type is never marked, so
    `landed()`'s clean arm — deliberately blind to the hash — answered True and
    the row exported `tx_success=1` with an EMPTY tx_hash.

    Reachable and routine: a Compound V3 `withdraw_all` against zero collateral
    returns SUCCESS with `transactions=[]` and `no_op=True`, which the runner
    writes as `success=True, tx_hash=""`.

    A blank hash means no transaction was submitted, so there is nothing to
    grade — whatever the intent type. (VIB-6043 review.)
    """
    import csv as _csv
    import io

    from almanak.framework.dashboard.pages.trade_tape import _rows_to_csv

    for intent in ("WITHDRAW", "REPAY", "UNSTAKE", "LP_CLOSE"):
        row = make_row(extracted_data_json="", success=True, error="")
        row.tx_hash = ""
        row.intent_type = intent
        out = list(_csv.DictReader(io.StringIO(_rows_to_csv([row])[0])))
        assert out[0]["tx_success"] == "", (
            f"{intent}: a no-op with no tx_hash must export unmeasured, not landed"
        )


def test_a_real_transaction_of_the_same_intent_types_still_exports_landed(make_row) -> None:  # type: ignore[no-untyped-def]
    """Anti-vacuity partner — the blank-hash guard must not blank everything."""
    import csv as _csv
    import io

    from almanak.framework.dashboard.pages.trade_tape import _rows_to_csv

    for intent in ("WITHDRAW", "REPAY", "UNSTAKE"):
        row = make_row(extracted_data_json="", success=True, error="")
        row.intent_type = intent
        assert row.tx_hash
        out = list(_csv.DictReader(io.StringIO(_rows_to_csv([row])[0])))
        assert out[0]["tx_success"] == "1", f"{intent}: a real tx must still export landed"


class TestANoOpRowRendersWithoutCrashingTheTape:
    """Regression: the no-op branch left `onchain_ok` unbound.

    `_render_tape_row` binds the headline verdict once and reads it in THREE
    places (the ✓/✗ marker, the intent border colour, the error chip). The
    first version of the no-op third state assigned it inside the `else` arm
    only, so every no-op row raised `UnboundLocalError` at the border-colour
    line — aborting the row loop and blanking the tape.

    It shipped green because the test written alongside it called
    `_submitted_nothing()` and never `_render_tape_row()`. A predicate test
    cannot observe an unbound local in its caller: the vacuity class the whole
    PR exists to close, committed by the fix for it.

    Reachable exactly where the docstring advertises: a Compound V3
    `withdraw_all` against zero collateral, or a Euler V2 full exit — a routine
    idempotent teardown, which writes `success=1, tx_hash=""`.
    """

    def test_an_unmarked_no_op_renders_at_all(self, make_row, monkeypatch) -> None:  # type: ignore[no-untyped-def]
        """The crash. Without the fix this raises UnboundLocalError."""
        row = make_row(tx_hash="", success=True, error="")
        html = TestRenderTapeRowMarkers._render_html(monkeypatch, row)
        assert html, "the row rendered nothing at all"

    def test_a_marked_no_op_renders_at_all(self, make_row, monkeypatch) -> None:  # type: ignore[no-untyped-def]
        """The degraded sibling takes the same branch and must also survive."""
        row = make_row(
            tx_hash="", success=False, error="accounting_degraded:amounts_unmeasured: intent=LP_CLOSE"
        )
        html = TestRenderTapeRowMarkers._render_html(monkeypatch, row)
        assert html, "the row rendered nothing at all"

    def test_the_no_op_claims_neither_success_nor_failure(self, make_row, monkeypatch) -> None:  # type: ignore[no-untyped-def]
        """Both glyphs are claims about a transaction that does not exist."""
        row = make_row(tx_hash="", success=True, error="")
        html = TestRenderTapeRowMarkers._render_html(monkeypatch, row)
        assert "#00c853;'>✓" not in html, "green tick asserts a trade that was never submitted"
        assert "#f44336;'>✗" not in html, "red cross asserts a failure that never happened"
        assert ">—<" in html, "expected the neutral no-op glyph"

    def test_the_no_op_does_not_borrow_the_failure_colour(self, make_row, monkeypatch) -> None:  # type: ignore[no-untyped-def]
        """`not None` is True, so a truthiness test painted the row red while
        the glyph beside it said "—". The border must be neutral too."""
        row = make_row(tx_hash="", success=True, error="")
        html = TestRenderTapeRowMarkers._render_html(monkeypatch, row)
        assert "#f44336" not in html, "no-op row carries the failure colour somewhere"

    def test_a_real_trade_is_unaffected(self, make_row, monkeypatch) -> None:  # type: ignore[no-untyped-def]
        """The guard must not swallow rows that DID submit."""
        row = make_row(extracted_data_json=_sub_txs("success"), tx_hash="0xtail")
        html = TestRenderTapeRowMarkers._render_html(monkeypatch, row)
        assert "#00c853;'>✓" in html, "a real landed trade must still render green"

    def test_a_real_failure_is_unaffected(self, make_row, monkeypatch) -> None:  # type: ignore[no-untyped-def]
        row = make_row(extracted_data_json=_sub_txs("failure"), tx_hash="0xtail", success=False, error="reverted")
        html = TestRenderTapeRowMarkers._render_html(monkeypatch, row)
        assert "#f44336;'>✗" in html, "a real reverted trade must still render red"


class TestAPreSubmissionFailureIsNotDisguisedAsANoOp:
    """A row that FAILED before broadcast must not render as "nothing happened".

    Compile failure, validation failure, retries-exhausted-before-broadcast:
    all write a row with no tx_hash and no legs — structurally identical to a
    no-op. `_submitted_nothing` originally keyed only on "blank hash + no
    measured legs", so those rows took the no-op path and rendered a neutral
    glyph, a grey border, a tooltip reading literally "(no-op) — nothing to
    grade", and **no error text at all**.

    Pre-PR they rendered red with the full error. So the delta hid a genuine
    failure on the surface an operator opens to investigate one — this PR's own
    misreport class, turned onto failures. It was masked by the UnboundLocalError
    until that was fixed, then became silently reachable.

    The discriminator is three-way, not `row.success`: a MARKED no-op (LP_CLOSE
    against an empty position) carries success=0 plus the degradation marker,
    and keying on success alone would push it back to the red cross VIB-6173
    exists to remove.
    """

    def test_a_failure_before_broadcast_keeps_its_cross(self, make_row, monkeypatch) -> None:  # type: ignore[no-untyped-def]
        row = make_row(tx_hash="", success=False, error="Intent compilation failed: no route found")
        html = TestRenderTapeRowMarkers._render_html(monkeypatch, row)
        assert "#f44336;'>✗" in html, "a genuine failure must render as a failure"
        assert ">—<" not in html, "a failure must not borrow the no-op glyph"

    def test_the_failure_reason_is_still_shown(self, make_row, monkeypatch) -> None:  # type: ignore[no-untyped-def]
        """Suppressing the error is the worse half — it removes the only thing
        the operator opened the row to read."""
        row = make_row(tx_hash="", success=False, error="Intent compilation failed: no route found")
        html = TestRenderTapeRowMarkers._render_html(monkeypatch, row)
        assert "no route found" in html, "the failure reason was suppressed entirely"

    def test_the_marked_no_op_still_reads_as_a_no_op(self, make_row, monkeypatch) -> None:  # type: ignore[no-untyped-def]
        """The reason this is a three-way test and not `row.success`."""
        row = make_row(
            tx_hash="", success=False, error="accounting_degraded:amounts_unmeasured: intent=LP_CLOSE"
        )
        html = TestRenderTapeRowMarkers._render_html(monkeypatch, row)
        assert ">—<" in html, "a marked no-op must stay neutral, not revert to the red cross"
        assert "#f44336;'>✗" not in html

    def test_an_unmarked_no_op_still_reads_as_a_no_op(self, make_row, monkeypatch) -> None:  # type: ignore[no-untyped-def]
        row = make_row(tx_hash="", success=True, error="")
        html = TestRenderTapeRowMarkers._render_html(monkeypatch, row)
        assert ">—<" in html


class TestTheHeadlineIsNotIdentitySensitive:
    """`0 is False` is False in Python — an int-valued `success` must still work.

    The error-chip branch has to exclude the no-op (None) without collapsing to
    `is False`. SQLite returns integer 0/1 for booleans and `ledger_guard.landed`
    deliberately accepts the integer 1 for that reason, so a row whose `success`
    arrives as an int is not an exotic shape in this codebase — it is the shape
    the write path is documented to tolerate. An `is False` test would silently
    drop the failure chip on exactly those rows.
    """

    def test_an_integer_zero_success_still_renders_as_a_failure(self, make_row, monkeypatch) -> None:  # type: ignore[no-untyped-def]
        row = make_row(extracted_data_json="", tx_hash="0xtail", error="reverted")
        row.success = 0  # SQLite's spelling of False
        html = TestRenderTapeRowMarkers._render_html(monkeypatch, row)
        assert "#f44336;'>✗" in html, "an int-0 success must still read as a failure"
        assert "reverted" in html, "the failure reason must still be shown for an int-0 success"

    def test_an_integer_one_success_still_renders_as_landed(self, make_row, monkeypatch) -> None:  # type: ignore[no-untyped-def]
        row = make_row(extracted_data_json="", tx_hash="0xtail", error="")
        row.success = 1
        html = TestRenderTapeRowMarkers._render_html(monkeypatch, row)
        assert "#00c853;'>✓" in html, "an int-1 success must still read as landed"
