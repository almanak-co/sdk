"""VIB-4046 — sub-transaction surfacing in the trade tape.

Tests the three new helpers (``decode_selector``, ``is_approval_tx``,
``pick_action_tx``) against the six bundle patterns from the audit:

1. Single-tx supply (no ``all_tx_results``) — degenerate case.
2. Multi-tx swap with **approval-trailing** (action then reset-approve).
3. Multi-tx swap with **action-trailing** (approve then action).
4. Multi-tx full-repay (approve, repay, reset-approve).
5. Failed action with trailing approval reset.
6. All-approvals bundle (defensive — should never happen in production
   but the helper must not crash).

The helpers degrade cleanly when ``function_selector`` is absent
(today's ``all_tx_results`` shape) by falling back to a gas-band
heuristic.
"""

from __future__ import annotations

import pytest

from almanak.framework.dashboard.utils import (
    APPROVE_SELECTOR,
    decode_selector,
    is_approval_tx,
    pick_action_tx,
)


#: A ``TradeTapeRow`` builder. Module-level (it was class-scoped inside
#: ``TestSubTxParsing``) so the role-preference tests below can build rows
#: too — a second copy would be one more place for the fixture shape to
#: drift from ``TradeTapeRow``.
@pytest.fixture
def make_row() -> object:
    from almanak.framework.dashboard.gateway_client import TradeTapeRow

    def _make(extracted_data_json: str = "", tx_hash: str = "0xtail") -> TradeTapeRow:
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
            success=True,
            error="",
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


class TestDecodeSelector:
    def test_known_selector_returns_label(self) -> None:
        assert decode_selector(APPROVE_SELECTOR) == "approve"

    def test_known_selector_supply(self) -> None:
        assert decode_selector("0x617ba037") == "supply"

    def test_known_selector_uniswap_v3_exact_input(self) -> None:
        assert decode_selector("0x04e45aaf") == "exactInputSingle (R02)"

    def test_uppercase_selector_normalised(self) -> None:
        assert decode_selector("0X095EA7B3") == "approve"

    def test_unknown_selector_returned_as_is(self) -> None:
        # An unknown selector is still useful diagnostic — surface it.
        assert decode_selector("0xdeadbeef") == "0xdeadbeef"

    def test_unknown_uppercase_selector_normalised(self) -> None:
        # Unknown selectors are returned in normalized (lowercased,
        # 0x-prefixed) form so downstream tooling joining on selector
        # strings sees a consistent shape (Claude pr-auditor #5).
        assert decode_selector("0XDEADBEEF") == "0xdeadbeef"

    def test_unknown_no_prefix_normalised(self) -> None:
        assert decode_selector("DEADBEEF") == "0xdeadbeef"

    def test_none_returns_empty(self) -> None:
        assert decode_selector(None) == ""

    def test_empty_string_returns_empty(self) -> None:
        assert decode_selector("") == ""

    def test_no_0x_prefix_normalised(self) -> None:
        assert decode_selector("095ea7b3") == "approve"


class TestIsApprovalTx:
    def test_explicit_approve_selector(self) -> None:
        assert is_approval_tx({"function_selector": APPROVE_SELECTOR, "gas_used": 200_000}) is True

    def test_explicit_non_approve_selector_overrides_gas_heuristic(self) -> None:
        # Even a sub-50k gas tx is NOT an approval if the selector says
        # otherwise. Selector-first means selector wins.
        assert is_approval_tx({"function_selector": "0x617ba037", "gas_used": 46_000}) is False

    def test_no_selector_low_gas_is_approval(self) -> None:
        # Today's ``all_tx_results`` shape — no selector field — must
        # still detect a typical 46k-gas approve.
        assert is_approval_tx({"gas_used": 46_000, "tx_hash": "0xa", "success": True}) is True

    def test_no_selector_high_gas_is_action(self) -> None:
        assert is_approval_tx({"gas_used": 200_000, "tx_hash": "0xb", "success": True}) is False

    def test_no_selector_set_collateral_band_is_not_approval(self) -> None:
        # Aave V3 ``setUserUseReserveAsCollateral`` measures ~50–70k.
        # Tightened gas ceiling to 50k must NOT classify it as approval
        # (Claude pr-auditor #2 — the prior 80k ceiling silently hid
        # this leg from the default view).
        assert is_approval_tx({"gas_used": 60_000, "tx_hash": "0xc", "success": True}) is False
        assert is_approval_tx({"gas_used": 70_000, "tx_hash": "0xd", "success": True}) is False

    def test_no_selector_canonical_approve_is_approval(self) -> None:
        # Canonical ERC-20 approve costs land in 28–46k. Both must
        # still be tagged as approvals after the gas-band tightening.
        assert is_approval_tx({"gas_used": 28_000, "tx_hash": "0xa", "success": True}) is True
        assert is_approval_tx({"gas_used": 46_000, "tx_hash": "0xb", "success": True}) is True

    def test_no_selector_zero_gas_is_not_approval(self) -> None:
        # A failed/0-gas tx should not be classified as an approval.
        assert is_approval_tx({"gas_used": 0, "tx_hash": "0xc", "success": False}) is False

    def test_non_dict_input_is_safe(self) -> None:
        assert is_approval_tx("not a dict") is False  # type: ignore[arg-type]

    def test_garbage_gas_value_is_safe(self) -> None:
        assert is_approval_tx({"gas_used": "not-a-number"}) is False


class TestPickActionTx:
    """Six bundle patterns from VIB-4046's audit."""

    def test_pattern_1_single_tx_supply(self) -> None:
        # No ``all_tx_results`` — caller resolves to today's behavior
        # (use the row's ``tx_hash``). Helper still handles the
        # degenerate single-leg case for symmetry.
        legs = [{"tx_hash": "0xsupply", "gas_used": 180_000, "success": True}]
        assert pick_action_tx(legs)["tx_hash"] == "0xsupply"

    def test_pattern_2_swap_with_approval_trailing(self) -> None:
        # Approve → swap → reset-approve. This is the bug case from
        # the ticket: today's last-tx behavior would link the reset.
        legs = [
            {"tx_hash": "0xapprove", "gas_used": 46_000, "success": True},
            {"tx_hash": "0xswap", "gas_used": 220_000, "success": True},
            {"tx_hash": "0xreset", "gas_used": 28_000, "success": True},
        ]
        assert pick_action_tx(legs)["tx_hash"] == "0xswap"

    def test_pattern_3_swap_with_action_trailing(self) -> None:
        # Approve → swap. Action is the last leg AND the only non-
        # approval — trivially correct.
        legs = [
            {"tx_hash": "0xapprove", "gas_used": 46_000, "success": True},
            {"tx_hash": "0xswap", "gas_used": 220_000, "success": True},
        ]
        assert pick_action_tx(legs)["tx_hash"] == "0xswap"

    def test_pattern_4_full_repay(self) -> None:
        # Aave-style "amount=full" repay: approve → repay → reset.
        legs = [
            {"tx_hash": "0xapprove", "gas_used": 46_000, "success": True},
            {"tx_hash": "0xrepay", "gas_used": 180_000, "success": True},
            {"tx_hash": "0xreset", "gas_used": 28_000, "success": True},
        ]
        assert pick_action_tx(legs)["tx_hash"] == "0xrepay"

    def test_pattern_5_failed_action_with_trailing_approval(self) -> None:
        # Action reverted; subsequent reset-approve still landed.
        # Operator clicking the headline link must land on the
        # failure, not the trailing successful reset (Codex P2 +
        # Claude pr-auditor finding #3 — the reset link looks like
        # a successful tx, which destroys diagnostic trust).
        legs = [
            {"tx_hash": "0xapprove", "gas_used": 46_000, "success": True},
            {"tx_hash": "0xfailed", "gas_used": 220_000, "success": False},
            {"tx_hash": "0xreset", "gas_used": 28_000, "success": True},
        ]
        result = pick_action_tx(legs)
        # Failed action wins over trailing successful approval-reset.
        assert result["tx_hash"] == "0xfailed"

    def test_pattern_5b_failed_action_no_trailing_success(self) -> None:
        # Action reverted, no trailing reset. Picker still surfaces
        # the failed action so the operator can investigate.
        legs = [
            {"tx_hash": "0xapprove", "gas_used": 46_000, "success": True},
            {"tx_hash": "0xfailed", "gas_used": 220_000, "success": False},
        ]
        result = pick_action_tx(legs)
        assert result["tx_hash"] == "0xfailed"

    def test_pattern_6_all_approvals_bundle(self) -> None:
        # Defensive case — should never happen in production. Helper
        # must not crash and returns the last leg (today's behavior).
        legs = [
            {"tx_hash": "0xapprove1", "gas_used": 46_000, "success": True},
            {"tx_hash": "0xapprove2", "gas_used": 28_000, "success": True},
        ]
        assert pick_action_tx(legs)["tx_hash"] == "0xapprove2"

    def test_empty_list_returns_none(self) -> None:
        assert pick_action_tx([]) is None

    def test_none_returns_none(self) -> None:
        assert pick_action_tx(None) is None

    def test_selector_field_takes_precedence_over_gas(self) -> None:
        # If the receipt parser ever stamps ``function_selector`` on
        # sub-txs (out-of-scope in VIB-4046 but forward-compatible),
        # the selector decides — gas band is ignored.
        legs = [
            {
                "tx_hash": "0xa",
                "gas_used": 200_000,  # action-band gas...
                "success": True,
                "function_selector": APPROVE_SELECTOR,  # ...but selector says approve
            },
            {
                "tx_hash": "0xb",
                "gas_used": 46_000,  # approval-band gas...
                "success": True,
                "function_selector": "0x617ba037",  # ...but selector says supply
            },
        ]
        # Non-approval-by-selector is the second leg.
        assert pick_action_tx(legs)["tx_hash"] == "0xb"


class TestPickActionTxSingleLeg:
    """Single-leg edge cases for the CSV export contract.

    A genuine 1-element ``all_tx_results`` is, by definition, the
    action — picker still returns it. The CSV-export branch around
    that contract is verified at the call-site level (see comments in
    ``trade_tape.py:_render_csv_export``: ``is_single_leg = len(legs)
    == 1``). Caught by CodeRabbit on the second review pass —
    previously the ``not sub_txs`` guard missed the single-element
    list case.
    """

    def test_single_element_list_returns_only_leg(self) -> None:
        legs = [{"tx_hash": "0xa", "gas_used": 46_000, "success": True}]
        assert pick_action_tx(legs)["tx_hash"] == "0xa"


class TestCoerceGas:
    """Defensive ``gas_used`` coercion used by both the table renderer
    and the CSV export. A raw ``int(...)`` on a malformed historical
    ledger row would raise inside Streamlit's render loop and delete
    the entire trade-tape page (Claude pr-auditor #1)."""

    def test_int_passthrough(self) -> None:
        from almanak.framework.dashboard.pages.trade_tape import _coerce_gas

        assert _coerce_gas(220_000) == 220_000

    def test_numeric_string_coerced(self) -> None:
        from almanak.framework.dashboard.pages.trade_tape import _coerce_gas

        assert _coerce_gas("46000") == 46_000

    def test_none_returns_zero(self) -> None:
        from almanak.framework.dashboard.pages.trade_tape import _coerce_gas

        assert _coerce_gas(None) == 0

    def test_garbage_string_returns_zero(self) -> None:
        from almanak.framework.dashboard.pages.trade_tape import _coerce_gas

        assert _coerce_gas("not-a-number") == 0

    def test_list_returns_zero(self) -> None:
        from almanak.framework.dashboard.pages.trade_tape import _coerce_gas

        # Schema-skew defense: a future shape change that lands a list
        # in ``gas_used`` must not crash the render.
        assert _coerce_gas([1, 2, 3]) == 0

    def test_dict_returns_zero(self) -> None:
        from almanak.framework.dashboard.pages.trade_tape import _coerce_gas

        assert _coerce_gas({"foo": "bar"}) == 0


class TestSubTxParsing:
    """The dashboard's parse helpers. Imported lazily so streamlit isn't
    required just to run these tests."""

    def test_get_all_tx_results_missing_returns_empty(self, make_row) -> None:  # type: ignore[no-untyped-def]
        from almanak.framework.dashboard.pages.trade_tape import _get_all_tx_results

        row = make_row(extracted_data_json="")
        assert _get_all_tx_results(row) == []

    def test_get_all_tx_results_invalid_json_returns_empty(self, make_row) -> None:  # type: ignore[no-untyped-def]
        from almanak.framework.dashboard.pages.trade_tape import _get_all_tx_results

        row = make_row(extracted_data_json="{not json")
        assert _get_all_tx_results(row) == []

    def test_get_all_tx_results_no_field_returns_empty(self, make_row) -> None:  # type: ignore[no-untyped-def]
        from almanak.framework.dashboard.pages.trade_tape import _get_all_tx_results

        row = make_row(extracted_data_json='{"swap_amounts": {}}')
        assert _get_all_tx_results(row) == []

    def test_get_all_tx_results_populated(self, make_row) -> None:  # type: ignore[no-untyped-def]
        import json as _json

        from almanak.framework.dashboard.pages.trade_tape import _get_all_tx_results

        legs = [
            {"tx_hash": "0xa", "gas_used": 46_000, "success": True},
            {"tx_hash": "0xb", "gas_used": 220_000, "success": True},
        ]
        row = make_row(extracted_data_json=_json.dumps({"all_tx_results": legs}))
        assert _get_all_tx_results(row) == legs

    def test_get_all_tx_results_drops_non_dict_entries(self, make_row) -> None:  # type: ignore[no-untyped-def]
        import json as _json

        from almanak.framework.dashboard.pages.trade_tape import _get_all_tx_results

        # Defensive: a malformed entry (string instead of dict) must
        # not crash the dashboard. We drop it.
        row = make_row(
            extracted_data_json=_json.dumps(
                {"all_tx_results": [{"tx_hash": "0xa", "gas_used": 46_000, "success": True}, "junk"]}
            )
        )
        legs = _get_all_tx_results(row)
        assert len(legs) == 1
        assert legs[0]["tx_hash"] == "0xa"


class TestActionPickIsDerivedFromTheRealWriter:
    """VIB-6043 review — a `role`-first rule was added and then REMOVED.

    The rule was justified by a divergence that does not exist. The claim was
    that the tape headline read `all_tx_results` (gas ladder) while the CSV read
    `sub_transactions` (exact selector), so the two named different legs. Run
    against the REAL writer:

        roles     -> ['ACTION', 'ACTION', 'ACTION']
        selectors -> ['', '', '']

    `_function_selector_from_receipt` returns `""` unconditionally, so
    `sub_transactions` carries no selector either — both surfaces already fell
    to the gas ladder and already agreed. And `_classify_sub_tx_role` emits only
    APPROVAL/ACTION; `INCIDENTAL` is reserved-for-future and emitted nowhere.

    So "last ACTION" named the 30k unwrap where the ladder named `collect` — a
    regression, defended by three tests whose fixtures used `role="INCIDENTAL"`,
    a value no writer can produce. Vacuity class 1: the tests authorised a
    behaviour production cannot reach.

    These replacements build the bundle with `_build_sub_transactions` so the
    fixture is whatever the writer actually emits.
    """

    # Real event signatures. `_classify_sub_tx_role` returns "ACTION" immediately
    # for a leg with NO logs, so a `logs=[]` fixture never reaches the branch that
    # actually classifies anything — it would assert {"ACTION"} even if the log
    # branch were inverted. (Measured: inverting that branch left the whole suite
    # green.) Every real receipt carries logs, so the fixture must too.
    _TRANSFER = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
    _BURN = "0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c"
    _COLLECT = "0x70935338e69775456a85ddef226c395fb668b63fa0115f5f20610b388e6ca9c0"
    _WITHDRAWAL = "0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65"
    _APPROVAL = "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925"

    @classmethod
    def _leg(cls, h: str, gas: int, topics: list[str]):  # type: ignore[no-untyped-def]
        from types import SimpleNamespace as NS

        return NS(
            tx_hash=h, gas_used=gas, success=True, status=1,
            receipt=NS(status=1, logs=[{"topics": [t]} for t in topics]), error=None,
        )

    @classmethod
    def _real_lp_close_bundle(cls):  # type: ignore[no-untyped-def]
        from almanak.framework.observability.ledger import _build_sub_transactions

        # The repo's own documented shape: decreaseLiquidity + collect + unwrap,
        # each with the events it really emits. None is approval-only, so all
        # three classify ACTION through the REAL branch, not the empty-log default.
        return _build_sub_transactions([
            cls._leg("0xdec", 120_000, [cls._BURN, cls._TRANSFER]),
            cls._leg("0xcol", 90_000, [cls._COLLECT, cls._TRANSFER]),
            cls._leg("0xunwrap", 30_000, [cls._WITHDRAWAL]),
        ])

    def test_the_writer_emits_no_selectors_and_only_action_roles(self):
        """Pins the precondition the removed rule got wrong.

        If a future change starts emitting real selectors or INCIDENTAL roles,
        this fails first and names the reason — rather than a role-based picker
        silently becoming reachable and changing which tx the operator clicks.
        """
        subs = self._real_lp_close_bundle()
        assert {s.get("role") for s in subs} == {"ACTION"}
        assert {s.get("function_selector") for s in subs} == {""}

        # The canary is only meaningful if the classifier it watches CAN say
        # something else on this fixture shape. An approval-only leg must come
        # back APPROVAL through the same path — otherwise the assertion above
        # is pinned by the empty-log default rather than by the writer.
        from almanak.framework.observability.ledger import _build_sub_transactions

        approval_only = _build_sub_transactions([self._leg("0xapp", 46_000, [self._APPROVAL])])
        assert approval_only[0]["role"] == "APPROVAL", (
            "the role classifier cannot discriminate on this fixture shape — "
            "the ACTION assertion above would hold vacuously"
        )

    def test_the_action_pick_matches_the_pre_delta_answer(self):
        """The 30k unwrap is not the action leg; `collect` is."""
        from almanak.framework.dashboard.utils import pick_action_tx

        subs = self._real_lp_close_bundle()
        assert pick_action_tx(subs, "LP_CLOSE")["tx_hash"] == "0xcol"

    def test_both_leg_arrays_name_the_same_action(self):
        """The property the removed rule claimed to establish — it already held.

        `all_tx_results` and `sub_transactions` must agree about which leg is
        the action, and they do, because neither carries a selector and both
        fall to the same gas ladder.
        """
        from almanak.framework.dashboard.utils import pick_action_tx

        subs = self._real_lp_close_bundle()
        all_tx = [
            {"tx_hash": s["tx_hash"], "gas_used": s["gas_used"], "success": True}
            for s in subs
        ]
        assert pick_action_tx(subs, "LP_CLOSE")["tx_hash"] == pick_action_tx(all_tx, "LP_CLOSE")["tx_hash"]

class TestSubTxBlockRendersTheMeasuredLegVerdict:
    """The expander table must not repeat the CSV's key-mismatch bug.

    ``_render_sub_tx_block`` read ``tx.get("success", True)``. That was correct
    while it was fed ``all_tx_results`` (whose entries carry ``success``) and
    became wrong the moment it was fed ``_resolve_legs`` — ``sub_transactions``
    entries carry ``status: "success"/"failure"`` and no ``success`` key at all,
    so every leg defaulted to True and a REVERTED leg rendered a green tick.
    Identical defect to the one already fixed in the CSV export, one function
    over; pointing the headline at the shared resolver is what would have
    reintroduced it here.
    """

    @staticmethod
    def _render(legs: list[dict], monkeypatch) -> str:  # type: ignore[no-untyped-def]
        from almanak.framework.dashboard.pages import trade_tape as tt

        captured: list[str] = []
        monkeypatch.setattr(tt.st, "markdown", lambda html, **kw: captured.append(str(html)))
        row = type("R", (), {"chain": "arbitrum"})()
        tt._render_sub_tx_block(row, legs, show_approvals=True)  # type: ignore[arg-type]
        return "\n".join(captured)

    def test_a_reverted_leg_renders_red(self, monkeypatch) -> None:  # type: ignore[no-untyped-def]
        html = self._render(
            [{"tx_hash": "0xact", "gas_used": 200_000, "status": "failure", "role": "ACTION"}],
            monkeypatch,
        )
        assert "✗" in html and "✓" not in html, "a reverted sub-tx must not render as landed"

    def test_a_successful_leg_renders_green(self, monkeypatch) -> None:  # type: ignore[no-untyped-def]
        """Anti-vacuity control — the fix must not paint everything red."""
        html = self._render(
            [{"tx_hash": "0xact", "gas_used": 200_000, "status": "success", "role": "ACTION"}],
            monkeypatch,
        )
        assert "✓" in html and "✗" not in html

    def test_an_unmeasured_leg_renders_neither(self, monkeypatch) -> None:  # type: ignore[no-untyped-def]
        """Empty != Zero on screen: no measurement is not a failure."""
        html = self._render([{"tx_hash": "0xact", "gas_used": 200_000}], monkeypatch)
        assert "✓" not in html and "✗" not in html
        assert "not measured" in html


class TestANoOpIsNotGradedAsALandedTrade:
    """A row that submitted NOTHING must not render as an executed trade.

    A lending no-op (Compound V3 `withdraw_all` on zero collateral, Euler V2
    full exit) compiles to `transactions=[]` with `metadata["no_op"]=True`. The
    orchestrator returns success with no transaction results and the runner
    writes `success=1, tx_hash=""`. `WITHDRAW` is not in `REQUIRED_MONEY_SLOTS`,
    so nothing marks the row and `landed()`'s hash-blind clean arm answers True.

    Before this fix the headline rendered a GREEN TICK — an affirmative claim
    that a trade executed, for a transaction that was never sent — while the CSV
    lane on the same row refused to grade it. Green-for-nothing-happened is
    worse than the red tick it replaced, and the two lanes disagreeing is the
    surface split this delta exists to close.
    """

    @staticmethod
    def _noop_row(make_row, intent_type="WITHDRAW"):  # type: ignore[no-untyped-def]
        row = make_row(tx_hash="")
        row.intent_type = intent_type
        row.success = True   # the framework verdict a no-op really produces
        row.error = ""       # unmarked: not in REQUIRED_MONEY_SLOTS
        return row

    def test_the_headline_does_not_claim_a_no_op_executed(self, make_row):
        from almanak.framework.dashboard.pages.trade_tape import _submitted_nothing

        row = self._noop_row(make_row)
        assert _submitted_nothing(row) is True, (
            "a blank tx_hash with no measured legs is a MEASURED 'nothing was sent', "
            "not an unknown — if this reads False the headline falls back to "
            "row.success and paints a green tick for a trade that never happened"
        )

    def test_a_real_trade_is_still_graded(self, make_row):
        """The guard must not silence rows that DID submit — that would be the
        same misreport inverted, and it is how a hash gate usually goes wrong."""
        from almanak.framework.dashboard.pages.trade_tape import _submitted_nothing

        assert _submitted_nothing(make_row(tx_hash="0xtail")) is False

    def test_a_leg_bearing_row_with_no_parent_hash_is_still_graded(self, make_row):
        """A blank PARENT hash does not mean nothing was submitted when the legs
        carry measured receipts — grade it from the legs, as the CSV lane does."""
        import json

        from almanak.framework.dashboard.pages.trade_tape import _submitted_nothing

        row = self._noop_row(make_row)
        row.extracted_data_json = json.dumps(
            {"sub_transactions": [{"tx_hash": "0xleg", "status": "success", "gas_used": 90_000}]}
        )
        assert _submitted_nothing(row) is False

    def test_both_lanes_agree_that_a_no_op_is_ungraded(self, make_row):
        """The headline and the CSV must not disagree on the same row."""
        from almanak.framework.dashboard.pages.trade_tape import (
            _resolve_onchain_display_status,
            _submitted_nothing,
        )

        row = self._noop_row(make_row)
        landed, _ = _resolve_onchain_display_status(row)

        # The CSV lane's answer: not gradeable.
        assert landed is None
        # The headline must NOT resolve that None into row.success (True).
        assert _submitted_nothing(row) is True, (
            "headline would fall back to row.success=True while the CSV exports "
            "blank — the two lanes disagreeing about the same row"
        )


class TestTheCsvActionColumnStillHasAnOwner:
    """`is_action_tx` had ZERO test coverage anywhere in tests/ at HEAD.

    The delta deleted `test_the_headline_and_the_csv_name_the_same_action_tx` —
    the only test asserting end-to-end that the row flagged `is_action_tx=1` is
    the transaction the headline links to. The deletion was justified: all five
    of that class's fixtures used `role="INCIDENTAL"` or a fabricated selector,
    values no writer can produce (vacuity class 1).

    But the PROPERTY was producible even though the fixtures were not. Deleting
    the property along with the bad fixture left an operator-facing CSV column
    with no owner: an auditor filtering on `is_action_tx=1` gets whatever
    `pick_action_tx` happens to return, and nothing would notice a change.

    Re-pointed at `_real_lp_close_bundle` — built by `_build_sub_transactions`
    from real receipt logs — and driven through the REAL `_rows_to_csv`, which
    the replacement test never called.
    """

    @staticmethod
    def _csv_rows(make_row):  # type: ignore[no-untyped-def]
        import csv
        import io
        import json

        from almanak.framework.dashboard.pages.trade_tape import _rows_to_csv

        subs = TestActionPickIsDerivedFromTheRealWriter._real_lp_close_bundle()
        row = make_row(extracted_data_json=json.dumps({"sub_transactions": subs}))
        row.intent_type = "LP_CLOSE"
        text, _ = _rows_to_csv([row])
        return subs, list(csv.DictReader(io.StringIO(text)))

    def test_exactly_one_leg_is_flagged_as_the_action(self, make_row):
        _, rows = self._csv_rows(make_row)
        flagged = [r for r in rows if r["is_action_tx"] in ("1", "True", "true")]
        assert len(flagged) == 1, (
            f"expected exactly one action leg in the CSV, got {len(flagged)}: "
            f"{[r['tx_hash'] for r in flagged]}"
        )

    def test_the_csv_action_leg_is_the_one_the_headline_links_to(self, make_row):
        """The end-to-end property the deleted test owned."""
        from almanak.framework.dashboard.utils import pick_action_tx

        subs, rows = self._csv_rows(make_row)
        headline = pick_action_tx(subs, "LP_CLOSE")["tx_hash"]
        flagged = [r["tx_hash"] for r in rows if r["is_action_tx"] in ("1", "True", "true")]
        assert flagged == [headline], (
            f"CSV flags {flagged} as the action tx but the headline links to "
            f"{headline!r} — an auditor joining the two gets no match"
        )
