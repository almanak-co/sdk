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

    @pytest.fixture
    def make_row(self) -> object:
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
