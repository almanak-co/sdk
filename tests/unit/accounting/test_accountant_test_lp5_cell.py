"""Unit tests for the LP5 decomposition cell (VIB-4263) in
``almanak.framework.accounting.accountant_test``.

Background — VIB-4263: LP5 ("LP open→close delta decomposition") was an
UNCONDITIONAL XFAIL with no PASS branch, so the cell could never validate
what it claims even once the upstream attribution (VIB-3954, computed by
``attribute_lp`` → ``run_attribution_on_close``) lands. This test pins the
new conditional-XFAIL shape: PASS when a CLOSE position_event's
``attribution_json`` carries the LP decomposition (mirrors LP2 / LP6);
XFAIL with the original diagnostic otherwise.
"""

from __future__ import annotations

import json
from typing import Any

from almanak.framework.accounting.accountant_test import (
    _LP5_REQUIRED_FIELDS,
    _lp5_decomposition_cell,
)


def _populated_attribution(**overrides: Any) -> dict[str, Any]:
    """A CLOSE attribution_json shaped like ``attribute_lp`` output —
    LP marker + the four required USD legs (string Decimals)."""
    decomp = {
        "version": 4,
        "position_type": "LP",
        "principal_deposited_usd": "1000.00",
        "principal_recovered_usd": "1012.50",
        "price_pnl_usd": "8.30",
        "net_pnl_usd": "12.50",
        # extra legs attribute_lp also emits — irrelevant to the predicate
        "impermanent_loss_usd": "-1.20",
        "fee_pnl_usd": "5.40",
    }
    decomp.update(overrides)
    return decomp


def _close_row(attribution_json: str, rid: str = "pe-close") -> dict[str, Any]:
    return {"id": rid, "event_type": "CLOSE", "attribution_json": attribution_json}


def _open_row(rid: str = "pe-open") -> dict[str, Any]:
    return {"id": rid, "event_type": "OPEN", "attribution_json": "{}"}


class TestLp5DecompositionCell:
    def test_populated_attribution_passes(self) -> None:
        """A CLOSE event whose attribution_json carries the full LP
        decomposition → PASS."""
        rows = [_open_row(), _close_row(json.dumps(_populated_attribution()))]
        cell = _lp5_decomposition_cell(rows)
        assert cell.cell_id == "LP5"
        assert cell.status == "PASS"

    def test_empty_attribution_xfails_with_original_message(self) -> None:
        """The frozen-fixture state: CLOSE attribution_json is ``{}`` (VIB-3954
        not yet computed) → XFAIL, NOT a silent pass, and the original
        diagnostic is preserved so the ratchet floor is unchanged."""
        rows = [_open_row(), _close_row("{}")]
        cell = _lp5_decomposition_cell(rows)
        assert cell.cell_id == "LP5"
        assert cell.status == "XFAIL"
        assert cell.diagnostic == "attribution_json LP decomposition not yet computed"

    def test_no_close_event_xfails(self) -> None:
        """No CLOSE position_event at all → XFAIL (nothing to decompose)."""
        cell = _lp5_decomposition_cell([_open_row()])
        assert cell.status == "XFAIL"

    def test_empty_string_leg_treated_as_not_computed(self) -> None:
        """Empty != zero: a leg present but empty-string means the value was
        not computed → XFAIL, never PASS on a half-filled decomposition."""
        rows = [_close_row(json.dumps(_populated_attribution(net_pnl_usd="")))]
        cell = _lp5_decomposition_cell(rows)
        assert cell.status == "XFAIL"

    def test_missing_required_leg_xfails(self) -> None:
        """A decomposition missing one required leg entirely → XFAIL."""
        decomp = _populated_attribution()
        del decomp["price_pnl_usd"]
        rows = [_close_row(json.dumps(decomp))]
        cell = _lp5_decomposition_cell(rows)
        assert cell.status == "XFAIL"

    def test_non_lp_position_type_xfails(self) -> None:
        """A CLOSE whose decomposition is not position_type==LP (e.g. a perp
        close) must not satisfy LP5."""
        decomp = _populated_attribution(position_type="PERP")
        rows = [_close_row(json.dumps(decomp))]
        cell = _lp5_decomposition_cell(rows)
        assert cell.status == "XFAIL"

    def test_measured_zero_legs_pass(self) -> None:
        """A genuine all-zero round-trip (``"0"`` legs) IS computed data —
        Empty != zero — so it PASSes."""
        decomp = _populated_attribution(
            principal_deposited_usd="0",
            principal_recovered_usd="0",
            price_pnl_usd="0",
            net_pnl_usd="0",
        )
        rows = [_close_row(json.dumps(decomp))]
        cell = _lp5_decomposition_cell(rows)
        assert cell.status == "PASS"

    def test_required_fields_present_in_real_attribute_lp_output(self) -> None:
        """Guard (real, not a tautology): every field LP5 requires must
        actually appear in ``attribute_lp``'s output. This exercises the real
        producer with a minimal valid OPEN/CLOSE round-trip, so a rename of
        any leg in ``attribute_lp`` breaks this test — which would otherwise
        let LP5 silently XFAIL forever against a frozen hardcoded set.

        Minimal valid input: ``attribute_lp`` reads ``value_usd`` off both
        events to derive principal deposited/recovered and the PnL legs; an
        OPEN at $1000 and a CLOSE at $1012.50 is enough to produce the full
        decomposition.
        """
        from almanak.framework.observability.pnl_attributor import attribute_lp

        open_event = {"event_type": "OPEN", "value_usd": "1000.00"}
        close_event = {"event_type": "CLOSE", "value_usd": "1012.50"}
        output = attribute_lp(open_event, close_event)

        # The producer is an LP decomposition, and every LP5-required leg is
        # really there (not a hardcoded mirror of the constant).
        assert output.get("position_type") == "LP"
        missing = [f for f in _LP5_REQUIRED_FIELDS if f not in output]
        assert not missing, f"attribute_lp output missing LP5-required fields: {missing}"

        # Keep the explicit set too — it documents the intended contract and
        # makes a drift obvious in review even before the producer check runs.
        assert set(_LP5_REQUIRED_FIELDS) == {
            "net_pnl_usd",
            "principal_deposited_usd",
            "principal_recovered_usd",
            "price_pnl_usd",
        }
