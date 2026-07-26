"""VIB-3872 WI-4 — dashboard CONSUMES the PERP_SETTLEMENT measured economics.

**CONTRACT CHANGE from WI-3.** WI-3 locked in that a ``PERP_SETTLEMENT`` row was
SILENTLY IGNORED by the ``quant_aggregations`` folds (forward-compatibility, no
crash). WI-4 changes that contract: the measured settlement economics
(realized_pnl_usd, position/funding/borrowing fees) are now FOLDED into the perp
story, and — critically — SUPERSEDE the ESTIMATED PERP_OPEN/PERP_CLOSE submission
event for the same submission ledger link so the two are never double-counted.

These tests assert the new consumption + the precedence (supersede, no double-count)
+ Empty≠Zero + timeline labeling. The ``_open_position_cost_basis`` fold is
intentionally NOT changed (its position_key join is a different scheme — out of
scope), so it still ignores settlement.

**Supersession is PER COMPONENT, measured-only (CodeRabbit MAJOR fix).** An EXECUTED
settlement does NOT skip the whole estimate fold — for each economic field the
settlement's MEASURED value wins, and an unmeasured settlement field falls back to the
estimate's value (never a fabricated zero). Each field is folded from exactly ONE
source, so there is still no double-count.
"""

from __future__ import annotations

import json
from decimal import Decimal

from almanak.framework.dashboard.data_source import _convert_event_type
from almanak.framework.dashboard.models import TimelineEventType
from almanak.framework.dashboard.quant_aggregations import compute_cost_stack, compute_reconciliation


def _row(payload: dict, *, ledger_entry_id: str) -> dict:
    return {
        "event_type": payload["event_type"],
        "payload_json": json.dumps(payload),
        "ledger_entry_id": ledger_entry_id,
    }


def _perp_open(*, ledger_entry_id: str = "l-open", open_fee: str | None = None) -> dict:
    p = {
        "event_type": "PERP_OPEN",
        "position_key": "perp:arbitrum:gmx_v2:w:ETH",
        "protocol": "gmx_v2",
        "size": "2000",
        "confidence": "HIGH",
    }
    if open_fee is not None:
        p["open_fee_usd"] = open_fee
    return _row(p, ledger_entry_id=ledger_entry_id)


def _perp_close(*, ledger_entry_id: str = "l-close", realized_pnl: str = "-100", close_fee: str | None = None) -> dict:
    p = {
        "event_type": "PERP_CLOSE",
        "position_key": "perp:arbitrum:gmx_v2:w:ETH",
        "protocol": "gmx_v2",
        "size": "2000",
        "realized_pnl_usd": realized_pnl,
        "confidence": "HIGH",
    }
    if close_fee is not None:
        p["close_fee_usd"] = close_fee
    return _row(p, ledger_entry_id=ledger_entry_id)


def _settlement(
    *,
    submission_ledger_entry_id: str = "l-close",
    ledger_entry_id: str = "l-close",
    state: str = "EXECUTED",
    realized_pnl: str | None = "-30.96",
    position_fee: str | None = "2.87",
    borrowing_fee: str | None = "0",
    funding_fee: str | None = "2.40",
    price_impact: str | None = "-1.5",
) -> dict:
    p = {
        "event_type": "PERP_SETTLEMENT",
        "protocol": "gmx_v2",
        "position_key": "0x" + "cd" * 32,
        "submission_ledger_entry_id": submission_ledger_entry_id,
        "order_key": "0x" + "ab" * 32,
        "settlement_state": state,
        "is_open": False,
        "realized_pnl_usd": realized_pnl,
        "position_fee_usd": position_fee,
        "borrowing_fee_usd": borrowing_fee,
        "funding_fee_usd": funding_fee,
        "price_impact_usd": price_impact,
        "confidence": "HIGH" if state == "EXECUTED" else "UNAVAILABLE",
    }
    if state != "EXECUTED":
        p["unavailable_reason"] = f"state={state}"
    return _row(p, ledger_entry_id=ledger_entry_id)


class TestCostStackConsumesSettlement:
    def test_settlement_supersedes_close_no_double_count(self) -> None:
        # Close ESTIMATE says -100; settlement MEASURES -30.96. The fold must use the
        # settlement (supersede), never fold both (-130.96) nor keep the estimate.
        events = [_perp_close(realized_pnl="-100"), _settlement(realized_pnl="-30.96")]
        cs = compute_cost_stack([], events)
        assert cs.realized_pnl_usd == Decimal("-30.96")

    def test_settlement_funding_and_fees_fold_in(self) -> None:
        # Only the settlement present (close superseded): measured fee + funding fold.
        events = [_perp_close(), _settlement(position_fee="2.87", borrowing_fee="0.13", funding_fee="2.40")]
        cs = compute_cost_stack([], events)
        assert cs.protocol_fees_usd == Decimal("3.00")  # 2.87 + 0.13
        assert cs.funding_paid_usd == Decimal("2.40")  # positive funding = paid
        assert cs.funding_earned_usd == Decimal("0")

    def test_negative_funding_is_received(self) -> None:
        events = [_perp_close(), _settlement(funding_fee="-1.25")]
        cs = compute_cost_stack([], events)
        assert cs.funding_earned_usd == Decimal("1.25")
        assert cs.funding_paid_usd == Decimal("0")

    def test_fee_measured_when_only_settlement(self) -> None:
        # No PERP_OPEN to poison the meter; settlement fee fully measured.
        cs = compute_cost_stack([], [_perp_close(), _settlement(position_fee="2.0", borrowing_fee="0")])
        assert cs.protocol_fees_measured is True
        assert cs.protocol_fees_usd == Decimal("2.0")

    def test_borrowing_none_makes_fee_unmeasured(self) -> None:
        # Empty≠Zero: an unmeasured borrowing component leaves the fee bucket unmeasured.
        cs = compute_cost_stack([], [_perp_close(), _settlement(position_fee="2.0", borrowing_fee=None)])
        assert cs.protocol_fees_measured is False

    def test_non_executed_settlement_does_not_supersede(self) -> None:
        # A CANCELLED settlement carries no measured fill → the estimate stands.
        events = [_perp_close(realized_pnl="-100"), _settlement(state="CANCELLED", realized_pnl=None)]
        cs = compute_cost_stack([], events)
        assert cs.realized_pnl_usd == Decimal("-100")  # estimate NOT superseded

    def test_executed_settlement_none_pnl_falls_back_to_estimate(self) -> None:
        # MAJOR fix: an EXECUTED settlement whose realized_pnl_usd is UNMEASURED must
        # NOT replace the measured -100 estimate with a fabricated 0 — it falls back
        # to the estimate PER COMPONENT (Empty≠Zero). Mutation-resistant: reverting to
        # the event-level skip folds 0 here and this fails.
        events = [_perp_close(realized_pnl="-100"), _settlement(realized_pnl=None)]
        cs = compute_cost_stack([], events)
        assert cs.realized_pnl_usd == Decimal("-100")

    def test_executed_settlement_none_funding_falls_back_to_estimate(self) -> None:
        # Funding is a component too: settlement funding None → estimate funding wins,
        # not a fabricated 0. The close estimate carries measured funding_paid=3.
        close = _perp_close(realized_pnl="-10")
        cp = json.loads(close["payload_json"])
        cp["funding_paid_usd"] = "3"
        close["payload_json"] = json.dumps(cp)
        events = [close, _settlement(realized_pnl="-10", funding_fee=None)]
        cs = compute_cost_stack([], events)
        assert cs.funding_paid_usd == Decimal("3")  # estimate funding, NOT 0

    def test_incomplete_settlement_fee_falls_back_to_estimate(self) -> None:
        # Settlement fee is partially unmeasured (borrowing None) → the estimate's
        # measured close_fee stands in rather than a settlement partial that silently
        # drops the borrowing component. The None borrowing NEVER zeroes the bucket.
        events = [_perp_close(close_fee="1.5"), _settlement(position_fee="2.0", borrowing_fee=None)]
        cs = compute_cost_stack([], events)
        assert cs.protocol_fees_measured is True
        assert cs.protocol_fees_usd == Decimal("1.5")  # measured estimate, not a fabricated 0


class TestOpenSideSupersession:
    def test_settlement_supersedes_open_fee_no_double_count(self) -> None:
        # FIX-4: the OPEN leg supersedes too. Open ESTIMATE fee 1.0; the linked open
        # settlement MEASURES 0.5 (position 0.5 + borrowing 0). The fold must use 0.5
        # (settlement), never 1.0 (estimate) nor 1.5 (both). Uses _perp_open — the
        # previously-untested open-side branch.
        events = [
            _perp_open(ledger_entry_id="l-open", open_fee="1.0"),
            _settlement(
                submission_ledger_entry_id="l-open",
                ledger_entry_id="l-open",
                position_fee="0.5",
                borrowing_fee="0",
                price_impact="-0.3",
                realized_pnl=None,
                funding_fee=None,
            ),
        ]
        cs = compute_cost_stack([], events)
        assert cs.protocol_fees_usd == Decimal("0.5")
        assert cs.protocol_fees_measured is True
        assert cs.slippage_usd == Decimal("-0.3")

    def test_open_settlement_none_fee_falls_back_to_estimate(self) -> None:
        # Open settlement fee unmeasured (borrowing None) → fall back to the estimate's
        # measured open_fee, never a fabricated 0.
        events = [
            _perp_open(ledger_entry_id="l-open", open_fee="1.0"),
            _settlement(
                submission_ledger_entry_id="l-open",
                ledger_entry_id="l-open",
                position_fee="0.5",
                borrowing_fee=None,
                realized_pnl=None,
                funding_fee=None,
            ),
        ]
        cs = compute_cost_stack([], events)
        assert cs.protocol_fees_usd == Decimal("1.0")  # estimate open_fee, not 0
        assert cs.protocol_fees_measured is True


class TestReconciliationConsumesSettlement:
    def test_sum_perp_uses_settlement_not_estimate(self) -> None:
        events = [_perp_close(realized_pnl="-100"), _settlement(realized_pnl="-30.96", funding_fee="2.40")]
        cs = compute_cost_stack([], events)
        r = compute_reconciliation(Decimal("1000"), Decimal("970"), cs, events)
        assert r.sum_perp == Decimal("-30.96")  # measured, not the -100 estimate
        assert r.sum_funding == Decimal("-2.40")  # paid funding reduces PnL

    def test_non_executed_reconciliation_keeps_estimate(self) -> None:
        events = [_perp_close(realized_pnl="-100"), _settlement(state="FROZEN", realized_pnl=None)]
        cs = compute_cost_stack([], events)
        r = compute_reconciliation(Decimal("1000"), Decimal("970"), cs, events)
        assert r.sum_perp == Decimal("-100")

    def test_executed_settlement_none_pnl_reconciliation_falls_back(self) -> None:
        # MAJOR fix (reconciliation lane): EXECUTED settlement, realized_pnl UNMEASURED
        # → sum_perp uses the -100 estimate, NOT a fabricated 0.
        events = [_perp_close(realized_pnl="-100"), _settlement(realized_pnl=None, funding_fee=None)]
        cs = compute_cost_stack([], events)
        r = compute_reconciliation(Decimal("1000"), Decimal("970"), cs, events)
        assert r.sum_perp == Decimal("-100")


class TestTimelineLabelsSettlement:
    def test_perp_settlement_has_its_own_label(self) -> None:
        assert _convert_event_type("PERP_SETTLEMENT") is TimelineEventType.PERP_SETTLEMENT
        # And is no longer mislabeled as a generic trade.
        assert _convert_event_type("PERP_SETTLEMENT") is not TimelineEventType.TRADE

    def test_perp_open_close_labeled(self) -> None:
        assert _convert_event_type("PERP_OPEN") is TimelineEventType.PERP_OPEN
        assert _convert_event_type("PERP_CLOSE") is TimelineEventType.PERP_CLOSE
