"""Venue execution fee in the Cost Stack — VIB-6061.

The defect: the Cost Stack counted transaction gas only, so the GMX keeper
execution fee — the dominant native cost of a small perp order — appeared in no
bucket at all. Not in Gas, not in Fees (permanently "unmeasured"), not in
Slippage. These tests pin the new bucket AND the Empty-vs-Zero states around it,
because rendering an unmeasured venue fee as "$0.00" would assert exactly the
absence this ticket removes.
"""

from __future__ import annotations

import json
from decimal import Decimal

import pytest

from almanak.framework.dashboard.quant_aggregations import compute_cost_stack


def _settlement(
    *,
    ledger_id: str = "led-1",
    state: str = "EXECUTED",
    keeper_fee_usd: str | None = "0.2216",
    order_key: str = "0xabc",
) -> dict:
    payload: dict = {
        "event_type": "PERP_SETTLEMENT",
        "settlement_state": state,
        "submission_ledger_entry_id": ledger_id,
        "order_key": order_key,
    }
    if state != "EXECUTED":
        payload["unavailable_reason"] = "order cancelled by keeper"
    if keeper_fee_usd is not None:
        payload["keeper_execution_fee_usd"] = keeper_fee_usd
        payload["keeper_execution_fee_wei"] = 117808422544000
        payload["execution_fee_refund_wei"] = 39858593456000
    return {
        "event_type": "PERP_SETTLEMENT",
        "ledger_entry_id": ledger_id,
        "payload_json": json.dumps(payload),
    }


def _perp_open(ledger_id: str = "led-1") -> dict:
    return {
        "event_type": "PERP_OPEN",
        "ledger_entry_id": ledger_id,
        "payload_json": json.dumps({"event_type": "PERP_OPEN", "size": "4.000"}),
    }


def test_the_keeper_fee_lands_in_its_own_bucket():
    stack = compute_cost_stack([], [_perp_open(), _settlement()])

    assert stack.venue_execution_fee_usd == Decimal("0.2216")
    assert stack.venue_execution_fee_measured is True
    assert stack.venue_execution_fee_applicable is True


def test_the_keeper_fee_is_not_folded_into_gas():
    """Its own row, not summed into Gas.

    Gas must stay reconcilable against the ledger's own gas_usd sum — that is the
    check a user performs when a cost surprises them, and it is how the original
    $0.1388 was confirmed against four ledger rows.
    """
    stack = compute_cost_stack([], [_perp_open(), _settlement()])

    assert stack.gas_usd == Decimal("0")


def test_the_keeper_fee_is_not_folded_into_protocol_fees_or_slippage():
    stack = compute_cost_stack([], [_perp_open(), _settlement()])

    assert stack.protocol_fees_usd == Decimal("0")
    assert stack.slippage_usd == Decimal("0")


def test_fees_across_several_settlements_accumulate():
    events = [
        _perp_open("led-1"),
        _settlement(ledger_id="led-1", order_key="0xa"),
        _perp_open("led-2"),
        _settlement(ledger_id="led-2", order_key="0xb", keeper_fee_usd="0.2251"),
    ]

    stack = compute_cost_stack([], events)

    assert stack.venue_execution_fee_usd == Decimal("0.4467")


def test_a_settlement_carrying_no_fee_is_unmeasured_not_zero():
    """Empty != Zero — the whole point of the ticket.

    A perp strategy whose settlements predate this change has an APPLICABLE venue
    fee that was never measured. Reporting Decimal("0") as measured would restate
    "we did not look" as "there was no fee", which is the original defect wearing
    a new bucket.
    """
    stack = compute_cost_stack([], [_perp_open(), _settlement(keeper_fee_usd=None)])

    assert stack.venue_execution_fee_applicable is True
    assert stack.venue_execution_fee_any_measured is False
    assert stack.venue_execution_fee_measured is False


def test_a_partial_measurement_still_carries_the_real_sum():
    """A measured fee must travel even when a sibling settlement withheld one.

    Dropping it would delete real cost — the exact failure mode recorded in
    ``cost_stack_to_proto``'s docstring for the LP fee bucket.
    """
    events = [
        _perp_open("led-1"),
        _settlement(ledger_id="led-1", order_key="0xa"),
        _perp_open("led-2"),
        _settlement(ledger_id="led-2", order_key="0xb", keeper_fee_usd=None),
    ]

    stack = compute_cost_stack([], events)

    assert stack.venue_execution_fee_usd == Decimal("0.2216")
    assert stack.venue_execution_fee_any_measured is True
    assert stack.venue_execution_fee_measured is False  # partial


def test_a_strategy_with_no_perp_settlement_is_inapplicable():
    """A swap / LP strategy must say NOTHING about venue fees."""
    swap = {
        "event_type": "SWAP",
        "payload_json": json.dumps({"event_type": "SWAP", "slippage_usd": "0.01"}),
    }

    stack = compute_cost_stack([], [swap])

    assert stack.venue_execution_fee_applicable is False
    assert stack.venue_execution_fee_any_measured is False


def test_an_orphan_settlement_still_contributes_its_fee():
    """The fee exists on the settlement row alone, so the orphan gate must not drop it.

    The economics folds skip a settlement whose submission estimate is present (to
    avoid double-counting). The keeper fee appears on NO other row, so applying
    that gate to it would silently lose the common case.
    """
    orphan = compute_cost_stack([], [_settlement(ledger_id="orphan")])
    paired = compute_cost_stack([], [_perp_open(), _settlement()])

    assert orphan.venue_execution_fee_usd == Decimal("0.2216")
    assert paired.venue_execution_fee_usd == Decimal("0.2216")


def test_a_non_executed_settlement_is_applicable_but_carries_no_fee():
    stack = compute_cost_stack([], [_settlement(state="CANCELLED", keeper_fee_usd=None)])

    assert stack.venue_execution_fee_applicable is True
    assert stack.venue_execution_fee_any_measured is False


def test_the_fee_does_not_reach_the_g6_reconciliation_fold():
    """Scope guard for the deferred limb (trigger d).

    G6 is the drawdown-reachable portfolio fold; widening it is a separate change
    under its own real-fork proof. This test exists so that widening is a DELIBERATE
    act with a failing test attached, not something that happens by accident because
    someone summed the dataclass.
    """
    from almanak.framework.dashboard.quant_aggregations import compute_reconciliation

    events = [_perp_open(), _settlement()]
    stack = compute_cost_stack([], events)

    recon = compute_reconciliation(
        initial_value_usd=Decimal("100"),
        nav_usd=Decimal("100"),
        cost_stack=stack,
        accounting_events=events,
        snapshot_initial=None,
        snapshot_final=None,
        deployment_id="deployment:test",
    )

    assert stack.venue_execution_fee_usd == Decimal("0.2216")
    assert recon.sum_gas == Decimal("0")  # gas only, fee excluded


class TestWireEncoding:
    """``cost_stack_to_proto`` — the only place this money crosses the boundary."""

    @staticmethod
    def _to_proto(stack):
        from almanak.gateway.services.dashboard_service import cost_stack_to_proto

        return cost_stack_to_proto(stack)

    def test_a_measured_fee_travels_with_no_partial_flag(self):
        stack = compute_cost_stack([], [_perp_open(), _settlement()])

        proto = self._to_proto(stack)

        assert proto.cost_venue_execution_fee_usd == "0.2216"
        assert proto.venue_execution_fee_partial is False

    def test_an_inapplicable_bucket_sends_the_empty_sentinel_and_no_flag(self):
        stack = compute_cost_stack([], [])

        proto = self._to_proto(stack)

        assert proto.cost_venue_execution_fee_usd == ""
        assert proto.venue_execution_fee_partial is False

    def test_applicable_but_unmeasured_sends_empty_AND_raises_partial(self):
        """The two ""-states are different facts and must be distinguishable."""
        stack = compute_cost_stack([], [_perp_open(), _settlement(keeper_fee_usd=None)])

        proto = self._to_proto(stack)

        assert proto.cost_venue_execution_fee_usd == ""
        assert proto.venue_execution_fee_partial is True

    def test_a_partial_measurement_sends_the_real_sum_flagged(self):
        events = [
            _perp_open("led-1"),
            _settlement(ledger_id="led-1", order_key="0xa"),
            _perp_open("led-2"),
            _settlement(ledger_id="led-2", order_key="0xb", keeper_fee_usd=None),
        ]
        stack = compute_cost_stack([], events)

        proto = self._to_proto(stack)

        assert proto.cost_venue_execution_fee_usd == "0.2216"
        assert proto.venue_execution_fee_partial is True

    @pytest.mark.parametrize(
        ("wire_value", "expected"),
        [("0.2216", Decimal("0.2216")), ("", None)],
    )
    def test_the_client_reads_the_sentinel_back_as_none_never_zero(self, wire_value, expected):
        from almanak.framework.dashboard.gateway_client import _convert_cost_stack
        from almanak.gateway.proto import gateway_pb2

        proto = gateway_pb2.CostStackInfo(cost_venue_execution_fee_usd=wire_value)

        assert _convert_cost_stack(proto).cost_venue_execution_fee_usd == expected
