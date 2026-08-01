"""Partial LP fee coverage must not delete measured money (VIB-6283, round 4).

The defect this pins: an earlier revision gated the WIRE VALUE of
``fees_earned_usd`` on the all-or-nothing ``fees_earned_measured`` meter. A
strategy with two LP closes — one carrying ``fees_total_usd``, one not — sent
``""`` for the whole bucket, the client mapped it to ``None``, and
``_net_realized_pnl_usd``'s ``or Decimal("0")`` booked genuinely measured fee
income as zero. The Strategy PnL headline understated by the full amount, with
no unmeasured marker on that tile.

It is not a corner case: ``quant_aggregations.CostStack`` documents that 18 of
66 ``LP_CLOSE`` rows in the 2026-06/07 corpus carry no ``fees_total_usd``, and
folding mid-life ``LP_COLLECT_FEES`` (this PR's own headline improvement)
*raises* the applicable-event count, making partial coverage MORE likely.

The fix separates two questions that were folded onto one string:

* ``fees_earned_usd is None``  -> the bucket does not APPLY (no close/collect yet)
* ``fees_earned_partial``      -> it applies, but some event withheld its term

Money is carried whenever the bucket applies; coverage travels beside it. The
flag is spelled PARTIAL, not MEASURED, so proto3's ``false`` default is the
harmless reading — an old gateway that omits it keeps exactly its pre-VIB-6283
behaviour instead of captioning every deployment as unmeasured.
"""

from __future__ import annotations

import json
from decimal import Decimal

from almanak.framework.dashboard.gateway_client import CostStackInfo, _convert_cost_stack
from almanak.framework.dashboard.pages._detail_header import _net_realized_pnl_usd
from almanak.framework.dashboard.quant_aggregations import compute_cost_stack
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.dashboard_service import cost_stack_to_proto


def _event(event_type: str, payload: dict) -> dict:
    return {
        "event_type": event_type,
        "payload_json": json.dumps(payload),
        "timestamp": "2026-01-01T00:00:00+00:00",
    }


def _wire(cs) -> tuple[str, bool]:
    """Serialise through the REAL gateway path.

    ``cost_stack_to_proto`` is the function the servicer calls. Re-deriving the
    expression here instead would leave the shipped line untested — which is
    precisely why the original defect survived a full unit suite and a
    17-mutation harness: nothing could reach it.
    """
    proto = cost_stack_to_proto(cs)
    return proto.fees_earned_usd, proto.fees_earned_partial


# ── the money assertion ──────────────────────────────────────────────────────


def test_partial_fee_coverage_still_carries_the_measured_sum() -> None:
    """Two closes, one without a fee term: the $10 that WAS measured survives."""
    stack = compute_cost_stack(
        [],
        [
            _event("LP_CLOSE", {"fees_total_usd": "10", "realized_pnl_usd": "5", "il_usd": "-1"}),
            _event("LP_CLOSE", {"realized_pnl_usd": "3"}),
        ],
    )

    assert stack.fees_earned_usd == Decimal("10")
    assert stack.fees_earned_applicable is True, "an LP close happened; the bucket applies"
    assert stack.fees_earned_any_measured is True, "one close DID supply a fee term"
    assert stack.fees_earned_measured is False, "the other close supplied none"

    value, partial = _wire(stack)
    assert value == "10", "the measured $10 must reach the client, flagged — never dropped"
    assert partial is True


def test_headline_includes_partially_measured_fees() -> None:
    """The bug's blast radius: the Strategy PnL headline itself.

    Before the fix this returned ``5`` (realized only); the $10 of real fee
    income was laundered to zero by ``or Decimal("0")``.
    """
    cost = CostStackInfo(
        cost_gas_usd=Decimal("0"),
        cost_protocol_fees_usd=None,
        cost_slippage_usd=None,
        fees_earned_usd=Decimal("10"),
        interest_paid_usd=Decimal("0"),
        interest_earned_usd=Decimal("0"),
        funding_paid_usd=Decimal("0"),
        funding_earned_usd=Decimal("0"),
        realized_pnl_usd=Decimal("5"),
        il_usd=None,
        fees_earned_partial=True,
    )
    assert _net_realized_pnl_usd(cost) == Decimal("15")


def test_inapplicable_fee_bucket_is_still_a_zero_contribution() -> None:
    """``None`` remains a true zero contribution — no LP events, no fee income.

    This is the half of the old comment that WAS right, and it must not
    regress into fabricating a number.
    """
    cost = CostStackInfo(
        cost_gas_usd=Decimal("1"),
        cost_protocol_fees_usd=None,
        cost_slippage_usd=None,
        fees_earned_usd=None,
        interest_paid_usd=Decimal("0"),
        interest_earned_usd=Decimal("0"),
        funding_paid_usd=Decimal("0"),
        funding_earned_usd=Decimal("0"),
        realized_pnl_usd=Decimal("5"),
        il_usd=None,
        fees_earned_partial=False,
    )
    assert _net_realized_pnl_usd(cost) == Decimal("4")


# ── the two states either side of the partial case ───────────────────────────


def test_no_lp_events_leaves_the_bucket_inapplicable() -> None:
    """A perp / swap / lending strategy: "—", not "$0.00", and no caveat."""
    stack = compute_cost_stack([], [_event("SWAP", {"realized_pnl_usd": "1"})])

    assert stack.fees_earned_applicable is False
    assert stack.fees_earned_any_measured is False
    value, partial = _wire(stack)
    assert value == "", "inapplicable stays the ''-sentinel so the tile renders an em-dash"
    assert partial is False, (
        "not-applicable is already said by the ''-sentinel; raising PARTIAL too would "
        "caption every perp / lending / swap strategy with an LP caveat forever"
    )


def test_full_fee_coverage_is_reported_measured() -> None:
    stack = compute_cost_stack(
        [],
        [
            _event("LP_CLOSE", {"fees_total_usd": "10", "realized_pnl_usd": "5"}),
            _event("LP_CLOSE", {"fees_total_usd": "4", "realized_pnl_usd": "3"}),
        ],
    )
    value, partial = _wire(stack)
    assert (value, partial) == ("14", False)


def test_il_bucket_follows_the_same_rule() -> None:
    """IL is diagnostic, not in net PnL — but the same partial drop applied.

    Fixing only the fee half would leave the identical defect one field over,
    which is how this PR already shipped a one-of-two-entrypoints gate once.
    """
    stack = compute_cost_stack(
        [],
        [
            _event("LP_CLOSE", {"fees_total_usd": "1", "il_usd": "-7", "realized_pnl_usd": "0"}),
            _event("LP_CLOSE", {"fees_total_usd": "1", "realized_pnl_usd": "0"}),
        ],
    )
    assert stack.il_usd == Decimal("-7")
    assert stack.il_applicable is True
    assert stack.il_measured is False
    proto = cost_stack_to_proto(stack)
    assert proto.il_usd == "-7", "the measured IL survives a partially-covered bucket"
    assert proto.il_partial is True


# ── the wire round trip, over the REAL proto message ─────────────────────────


def test_proto_round_trip_preserves_value_and_coverage_independently() -> None:
    """Built from ``gateway_pb2`` rather than a stand-in.

    A hand-rolled stub carrying whatever attributes the accessor happens to
    read is exactly how this PR shipped an inert gate earlier; the point of
    this test is that the generated message really has both new fields.
    """
    proto = gateway_pb2.CostStackInfo(
        cost_gas_usd="1",
        fees_earned_usd="10",
        fees_earned_partial=True,
        il_usd="-7",
        il_partial=True,
        interest_paid_usd="0",
        interest_earned_usd="0",
        funding_paid_usd="0",
        funding_earned_usd="0",
        realized_pnl_usd="5",
    )
    converted = _convert_cost_stack(proto)

    assert converted.fees_earned_usd == Decimal("10")
    assert converted.fees_earned_partial is True
    assert converted.il_usd == Decimal("-7")
    assert converted.il_partial is True
    assert _net_realized_pnl_usd(converted) == Decimal("14")


def test_old_gateway_without_the_flags_keeps_its_money() -> None:
    """Additive-optional, like ``age_days_exact``.

    An old gateway sends the value and leaves the bools at proto3's ``false``.
    The safe direction is to keep the money and over-caveat, never to discard a
    number because a flag we invented is absent.
    """
    proto = gateway_pb2.CostStackInfo(
        cost_gas_usd="0",
        fees_earned_usd="10",
        interest_paid_usd="0",
        interest_earned_usd="0",
        funding_paid_usd="0",
        funding_earned_usd="0",
        realized_pnl_usd="5",
    )
    converted = _convert_cost_stack(proto)
    assert converted.fees_earned_usd == Decimal("10")
    assert _net_realized_pnl_usd(converted) == Decimal("15")


# ── the caveat is applicability-aware, not value-aware ───────────────────────


def test_earn_caveat_only_fires_when_applicable_but_incomplete(monkeypatch) -> None:
    """Non-LP strategies must not be captioned "LP fees unmeasured" forever.

    Value is not applicability — the same distinction the interest leg already
    respected, applied to the fee leg. Asserted against what
    ``render_cost_stack`` actually emits, not against a re-derivation of the
    branch, so the test fails if the rendered caption stops matching.
    """
    from almanak.framework.dashboard.pages import _detail_header

    def _render(cost: CostStackInfo) -> str:
        captured: list[str] = []
        monkeypatch.setattr(
            _detail_header.st, "markdown", lambda html, **kw: captured.append(str(html))
        )
        _detail_header.render_cost_stack(cost)
        return "".join(captured)

    base = dict(
        cost_gas_usd=Decimal("0"),
        cost_protocol_fees_usd=None,
        cost_slippage_usd=None,
        interest_paid_usd=Decimal("0"),
        interest_earned_usd=Decimal("0"),
        funding_paid_usd=Decimal("0"),
        funding_earned_usd=Decimal("0"),
        realized_pnl_usd=Decimal("0"),
        il_usd=None,
    )

    # PARTIAL with a value: show the money, caption it as incomplete.
    partial_html = _render(
        CostStackInfo(**base, fees_earned_usd=Decimal("10"), fees_earned_partial=True)
    )
    assert "partially measured" in partial_html
    assert "Earn +$10.00" in partial_html, "the partial sum is shown, not suppressed"

    # An LP leg exists but NOTHING was measured: "unmeasured", never "$0.00".
    unmeasured_html = _render(CostStackInfo(**base, fees_earned_usd=None, fees_earned_partial=True))
    assert "LP fees unmeasured" in unmeasured_html
    assert "partially measured" not in unmeasured_html

    # No LP leg at all: say nothing about LP fees.
    inapplicable_html = _render(
        CostStackInfo(**base, fees_earned_usd=None, fees_earned_partial=False)
    )
    assert "(LP fees unmeasured)" not in inapplicable_html, (
        "a perp / lending / swap strategy has no LP fee bucket to caveat"
    )
    assert "(LP fees partially measured)" not in inapplicable_html

    full_html = _render(
        CostStackInfo(**base, fees_earned_usd=Decimal("10"), fees_earned_partial=False)
    )
    assert "(LP fees unmeasured)" not in full_html
    assert "(LP fees partially measured)" not in full_html
