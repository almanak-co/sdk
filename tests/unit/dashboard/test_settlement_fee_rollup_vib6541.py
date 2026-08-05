"""Settlement-carried perp fees in the PnL roll-up — VIB-6541.

The defect, measured on a sealed AUDIT_CONFIRMED mainnet run
(``20260804-2310-gmxrt-vib6522-5513``, Arbitrum, ``gmx_v2_directional_perp``,
``deployment:926e7ed624d4``, tested SHA ``7246023835``): the dashboard's
"Strategy PnL" tile rendered **−$0.07** on a run that truly lost **−$0.52**.

The write side was already correct — ``accounting_events`` carried two
``PERP_SETTLEMENT`` rows measuring $0.4563 of keeper execution fee and $0.0060 of
position fee. The READ side never folded them: ``_net_realized_pnl_usd``
subtracted exactly one cost, gas, so 87% of the loss was invisible on the
headline while its evidence sat in the same DB the dashboard was reading.

VIB-6061 fixed this class on the Cost Stack and deliberately stopped there,
naming the residual it left behind rather than absorbing it silently. This is the
roll-up half.

THE FIXTURE IS THE RUN. ``vib6541_gmx_arb_mainnet.sqlite`` is the frozen bundle
DB from that audited run — real mainnet rows, not synthesised ones — so these
assertions are against measured economics and the pre-fix expectations below are
the numbers the sealed dashboard capture actually showed.
"""

from __future__ import annotations

import sqlite3
from decimal import Decimal
from pathlib import Path

from almanak.framework.dashboard.pages._detail_header import _net_realized_pnl_usd
from almanak.framework.dashboard.quant_aggregations import (
    CostStack,
    compute_cost_stack,
    compute_reconciliation,
)

_FIXTURE = (
    Path(__file__).resolve().parents[3]
    / "tests"
    / "fixtures"
    / "accounting"
    / "perp"
    / "vib6541_gmx_arb_mainnet.sqlite"
)

# Measured on the bundle, to the full precision the payloads carry.
_KEEPER_FEE = Decimal("0.45634482836671465088928")
_POSITION_FEE = Decimal("0.00599868491185265436")
_GAS = Decimal("0.07029694193701937431086")
_PERP_REALIZED = Decimal("0.004243951356245226952293017825")

# The tile, before and after. ``_TILE_PRE_FIX`` is the −$0.07 the sealed capture
# showed; it is asserted here as the NEGATIVE CONTROL — if a future change makes
# the post-fix value drift back toward it, that is the regression this file
# exists to catch.
_TILE_PRE_FIX = _PERP_REALIZED - _GAS
_TILE_POST_FIX = _TILE_PRE_FIX - _POSITION_FEE - _KEEPER_FEE

# The audit's independently re-derived wallet-method PnL for the same run
# (funded-in vs end-state at pinned blocks, finding.json A5). The roll-up must
# land on it, which is the property the ticket asks for and the only one that
# makes the tile trustworthy.
_AUDITED_WALLET_METHOD_PNL = Decimal("-0.5191")


def _bundle_rows() -> tuple[list[dict], list[dict]]:
    """``(ledger_rows, accounting_events)`` from the frozen mainnet bundle."""
    if not _FIXTURE.is_file():  # pragma: no cover - fixture is committed
        # FAIL, never skip. Every real-economics assertion in this module reads this
        # bundle, so a skip here would turn the whole regression suite green while
        # proving nothing — the precise shape of vacuous-pass this suite exists to
        # rule out. The fixture is committed alongside these tests; its absence is a
        # broken checkout or a bad merge, and it must be loud.
        raise AssertionError(f"missing committed bundle fixture, refusing to pass vacuously: {_FIXTURE}")
    conn = sqlite3.connect(f"file:{_FIXTURE}?mode=ro", uri=True)
    try:
        conn.row_factory = sqlite3.Row
        ledger = [dict(r) for r in conn.execute("SELECT * FROM transaction_ledger ORDER BY timestamp")]
        events = [dict(r) for r in conn.execute("SELECT * FROM accounting_events ORDER BY timestamp")]
    finally:
        conn.close()
    return ledger, events


def _bundle_cost_stack() -> CostStack:
    ledger, events = _bundle_rows()
    return compute_cost_stack(ledger, events)


class TestTheHeadlineNumber:
    """The tile itself — the thing a user reads and believes."""

    def test_the_pre_fix_formula_reproduces_the_wrong_number_from_the_sealed_capture(self):
        """NEGATIVE CONTROL — without the two new terms the tile is −$0.07.

        This is not a hypothetical. It is what the dashboard rendered on the
        audited run, and the blind dashboard-auditor flagged it as the headline
        divergence. Reproducing it here proves the fixture actually contains the
        defect, so a green result from the sibling test below cannot be a fixture
        that was never broken.
        """
        stack = _bundle_cost_stack()

        pre_fix = (
            stack.realized_pnl_usd
            + (stack.fees_earned_usd or Decimal("0"))
            + (stack.funding_earned_usd - stack.funding_paid_usd)
            + (stack.interest_earned_usd - stack.interest_paid_usd)
            - stack.gas_usd
        )

        assert pre_fix == _TILE_PRE_FIX
        assert round(pre_fix, 2) == Decimal("-0.07")
        # And it is WRONG by more than 7x against the audited truth.
        assert abs(pre_fix - _AUDITED_WALLET_METHOD_PNL) > Decimal("0.45")

    def test_the_roll_up_now_lands_on_the_audited_wallet_method_pnl(self):
        stack = _bundle_cost_stack()
        cost = _cost_stack_info_from(stack)

        net_realized = _net_realized_pnl_usd(cost)

        assert net_realized == _TILE_POST_FIX
        assert round(net_realized, 2) == Decimal("-0.53")
        # The whole point: the headline now agrees with the independently
        # re-derived wallet-method PnL. The residual is the price-impact /
        # native-lane dust the audit itself measured, an order of magnitude
        # under the omission this fix removes.
        assert abs(net_realized - _AUDITED_WALLET_METHOD_PNL) < Decimal("0.02")

    def test_the_move_is_exactly_the_two_settlement_costs(self):
        """No third term crept in — the delta is the fees and nothing else."""
        stack = _bundle_cost_stack()

        assert _TILE_PRE_FIX - _net_realized_pnl_usd(_cost_stack_info_from(stack)) == _KEEPER_FEE + _POSITION_FEE


class TestTheBucket:
    """``CostStack.perp_settlement_fee_usd`` — the new lane-scoped fee bucket."""

    def test_it_sums_the_settlement_carried_trading_fees(self):
        stack = _bundle_cost_stack()

        assert stack.perp_settlement_fee_usd == _POSITION_FEE
        assert stack.perp_settlement_fee_applicable is True
        assert stack.perp_settlement_fee_measured is True

    def test_a_strategy_with_no_perp_settlement_says_nothing_about_perp_fees(self):
        """Inapplicable, not zero — a swap / LP strategy must not carry this row."""
        stack = compute_cost_stack([], [])

        assert stack.perp_settlement_fee_applicable is False
        assert stack.perp_settlement_fee_any_measured is False

    def test_an_unmeasured_component_leaves_the_bucket_unmeasured(self):
        """Empty≠Zero on BOTH components.

        A settlement carrying a position fee but no borrowing fee is unmeasured
        in TOTAL. Booking the half we have would be a wrong number, not a smaller
        one, and it would silently understate the headline by the missing limb.
        """
        import json

        events = [
            {
                "event_type": "PERP_SETTLEMENT",
                "ledger_entry_id": "led-1",
                "payload_json": json.dumps(
                    {
                        "event_type": "PERP_SETTLEMENT",
                        "settlement_state": "EXECUTED",
                        "submission_ledger_entry_id": "led-1",
                        "position_fee_usd": "0.5",
                        # borrowing_fee_usd deliberately absent
                    }
                ),
            }
        ]

        stack = compute_cost_stack([], events)

        assert stack.perp_settlement_fee_usd == Decimal("0")
        assert stack.perp_settlement_fee_applicable is True
        assert stack.perp_settlement_fee_any_measured is False

    def test_a_cancelled_settlement_carrying_a_fill_fee_is_not_folded(self):
        """The lockstep gate, found by all three panel engines.

        A CANCELLED / FROZEN verdict DOES carry ``fill_data`` when the venue tx
        yielded measurable money movement — a collateral-returning
        ``PositionDecrease`` — and the GMX receipt parser populates
        ``position_fee_usd`` from exactly that event. So this payload shape is a
        designed branch, not a hypothetical.

        Ungated, the dashboard subtracted this fee from the headline while
        ``_cell_g6_reconciliation`` — the documented lockstep partner, which scopes
        the trading fee to EXECUTED — excluded it. The two sites then disagreed
        about real money. This pins the dashboard to G6's scoping.

        NEGATIVE CONTROL: delete the ``settlement_state != "EXECUTED"`` guard in
        ``_fold_perp_settlement_fee`` and this test fails with
        ``perp_settlement_fee_usd == Decimal("0.75")``.
        """
        import json

        events = [
            {
                "event_type": "PERP_SETTLEMENT",
                "ledger_entry_id": "led-cancelled",
                "payload_json": json.dumps(
                    {
                        "event_type": "PERP_SETTLEMENT",
                        "settlement_state": "CANCELLED",
                        "submission_ledger_entry_id": "led-cancelled",
                        # A collateral-returning PositionDecrease priced both limbs.
                        "position_fee_usd": "0.5",
                        "borrowing_fee_usd": "0.25",
                    }
                ),
            }
        ]

        stack = compute_cost_stack([], events)

        assert stack.perp_settlement_fee_usd == Decimal("0")
        # Not merely unmeasured — NOT APPLICABLE. A cancelled order owes no trading
        # fee, so reporting "applicable but unmeasured" would mark a cancelled-only
        # run partial when its coverage is in fact complete.
        assert stack.perp_settlement_fee_applicable is False
        assert stack.perp_settlement_fee_any_measured is False

    def test_a_cancelled_settlement_still_yields_its_keeper_fee(self):
        """The gate above is scoped to the TRADING fee only.

        The keeper consumed the execution-fee escrow whatever the order did, so
        VIB-6061's bucket must keep folding a cancelled settlement's keeper fee.
        Without this, the narrowing fix would silently delete real money — the
        failure mode that makes "tighten the guard" a regression rather than an
        improvement.
        """
        import json

        events = [
            {
                "event_type": "PERP_SETTLEMENT",
                "ledger_entry_id": "led-cancelled",
                "payload_json": json.dumps(
                    {
                        "event_type": "PERP_SETTLEMENT",
                        "settlement_state": "CANCELLED",
                        "submission_ledger_entry_id": "led-cancelled",
                        "keeper_execution_fee_usd": "0.2216",
                    }
                ),
            }
        ]

        stack = compute_cost_stack([], events)

        assert stack.venue_execution_fee_usd == Decimal("0.2216")
        assert stack.venue_execution_fee_any_measured is True

    def test_it_does_not_disturb_the_display_bucket(self):
        """``protocol_fees_usd`` stays the Cost Stack's rendered fee row.

        The two carry the same perp money on purpose (see ``CostStack``); this
        pins that adding the roll-up bucket did not move the displayed one, so
        the Cost Stack tile is byte-identical to VIB-6061's.
        """
        stack = _bundle_cost_stack()

        assert stack.protocol_fees_usd == _POSITION_FEE
        assert stack.protocol_fees_measured is True


class TestTheG6Fold:
    """``compute_reconciliation`` — the dashboard half of the G6 lockstep pair."""

    def test_the_settlement_costs_reach_sum_fees(self):
        ledger, events = _bundle_rows()
        stack = compute_cost_stack(ledger, events)

        recon = compute_reconciliation(
            initial_value_usd=Decimal("14.72236160521146466688"),
            nav_usd=Decimal("14.4660673221159025452913"),
            cost_stack=stack,
            accounting_events=events,
            deployment_id="deployment:926e7ed624d4",
        )

        # LP fee income is zero on a perp run, so Σ_fees is exactly the two costs,
        # negated (the bucket is a NET fee contribution to PnL).
        assert recon.sum_fees == -(_KEEPER_FEE + _POSITION_FEE)
        # And they did NOT land in gas: the gas bucket must stay reconcilable
        # against transaction_ledger's own gas sum, which is the first check a
        # user performs when a number surprises them.
        assert recon.sum_gas == -_GAS

    def test_the_component_sum_now_explains_the_run(self):
        """Pre-fix Σ component was −$0.06 against a −$0.52 run."""
        ledger, events = _bundle_rows()
        stack = compute_cost_stack(ledger, events)

        recon = compute_reconciliation(
            initial_value_usd=Decimal("14.72236160521146466688"),
            nav_usd=Decimal("14.4660673221159025452913"),
            cost_stack=stack,
            accounting_events=events,
            deployment_id="deployment:926e7ed624d4",
        )

        assert abs(recon.component_pnl_usd - _AUDITED_WALLET_METHOD_PNL) < Decimal("0.02")


class TestNonPerpStrategiesAreUntouched:
    """The fold must be invisible to every lane that has no perp settlement."""

    def test_an_empty_cost_stack_still_yields_the_legacy_formula(self):
        cost = _cost_stack_info_from(CostStack())

        assert _net_realized_pnl_usd(cost) == Decimal("0")

    def test_an_old_gateway_that_never_sends_the_field_contributes_zero(self):
        """``None`` is the wire's inapplicable/unmeasured state, and it must not
        move the headline — that is what keeps this change backwards-compatible
        against a gateway predating it."""
        from almanak.framework.dashboard.gateway_client import CostStackInfo

        cost = CostStackInfo(
            cost_gas_usd=Decimal("2"),
            cost_protocol_fees_usd=Decimal("9"),
            cost_slippage_usd=Decimal("9"),
            fees_earned_usd=Decimal("1"),
            interest_paid_usd=Decimal("0"),
            interest_earned_usd=Decimal("0"),
            funding_paid_usd=Decimal("0"),
            funding_earned_usd=Decimal("0"),
            realized_pnl_usd=Decimal("5"),
            il_usd=Decimal("0"),
            cost_venue_execution_fee_usd=None,
            cost_perp_settlement_fee_usd=None,
        )

        assert _net_realized_pnl_usd(cost) == Decimal("4")  # 5 + 1 - 2


class TestWireEncoding:
    """``cost_stack_to_proto`` — the only place this money crosses the gateway
    boundary, and the surface a reviewer flagged as carrying no direct coverage.

    The tests above round-trip through the wire on their way to the tile, so a
    total wire break would fail them; what they do NOT pin is the ENCODING itself
    — which of the two distinct ""-states the producer emits. Those two states
    are different facts (`inapplicable` vs `applicable but unmeasured`) and only
    the `partial` flag tells them apart, so they are asserted here directly.
    """

    @staticmethod
    def _settlement(state="EXECUTED", position="0.5", borrowing="0.25", link="led-1"):
        import json

        payload = {
            "event_type": "PERP_SETTLEMENT",
            "settlement_state": state,
            "submission_ledger_entry_id": link,
        }
        if position is not None:
            payload["position_fee_usd"] = position
        if borrowing is not None:
            payload["borrowing_fee_usd"] = borrowing
        return {
            "event_type": "PERP_SETTLEMENT",
            "ledger_entry_id": link,
            "payload_json": json.dumps(payload),
        }

    @staticmethod
    def _to_proto(stack):
        from almanak.gateway.services.dashboard_service import cost_stack_to_proto

        return cost_stack_to_proto(stack)

    def test_a_measured_fee_travels_with_no_partial_flag(self):
        proto = self._to_proto(compute_cost_stack([], [self._settlement()]))

        assert proto.cost_perp_settlement_fee_usd == "0.75"
        assert proto.perp_settlement_fee_partial is False

    def test_an_inapplicable_bucket_sends_the_empty_sentinel_and_no_flag(self):
        """A swap / LP strategy: no settlement at all. Empty≠Zero on the wire —
        sending "0" here would claim a measured zero cost that nobody measured."""
        proto = self._to_proto(compute_cost_stack([], []))

        assert proto.cost_perp_settlement_fee_usd == ""
        assert proto.perp_settlement_fee_partial is False

    def test_a_partial_measurement_sends_the_real_SUM_flagged_not_the_sentinel(self):
        """The producer gates the VALUE on ``any_measured``, never ``measured``.

        This is the one expression in the whole encode path with a shipped-loss
        history: a revision of this function once gated ``fees_earned_usd`` on the
        all-or-nothing ``fees_earned_measured`` meter and silently deleted real
        measured fee income from the Strategy PnL headline, with the entire unit
        suite green because nothing reached the expression. The VIB-6541 comment
        beside it says the same thing in the present tense about THIS field.

        So it must be pinned by a scenario where the two meters DISAGREE — one
        measured settlement and one missing a component. Every other test in this
        class has ``measured == any_measured``, which makes them all blind here:
        swapping ``any_measured`` for ``measured`` on line 722 of
        ``dashboard_service.py`` leaves them green and deletes $0.75 of real cost.

        Mirrors ``test_a_partial_measurement_sends_the_real_sum_flagged`` in the
        VIB-6061 sibling, which pins exactly this for the keeper-fee bucket. The
        two buckets are documented as behaving identically on this field, so
        asymmetric coverage of it is how they drift apart.
        """
        stack = compute_cost_stack(
            [],
            [self._settlement(), self._settlement(link="led-2", borrowing=None)],
        )
        proto = self._to_proto(stack)

        # The measured half SURVIVES — sending "" here would delete real cost.
        assert proto.cost_perp_settlement_fee_usd == "0.75"
        assert proto.perp_settlement_fee_partial is True

    def test_applicable_but_unmeasured_sends_empty_AND_raises_partial(self):
        """The two ""-states are different facts and must be distinguishable."""
        stack = compute_cost_stack([], [self._settlement(borrowing=None)])
        proto = self._to_proto(stack)

        assert proto.cost_perp_settlement_fee_usd == ""
        assert proto.perp_settlement_fee_partial is True

    def test_a_cancelled_settlement_is_inapplicable_on_the_wire_not_partial(self):
        """The EXECUTED gate, carried through one more layer to the wire.

        A cancelled order owes no trading fee, so its bucket is INAPPLICABLE —
        "" with `partial=False`. Were the gate removed, this row would instead
        encode a measured "0.75" and the tile would subtract a cost the
        Accountant's G6 excludes.

        Honest about what it adds: it has no UNIQUE mutation kill. Removing the
        gate is already caught in-process by
        ``test_a_cancelled_settlement_carrying_a_fill_fee_is_not_folded``, and the
        partial-flag half is already caught by the inapplicable test above. This
        exercises the same guard through the encode path rather than covering
        anything the class would otherwise miss — cheap and correct, but not
        load-bearing on its own.
        """
        stack = compute_cost_stack([], [self._settlement(state="CANCELLED")])
        proto = self._to_proto(stack)

        assert proto.cost_perp_settlement_fee_usd == ""
        assert proto.perp_settlement_fee_partial is False

    def test_the_client_reads_the_sentinel_back_as_none_never_zero(self):
        """Closing the loop: "" must decode to ``None``, not ``Decimal("0")``.
        A fabricated zero here would be indistinguishable from a measured
        zero-cost run, which is the whole point of the sentinel."""
        from almanak.framework.dashboard.gateway_client import _convert_cost_stack

        info = _convert_cost_stack(self._to_proto(compute_cost_stack([], [])))

        assert info.cost_perp_settlement_fee_usd is None
        assert info.perp_settlement_fee_partial is False


def _cost_stack_info_from(stack: CostStack):
    """Round the aggregator's ``CostStack`` through the wire into the client's
    ``CostStackInfo``, so these tests exercise the SAME encode/decode the tile
    does rather than a hand-built stand-in that could disagree with it."""
    from almanak.framework.dashboard.gateway_client import _convert_cost_stack
    from almanak.gateway.services.dashboard_service import cost_stack_to_proto

    return _convert_cost_stack(cost_stack_to_proto(stack))
