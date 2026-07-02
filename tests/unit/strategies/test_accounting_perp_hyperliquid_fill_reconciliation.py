"""Fill-vs-submission reconciliation for the Hyperliquid accounting fixture (VIB-5597).

CoreWriter settles async — a PERP_OPEN submission returning status 1 does NOT
prove a fill. The fixture must not book an OPEN position (nor drive teardown to
close one) until the fill is OBSERVED on HyperCore; a booked-but-unfilled
position would corrupt the Accountant Test's ground truth.

Construction mirrors ``test_accounting_looping_hardening.py``: skip
``IntentStrategy.__init__`` and inject only the attributes the code reads.
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from typing import Any

from almanak.connectors.hyperliquid.fill_reconciliation import FillStatus
from strategies.accounting.perp_hyperliquid.strategy import (
    PHASE_INIT,
    PHASE_OPEN,
    PHASE_PENDING_FILL,
    AccountingQuantPerpHyperliquidStrategy,
)


def _bare_strategy(**overrides: Any) -> AccountingQuantPerpHyperliquidStrategy:
    obj = AccountingQuantPerpHyperliquidStrategy.__new__(AccountingQuantPerpHyperliquidStrategy)
    obj.protocol = "hyperliquid"
    obj.market = "ETH/USD"
    obj.collateral_token = "USDC"
    obj.collateral_amount = Decimal("5")
    obj.leverage = Decimal("2")
    obj.is_long = True
    obj.max_slippage_pct = Decimal("2")
    obj._chain = "hyperevm"
    obj._deployment_id = "deployment:test"
    obj._phase = PHASE_INIT
    obj._previous_stable_phase = PHASE_INIT
    obj._position_size_usd = Decimal("0")
    obj._initial_balance_usd = Decimal("100")
    obj._initial_balance_token = Decimal("100")
    for key, value in overrides.items():
        setattr(obj, key, value)
    return obj


def _open_intent() -> SimpleNamespace:
    return SimpleNamespace(intent_type=SimpleNamespace(value="PERP_OPEN"))


class TestSubmissionIsPending:
    def test_open_submission_enters_pending_not_open(self):
        strat = _bare_strategy(_position_size_usd=Decimal("10"))
        strat.on_intent_executed(_open_intent(), success=True, result=None)
        # VIB-5597: submission does NOT advance to OPEN — it enters PENDING_FILL.
        assert strat._phase == PHASE_PENDING_FILL
        # previous_stable_phase stays at INIT so a reject cleanly reverts.
        assert strat._previous_stable_phase == PHASE_INIT

    def test_pending_phase_holds(self):
        strat = _bare_strategy(_phase=PHASE_PENDING_FILL)
        market = SimpleNamespace()
        # decide() reads initial snapshot first; pre-seed it to skip that path.
        strat._initial_balance_usd = Decimal("100")
        intent = strat.decide(market)
        assert intent.intent_type.value == "HOLD"


class TestReconcileFill:
    def test_confirmed_fill_advances_to_open(self):
        strat = _bare_strategy(_phase=PHASE_PENDING_FILL, _position_size_usd=Decimal("10"))
        strat.reconcile_fill("PERP_OPEN", FillStatus.FILLED)
        assert strat._phase == PHASE_OPEN
        assert strat._previous_stable_phase == PHASE_OPEN

    def test_partial_fill_advances_to_open(self):
        strat = _bare_strategy(_phase=PHASE_PENDING_FILL, _position_size_usd=Decimal("10"))
        strat.reconcile_fill("PERP_OPEN", FillStatus.PARTIALLY_FILLED)
        assert strat._phase == PHASE_OPEN

    def test_rejected_reverts_to_init_no_position(self):
        strat = _bare_strategy(_phase=PHASE_PENDING_FILL, _position_size_usd=Decimal("10"))
        strat.reconcile_fill("PERP_OPEN", FillStatus.REJECTED)
        assert strat._phase == PHASE_INIT
        assert strat._position_size_usd == Decimal("0")

    def test_unmeasured_stays_pending(self):
        strat = _bare_strategy(_phase=PHASE_PENDING_FILL, _position_size_usd=Decimal("10"))
        strat.reconcile_fill("PERP_OPEN", FillStatus.UNMEASURED)
        assert strat._phase == PHASE_PENDING_FILL
        assert strat._position_size_usd == Decimal("10")

    def test_reconcile_noop_when_not_pending(self):
        strat = _bare_strategy(_phase=PHASE_OPEN, _position_size_usd=Decimal("10"))
        strat.reconcile_fill("PERP_OPEN", FillStatus.REJECTED)
        # Already OPEN — a spurious reconcile must not tear it down.
        assert strat._phase == PHASE_OPEN


class TestTeardownCoversPending:
    def test_pending_position_is_surfaced_for_teardown(self):
        strat = _bare_strategy(_phase=PHASE_PENDING_FILL, _position_size_usd=Decimal("10"))
        summary = strat.get_open_positions()
        assert len(summary.positions) == 1
        assert summary.positions[0].details["fill_confirmed"] is False

    def test_pending_position_generates_teardown_close(self):
        from almanak.framework.teardown import TeardownMode

        strat = _bare_strategy(_phase=PHASE_PENDING_FILL, _position_size_usd=Decimal("10"))
        intents = strat.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 1
        assert intents[0].intent_type.value == "PERP_CLOSE"
        assert intents[0].size_usd is None  # full reduce-only close

    def test_open_position_still_covered(self):
        strat = _bare_strategy(_phase=PHASE_OPEN, _position_size_usd=Decimal("10"))
        summary = strat.get_open_positions()
        assert len(summary.positions) == 1
        assert summary.positions[0].details["fill_confirmed"] is True


class TestPersistence:
    def test_pending_phase_survives_restart(self):
        strat = _bare_strategy(_phase=PHASE_PENDING_FILL, _position_size_usd=Decimal("10"))
        state = strat.get_persistent_state()
        restored = _bare_strategy()
        restored.load_persistent_state(state)
        # A restart mid-PENDING stays PENDING — the fill must be re-observed and
        # teardown must still cover the possibly-filled order (fail-safe).
        assert restored._phase == PHASE_PENDING_FILL
