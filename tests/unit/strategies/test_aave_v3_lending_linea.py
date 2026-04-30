"""Regression tests for VIB-3745 (BUG-52) — Linea Aave teardown crash.

QA April29 Batch 17: ``aave_v3_lending_linea`` crashed during teardown with
``'AaveV3LendingLineaStrategy' object has no attribute 'state'``. The strategy
read ``self.state.get(...)`` in ``get_open_positions`` and
``generate_teardown_intents`` but never initialized that attribute. The fix
reads from the existing ``self._supplied_amount`` / ``self._borrowed_amount``
instance attributes (which are populated by ``load_persistent_state``).

These tests pin the contract:

* ``get_open_positions`` and ``generate_teardown_intents`` do not raise
  ``AttributeError`` when ``self.state`` does not exist.
* They report the position values from the framework-restored instance
  attributes.
"""

from __future__ import annotations

from decimal import Decimal

from strategies.incubating.aave_v3_lending_linea.strategy import (
    AaveV3LendingLineaStrategy,
)


def _make_strategy() -> AaveV3LendingLineaStrategy:
    return AaveV3LendingLineaStrategy(
        config={
            "chain": "linea",
            "wallet_address": "0x" + "aa" * 20,
        },
        chain="linea",
        wallet_address="0x" + "aa" * 20,
    )


class TestLineaTeardownDoesNotRequireSelfState:
    """The strategy must not depend on a ``self.state`` dict that is never set."""

    def test_get_open_positions_no_position_no_self_state_attr(self):
        s = _make_strategy()
        assert not hasattr(s, "state"), "self.state must not be auto-set"

        # Pre-fix: this raised AttributeError on self.state.get(...).
        summary = s.get_open_positions()
        assert summary.positions == []

    def test_generate_teardown_intents_no_position_no_self_state_attr(self):
        s = _make_strategy()
        assert not hasattr(s, "state")

        intents = s.generate_teardown_intents(mode="graceful")
        assert intents == []

    def test_get_open_positions_reports_supply_and_borrow_from_instance_attrs(self):
        s = _make_strategy()
        # load_persistent_state restores these in the real flow; simulate that.
        s._supplied_amount = Decimal("0.5")
        s._borrowed_amount = Decimal("100")

        summary = s.get_open_positions()

        assert len(summary.positions) == 2
        # Order: supply first, then borrow (matches strategy's emit order).
        supply, borrow = summary.positions
        assert str(supply.position_type).endswith("SUPPLY")
        assert supply.protocol == "aave_v3"
        assert supply.chain == "linea"
        assert supply.value_usd == Decimal("0.5")
        assert str(borrow.position_type).endswith("BORROW")
        assert borrow.value_usd == Decimal("100")

    def test_generate_teardown_intents_emits_repay_then_withdraw(self):
        s = _make_strategy()
        s._supplied_amount = Decimal("0.5")
        s._borrowed_amount = Decimal("100")

        intents = s.generate_teardown_intents(mode="graceful")

        # Order matters: must repay debt before withdrawing collateral.
        assert len(intents) == 2
        assert intents[0].intent_type.value == "REPAY"
        assert intents[1].intent_type.value == "WITHDRAW"

    def test_generate_teardown_intents_skips_unset_legs(self):
        # Borrow only (no supply) — the conditional branches must work
        # independently. (Unrealistic state, but it pins the conditionals.)
        s = _make_strategy()
        s._supplied_amount = Decimal("0")
        s._borrowed_amount = Decimal("50")
        intents = s.generate_teardown_intents(mode="graceful")
        assert len(intents) == 1
        assert intents[0].intent_type.value == "REPAY"

        # Supply only.
        s._supplied_amount = Decimal("0.25")
        s._borrowed_amount = Decimal("0")
        intents = s.generate_teardown_intents(mode="graceful")
        assert len(intents) == 1
        assert intents[0].intent_type.value == "WITHDRAW"
