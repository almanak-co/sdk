"""Tests for the Compound V3 full lending lifecycle strategy on Base.

Regression coverage for VIB-3586: the strategy must emit a *standalone*
SUPPLY intent for the collateral leg before the BORROW intent, rather than
bundling the collateral into ``Intent.borrow(collateral_amount>0)``. Bundling
collapses the supply into the single BORROW accounting event (one
``transaction_ledger`` row -> one ``accounting_events`` row), silently
dropping the SUPPLY ``accounting_events`` row and its ``supply:`` FIFO lot.

Validates:
1. State machine transitions
   (idle -> supplying -> supplied -> borrowing -> borrowed -> repaying ->
    repaid -> withdrawing -> complete)
2. SUPPLY is emitted first as its own intent; BORROW carries
   ``collateral_amount == 0`` (collateral already supplied)
3. Intent parameters (market/collateral/borrow)
4. Lifecycle callbacks advance the state machine and track amounts
5. Failure recovery (revert to previous stable state)
6. State persistence and restoration
7. Teardown support
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def strategy():
    """Instantiate the strategy with mock internals (no runner / gateway)."""
    from strategies.incubating.compound_v3_lifecycle.strategy import (
        CompoundV3LifecycleStrategy,
    )

    strat = CompoundV3LifecycleStrategy.__new__(CompoundV3LifecycleStrategy)
    strat._chain = "base"
    strat._wallet_address = "0x" + "0" * 40
    strat._deployment_id = "test-compound-lifecycle-base"
    strat.STRATEGY_NAME = "compound_v3_lifecycle"

    # Config (mirrors config.json)
    strat.collateral_token = "WETH"
    strat.collateral_amount = Decimal("0.01")
    strat.borrow_token = "USDC"
    strat.ltv_target = Decimal("0.3")
    strat.market = "usdc"

    # State
    strat._loop_state = "idle"
    strat._previous_stable_state = "idle"
    strat._collateral_supplied = Decimal("0")
    strat._borrowed_amount = Decimal("0")

    return strat


def _mock_market(weth_price: str = "1700", usdc_price: str = "1") -> MagicMock:
    """Mock MarketSnapshot with WETH/USDC prices."""
    market = MagicMock()

    def price_fn(symbol):
        prices = {"WETH": Decimal(weth_price), "USDC": Decimal(usdc_price)}
        if symbol in prices:
            return prices[symbol]
        raise ValueError(f"Unknown token: {symbol}")

    market.price = MagicMock(side_effect=price_fn)
    return market


# -------------------------------------------------------------------------
# State machine transitions
# -------------------------------------------------------------------------


class TestCompoundV3LifecycleStateMachine:
    def test_idle_emits_supply_intent(self, strategy):
        """VIB-3586: idle emits SUPPLY first, not a bundled BORROW."""
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent is not None
        assert intent.intent_type.value == "SUPPLY"
        assert strategy._loop_state == "supplying"

    def test_supplied_emits_borrow_intent(self, strategy):
        strategy._loop_state = "supplied"
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "BORROW"
        assert strategy._loop_state == "borrowing"

    def test_borrowed_emits_repay_intent(self, strategy):
        strategy._loop_state = "borrowed"
        strategy._borrowed_amount = Decimal("5.1")
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "REPAY"
        assert strategy._loop_state == "repaying"

    def test_repaid_emits_withdraw_intent(self, strategy):
        strategy._loop_state = "repaid"
        strategy._collateral_supplied = Decimal("0.01")
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "WITHDRAW"
        assert strategy._loop_state == "withdrawing"

    def test_complete_emits_hold(self, strategy):
        strategy._loop_state = "complete"
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "complete" in intent.reason.lower()

    def test_stuck_in_supplying_reverts(self, strategy):
        strategy._loop_state = "supplying"
        strategy._previous_stable_state = "idle"
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert strategy._loop_state == "idle"

    def test_stuck_in_borrowing_reverts(self, strategy):
        strategy._loop_state = "borrowing"
        strategy._previous_stable_state = "supplied"
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert strategy._loop_state == "supplied"

    def test_price_unavailable_returns_hold(self, strategy):
        market = MagicMock()
        market.price = MagicMock(side_effect=ValueError("no price"))
        # IDLE does not need prices -> SUPPLY proceeds even during a price outage.
        assert strategy.decide(market).intent_type.value == "SUPPLY"
        # Only the BORROW step needs prices -> HOLD on outage in the SUPPLIED state.
        strategy._loop_state = "supplied"
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "unavailable" in intent.reason.lower()

    def test_full_supply_then_borrow_sequence(self, strategy):
        """Drive idle -> SUPPLY -> (success) -> BORROW end to end.

        This is the VIB-3586 contract: two distinct intents, each of which
        becomes its own accounting event, instead of one bundled BORROW.
        """
        market = _mock_market()

        supply_intent = strategy.decide(market)
        assert supply_intent.intent_type.value == "SUPPLY"
        strategy.on_intent_executed(supply_intent, success=True, result=None)
        assert strategy._loop_state == "supplied"

        borrow_intent = strategy.decide(market)
        assert borrow_intent.intent_type.value == "BORROW"
        # Collateral already supplied -> the borrow must NOT re-bundle it.
        assert borrow_intent.collateral_amount == Decimal("0")

    def test_emitted_intents_persist_distinct_supply_and_borrow_events(self, strategy):
        """VIB-3586 accounting-seam regression (coderabbit Major).

        The decision-layer tests above prove the strategy emits two distinct
        intents. This test closes the loop at the *accounting seam* that was
        the actual bug: each emitted intent, when drained through the lending
        category handler, must produce its own typed accounting event
        (one ``transaction_ledger`` row -> one ``accounting_events`` row).
        The pre-fix bundled BORROW collapsed the supply into a single BORROW
        event and the SUPPLY row was never written — this asserts both a
        SUPPLY and a BORROW ``LendingAccountingEvent`` are produced, in order.

        Routing through ``handle_lending`` (the function ``AccountingProcessor``
        calls per ledger row) keeps the seam honest without mocking the whole
        processor/DB; the heavier per-branch handler coverage lives in
        ``tests/unit/framework/accounting/test_lending_accounting.py``.
        """
        import json

        from almanak.framework.accounting.basis import FIFOBasisStore
        from almanak.framework.accounting.category_handlers.lending_handler import (
            handle_lending,
        )
        from almanak.framework.accounting.models import LendingEventType

        market = _mock_market()

        # 1) Strategy emits SUPPLY, then (after success) BORROW — the real intents.
        supply_intent = strategy.decide(market)
        strategy.on_intent_executed(supply_intent, success=True, result=None)
        borrow_intent = strategy.decide(market)

        assert supply_intent.intent_type.value == "SUPPLY"
        assert borrow_intent.intent_type.value == "BORROW"

        # 2) Build the ledger/outbox row pair each intent produces, using the
        #    extracted_data keys the Compound V3 enricher actually emits
        #    (SUPPLY -> supply_collateral_amount, BORROW -> borrow_amount).
        def _rows(intent_type, extracted, token_in):
            led_id = f"led-{intent_type.lower()}"
            outbox = {
                "id": f"ob-{intent_type.lower()}",
                "ledger_entry_id": led_id,
                "deployment_id": "dep-test",
                "cycle_id": "cycle-1",
                "intent_type": intent_type,
                "wallet_address": "0xwallet",
                "position_key": "lending:base:compound_v3:0xwallet:usdc",
                "market_id": "usdc",
            }
            ledger = {
                "id": led_id,
                "deployment_id": "dep-test",
                "cycle_id": "cycle-1",
                "execution_mode": "live",
                "timestamp": "2026-06-14T00:00:00+00:00",
                "intent_type": intent_type,
                "token_in": token_in,
                "gas_usd": "0.01",
                "tx_hash": f"0x{intent_type.lower()}",
                "chain": "base",
                "protocol": "compound_v3",
                "success": True,
                "error": "",
                "extracted_data_json": json.dumps(extracted),
                "price_inputs_json": json.dumps({"WETH": "1700.0", "USDC": "1.0"}),
                "pre_state_json": "",
                "post_state_json": "",
            }
            return outbox, ledger

        # WETH 0.01 (18 decimals) collateral supply; USDC 5.10 (6 decimals) borrow.
        supply_outbox, supply_ledger = _rows(
            "SUPPLY", {"supply_collateral_amount": 10_000_000_000_000_000}, "WETH"
        )
        borrow_outbox, borrow_ledger = _rows("BORROW", {"borrow_amount": 5_100_000}, "USDC")

        basis = FIFOBasisStore()  # shared across the lifecycle, as in the processor

        def _resolver_for(decimals):
            info = MagicMock()
            info.decimals = decimals
            r = MagicMock()
            r.resolve.return_value = info
            return r

        from unittest.mock import patch

        with patch(
            "almanak.framework.data.tokens.resolver.get_token_resolver",
            return_value=_resolver_for(18),
        ):
            supply_event = handle_lending(supply_outbox, supply_ledger, basis)
        with patch(
            "almanak.framework.data.tokens.resolver.get_token_resolver",
            return_value=_resolver_for(6),
        ):
            borrow_event = handle_lending(borrow_outbox, borrow_ledger, basis)

        # 3) Both intents persist as distinct, correctly-typed events — in order.
        assert supply_event is not None, "SUPPLY intent produced no accounting event"
        assert borrow_event is not None, "BORROW intent produced no accounting event"
        assert supply_event.event_type == LendingEventType.SUPPLY
        assert borrow_event.event_type == LendingEventType.BORROW
        assert supply_event.asset == "WETH"
        assert borrow_event.asset == "USDC"
        assert supply_event.amount_token == Decimal("0.01")

        # 4) Prove the SUPPLY leg actually SEEDED the supply: FIFO lot — the gap
        #    the bundled-BORROW form silently left empty (coderabbit Major).
        #    Asserting amount_token alone would pass even if the lot write
        #    regressed, so check the basis store directly AND prove the lot is
        #    consumable by draining a matching WITHDRAW through the same basis:
        #    the closing WITHDRAW FIFO-matches the supplied 0.01 WETH and reports
        #    it as principal (principal_delta_usd ≈ 0.01 * $1700 = $17), which is
        #    only possible because the supply lot exists.
        # The FIFO key folds the asset to lower-case.
        supply_key = "dep-test:supply:lending:base:compound_v3:0xwallet:usdc:weth"
        assert supply_key in basis._lots, "SUPPLY did not seed the supply: FIFO lot"
        assert basis._lots[supply_key][0]["remaining"] == Decimal("0.01")

        withdraw_outbox, withdraw_ledger = _rows(
            "WITHDRAW", {"withdraw_collateral_amount": 10_000_000_000_000_000}, "WETH"
        )
        with patch(
            "almanak.framework.data.tokens.resolver.get_token_resolver",
            return_value=_resolver_for(18),
        ):
            withdraw_event = handle_lending(withdraw_outbox, withdraw_ledger, basis)

        assert withdraw_event is not None
        assert withdraw_event.event_type == LendingEventType.WITHDRAW
        # FIFO-matched the full supplied principal (0.01 WETH @ $1700 = $17).
        assert withdraw_event.principal_delta_usd == Decimal("17.00")
        # Lot fully consumed by the matching withdraw.
        assert basis._lots[supply_key][0]["remaining"] == Decimal("0")


# -------------------------------------------------------------------------
# Intent content
# -------------------------------------------------------------------------


class TestCompoundV3LifecycleIntents:
    def test_supply_intent_params(self, strategy):
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.protocol == "compound_v3"
        assert intent.token == "WETH"
        assert intent.amount == Decimal("0.01")
        assert intent.use_as_collateral is True
        assert intent.market_id == "usdc"
        assert intent.chain == "base"

    def test_borrow_intent_does_not_rebundle_collateral(self, strategy):
        """VIB-3586 core regression: BORROW carries collateral_amount == 0."""
        strategy._loop_state = "supplied"
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.protocol == "compound_v3"
        assert intent.market_id == "usdc"
        assert intent.collateral_token == "WETH"
        assert intent.borrow_token == "USDC"
        assert intent.collateral_amount == Decimal("0")

    def test_borrow_amount_respects_ltv(self, strategy):
        # WETH=$1700, USDC=$1, collateral=0.01
        # value = 0.01 * 1700 = 17
        # borrow = 17 * 0.3 / 1 = 5.10
        strategy._loop_state = "supplied"
        market = _mock_market("1700", "1")
        intent = strategy.decide(market)
        assert intent.borrow_amount == Decimal("5.10")

    def test_repay_intent_uses_repay_full(self, strategy):
        strategy._loop_state = "borrowed"
        strategy._borrowed_amount = Decimal("5.1")
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.repay_full is True
        assert intent.token == "USDC"
        assert intent.market_id == "usdc"

    def test_withdraw_intent_uses_withdraw_all(self, strategy):
        strategy._loop_state = "repaid"
        strategy._collateral_supplied = Decimal("0.01")
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.withdraw_all is True
        assert intent.token == "WETH"
        assert intent.market_id == "usdc"


# -------------------------------------------------------------------------
# Lifecycle callbacks
# -------------------------------------------------------------------------


class TestCompoundV3LifecycleCallbacks:
    def test_supply_success_advances_state(self, strategy):
        strategy._loop_state = "supplying"
        intent = MagicMock()
        intent.intent_type.value = "SUPPLY"
        # on_intent_executed records the amount actually supplied by the
        # executed intent, not the config value.
        intent.amount = Decimal("0.01")
        strategy.on_intent_executed(intent, success=True, result=None)
        assert strategy._loop_state == "supplied"
        assert strategy._collateral_supplied == Decimal("0.01")

    def test_supply_success_tracks_executed_amount_not_config(self, strategy):
        """VIB-3586 / gemini: _collateral_supplied comes from the executed
        intent's amount, so a config drift mid-flight does not corrupt it."""
        strategy._loop_state = "supplying"
        strategy.collateral_amount = Decimal("0.99")  # config drifts after emit
        intent = MagicMock()
        intent.intent_type.value = "SUPPLY"
        intent.amount = Decimal("0.01")  # what actually executed
        strategy.on_intent_executed(intent, success=True, result=None)
        assert strategy._collateral_supplied == Decimal("0.01")

    def test_borrow_success_advances_state(self, strategy):
        strategy._loop_state = "borrowing"
        strategy._collateral_supplied = Decimal("0.01")
        intent = MagicMock()
        intent.intent_type.value = "BORROW"
        intent.borrow_amount = Decimal("5.1")
        strategy.on_intent_executed(intent, success=True, result=None)
        assert strategy._loop_state == "borrowed"
        # collateral was already tracked by the SUPPLY leg
        assert strategy._collateral_supplied == Decimal("0.01")
        assert strategy._borrowed_amount == Decimal("5.1")

    def test_repay_success_advances_state(self, strategy):
        strategy._loop_state = "repaying"
        strategy._borrowed_amount = Decimal("5.1")
        intent = MagicMock()
        intent.intent_type.value = "REPAY"
        strategy.on_intent_executed(intent, success=True, result=None)
        assert strategy._loop_state == "repaid"
        assert strategy._borrowed_amount == Decimal("0")

    def test_withdraw_success_completes(self, strategy):
        strategy._loop_state = "withdrawing"
        strategy._collateral_supplied = Decimal("0.01")
        intent = MagicMock()
        intent.intent_type.value = "WITHDRAW"
        strategy.on_intent_executed(intent, success=True, result=None)
        assert strategy._loop_state == "complete"
        assert strategy._collateral_supplied == Decimal("0")

    def test_supply_failure_reverts_state(self, strategy):
        strategy._loop_state = "supplying"
        strategy._previous_stable_state = "idle"
        intent = MagicMock()
        intent.intent_type.value = "SUPPLY"
        strategy.on_intent_executed(intent, success=False, result=None)
        assert strategy._loop_state == "idle"

    def test_borrow_failure_reverts_state(self, strategy):
        strategy._loop_state = "borrowing"
        strategy._previous_stable_state = "supplied"
        intent = MagicMock()
        intent.intent_type.value = "BORROW"
        strategy.on_intent_executed(intent, success=False, result=None)
        assert strategy._loop_state == "supplied"


# -------------------------------------------------------------------------
# State persistence
# -------------------------------------------------------------------------


class TestCompoundV3LifecyclePersistence:
    def test_get_persistent_state(self, strategy):
        strategy._loop_state = "borrowed"
        strategy._previous_stable_state = "supplied"
        strategy._collateral_supplied = Decimal("0.01")
        strategy._borrowed_amount = Decimal("5.1")

        state = strategy.get_persistent_state()
        assert state["loop_state"] == "borrowed"
        assert state["previous_stable_state"] == "supplied"
        assert state["collateral_supplied"] == "0.01"
        assert state["borrowed_amount"] == "5.1"

    def test_load_persistent_state(self, strategy):
        state = {
            "loop_state": "supplied",
            "previous_stable_state": "supplying",
            "collateral_supplied": "0.01",
            "borrowed_amount": "0",
        }
        strategy.load_persistent_state(state)
        assert strategy._loop_state == "supplied"
        assert strategy._previous_stable_state == "supplying"
        assert strategy._collateral_supplied == Decimal("0.01")
        assert strategy._borrowed_amount == Decimal("0")


# -------------------------------------------------------------------------
# Teardown
# -------------------------------------------------------------------------


class TestCompoundV3LifecycleTeardown:
    def test_no_positions_when_idle(self, strategy):
        summary = strategy.get_open_positions()
        assert len(summary.positions) == 0

    def test_positions_when_borrowed(self, strategy):
        # Mock the snapshot so valuation logic is exercised rather than the
        # get_open_positions() try/except fallback (collateral_price = 0).
        strategy.create_market_snapshot = MagicMock(return_value=_mock_market("1700", "1"))
        strategy._collateral_supplied = Decimal("0.01")
        strategy._borrowed_amount = Decimal("5.1")
        summary = strategy.get_open_positions()
        assert len(summary.positions) == 2
        types = {p.position_type.value for p in summary.positions}
        assert "SUPPLY" in types
        assert "BORROW" in types
        # 0.01 WETH * $1700 = $17.00 — proves valuation ran (not the fallback).
        supply = next(p for p in summary.positions if p.position_type.value == "SUPPLY")
        assert supply.value_usd == Decimal("17.00")

    def test_teardown_intents_repay_then_withdraw(self, strategy):
        from almanak.framework.teardown import TeardownMode

        strategy._collateral_supplied = Decimal("0.01")
        strategy._borrowed_amount = Decimal("5.1")

        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 2
        assert intents[0].intent_type.value == "REPAY"
        assert intents[1].intent_type.value == "WITHDRAW"

    def test_teardown_only_withdraw_when_no_debt(self, strategy):
        from almanak.framework.teardown import TeardownMode

        strategy._collateral_supplied = Decimal("0.01")
        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 1
        assert intents[0].intent_type.value == "WITHDRAW"
