"""Unit tests for the consolidated chain-generic Morpho Blue lending strategy.

Covers the lifecycle state machine, intent generation, on-execution advancement,
and teardown for ``MorphoBlueLifecycleStrategy``
(``strategies/incubating/morpho_blue_lifecycle``), driven by per-chain config.
Establishes CI coverage for the family (the three former per-chain clones shipped
no committed unit test); proves one class drives every chain via config.
"""

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

_MARKETS = {
    "base": "0x13c42741a359ac4a8aa8287d2be109dcf28344484f91185f9a79bd5a805a55ae",
    "arbitrum": "0x33e0c8ab132390822b07e5dc95033cf250c963153320b7ffca73220664da2ea0",
    "ethereum": "0xb323495f7e4148be5643a4ea4a8221eef163e4bccfdedc2a6f4696baacbc86cc",
}


@pytest.fixture(params=list(_MARKETS), ids=list(_MARKETS))
def chain(request):
    return request.param


@pytest.fixture
def mock_market():
    market = MagicMock()
    market.price = MagicMock(side_effect=lambda t: {"wstETH": Decimal("4000"), "USDC": Decimal("1")}.get(t, Decimal("0")))
    return market


@pytest.fixture
def strategy(chain):
    from strategies.incubating.morpho_blue_lifecycle.strategy import MorphoBlueLifecycleStrategy

    s = MorphoBlueLifecycleStrategy.__new__(MorphoBlueLifecycleStrategy)
    s.config = {}
    s._chain = chain
    s._wallet_address = "0x" + "0" * 40
    s._deployment_id = f"morpho_blue_lifecycle_{chain}"
    s.collateral_token = "wstETH"
    s.collateral_amount = Decimal("0.1")
    s.borrow_token = "USDC"
    s.ltv_target = Decimal("0.3")
    s.market_id = _MARKETS[chain]
    s._loop_state = "idle"
    s._previous_stable_state = "idle"
    s._collateral_supplied = Decimal("0")
    s._borrowed_amount = Decimal("0")
    return s


class TestConstruction:
    def test_missing_market_id_raises(self):
        """Morpho markets are per-chain — a config without market_id must fail loud,
        not silently default to one chain's market."""
        from strategies.incubating.morpho_blue_lifecycle.strategy import MorphoBlueLifecycleStrategy

        with pytest.raises(ValueError, match="market_id"):
            MorphoBlueLifecycleStrategy(
                config={"chain": "base", "collateral_token": "wstETH", "borrow_token": "USDC"},
                chain="base",
                wallet_address="0x" + "0" * 40,
            )


class TestStateMachine:
    def test_idle_supplies(self, strategy, mock_market):
        intent = strategy.decide(mock_market)
        assert intent.intent_type.value == "SUPPLY"
        assert strategy._loop_state == "supplying"

    def test_supplied_borrows(self, strategy, mock_market):
        strategy._loop_state = "supplied"
        strategy._collateral_supplied = Decimal("0.1")
        intent = strategy.decide(mock_market)
        assert intent.intent_type.value == "BORROW"
        assert strategy._loop_state == "borrowing"

    def test_borrowed_repays(self, strategy, mock_market):
        strategy._loop_state = "borrowed"
        strategy._borrowed_amount = Decimal("100")
        intent = strategy.decide(mock_market)
        assert intent.intent_type.value == "REPAY"
        assert strategy._loop_state == "repaying"

    def test_repaid_withdraws(self, strategy, mock_market):
        strategy._loop_state = "repaid"
        strategy._collateral_supplied = Decimal("0.1")
        intent = strategy.decide(mock_market)
        assert intent.intent_type.value == "WITHDRAW"
        assert strategy._loop_state == "withdrawing"

    def test_complete_holds(self, strategy, mock_market):
        strategy._loop_state = "complete"
        assert strategy.decide(mock_market).intent_type.value == "HOLD"

    def test_price_unavailable_holds(self, strategy):
        m = MagicMock()
        m.price = MagicMock(side_effect=ValueError("no price"))
        # IDLE needs no prices -> SUPPLY proceeds even during a price outage.
        assert strategy.decide(m).intent_type.value == "SUPPLY"
        # Only the BORROW step needs prices -> HOLD on outage in the SUPPLIED state.
        strategy._loop_state = "supplied"
        assert strategy.decide(m).intent_type.value == "HOLD"


class TestIntents:
    def test_supply_uses_collateral_and_market_id(self, strategy, chain):
        intent = strategy._create_supply_intent()
        assert intent.intent_type.value == "SUPPLY"
        assert intent.token == "wstETH"
        assert intent.amount == Decimal("0.1")
        assert intent.market_id == _MARKETS[chain]
        assert intent.chain == chain

    def test_borrow_is_standalone_zero_collateral(self, strategy, chain):
        # VIB-3586: collateral is supplied by the standalone SUPPLY intent, so the
        # BORROW must carry collateral_amount=0 (bundled borrow is framework-rejected).
        intent = strategy._create_borrow_intent(Decimal("4000"), Decimal("1"))
        assert intent.intent_type.value == "BORROW"
        assert intent.collateral_amount == Decimal("0")
        assert intent.collateral_token == "wstETH"
        assert intent.borrow_token == "USDC"
        # borrow = 0.1 wstETH * $4000 * 0.3 LTV / $1 = $120
        assert intent.borrow_amount == Decimal("120.00")
        assert intent.market_id == _MARKETS[chain]
        assert intent.chain == chain

    def test_repay_full_and_market(self, strategy, chain):
        strategy._borrowed_amount = Decimal("120")
        intent = strategy._create_repay_intent()
        assert intent.intent_type.value == "REPAY"
        assert intent.repay_full is True
        assert intent.market_id == _MARKETS[chain]

    def test_withdraw_all_and_market(self, strategy, chain):
        strategy._collateral_supplied = Decimal("0.1")
        intent = strategy._create_withdraw_intent()
        assert intent.intent_type.value == "WITHDRAW"
        assert intent.withdraw_all is True
        assert intent.token == "wstETH"
        assert intent.market_id == _MARKETS[chain]


class TestOnExecuted:
    def test_supply_advances(self, strategy):
        intent = MagicMock()
        intent.intent_type.value = "SUPPLY"
        intent.amount = Decimal("0.1")
        strategy._loop_state = "supplying"
        strategy.on_intent_executed(intent, True, None)
        assert strategy._loop_state == "supplied"
        assert strategy._collateral_supplied == Decimal("0.1")

    def test_borrow_advances(self, strategy):
        intent = MagicMock()
        intent.intent_type.value = "BORROW"
        intent.borrow_amount = Decimal("120")
        strategy._loop_state = "borrowing"
        strategy._collateral_supplied = Decimal("0.1")
        strategy.on_intent_executed(intent, True, None)
        assert strategy._loop_state == "borrowed"
        assert strategy._borrowed_amount == Decimal("120")

    def test_failure_reverts(self, strategy):
        intent = MagicMock()
        intent.intent_type.value = "SUPPLY"
        strategy._loop_state = "supplying"
        strategy._previous_stable_state = "idle"
        strategy.on_intent_executed(intent, False, None)
        assert strategy._loop_state == "idle"


class TestTeardown:
    def test_teardown_repays_then_withdraws(self, strategy):
        from almanak.framework.teardown import TeardownMode

        strategy._borrowed_amount = Decimal("120")
        strategy._collateral_supplied = Decimal("0.1")
        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert [i.intent_type.value for i in intents] == ["REPAY", "WITHDRAW"]

    def test_open_positions_when_borrowed(self, strategy):
        strategy._collateral_supplied = Decimal("0.1")
        strategy._borrowed_amount = Decimal("120")
        summary = strategy.get_open_positions()
        assert {p.position_type.value for p in summary.positions} == {"SUPPLY", "BORROW"}

    def test_no_positions_when_idle(self, strategy):
        assert len(strategy.get_open_positions().positions) == 0


class TestStatus:
    def test_status_uses_registered_name(self, strategy, chain):
        status = strategy.get_status()
        assert status["strategy"] == "morpho_blue_lifecycle"
        assert status["chain"] == chain
        assert status["market_id"] == _MARKETS[chain]
