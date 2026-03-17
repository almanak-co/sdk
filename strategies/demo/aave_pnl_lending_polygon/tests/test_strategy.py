"""Unit tests for the Aave V3 PnL Lending Strategy on Polygon.

Tests validate:
1. Strategy initialization with Polygon config
2. State machine transitions (idle -> supplied -> borrowed -> repaid)
3. Price-based decision logic (drop triggers borrow, rise triggers repay)
4. Teardown interface compliance
5. Persistence round-trip
"""

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from strategies.demo.aave_pnl_lending_polygon.strategy import AavePnLLendingPolygonStrategy


def _create_strategy(config_overrides=None):
    """Create a strategy instance with default Polygon config."""
    config = {
        "supply_token": "WETH",
        "borrow_token": "USDC",
        "supply_amount": "0.01",
        "ltv_target": 0.4,
        "price_drop_threshold": 0.03,
        "price_rise_threshold": 0.05,
        "chain": "polygon",
    }
    if config_overrides:
        config.update(config_overrides)
    return AavePnLLendingPolygonStrategy(
        config=config,
        chain="polygon",
        wallet_address="0x" + "a" * 40,
    )


@pytest.fixture
def strategy():
    return _create_strategy()


def _make_market(eth_price: Decimal) -> MagicMock:
    """Create a mock MarketSnapshot with given ETH price."""
    market = MagicMock()
    market.price.side_effect = lambda token: {
        "WETH": eth_price,
        "USDC": Decimal("1"),
    }.get(token, Decimal("1"))
    return market


class TestInitialization:
    def test_default_config(self, strategy):
        assert strategy.supply_token == "WETH"
        assert strategy.borrow_token == "USDC"
        assert strategy.supply_amount == Decimal("0.01")
        assert strategy.ltv_target == Decimal("0.4")
        assert strategy._state == "idle"
        assert strategy.chain == "polygon"

    def test_custom_config(self):
        s = _create_strategy({
            "supply_token": "wstETH",
            "borrow_token": "DAI",
            "supply_amount": "1.0",
        })
        assert s.supply_token == "wstETH"
        assert s.borrow_token == "DAI"
        assert s.supply_amount == Decimal("1.0")


class TestDecisionLogic:
    def test_first_tick_supplies(self, strategy):
        """First tick should always supply collateral."""
        market = _make_market(Decimal("3400"))
        intent = strategy.decide(market)
        assert intent is not None
        assert intent.intent_type.value == "SUPPLY"
        assert strategy._state == "supplying"

    def test_supply_success_transitions_to_supplied(self, strategy):
        market = _make_market(Decimal("3400"))
        intent = strategy.decide(market)
        strategy.on_intent_executed(intent, success=True, result=None)
        assert strategy._state == "supplied"
        assert strategy._supplied_amount == Decimal("0.01")

    def test_price_drop_triggers_borrow(self, strategy):
        market = _make_market(Decimal("3400"))
        intent = strategy.decide(market)
        strategy.on_intent_executed(intent, success=True, result=None)

        # Price drops 5% (beyond 3% threshold)
        market_drop = _make_market(Decimal("3230"))
        intent = strategy.decide(market_drop)
        assert intent is not None
        assert intent.intent_type.value == "BORROW"

    def test_no_borrow_on_small_drop(self, strategy):
        market = _make_market(Decimal("3400"))
        intent = strategy.decide(market)
        strategy.on_intent_executed(intent, success=True, result=None)

        # Price drops 1% (under 3% threshold)
        market_small = _make_market(Decimal("3366"))
        intent = strategy.decide(market_small)
        assert intent.intent_type.value == "HOLD"

    def test_price_rise_triggers_repay(self, strategy):
        market = _make_market(Decimal("3400"))
        intent = strategy.decide(market)
        strategy.on_intent_executed(intent, success=True, result=None)

        market_drop = _make_market(Decimal("3230"))
        intent = strategy.decide(market_drop)
        strategy.on_intent_executed(intent, success=True, result=None)
        assert strategy._state == "borrowed"

        # Price rises 6% from reference (beyond 5% threshold)
        market_rise = _make_market(Decimal("3424"))
        intent = strategy.decide(market_rise)
        assert intent.intent_type.value == "REPAY"

    def test_repay_returns_to_supplied(self, strategy):
        market = _make_market(Decimal("3400"))
        intent = strategy.decide(market)
        strategy.on_intent_executed(intent, success=True, result=None)

        market_drop = _make_market(Decimal("3230"))
        intent = strategy.decide(market_drop)
        strategy.on_intent_executed(intent, success=True, result=None)

        market_rise = _make_market(Decimal("3424"))
        intent = strategy.decide(market_rise)
        strategy.on_intent_executed(intent, success=True, result=None)

        assert strategy._state == "supplied"
        assert strategy._borrowed_amount == Decimal("0")

    def test_hold_when_price_unavailable(self, strategy):
        market = MagicMock()
        market.price.side_effect = ValueError("No data")
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "unavailable" in intent.reason.lower()


class TestFailureRecovery:
    def test_supply_failure_reverts_to_idle(self, strategy):
        market = _make_market(Decimal("3400"))
        intent = strategy.decide(market)
        strategy.on_intent_executed(intent, success=False, result=None)
        assert strategy._state == "idle"

    def test_borrow_failure_reverts_to_supplied(self, strategy):
        market = _make_market(Decimal("3400"))
        intent = strategy.decide(market)
        strategy.on_intent_executed(intent, success=True, result=None)

        market_drop = _make_market(Decimal("3230"))
        intent = strategy.decide(market_drop)
        strategy.on_intent_executed(intent, success=False, result=None)
        assert strategy._state == "supplied"

    def test_borrow_failure_restores_reference_price(self, strategy):
        """Reference price must revert so the same dip can re-trigger a borrow."""
        market = _make_market(Decimal("3400"))
        intent = strategy.decide(market)
        strategy.on_intent_executed(intent, success=True, result=None)
        ref_before = strategy._reference_price

        market_drop = _make_market(Decimal("3230"))
        intent = strategy.decide(market_drop)
        # reference_price was overwritten to 3230 during decide()
        assert strategy._reference_price == Decimal("3230")
        strategy.on_intent_executed(intent, success=False, result=None)
        # After failure, reference price should revert to 3400
        assert strategy._reference_price == ref_before


class TestTeardown:
    def test_supports_teardown(self, strategy):
        assert strategy.supports_teardown() is True

    def test_teardown_with_supply_only(self, strategy):
        strategy._state = "supplied"
        strategy._supplied_amount = Decimal("0.01")

        from almanak.framework.teardown import TeardownMode

        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 1
        assert intents[0].intent_type.value == "WITHDRAW"

    def test_teardown_with_borrow_and_supply(self, strategy):
        strategy._state = "borrowed"
        strategy._supplied_amount = Decimal("0.01")
        strategy._borrowed_amount = Decimal("10")

        from almanak.framework.teardown import TeardownMode

        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 2
        assert intents[0].intent_type.value == "REPAY"
        assert intents[1].intent_type.value == "WITHDRAW"

    def test_teardown_withdraw_resets_state(self, strategy):
        """After a successful WITHDRAW, state reverts to idle with zero supply."""
        strategy._state = "supplied"
        strategy._supplied_amount = Decimal("0.01")
        strategy._reference_price = Decimal("3400")

        from almanak.framework.teardown import TeardownMode

        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 1
        withdraw_intent = intents[0]
        assert withdraw_intent.intent_type.value == "WITHDRAW"

        strategy.on_intent_executed(withdraw_intent, success=True, result=None)
        assert strategy._state == "idle"
        assert strategy._supplied_amount == Decimal("0")
        assert strategy._reference_price is None

    def test_open_positions_empty_when_idle(self, strategy):
        summary = strategy.get_open_positions()
        assert len(summary.positions) == 0

    def test_open_positions_with_supply(self, strategy):
        strategy._supplied_amount = Decimal("0.01")
        summary = strategy.get_open_positions()
        assert len(summary.positions) == 1
        assert summary.positions[0].position_type.value == "SUPPLY"
        assert summary.positions[0].chain == "polygon"


class TestPersistence:
    def test_round_trip(self, strategy):
        strategy._state = "borrowed"
        strategy._supplied_amount = Decimal("0.01")
        strategy._borrowed_amount = Decimal("10")
        strategy._reference_price = Decimal("3400")

        state = strategy.get_persistent_state()

        new_strategy = _create_strategy()
        new_strategy.load_persistent_state(state)

        assert new_strategy._state == "borrowed"
        assert new_strategy._supplied_amount == Decimal("0.01")
        assert new_strategy._borrowed_amount == Decimal("10")
        assert new_strategy._reference_price == Decimal("3400")


class TestStatus:
    def test_status_includes_polygon(self, strategy):
        status = strategy.get_status()
        assert status["strategy"] == "demo_aave_pnl_lending_polygon"
        assert status["chain"] == "polygon"
        assert status["state"] == "idle"
