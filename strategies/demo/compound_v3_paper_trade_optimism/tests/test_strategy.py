"""Unit tests for Compound V3 Paper Trade Strategy on Optimism.

Tests validate:
1. Initialization with config
2. Price-gated supply/withdraw logic
3. State machine transitions
4. Teardown interface
5. Persistence round-trip
"""

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from strategies.demo.compound_v3_paper_trade_optimism.strategy import (
    CompoundV3PaperTradeOptimismStrategy,
)


def _create_strategy(config_overrides=None):
    config = {
        "supply_token": "USDC",
        "supply_amount": "100",
        "market": "usdc",
        "price_supply_above": 2000,
        "price_withdraw_below": 1500,
        "chain": "optimism",
    }
    if config_overrides:
        config.update(config_overrides)
    return CompoundV3PaperTradeOptimismStrategy(
        config=config,
        chain="optimism",
        wallet_address="0x" + "a" * 40,
    )


@pytest.fixture
def strategy():
    return _create_strategy()


def _make_market(eth_price: Decimal, usdc_balance: Decimal = Decimal("1000")) -> MagicMock:
    market = MagicMock()
    market.price.side_effect = lambda token: eth_price if token == "ETH" else Decimal("1")
    market.balance.return_value = usdc_balance
    return market


class TestInitialization:
    def test_defaults(self, strategy):
        assert strategy.supply_token == "USDC"
        assert strategy.supply_amount == Decimal("100")
        assert strategy.market == "usdc"
        assert strategy.price_supply_above == Decimal("2000")
        assert strategy.price_withdraw_below == Decimal("1500")
        assert strategy._has_supply is False
        assert strategy._supplied_amount == Decimal("0")

    def test_chain_is_optimism(self, strategy):
        assert strategy.chain == "optimism"

    def test_invalid_thresholds(self):
        with pytest.raises(ValueError, match="price_supply_above"):
            _create_strategy({"price_supply_above": 1000, "price_withdraw_below": 2000})

    def test_invalid_supply_amount(self):
        with pytest.raises(ValueError, match="supply_amount"):
            _create_strategy({"supply_amount": "0"})


class TestDecisionLogic:
    def test_supplies_above_threshold(self, strategy):
        """ETH price above threshold → supply USDC."""
        market = _make_market(Decimal("2500"))
        intent = strategy.decide(market)
        assert intent is not None
        assert intent.intent_type.value == "SUPPLY"
        assert intent.protocol == "compound_v3"
        assert intent.chain == "optimism"

    def test_holds_below_threshold_no_supply(self, strategy):
        """ETH price below supply threshold and no active position → hold."""
        market = _make_market(Decimal("1800"))
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"

    def test_holds_when_supplied_and_price_ok(self, strategy):
        """When supplied and ETH > withdraw threshold → hold."""
        market = _make_market(Decimal("2500"))
        intent = strategy.decide(market)
        strategy.on_intent_executed(intent, success=True, result=None)

        market2 = _make_market(Decimal("2200"))
        intent2 = strategy.decide(market2)
        assert intent2.intent_type.value == "HOLD"

    def test_withdraws_on_price_drop(self, strategy):
        """When supplied and ETH drops below withdraw threshold → withdraw."""
        market_high = _make_market(Decimal("2500"))
        intent = strategy.decide(market_high)
        strategy.on_intent_executed(intent, success=True, result=None)

        market_low = _make_market(Decimal("1400"))
        intent2 = strategy.decide(market_low)
        assert intent2.intent_type.value == "WITHDRAW"
        assert intent2.protocol == "compound_v3"

    def test_holds_when_insufficient_funds(self, strategy):
        """Not enough USDC → hold even if price is above threshold."""
        market = _make_market(Decimal("2500"), usdc_balance=Decimal("50"))
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"


class TestStateTransitions:
    def test_supply_success_sets_has_supply(self, strategy):
        market = _make_market(Decimal("2500"))
        intent = strategy.decide(market)
        strategy.on_intent_executed(intent, success=True, result=None)
        assert strategy._has_supply is True
        assert strategy._supplied_amount == Decimal("100")

    def test_supply_failure_leaves_no_supply(self, strategy):
        market = _make_market(Decimal("2500"))
        intent = strategy.decide(market)
        strategy.on_intent_executed(intent, success=False, result=None)
        assert strategy._has_supply is False

    def test_withdraw_success_clears_supply(self, strategy):
        market = _make_market(Decimal("2500"))
        intent = strategy.decide(market)
        strategy.on_intent_executed(intent, success=True, result=None)

        market_low = _make_market(Decimal("1400"))
        intent2 = strategy.decide(market_low)
        strategy.on_intent_executed(intent2, success=True, result=None)
        assert strategy._has_supply is False
        assert strategy._supplied_amount == Decimal("0")

    def test_ticks_counted(self, strategy):
        """Ticks with supply are counted."""
        market = _make_market(Decimal("2500"))
        intent = strategy.decide(market)
        strategy.on_intent_executed(intent, success=True, result=None)

        for _ in range(3):
            strategy.decide(market)

        assert strategy._ticks_with_supply == 3


class TestTeardown:
    def test_teardown_with_supply(self, strategy):
        strategy._has_supply = True
        strategy._supplied_amount = Decimal("100")

        from almanak.framework.teardown import TeardownMode
        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 1
        assert intents[0].intent_type.value == "WITHDRAW"
        assert intents[0].protocol == "compound_v3"

    def test_teardown_empty_when_idle(self, strategy):
        from almanak.framework.teardown import TeardownMode
        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 0

    def test_open_positions_with_supply(self, strategy):
        strategy._has_supply = True
        strategy._supplied_amount = Decimal("100")

        summary = strategy.get_open_positions()
        assert len(summary.positions) == 1
        assert summary.positions[0].protocol == "compound_v3"
        assert summary.positions[0].position_type.value == "SUPPLY"

    def test_open_positions_empty(self, strategy):
        summary = strategy.get_open_positions()
        assert len(summary.positions) == 0

    def test_supports_teardown(self, strategy):
        assert strategy.supports_teardown() is True


class TestPersistence:
    def test_round_trip(self, strategy):
        strategy._has_supply = True
        strategy._supplied_amount = Decimal("200")
        strategy._ticks_with_supply = 7

        state = strategy.get_persistent_state()
        new_s = _create_strategy()
        new_s.load_persistent_state(state)

        assert new_s._has_supply is True
        assert new_s._supplied_amount == Decimal("200")
        assert new_s._ticks_with_supply == 7
