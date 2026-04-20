"""Unit tests for the Morpho Blue Paper Trade Strategy.

Tests validate:
1. Strategy initialization with Morpho Blue config
2. State machine transitions (idle -> supplied -> borrowed -> repaid)
3. Price-based decision logic with Morpho Blue market_id
4. Teardown interface compliance (repay before withdraw)
5. Paper trading compatibility (decide returns valid intents)
"""

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from strategies.demo.morpho_paper_trade.strategy import MorphoPaperTradeStrategy


def _create_strategy(config_overrides=None):
    """Create a strategy instance with default config."""
    config = {
        "market_id": "0xb323495f7e4148be5643a4ea4a8221eef163e4bccfdedc2a6f4696baacbc86cc",
        "collateral_token": "wstETH",
        "borrow_token": "USDC",
        "collateral_amount": "0.05",
        "ltv_target": 0.5,
        "price_drop_pct": 0.02,
        "price_rise_pct": 0.04,
        "chain": "ethereum",
    }
    if config_overrides:
        config.update(config_overrides)
    return MorphoPaperTradeStrategy(
        config=config,
        chain="ethereum",
        wallet_address="0x" + "a" * 40,
    )


@pytest.fixture
def strategy():
    return _create_strategy()


def _make_market(wsteth_price: Decimal) -> MagicMock:
    """Create a mock MarketSnapshot with given wstETH price."""
    market = MagicMock()
    market.price.side_effect = lambda token: {
        "wstETH": wsteth_price,
        "USDC": Decimal("1"),
    }.get(token, Decimal("1"))
    return market


class TestInitialization:
    def test_default_config(self, strategy):
        assert strategy.collateral_token == "wstETH"
        assert strategy.borrow_token == "USDC"
        assert strategy.collateral_amount == Decimal("0.05")
        assert strategy.ltv_target == Decimal("0.5")
        assert strategy._state == "idle"
        assert strategy.market_id.startswith("0xb323")

    def test_custom_config(self):
        s = _create_strategy({
            "collateral_token": "WETH",
            "collateral_amount": "0.1",
            "ltv_target": 0.3,
        })
        assert s.collateral_token == "WETH"
        assert s.collateral_amount == Decimal("0.1")


class TestDecisionLogic:
    def test_first_tick_supplies_to_morpho(self, strategy):
        """First tick should supply collateral to Morpho Blue."""
        market = _make_market(Decimal("3800"))
        intent = strategy.decide(market)
        assert intent is not None
        assert intent.intent_type.value == "SUPPLY"
        assert strategy._state == "supplying"

    def test_supply_success_transitions(self, strategy):
        market = _make_market(Decimal("3800"))
        intent = strategy.decide(market)
        strategy.on_intent_executed(intent, success=True, result=None)
        assert strategy._state == "supplied"
        assert strategy._supplied_amount == Decimal("0.05")

    def test_price_drop_triggers_borrow(self, strategy):
        """2%+ price drop triggers borrow from Morpho Blue."""
        market = _make_market(Decimal("3800"))
        intent = strategy.decide(market)
        strategy.on_intent_executed(intent, success=True, result=None)

        # Price drops 3% (beyond 2% threshold)
        market_drop = _make_market(Decimal("3686"))
        intent = strategy.decide(market_drop)
        assert intent is not None
        assert intent.intent_type.value == "BORROW"

    def test_no_borrow_on_small_drop(self, strategy):
        """Small price drops below threshold should hold."""
        market = _make_market(Decimal("3800"))
        intent = strategy.decide(market)
        strategy.on_intent_executed(intent, success=True, result=None)

        # Price drops 1% (under 2% threshold)
        market_small = _make_market(Decimal("3762"))
        intent = strategy.decide(market_small)
        assert intent is not None
        assert intent.intent_type.value == "HOLD"

    def test_price_rise_triggers_repay(self, strategy):
        """4%+ price rise after borrow triggers repay."""
        market = _make_market(Decimal("3800"))
        intent = strategy.decide(market)
        strategy.on_intent_executed(intent, success=True, result=None)

        market_drop = _make_market(Decimal("3686"))
        intent = strategy.decide(market_drop)
        strategy.on_intent_executed(intent, success=True, result=None)
        assert strategy._state == "borrowed"

        # Price rises 5% from reference (beyond 4% threshold)
        market_rise = _make_market(Decimal("3870"))
        intent = strategy.decide(market_rise)
        assert intent is not None
        assert intent.intent_type.value == "REPAY"

    def test_repay_returns_to_supplied(self, strategy):
        """Repay cycles back to supplied state."""
        market = _make_market(Decimal("3800"))
        intent = strategy.decide(market)
        strategy.on_intent_executed(intent, success=True, result=None)

        market_drop = _make_market(Decimal("3686"))
        intent = strategy.decide(market_drop)
        strategy.on_intent_executed(intent, success=True, result=None)

        market_rise = _make_market(Decimal("3870"))
        intent = strategy.decide(market_rise)
        strategy.on_intent_executed(intent, success=True, result=None)

        assert strategy._state == "supplied"
        assert strategy._borrowed_amount == Decimal("0")


class TestFailureRecovery:
    def test_supply_failure_reverts(self, strategy):
        market = _make_market(Decimal("3800"))
        intent = strategy.decide(market)
        strategy.on_intent_executed(intent, success=False, result=None)
        assert strategy._state == "idle"

    def test_borrow_failure_reverts(self, strategy):
        market = _make_market(Decimal("3800"))
        intent = strategy.decide(market)
        strategy.on_intent_executed(intent, success=True, result=None)

        market_drop = _make_market(Decimal("3686"))
        intent = strategy.decide(market_drop)
        strategy.on_intent_executed(intent, success=False, result=None)
        assert strategy._state == "supplied"


class TestTeardown:
    def test_teardown_repay_then_withdraw(self, strategy):
        """Teardown with borrow + supply: repay first, then withdraw."""
        strategy._state = "borrowed"
        strategy._supplied_amount = Decimal("0.05")
        strategy._borrowed_amount = Decimal("95")

        from almanak.framework.teardown import TeardownMode
        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 2
        assert intents[0].intent_type.value == "REPAY"
        assert intents[1].intent_type.value == "WITHDRAW"

    def test_teardown_withdraw_only(self, strategy):
        """Teardown with supply only: just withdraw."""
        strategy._state = "supplied"
        strategy._supplied_amount = Decimal("0.05")

        from almanak.framework.teardown import TeardownMode
        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 1
        assert intents[0].intent_type.value == "WITHDRAW"

    def test_open_positions_includes_market_id(self, strategy):
        """Open positions should include Morpho market_id in details."""
        strategy._supplied_amount = Decimal("0.05")
        summary = strategy.get_open_positions()
        assert len(summary.positions) == 1
        assert "market_id" in summary.positions[0].details
        assert summary.positions[0].details["market_id"].startswith("0xb323")


class TestPersistence:
    def test_round_trip(self, strategy):
        """State survives save/restore cycle."""
        strategy._state = "borrowed"
        strategy._supplied_amount = Decimal("0.05")
        strategy._borrowed_amount = Decimal("95")
        strategy._reference_price = Decimal("3800")

        state = strategy.get_persistent_state()

        new = _create_strategy()
        new.load_persistent_state(state)

        assert new._state == "borrowed"
        assert new._supplied_amount == Decimal("0.05")
        assert new._borrowed_amount == Decimal("95")
        assert new._reference_price == Decimal("3800")
