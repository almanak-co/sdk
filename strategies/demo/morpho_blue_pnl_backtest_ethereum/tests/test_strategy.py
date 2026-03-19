"""Unit tests for the Morpho Blue PnL Backtest Strategy.

Tests validate:
1. Strategy initialization with config
2. State machine: idle -> supplying -> supplied -> hold
3. Supply intent parameters (protocol=morpho_blue, use_as_collateral=True)
4. Teardown interface compliance
5. Persistence round-trip
"""

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from strategies.demo.morpho_blue_pnl_backtest_ethereum.strategy import (
    DEFAULT_MARKET_ID,
    MorphoBluePnLBacktestStrategy,
)


def _create_strategy(config_overrides=None):
    """Create a strategy instance with test config."""
    config = {
        "market_id": DEFAULT_MARKET_ID,
        "supply_token": "wstETH",
        "supply_amount": "2",
        "min_apy_bps": 100,
        "chain": "ethereum",
    }
    if config_overrides:
        config.update(config_overrides)
    return MorphoBluePnLBacktestStrategy(
        config=config,
        chain="ethereum",
        wallet_address="0x" + "a" * 40,
    )


@pytest.fixture
def strategy():
    return _create_strategy()


def _make_market(wsteth_price: Decimal = Decimal("3500")) -> MagicMock:
    market = MagicMock()
    market.price.side_effect = lambda token: wsteth_price
    return market


class TestInitialization:
    def test_default_config(self, strategy):
        assert strategy.supply_token == "wstETH"
        assert strategy.supply_amount == Decimal("2")
        assert strategy.min_apy_bps == 100
        assert strategy.market_id == DEFAULT_MARKET_ID
        assert strategy._state == "idle"
        assert strategy._tick_count == 0

    def test_custom_config(self):
        s = _create_strategy({
            "supply_amount": "5",
            "min_apy_bps": 200,
        })
        assert s.supply_amount == Decimal("5")
        assert s.min_apy_bps == 200


class TestDecisionLogic:
    def test_first_tick_supplies(self, strategy):
        """First tick should supply wstETH to Morpho Blue as collateral."""
        market = _make_market()
        intent = strategy.decide(market)
        assert intent is not None
        assert intent.intent_type.value == "SUPPLY"
        assert strategy._state == "supplying"
        assert strategy._tick_count == 1

    def test_supply_intent_params(self, strategy):
        """Supply intent must target morpho_blue with correct market_id."""
        market = _make_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "SUPPLY"
        assert intent.protocol == "morpho_blue"
        assert intent.token == "wstETH"
        assert intent.market_id == DEFAULT_MARKET_ID
        assert intent.use_as_collateral is True

    def test_supply_success_transitions_to_supplied(self, strategy):
        """After successful supply, state becomes 'supplied'."""
        market = _make_market()
        intent = strategy.decide(market)
        strategy.on_intent_executed(intent, success=True, result=None)
        assert strategy._state == "supplied"
        assert strategy._supplied_amount == Decimal("2")

    def test_after_supply_holds(self, strategy):
        """After supplying, strategy should hold."""
        market = _make_market()
        intent = strategy.decide(market)
        strategy.on_intent_executed(intent, success=True, result=None)

        intent2 = strategy.decide(market)
        assert intent2 is not None
        assert intent2.intent_type.value == "HOLD"

    def test_tick_count_increments(self, strategy):
        """Each call to decide() increments the tick counter."""
        market = _make_market()
        intent = strategy.decide(market)
        strategy.on_intent_executed(intent, success=True, result=None)

        for _ in range(5):
            strategy.decide(market)

        assert strategy._tick_count == 6  # 1 supply + 5 holds


class TestFailureRecovery:
    def test_supply_failure_reverts_to_idle(self, strategy):
        """Supply failure should revert state to idle (allow retry)."""
        market = _make_market()
        intent = strategy.decide(market)
        strategy.on_intent_executed(intent, success=False, result=None)
        assert strategy._state == "idle"
        assert strategy._supplied_amount == Decimal("0")

    def test_failed_supply_allows_retry(self, strategy):
        """After supply failure, next tick should retry the supply."""
        market = _make_market()
        intent = strategy.decide(market)
        strategy.on_intent_executed(intent, success=False, result=None)

        intent2 = strategy.decide(market)
        assert intent2.intent_type.value == "SUPPLY"


class TestTeardown:
    def test_teardown_generates_withdraw_when_supplied(self, strategy):
        """Teardown should withdraw the supplied wstETH."""
        strategy._state = "supplied"
        strategy._supplied_amount = Decimal("2")

        from almanak.framework.teardown import TeardownMode
        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 1
        assert intents[0].intent_type.value == "WITHDRAW"
        assert intents[0].protocol == "morpho_blue"

    def test_teardown_empty_when_idle(self, strategy):
        """No teardown intents when strategy is idle."""
        from almanak.framework.teardown import TeardownMode
        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 0

    def test_open_positions_when_supplied(self, strategy):
        """Open positions should include supply position with USD value."""
        strategy._state = "supplied"
        strategy._supplied_amount = Decimal("2")
        strategy._last_token_price = Decimal("3500")

        summary = strategy.get_open_positions()
        assert len(summary.positions) == 1
        assert summary.positions[0].position_type.value == "SUPPLY"
        assert summary.positions[0].protocol == "morpho_blue"
        assert summary.positions[0].value_usd == Decimal("7000")

    def test_open_positions_empty_when_idle(self, strategy):
        """No positions when idle."""
        summary = strategy.get_open_positions()
        assert len(summary.positions) == 0

    def test_supports_teardown(self, strategy):
        assert strategy.supports_teardown() is True


class TestPersistence:
    def test_round_trip(self, strategy):
        """State can be saved and restored."""
        strategy._state = "supplied"
        strategy._supplied_amount = Decimal("2")
        strategy._last_token_price = Decimal("3500")
        strategy._tick_count = 42

        state = strategy.get_persistent_state()

        new_strategy = _create_strategy()
        new_strategy.load_persistent_state(state)

        assert new_strategy._state == "supplied"
        assert new_strategy._supplied_amount == Decimal("2")
        assert new_strategy._last_token_price == Decimal("3500")
        assert new_strategy._tick_count == 42
