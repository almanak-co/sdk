"""Unit tests for Morpho Blue Collateral Rotator (VIB-1551).

Validates:
1. Initialization with two-market config
2. State machine: idle -> supplying -> supplied -> hold -> withdrawing -> withdrawn -> supplying -> supplied
3. Rotation triggers correctly based on price momentum (cooldown + threshold)
4. No spurious rotation within cooldown window
5. Market ID routing (usdc vs weth market)
6. Teardown interface compliance
"""

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from strategies.demo.morpho_blue_collateral_rotator_ethereum.strategy import MorphoBlueCollateralRotatorStrategy

MARKET_USDC = "0xb323495f7e4148be5643a4ea4a8221eef163e4bccfdedc2a6f4696baacbc86cc"
MARKET_WETH = "0xc54d7acf14de29e0e5527cabd7a576506870346a78a11a6762e2cca66322ec41"


def _create_strategy(config_overrides=None):
    config = {
        "collateral_token": "wstETH",
        "collateral_amount": "0.05",
        "market_usdc_id": MARKET_USDC,
        "market_weth_id": MARKET_WETH,
        "rotation_threshold_bps": 30,
        "cooldown_ticks": 3,
        "chain": "ethereum",
    }
    if config_overrides:
        config.update(config_overrides)
    return MorphoBlueCollateralRotatorStrategy(
        config=config,
        chain="ethereum",
        wallet_address="0x" + "a" * 40,
    )


def _make_market(wsteth_price: Decimal) -> MagicMock:
    market = MagicMock()
    market.price.return_value = wsteth_price
    return market


@pytest.fixture
def strategy():
    return _create_strategy()


class TestInitialization:
    def test_defaults(self, strategy):
        assert strategy.collateral_token == "wstETH"
        assert strategy.collateral_amount == Decimal("0.05")
        assert strategy._state == "idle"
        assert strategy._current_market == "usdc"
        assert strategy.market_usdc_id == MARKET_USDC
        assert strategy.market_weth_id == MARKET_WETH

    def test_custom_threshold(self):
        s = _create_strategy({"rotation_threshold_bps": 100, "cooldown_ticks": 2})
        assert s.rotation_threshold == Decimal("0.01")
        assert s.cooldown_ticks == 2


class TestFirstTick:
    def test_idle_supplies_to_usdc_market_by_default(self, strategy):
        """First tick: no price history -> supplies to USDC market."""
        intent = strategy.decide(_make_market(Decimal("3500")))
        assert intent.intent_type.value == "SUPPLY"
        assert strategy._state == "supplying"

    def test_supply_uses_usdc_market_id(self, strategy):
        """Supply intent should use the USDC market ID."""
        intent = strategy.decide(_make_market(Decimal("3500")))
        assert intent.market_id == MARKET_USDC


class TestStateTransitions:
    def test_supply_success_enters_supplied_state(self, strategy):
        """on_intent_executed(success=True) after supply -> supplied state."""
        intent = strategy.decide(_make_market(Decimal("3500")))
        assert strategy._state == "supplying"

        strategy.on_intent_executed(intent, success=True, result=None)
        assert strategy._state == "supplied"
        assert strategy._current_market == "usdc"
        assert strategy._ticks_since_rotation == 0

    def test_supply_failure_reverts_to_idle(self, strategy):
        """on_intent_executed(success=False) after supply -> revert to idle."""
        intent = strategy.decide(_make_market(Decimal("3500")))
        strategy.on_intent_executed(intent, success=False, result=None)
        assert strategy._state == "idle"

    def test_supplied_holds_within_cooldown(self, strategy):
        """No rotation within cooldown_ticks ticks even if price moves."""
        # Supply first
        intent = strategy.decide(_make_market(Decimal("3500")))
        strategy.on_intent_executed(intent, success=True, result=None)
        assert strategy._state == "supplied"

        # Set entry price low so threshold is exceeded, but cooldown is 3
        strategy._entry_price = Decimal("3000")  # +16% rise -> would trigger rotation
        # Tick 1 and 2 (within cooldown=3, ticks_since_rotation was reset to 0)
        for _ in range(2):
            intent = strategy.decide(_make_market(Decimal("3500")))
            assert intent.intent_type.value == "HOLD"

    def test_rotation_triggers_after_cooldown(self, strategy):
        """Rotation fires after cooldown_ticks ticks with sufficient price movement."""
        intent = strategy.decide(_make_market(Decimal("3000")))
        strategy.on_intent_executed(intent, success=True, result=None)
        assert strategy._state == "supplied"

        # Force entry price low so +2% rise exceeds 30bps threshold
        strategy._entry_price = Decimal("3000")
        # Burn cooldown ticks (cooldown=3, ticks_since_rotation reset to 0 after supply)
        for _ in range(3):
            strategy.decide(_make_market(Decimal("3000")))  # below threshold, no rotation

        # Now a tick with price above threshold should trigger withdraw
        intent = strategy.decide(_make_market(Decimal("3100")))  # +3.3% > 0.3%
        assert intent.intent_type.value == "WITHDRAW"
        assert strategy._state == "withdrawing"

    def test_withdraw_success_enters_withdrawn(self, strategy):
        """Withdraw success -> withdrawn state."""
        intent = strategy.decide(_make_market(Decimal("3000")))
        strategy.on_intent_executed(intent, success=True, result=None)
        strategy._entry_price = Decimal("3000")
        strategy._ticks_since_rotation = strategy.cooldown_ticks + 1

        withdraw_intent = strategy.decide(_make_market(Decimal("3100")))
        assert strategy._state == "withdrawing"

        strategy.on_intent_executed(withdraw_intent, success=True, result=None)
        assert strategy._state == "withdrawn"

    def test_withdrawn_supplies_to_target_market(self, strategy):
        """Withdrawn state: next decide() should supply to the new market."""
        intent = strategy.decide(_make_market(Decimal("3000")))
        strategy.on_intent_executed(intent, success=True, result=None)
        strategy._entry_price = Decimal("3000")
        strategy._ticks_since_rotation = strategy.cooldown_ticks + 1

        # Trigger rotation to weth market
        withdraw_intent = strategy.decide(_make_market(Decimal("3100")))
        strategy.on_intent_executed(withdraw_intent, success=True, result=None)
        assert strategy._state == "withdrawn"
        assert strategy._target_market == "weth"

        # Next tick should supply to weth market
        supply_intent = strategy.decide(_make_market(Decimal("3100")))
        assert supply_intent.intent_type.value == "SUPPLY"
        assert supply_intent.market_id == MARKET_WETH


class TestMarketRouting:
    def test_usdc_market_id(self, strategy):
        assert strategy._market_id("usdc") == MARKET_USDC

    def test_weth_market_id(self, strategy):
        assert strategy._market_id("weth") == MARKET_WETH


class TestPersistentState:
    def test_round_trip(self, strategy):
        strategy._state = "supplied"
        strategy._current_market = "weth"
        strategy._ticks_since_rotation = 5
        strategy._entry_price = Decimal("3500.123")

        state = strategy.get_persistent_state()
        new_strategy = _create_strategy()
        new_strategy.load_persistent_state(state)

        assert new_strategy._state == "supplied"
        assert new_strategy._current_market == "weth"
        assert new_strategy._ticks_since_rotation == 5
        assert new_strategy._entry_price == Decimal("3500.123")


class TestTeardown:
    def test_no_position_returns_empty(self, strategy):
        intents = strategy.generate_teardown_intents(mode=None)
        assert intents == []

    def test_supplied_returns_withdraw_intent(self, strategy):
        intent = strategy.decide(_make_market(Decimal("3500")))
        strategy.on_intent_executed(intent, success=True, result=None)
        assert strategy._state == "supplied"

        teardown_intents = strategy.generate_teardown_intents(mode=None)
        assert len(teardown_intents) == 1
        assert teardown_intents[0].intent_type.value == "WITHDRAW"
        assert teardown_intents[0].withdraw_all is True

    def test_get_open_positions_when_supplied(self, strategy):
        intent = strategy.decide(_make_market(Decimal("3500")))
        strategy.on_intent_executed(intent, success=True, result=None)
        summary = strategy.get_open_positions()
        assert len(summary.positions) == 1
        pos = summary.positions[0]
        assert pos.protocol == "morpho_blue"
        assert "usdc" in pos.position_id
