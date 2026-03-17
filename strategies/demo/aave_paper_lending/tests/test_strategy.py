"""Unit tests for the Aave V3 Paper Trading Lending Strategy.

Tests validate:
1. Strategy initialization with config
2. State machine transitions (idle -> supplied -> borrowed -> repaid)
3. Price-based decision logic
4. Borrow cycle limit enforcement
5. Teardown interface compliance
6. State persistence round-trip
"""

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from strategies.demo.aave_paper_lending.strategy import AavePaperLendingStrategy


def _create_strategy(config_overrides=None):
    """Create a strategy instance with default config."""
    config = {
        "supply_token": "WETH",
        "borrow_token": "USDC",
        "supply_amount": "0.01",
        "ltv_target": "0.4",
        "price_drop_threshold": "0.02",
        "price_rise_threshold": "0.03",
        "max_borrow_cycles": 3,
        "chain": "arbitrum",
    }
    if config_overrides:
        config.update(config_overrides)
    return AavePaperLendingStrategy(
        config=config,
        chain="arbitrum",
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
        assert strategy.max_borrow_cycles == 3
        assert strategy._state == "idle"
        assert strategy._borrow_cycles == 0

    def test_custom_config(self):
        s = _create_strategy({
            "supply_token": "wstETH",
            "borrow_token": "DAI",
            "supply_amount": "1.0",
            "ltv_target": "0.3",
            "max_borrow_cycles": 5,
        })
        assert s.supply_token == "wstETH"
        assert s.borrow_token == "DAI"
        assert s.supply_amount == Decimal("1.0")
        assert s.max_borrow_cycles == 5


class TestDecisionLogic:
    def test_first_tick_supplies(self, strategy):
        """First tick should always supply collateral."""
        market = _make_market(Decimal("3400"))
        intent = strategy.decide(market)
        assert intent is not None
        assert intent.intent_type.value == "SUPPLY"
        assert strategy._state == "supplying"
        assert strategy._tick_count == 1

    def test_supply_success_transitions_to_supplied(self, strategy):
        """After successful supply, state transitions to supplied."""
        market = _make_market(Decimal("3400"))
        intent = strategy.decide(market)
        strategy.on_intent_executed(intent, success=True, result=None)
        assert strategy._state == "supplied"
        assert strategy._supplied_amount == Decimal("0.01")

    def test_price_drop_triggers_borrow(self, strategy):
        """When price drops beyond threshold, strategy borrows."""
        market = _make_market(Decimal("3400"))
        intent = strategy.decide(market)
        strategy.on_intent_executed(intent, success=True, result=None)

        # Price drops 3% (beyond 2% threshold)
        market_drop = _make_market(Decimal("3298"))
        intent = strategy.decide(market_drop)
        assert intent is not None
        assert intent.intent_type.value == "BORROW"
        assert strategy._state == "borrowing"

    def test_no_borrow_on_small_drop(self, strategy):
        """Small price drops should not trigger borrows."""
        market = _make_market(Decimal("3400"))
        intent = strategy.decide(market)
        strategy.on_intent_executed(intent, success=True, result=None)

        # Price drops 1% (under 2% threshold)
        market_small_drop = _make_market(Decimal("3366"))
        intent = strategy.decide(market_small_drop)
        assert intent is not None
        assert intent.intent_type.value == "HOLD"

    def test_price_rise_triggers_repay(self, strategy):
        """After borrowing, price rise beyond threshold triggers repay."""
        market = _make_market(Decimal("3400"))
        intent = strategy.decide(market)
        strategy.on_intent_executed(intent, success=True, result=None)

        market_drop = _make_market(Decimal("3298"))
        intent = strategy.decide(market_drop)
        strategy.on_intent_executed(intent, success=True, result=None)
        assert strategy._state == "borrowed"

        # Price rises 4% from reference (beyond 3% threshold)
        market_rise = _make_market(Decimal("3430"))
        intent = strategy.decide(market_rise)
        assert intent is not None
        assert intent.intent_type.value == "REPAY"

    def test_repay_returns_to_supplied(self, strategy):
        """After repay, state returns to supplied (can borrow again)."""
        market = _make_market(Decimal("3400"))
        intent = strategy.decide(market)
        strategy.on_intent_executed(intent, success=True, result=None)

        market_drop = _make_market(Decimal("3298"))
        intent = strategy.decide(market_drop)
        strategy.on_intent_executed(intent, success=True, result=None)

        market_rise = _make_market(Decimal("3430"))
        intent = strategy.decide(market_rise)
        strategy.on_intent_executed(intent, success=True, result=None)

        assert strategy._state == "supplied"
        assert strategy._borrowed_amount == Decimal("0")
        assert strategy._borrow_cycles == 1


class TestBorrowCycleLimit:
    def _run_borrow_cycle(self, strategy, base_price=3400):
        """Run one supply -> borrow -> repay cycle."""
        if strategy._state == "idle":
            market = _make_market(Decimal(str(base_price)))
            intent = strategy.decide(market)
            strategy.on_intent_executed(intent, success=True, result=None)

        # Borrow on drop
        drop_price = int(base_price * 0.97)
        market_drop = _make_market(Decimal(str(drop_price)))
        intent = strategy.decide(market_drop)
        if intent.intent_type.value == "HOLD":
            return  # Max cycles reached
        strategy.on_intent_executed(intent, success=True, result=None)

        # Repay on rise
        rise_price = int(drop_price * 1.04)
        market_rise = _make_market(Decimal(str(rise_price)))
        intent = strategy.decide(market_rise)
        strategy.on_intent_executed(intent, success=True, result=None)

    def test_max_cycles_enforced(self, strategy):
        """After max_borrow_cycles, strategy holds instead of borrowing."""
        # Run 3 full cycles
        for _ in range(3):
            self._run_borrow_cycle(strategy)

        assert strategy._borrow_cycles == 3

        # Next drop should produce HOLD, not BORROW
        market_drop = _make_market(Decimal("3200"))
        strategy._reference_price = Decimal("3400")
        intent = strategy.decide(market_drop)
        assert intent.intent_type.value == "HOLD"
        assert "Max borrow cycles" in intent.reason


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

        original_ref_price = strategy._reference_price

        market_drop = _make_market(Decimal("3298"))
        intent = strategy.decide(market_drop)
        strategy.on_intent_executed(intent, success=False, result=None)
        assert strategy._state == "supplied"
        # Borrow cycles should NOT increment on failure
        assert strategy._borrow_cycles == 0
        # Reference price should be restored to pre-borrow value
        assert strategy._reference_price == original_ref_price

    def test_stuck_state_recovery(self, strategy):
        """Stuck transitional state reverts on next decide(), retry still works."""
        strategy._state = "supplying"
        strategy._previous_stable_state = "idle"

        market = _make_market(Decimal("3400"))
        intent = strategy.decide(market)
        # Should revert to idle (hold this tick due to state revert)
        assert strategy._state == "idle"

        # Next tick should supply again from idle
        intent = strategy.decide(market)
        assert intent.intent_type.value == "SUPPLY"
        assert strategy._state == "supplying"


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
        strategy._borrowed_amount = Decimal("13.60")

        from almanak.framework.teardown import TeardownMode
        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 2
        assert intents[0].intent_type.value == "REPAY"
        assert intents[1].intent_type.value == "WITHDRAW"

    def test_withdraw_clears_supply_state(self, strategy):
        """After successful withdraw (teardown), state returns to idle with zero supply."""
        strategy._state = "supplied"
        strategy._supplied_amount = Decimal("0.01")

        from almanak.framework.teardown import TeardownMode
        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 1

        strategy.on_intent_executed(intents[0], success=True, result=None)
        assert strategy._state == "idle"
        assert strategy._supplied_amount == Decimal("0")
        # After teardown, no positions should be reported
        summary = strategy.get_open_positions()
        assert len(summary.positions) == 0

    def test_open_positions_empty_when_idle(self, strategy):
        summary = strategy.get_open_positions()
        assert len(summary.positions) == 0

    def test_open_positions_with_supply_and_borrow(self, strategy):
        strategy._supplied_amount = Decimal("0.01")
        strategy._borrowed_amount = Decimal("13.60")
        summary = strategy.get_open_positions()
        assert len(summary.positions) == 2
        types = {p.position_type.value for p in summary.positions}
        assert "SUPPLY" in types
        assert "BORROW" in types


class TestPersistence:
    def test_round_trip(self, strategy):
        """State can be saved and restored."""
        strategy._state = "borrowed"
        strategy._supplied_amount = Decimal("0.01")
        strategy._borrowed_amount = Decimal("13.60")
        strategy._reference_price = Decimal("3298")
        strategy._borrow_cycles = 2
        strategy._tick_count = 5

        state = strategy.get_persistent_state()

        new_strategy = _create_strategy()
        new_strategy.load_persistent_state(state)

        assert new_strategy._state == "borrowed"
        assert new_strategy._supplied_amount == Decimal("0.01")
        assert new_strategy._borrowed_amount == Decimal("13.60")
        assert new_strategy._reference_price == Decimal("3298")
        assert new_strategy._borrow_cycles == 2
        assert new_strategy._tick_count == 5

    def test_status_includes_cycles(self, strategy):
        strategy._borrow_cycles = 2
        strategy._tick_count = 7
        status = strategy.get_status()
        assert status["borrow_cycles"] == 2
        assert status["tick_count"] == 7
