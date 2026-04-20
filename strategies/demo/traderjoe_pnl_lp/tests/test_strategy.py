"""Unit tests for the TraderJoe V2 PnL LP Strategy.

Tests validate:
1. Strategy initialization with config
2. State machine transitions (idle -> opening -> active -> closing -> idle)
3. Price-based rebalance logic
4. Teardown interface compliance
5. PnL backtester compatibility (decide returns valid intents)

First PnL backtest test coverage for Avalanche chain (VIB-1374).
"""

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from strategies.demo.traderjoe_pnl_lp.strategy import TraderJoePnLLPStrategy


def _create_strategy(config_overrides=None):
    """Create a strategy instance with default config."""
    config = {
        "pool": "WAVAX/USDC/20",
        "range_width_pct": "0.10",
        "amount_x": "0.5",
        "amount_y": "10",
        "num_bins": 11,
        "rebalance_threshold_pct": "0.08",
        "chain": "avalanche",
    }
    if config_overrides:
        config.update(config_overrides)
    return TraderJoePnLLPStrategy(
        config=config,
        chain="avalanche",
        wallet_address="0x" + "a" * 40,
    )


@pytest.fixture
def strategy():
    return _create_strategy()


def _make_market(wavax_price: Decimal, wavax_balance: Decimal = Decimal("100"), usdc_balance: Decimal = Decimal("10000")) -> MagicMock:
    """Create a mock MarketSnapshot."""
    market = MagicMock()
    market.price.side_effect = lambda token: {
        "WAVAX": wavax_price,
        "USDC": Decimal("1"),
    }.get(token, Decimal("1"))

    wavax_bal = MagicMock()
    wavax_bal.balance = wavax_balance
    wavax_bal.balance_usd = wavax_balance * wavax_price

    usdc_bal = MagicMock()
    usdc_bal.balance = usdc_balance
    usdc_bal.balance_usd = usdc_balance

    market.balance.side_effect = lambda token: {
        "WAVAX": wavax_bal,
        "USDC": usdc_bal,
    }.get(token, usdc_bal)

    return market


class TestInitialization:
    def test_default_config(self, strategy):
        assert strategy.token_x == "WAVAX"
        assert strategy.token_y == "USDC"
        assert strategy.bin_step == 20
        assert strategy.amount_x == Decimal("0.5")
        assert strategy.amount_y == Decimal("10")
        assert strategy.num_bins == 11
        assert strategy._state == "idle"

    def test_custom_config(self):
        s = _create_strategy({
            "pool": "WETH/USDT/25",
            "amount_x": "1.0",
            "amount_y": "3000",
            "num_bins": 21,
        })
        assert s.token_x == "WETH"
        assert s.token_y == "USDT"
        assert s.bin_step == 25
        assert s.amount_x == Decimal("1.0")


class TestDecisionLogic:
    def test_first_tick_opens_lp(self, strategy):
        """First tick should open LP position."""
        market = _make_market(Decimal("30"))
        intent = strategy.decide(market)
        assert intent is not None
        assert intent.intent_type.value == "LP_OPEN"
        assert strategy._state == "opening"

    def test_opening_state_auto_advances_to_active(self, strategy):
        """Stuck in opening auto-advances to active (PnL backtester compat)."""
        strategy._state = "opening"
        strategy._entry_price = Decimal("30")
        market = _make_market(Decimal("30"))
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert strategy._state == "active"

    def test_closing_state_auto_advances_to_idle(self, strategy):
        """Stuck in closing auto-advances to idle (PnL backtester compat)."""
        strategy._state = "closing"
        strategy._entry_price = Decimal("30")
        market = _make_market(Decimal("30"))
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert strategy._state == "idle"
        assert strategy._entry_price is None

    def test_open_success_transitions_to_active(self, strategy):
        """After successful LP_OPEN, state transitions to active."""
        market = _make_market(Decimal("30"))
        intent = strategy.decide(market)
        strategy.on_intent_executed(intent, success=True, result=None)
        assert strategy._state == "active"
        assert strategy._entry_price == Decimal("30")

    def test_price_within_range_holds(self, strategy):
        """When price is within rebalance threshold, strategy holds."""
        strategy._state = "active"
        strategy._entry_price = Decimal("30")

        # Price moves 5% (below 8% threshold)
        market = _make_market(Decimal("31.5"))
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"

    def test_price_beyond_threshold_triggers_close(self, strategy):
        """When price moves beyond rebalance threshold, close LP."""
        strategy._state = "active"
        strategy._entry_price = Decimal("30")

        # Price moves 10% (beyond 8% threshold)
        market = _make_market(Decimal("33"))
        intent = strategy.decide(market)
        assert intent.intent_type.value == "LP_CLOSE"
        assert strategy._state == "closing"

    def test_close_success_returns_to_idle(self, strategy):
        """After successful LP_CLOSE, state returns to idle."""
        strategy._state = "closing"
        strategy._entry_price = Decimal("30")
        strategy._rebalance_count = 0

        intent = MagicMock()
        strategy.on_intent_executed(intent, success=True, result=None)

        assert strategy._state == "idle"
        assert strategy._entry_price is None
        assert strategy._rebalance_count == 1

    def test_pnl_backtest_lifecycle(self, strategy):
        """Simulate PnL backtester: decide() only, no on_intent_executed callbacks."""
        # Tick 1: idle -> LP_OPEN (state becomes opening)
        market = _make_market(Decimal("30"))
        intent = strategy.decide(market)
        assert intent.intent_type.value == "LP_OPEN"
        assert strategy._state == "opening"

        # Tick 2: stuck recovery auto-advances opening -> active
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert strategy._state == "active"

        # Tick 3: price within range -> hold
        market = _make_market(Decimal("31"))
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"

        # Tick 4: price beyond threshold -> LP_CLOSE (state becomes closing)
        market = _make_market(Decimal("33"))
        intent = strategy.decide(market)
        assert intent.intent_type.value == "LP_CLOSE"
        assert strategy._state == "closing"

        # Tick 5: stuck recovery auto-advances closing -> idle
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert strategy._state == "idle"

        # Tick 6: idle again -> new LP_OPEN
        intent = strategy.decide(market)
        assert intent.intent_type.value == "LP_OPEN"


class TestFailureRecovery:
    def test_open_failure_reverts_to_idle(self, strategy):
        """LP_OPEN failure reverts state to idle and clears transient fields."""
        market = _make_market(Decimal("30"))
        intent = strategy.decide(market)
        strategy.on_intent_executed(intent, success=False, result=None)
        assert strategy._state == "idle"
        assert strategy._entry_price is None
        assert strategy._position_bin_ids == []

    def test_close_failure_reverts_to_active(self, strategy):
        """LP_CLOSE failure reverts state to active."""
        strategy._state = "closing"
        intent = MagicMock()
        strategy.on_intent_executed(intent, success=False, result=None)
        assert strategy._state == "active"


class TestTeardown:
    def test_supports_teardown(self, strategy):
        assert strategy.supports_teardown() is True

    def test_teardown_with_active_position(self, strategy):
        """Teardown with active LP generates LP_CLOSE intent."""
        strategy._state = "active"

        from almanak.framework.teardown import TeardownMode
        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 1
        assert intents[0].intent_type.value == "LP_CLOSE"

    def test_teardown_when_idle(self, strategy):
        """Teardown when idle generates no intents."""
        from almanak.framework.teardown import TeardownMode
        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 0

    def test_open_positions_when_active(self, strategy):
        """Active LP appears in open positions."""
        strategy._state = "active"
        summary = strategy.get_open_positions()
        assert len(summary.positions) == 1
        assert summary.positions[0].position_type.value == "LP"

    def test_open_positions_when_idle(self, strategy):
        """No positions when strategy is idle."""
        summary = strategy.get_open_positions()
        assert len(summary.positions) == 0


class TestPersistence:
    def test_round_trip(self, strategy):
        """State can be saved and restored."""
        strategy._state = "active"
        strategy._entry_price = Decimal("30")
        strategy._position_bin_ids = [8388608, 8388609, 8388610]
        strategy._rebalance_count = 2

        state = strategy.get_persistent_state()

        new_strategy = _create_strategy()
        new_strategy.load_persistent_state(state)

        assert new_strategy._state == "active"
        assert new_strategy._entry_price == Decimal("30")
        assert new_strategy._position_bin_ids == [8388608, 8388609, 8388610]
        assert new_strategy._rebalance_count == 2


class TestStrategyRegistration:
    def test_strategy_is_discoverable(self):
        """Verify demo_traderjoe_pnl_lp is in the strategy registry."""
        from almanak.framework.strategies import list_strategies
        strategies = list_strategies()
        assert "demo_traderjoe_pnl_lp" in strategies

    def test_strategy_metadata(self):
        """Verify strategy metadata."""
        from almanak.framework.strategies import get_strategy
        strategy_class = get_strategy("demo_traderjoe_pnl_lp")
        metadata = strategy_class.STRATEGY_METADATA
        assert "avalanche" in metadata.supported_chains
        assert "traderjoe_v2" in metadata.supported_protocols
        assert "LP_OPEN" in metadata.intent_types
        assert "LP_CLOSE" in metadata.intent_types
        assert metadata.default_chain == "avalanche"
