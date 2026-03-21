"""Unit tests for the Aerodrome PnL LP Strategy.

Tests validate:
1. Strategy initialization with config
2. State machine transitions (idle -> opening -> active -> closing -> idle)
3. RSI-based LP entry/exit logic
4. Teardown interface compliance
5. PnL backtester compatibility (decide returns valid intents)

First PnL backtest test coverage for Base chain with Aerodrome (VIB-1626).
"""

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from strategies.demo.aerodrome_pnl_lp.strategy import AerodromePnLLPStrategy


def _create_strategy(config_overrides=None):
    """Create a strategy instance with default config."""
    config = {
        "pool": "WETH/USDC",
        "stable": False,
        "amount0": "0.001",
        "amount1": "3",
        "rsi_period": 14,
        "rsi_overbought": 70,
        "rsi_oversold": 30,
        "chain": "base",
    }
    if config_overrides:
        config.update(config_overrides)
    return AerodromePnLLPStrategy(
        config=config,
        chain="base",
        wallet_address="0x" + "a" * 40,
    )


@pytest.fixture
def strategy():
    return _create_strategy()


def _make_market(weth_price: Decimal) -> MagicMock:
    """Create a mock MarketSnapshot."""
    market = MagicMock()
    prices = {
        "WETH": weth_price,
        "USDC": Decimal("1"),
    }
    market.price.side_effect = lambda token: prices[token]
    return market


class TestInitialization:
    def test_default_config(self, strategy):
        assert strategy.token0 == "WETH"
        assert strategy.token1 == "USDC"
        assert strategy.stable is False
        assert strategy.amount0 == Decimal("0.001")
        assert strategy.amount1 == Decimal("3")
        assert strategy.rsi_period == 14
        assert strategy._state == "idle"

    def test_custom_config(self):
        s = _create_strategy({
            "pool": "cbETH/USDC",
            "stable": True,
            "amount0": "0.5",
            "amount1": "1000",
            "rsi_period": 7,
            "rsi_overbought": 80,
            "rsi_oversold": 20,
        })
        assert s.token0 == "cbETH"
        assert s.token1 == "USDC"
        assert s.stable is True
        assert s.amount0 == Decimal("0.5")
        assert s.rsi_period == 7

    def test_rejects_zero_rsi_period(self):
        with pytest.raises(ValueError, match="rsi_period must be greater than 0"):
            _create_strategy({"rsi_period": 0})


class TestDecideLogic:
    def test_first_tick_opens_lp(self, strategy):
        """First tick should trigger LP_OPEN (RSI starts oversold)."""
        market = _make_market(Decimal("3400"))
        intent = strategy.decide(market)
        assert intent is not None
        assert intent.intent_type.value == "LP_OPEN"
        assert strategy._state == "opening"

    def test_lp_open_fields(self, strategy):
        """LP_OPEN intent should have correct pool and protocol fields."""
        market = _make_market(Decimal("3400"))
        intent = strategy.decide(market)
        assert intent.pool == "WETH/USDC/volatile"
        assert intent.protocol == "aerodrome"

    def test_state_machine_full_cycle(self, strategy):
        """Verify idle -> opening -> active -> hold -> closing -> idle cycle."""
        market = _make_market(Decimal("3400"))

        # Tick 1: idle -> opening (RSI=20, oversold)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "LP_OPEN"
        assert strategy._state == "opening"

        # Simulate successful fill via on_intent_executed callback
        strategy.on_intent_executed(intent, True)
        assert strategy._state == "active"

        # Manually set tick_count to enter overbought phase.
        # cycle_length = rsi_period * 3 = 42, overbought starts at tick 29.
        # _estimate_rsi uses (tick_count - 1), called AFTER incrementing tick_count.
        strategy._tick_count = 28  # next decide() increments to 29, RSI=80
        intent = strategy.decide(market)
        assert intent.intent_type.value == "LP_CLOSE"
        assert strategy._state == "closing"

    def test_hold_when_price_unavailable(self, strategy):
        """Should hold when price data is unavailable."""
        market = MagicMock()
        market.price.side_effect = ValueError("No price data")
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"

    def test_rsi_cycling(self, strategy):
        """Verify RSI estimate cycles correctly through phases."""
        # tick_count is 1-based at runtime (incremented before _estimate_rsi)
        # Period 1 (ticks 1-14): oversold (20)
        strategy._tick_count = 1
        assert strategy._estimate_rsi() == 20
        strategy._tick_count = 14
        assert strategy._estimate_rsi() == 20  # still in first third

        # Period 2 (ticks 15-28): neutral (50)
        strategy._tick_count = 15
        assert strategy._estimate_rsi() == 50

        # Period 3 (ticks 29-42): overbought (80)
        strategy._tick_count = 29
        assert strategy._estimate_rsi() == 80

        # Cycle repeats
        strategy._tick_count = 43
        assert strategy._estimate_rsi() == 20


class TestOnIntentExecuted:
    def test_successful_open(self, strategy):
        strategy._state = "opening"
        strategy.on_intent_executed(MagicMock(), success=True)
        assert strategy._state == "active"

    def test_failed_open_reverts(self, strategy):
        strategy._state = "opening"
        strategy._entry_price = Decimal("3400")
        strategy.on_intent_executed(MagicMock(), success=False)
        assert strategy._state == "idle"
        assert strategy._entry_price is None

    def test_successful_close(self, strategy):
        strategy._state = "closing"
        strategy._entry_price = Decimal("3400")
        strategy.on_intent_executed(MagicMock(), success=True)
        assert strategy._state == "idle"
        assert strategy._entry_price is None

    def test_failed_close_reverts(self, strategy):
        strategy._state = "closing"
        strategy.on_intent_executed(MagicMock(), success=False)
        assert strategy._state == "active"


class TestPersistence:
    def test_get_persistent_state(self, strategy):
        strategy._state = "active"
        strategy._entry_price = Decimal("3400")
        strategy._tick_count = 5
        state = strategy.get_persistent_state()
        assert state["state"] == "active"
        assert state["entry_price"] == "3400"
        assert state["tick_count"] == 5

    def test_load_persistent_state(self, strategy):
        strategy.load_persistent_state({
            "state": "active",
            "entry_price": "3200",
            "tick_count": 10,
        })
        assert strategy._state == "active"
        assert strategy._entry_price == Decimal("3200")
        assert strategy._tick_count == 10


class TestTeardown:
    def test_supports_teardown(self, strategy):
        assert strategy.supports_teardown() is True

    def test_no_positions_when_idle(self, strategy):
        summary = strategy.get_open_positions()
        assert len(summary.positions) == 0

    def test_has_position_when_active(self, strategy):
        strategy._state = "active"
        summary = strategy.get_open_positions()
        assert len(summary.positions) == 1
        pos = summary.positions[0]
        assert pos.protocol == "aerodrome"
        assert pos.position_id == "WETH/USDC/volatile"

    def test_teardown_intents_when_active(self, strategy):
        strategy._state = "active"
        intents = strategy.generate_teardown_intents(mode=MagicMock())
        assert len(intents) == 1
        assert intents[0].intent_type.value == "LP_CLOSE"

    def test_no_teardown_intents_when_idle(self, strategy):
        intents = strategy.generate_teardown_intents(mode=MagicMock())
        assert len(intents) == 0
