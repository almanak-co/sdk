"""Unit tests for the Aerodrome Sweep LP Strategy.

Tests validate:
1. Strategy initialization with sweepable config
2. RSI-gated LP open/close decisions
3. Reentry cooldown enforcement
4. LP cycle limit enforcement
5. Parameter override via config (sweep simulation)
6. Teardown interface compliance
7. State persistence round-trip
"""

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from strategies.demo.aerodrome_sweep_lp.strategy import AerodromeSweepLPStrategy


def _create_strategy(config_overrides=None):
    """Create a strategy instance with default config."""
    config = {
        "pool": "WETH/USDC",
        "stable": False,
        "amount0": "0.001",
        "amount1": "3",
        "rsi_period": 14,
        "rsi_oversold": "30",
        "rsi_overbought": "70",
        "reentry_cooldown": 2,
        "max_lp_cycles": 5,
        "chain": "base",
    }
    if config_overrides:
        config.update(config_overrides)
    return AerodromeSweepLPStrategy(
        config=config,
        chain="base",
        wallet_address="0x" + "b" * 40,
    )


@pytest.fixture
def strategy():
    return _create_strategy()


def _make_market(rsi_value: Decimal, has_funds: bool = True) -> MagicMock:
    """Create a mock MarketSnapshot with given RSI and balance."""
    market = MagicMock()

    rsi_mock = MagicMock()
    rsi_mock.value = rsi_value
    market.rsi.return_value = rsi_mock

    bal_mock = MagicMock()
    bal_mock.balance = Decimal("10") if has_funds else Decimal("0")
    market.balance.return_value = bal_mock

    return market


class TestInitialization:
    def test_default_config(self, strategy):
        assert strategy.pool == "WETH/USDC"
        assert strategy.token0 == "WETH"
        assert strategy.token1 == "USDC"
        assert strategy.amount0 == Decimal("0.001")
        assert strategy.rsi_oversold == Decimal("30")
        assert strategy.rsi_overbought == Decimal("70")
        assert strategy.reentry_cooldown == 2
        assert strategy.max_lp_cycles == 5
        assert strategy._has_position is False
        assert strategy._lp_cycles == 0

    def test_sweep_parameter_override(self):
        """Sweep engine overrides config parameters — verify they take effect."""
        s = _create_strategy({
            "rsi_oversold": "25",
            "rsi_overbought": "75",
            "amount0": "0.005",
            "reentry_cooldown": 3,
        })
        assert s.rsi_oversold == Decimal("25")
        assert s.rsi_overbought == Decimal("75")
        assert s.amount0 == Decimal("0.005")
        assert s.reentry_cooldown == 3


class TestLPOpenDecision:
    def test_open_when_rsi_in_range(self, strategy):
        """Opens LP when RSI is within range and has funds."""
        market = _make_market(Decimal("50"))
        intent = strategy.decide(market)
        assert intent is not None
        assert intent.intent_type.value == "LP_OPEN"

    def test_hold_when_rsi_outside_range(self, strategy):
        """Holds when RSI is outside entry range."""
        market = _make_market(Decimal("20"))  # Below oversold=30
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"

    def test_hold_when_no_funds(self, strategy):
        """Holds when insufficient funds."""
        market = _make_market(Decimal("50"), has_funds=False)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"

    def test_open_at_boundary(self, strategy):
        """Opens LP at exact RSI boundary values."""
        market = _make_market(Decimal("30"))  # Exact oversold
        intent = strategy.decide(market)
        assert intent.intent_type.value == "LP_OPEN"

        strategy2 = _create_strategy()
        market2 = _make_market(Decimal("70"))  # Exact overbought
        intent2 = strategy2.decide(market2)
        assert intent2.intent_type.value == "LP_OPEN"


class TestLPCloseDecision:
    def _open_position(self, strategy):
        """Helper to open a position."""
        market = _make_market(Decimal("50"))
        intent = strategy.decide(market)
        strategy.on_intent_executed(intent, success=True, result=None)

    def test_close_on_rsi_extreme(self, strategy):
        """Closes LP when RSI becomes extreme."""
        self._open_position(strategy)
        assert strategy._has_position is True

        market = _make_market(Decimal("75"))  # Above overbought=70
        intent = strategy.decide(market)
        assert intent.intent_type.value == "LP_CLOSE"

    def test_hold_when_rsi_still_in_range(self, strategy):
        """Keeps LP open when RSI is in range."""
        self._open_position(strategy)

        market = _make_market(Decimal("50"))
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"

    def test_ticks_with_position_tracked(self, strategy):
        """Tracks how many ticks the position has been open."""
        self._open_position(strategy)

        market = _make_market(Decimal("50"))
        strategy.decide(market)
        assert strategy._ticks_with_position == 1

        strategy.decide(market)
        assert strategy._ticks_with_position == 2


class TestCooldown:
    def _run_one_cycle(self, strategy):
        """Open and close one LP cycle."""
        # Open
        market = _make_market(Decimal("50"))
        intent = strategy.decide(market)
        strategy.on_intent_executed(intent, success=True, result=None)
        # Close
        market = _make_market(Decimal("75"))
        intent = strategy.decide(market)
        strategy.on_intent_executed(intent, success=True, result=None)

    def test_cooldown_after_close(self, strategy):
        """After closing, holds for cooldown ticks before re-opening."""
        self._run_one_cycle(strategy)
        assert strategy._cooldown_remaining == 2

        # Tick 1: cooldown
        market = _make_market(Decimal("50"))
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "Cooldown" in intent.reason

        # Tick 2: cooldown
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"

        # Tick 3: cooldown expired, should open
        intent = strategy.decide(market)
        assert intent.intent_type.value == "LP_OPEN"

    def test_zero_cooldown(self):
        """With cooldown=0, can re-open immediately."""
        s = _create_strategy({"reentry_cooldown": 0})

        # Open and close
        market = _make_market(Decimal("50"))
        intent = s.decide(market)
        s.on_intent_executed(intent, success=True, result=None)

        market = _make_market(Decimal("75"))
        intent = s.decide(market)
        s.on_intent_executed(intent, success=True, result=None)

        # Should be able to open immediately
        market = _make_market(Decimal("50"))
        intent = s.decide(market)
        assert intent.intent_type.value == "LP_OPEN"


class TestCycleLimit:
    def _run_cycles(self, strategy, n):
        """Run n open/close cycles."""
        for _ in range(n):
            # Open
            market = _make_market(Decimal("50"))
            intent = strategy.decide(market)
            if intent.intent_type.value == "HOLD":
                return intent
            strategy.on_intent_executed(intent, success=True, result=None)

            # Close
            market = _make_market(Decimal("75"))
            intent = strategy.decide(market)
            strategy.on_intent_executed(intent, success=True, result=None)

            # Drain cooldown
            for _ in range(strategy.reentry_cooldown):
                strategy.decide(_make_market(Decimal("50")))

    def test_max_cycles_enforced(self):
        """After max_lp_cycles, holds instead of opening."""
        s = _create_strategy({"max_lp_cycles": 2, "reentry_cooldown": 0})
        self._run_cycles(s, 2)
        assert s._lp_cycles == 2

        market = _make_market(Decimal("50"))
        intent = s.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "Max LP cycles" in intent.reason


class TestSweepParameterVariation:
    """Test that different sweep parameter values produce different behavior."""

    def test_narrow_rsi_range_opens_less(self):
        """Narrow RSI range (40-60) should reject RSI=35."""
        narrow = _create_strategy({"rsi_oversold": "40", "rsi_overbought": "60"})
        market = _make_market(Decimal("35"))
        intent = narrow.decide(market)
        assert intent.intent_type.value == "HOLD"

    def test_wide_rsi_range_opens_more(self):
        """Wide RSI range (20-80) should accept RSI=35."""
        wide = _create_strategy({"rsi_oversold": "20", "rsi_overbought": "80"})
        market = _make_market(Decimal("35"))
        intent = wide.decide(market)
        assert intent.intent_type.value == "LP_OPEN"

    def test_different_amounts_in_intent(self):
        """Verify sweep amount overrides appear in the generated intent."""
        s = _create_strategy({"amount0": "0.005", "amount1": "5"})
        market = _make_market(Decimal("50"))
        intent = s.decide(market)
        assert intent.intent_type.value == "LP_OPEN"
        assert intent.amount0 == Decimal("0.005")
        assert intent.amount1 == Decimal("5")


class TestTeardown:
    def test_supports_teardown(self, strategy):
        assert strategy.supports_teardown() is True

    def test_teardown_with_position(self, strategy):
        strategy._has_position = True
        strategy._lp_token_balance = Decimal("1000")

        from almanak.framework.teardown import TeardownMode
        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 1
        assert intents[0].intent_type.value == "LP_CLOSE"

    def test_teardown_without_position(self, strategy):
        from almanak.framework.teardown import TeardownMode
        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 0

    def test_open_positions_with_lp(self, strategy):
        strategy._has_position = True
        strategy._lp_cycles = 3
        summary = strategy.get_open_positions()
        assert len(summary.positions) == 1
        assert summary.positions[0].position_type.value == "LP"
        assert summary.positions[0].details["cycle"] == 3


class TestPersistence:
    def test_round_trip(self, strategy):
        strategy._has_position = True
        strategy._lp_token_balance = Decimal("1234")
        strategy._lp_cycles = 3
        strategy._cooldown_remaining = 1
        strategy._tick_count = 10
        strategy._ticks_with_position = 4

        state = strategy.get_persistent_state()

        new_strategy = _create_strategy()
        new_strategy.load_persistent_state(state)

        assert new_strategy._has_position is True
        assert new_strategy._lp_token_balance == Decimal("1234")
        assert new_strategy._lp_cycles == 3
        assert new_strategy._cooldown_remaining == 1
        assert new_strategy._tick_count == 10
        assert new_strategy._ticks_with_position == 4

    def test_status_includes_sweep_params(self, strategy):
        status = strategy.get_status()
        assert status["rsi_oversold"] == "30"
        assert status["rsi_overbought"] == "70"
        assert status["reentry_cooldown"] == 2
