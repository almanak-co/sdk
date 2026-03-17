"""Tests for the Aerodrome LP Sweep strategy decision logic.

Validates:
1. Strategy instantiation with config
2. LP open intent generation
3. Rebalance threshold triggering
4. Position tracking via on_intent_executed
5. Teardown support
6. State persistence and restoration

Kitchen Loop iteration 83, VIB-1360.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock, PropertyMock

import pytest


@pytest.fixture
def sweep_config():
    """Default sweep strategy config dict."""
    return {
        "pool": "WETH/USDC",
        "stable": False,
        "amount0": "0.01",
        "amount1": "30",
        "rebalance_threshold_pct": "5.0",
        "max_position_value_usd": "500",
    }


@pytest.fixture
def strategy(sweep_config):
    """Instantiate the sweep strategy with mock config."""
    from strategies.incubating.aerodrome_lp_sweep.strategy import (
        AerodromeLPSweepConfig,
        AerodromeLPSweepStrategy,
    )

    config = AerodromeLPSweepConfig(**sweep_config)
    # Use mock to avoid IntentStrategy __init__ complexities
    strat = AerodromeLPSweepStrategy.__new__(AerodromeLPSweepStrategy)
    strat.config = config
    strat._chain = "base"
    strat._wallet_address = "0x" + "0" * 40
    strat._strategy_id = "test-sweep"

    # Re-run init logic
    strat.pool = config.pool
    strat.token0_symbol = "WETH"
    strat.token1_symbol = "USDC"
    strat.stable = config.stable
    strat.amount0 = config.amount0
    strat.amount1 = config.amount1
    strat.rebalance_threshold_pct = config.rebalance_threshold_pct
    strat.max_position_value_usd = config.max_position_value_usd
    strat._has_position = False
    strat._entry_price = Decimal("0")
    strat._entry_token0_usd = Decimal("0")
    strat._entry_token1_usd = Decimal("0")
    strat._lp_token_balance = Decimal("0")
    strat._ticks_in_position = 0
    return strat


def _mock_market(eth_price: str = "3000", usdc_price: str = "1") -> MagicMock:
    """Create a mock MarketSnapshot."""
    market = MagicMock()

    def price_fn(symbol):
        if symbol == "WETH":
            return Decimal(eth_price)
        if symbol == "USDC":
            return Decimal(usdc_price)
        raise ValueError(f"Unknown token: {symbol}")

    def balance_fn(symbol):
        bal = MagicMock()
        bal.balance = Decimal("100")
        return bal

    market.price = MagicMock(side_effect=price_fn)
    market.balance = MagicMock(side_effect=balance_fn)
    return market


class TestAerodromeLPSweepConfig:
    """Test config dataclass."""

    def test_string_conversion(self):
        from strategies.incubating.aerodrome_lp_sweep.strategy import AerodromeLPSweepConfig

        config = AerodromeLPSweepConfig(
            amount0="0.05",
            amount1="100",
            rebalance_threshold_pct="3.0",
            max_position_value_usd="1000",
            stable="true",
        )
        assert config.amount0 == Decimal("0.05")
        assert config.amount1 == Decimal("100")
        assert config.rebalance_threshold_pct == Decimal("3.0")
        assert config.max_position_value_usd == Decimal("1000")
        assert config.stable is True

    def test_float_conversion(self):
        from strategies.incubating.aerodrome_lp_sweep.strategy import AerodromeLPSweepConfig

        config = AerodromeLPSweepConfig(amount0=0.01, amount1=30.0)
        assert config.amount0 == Decimal("0.01")
        assert config.amount1 == Decimal("30.0")

    def test_to_dict(self):
        from strategies.incubating.aerodrome_lp_sweep.strategy import AerodromeLPSweepConfig

        config = AerodromeLPSweepConfig()
        d = config.to_dict()
        assert d["pool"] == "WETH/USDC"
        assert d["stable"] is False
        assert isinstance(d["amount0"], str)


class TestAerodromeLPSweepDecision:
    """Test strategy decision logic."""

    def test_opens_position_when_none_exists(self, strategy):
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent is not None
        assert intent.intent_type.value == "LP_OPEN"

    def test_holds_with_existing_position(self, strategy):
        strategy._has_position = True
        strategy._entry_price = Decimal("3000")
        market = _mock_market("3000")  # No price change
        intent = strategy.decide(market)
        assert intent is not None
        assert intent.intent_type.value == "HOLD"

    def test_rebalance_triggered_on_price_move(self, strategy):
        """Rebalance when price moves beyond threshold."""
        strategy._has_position = True
        strategy._entry_price = Decimal("3000")
        strategy.rebalance_threshold_pct = Decimal("5.0")

        # Price moved 6% (3000 -> 3180) - should trigger close for rebalance
        market = _mock_market("3180")
        intent = strategy.decide(market)
        assert intent is not None
        assert intent.intent_type.value == "LP_CLOSE"

    def test_no_rebalance_within_threshold(self, strategy):
        """Hold when price is within threshold."""
        strategy._has_position = True
        strategy._entry_price = Decimal("3000")
        strategy.rebalance_threshold_pct = Decimal("5.0")

        # Price moved 3% - within threshold
        market = _mock_market("3090")
        intent = strategy.decide(market)
        assert intent is not None
        assert intent.intent_type.value == "HOLD"

    def test_max_position_value_cap(self, strategy):
        """Hold if position value exceeds max."""
        strategy.max_position_value_usd = Decimal("10")  # Very low cap
        market = _mock_market("3000")
        intent = strategy.decide(market)
        assert intent is not None
        assert intent.intent_type.value == "HOLD"
        assert "exceeds max" in intent.reason

    def test_ticks_in_position_tracked(self, strategy):
        """Tick counter increments each decide() call."""
        strategy._has_position = True
        strategy._entry_price = Decimal("3000")
        market = _mock_market("3000")

        strategy.decide(market)
        assert strategy._ticks_in_position == 1

        strategy.decide(market)
        assert strategy._ticks_in_position == 2


class TestAerodromeLPSweepLifecycle:
    """Test on_intent_executed callbacks."""

    def test_open_updates_state(self, strategy):
        intent = MagicMock()
        intent.intent_type.value = "LP_OPEN"
        strategy.on_intent_executed(intent, success=True, result=None)
        assert strategy._has_position is True
        assert strategy._lp_token_balance == Decimal("1")
        assert strategy._ticks_in_position == 0

    def test_close_resets_state(self, strategy):
        strategy._has_position = True
        strategy._entry_price = Decimal("3000")
        strategy._lp_token_balance = Decimal("1")
        strategy._ticks_in_position = 5

        intent = MagicMock()
        intent.intent_type.value = "LP_CLOSE"
        strategy.on_intent_executed(intent, success=True, result=None)
        assert strategy._has_position is False
        assert strategy._entry_price == Decimal("0")
        assert strategy._lp_token_balance == Decimal("0")

    def test_failed_execution_no_state_change(self, strategy):
        intent = MagicMock()
        intent.intent_type.value = "LP_OPEN"
        strategy.on_intent_executed(intent, success=False, result=None)
        assert strategy._has_position is False


class TestAerodromeLPSweepPersistence:
    """Test state persistence and restoration."""

    def test_get_persistent_state(self, strategy):
        strategy._has_position = True
        strategy._entry_price = Decimal("3000")
        strategy._entry_token0_usd = Decimal("3000")
        strategy._entry_token1_usd = Decimal("1")
        strategy._lp_token_balance = Decimal("1.5")
        strategy._ticks_in_position = 10
        state = strategy.get_persistent_state()
        assert state["has_position"] is True
        assert state["entry_price"] == "3000"
        assert state["entry_token0_usd"] == "3000"
        assert state["entry_token1_usd"] == "1"
        assert state["lp_token_balance"] == "1.5"
        assert state["ticks_in_position"] == 10

    def test_load_persistent_state(self, strategy):
        state = {
            "has_position": True,
            "entry_price": "2800",
            "entry_token0_usd": "2800",
            "entry_token1_usd": "1",
            "lp_token_balance": "2.0",
            "ticks_in_position": 5,
        }
        strategy.load_persistent_state(state)
        assert strategy._has_position is True
        assert strategy._entry_price == Decimal("2800")
        assert strategy._entry_token0_usd == Decimal("2800")
        assert strategy._entry_token1_usd == Decimal("1")
        assert strategy._lp_token_balance == Decimal("2.0")
        assert strategy._ticks_in_position == 5

    def test_load_persistent_state_string_bool(self, strategy):
        state = {"has_position": "true", "entry_price": "0", "lp_token_balance": "0"}
        strategy.load_persistent_state(state)
        assert strategy._has_position is True


class TestAerodromeLPSweepTeardown:
    """Test teardown support."""

    def test_supports_teardown(self, strategy):
        assert strategy.supports_teardown() is True

    def test_get_open_positions_with_position(self, strategy):
        strategy._has_position = True
        strategy._lp_token_balance = Decimal("1")
        strategy._entry_token0_usd = Decimal("3000")
        strategy._entry_token1_usd = Decimal("1")
        summary = strategy.get_open_positions()
        assert len(summary.positions) == 1
        assert summary.positions[0].protocol == "aerodrome"
        assert summary.total_value_usd > 0

    def test_get_open_positions_empty(self, strategy):
        summary = strategy.get_open_positions()
        assert len(summary.positions) == 0

    def test_generate_teardown_intents(self, strategy):
        from almanak.framework.teardown import TeardownMode

        strategy._has_position = True
        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 1
        assert intents[0].intent_type.value == "LP_CLOSE"

    def test_generate_teardown_intents_no_position(self, strategy):
        from almanak.framework.teardown import TeardownMode

        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 0
