"""Tests for TraderJoe V2 LP bin-width sweep strategy on Avalanche.

Validates:
1. Config dataclass type coercion (string/float -> Decimal/int)
2. LP open intent generation with correct parameters
3. Rebalance threshold triggering
4. Position tracking via on_intent_executed
5. State persistence and restoration
6. Teardown support

Kitchen Loop iteration 123, VIB-1717.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def sweep_config():
    """Default sweep strategy config dict."""
    return {
        "pool": "WAVAX/USDC/20",
        "num_bins": 11,
        "range_width_pct": "0.10",
        "amount_x": "0.5",
        "amount_y": "10",
        "rebalance_threshold_pct": "10.0",
    }


@pytest.fixture
def strategy(sweep_config):
    """Instantiate the sweep strategy with mock internals."""
    from strategies.incubating.traderjoe_lp_sweep_avalanche.strategy import (
        TraderJoeLPSweepConfig,
        TraderJoeLPSweepStrategy,
    )

    config = TraderJoeLPSweepConfig(**sweep_config)
    strat = TraderJoeLPSweepStrategy.__new__(TraderJoeLPSweepStrategy)
    strat.config = config
    strat._chain = "avalanche"
    strat._wallet_address = "0x" + "0" * 40
    strat._strategy_id = "test-traderjoe-sweep"

    # Mirror __init__ logic
    strat.token_x = "WAVAX"
    strat.token_y = "USDC"
    strat.bin_step = 20
    strat.num_bins = config.num_bins
    strat.range_width_pct = config.range_width_pct
    strat.amount_x = config.amount_x
    strat.amount_y = config.amount_y
    strat.rebalance_threshold_pct = config.rebalance_threshold_pct
    strat._has_position = False
    strat._entry_price = Decimal("0")
    strat._position_bin_ids = []
    strat._ticks_in_position = 0
    return strat


def _mock_market(wavax_price: str = "25", usdc_price: str = "1") -> MagicMock:
    """Create a mock MarketSnapshot with WAVAX/USDC prices."""
    market = MagicMock()

    def price_fn(symbol):
        if symbol == "WAVAX":
            return Decimal(wavax_price)
        if symbol == "USDC":
            return Decimal(usdc_price)
        raise ValueError(f"Unknown token: {symbol}")

    def balance_fn(symbol):
        bal = MagicMock()
        bal.balance = Decimal("1000")
        return bal

    market.price = MagicMock(side_effect=price_fn)
    market.balance = MagicMock(side_effect=balance_fn)
    return market


# -------------------------------------------------------------------------
# Config tests
# -------------------------------------------------------------------------


class TestTraderJoeLPSweepConfig:
    """Config dataclass type coercion."""

    def test_string_to_decimal_coercion(self):
        from strategies.incubating.traderjoe_lp_sweep_avalanche.strategy import TraderJoeLPSweepConfig

        cfg = TraderJoeLPSweepConfig(
            amount_x="1.5",
            amount_y="50",
            range_width_pct="0.20",
            rebalance_threshold_pct="7.5",
            num_bins="9",
        )
        assert cfg.amount_x == Decimal("1.5")
        assert cfg.amount_y == Decimal("50")
        assert cfg.range_width_pct == Decimal("0.20")
        assert cfg.rebalance_threshold_pct == Decimal("7.5")
        assert cfg.num_bins == 9

    def test_float_to_decimal_coercion(self):
        from strategies.incubating.traderjoe_lp_sweep_avalanche.strategy import TraderJoeLPSweepConfig

        cfg = TraderJoeLPSweepConfig(amount_x=0.5, amount_y=10.0)
        assert cfg.amount_x == Decimal("0.5")
        assert cfg.amount_y == Decimal("10.0")

    def test_to_dict_serializes_correctly(self):
        from strategies.incubating.traderjoe_lp_sweep_avalanche.strategy import TraderJoeLPSweepConfig

        cfg = TraderJoeLPSweepConfig()
        d = cfg.to_dict()
        assert d["pool"] == "WAVAX/USDC/20"
        assert d["num_bins"] == 11
        assert isinstance(d["amount_x"], str)
        assert isinstance(d["range_width_pct"], str)

    def test_sweep_values_num_bins(self):
        """Verify all sweep values from the ticket are valid config values."""
        from strategies.incubating.traderjoe_lp_sweep_avalanche.strategy import TraderJoeLPSweepConfig

        for nb in [3, 5, 7, 11, 15]:
            cfg = TraderJoeLPSweepConfig(num_bins=nb)
            assert cfg.num_bins == nb

    def test_rejects_even_num_bins(self):
        from strategies.incubating.traderjoe_lp_sweep_avalanche.strategy import TraderJoeLPSweepConfig

        with pytest.raises(ValueError, match="positive odd integer"):
            TraderJoeLPSweepConfig(num_bins=4)

    def test_rejects_zero_num_bins(self):
        from strategies.incubating.traderjoe_lp_sweep_avalanche.strategy import TraderJoeLPSweepConfig

        with pytest.raises(ValueError, match="positive odd integer"):
            TraderJoeLPSweepConfig(num_bins=0)


# -------------------------------------------------------------------------
# Decision logic
# -------------------------------------------------------------------------


class TestTraderJoeLPSweepDecision:
    """Test decide() logic."""

    def test_opens_position_when_none_exists(self, strategy):
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent is not None
        assert intent.intent_type.value == "LP_OPEN"

    def test_holds_with_existing_position(self, strategy):
        strategy._has_position = True
        strategy._entry_price = Decimal("25")
        market = _mock_market("25")
        intent = strategy.decide(market)
        assert intent is not None
        assert intent.intent_type.value == "HOLD"

    def test_rebalance_triggered_beyond_threshold(self, strategy):
        strategy._has_position = True
        strategy._entry_price = Decimal("25")
        strategy.rebalance_threshold_pct = Decimal("10.0")

        # Price moved 12% (25 -> 28) — beyond 10% threshold
        market = _mock_market("28")
        intent = strategy.decide(market)
        assert intent is not None
        assert intent.intent_type.value == "LP_CLOSE"

    def test_no_rebalance_within_threshold(self, strategy):
        strategy._has_position = True
        strategy._entry_price = Decimal("25")
        strategy.rebalance_threshold_pct = Decimal("10.0")

        # Price moved ~4% — within threshold
        market = _mock_market("26")
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"

    def test_holds_on_insufficient_balance(self, strategy):
        strategy.amount_x = Decimal("9999")  # More than the mock provides
        market = _mock_market()
        # Mock returns balance of 1000
        bal_mock = MagicMock()
        bal_mock.balance = Decimal("100")
        market.balance = MagicMock(return_value=bal_mock)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "Insufficient" in intent.reason

    def test_holds_on_price_unavailable(self, strategy):
        market = MagicMock()
        market.price = MagicMock(side_effect=ValueError("no price"))
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "unavailable" in intent.reason.lower()

    @pytest.mark.parametrize(
        ("wavax_price", "usdc_price"),
        [("0", "1"), ("25", "0"), ("-1", "1"), ("25", "-1")],
    )
    def test_holds_on_non_positive_price(self, strategy, wavax_price, usdc_price):
        intent = strategy.decide(_mock_market(wavax_price, usdc_price))
        assert intent.intent_type.value == "HOLD"
        assert "invalid price snapshot" in intent.reason.lower()

    def test_ticks_in_position_increments(self, strategy):
        strategy._has_position = True
        strategy._entry_price = Decimal("25")
        market = _mock_market("25")

        strategy.decide(market)
        assert strategy._ticks_in_position == 1
        strategy.decide(market)
        assert strategy._ticks_in_position == 2


# -------------------------------------------------------------------------
# Intent content
# -------------------------------------------------------------------------


class TestTraderJoeLPSweepIntents:
    """Verify intent parameters are correct."""

    @pytest.mark.parametrize(
        ("num_bins", "expected_bin_range"),
        [(3, 1), (5, 2), (7, 3), (11, 5), (15, 7)],
    )
    def test_open_intent_passes_bin_range(self, strategy, num_bins, expected_bin_range):
        strategy.num_bins = num_bins
        intent = strategy.decide(_mock_market())
        assert intent.protocol_params["bin_range"] == expected_bin_range

    def test_open_intent_has_traderjoe_protocol(self, strategy):
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.protocol == "traderjoe_v2"

    def test_open_intent_has_pool(self, strategy):
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.pool == "WAVAX/USDC/20"

    def test_open_intent_has_amounts(self, strategy):
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.amount0 == Decimal("0.5")
        assert intent.amount1 == Decimal("10")

    def test_open_intent_range_is_centered(self, strategy):
        """Price range should be symmetric around current price."""
        market = _mock_market("25")  # price = 25/1 = 25
        intent = strategy.decide(market)
        # range_width_pct = 0.10, so ±5%
        assert intent.range_lower < Decimal("25")
        assert intent.range_upper > Decimal("25")
        midpoint = (intent.range_lower + intent.range_upper) / 2
        assert abs(midpoint - Decimal("25")) < Decimal("0.01")

    def test_close_intent_has_pool_as_position_id(self, strategy):
        strategy._has_position = True
        strategy._entry_price = Decimal("25")
        strategy.rebalance_threshold_pct = Decimal("5.0")
        market = _mock_market("28")  # 12% drift
        intent = strategy.decide(market)
        assert intent.intent_type.value == "LP_CLOSE"
        assert intent.position_id == "WAVAX/USDC/20"


# -------------------------------------------------------------------------
# Lifecycle callbacks
# -------------------------------------------------------------------------


class TestTraderJoeLPSweepLifecycle:
    """Test on_intent_executed callbacks."""

    def test_open_updates_state(self, strategy):
        intent = MagicMock()
        intent.intent_type.value = "LP_OPEN"
        strategy.on_intent_executed(intent, success=True, result=None)
        assert strategy._has_position is True
        assert strategy._ticks_in_position == 0

    def test_close_resets_state(self, strategy):
        strategy._has_position = True
        strategy._entry_price = Decimal("25")
        strategy._ticks_in_position = 5

        intent = MagicMock()
        intent.intent_type.value = "LP_CLOSE"
        strategy.on_intent_executed(intent, success=True, result=None)
        assert strategy._has_position is False
        assert strategy._entry_price == Decimal("0")
        assert strategy._ticks_in_position == 0

    def test_failed_execution_no_state_change(self, strategy):
        intent = MagicMock()
        intent.intent_type.value = "LP_OPEN"
        strategy.on_intent_executed(intent, success=False, result=None)
        assert strategy._has_position is False

    def test_open_captures_bin_ids_from_result(self, strategy):
        intent = MagicMock()
        intent.intent_type.value = "LP_OPEN"
        result = MagicMock()
        result.bin_ids = [8388600, 8388601, 8388602]
        strategy.on_intent_executed(intent, success=True, result=result)
        assert strategy._position_bin_ids == [8388600, 8388601, 8388602]


# -------------------------------------------------------------------------
# State persistence
# -------------------------------------------------------------------------


class TestTraderJoeLPSweepPersistence:
    """State persistence and restoration."""

    def test_get_persistent_state(self, strategy):
        strategy._has_position = True
        strategy._entry_price = Decimal("25.5")
        strategy._ticks_in_position = 7
        strategy.num_bins = 15

        state = strategy.get_persistent_state()
        assert state["has_position"] is True
        assert state["entry_price"] == "25.5"
        assert state["ticks_in_position"] == 7
        assert state["num_bins"] == 15

    def test_load_persistent_state(self, strategy):
        state = {
            "has_position": True,
            "entry_price": "30.0",
            "ticks_in_position": 3,
            "num_bins": 7,
        }
        strategy.load_persistent_state(state)
        assert strategy._has_position is True
        assert strategy._entry_price == Decimal("30.0")
        assert strategy._ticks_in_position == 3
        assert strategy.num_bins == 7
        assert strategy.config.num_bins == 7

    def test_load_persistent_state_string_bool(self, strategy):
        state = {"has_position": "true", "entry_price": "0"}
        strategy.load_persistent_state(state)
        assert strategy._has_position is True

    def test_load_persistent_state_rejects_even_num_bins(self, strategy):
        with pytest.raises(ValueError, match="positive odd integer"):
            strategy.load_persistent_state({"num_bins": 4})

    def test_load_persistent_state_rejects_fractional_num_bins(self, strategy):
        with pytest.raises(ValueError, match="must be an integer"):
            strategy.load_persistent_state({"num_bins": 4.9})

    def test_round_trip_persistence(self, strategy):
        strategy._has_position = True
        strategy._entry_price = Decimal("28.3")
        strategy._ticks_in_position = 12
        strategy.num_bins = 5

        state = strategy.get_persistent_state()

        # Reset and reload
        strategy._has_position = False
        strategy._entry_price = Decimal("0")
        strategy.load_persistent_state(state)

        assert strategy._has_position is True
        assert strategy._entry_price == Decimal("28.3")
        assert strategy.num_bins == 5


# -------------------------------------------------------------------------
# Teardown
# -------------------------------------------------------------------------


class TestTraderJoeLPSweepTeardown:
    """Teardown support."""

    def test_get_open_positions_empty(self, strategy):
        """No position reported when on-chain balances are negligible."""
        market = MagicMock()
        bal = MagicMock()
        bal.balance = Decimal("0")
        market.balance = MagicMock(return_value=bal)
        market.price = MagicMock(return_value=Decimal("1"))
        strategy.create_market_snapshot = MagicMock(return_value=market)
        summary = strategy.get_open_positions()
        assert len(summary.positions) == 0

    def test_get_open_positions_with_position(self, strategy):
        """Position reported when on-chain balances have meaningful value."""
        strategy.create_market_snapshot = MagicMock(return_value=_mock_market())
        summary = strategy.get_open_positions()
        assert len(summary.positions) == 1
        pos = summary.positions[0]
        assert pos.protocol == "traderjoe_v2"
        assert pos.details["num_bins"] == 11

    def test_generate_teardown_intents_with_position(self, strategy):
        from almanak.framework.teardown import TeardownMode

        strategy._has_position = True
        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 1
        assert intents[0].intent_type.value == "LP_CLOSE"

    def test_generate_teardown_intents_no_position(self, strategy):
        from almanak.framework.teardown import TeardownMode

        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 0
