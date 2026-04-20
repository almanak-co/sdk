"""Unit tests for TraderJoe Sweep LP strategy."""

from decimal import Decimal
from unittest.mock import MagicMock, PropertyMock

import pytest

from strategies.demo.traderjoe_sweep_lp.strategy import TraderJoeSweepLPStrategy


def _make_strategy(**overrides) -> TraderJoeSweepLPStrategy:
    """Create a strategy instance with test defaults."""
    config = {
        "pool": "WAVAX/USDC/20",
        "range_width_pct": "0.10",
        "amount_x": "0.5",
        "amount_y": "15",
        "num_bins": 11,
        "rsi_period": 14,
        "rsi_oversold": "30",
        "rsi_overbought": "70",
        "reentry_cooldown": 2,
        "max_lp_cycles": 5,
    }
    config.update(overrides)

    strategy = TraderJoeSweepLPStrategy.__new__(TraderJoeSweepLPStrategy)
    strategy._chain = "avalanche"
    strategy._wallet_address = "0x" + "a" * 40
    strategy._strategy_id = "test_traderjoe_sweep"
    strategy._config = MagicMock()
    strategy._config_dict = config

    # Mock get_config to read from config dict
    def get_config(key, default=None):
        return config.get(key, default)

    strategy.get_config = get_config

    # Call __init__ logic manually
    strategy.pool = str(config["pool"])
    pool_parts = strategy.pool.split("/")
    strategy.token_x = pool_parts[0]
    strategy.token_y = pool_parts[1]
    strategy.bin_step = int(pool_parts[2])

    strategy.range_width_pct = Decimal(str(config["range_width_pct"]))
    strategy.num_bins = int(config["num_bins"])
    strategy.amount_x = Decimal(str(config["amount_x"]))
    strategy.amount_y = Decimal(str(config["amount_y"]))
    strategy.rsi_period = int(config["rsi_period"])
    strategy.rsi_oversold = Decimal(str(config["rsi_oversold"]))
    strategy.rsi_overbought = Decimal(str(config["rsi_overbought"]))
    strategy.reentry_cooldown = int(config["reentry_cooldown"])
    strategy.max_lp_cycles = int(config["max_lp_cycles"])

    strategy._has_position = False
    strategy._lp_cycles = 0
    strategy._cooldown_remaining = 0
    strategy._tick_count = 0
    strategy._ticks_with_position = 0

    return strategy


def _mock_market(
    wavax_price: Decimal = Decimal("30"),
    usdc_price: Decimal = Decimal("1"),
    rsi_value: Decimal = Decimal("50"),
    wavax_balance: Decimal = Decimal("10"),
    usdc_balance: Decimal = Decimal("500"),
) -> MagicMock:
    """Create a mock market snapshot."""
    market = MagicMock()

    def price_fn(token):
        prices = {"WAVAX": wavax_price, "USDC": usdc_price}
        if token not in prices:
            raise ValueError(f"Unknown token: {token}")
        return prices[token]

    market.price.side_effect = price_fn

    rsi_mock = MagicMock()
    rsi_mock.value = rsi_value
    market.rsi.return_value = rsi_mock

    def balance_fn(token):
        balances = {"WAVAX": wavax_balance, "USDC": usdc_balance}
        bal = MagicMock()
        bal.balance = balances.get(token, Decimal("0"))
        return bal

    market.balance.side_effect = balance_fn

    return market


class TestTraderJoeSweepLP:
    """Test the sweep LP strategy decision logic."""

    def test_opens_lp_when_rsi_in_range(self):
        """Strategy opens LP when RSI is within range and has funds."""
        strategy = _make_strategy()
        market = _mock_market(rsi_value=Decimal("50"))

        intent = strategy.decide(market)

        assert intent is not None
        assert intent.intent_type.value == "LP_OPEN"

    def test_holds_when_rsi_too_low(self):
        """Strategy holds when RSI is below oversold threshold."""
        strategy = _make_strategy(rsi_oversold="30", rsi_overbought="70")
        market = _mock_market(rsi_value=Decimal("20"))

        intent = strategy.decide(market)

        assert intent is not None
        assert intent.intent_type.value == "HOLD"

    def test_holds_when_rsi_too_high(self):
        """Strategy holds when RSI is above overbought threshold."""
        strategy = _make_strategy(rsi_oversold="30", rsi_overbought="70")
        market = _mock_market(rsi_value=Decimal("80"))

        intent = strategy.decide(market)

        assert intent is not None
        assert intent.intent_type.value == "HOLD"

    def test_closes_lp_on_rsi_extreme(self):
        """Strategy closes LP when RSI goes extreme."""
        strategy = _make_strategy()
        strategy._has_position = True
        market = _mock_market(rsi_value=Decimal("25"))  # Below oversold=30

        intent = strategy.decide(market)

        assert intent is not None
        assert intent.intent_type.value == "LP_CLOSE"

    def test_holds_during_cooldown(self):
        """Strategy holds during reentry cooldown."""
        strategy = _make_strategy(reentry_cooldown="3")
        strategy._cooldown_remaining = 2
        market = _mock_market(rsi_value=Decimal("50"))

        intent = strategy.decide(market)

        assert intent.intent_type.value == "HOLD"
        assert strategy._cooldown_remaining == 1

    def test_holds_when_max_cycles_reached(self):
        """Strategy holds when max LP cycles reached."""
        strategy = _make_strategy(max_lp_cycles="3")
        strategy._lp_cycles = 3
        market = _mock_market(rsi_value=Decimal("50"))

        intent = strategy.decide(market)

        assert intent.intent_type.value == "HOLD"

    def test_holds_with_insufficient_funds(self):
        """Strategy holds when balance is insufficient."""
        strategy = _make_strategy(amount_x="100")  # Need 100 WAVAX
        market = _mock_market(wavax_balance=Decimal("1"))  # Only have 1

        intent = strategy.decide(market)

        assert intent.intent_type.value == "HOLD"

    def test_range_width_affects_intent(self):
        """Different range widths produce different LP ranges."""
        narrow = _make_strategy(range_width_pct="0.05")
        wide = _make_strategy(range_width_pct="0.20")
        market = _mock_market()

        narrow_intent = narrow.decide(market)
        wide_intent = wide.decide(market)

        # Both should open, but with different ranges
        assert narrow_intent.intent_type.value == "LP_OPEN"
        assert wide_intent.intent_type.value == "LP_OPEN"
        # Wide range should have wider bounds
        assert wide_intent.range_lower < narrow_intent.range_lower
        assert wide_intent.range_upper > narrow_intent.range_upper

    def test_sweep_parameters_applied(self):
        """Verify sweep parameters are properly applied."""
        strategy = _make_strategy(
            range_width_pct="0.15",
            num_bins="21",
            rsi_oversold="25",
            rsi_overbought="75",
            reentry_cooldown="5",
        )

        assert strategy.range_width_pct == Decimal("0.15")
        assert strategy.num_bins == 21
        assert strategy.rsi_oversold == Decimal("25")
        assert strategy.rsi_overbought == Decimal("75")
        assert strategy.reentry_cooldown == 5

    def test_on_intent_executed_tracks_cycles(self):
        """on_intent_executed tracks LP cycles correctly."""
        strategy = _make_strategy()

        # Simulate LP_OPEN
        mock_intent = MagicMock()
        mock_intent.intent_type.value = "LP_OPEN"
        strategy.on_intent_executed(mock_intent, True, None)

        assert strategy._has_position is True
        assert strategy._lp_cycles == 1
        assert strategy._ticks_with_position == 0

        # Simulate LP_CLOSE
        mock_intent.intent_type.value = "LP_CLOSE"
        strategy.on_intent_executed(mock_intent, False, None)
        # Failed intent should not change state
        assert strategy._has_position is True

        strategy.on_intent_executed(mock_intent, True, None)
        assert strategy._has_position is False
        assert strategy._cooldown_remaining == 2

    def test_persistent_state_roundtrip(self):
        """State save/load preserves all fields."""
        strategy = _make_strategy()
        strategy._has_position = True
        strategy._lp_cycles = 3
        strategy._cooldown_remaining = 1
        strategy._tick_count = 42
        strategy._ticks_with_position = 7

        state = strategy.get_persistent_state()

        strategy2 = _make_strategy()
        strategy2.load_persistent_state(state)

        assert strategy2._has_position is True
        assert strategy2._lp_cycles == 3
        assert strategy2._cooldown_remaining == 1
        assert strategy2._tick_count == 42
        assert strategy2._ticks_with_position == 7

    def test_teardown_with_position(self):
        """Teardown generates close intent when position is open."""
        from almanak.framework.teardown.models import TeardownMode

        strategy = _make_strategy()
        strategy._has_position = True

        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 1
        assert intents[0].intent_type.value == "LP_CLOSE"

    def test_teardown_without_position(self):
        """Teardown generates no intents when no position."""
        from almanak.framework.teardown.models import TeardownMode

        strategy = _make_strategy()

        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 0
