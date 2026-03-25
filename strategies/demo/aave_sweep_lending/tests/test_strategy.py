"""Unit tests for the Aave V3 Sweep Lending Strategy.

Tests validate:
1. Strategy initialization with sweepable config
2. State machine transitions (idle -> supplied -> borrowed -> repaid)
3. Sweep parameter overrides and threshold gating
4. Borrow cycle limit enforcement
5. Teardown interface compliance
6. State persistence round-trip
7. Failure recovery (revert to previous stable state)
"""

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from strategies.demo.aave_sweep_lending.strategy import AaveSweepLendingStrategy


def _create_strategy(config_overrides=None):
    """Create a strategy instance with default config."""
    config = {
        "supply_token": "WETH",
        "borrow_token": "USDC",
        "supply_amount": "0.5",
        "supply_rate_threshold": "4.0",
        "borrow_rate_threshold": "6.0",
        "ltv_target": "0.4",
        "max_borrow_cycles": 5,
        "chain": "arbitrum",
    }
    if config_overrides:
        config.update(config_overrides)
    return AaveSweepLendingStrategy(
        config=config,
        chain="arbitrum",
        wallet_address="0x" + "a" * 40,
    )


@pytest.fixture
def strategy():
    return _create_strategy()


def _make_market(supply_price: Decimal = Decimal("2500"), borrow_price: Decimal = Decimal("1")) -> MagicMock:
    """Create a mock MarketSnapshot."""
    market = MagicMock()

    def price_fn(token):
        if token in ("WETH", "ETH"):
            return supply_price
        if token == "USDC":
            return borrow_price
        raise ValueError(f"Unknown token: {token}")

    market.price.side_effect = price_fn
    return market


class TestInitialization:
    def test_default_config(self, strategy):
        assert strategy.supply_token == "WETH"
        assert strategy.borrow_token == "USDC"
        assert strategy.supply_amount == Decimal("0.5")
        assert strategy.supply_rate_threshold == Decimal("4.0")
        assert strategy.borrow_rate_threshold == Decimal("6.0")
        assert strategy.ltv_target == Decimal("0.4")
        assert strategy.max_borrow_cycles == 5
        assert strategy._state == "idle"
        assert strategy._borrow_cycles == 0
        assert strategy._reference_price is None

    def test_sweep_parameter_override(self):
        """Sweep engine overrides config parameters -- verify they take effect."""
        s = _create_strategy({
            "supply_rate_threshold": "2.5",
            "borrow_rate_threshold": "8.0",
            "ltv_target": "0.3",
        })
        assert s.supply_rate_threshold == Decimal("2.5")
        assert s.borrow_rate_threshold == Decimal("8.0")
        assert s.ltv_target == Decimal("0.3")

    def test_invalid_supply_rate_threshold(self):
        with pytest.raises(ValueError, match="supply_rate_threshold"):
            _create_strategy({"supply_rate_threshold": "-1"})

    def test_invalid_borrow_rate_threshold(self):
        with pytest.raises(ValueError, match="borrow_rate_threshold"):
            _create_strategy({"borrow_rate_threshold": "-1"})

    def test_invalid_ltv_target_zero(self):
        with pytest.raises(ValueError, match="ltv_target"):
            _create_strategy({"ltv_target": "0"})

    def test_invalid_ltv_target_one(self):
        with pytest.raises(ValueError, match="ltv_target"):
            _create_strategy({"ltv_target": "1.0"})


class TestDecisionLogic:
    def test_first_tick_supplies(self, strategy):
        """Strategy should supply collateral on first tick."""
        market = _make_market()
        intent = strategy.decide(market)
        assert intent is not None
        assert intent.intent_type.value == "SUPPLY"
        assert strategy._state == "supplying"
        assert strategy._reference_price == Decimal("2500")

    def test_hold_while_supplying(self, strategy):
        """Should hold while waiting for supply confirmation."""
        strategy._state = "supplying"
        market = _make_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "Waiting" in intent.reason

    def test_borrow_after_supply_confirmed(self, strategy):
        """After supply is confirmed, should borrow when price is stable."""
        strategy._state = "supplied"
        strategy._supplied_amount = Decimal("0.5")
        strategy._reference_price = Decimal("2500")  # same as market -> 0% change < 6% threshold
        market = _make_market()
        intent = strategy.decide(market)
        assert intent is not None
        assert intent.intent_type.value == "BORROW"
        assert strategy._state == "borrowing"

    def test_borrow_blocked_by_high_volatility(self, strategy):
        """Should hold when price volatility exceeds borrow_rate_threshold."""
        strategy._state = "supplied"
        strategy._supplied_amount = Decimal("0.5")
        strategy._reference_price = Decimal("2300")  # 2500 vs 2300 = ~8.7% > 6% threshold
        market = _make_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "volatility" in intent.reason.lower()

    def test_borrow_amount_respects_ltv(self, strategy):
        """Borrow amount should respect LTV target."""
        strategy._state = "supplied"
        strategy._supplied_amount = Decimal("0.5")
        strategy._reference_price = Decimal("2000")  # same price -> 0% change
        market = _make_market(supply_price=Decimal("2000"), borrow_price=Decimal("1"))
        intent = strategy.decide(market)
        # Collateral value = 0.5 * 2000 = 1000
        # Borrow value = 1000 * 0.4 = 400
        # Borrow amount = 400 / 1 = 400.00
        assert intent.intent_type.value == "BORROW"
        assert intent.borrow_amount == Decimal("400.00")

    def test_repay_when_price_moves_significantly(self, strategy):
        """After borrow is confirmed, should repay when price volatility > threshold * 1.5."""
        strategy._state = "borrowed"
        strategy._borrowed_amount = Decimal("400")
        # borrow_rate_threshold=6.0, repay threshold=9.0%
        # reference_price=2500, current=2750 -> 10% > 9%
        strategy._reference_price = Decimal("2500")
        market = _make_market(supply_price=Decimal("2750"))
        intent = strategy.decide(market)
        assert intent is not None
        assert intent.intent_type.value == "REPAY"
        assert strategy._state == "repaying"

    def test_hold_borrowed_when_price_stable(self, strategy):
        """Should hold borrowed position when price is stable."""
        strategy._state = "borrowed"
        strategy._borrowed_amount = Decimal("400")
        strategy._reference_price = Decimal("2500")  # same -> 0% < 9% repay threshold
        market = _make_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"

    def test_max_borrow_cycles_enforced(self, strategy):
        """Should hold when max borrow cycles reached."""
        strategy._state = "supplied"
        strategy._supplied_amount = Decimal("0.5")
        strategy._borrow_cycles = 5
        strategy._reference_price = Decimal("2500")
        market = _make_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "Max borrow cycles" in intent.reason

    def test_hold_while_borrowing(self, strategy):
        strategy._state = "borrowing"
        market = _make_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"

    def test_hold_while_repaying(self, strategy):
        strategy._state = "repaying"
        market = _make_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"

    def test_hold_on_price_unavailable(self, strategy):
        """Should hold when price data is unavailable."""
        market = MagicMock()
        market.price.side_effect = ValueError("No price")
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "unavailable" in intent.reason.lower()

    def test_hold_on_zero_borrow_price(self, strategy):
        """Should hold when borrow token price is zero (avoid division by zero)."""
        strategy._state = "supplied"
        strategy._supplied_amount = Decimal("0.5")
        strategy._reference_price = Decimal("2500")
        market = _make_market(borrow_price=Decimal("0"))
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "Invalid" in intent.reason


class TestOnIntentExecuted:
    def test_supply_success(self, strategy):
        strategy._state = "supplying"
        intent = MagicMock()
        intent.intent_type.value = "SUPPLY"
        strategy.on_intent_executed(intent, success=True, result=None)
        assert strategy._state == "supplied"
        assert strategy._supplied_amount == Decimal("0.5")

    def test_borrow_success(self, strategy):
        strategy._state = "borrowing"
        intent = MagicMock()
        intent.intent_type.value = "BORROW"
        intent.borrow_amount = Decimal("400")
        strategy.on_intent_executed(intent, success=True, result=None)
        assert strategy._state == "borrowed"
        assert strategy._borrowed_amount == Decimal("400")
        assert strategy._borrow_cycles == 1

    def test_repay_success(self, strategy):
        strategy._state = "repaying"
        strategy._borrowed_amount = Decimal("400")
        intent = MagicMock()
        intent.intent_type.value = "REPAY"
        strategy.on_intent_executed(intent, success=True, result=None)
        assert strategy._state == "supplied"
        assert strategy._borrowed_amount == Decimal("0")

    def test_withdraw_success(self, strategy):
        """WITHDRAW intent clears supply state (teardown path)."""
        strategy._state = "supplied"
        strategy._supplied_amount = Decimal("0.5")
        intent = MagicMock()
        intent.intent_type.value = "WITHDRAW"
        strategy.on_intent_executed(intent, success=True, result=None)
        assert strategy._state == "idle"
        assert strategy._supplied_amount == Decimal("0")

    def test_failure_reverts_state(self, strategy):
        """On failure, revert to previous stable state."""
        strategy._state = "borrowing"
        strategy._previous_stable_state = "supplied"
        intent = MagicMock()
        intent.intent_type.value = "BORROW"
        strategy.on_intent_executed(intent, success=False, result=None)
        assert strategy._state == "supplied"

    def test_failure_restores_reference_price(self, strategy):
        """On borrow failure, reference price reverts to pre-borrow value."""
        strategy._state = "borrowing"
        strategy._previous_stable_state = "supplied"
        strategy._reference_price = Decimal("2600")  # set during borrow attempt
        strategy._previous_reference_price = Decimal("2500")  # pre-borrow price
        intent = MagicMock()
        intent.intent_type.value = "BORROW"
        strategy.on_intent_executed(intent, success=False, result=None)
        assert strategy._reference_price == Decimal("2500")


class TestFullCycle:
    def test_supply_borrow_repay_cycle(self, strategy):
        """Full lifecycle: idle -> supply -> borrow -> repay -> supplied."""
        base_price = Decimal("2000")
        market_stable = _make_market(supply_price=base_price, borrow_price=Decimal("1"))

        # Tick 1: Supply (first tick, no reference price)
        intent = strategy.decide(market_stable)
        assert intent.intent_type.value == "SUPPLY"
        supply_intent = MagicMock()
        supply_intent.intent_type.value = "SUPPLY"
        strategy.on_intent_executed(supply_intent, success=True, result=None)
        assert strategy._state == "supplied"

        # Tick 2: Borrow (price unchanged -> 0% volatility < 6% threshold)
        intent = strategy.decide(market_stable)
        assert intent.intent_type.value == "BORROW"
        borrow_intent = MagicMock()
        borrow_intent.intent_type.value = "BORROW"
        borrow_intent.borrow_amount = Decimal("400")
        strategy.on_intent_executed(borrow_intent, success=True, result=None)
        assert strategy._state == "borrowed"
        assert strategy._borrow_cycles == 1

        # Tick 3: Repay (price moved significantly > 9% repay threshold)
        # reference_price was updated to 2000 on borrow; need >9% move
        market_spiked = _make_market(supply_price=Decimal("2200"), borrow_price=Decimal("1"))
        intent = strategy.decide(market_spiked)
        assert intent.intent_type.value == "REPAY"
        repay_intent = MagicMock()
        repay_intent.intent_type.value = "REPAY"
        strategy.on_intent_executed(repay_intent, success=True, result=None)
        assert strategy._state == "supplied"
        assert strategy._borrowed_amount == Decimal("0")


class TestSweepVariations:
    def test_different_ltv_targets_produce_different_borrows(self):
        """Different LTV targets should produce different borrow amounts."""
        market = _make_market(supply_price=Decimal("2000"), borrow_price=Decimal("1"))
        amounts = []
        for ltv in ["0.2", "0.4", "0.6"]:
            s = _create_strategy({"ltv_target": ltv})
            s._state = "supplied"
            s._supplied_amount = Decimal("0.5")
            s._reference_price = Decimal("2000")  # stable price -> pass threshold
            intent = s.decide(market)
            amounts.append(intent.borrow_amount)

        # 0.2 -> 200, 0.4 -> 400, 0.6 -> 600
        assert amounts[0] < amounts[1] < amounts[2]
        assert amounts[0] == Decimal("200.00")
        assert amounts[1] == Decimal("400.00")
        assert amounts[2] == Decimal("600.00")

    def test_sweep_max_cycles(self):
        """Different max_borrow_cycles should limit differently."""
        for cycles in [1, 3, 5]:
            s = _create_strategy({"max_borrow_cycles": cycles})
            s._state = "supplied"
            s._supplied_amount = Decimal("0.5")
            s._borrow_cycles = cycles
            s._reference_price = Decimal("2500")
            market = _make_market()
            intent = s.decide(market)
            assert intent.intent_type.value == "HOLD"
            assert "Max borrow cycles" in intent.reason

    def test_different_borrow_thresholds_gate_differently(self):
        """Higher borrow_rate_threshold allows borrowing in more volatile markets."""
        # Price moved 5% from reference
        market = _make_market(supply_price=Decimal("2625"))

        # threshold=4% -> 5% > 4% -> HOLD (too volatile)
        s_tight = _create_strategy({"borrow_rate_threshold": "4.0"})
        s_tight._state = "supplied"
        s_tight._supplied_amount = Decimal("0.5")
        s_tight._reference_price = Decimal("2500")
        assert s_tight.decide(market).intent_type.value == "HOLD"

        # threshold=10% -> 5% < 10% -> BORROW
        s_loose = _create_strategy({"borrow_rate_threshold": "10.0"})
        s_loose._state = "supplied"
        s_loose._supplied_amount = Decimal("0.5")
        s_loose._reference_price = Decimal("2500")
        assert s_loose.decide(market).intent_type.value == "BORROW"

    def test_different_supply_thresholds_gate_resupply(self):
        """supply_rate_threshold gates re-entry after a full cycle."""
        # Price moved 3% from reference
        market = _make_market(supply_price=Decimal("2575"))

        # threshold=2% -> 3% > 2% -> SUPPLY (re-enter)
        s_low = _create_strategy({"supply_rate_threshold": "2.0"})
        s_low._state = "idle"
        s_low._reference_price = Decimal("2500")
        assert s_low.decide(market).intent_type.value == "SUPPLY"

        # threshold=5% -> 3% < 5% -> HOLD (not enough volatility)
        s_high = _create_strategy({"supply_rate_threshold": "5.0"})
        s_high._state = "idle"
        s_high._reference_price = Decimal("2500")
        assert s_high.decide(market).intent_type.value == "HOLD"


class TestStatePersistence:
    def test_round_trip(self, strategy):
        """State should survive save/load cycle."""
        strategy._state = "borrowed"
        strategy._supplied_amount = Decimal("0.5")
        strategy._borrowed_amount = Decimal("400")
        strategy._borrow_cycles = 2
        strategy._tick_count = 10
        strategy._reference_price = Decimal("2500")

        state = strategy.get_persistent_state()
        new_strategy = _create_strategy()
        new_strategy.load_persistent_state(state)

        assert new_strategy._state == "borrowed"
        assert new_strategy._supplied_amount == Decimal("0.5")
        assert new_strategy._borrowed_amount == Decimal("400")
        assert new_strategy._borrow_cycles == 2
        assert new_strategy._tick_count == 10
        assert new_strategy._reference_price == Decimal("2500")

    def test_invalid_state_falls_back_to_idle(self):
        """Corrupted state should fall back to idle."""
        s = _create_strategy()
        s.load_persistent_state({"state": "garbage", "previous_stable_state": "also_garbage"})
        assert s._state == "idle"
        assert s._previous_stable_state == "idle"

    def test_none_reference_price_round_trip(self, strategy):
        """None reference_price should survive persistence."""
        strategy._reference_price = None
        state = strategy.get_persistent_state()
        assert state["reference_price"] is None

        new_strategy = _create_strategy()
        new_strategy.load_persistent_state(state)
        assert new_strategy._reference_price is None


class TestTeardown:
    def test_supports_teardown(self, strategy):
        assert strategy.supports_teardown() is True

    def test_teardown_with_supply_only(self, strategy):
        strategy._supplied_amount = Decimal("0.5")
        strategy._state = "supplied"
        positions = strategy.get_open_positions()
        assert len(positions.positions) == 1
        assert positions.positions[0].protocol == "aave_v3"

        intents = strategy.generate_teardown_intents(mode=MagicMock())
        assert len(intents) == 1
        assert intents[0].intent_type.value == "WITHDRAW"
        assert intents[0].withdraw_all is True

    def test_teardown_with_supply_and_borrow(self, strategy):
        strategy._supplied_amount = Decimal("0.5")
        strategy._borrowed_amount = Decimal("400")
        strategy._state = "borrowed"
        positions = strategy.get_open_positions()
        assert len(positions.positions) == 2

        intents = strategy.generate_teardown_intents(mode=MagicMock())
        assert len(intents) == 2
        # Repay first, then withdraw
        assert intents[0].intent_type.value == "REPAY"
        assert intents[1].intent_type.value == "WITHDRAW"
        assert intents[1].withdraw_all is True

    def test_teardown_no_positions(self, strategy):
        positions = strategy.get_open_positions()
        assert len(positions.positions) == 0
        intents = strategy.generate_teardown_intents(mode=MagicMock())
        assert len(intents) == 0


class TestGetStatus:
    def test_status_includes_sweep_params(self, strategy):
        status = strategy.get_status()
        assert status["supply_rate_threshold"] == "4.0"
        assert status["borrow_rate_threshold"] == "6.0"
        assert status["ltv_target"] == "0.4"
        assert status["chain"] == "arbitrum"
