"""
Tests for edge_ankreth_mean_rev strategy.

Tests the mean reversion state machine:
  idle -> swapped (one-shot trade)

Key scenarios:
  - Premium above threshold (vs 1.05 peg) -> swap
  - Premium below threshold -> hold
  - After swap -> hold (trade complete)
  - Time horizon expired -> hold
  - Insufficient balance -> hold
  - Price data unavailable -> hold (no crash)
  - Teardown in each state
  - Persistent state round-trip
"""

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from strategy import ANKRETH_ADDRESS, ANKRETH_PEG_RATIO, STATE_IDLE, STATE_SWAPPED, EdgeAnkrethMeanRevStrategy


@pytest.fixture
def config() -> dict:
    """Load test configuration from config.json."""
    config_path = Path(__file__).parent.parent / "config.json"
    if config_path.exists():
        with open(config_path) as f:
            return json.load(f)
    return {
        "sell_token": ANKRETH_ADDRESS,
        "buy_token": "WETH",
        "sell_amount": "0.001",
        "peg_ratio": "1.05",
        "min_premium_pct": "10.0",
        "stop_loss_pct": "-0.075",
        "time_horizon_hours": 72,
        "max_slippage_bps": 50,
        "signal_id": "92e14fed-96ac-4b3c-9ae3-d4b4bde1203c",
    }


@pytest.fixture
def strategy(config: dict) -> EdgeAnkrethMeanRevStrategy:
    """Create strategy instance for testing."""
    return EdgeAnkrethMeanRevStrategy(
        config=config,
        chain="ethereum",
        wallet_address="0x" + "1" * 40,
    )


def _make_market(
    ankreth_price: Decimal = Decimal("2500"),
    eth_price: Decimal = Decimal("2000"),
    ankreth_balance: Decimal = Decimal("1.0"),
) -> MagicMock:
    """Create a mock MarketSnapshot with configurable prices.

    Default prices: ankrETH=$2500, ETH=$2000 => ratio=1.25
    Peg is 1.05, so premium = (1.25 - 1.05) / 1.05 * 100 = 19.05%
    """
    market = MagicMock()
    market.chain = "ethereum"
    market.wallet_address = "0x" + "1" * 40

    def mock_price(token, quote="USD"):
        if token == ANKRETH_ADDRESS:
            return ankreth_price
        if token == "WETH":
            return eth_price
        return Decimal("0")

    market.price.side_effect = mock_price

    # Mock balance for ankrETH
    balance_mock = MagicMock()
    balance_mock.balance = ankreth_balance
    balance_mock.balance_usd = ankreth_balance * ankreth_price
    market.balance.return_value = balance_mock

    return market


class TestEdgeAnkrethMeanRevStrategy:
    """Tests for EdgeAnkrethMeanRevStrategy."""

    def test_initialization(self, strategy: EdgeAnkrethMeanRevStrategy) -> None:
        """Test strategy initialization with config values."""
        assert strategy.chain == "ethereum"
        assert strategy.sell_token == ANKRETH_ADDRESS
        assert strategy.buy_token == "WETH"
        assert strategy.sell_amount == Decimal("0.001")
        assert strategy.peg_ratio == ANKRETH_PEG_RATIO
        assert strategy.min_premium_pct == Decimal("10.0")
        assert strategy.stop_loss_pct == Decimal("-0.075")
        assert strategy._state == STATE_IDLE

    def test_decide_swap_when_premium_above_threshold(
        self, strategy: EdgeAnkrethMeanRevStrategy
    ) -> None:
        """Premium > 10% above 1.05 peg -> should trigger swap.

        ankrETH=$2500, ETH=$2000 => ratio=1.25
        premium = (1.25 - 1.05) / 1.05 * 100 = 19.05% -> above 10% threshold
        """
        market = _make_market(ankreth_price=Decimal("2500"), eth_price=Decimal("2000"))

        result = strategy.decide(market)

        assert result is not None
        assert hasattr(result, "intent_type")
        assert result.intent_type.value == "SWAP"
        assert result.from_token == ANKRETH_ADDRESS
        assert result.to_token == "WETH"
        assert result.amount == Decimal("0.001")

    def test_decide_swap_at_exact_threshold(
        self, strategy: EdgeAnkrethMeanRevStrategy
    ) -> None:
        """Premium exactly at 10% above peg -> should trigger swap.

        peg = 1.05, 10% above peg => ratio = 1.05 * 1.10 = 1.155
        ankrETH = 1.155 * 2000 = $2310, ETH = $2000
        """
        market = _make_market(ankreth_price=Decimal("2310"), eth_price=Decimal("2000"))

        result = strategy.decide(market)

        assert result is not None
        assert result.intent_type.value == "SWAP"

    def test_decide_hold_when_premium_below_threshold(
        self, strategy: EdgeAnkrethMeanRevStrategy
    ) -> None:
        """Premium < 10% above peg -> should hold.

        ankrETH=$2150, ETH=$2000 => ratio=1.075
        premium = (1.075 - 1.05) / 1.05 * 100 = 2.38% -> below 10%
        """
        market = _make_market(ankreth_price=Decimal("2150"), eth_price=Decimal("2000"))

        result = strategy.decide(market)

        assert result is not None
        assert result.intent_type.value == "HOLD"
        assert "below" in result.reason.lower() or "threshold" in result.reason.lower()

    def test_decide_hold_when_at_peg(
        self, strategy: EdgeAnkrethMeanRevStrategy
    ) -> None:
        """ankrETH at peg (ratio = 1.05) -> should hold (0% premium)."""
        # ratio = 2100 / 2000 = 1.05 = peg
        market = _make_market(ankreth_price=Decimal("2100"), eth_price=Decimal("2000"))

        result = strategy.decide(market)

        assert result is not None
        assert result.intent_type.value == "HOLD"

    def test_decide_hold_when_below_peg(
        self, strategy: EdgeAnkrethMeanRevStrategy
    ) -> None:
        """ankrETH below peg -> should hold (negative premium)."""
        # ratio = 2000 / 2000 = 1.0, below 1.05 peg
        market = _make_market(ankreth_price=Decimal("2000"), eth_price=Decimal("2000"))

        result = strategy.decide(market)

        assert result is not None
        assert result.intent_type.value == "HOLD"

    def test_decide_hold_after_swap(
        self, strategy: EdgeAnkrethMeanRevStrategy
    ) -> None:
        """After swap completes, should hold indefinitely."""
        strategy._state = STATE_SWAPPED
        market = _make_market(ankreth_price=Decimal("2500"), eth_price=Decimal("2000"))

        result = strategy.decide(market)

        assert result is not None
        assert result.intent_type.value == "HOLD"
        assert "complete" in result.reason.lower()

    def test_decide_hold_insufficient_balance(
        self, strategy: EdgeAnkrethMeanRevStrategy
    ) -> None:
        """Not enough ankrETH -> hold."""
        market = _make_market(
            ankreth_price=Decimal("2500"),
            eth_price=Decimal("2000"),
            ankreth_balance=Decimal("0.0001"),  # Less than sell_amount of 0.001
        )

        result = strategy.decide(market)

        assert result is not None
        assert result.intent_type.value == "HOLD"
        assert "insufficient" in result.reason.lower() or "balance" in result.reason.lower()

    def test_decide_hold_time_horizon_expired(
        self, strategy: EdgeAnkrethMeanRevStrategy
    ) -> None:
        """Time horizon exceeded -> hold (no more entry attempts)."""
        # Set entry time to 100 hours ago (beyond 72h horizon)
        strategy._entry_time = (datetime.now(UTC) - timedelta(hours=100)).isoformat()
        market = _make_market(ankreth_price=Decimal("2500"), eth_price=Decimal("2000"))

        result = strategy.decide(market)

        assert result is not None
        assert result.intent_type.value == "HOLD"
        assert "expired" in result.reason.lower() or "horizon" in result.reason.lower()

    def test_decide_handles_price_error(
        self, strategy: EdgeAnkrethMeanRevStrategy
    ) -> None:
        """Price data unavailable -> hold (no crash)."""
        market = _make_market()
        market.price.side_effect = ValueError("Price unavailable")

        result = strategy.decide(market)

        assert result is not None
        assert result.intent_type.value == "HOLD"

    def test_decide_handles_balance_error(
        self, strategy: EdgeAnkrethMeanRevStrategy
    ) -> None:
        """Balance data unavailable -> hold (no crash)."""
        market = _make_market(ankreth_price=Decimal("2500"), eth_price=Decimal("2000"))
        market.balance.side_effect = ValueError("Balance unavailable")

        result = strategy.decide(market)

        assert result is not None
        assert result.intent_type.value == "HOLD"

    def test_decide_handles_generic_error(
        self, strategy: EdgeAnkrethMeanRevStrategy
    ) -> None:
        """Unexpected error -> hold (no crash)."""
        market = _make_market()
        market.price.side_effect = RuntimeError("Unexpected failure")

        result = strategy.decide(market)

        assert result is not None
        assert "error" in result.reason.lower()

    def test_decide_handles_zero_eth_price(
        self, strategy: EdgeAnkrethMeanRevStrategy
    ) -> None:
        """ETH price = 0 -> hold (avoid division by zero)."""
        market = _make_market(ankreth_price=Decimal("2500"), eth_price=Decimal("0"))

        result = strategy.decide(market)

        assert result is not None
        assert result.intent_type.value == "HOLD"

    def test_on_intent_executed_transitions_state(
        self, strategy: EdgeAnkrethMeanRevStrategy
    ) -> None:
        """Successful swap should transition state to 'swapped'."""
        assert strategy._state == STATE_IDLE

        mock_intent = MagicMock()
        mock_intent.intent_type.value = "SWAP"
        mock_intent.from_token = ANKRETH_ADDRESS

        strategy.on_intent_executed(mock_intent, success=True, result=None)

        assert strategy._state == STATE_SWAPPED

    def test_on_intent_executed_no_transition_on_failure(
        self, strategy: EdgeAnkrethMeanRevStrategy
    ) -> None:
        """Failed swap should not change state."""
        assert strategy._state == STATE_IDLE

        mock_intent = MagicMock()
        mock_intent.intent_type.value = "SWAP"
        mock_intent.from_token = ANKRETH_ADDRESS

        strategy.on_intent_executed(mock_intent, success=False, result=None)

        assert strategy._state == STATE_IDLE

    def test_on_intent_executed_ignores_unrelated_swap(
        self, strategy: EdgeAnkrethMeanRevStrategy
    ) -> None:
        """Swap of a different token should not change state."""
        assert strategy._state == STATE_IDLE

        mock_intent = MagicMock()
        mock_intent.intent_type.value = "SWAP"
        mock_intent.from_token = "SOME_OTHER_TOKEN"

        strategy.on_intent_executed(mock_intent, success=True, result=None)

        assert strategy._state == STATE_IDLE

    def test_get_status(self, strategy: EdgeAnkrethMeanRevStrategy) -> None:
        """Test get_status returns expected fields."""
        status = strategy.get_status()

        assert status["strategy"] == "edge_ankreth_mean_rev"
        assert status["chain"] == "ethereum"
        assert status["state"] == STATE_IDLE
        assert status["sell_token"] == ANKRETH_ADDRESS
        assert status["buy_token"] == "WETH"
        assert status["peg_ratio"] == "1.05"
        assert "signal_id" in status

    def test_persistent_state_round_trip(
        self, strategy: EdgeAnkrethMeanRevStrategy
    ) -> None:
        """State should survive save/restore cycle."""
        strategy._state = STATE_SWAPPED
        strategy._entry_time = "2026-04-01T12:00:00+00:00"
        strategy._swap_price_ratio = Decimal("1.22")

        saved = strategy.get_persistent_state()

        # Create a fresh strategy and restore
        new_strategy = EdgeAnkrethMeanRevStrategy(
            config=strategy.config,
            chain="ethereum",
            wallet_address="0x" + "1" * 40,
        )
        new_strategy.load_persistent_state(saved)

        assert new_strategy._state == STATE_SWAPPED
        assert new_strategy._entry_time == "2026-04-01T12:00:00+00:00"
        assert new_strategy._swap_price_ratio == Decimal("1.22")

    def test_persistent_state_empty_restore(
        self, strategy: EdgeAnkrethMeanRevStrategy
    ) -> None:
        """Restoring None/empty state should be safe."""
        strategy.load_persistent_state(None)
        assert strategy._state == STATE_IDLE

        strategy.load_persistent_state({})
        assert strategy._state == STATE_IDLE

    def test_premium_calculation_accuracy(
        self, strategy: EdgeAnkrethMeanRevStrategy
    ) -> None:
        """Verify premium is calculated against 1.05 peg, not 1:1.

        ankrETH=$2200, ETH=$2000 => ratio=1.10
        premium vs 1.05 peg = (1.10 - 1.05) / 1.05 * 100 = 4.76%
        This is BELOW the 10% threshold, so should HOLD.

        If we used 1:1 peg, premium would be 10% -> incorrectly SWAP.
        """
        market = _make_market(ankreth_price=Decimal("2200"), eth_price=Decimal("2000"))

        result = strategy.decide(market)

        # 4.76% premium is below 10% threshold -> HOLD
        assert result.intent_type.value == "HOLD"


class TestTeardown:
    """Tests for teardown methods."""

    def test_teardown_idle_state(self, strategy: EdgeAnkrethMeanRevStrategy) -> None:
        """In idle state, teardown should sell ankrETH."""
        strategy._state = STATE_IDLE

        positions = strategy.get_open_positions()
        assert len(positions.positions) == 1
        assert positions.positions[0].details["asset"] == ANKRETH_ADDRESS

        intents = strategy.generate_teardown_intents(mode=None)
        assert len(intents) == 1
        assert intents[0].from_token == ANKRETH_ADDRESS
        assert intents[0].to_token == "WETH"

    def test_teardown_swapped_state(self, strategy: EdgeAnkrethMeanRevStrategy) -> None:
        """In swapped state, teardown has nothing to unwind (holding ETH is fine)."""
        strategy._state = STATE_SWAPPED

        positions = strategy.get_open_positions()
        assert len(positions.positions) == 1
        assert positions.positions[0].details["asset"] == "WETH"

        intents = strategy.generate_teardown_intents(mode=None)
        assert len(intents) == 0  # Nothing to unwind — already holding ETH

    def test_teardown_hard_mode_wider_slippage(
        self, strategy: EdgeAnkrethMeanRevStrategy
    ) -> None:
        """Hard teardown should use wider slippage."""
        from almanak.framework.teardown import TeardownMode

        strategy._state = STATE_IDLE

        intents = strategy.generate_teardown_intents(mode=TeardownMode.HARD)
        assert len(intents) == 1
        assert intents[0].max_slippage == Decimal("0.03")

    def test_teardown_soft_mode_tighter_slippage(
        self, strategy: EdgeAnkrethMeanRevStrategy
    ) -> None:
        """Soft/default teardown should use tighter slippage."""
        from almanak.framework.teardown import TeardownMode

        strategy._state = STATE_IDLE

        intents = strategy.generate_teardown_intents(mode=TeardownMode.SOFT)
        assert len(intents) == 1
        assert intents[0].max_slippage == Decimal("0.01")
