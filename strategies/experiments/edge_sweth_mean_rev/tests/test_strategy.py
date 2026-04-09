"""
Tests for edge_sweth_mean_rev strategy.

Tests the mean reversion state machine:
  idle -> swapped (one-shot trade)

Key scenarios:
  - Premium above threshold -> swap
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

from strategy import SWETH_ADDRESS, STATE_IDLE, STATE_SWAPPED, EdgeSwethMeanRevStrategy


@pytest.fixture
def config() -> dict:
    """Load test configuration from config.json."""
    config_path = Path(__file__).parent.parent / "config.json"
    if config_path.exists():
        with open(config_path) as f:
            return json.load(f)
    return {
        "sell_token": SWETH_ADDRESS,
        "buy_token": "WETH",
        "sell_amount": "0.001",
        "min_premium_pct": "5.0",
        "stop_loss_pct": "-0.05",
        "time_horizon_hours": 72,
        "max_slippage_bps": 50,
        "signal_id": "cfc82bb4-eb44-42b0-b085-84a7095a42a5",
    }


@pytest.fixture
def strategy(config: dict) -> EdgeSwethMeanRevStrategy:
    """Create strategy instance for testing."""
    return EdgeSwethMeanRevStrategy(
        config=config,
        chain="ethereum",
        wallet_address="0x" + "1" * 40,
    )


def _make_market(
    sweth_price: Decimal = Decimal("2300"),
    eth_price: Decimal = Decimal("2000"),
    sweth_balance: Decimal = Decimal("1.0"),
) -> MagicMock:
    """Create a mock MarketSnapshot with configurable prices."""
    market = MagicMock()
    market.chain = "ethereum"
    market.wallet_address = "0x" + "1" * 40

    def mock_price(token, quote="USD"):
        if token == SWETH_ADDRESS:
            return sweth_price
        if token == "WETH":
            return eth_price
        return Decimal("0")

    market.price.side_effect = mock_price

    # Mock balance for swETH
    balance_mock = MagicMock()
    balance_mock.balance = sweth_balance
    balance_mock.balance_usd = sweth_balance * sweth_price
    market.balance.return_value = balance_mock

    return market


class TestEdgeSwethMeanRevStrategy:
    """Tests for EdgeSwethMeanRevStrategy."""

    def test_initialization(self, strategy: EdgeSwethMeanRevStrategy) -> None:
        """Test strategy initialization with config values."""
        assert strategy.chain == "ethereum"
        assert strategy.sell_token == SWETH_ADDRESS
        assert strategy.buy_token == "WETH"
        assert strategy.sell_amount == Decimal("0.001")
        assert strategy.min_premium_pct == Decimal("5.0")
        assert strategy._state == STATE_IDLE

    def test_decide_swap_when_premium_above_threshold(
        self, strategy: EdgeSwethMeanRevStrategy
    ) -> None:
        """Premium > 5% -> should trigger swap."""
        # swETH at $2200, ETH at $2000 -> 10% premium
        market = _make_market(sweth_price=Decimal("2200"), eth_price=Decimal("2000"))

        result = strategy.decide(market)

        assert result is not None
        assert hasattr(result, "intent_type")
        assert result.intent_type.value == "SWAP"
        assert result.from_token == SWETH_ADDRESS
        assert result.to_token == "WETH"
        assert result.amount == Decimal("0.001")

    def test_decide_hold_when_premium_below_threshold(
        self, strategy: EdgeSwethMeanRevStrategy
    ) -> None:
        """Premium < 5% -> should hold."""
        # swETH at $2040, ETH at $2000 -> 2% premium (below 5% threshold)
        market = _make_market(sweth_price=Decimal("2040"), eth_price=Decimal("2000"))

        result = strategy.decide(market)

        assert result is not None
        assert result.intent_type.value == "HOLD"
        assert "below" in result.reason.lower() or "threshold" in result.reason.lower()

    def test_decide_hold_when_at_peg(
        self, strategy: EdgeSwethMeanRevStrategy
    ) -> None:
        """swETH at peg (1:1 with ETH) -> should hold."""
        market = _make_market(sweth_price=Decimal("2000"), eth_price=Decimal("2000"))

        result = strategy.decide(market)

        assert result is not None
        assert result.intent_type.value == "HOLD"

    def test_decide_hold_after_swap(
        self, strategy: EdgeSwethMeanRevStrategy
    ) -> None:
        """After swap completes, should hold indefinitely."""
        strategy._state = STATE_SWAPPED
        market = _make_market(sweth_price=Decimal("2200"), eth_price=Decimal("2000"))

        result = strategy.decide(market)

        assert result is not None
        assert result.intent_type.value == "HOLD"
        assert "complete" in result.reason.lower()

    def test_decide_hold_insufficient_balance(
        self, strategy: EdgeSwethMeanRevStrategy
    ) -> None:
        """Not enough swETH -> hold."""
        market = _make_market(
            sweth_price=Decimal("2200"),
            eth_price=Decimal("2000"),
            sweth_balance=Decimal("0.0001"),  # Less than sell_amount of 0.001
        )

        result = strategy.decide(market)

        assert result is not None
        assert result.intent_type.value == "HOLD"
        assert "insufficient" in result.reason.lower() or "balance" in result.reason.lower()

    def test_decide_hold_time_horizon_expired(
        self, strategy: EdgeSwethMeanRevStrategy
    ) -> None:
        """Time horizon exceeded -> hold (no more entry attempts)."""
        # Set entry time to 100 hours ago (beyond 72h horizon)
        strategy._entry_time = (datetime.now(UTC) - timedelta(hours=100)).isoformat()
        market = _make_market(sweth_price=Decimal("2200"), eth_price=Decimal("2000"))

        result = strategy.decide(market)

        assert result is not None
        assert result.intent_type.value == "HOLD"
        assert "expired" in result.reason.lower() or "horizon" in result.reason.lower()

    def test_decide_handles_price_error(
        self, strategy: EdgeSwethMeanRevStrategy
    ) -> None:
        """Price data unavailable -> hold (no crash)."""
        market = _make_market()
        market.price.side_effect = ValueError("Price unavailable")

        result = strategy.decide(market)

        assert result is not None
        assert result.intent_type.value == "HOLD"

    def test_decide_handles_balance_error(
        self, strategy: EdgeSwethMeanRevStrategy
    ) -> None:
        """Balance data unavailable -> hold (no crash)."""
        market = _make_market(sweth_price=Decimal("2200"), eth_price=Decimal("2000"))
        market.balance.side_effect = ValueError("Balance unavailable")

        result = strategy.decide(market)

        assert result is not None
        assert result.intent_type.value == "HOLD"

    def test_decide_handles_generic_error(
        self, strategy: EdgeSwethMeanRevStrategy
    ) -> None:
        """Unexpected error -> hold (no crash)."""
        market = _make_market()
        market.price.side_effect = RuntimeError("Unexpected failure")

        result = strategy.decide(market)

        assert result is not None
        assert "error" in result.reason.lower()

    def test_on_intent_executed_transitions_state(
        self, strategy: EdgeSwethMeanRevStrategy
    ) -> None:
        """Successful swap should transition state to 'swapped'."""
        assert strategy._state == STATE_IDLE

        mock_intent = MagicMock()
        mock_intent.intent_type.value = "SWAP"
        mock_intent.from_token = SWETH_ADDRESS

        strategy.on_intent_executed(mock_intent, success=True, result=None)

        assert strategy._state == STATE_SWAPPED

    def test_on_intent_executed_no_transition_on_failure(
        self, strategy: EdgeSwethMeanRevStrategy
    ) -> None:
        """Failed swap should not change state."""
        assert strategy._state == STATE_IDLE

        mock_intent = MagicMock()
        mock_intent.intent_type.value = "SWAP"
        mock_intent.from_token = SWETH_ADDRESS

        strategy.on_intent_executed(mock_intent, success=False, result=None)

        assert strategy._state == STATE_IDLE

    def test_get_status(self, strategy: EdgeSwethMeanRevStrategy) -> None:
        """Test get_status returns expected fields."""
        status = strategy.get_status()

        assert status["strategy"] == "edge_sweth_mean_rev"
        assert status["chain"] == "ethereum"
        assert status["state"] == STATE_IDLE
        assert status["sell_token"] == SWETH_ADDRESS
        assert status["buy_token"] == "WETH"
        assert "signal_id" in status

    def test_persistent_state_round_trip(
        self, strategy: EdgeSwethMeanRevStrategy
    ) -> None:
        """State should survive save/restore cycle."""
        strategy._state = STATE_SWAPPED
        strategy._entry_time = "2026-04-01T12:00:00+00:00"
        strategy._swap_price_ratio = Decimal("1.115")

        saved = strategy.get_persistent_state()

        # Create a fresh strategy and restore
        new_strategy = EdgeSwethMeanRevStrategy(
            config=strategy.config,
            chain="ethereum",
            wallet_address="0x" + "1" * 40,
        )
        new_strategy.load_persistent_state(saved)

        assert new_strategy._state == STATE_SWAPPED
        assert new_strategy._entry_time == "2026-04-01T12:00:00+00:00"
        assert new_strategy._swap_price_ratio == Decimal("1.115")

    def test_persistent_state_empty_restore(
        self, strategy: EdgeSwethMeanRevStrategy
    ) -> None:
        """Restoring None/empty state should be safe."""
        strategy.load_persistent_state(None)
        assert strategy._state == STATE_IDLE

        strategy.load_persistent_state({})
        assert strategy._state == STATE_IDLE


class TestTeardown:
    """Tests for teardown methods."""

    def test_teardown_idle_state(self, strategy: EdgeSwethMeanRevStrategy) -> None:
        """In idle state, teardown should sell swETH."""
        strategy._state = STATE_IDLE

        positions = strategy.get_open_positions()
        assert len(positions.positions) == 1
        assert positions.positions[0].details["asset"] == SWETH_ADDRESS

        intents = strategy.generate_teardown_intents(mode=None)
        assert len(intents) == 1
        assert intents[0].from_token == SWETH_ADDRESS
        assert intents[0].to_token == "WETH"

    def test_teardown_swapped_state(self, strategy: EdgeSwethMeanRevStrategy) -> None:
        """In swapped state, teardown has nothing to unwind (holding ETH is fine)."""
        strategy._state = STATE_SWAPPED

        positions = strategy.get_open_positions()
        assert len(positions.positions) == 1
        assert positions.positions[0].details["asset"] == "WETH"

        intents = strategy.generate_teardown_intents(mode=None)
        assert len(intents) == 0  # Nothing to unwind — already holding ETH

    def test_teardown_hard_mode_wider_slippage(
        self, strategy: EdgeSwethMeanRevStrategy
    ) -> None:
        """Hard teardown should use wider slippage."""
        from almanak.framework.teardown import TeardownMode

        strategy._state = STATE_IDLE

        intents = strategy.generate_teardown_intents(mode=TeardownMode.HARD)
        assert len(intents) == 1
        assert intents[0].max_slippage == Decimal("0.03")

    def test_teardown_soft_mode_tighter_slippage(
        self, strategy: EdgeSwethMeanRevStrategy
    ) -> None:
        """Soft/default teardown should use tighter slippage."""
        from almanak.framework.teardown import TeardownMode

        strategy._state = STATE_IDLE

        intents = strategy.generate_teardown_intents(mode=TeardownMode.SOFT)
        assert len(intents) == 1
        assert intents[0].max_slippage == Decimal("0.01")
