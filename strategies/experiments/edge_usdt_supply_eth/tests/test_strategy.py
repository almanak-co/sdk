"""
Tests for edge_usdt_supply_eth strategy.

Covers the supply-only state machine:
    idle -> supplying -> supplied -> withdrawing -> done
"""

import json
import time
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from strategy import EdgeUsdtSupplyEthStrategy


@pytest.fixture
def config() -> dict:
    """Load test configuration from config.json."""
    config_path = Path(__file__).parent.parent / "config.json"
    if config_path.exists():
        with open(config_path) as f:
            return json.load(f)
    return {
        "collateral_token": "USDT",
        "supply_amount": "2",
        "min_collateral_usd": "1",
        "stop_loss_pct": "-0.10",
        "time_horizon_hours": 168,
        "signal_id": "85fa7410-bd84-4e73-83cd-2f68a23d9145",
        "chain": "ethereum",
    }


@pytest.fixture
def strategy(config: dict) -> EdgeUsdtSupplyEthStrategy:
    """Create strategy instance for testing."""
    return EdgeUsdtSupplyEthStrategy(
        config=config,
        chain=config.get("chain", "ethereum"),
        wallet_address="0x" + "1" * 40,
    )


@pytest.fixture
def mock_market() -> MagicMock:
    """Create a mock MarketSnapshot with USDT at $1."""
    market = MagicMock()
    market.price.return_value = Decimal("1.00")
    market.chain = "ethereum"
    market.wallet_address = "0x" + "1" * 40

    # Mock balance: 100 USDT available
    balance_mock = MagicMock()
    balance_mock.balance = Decimal("100")
    balance_mock.balance_usd = Decimal("100")
    market.balance.return_value = balance_mock

    return market


def _make_intent_mock(intent_type_value: str) -> MagicMock:
    """Create a mock intent with the given type value."""
    intent = MagicMock()
    intent.intent_type = MagicMock()
    intent.intent_type.value = intent_type_value
    return intent


class TestEdgeUsdtSupplyEthStrategy:
    """Tests for EdgeUsdtSupplyEthStrategy strategy."""

    def test_initialization(self, strategy: EdgeUsdtSupplyEthStrategy) -> None:
        """Test strategy initialization from config."""
        assert strategy.chain == "ethereum"
        assert strategy.collateral_token == "USDT"
        assert strategy.supply_amount == Decimal("2")
        assert strategy.stop_loss_pct == Decimal("-0.10")
        assert strategy.time_horizon_hours == 168
        assert strategy._state == "idle"
        assert strategy._supplied_amount == Decimal("0")
        assert strategy._entry_time is None
        assert strategy._entry_price is None

    def test_decide_idle_supplies(
        self, strategy: EdgeUsdtSupplyEthStrategy, mock_market: MagicMock
    ) -> None:
        """Test that idle state emits a SUPPLY intent."""
        result = strategy.decide(mock_market)

        assert result is not None
        assert result.intent_type.value == "SUPPLY"
        assert strategy._state == "supplying"

    def test_decide_idle_insufficient_balance(
        self, strategy: EdgeUsdtSupplyEthStrategy, mock_market: MagicMock
    ) -> None:
        """Test that idle state holds when balance is insufficient."""
        balance_mock = MagicMock()
        balance_mock.balance = Decimal("0.5")  # Less than supply_amount=2
        balance_mock.balance_usd = Decimal("0.5")
        mock_market.balance.return_value = balance_mock

        result = strategy.decide(mock_market)

        assert result is not None
        assert result.intent_type.value == "HOLD"
        assert "Insufficient" in result.reason
        assert strategy._state == "idle"  # Did not transition

    def test_decide_idle_balance_error(
        self, strategy: EdgeUsdtSupplyEthStrategy, mock_market: MagicMock
    ) -> None:
        """Test that idle state holds when balance check fails."""
        mock_market.balance.side_effect = ValueError("Balance unavailable")

        result = strategy.decide(mock_market)

        assert result is not None
        assert result.intent_type.value == "HOLD"
        assert "Cannot check" in result.reason

    def test_decide_supplying_holds(
        self, strategy: EdgeUsdtSupplyEthStrategy, mock_market: MagicMock
    ) -> None:
        """Test that supplying state holds waiting for confirmation."""
        strategy._state = "supplying"

        result = strategy.decide(mock_market)

        assert result is not None
        assert result.intent_type.value == "HOLD"
        assert "confirmation" in result.reason.lower()

    def test_decide_supplied_holds(
        self, strategy: EdgeUsdtSupplyEthStrategy, mock_market: MagicMock
    ) -> None:
        """Test that supplied state holds when no exit conditions are met."""
        strategy._state = "supplied"
        strategy._supplied_amount = Decimal("2")
        strategy._entry_time = time.time()  # Just entered
        strategy._entry_price = Decimal("1.00")

        result = strategy.decide(mock_market)

        assert result is not None
        assert result.intent_type.value == "HOLD"
        assert "Holding" in result.reason

    def test_decide_supplied_exits_on_time_horizon(
        self, strategy: EdgeUsdtSupplyEthStrategy, mock_market: MagicMock
    ) -> None:
        """Test that supplied state triggers withdrawal after time horizon."""
        strategy._state = "supplied"
        strategy._supplied_amount = Decimal("2")
        # Set entry time to 200 hours ago (exceeds 168h horizon)
        strategy._entry_time = time.time() - (200 * 3600)
        strategy._entry_price = Decimal("1.00")

        result = strategy.decide(mock_market)

        assert result is not None
        assert result.intent_type.value == "WITHDRAW"
        assert strategy._state == "withdrawing"

    def test_decide_supplied_exits_on_stop_loss(
        self, strategy: EdgeUsdtSupplyEthStrategy, mock_market: MagicMock
    ) -> None:
        """Test that supplied state triggers withdrawal on USDT depeg stop-loss."""
        strategy._state = "supplied"
        strategy._supplied_amount = Decimal("2")
        strategy._entry_time = time.time()
        strategy._entry_price = Decimal("1.00")

        # Simulate USDT depegging to $0.85 (15% drop, exceeds -10% stop-loss)
        mock_market.price.return_value = Decimal("0.85")

        result = strategy.decide(mock_market)

        assert result is not None
        assert result.intent_type.value == "WITHDRAW"
        assert strategy._state == "withdrawing"

    def test_decide_supplied_holds_within_stop_loss(
        self, strategy: EdgeUsdtSupplyEthStrategy, mock_market: MagicMock
    ) -> None:
        """Test that supplied state holds when price drop is within stop-loss threshold."""
        strategy._state = "supplied"
        strategy._supplied_amount = Decimal("2")
        strategy._entry_time = time.time()
        strategy._entry_price = Decimal("1.00")

        # USDT at $0.95 (5% drop, within -10% stop-loss)
        mock_market.price.return_value = Decimal("0.95")

        result = strategy.decide(mock_market)

        assert result is not None
        assert result.intent_type.value == "HOLD"

    def test_decide_withdrawing_holds(
        self, strategy: EdgeUsdtSupplyEthStrategy, mock_market: MagicMock
    ) -> None:
        """Test that withdrawing state holds waiting for confirmation."""
        strategy._state = "withdrawing"

        result = strategy.decide(mock_market)

        assert result is not None
        assert result.intent_type.value == "HOLD"
        assert "confirmation" in result.reason.lower()

    def test_decide_done_holds(
        self, strategy: EdgeUsdtSupplyEthStrategy, mock_market: MagicMock
    ) -> None:
        """Test that done state holds permanently."""
        strategy._state = "done"

        result = strategy.decide(mock_market)

        assert result is not None
        assert result.intent_type.value == "HOLD"
        assert "completed" in result.reason.lower()

    def test_decide_handles_errors(
        self, strategy: EdgeUsdtSupplyEthStrategy, mock_market: MagicMock
    ) -> None:
        """Test that decide() handles unexpected errors gracefully."""
        mock_market.balance.side_effect = RuntimeError("Unexpected failure")

        result = strategy.decide(mock_market)

        assert result is not None
        assert "Error" in result.reason

    # -------------------------------------------------------------------------
    # on_intent_executed tests
    # -------------------------------------------------------------------------

    def test_on_supply_success(self, strategy: EdgeUsdtSupplyEthStrategy) -> None:
        """Test state transition on successful supply."""
        strategy._state = "supplying"
        intent = _make_intent_mock("SUPPLY")

        with patch.object(strategy, "create_market_snapshot") as mock_snap:
            snap = MagicMock()
            snap.price.return_value = Decimal("1.00")
            mock_snap.return_value = snap
            strategy.on_intent_executed(intent, success=True, result=None)

        assert strategy._state == "supplied"
        assert strategy._supplied_amount == Decimal("2")
        assert strategy._entry_time is not None
        assert strategy._entry_price == Decimal("1.00")

    def test_on_supply_success_snapshot_fails(
        self, strategy: EdgeUsdtSupplyEthStrategy
    ) -> None:
        """Test supply success fallback when snapshot fails."""
        strategy._state = "supplying"
        intent = _make_intent_mock("SUPPLY")

        with patch.object(
            strategy, "create_market_snapshot", side_effect=Exception("no gateway")
        ):
            strategy.on_intent_executed(intent, success=True, result=None)

        assert strategy._state == "supplied"
        assert strategy._entry_price == Decimal("1")  # USDT fallback

    def test_on_withdraw_success(self, strategy: EdgeUsdtSupplyEthStrategy) -> None:
        """Test state transition on successful withdrawal."""
        strategy._state = "withdrawing"
        strategy._supplied_amount = Decimal("2")
        intent = _make_intent_mock("WITHDRAW")

        strategy.on_intent_executed(intent, success=True, result=None)

        assert strategy._state == "done"
        assert strategy._supplied_amount == Decimal("0")

    def test_on_intent_failure_reverts(
        self, strategy: EdgeUsdtSupplyEthStrategy
    ) -> None:
        """Test state reversion on failed intent."""
        strategy._state = "supplying"
        strategy._previous_stable_state = "idle"
        intent = _make_intent_mock("SUPPLY")

        strategy.on_intent_executed(intent, success=False, result=None)

        assert strategy._state == "idle"

    def test_on_withdraw_failure_reverts(
        self, strategy: EdgeUsdtSupplyEthStrategy
    ) -> None:
        """Test state reversion on failed withdrawal."""
        strategy._state = "withdrawing"
        strategy._previous_stable_state = "supplied"
        intent = _make_intent_mock("WITHDRAW")

        strategy.on_intent_executed(intent, success=False, result=None)

        assert strategy._state == "supplied"

    # -------------------------------------------------------------------------
    # Persistent state tests
    # -------------------------------------------------------------------------

    def test_get_persistent_state(self, strategy: EdgeUsdtSupplyEthStrategy) -> None:
        """Test saving persistent state."""
        strategy._state = "supplied"
        strategy._supplied_amount = Decimal("2")
        strategy._entry_time = 1000000.0
        strategy._entry_price = Decimal("0.999")

        state = strategy.get_persistent_state()

        assert state["state"] == "supplied"
        assert state["supplied_amount"] == "2"
        assert state["entry_time"] == 1000000.0
        assert state["entry_price"] == "0.999"

    def test_load_persistent_state(self, strategy: EdgeUsdtSupplyEthStrategy) -> None:
        """Test restoring persistent state."""
        state = {
            "state": "supplied",
            "previous_stable_state": "idle",
            "supplied_amount": "2",
            "entry_time": 1000000.0,
            "entry_price": "0.998",
        }

        strategy.load_persistent_state(state)

        assert strategy._state == "supplied"
        assert strategy._previous_stable_state == "idle"
        assert strategy._supplied_amount == Decimal("2")
        assert strategy._entry_time == 1000000.0
        assert strategy._entry_price == Decimal("0.998")

    def test_load_persistent_state_empty(
        self, strategy: EdgeUsdtSupplyEthStrategy
    ) -> None:
        """Test loading empty state does not crash."""
        strategy.load_persistent_state({})

        assert strategy._state == "idle"
        assert strategy._supplied_amount == Decimal("0")

    # -------------------------------------------------------------------------
    # get_status tests
    # -------------------------------------------------------------------------

    def test_get_status(self, strategy: EdgeUsdtSupplyEthStrategy) -> None:
        """Test get_status returns expected fields."""
        status = strategy.get_status()

        assert status["strategy"] == "edge_usdt_supply_eth"
        assert status["chain"] == "ethereum"
        assert status["state"] == "idle"
        assert status["protocol"] == "aave_v3"
        assert status["collateral_token"] == "USDT"
        assert "signal_id" in status

    def test_get_status_with_elapsed(
        self, strategy: EdgeUsdtSupplyEthStrategy
    ) -> None:
        """Test get_status includes elapsed hours when position is active."""
        strategy._entry_time = time.time() - 3600  # 1 hour ago
        status = strategy.get_status()

        assert status["elapsed_hours"] is not None
        assert 0.9 <= status["elapsed_hours"] <= 1.1

    # -------------------------------------------------------------------------
    # Teardown tests
    # -------------------------------------------------------------------------

    def test_get_open_positions_no_supply(
        self, strategy: EdgeUsdtSupplyEthStrategy
    ) -> None:
        """Test get_open_positions returns empty when nothing supplied."""
        summary = strategy.get_open_positions()

        assert len(summary.positions) == 0

    def test_get_open_positions_with_supply(
        self, strategy: EdgeUsdtSupplyEthStrategy
    ) -> None:
        """Test get_open_positions returns supply position."""
        strategy._supplied_amount = Decimal("2")

        with patch.object(strategy, "create_market_snapshot") as mock_snap:
            snap = MagicMock()
            snap.price.return_value = Decimal("1.00")
            mock_snap.return_value = snap
            summary = strategy.get_open_positions()

        assert len(summary.positions) == 1
        pos = summary.positions[0]
        assert pos.position_type.value == "SUPPLY"
        assert pos.protocol == "aave_v3"
        assert pos.value_usd == Decimal("2")

    def test_generate_teardown_intents_with_supply(
        self, strategy: EdgeUsdtSupplyEthStrategy
    ) -> None:
        """Test teardown generates withdraw intent."""
        strategy._supplied_amount = Decimal("2")

        intents = strategy.generate_teardown_intents()

        assert len(intents) == 1
        assert intents[0].intent_type.value == "WITHDRAW"

    def test_generate_teardown_intents_no_supply(
        self, strategy: EdgeUsdtSupplyEthStrategy
    ) -> None:
        """Test teardown generates no intents when nothing supplied."""
        intents = strategy.generate_teardown_intents()

        assert len(intents) == 0

    # -------------------------------------------------------------------------
    # Full lifecycle test
    # -------------------------------------------------------------------------

    def test_full_lifecycle(
        self, strategy: EdgeUsdtSupplyEthStrategy, mock_market: MagicMock
    ) -> None:
        """Test the full lifecycle: idle -> supply -> monitor -> time exit -> withdraw -> done."""
        # 1. idle -> supply
        result = strategy.decide(mock_market)
        assert result.intent_type.value == "SUPPLY"
        assert strategy._state == "supplying"

        # 2. Confirm supply
        intent = _make_intent_mock("SUPPLY")
        with patch.object(strategy, "create_market_snapshot") as mock_snap:
            snap = MagicMock()
            snap.price.return_value = Decimal("1.00")
            mock_snap.return_value = snap
            strategy.on_intent_executed(intent, success=True, result=None)
        assert strategy._state == "supplied"

        # 3. supplied -> hold (no exit conditions yet)
        result = strategy.decide(mock_market)
        assert result.intent_type.value == "HOLD"
        assert "Holding" in result.reason

        # 4. Time passes beyond horizon -> withdraw
        strategy._entry_time = time.time() - (200 * 3600)
        result = strategy.decide(mock_market)
        assert result.intent_type.value == "WITHDRAW"
        assert strategy._state == "withdrawing"

        # 5. Confirm withdrawal
        withdraw_intent = _make_intent_mock("WITHDRAW")
        strategy.on_intent_executed(withdraw_intent, success=True, result=None)
        assert strategy._state == "done"
        assert strategy._supplied_amount == Decimal("0")

        # 6. done -> hold
        result = strategy.decide(mock_market)
        assert result.intent_type.value == "HOLD"
        assert "completed" in result.reason.lower()
