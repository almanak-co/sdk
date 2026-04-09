"""
Tests for edge_eth_funding_long strategy.

Tests the state machine, exit conditions, persistence, and teardown.
"""

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from strategy import EdgeEthFundingLongStrategy


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def config() -> dict:
    """Load test configuration from config.json."""
    config_path = Path(__file__).parent.parent / "config.json"
    if config_path.exists():
        with open(config_path) as f:
            return json.load(f)
    return {
        "perp_market": "ETH/USD",
        "collateral_token": "USDC",
        "collateral_amount": "2.5",
        "position_size_usd": "5",
        "leverage": "2",
        "max_slippage": "0.005",
        "base_token": "ETH",
        "take_profit_pct": "0.25",
        "stop_loss_pct": "0.15",
        "time_horizon_hours": 168,
        "chain": "arbitrum",
    }


@pytest.fixture
def strategy(config: dict) -> EdgeEthFundingLongStrategy:
    """Create strategy instance for testing."""
    return EdgeEthFundingLongStrategy(
        config=config,
        chain=config.get("chain", "arbitrum"),
        wallet_address="0x" + "1" * 40,
    )


@pytest.fixture
def mock_market() -> MagicMock:
    """Create a mock MarketSnapshot with sufficient balance."""
    market = MagicMock()
    market.price.return_value = Decimal("2000")
    market.chain = "arbitrum"
    market.wallet_address = "0x" + "1" * 40

    balance_mock = MagicMock()
    balance_mock.balance = Decimal("100")  # Plenty of USDC
    balance_mock.balance_usd = Decimal("100")
    market.balance.return_value = balance_mock

    return market


def _make_intent_mock(intent_type_value: str) -> MagicMock:
    """Create a mock intent with the given type value."""
    intent = MagicMock()
    intent.intent_type.value = intent_type_value
    return intent


def _make_result_mock(entry_price=None) -> MagicMock:
    """Create a mock execution result."""
    result = MagicMock()
    if entry_price is not None:
        result.extracted_data = {"entry_price": entry_price}
    else:
        result.extracted_data = {}
    return result


# ---------------------------------------------------------------------------
# Initialization
# ---------------------------------------------------------------------------


class TestInitialization:
    def test_defaults(self, strategy: EdgeEthFundingLongStrategy) -> None:
        assert strategy.perp_market == "ETH/USD"
        assert strategy.collateral_token == "USDC"
        assert strategy.position_size_usd == Decimal("5")
        assert strategy.leverage == Decimal("2")
        assert strategy.take_profit_pct == Decimal("0.25")
        assert strategy.stop_loss_pct == Decimal("0.15")
        assert strategy.time_horizon_hours == 168
        assert strategy._state == "idle"

    def test_chain(self, strategy: EdgeEthFundingLongStrategy) -> None:
        assert strategy.chain == "arbitrum"


# ---------------------------------------------------------------------------
# State machine: idle -> opening -> open -> closing -> done
# ---------------------------------------------------------------------------


class TestStateMachine:
    def test_idle_opens_position(self, strategy, mock_market) -> None:
        """idle state should emit PERP_OPEN."""
        result = strategy.decide(mock_market)
        assert result is not None
        assert result.intent_type.value == "PERP_OPEN"
        assert strategy._state == "opening"

    def test_opening_holds(self, strategy, mock_market) -> None:
        """opening state should hold while waiting for callback."""
        strategy._state = "opening"
        result = strategy.decide(mock_market)
        assert result.intent_type.value == "HOLD"

    def test_opening_success_transitions_to_open(self, strategy) -> None:
        """Successful PERP_OPEN should advance to open."""
        strategy._state = "opening"
        strategy._pending_entry_price = Decimal("2000")

        intent = _make_intent_mock("PERP_OPEN")
        result = _make_result_mock()
        strategy.on_intent_executed(intent, success=True, result=result)

        assert strategy._state == "open"
        assert strategy._entry_price == Decimal("2000")
        assert strategy._opened_at is not None

    def test_opening_success_uses_extracted_price(self, strategy) -> None:
        """Entry price from ResultEnricher should be preferred."""
        strategy._state = "opening"
        strategy._pending_entry_price = Decimal("2000")

        intent = _make_intent_mock("PERP_OPEN")
        result = _make_result_mock(entry_price=Decimal("2010"))
        strategy.on_intent_executed(intent, success=True, result=result)

        assert strategy._entry_price == Decimal("2010")

    def test_opening_failure_rolls_back(self, strategy) -> None:
        """Failed PERP_OPEN should rollback to idle."""
        strategy._state = "opening"
        strategy._previous_stable_state = "idle"

        intent = _make_intent_mock("PERP_OPEN")
        strategy.on_intent_executed(intent, success=False, result=MagicMock())

        assert strategy._state == "idle"

    def test_open_hold_when_no_exit_conditions(self, strategy, mock_market) -> None:
        """Open position with no exit triggers should hold."""
        strategy._state = "open"
        strategy._entry_price = Decimal("2000")
        strategy._opened_at = datetime.now(UTC).isoformat()

        result = strategy.decide(mock_market)
        assert result.intent_type.value == "HOLD"
        assert strategy._state == "open"

    def test_closing_holds(self, strategy, mock_market) -> None:
        """closing state should hold while waiting for callback."""
        strategy._state = "closing"
        result = strategy.decide(mock_market)
        assert result.intent_type.value == "HOLD"

    def test_closing_success_transitions_to_done(self, strategy) -> None:
        """Successful PERP_CLOSE should advance to done."""
        strategy._state = "closing"
        intent = _make_intent_mock("PERP_CLOSE")
        strategy.on_intent_executed(intent, success=True, result=MagicMock())

        assert strategy._state == "done"

    def test_closing_failure_rolls_back(self, strategy) -> None:
        """Failed PERP_CLOSE should rollback to open."""
        strategy._state = "closing"
        strategy._previous_stable_state = "open"
        intent = _make_intent_mock("PERP_CLOSE")
        strategy.on_intent_executed(intent, success=False, result=MagicMock())

        assert strategy._state == "open"

    def test_done_holds(self, strategy, mock_market) -> None:
        """done state should hold forever."""
        strategy._state = "done"
        result = strategy.decide(mock_market)
        assert result.intent_type.value == "HOLD"
        assert "complete" in result.reason.lower()


# ---------------------------------------------------------------------------
# Exit conditions
# ---------------------------------------------------------------------------


class TestExitConditions:
    def test_take_profit_triggers_close(self, strategy, mock_market) -> None:
        """Price above TP threshold should trigger close."""
        strategy._state = "open"
        strategy._entry_price = Decimal("2000")
        strategy._opened_at = datetime.now(UTC).isoformat()

        # 30% gain -> exceeds 25% TP
        mock_market.price.return_value = Decimal("2600")
        result = strategy.decide(mock_market)

        assert result.intent_type.value == "PERP_CLOSE"
        assert strategy._state == "closing"

    def test_stop_loss_triggers_close(self, strategy, mock_market) -> None:
        """Price below SL threshold should trigger close."""
        strategy._state = "open"
        strategy._entry_price = Decimal("2000")
        strategy._opened_at = datetime.now(UTC).isoformat()

        # 20% loss -> exceeds 15% SL
        mock_market.price.return_value = Decimal("1600")
        result = strategy.decide(mock_market)

        assert result.intent_type.value == "PERP_CLOSE"
        assert strategy._state == "closing"

    def test_time_horizon_triggers_close(self, strategy, mock_market) -> None:
        """Position held past time horizon should trigger close."""
        strategy._state = "open"
        strategy._entry_price = Decimal("2000")
        # Opened 200 hours ago (exceeds 168h horizon)
        opened = datetime.now(UTC) - timedelta(hours=200)
        strategy._opened_at = opened.isoformat()

        result = strategy.decide(mock_market)

        assert result.intent_type.value == "PERP_CLOSE"
        assert strategy._state == "closing"

    def test_no_exit_within_bounds(self, strategy, mock_market) -> None:
        """Price within bounds and within time horizon should hold."""
        strategy._state = "open"
        strategy._entry_price = Decimal("2000")
        strategy._opened_at = datetime.now(UTC).isoformat()

        # 5% gain — within TP (25%) and SL (15%)
        mock_market.price.return_value = Decimal("2100")
        result = strategy.decide(mock_market)

        assert result.intent_type.value == "HOLD"
        assert strategy._state == "open"


# ---------------------------------------------------------------------------
# Insufficient balance
# ---------------------------------------------------------------------------


class TestBalanceCheck:
    def test_insufficient_balance_holds(self, strategy, mock_market) -> None:
        """Should hold if insufficient collateral."""
        balance_mock = MagicMock()
        balance_mock.balance = Decimal("0.5")  # < 2.5 required
        mock_market.balance.return_value = balance_mock

        result = strategy.decide(mock_market)
        assert result.intent_type.value == "HOLD"
        assert "Insufficient" in result.reason
        assert strategy._state == "idle"

    def test_balance_error_holds(self, strategy, mock_market) -> None:
        """Should hold gracefully if balance check raises."""
        mock_market.balance.side_effect = ValueError("Balance unavailable")
        result = strategy.decide(mock_market)
        assert result.intent_type.value == "HOLD"
        assert strategy._state == "idle"


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


class TestErrorHandling:
    def test_price_error_returns_hold(self, strategy, mock_market) -> None:
        """Price fetch error should return hold, not crash."""
        mock_market.price.side_effect = ValueError("Price unavailable")
        result = strategy.decide(mock_market)
        assert result is not None
        assert result.intent_type.value == "HOLD"

    def test_unknown_state_returns_hold(self, strategy, mock_market) -> None:
        """Unknown state should return hold."""
        strategy._state = "garbage"
        result = strategy.decide(mock_market)
        assert result.intent_type.value == "HOLD"


# ---------------------------------------------------------------------------
# State persistence
# ---------------------------------------------------------------------------


class TestPersistence:
    def test_get_persistent_state(self, strategy) -> None:
        strategy._state = "open"
        strategy._entry_price = Decimal("1950.50")
        strategy._opened_at = "2026-04-01T12:00:00+00:00"
        strategy._previous_stable_state = "idle"

        state = strategy.get_persistent_state()
        assert state["state"] == "open"
        assert state["entry_price"] == "1950.50"
        assert state["opened_at"] == "2026-04-01T12:00:00+00:00"
        assert state["previous_stable_state"] == "idle"

    def test_load_persistent_state(self, strategy) -> None:
        state = {
            "state": "open",
            "previous_stable_state": "idle",
            "entry_price": "1950.50",
            "opened_at": "2026-04-01T12:00:00+00:00",
        }
        strategy.load_persistent_state(state)
        assert strategy._state == "open"
        assert strategy._entry_price == Decimal("1950.50")
        assert strategy._opened_at == "2026-04-01T12:00:00+00:00"

    def test_load_empty_state(self, strategy) -> None:
        """Loading None/empty should keep defaults."""
        strategy.load_persistent_state(None)
        assert strategy._state == "idle"

        strategy.load_persistent_state({})
        assert strategy._state == "idle"

    def test_roundtrip(self, strategy) -> None:
        """get -> load should be lossless."""
        strategy._state = "open"
        strategy._entry_price = Decimal("2500")
        strategy._opened_at = "2026-04-01T00:00:00+00:00"
        strategy._previous_stable_state = "idle"

        saved = strategy.get_persistent_state()

        # Reset and reload
        strategy._state = "idle"
        strategy._entry_price = None
        strategy._opened_at = None

        strategy.load_persistent_state(saved)
        assert strategy._state == "open"
        assert strategy._entry_price == Decimal("2500")
        assert strategy._opened_at == "2026-04-01T00:00:00+00:00"


# ---------------------------------------------------------------------------
# Teardown
# ---------------------------------------------------------------------------


class TestTeardown:
    def test_teardown_when_open(self, strategy) -> None:
        """Open position should generate PERP_CLOSE teardown intent."""
        strategy._state = "open"
        strategy._entry_price = Decimal("2000")

        intents = strategy.generate_teardown_intents(mode=None, market=None)
        assert len(intents) == 1
        assert intents[0].intent_type.value == "PERP_CLOSE"

    def test_teardown_when_idle(self, strategy) -> None:
        """No position should generate no teardown intents."""
        strategy._state = "idle"
        intents = strategy.generate_teardown_intents(mode=None, market=None)
        assert len(intents) == 0

    def test_teardown_when_done(self, strategy) -> None:
        """Done state should generate no teardown intents."""
        strategy._state = "done"
        intents = strategy.generate_teardown_intents(mode=None, market=None)
        assert len(intents) == 0

    def test_teardown_hard_mode_slippage(self, strategy) -> None:
        """HARD mode should use higher slippage tolerance."""
        from almanak.framework.teardown import TeardownMode

        strategy._state = "open"
        intents = strategy.generate_teardown_intents(mode=TeardownMode.HARD, market=None)
        assert len(intents) == 1
        assert intents[0].max_slippage == Decimal("0.03")

    def test_get_open_positions_when_open(self, strategy) -> None:
        """Should report position when state is open."""
        strategy._state = "open"
        strategy._entry_price = Decimal("2000")
        strategy._opened_at = "2026-04-01T00:00:00+00:00"

        summary = strategy.get_open_positions()
        assert len(summary.positions) == 1
        pos = summary.positions[0]
        assert pos.protocol == "gmx_v2"
        assert pos.details["is_long"] is True

    def test_get_open_positions_when_idle(self, strategy) -> None:
        """Should report no positions when idle."""
        summary = strategy.get_open_positions()
        assert len(summary.positions) == 0


# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------


class TestStatus:
    def test_get_status(self, strategy) -> None:
        status = strategy.get_status()
        assert status["strategy"] == "edge_eth_funding_long"
        assert status["signal_id"] == "4869b240-e380-42c6-95dd-a70c913a35a8"
        assert status["chain"] == "arbitrum"
        assert status["state"] == "idle"
        assert status["position_size_usd"] == "5"
