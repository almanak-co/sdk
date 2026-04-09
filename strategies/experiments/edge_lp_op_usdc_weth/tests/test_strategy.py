"""
Tests for edge_lp_op_usdc_weth strategy.

Covers:
- Initialization and config loading
- State machine transitions (idle -> opening -> open -> closing -> done)
- Entry conditions (balance checks, position sizing)
- Exit conditions (stop-loss, take-profit, time horizon)
- Rebalance trigger (out-of-range detection)
- Persistence (get_persistent_state / load_persistent_state)
- Teardown (get_open_positions / generate_teardown_intents)
- Error handling
"""

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from strategy import EdgeLpOpUsdcWethStrategy, StrategyState


# =============================================================================
# FIXTURES
# =============================================================================


@pytest.fixture
def config() -> dict:
    """Load test configuration from config.json."""
    config_path = Path(__file__).parent.parent / "config.json"
    if config_path.exists():
        with open(config_path) as f:
            return json.load(f)
    return {
        "pool": "USDC/WETH/3000",
        "token0": "USDC",
        "token1": "WETH",
        "fee_tier": 3000,
        "max_position_usd": "5",
        "entry_apr_threshold": "50",
        "range_pct": "30",
        "rebalance_threshold_pct": "80",
        "stop_loss_pct": "-0.20",
        "take_profit_pct": "0.25",
        "time_horizon_hours": 168,
        "max_slippage_bps": 50,
    }


@pytest.fixture
def strategy(config: dict) -> EdgeLpOpUsdcWethStrategy:
    """Create strategy instance for testing."""
    return EdgeLpOpUsdcWethStrategy(
        config=config,
        chain="optimism",
        wallet_address="0x" + "1" * 40,
    )


@pytest.fixture
def mock_market() -> MagicMock:
    """Create a mock MarketSnapshot with USDC=1, WETH=2000."""
    market = MagicMock()
    # USDC=$1, WETH=$2000
    def price_side_effect(token):
        if token == "USDC":
            return Decimal("1")
        if token == "WETH":
            return Decimal("2000")
        raise ValueError(f"Unknown token: {token}")

    market.price.side_effect = price_side_effect
    market.chain = "optimism"
    market.wallet_address = "0x" + "1" * 40

    # Balances: 10 USDC, 0.01 WETH (enough for $5 position)
    def balance_side_effect(token):
        bal = MagicMock()
        if token == "USDC":
            bal.balance = Decimal("10")
            bal.balance_usd = Decimal("10")
        elif token == "WETH":
            bal.balance = Decimal("0.01")
            bal.balance_usd = Decimal("20")
        else:
            raise ValueError(f"Unknown token: {token}")
        return bal

    market.balance.side_effect = balance_side_effect
    return market


# =============================================================================
# INITIALIZATION TESTS
# =============================================================================


class TestInitialization:
    """Test strategy initialization and config loading."""

    def test_initialization(self, strategy: EdgeLpOpUsdcWethStrategy) -> None:
        """Test strategy initializes with correct parameters."""
        assert strategy.chain == "optimism"
        assert strategy.pool == "USDC/WETH/3000"
        assert strategy.token0 == "USDC"
        assert strategy.token1 == "WETH"
        assert strategy.fee_tier == 3000
        assert strategy.max_position_usd == Decimal("5")
        assert strategy.stop_loss_pct == Decimal("-0.20")
        assert strategy.take_profit_pct == Decimal("0.25")
        assert strategy.time_horizon_hours == 168
        assert strategy._state == StrategyState.IDLE
        assert strategy._position_id is None

    def test_get_status(self, strategy: EdgeLpOpUsdcWethStrategy) -> None:
        """Test get_status returns expected fields."""
        status = strategy.get_status()
        assert status["strategy"] == "edge_lp_op_usdc_weth"
        assert status["chain"] == "optimism"
        assert status["state"] == "idle"
        assert status["position_id"] is None


# =============================================================================
# DECIDE: IDLE STATE TESTS
# =============================================================================


class TestIdleState:
    """Test decide() behavior when no position is open."""

    def test_opens_lp_when_conditions_met(
        self, strategy: EdgeLpOpUsdcWethStrategy, mock_market: MagicMock
    ) -> None:
        """Test that decide() returns LP_OPEN when balances are sufficient."""
        result = strategy.decide(mock_market)
        assert result is not None
        assert result.intent_type.value == "LP_OPEN"
        assert strategy._state == StrategyState.OPENING

    def test_hold_when_insufficient_token0(
        self, strategy: EdgeLpOpUsdcWethStrategy, mock_market: MagicMock
    ) -> None:
        """Test HOLD when USDC balance is too low."""
        def balance_side_effect(token):
            bal = MagicMock()
            if token == "USDC":
                bal.balance = Decimal("0.01")  # Very low
                bal.balance_usd = Decimal("0.01")
            elif token == "WETH":
                bal.balance = Decimal("0.01")
                bal.balance_usd = Decimal("20")
            return bal

        mock_market.balance.side_effect = balance_side_effect

        result = strategy.decide(mock_market)
        assert result is not None
        assert result.intent_type.value == "HOLD"
        assert "Insufficient USDC" in result.reason

    def test_hold_when_insufficient_token1(
        self, strategy: EdgeLpOpUsdcWethStrategy, mock_market: MagicMock
    ) -> None:
        """Test HOLD when WETH balance is too low."""
        def balance_side_effect(token):
            bal = MagicMock()
            if token == "USDC":
                bal.balance = Decimal("10")
                bal.balance_usd = Decimal("10")
            elif token == "WETH":
                bal.balance = Decimal("0.0000001")  # Very low
                bal.balance_usd = Decimal("0.0002")
            return bal

        mock_market.balance.side_effect = balance_side_effect

        result = strategy.decide(mock_market)
        assert result is not None
        assert result.intent_type.value == "HOLD"
        assert "Insufficient WETH" in result.reason

    def test_hold_when_balance_check_fails(
        self, strategy: EdgeLpOpUsdcWethStrategy, mock_market: MagicMock
    ) -> None:
        """Test HOLD when balance check throws error."""
        mock_market.balance.side_effect = ValueError("Balance unavailable")

        result = strategy.decide(mock_market)
        assert result is not None
        assert result.intent_type.value == "HOLD"

    def test_lp_open_price_range(
        self, strategy: EdgeLpOpUsdcWethStrategy, mock_market: MagicMock
    ) -> None:
        """Test that LP_OPEN has correct price range (+/-15% from current price)."""
        result = strategy.decide(mock_market)
        assert result.intent_type.value == "LP_OPEN"

        # Current price = WETH_USD / USDC_USD = 2000/1 = 2000
        # range_pct = 30 -> half = 15%
        # range_lower = 2000 * 0.85 = 1700
        # range_upper = 2000 * 1.15 = 2300
        assert result.range_lower == Decimal("2000") * Decimal("0.85")
        assert result.range_upper == Decimal("2000") * Decimal("1.15")

    def test_lp_open_amounts(
        self, strategy: EdgeLpOpUsdcWethStrategy, mock_market: MagicMock
    ) -> None:
        """Test that LP_OPEN has correct token amounts for $5 position."""
        result = strategy.decide(mock_market)
        assert result.intent_type.value == "LP_OPEN"

        # $5 position, 50/50 split -> $2.50 per side
        # amount0 (USDC) = $2.50 / $1 = 2.50
        # amount1 (WETH) = $2.50 / $2000 = 0.00125
        assert result.amount0 == Decimal("2.50") / Decimal("1")
        assert result.amount1 == Decimal("2.50") / Decimal("2000")


# =============================================================================
# DECIDE: OPEN STATE TESTS
# =============================================================================


class TestOpenState:
    """Test decide() behavior when position is open."""

    def _open_position(self, strategy: EdgeLpOpUsdcWethStrategy) -> None:
        """Helper: set strategy to OPEN state with a mock position."""
        strategy._state = StrategyState.OPEN
        strategy._position_id = "12345"
        strategy._range_lower = Decimal("1700")
        strategy._range_upper = Decimal("2300")
        strategy._position_opened_at = datetime.now(UTC)
        strategy._entry_value_usd = Decimal("5")

    def test_hold_when_in_range(
        self, strategy: EdgeLpOpUsdcWethStrategy, mock_market: MagicMock
    ) -> None:
        """Test HOLD when position is in range."""
        self._open_position(strategy)

        result = strategy.decide(mock_market)
        assert result is not None
        assert result.intent_type.value == "HOLD"
        assert "in range" in result.reason

    def test_close_on_time_horizon_exceeded(
        self, strategy: EdgeLpOpUsdcWethStrategy, mock_market: MagicMock
    ) -> None:
        """Test LP_CLOSE when time horizon is exceeded."""
        self._open_position(strategy)
        strategy._position_opened_at = datetime.now(UTC) - timedelta(hours=200)

        result = strategy.decide(mock_market)
        assert result is not None
        assert result.intent_type.value == "LP_CLOSE"
        assert strategy._state == StrategyState.CLOSING

    def test_close_on_stop_loss(
        self, strategy: EdgeLpOpUsdcWethStrategy, mock_market: MagicMock
    ) -> None:
        """Test LP_CLOSE when stop-loss is triggered."""
        self._open_position(strategy)

        # Simulate WETH price drop from 2000 to 1500 (−25%)
        # Entry midpoint was 2000, now 1500 -> ratio = 0.75 -> value drops ~25%
        def price_side_effect(token):
            if token == "USDC":
                return Decimal("1")
            if token == "WETH":
                return Decimal("1500")
            raise ValueError(f"Unknown: {token}")

        mock_market.price.side_effect = price_side_effect

        result = strategy.decide(mock_market)
        assert result is not None
        assert result.intent_type.value == "LP_CLOSE"
        assert strategy._state == StrategyState.CLOSING

    def test_close_on_take_profit(
        self, strategy: EdgeLpOpUsdcWethStrategy, mock_market: MagicMock
    ) -> None:
        """Test LP_CLOSE when take-profit is triggered."""
        self._open_position(strategy)

        # Simulate WETH price increase from 2000 to 2600 (+30%)
        # Entry midpoint was 2000, now 2600 -> ratio = 1.30 -> value +30% > +25% TP
        def price_side_effect(token):
            if token == "USDC":
                return Decimal("1")
            if token == "WETH":
                return Decimal("2600")
            raise ValueError(f"Unknown: {token}")

        mock_market.price.side_effect = price_side_effect

        result = strategy.decide(mock_market)
        assert result is not None
        assert result.intent_type.value == "LP_CLOSE"
        assert strategy._state == StrategyState.CLOSING

    def test_rebalance_when_out_of_range(
        self, strategy: EdgeLpOpUsdcWethStrategy, mock_market: MagicMock
    ) -> None:
        """Test LP_CLOSE for rebalance when price drifts near range edge."""
        self._open_position(strategy)
        # Range is [1700, 2300], width=600, center=2000
        # rebalance_threshold_pct=80 -> triggers when price is in bottom 10% or top 10%
        # lower_threshold = (1 - 0.80) / 2 = 0.10, upper_threshold = 0.90
        # At price 1750: position_in_range = (1750-1700)/600 = 0.083 < 0.10 -> rebalance

        def price_side_effect(token):
            if token == "USDC":
                return Decimal("1")
            if token == "WETH":
                return Decimal("1750")
            raise ValueError(f"Unknown: {token}")

        mock_market.price.side_effect = price_side_effect

        # Need entry value that doesn't trigger stop-loss
        # price_ratio = 1750/2000 = 0.875 -> PnL = -12.5% (within -20% SL)
        result = strategy.decide(mock_market)
        assert result is not None
        assert result.intent_type.value == "LP_CLOSE"
        assert strategy._state == StrategyState.REBALANCING
        assert strategy._rebalance_count == 1


# =============================================================================
# ON_INTENT_EXECUTED TESTS
# =============================================================================


class TestOnIntentExecuted:
    """Test lifecycle hooks for intent execution callbacks."""

    def test_lp_open_success(self, strategy: EdgeLpOpUsdcWethStrategy) -> None:
        """Test state transitions after successful LP_OPEN."""
        strategy._state = StrategyState.OPENING

        intent = MagicMock()
        intent.intent_type.value = "LP_OPEN"
        intent.range_lower = Decimal("1700")
        intent.range_upper = Decimal("2300")

        result = MagicMock()
        result.position_id = 12345

        strategy.on_intent_executed(intent, success=True, result=result)

        assert strategy._state == StrategyState.OPEN
        assert strategy._position_id == "12345"
        assert strategy._range_lower == Decimal("1700")
        assert strategy._range_upper == Decimal("2300")
        assert strategy._position_opened_at is not None

    def test_lp_open_failure(self, strategy: EdgeLpOpUsdcWethStrategy) -> None:
        """Test state reverts to IDLE on LP_OPEN failure."""
        strategy._state = StrategyState.OPENING

        intent = MagicMock()
        intent.intent_type.value = "LP_OPEN"

        strategy.on_intent_executed(intent, success=False, result=None)

        assert strategy._state == StrategyState.IDLE
        assert strategy._position_id is None

    def test_lp_close_success_from_closing(self, strategy: EdgeLpOpUsdcWethStrategy) -> None:
        """Test state transitions to DONE after close from CLOSING state."""
        strategy._state = StrategyState.CLOSING
        strategy._position_id = "12345"

        intent = MagicMock()
        intent.intent_type.value = "LP_CLOSE"

        strategy.on_intent_executed(intent, success=True, result=MagicMock())

        assert strategy._state == StrategyState.DONE
        assert strategy._position_id is None

    def test_lp_close_success_from_rebalancing(self, strategy: EdgeLpOpUsdcWethStrategy) -> None:
        """Test state stays REBALANCING after close for rebalance."""
        strategy._state = StrategyState.REBALANCING
        strategy._position_id = "12345"

        intent = MagicMock()
        intent.intent_type.value = "LP_CLOSE"

        strategy.on_intent_executed(intent, success=True, result=MagicMock())

        assert strategy._state == StrategyState.REBALANCING
        assert strategy._position_id is None

    def test_lp_close_failure(self, strategy: EdgeLpOpUsdcWethStrategy) -> None:
        """Test state reverts to OPEN on LP_CLOSE failure."""
        strategy._state = StrategyState.CLOSING
        strategy._position_id = "12345"

        intent = MagicMock()
        intent.intent_type.value = "LP_CLOSE"

        strategy.on_intent_executed(intent, success=False, result=None)

        assert strategy._state == StrategyState.OPEN
        assert strategy._position_id == "12345"


# =============================================================================
# PERSISTENCE TESTS
# =============================================================================


class TestPersistence:
    """Test state persistence for crash recovery."""

    def test_get_persistent_state_idle(self, strategy: EdgeLpOpUsdcWethStrategy) -> None:
        """Test persistent state in idle state."""
        state = strategy.get_persistent_state()
        assert state["strategy_state"] == "idle"
        assert "position_id" not in state

    def test_get_persistent_state_open(self, strategy: EdgeLpOpUsdcWethStrategy) -> None:
        """Test persistent state with open position."""
        strategy._state = StrategyState.OPEN
        strategy._position_id = "12345"
        strategy._range_lower = Decimal("1700")
        strategy._range_upper = Decimal("2300")
        strategy._position_opened_at = datetime(2026, 4, 1, tzinfo=UTC)
        strategy._entry_value_usd = Decimal("5")
        strategy._rebalance_count = 2

        state = strategy.get_persistent_state()
        assert state["strategy_state"] == "open"
        assert state["position_id"] == "12345"
        assert state["range_lower"] == "1700"
        assert state["range_upper"] == "2300"
        assert state["rebalance_count"] == 2
        assert "position_opened_at" in state
        assert state["entry_value_usd"] == "5"

    def test_load_persistent_state(self, strategy: EdgeLpOpUsdcWethStrategy) -> None:
        """Test restoring state from persistence."""
        saved_state = {
            "strategy_state": "open",
            "position_id": "67890",
            "range_lower": "1800",
            "range_upper": "2200",
            "position_opened_at": "2026-04-01T12:00:00+00:00",
            "entry_value_usd": "4.50",
            "rebalance_count": 1,
        }

        strategy.load_persistent_state(saved_state)

        assert strategy._state == StrategyState.OPEN
        assert strategy._position_id == "67890"
        assert strategy._range_lower == Decimal("1800")
        assert strategy._range_upper == Decimal("2200")
        assert strategy._entry_value_usd == Decimal("4.50")
        assert strategy._rebalance_count == 1
        assert strategy._position_opened_at is not None

    def test_load_empty_state(self, strategy: EdgeLpOpUsdcWethStrategy) -> None:
        """Test loading empty state doesn't crash."""
        strategy.load_persistent_state({})
        assert strategy._state == StrategyState.IDLE

    def test_roundtrip_persistence(self, strategy: EdgeLpOpUsdcWethStrategy) -> None:
        """Test save -> load roundtrip preserves all fields."""
        strategy._state = StrategyState.OPEN
        strategy._position_id = "99999"
        strategy._range_lower = Decimal("1600")
        strategy._range_upper = Decimal("2400")
        strategy._position_opened_at = datetime(2026, 4, 1, 10, 0, 0, tzinfo=UTC)
        strategy._entry_value_usd = Decimal("5")
        strategy._rebalance_count = 3

        saved = strategy.get_persistent_state()

        # Create fresh strategy and restore
        new_strategy = EdgeLpOpUsdcWethStrategy(
            config=strategy.config,
            chain="optimism",
            wallet_address="0x" + "1" * 40,
        )
        new_strategy.load_persistent_state(saved)

        assert new_strategy._state == StrategyState.OPEN
        assert new_strategy._position_id == "99999"
        assert new_strategy._range_lower == Decimal("1600")
        assert new_strategy._range_upper == Decimal("2400")
        assert new_strategy._entry_value_usd == Decimal("5")
        assert new_strategy._rebalance_count == 3


# =============================================================================
# TEARDOWN TESTS
# =============================================================================


class TestTeardown:
    """Test teardown support for safe position unwinding."""

    def test_no_positions_when_idle(self, strategy: EdgeLpOpUsdcWethStrategy) -> None:
        """Test get_open_positions returns empty when no position."""
        summary = strategy.get_open_positions()
        assert len(summary.positions) == 0

    def test_open_positions_when_active(self, strategy: EdgeLpOpUsdcWethStrategy) -> None:
        """Test get_open_positions returns position when active."""
        strategy._position_id = "12345"
        strategy._range_lower = Decimal("1700")
        strategy._range_upper = Decimal("2300")
        strategy._entry_value_usd = Decimal("5")

        summary = strategy.get_open_positions()
        assert len(summary.positions) == 1
        pos = summary.positions[0]
        assert pos.position_id == "12345"
        assert pos.protocol == "uniswap_v3"
        assert pos.value_usd == Decimal("5")

    def test_generate_teardown_intents_no_position(
        self, strategy: EdgeLpOpUsdcWethStrategy
    ) -> None:
        """Test teardown returns empty list when no position."""
        intents = strategy.generate_teardown_intents()
        assert intents == []

    def test_generate_teardown_intents_with_position(
        self, strategy: EdgeLpOpUsdcWethStrategy
    ) -> None:
        """Test teardown generates LP_CLOSE + SWAP intents."""
        strategy._position_id = "12345"

        intents = strategy.generate_teardown_intents()
        assert len(intents) == 2
        assert intents[0].intent_type.value == "LP_CLOSE"
        assert intents[0].position_id == "12345"
        assert intents[1].intent_type.value == "SWAP"
        assert intents[1].from_token == "WETH"
        assert intents[1].to_token == "USDC"

    def test_generate_teardown_hard_mode_slippage(
        self, strategy: EdgeLpOpUsdcWethStrategy
    ) -> None:
        """Test hard mode teardown uses higher slippage tolerance."""
        from almanak.framework.teardown import TeardownMode

        strategy._position_id = "12345"

        intents = strategy.generate_teardown_intents(mode=TeardownMode.HARD)
        swap_intent = intents[1]
        assert swap_intent.max_slippage == Decimal("0.03")


# =============================================================================
# ERROR HANDLING TESTS
# =============================================================================


class TestErrorHandling:
    """Test error handling in decide()."""

    def test_decide_handles_price_error(
        self, strategy: EdgeLpOpUsdcWethStrategy, mock_market: MagicMock
    ) -> None:
        """Test decide() returns HOLD on price error."""
        mock_market.price.side_effect = ValueError("Price unavailable")

        result = strategy.decide(mock_market)
        assert result is not None
        assert result.intent_type.value == "HOLD"
        assert "Error" in result.reason

    def test_decide_done_state(
        self, strategy: EdgeLpOpUsdcWethStrategy, mock_market: MagicMock
    ) -> None:
        """Test decide() returns HOLD in DONE state."""
        strategy._state = StrategyState.DONE

        result = strategy.decide(mock_market)
        assert result is not None
        assert result.intent_type.value == "HOLD"
        assert "done" in result.reason.lower()

    def test_decide_opening_state(
        self, strategy: EdgeLpOpUsdcWethStrategy, mock_market: MagicMock
    ) -> None:
        """Test decide() returns HOLD in OPENING state (waiting)."""
        strategy._state = StrategyState.OPENING

        result = strategy.decide(mock_market)
        assert result is not None
        assert result.intent_type.value == "HOLD"

    def test_lost_position_tracking(
        self, strategy: EdgeLpOpUsdcWethStrategy, mock_market: MagicMock
    ) -> None:
        """Test recovery when position_id is lost in OPEN state."""
        strategy._state = StrategyState.OPEN
        strategy._position_id = None  # Lost!

        result = strategy.decide(mock_market)
        # Should revert to IDLE and hold
        assert strategy._state == StrategyState.IDLE
        assert result.intent_type.value == "HOLD"
