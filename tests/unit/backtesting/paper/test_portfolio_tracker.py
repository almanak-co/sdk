"""Unit tests for PaperPortfolioTracker.

Tests cover:
- PaperPortfolioTracker session initialization
- record_trade updates balances correctly
- record_error records errors properly
- get_pnl_usd calculates PnL correctly
- get_summary returns valid PaperTradingSummary
- Token balance queries and changes
- Serialization and deserialization
"""

from datetime import datetime, timedelta
from decimal import Decimal

import pytest

from almanak.framework.backtesting.paper.models import (
    PaperTrade,
    PaperTradeError,
    PaperTradeErrorType,
    PaperTradingSummary,
)
from almanak.framework.backtesting.paper.portfolio_tracker import PaperPortfolioTracker

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def base_timestamp() -> datetime:
    """Base timestamp for tests."""
    return datetime(2024, 1, 1, 12, 0, 0)


@pytest.fixture
def tracker() -> PaperPortfolioTracker:
    """Create a fresh PaperPortfolioTracker."""
    return PaperPortfolioTracker(strategy_id="test_strategy")


@pytest.fixture
def tracker_with_session(tracker: PaperPortfolioTracker) -> PaperPortfolioTracker:
    """Create a tracker with an active session."""
    tracker.start_session(
        initial_balances={
            "ETH": Decimal("10"),
            "USDC": Decimal("10000"),
        },
        chain="arbitrum",
    )
    return tracker


@pytest.fixture
def sample_trade(base_timestamp: datetime) -> PaperTrade:
    """Create a sample successful trade."""
    return PaperTrade(
        timestamp=base_timestamp,
        block_number=12345678,
        intent={"type": "SWAP", "from_token": "ETH", "to_token": "USDC"},
        tx_hash="0xabcd1234",
        gas_used=150000,
        gas_cost_usd=Decimal("0.50"),
        tokens_in={"USDC": Decimal("3000")},
        tokens_out={"ETH": Decimal("1")},
        protocol="uniswap_v3",
        intent_type="SWAP",
    )


@pytest.fixture
def sample_error(base_timestamp: datetime) -> PaperTradeError:
    """Create a sample trade error."""
    return PaperTradeError(
        timestamp=base_timestamp,
        intent={"type": "SWAP"},
        error_type=PaperTradeErrorType.REVERT,
        error_message="Slippage exceeded",
    )


# =============================================================================
# Tracker Initialization Tests
# =============================================================================


class TestPaperPortfolioTrackerInit:
    """Tests for tracker initialization."""

    def test_tracker_creation(self) -> None:
        """Test creating a tracker with strategy_id."""
        tracker = PaperPortfolioTracker(strategy_id="my_strategy")
        assert tracker.strategy_id == "my_strategy"
        assert tracker.chain == "arbitrum"  # default
        assert tracker.session_started is None
        assert not tracker.is_session_active()

    def test_tracker_with_custom_chain(self) -> None:
        """Test creating a tracker with custom chain."""
        tracker = PaperPortfolioTracker(strategy_id="test", chain="ethereum")
        assert tracker.chain == "ethereum"


# =============================================================================
# Session Management Tests
# =============================================================================


class TestPaperPortfolioTrackerSession:
    """Tests for session management."""

    def test_start_session(self, tracker: PaperPortfolioTracker) -> None:
        """Test starting a session."""
        tracker.start_session(initial_balances={"ETH": Decimal("5"), "USDC": Decimal("5000")})

        assert tracker.is_session_active()
        assert tracker.session_started is not None
        assert tracker.initial_balances == {
            "ETH": Decimal("5"),
            "USDC": Decimal("5000"),
        }
        assert tracker.current_balances == {
            "ETH": Decimal("5"),
            "USDC": Decimal("5000"),
        }

    def test_start_session_resets_state(
        self, tracker_with_session: PaperPortfolioTracker, sample_trade: PaperTrade
    ) -> None:
        """Test starting a new session resets all state."""
        # Add some activity
        tracker_with_session.record_trade(sample_trade)
        assert len(tracker_with_session.trades) == 1

        # Start new session
        tracker_with_session.start_session(initial_balances={"BTC": Decimal("1")})

        # State should be reset
        assert tracker_with_session.trades == []
        assert tracker_with_session.errors == []
        assert tracker_with_session.total_gas_used == 0
        assert tracker_with_session.initial_balances == {"BTC": Decimal("1")}

    def test_start_session_with_chain(self, tracker: PaperPortfolioTracker) -> None:
        """Test starting a session with custom chain."""
        tracker.start_session(
            initial_balances={"ETH": Decimal("1")},
            chain="ethereum",
        )
        assert tracker.chain == "ethereum"


# =============================================================================
# Record Trade Tests
# =============================================================================


class TestPaperPortfolioTrackerRecordTrade:
    """Tests for record_trade method."""

    def test_record_trade_updates_balances(
        self,
        tracker_with_session: PaperPortfolioTracker,
        sample_trade: PaperTrade,
    ) -> None:
        """Test that record_trade updates balances correctly."""
        # Initial: ETH=10, USDC=10000
        # Trade: -1 ETH, +3000 USDC
        tracker_with_session.record_trade(sample_trade)

        assert tracker_with_session.get_token_balance("ETH") == Decimal("9")
        assert tracker_with_session.get_token_balance("USDC") == Decimal("13000")

    def test_record_trade_adds_to_trades_list(
        self,
        tracker_with_session: PaperPortfolioTracker,
        sample_trade: PaperTrade,
    ) -> None:
        """Test that record_trade adds trade to list."""
        tracker_with_session.record_trade(sample_trade)
        assert len(tracker_with_session.trades) == 1
        assert tracker_with_session.trades[0] == sample_trade

    def test_record_trade_updates_gas_tracking(
        self,
        tracker_with_session: PaperPortfolioTracker,
        sample_trade: PaperTrade,
    ) -> None:
        """Test that record_trade updates gas tracking."""
        tracker_with_session.record_trade(sample_trade)

        assert tracker_with_session.total_gas_used == 150000
        assert tracker_with_session.total_gas_cost_usd == Decimal("0.50")

    def test_record_multiple_trades(
        self,
        tracker_with_session: PaperPortfolioTracker,
        base_timestamp: datetime,
    ) -> None:
        """Test recording multiple trades."""
        # First trade: sell 1 ETH for 3000 USDC
        trade1 = PaperTrade(
            timestamp=base_timestamp,
            block_number=12345678,
            intent={"type": "SWAP"},
            tx_hash="0x1111",
            gas_used=150000,
            gas_cost_usd=Decimal("0.50"),
            tokens_in={"USDC": Decimal("3000")},
            tokens_out={"ETH": Decimal("1")},
        )

        # Second trade: sell 2000 USDC for 0.67 ETH
        trade2 = PaperTrade(
            timestamp=base_timestamp + timedelta(minutes=5),
            block_number=12345679,
            intent={"type": "SWAP"},
            tx_hash="0x2222",
            gas_used=160000,
            gas_cost_usd=Decimal("0.55"),
            tokens_in={"ETH": Decimal("0.67")},
            tokens_out={"USDC": Decimal("2000")},
        )

        tracker_with_session.record_trade(trade1)
        tracker_with_session.record_trade(trade2)

        # Initial: ETH=10, USDC=10000
        # After trade1: ETH=9, USDC=13000
        # After trade2: ETH=9.67, USDC=11000
        assert tracker_with_session.get_token_balance("ETH") == Decimal("9.67")
        assert tracker_with_session.get_token_balance("USDC") == Decimal("11000")

        assert tracker_with_session.total_gas_used == 310000
        assert tracker_with_session.total_gas_cost_usd == Decimal("1.05")

    def test_record_trade_adds_new_token(
        self,
        tracker_with_session: PaperPortfolioTracker,
        base_timestamp: datetime,
    ) -> None:
        """Test that record_trade can add a new token to balances."""
        trade = PaperTrade(
            timestamp=base_timestamp,
            block_number=12345678,
            intent={"type": "SWAP"},
            tx_hash="0xabcd",
            gas_used=150000,
            gas_cost_usd=Decimal("0.50"),
            tokens_in={"ARB": Decimal("1000")},  # New token
            tokens_out={"USDC": Decimal("1500")},
        )

        tracker_with_session.record_trade(trade)

        assert tracker_with_session.get_token_balance("ARB") == Decimal("1000")
        assert tracker_with_session.get_token_balance("USDC") == Decimal("8500")

    def test_record_trade_removes_zero_balances(
        self,
        tracker_with_session: PaperPortfolioTracker,
        base_timestamp: datetime,
    ) -> None:
        """Test that zero balances are cleaned up."""
        # Sell all ETH
        trade = PaperTrade(
            timestamp=base_timestamp,
            block_number=12345678,
            intent={"type": "SWAP"},
            tx_hash="0xabcd",
            gas_used=150000,
            gas_cost_usd=Decimal("0.50"),
            tokens_in={"USDC": Decimal("30000")},
            tokens_out={"ETH": Decimal("10")},  # All ETH
        )

        tracker_with_session.record_trade(trade)

        # ETH should be removed from balances
        assert "ETH" not in tracker_with_session.current_balances
        assert tracker_with_session.get_token_balance("ETH") == Decimal("0")


# =============================================================================
# Record Error Tests
# =============================================================================


class TestPaperPortfolioTrackerRecordError:
    """Tests for record_error method."""

    def test_record_error_adds_to_list(
        self,
        tracker_with_session: PaperPortfolioTracker,
        sample_error: PaperTradeError,
    ) -> None:
        """Test that record_error adds error to list."""
        tracker_with_session.record_error(sample_error)
        assert len(tracker_with_session.errors) == 1
        assert tracker_with_session.errors[0] == sample_error

    def test_record_error_does_not_affect_balances(
        self,
        tracker_with_session: PaperPortfolioTracker,
        sample_error: PaperTradeError,
    ) -> None:
        """Test that record_error does not change balances."""
        initial_eth = tracker_with_session.get_token_balance("ETH")
        initial_usdc = tracker_with_session.get_token_balance("USDC")

        tracker_with_session.record_error(sample_error)

        assert tracker_with_session.get_token_balance("ETH") == initial_eth
        assert tracker_with_session.get_token_balance("USDC") == initial_usdc

    def test_record_multiple_errors(
        self,
        tracker_with_session: PaperPortfolioTracker,
        base_timestamp: datetime,
    ) -> None:
        """Test recording multiple errors."""
        error1 = PaperTradeError(
            timestamp=base_timestamp,
            intent={"type": "SWAP"},
            error_type=PaperTradeErrorType.REVERT,
            error_message="Slippage exceeded",
        )
        error2 = PaperTradeError(
            timestamp=base_timestamp + timedelta(minutes=1),
            intent={"type": "SWAP"},
            error_type=PaperTradeErrorType.RPC_ERROR,
            error_message="Connection timeout",
        )

        tracker_with_session.record_error(error1)
        tracker_with_session.record_error(error2)

        assert len(tracker_with_session.errors) == 2
        assert tracker_with_session.get_error_count() == 2


# =============================================================================
# PnL Calculation Tests
# =============================================================================


class TestPaperPortfolioTrackerGetPnlUsd:
    """Tests for get_pnl_usd method."""

    def test_get_pnl_usd_no_change(self, tracker_with_session: PaperPortfolioTracker) -> None:
        """Test PnL when no trades occurred (should be 0)."""
        prices = {"ETH": Decimal("3000"), "USDC": Decimal("1")}
        pnl = tracker_with_session.get_pnl_usd(prices)

        # Initial: 10 ETH @ 3000 + 10000 USDC = 40000
        # Current: same = 40000
        # PnL = 0
        assert pnl == Decimal("0")

    def test_get_pnl_usd_after_profitable_trade(
        self,
        tracker_with_session: PaperPortfolioTracker,
        base_timestamp: datetime,
    ) -> None:
        """Test PnL after a profitable trade."""
        # Sell 1 ETH for 3100 USDC (better than market)
        trade = PaperTrade(
            timestamp=base_timestamp,
            block_number=12345678,
            intent={"type": "SWAP"},
            tx_hash="0xabcd",
            gas_used=150000,
            gas_cost_usd=Decimal("0.50"),
            tokens_in={"USDC": Decimal("3100")},
            tokens_out={"ETH": Decimal("1")},
        )
        tracker_with_session.record_trade(trade)

        # Now: 9 ETH + 13100 USDC
        # At prices: 9 * 3000 + 13100 = 27000 + 13100 = 40100
        # Initial value: 10 * 3000 + 10000 = 40000
        # PnL = 100
        prices = {"ETH": Decimal("3000"), "USDC": Decimal("1")}
        pnl = tracker_with_session.get_pnl_usd(prices)

        assert pnl == Decimal("100")

    def test_get_pnl_usd_after_losing_trade(
        self,
        tracker_with_session: PaperPortfolioTracker,
        base_timestamp: datetime,
    ) -> None:
        """Test PnL after a losing trade."""
        # Sell 1 ETH for 2900 USDC (worse than market)
        trade = PaperTrade(
            timestamp=base_timestamp,
            block_number=12345678,
            intent={"type": "SWAP"},
            tx_hash="0xabcd",
            gas_used=150000,
            gas_cost_usd=Decimal("0.50"),
            tokens_in={"USDC": Decimal("2900")},
            tokens_out={"ETH": Decimal("1")},
        )
        tracker_with_session.record_trade(trade)

        # Now: 9 ETH + 12900 USDC
        # At prices: 9 * 3000 + 12900 = 27000 + 12900 = 39900
        # Initial value: 40000
        # PnL = -100
        prices = {"ETH": Decimal("3000"), "USDC": Decimal("1")}
        pnl = tracker_with_session.get_pnl_usd(prices)

        assert pnl == Decimal("-100")

    def test_get_pnl_usd_with_price_change(self, tracker_with_session: PaperPortfolioTracker) -> None:
        """Test PnL when prices have changed."""
        # No trades, but ETH price increased
        prices = {"ETH": Decimal("3500"), "USDC": Decimal("1")}
        pnl = tracker_with_session.get_pnl_usd(prices)

        # Initial value at current prices: 10 * 3500 + 10000 = 45000
        # Current value: same = 45000
        # But PnL compares portfolio change, so still 0
        # Wait - the formula uses same prices for both
        assert pnl == Decimal("0")

    def test_get_pnl_usd_stablecoin_default(self, tracker_with_session: PaperPortfolioTracker) -> None:
        """Test that stablecoins default to $1 when no price provided."""
        # Only provide ETH price
        prices = {"ETH": Decimal("3000")}
        pnl = tracker_with_session.get_pnl_usd(prices)

        # USDC should be valued at $1
        assert pnl == Decimal("0")


# =============================================================================
# Get Summary Tests
# =============================================================================


class TestPaperPortfolioTrackerGetSummary:
    """Tests for get_summary method."""

    def test_get_summary_no_trades(self, tracker_with_session: PaperPortfolioTracker) -> None:
        """Test summary with no trades."""
        summary = tracker_with_session.get_summary()

        assert isinstance(summary, PaperTradingSummary)
        assert summary.strategy_id == "test_strategy"
        assert summary.total_trades == 0
        assert summary.successful_trades == 0
        assert summary.failed_trades == 0
        assert summary.success_rate == Decimal("1")  # 100% when no trades
        assert summary.chain == "arbitrum"

    def test_get_summary_with_trades_and_errors(
        self,
        tracker_with_session: PaperPortfolioTracker,
        sample_trade: PaperTrade,
        sample_error: PaperTradeError,
    ) -> None:
        """Test summary with trades and errors."""
        tracker_with_session.record_trade(sample_trade)
        tracker_with_session.record_error(sample_error)

        summary = tracker_with_session.get_summary()

        assert summary.total_trades == 2  # 1 success + 1 failure
        assert summary.successful_trades == 1
        assert summary.failed_trades == 1
        assert summary.success_rate == Decimal("0.5")  # 50%
        assert summary.total_gas_used == 150000
        assert summary.total_gas_cost_usd == Decimal("0.50")

    def test_get_summary_includes_balances(
        self,
        tracker_with_session: PaperPortfolioTracker,
        sample_trade: PaperTrade,
    ) -> None:
        """Test summary includes initial and final balances."""
        tracker_with_session.record_trade(sample_trade)
        summary = tracker_with_session.get_summary()

        assert summary.initial_balances == {
            "ETH": Decimal("10"),
            "USDC": Decimal("10000"),
        }
        assert summary.final_balances == {
            "ETH": Decimal("9"),
            "USDC": Decimal("13000"),
        }

    def test_get_summary_error_breakdown(
        self,
        tracker_with_session: PaperPortfolioTracker,
        base_timestamp: datetime,
    ) -> None:
        """Test summary includes error breakdown by type."""
        # Add multiple error types
        tracker_with_session.record_error(
            PaperTradeError(
                timestamp=base_timestamp,
                intent={},
                error_type=PaperTradeErrorType.REVERT,
                error_message="Reverted",
            )
        )
        tracker_with_session.record_error(
            PaperTradeError(
                timestamp=base_timestamp,
                intent={},
                error_type=PaperTradeErrorType.REVERT,
                error_message="Reverted again",
            )
        )
        tracker_with_session.record_error(
            PaperTradeError(
                timestamp=base_timestamp,
                intent={},
                error_type=PaperTradeErrorType.RPC_ERROR,
                error_message="RPC failed",
            )
        )

        summary = tracker_with_session.get_summary()

        assert summary.error_summary == {
            "revert": 2,
            "rpc_error": 1,
        }

    def test_get_summary_with_pnl(
        self,
        tracker_with_session: PaperPortfolioTracker,
        sample_trade: PaperTrade,
    ) -> None:
        """Test get_summary_with_pnl includes PnL."""
        tracker_with_session.record_trade(sample_trade)
        prices = {"ETH": Decimal("3000"), "USDC": Decimal("1")}

        summary = tracker_with_session.get_summary_with_pnl(prices)

        assert summary.pnl_usd is not None
        # After trade: 9 ETH + 13000 USDC = 27000 + 13000 = 40000
        # Initial: 10 ETH + 10000 USDC = 30000 + 10000 = 40000
        # PnL = 0 (trade was at market price)
        assert summary.pnl_usd == Decimal("0")


# =============================================================================
# Balance Query Tests
# =============================================================================


class TestPaperPortfolioTrackerBalances:
    """Tests for balance query methods."""

    def test_get_token_balance(self, tracker_with_session: PaperPortfolioTracker) -> None:
        """Test getting specific token balance."""
        assert tracker_with_session.get_token_balance("ETH") == Decimal("10")
        assert tracker_with_session.get_token_balance("USDC") == Decimal("10000")

    def test_get_token_balance_unknown_token(self, tracker_with_session: PaperPortfolioTracker) -> None:
        """Test getting balance of unknown token returns 0."""
        assert tracker_with_session.get_token_balance("BTC") == Decimal("0")

    def test_get_all_balances(self, tracker_with_session: PaperPortfolioTracker) -> None:
        """Test getting all balances."""
        balances = tracker_with_session.get_all_balances()
        assert balances == {
            "ETH": Decimal("10"),
            "USDC": Decimal("10000"),
        }
        # Should be a copy, not the internal dict
        balances["ETH"] = Decimal("0")
        assert tracker_with_session.get_token_balance("ETH") == Decimal("10")

    def test_get_balance_change(
        self,
        tracker_with_session: PaperPortfolioTracker,
        sample_trade: PaperTrade,
    ) -> None:
        """Test getting balance change since session start."""
        tracker_with_session.record_trade(sample_trade)

        eth_change = tracker_with_session.get_balance_change("ETH")
        usdc_change = tracker_with_session.get_balance_change("USDC")

        assert eth_change == Decimal("-1")
        assert usdc_change == Decimal("3000")

    def test_get_balance_change_new_token(
        self,
        tracker_with_session: PaperPortfolioTracker,
        base_timestamp: datetime,
    ) -> None:
        """Test balance change for token not in initial balances."""
        trade = PaperTrade(
            timestamp=base_timestamp,
            block_number=12345678,
            intent={},
            tx_hash="0xabcd",
            gas_used=150000,
            gas_cost_usd=Decimal("0.50"),
            tokens_in={"ARB": Decimal("500")},
            tokens_out={"USDC": Decimal("750")},
        )
        tracker_with_session.record_trade(trade)

        arb_change = tracker_with_session.get_balance_change("ARB")
        assert arb_change == Decimal("500")


# =============================================================================
# Trade/Error Count Tests
# =============================================================================


class TestPaperPortfolioTrackerCounts:
    """Tests for trade and error count methods."""

    def test_get_trade_count(
        self,
        tracker_with_session: PaperPortfolioTracker,
        sample_trade: PaperTrade,
    ) -> None:
        """Test getting trade count."""
        assert tracker_with_session.get_trade_count() == 0

        tracker_with_session.record_trade(sample_trade)
        assert tracker_with_session.get_trade_count() == 1

    def test_get_error_count(
        self,
        tracker_with_session: PaperPortfolioTracker,
        sample_error: PaperTradeError,
    ) -> None:
        """Test getting error count."""
        assert tracker_with_session.get_error_count() == 0

        tracker_with_session.record_error(sample_error)
        assert tracker_with_session.get_error_count() == 1


# =============================================================================
# Serialization Tests
# =============================================================================


class TestPaperPortfolioTrackerSerialization:
    """Tests for serialization and deserialization."""

    def test_to_dict(
        self,
        tracker_with_session: PaperPortfolioTracker,
        sample_trade: PaperTrade,
        sample_error: PaperTradeError,
    ) -> None:
        """Test serialization to dict."""
        tracker_with_session.record_trade(sample_trade)
        tracker_with_session.record_error(sample_error)

        data = tracker_with_session.to_dict()

        assert data["strategy_id"] == "test_strategy"
        assert data["chain"] == "arbitrum"
        assert data["session_started"] is not None
        assert data["initial_balances"] == {"ETH": "10", "USDC": "10000"}
        assert data["current_balances"] == {"ETH": "9", "USDC": "13000"}
        assert data["total_gas_used"] == 150000
        assert data["total_gas_cost_usd"] == "0.50"
        assert data["trade_count"] == 1
        assert data["error_count"] == 1
        assert len(data["trades"]) == 1
        assert len(data["errors"]) == 1

    def test_from_dict(self, base_timestamp: datetime) -> None:
        """Test deserialization from dict."""
        data = {
            "strategy_id": "restored_strategy",
            "chain": "ethereum",
            "session_started": base_timestamp.isoformat(),
            "initial_balances": {"ETH": "5", "USDC": "5000"},
            "current_balances": {"ETH": "4", "USDC": "8000"},
            "total_gas_used": 100000,
            "total_gas_cost_usd": "0.30",
            "trades": [
                {
                    "timestamp": base_timestamp.isoformat(),
                    "block_number": 12345,
                    "intent": {"type": "SWAP"},
                    "tx_hash": "0x1234",
                    "gas_used": 100000,
                    "gas_cost_usd": "0.30",
                    "tokens_in": {"USDC": "3000"},
                    "tokens_out": {"ETH": "1"},
                }
            ],
            "errors": [
                {
                    "timestamp": base_timestamp.isoformat(),
                    "intent": {"type": "SWAP"},
                    "error_type": "revert",
                    "error_message": "Failed",
                }
            ],
        }

        tracker = PaperPortfolioTracker.from_dict(data)

        assert tracker.strategy_id == "restored_strategy"
        assert tracker.chain == "ethereum"
        assert tracker.session_started == base_timestamp
        assert tracker.initial_balances == {
            "ETH": Decimal("5"),
            "USDC": Decimal("5000"),
        }
        assert tracker.current_balances == {
            "ETH": Decimal("4"),
            "USDC": Decimal("8000"),
        }
        assert tracker.total_gas_used == 100000
        assert tracker.total_gas_cost_usd == Decimal("0.30")
        assert len(tracker.trades) == 1
        assert len(tracker.errors) == 1

    def test_round_trip_serialization(
        self,
        tracker_with_session: PaperPortfolioTracker,
        sample_trade: PaperTrade,
        sample_error: PaperTradeError,
    ) -> None:
        """Test that serialization round-trips correctly."""
        tracker_with_session.record_trade(sample_trade)
        tracker_with_session.record_error(sample_error)

        # Serialize
        data = tracker_with_session.to_dict()

        # Deserialize
        restored = PaperPortfolioTracker.from_dict(data)

        # Verify
        assert restored.strategy_id == tracker_with_session.strategy_id
        assert restored.chain == tracker_with_session.chain
        assert restored.initial_balances == tracker_with_session.initial_balances
        assert restored.current_balances == tracker_with_session.current_balances
        assert restored.total_gas_used == tracker_with_session.total_gas_used
        assert len(restored.trades) == len(tracker_with_session.trades)
        assert len(restored.errors) == len(tracker_with_session.errors)
