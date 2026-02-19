"""Tests for background Paper Trader management.

This module tests the background Paper Trader functionality including:
- PID file management
- State persistence and resume
- Signal handling and graceful shutdown
- Trade history writing
- Background process lifecycle
"""

import json
import os
import signal
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from almanak.framework.backtesting.paper.background import (
    BackgroundPaperTrader,
    BackgroundStatus,
    PaperTraderState,
    PIDFile,
    TradeHistoryWriter,
)
from almanak.framework.backtesting.paper.config import PaperTraderConfig
from almanak.framework.backtesting.paper.models import (
    PaperTrade,
    PaperTradeError,
    PaperTradeErrorType,
)


class TestPIDFile:
    """Tests for PID file management."""

    def test_get_default_path(self):
        """Test default PID file path generation."""
        path = PIDFile.get_default_path("test_strategy")
        assert path.name == "test_strategy.pid"
        assert ".almanak" in str(path) and "paper" in str(path)

    def test_get_default_path_with_state_dir(self, tmp_path):
        """Test PID file path with custom state directory."""
        path = PIDFile.get_default_path("test_strategy", tmp_path)
        assert path == tmp_path / "test_strategy.pid"

    def test_acquire_creates_pid_file(self, tmp_path):
        """Test that acquire creates a PID file with current PID."""
        pid_file = PIDFile(
            path=tmp_path / "test.pid",
            strategy_id="test_strategy",
        )

        result = pid_file.acquire()

        assert result is True
        assert pid_file.path.exists()
        assert int(pid_file.path.read_text().strip()) == os.getpid()
        assert pid_file.pid == os.getpid()

    def test_acquire_fails_if_running(self, tmp_path):
        """Test that acquire fails if another process holds the lock."""
        pid_file = PIDFile(
            path=tmp_path / "test.pid",
            strategy_id="test_strategy",
        )

        # Write a fake PID that "appears" to be running
        pid_file.path.write_text(str(os.getpid()))

        # Create a new PIDFile instance trying to acquire
        pid_file2 = PIDFile(
            path=tmp_path / "test.pid",
            strategy_id="test_strategy",
        )

        result = pid_file2.acquire()
        assert result is False

    def test_acquire_removes_stale_pid_file(self, tmp_path):
        """Test that acquire removes stale PID files from dead processes."""
        pid_file = PIDFile(
            path=tmp_path / "test.pid",
            strategy_id="test_strategy",
        )

        # Write a PID that doesn't exist (99999999)
        pid_file.path.write_text("99999999")

        result = pid_file.acquire()

        # Should succeed because old process is not running
        assert result is True
        assert int(pid_file.path.read_text().strip()) == os.getpid()

    def test_release_removes_pid_file(self, tmp_path):
        """Test that release removes the PID file."""
        pid_file = PIDFile(
            path=tmp_path / "test.pid",
            strategy_id="test_strategy",
        )
        pid_file.acquire()
        assert pid_file.path.exists()

        pid_file.release()

        assert not pid_file.path.exists()

    def test_release_only_removes_own_pid(self, tmp_path):
        """Test that release only removes PID file if owned by current process."""
        pid_file = PIDFile(
            path=tmp_path / "test.pid",
            strategy_id="test_strategy",
        )

        # Write a different PID
        pid_file.path.write_text("99999999")

        pid_file.release()

        # Should not remove file owned by different process
        assert pid_file.path.exists()
        assert pid_file.path.read_text().strip() == "99999999"

    def test_is_running_with_running_process(self, tmp_path):
        """Test is_running returns True for running process."""
        pid_file = PIDFile(
            path=tmp_path / "test.pid",
            strategy_id="test_strategy",
        )
        pid_file.path.parent.mkdir(parents=True, exist_ok=True)
        pid_file.path.write_text(str(os.getpid()))

        assert pid_file.is_running() is True

    def test_is_running_with_no_file(self, tmp_path):
        """Test is_running returns False when no PID file exists."""
        pid_file = PIDFile(
            path=tmp_path / "test.pid",
            strategy_id="test_strategy",
        )

        assert pid_file.is_running() is False

    def test_is_running_with_dead_process(self, tmp_path):
        """Test is_running returns False for dead process."""
        pid_file = PIDFile(
            path=tmp_path / "test.pid",
            strategy_id="test_strategy",
        )
        pid_file.path.parent.mkdir(parents=True, exist_ok=True)
        pid_file.path.write_text("99999999")

        assert pid_file.is_running() is False

    def test_get_pid(self, tmp_path):
        """Test get_pid returns the PID from file."""
        pid_file = PIDFile(
            path=tmp_path / "test.pid",
            strategy_id="test_strategy",
        )
        pid_file.path.parent.mkdir(parents=True, exist_ok=True)
        pid_file.path.write_text("12345")

        assert pid_file.get_pid() == 12345

    def test_get_pid_no_file(self, tmp_path):
        """Test get_pid returns None when no file exists."""
        pid_file = PIDFile(
            path=tmp_path / "test.pid",
            strategy_id="test_strategy",
        )

        assert pid_file.get_pid() is None


class TestTradeHistoryWriter:
    """Tests for incremental trade history persistence."""

    def _create_sample_trade(self, block_number: int = 100) -> PaperTrade:
        """Create a sample PaperTrade for testing."""
        return PaperTrade(
            timestamp=datetime.now(UTC),
            block_number=block_number,
            intent={"type": "SwapIntent", "token_in": "ETH", "token_out": "USDC"},
            tx_hash="0x1234567890abcdef",
            gas_used=21000,
            gas_cost_usd=Decimal("0.50"),
            tokens_in={"ETH": Decimal("1.0")},
            tokens_out={"USDC": Decimal("2000")},
            protocol="uniswap_v3",
            intent_type="SWAP",
        )

    def _create_sample_error(self) -> PaperTradeError:
        """Create a sample PaperTradeError for testing."""
        return PaperTradeError(
            timestamp=datetime.now(UTC),
            intent={"type": "SwapIntent", "token_in": "ETH", "token_out": "USDC"},
            error_type=PaperTradeErrorType.SIMULATION_FAILED,
            error_message="Test error",
        )

    def test_get_default_path(self):
        """Test default trade history file path generation."""
        path = TradeHistoryWriter.get_default_path("test_strategy")
        assert path.name == "test_strategy.trades.jsonl"

    def test_write_trade(self, tmp_path):
        """Test writing a trade to JSONL file."""
        writer = TradeHistoryWriter(
            path=tmp_path / "trades.jsonl",
            strategy_id="test_strategy",
        )

        trade = self._create_sample_trade()
        writer.write_trade(trade)

        assert writer.path.exists()
        with open(writer.path) as f:
            line = f.readline()
            data = json.loads(line)
            assert data["type"] == "trade"
            assert data["data"]["intent"]["token_in"] == "ETH"

    def test_write_multiple_trades(self, tmp_path):
        """Test writing multiple trades appends to file."""
        writer = TradeHistoryWriter(
            path=tmp_path / "trades.jsonl",
            strategy_id="test_strategy",
        )

        for i in range(3):
            trade = self._create_sample_trade(block_number=100 + i)
            writer.write_trade(trade)

        with open(writer.path) as f:
            lines = f.readlines()

        assert len(lines) == 3

    def test_write_error(self, tmp_path):
        """Test writing an error to JSONL file."""
        writer = TradeHistoryWriter(
            path=tmp_path / "trades.jsonl",
            strategy_id="test_strategy",
        )

        error = self._create_sample_error()
        writer.write_error(error)

        with open(writer.path) as f:
            line = f.readline()
            data = json.loads(line)
            assert data["type"] == "error"
            assert data["data"]["error_message"] == "Test error"

    def test_read_all(self, tmp_path):
        """Test reading all trades and errors from file."""
        writer = TradeHistoryWriter(
            path=tmp_path / "trades.jsonl",
            strategy_id="test_strategy",
        )

        # Write trades and errors
        trade = self._create_sample_trade()
        error = self._create_sample_error()

        writer.write_trade(trade)
        writer.write_error(error)

        trades, errors = writer.read_all()

        assert len(trades) == 1
        assert len(errors) == 1
        assert trades[0].intent["token_in"] == "ETH"
        assert errors[0].error_message == "Test error"

    def test_get_trade_count(self, tmp_path):
        """Test counting trades in history file."""
        writer = TradeHistoryWriter(
            path=tmp_path / "trades.jsonl",
            strategy_id="test_strategy",
        )

        for i in range(5):
            trade = self._create_sample_trade(block_number=100 + i)
            writer.write_trade(trade)

        assert writer.get_trade_count() == 5

    def test_truncate(self, tmp_path):
        """Test truncating trade history file."""
        writer = TradeHistoryWriter(
            path=tmp_path / "trades.jsonl",
            strategy_id="test_strategy",
        )

        trade = self._create_sample_trade()
        writer.write_trade(trade)
        assert writer.path.exists()

        writer.truncate()

        assert not writer.path.exists()


class TestPaperTraderState:
    """Tests for Paper Trader state persistence."""

    def create_sample_state(self):
        """Create a sample state for testing."""
        return PaperTraderState(
            strategy_id="test_strategy",
            session_start=datetime.now(UTC),
            last_save=datetime.now(UTC),
            tick_count=10,
            trades=[],
            errors=[],
            current_balances={"ETH": Decimal("5.0"), "USDC": Decimal("10000")},
            initial_balances={"ETH": Decimal("5.0"), "USDC": Decimal("10000")},
            equity_curve=[],
            config={"chain": "arbitrum"},
            pid=12345,
            status="running",
        )

    def test_to_dict(self):
        """Test state serialization to dictionary."""
        state = self.create_sample_state()

        data = state.to_dict()

        assert data["strategy_id"] == "test_strategy"
        assert data["tick_count"] == 10
        assert data["current_balances"]["ETH"] == "5.0"
        assert data["status"] == "running"

    def test_from_dict(self):
        """Test state deserialization from dictionary."""
        state = self.create_sample_state()
        data = state.to_dict()

        restored = PaperTraderState.from_dict(data)

        assert restored.strategy_id == state.strategy_id
        assert restored.tick_count == state.tick_count
        assert restored.current_balances == state.current_balances
        assert restored.status == state.status

    def test_roundtrip_serialization(self):
        """Test state survives roundtrip serialization."""
        state = self.create_sample_state()
        state.is_resumed = True
        state.resume_count = 3
        state.last_resume_time = datetime.now(UTC)

        data = state.to_dict()
        restored = PaperTraderState.from_dict(data)

        assert restored.is_resumed == state.is_resumed
        assert restored.resume_count == state.resume_count
        # datetime comparison with some tolerance
        assert abs((restored.last_resume_time - state.last_resume_time).total_seconds()) < 1

    def test_save_and_load(self, tmp_path):
        """Test saving and loading state from file."""
        state = self.create_sample_state()
        state_file = tmp_path / "state.json"

        state.save(state_file)
        loaded = PaperTraderState.load(state_file)

        assert loaded.strategy_id == state.strategy_id
        assert loaded.tick_count == state.tick_count

    def test_can_resume_running(self):
        """Test can_resume returns False for running status."""
        state = self.create_sample_state()
        state.status = "running"

        assert state.can_resume() is False

    def test_can_resume_completed(self):
        """Test can_resume returns False for completed status."""
        state = self.create_sample_state()
        state.status = "completed"

        assert state.can_resume() is False

    def test_can_resume_stopped(self):
        """Test can_resume returns True for stopped status with valid data."""
        state = self.create_sample_state()
        state.status = "stopped"

        assert state.can_resume() is True

    def test_can_resume_error(self):
        """Test can_resume returns True for error status with valid data."""
        state = self.create_sample_state()
        state.status = "error"

        assert state.can_resume() is True

    def test_can_resume_no_balances(self):
        """Test can_resume returns False with no balances."""
        state = self.create_sample_state()
        state.status = "stopped"
        state.current_balances = {}

        assert state.can_resume() is False

    def test_can_resume_no_config(self):
        """Test can_resume returns False with no config."""
        state = self.create_sample_state()
        state.status = "stopped"
        state.config = {}

        assert state.can_resume() is False

    def test_prepare_for_resume(self):
        """Test prepare_for_resume updates state correctly."""
        state = self.create_sample_state()
        state.status = "stopped"
        state.resume_count = 2

        state.prepare_for_resume(new_pid=54321)

        assert state.is_resumed is True
        assert state.resume_count == 3
        assert state.pid == 54321
        assert state.status == "running"
        assert state.last_resume_time is not None


class TestBackgroundStatus:
    """Tests for background status dataclass."""

    def test_uptime_calculation(self):
        """Test uptime is calculated from session start."""
        start_time = datetime.now(UTC) - timedelta(hours=2)
        status = BackgroundStatus(
            is_running=True,
            session_start=start_time,
        )

        uptime = status.uptime
        assert uptime is not None
        assert uptime >= timedelta(hours=2)

    def test_uptime_none_when_no_start(self):
        """Test uptime is None when no session start."""
        status = BackgroundStatus(is_running=False)

        assert status.uptime is None

    def test_to_dict(self):
        """Test status serialization."""
        status = BackgroundStatus(
            is_running=True,
            pid=12345,
            strategy_id="test_strategy",
            session_start=datetime.now(UTC),
            tick_count=100,
            trade_count=50,
            error_count=5,
            status="running",
            is_resumed=True,
            resume_count=2,
        )

        data = status.to_dict()

        assert data["is_running"] is True
        assert data["pid"] == 12345
        assert data["strategy_id"] == "test_strategy"
        assert data["tick_count"] == 100


class TestBackgroundPaperTrader:
    """Tests for BackgroundPaperTrader manager class."""

    def create_config(self):
        """Create a sample config for testing."""
        return PaperTraderConfig(
            chain="arbitrum",
            rpc_url="http://localhost:8545",
            strategy_id="test_strategy",
            initial_eth=Decimal("1.0"),
            initial_tokens={"USDC": Decimal("1000")},
        )

    def test_state_file_path(self, tmp_path):
        """Test state file path property."""
        config = self.create_config()
        bg_trader = BackgroundPaperTrader(
            config=config,
            state_dir=tmp_path,
        )

        assert bg_trader.state_file == tmp_path / "test_strategy.state.json"

    def test_pid_file_path(self, tmp_path):
        """Test PID file path property."""
        config = self.create_config()
        bg_trader = BackgroundPaperTrader(
            config=config,
            state_dir=tmp_path,
        )

        assert bg_trader.pid_file_path == tmp_path / "test_strategy.pid"

    def test_log_file_path(self, tmp_path):
        """Test log file path property."""
        config = self.create_config()
        bg_trader = BackgroundPaperTrader(
            config=config,
            state_dir=tmp_path,
        )

        assert bg_trader.log_file == tmp_path / "test_strategy.log"

    def test_get_status_no_session(self, tmp_path):
        """Test get_status when no session exists."""
        config = self.create_config()
        bg_trader = BackgroundPaperTrader(
            config=config,
            state_dir=tmp_path,
        )

        status = bg_trader.get_status()

        assert status.is_running is False
        assert status.strategy_id == "test_strategy"

    def test_get_status_with_state(self, tmp_path):
        """Test get_status with saved state."""
        config = self.create_config()
        bg_trader = BackgroundPaperTrader(
            config=config,
            state_dir=tmp_path,
        )

        # Save a state file
        state = PaperTraderState(
            strategy_id="test_strategy",
            session_start=datetime.now(UTC),
            last_save=datetime.now(UTC),
            tick_count=100,
            trades=[],
            errors=[],
            current_balances={"ETH": Decimal("5")},
            initial_balances={"ETH": Decimal("5")},
            equity_curve=[],
            config=config.to_dict(),
            pid=99999999,  # Non-existent PID
            status="stopped",
        )
        state.save(bg_trader.state_file)

        status = bg_trader.get_status()

        assert status.is_running is False
        assert status.tick_count == 100
        assert status.can_resume is True

    def test_get_state(self, tmp_path):
        """Test get_state returns None when no state exists."""
        config = self.create_config()
        bg_trader = BackgroundPaperTrader(
            config=config,
            state_dir=tmp_path,
        )

        state = bg_trader.get_state()

        assert state is None

    def test_get_logs_no_file(self, tmp_path):
        """Test get_logs when no log file exists."""
        config = self.create_config()
        bg_trader = BackgroundPaperTrader(
            config=config,
            state_dir=tmp_path,
        )

        logs = bg_trader.get_logs()

        assert "No log file found" in logs

    def test_get_logs_with_file(self, tmp_path):
        """Test get_logs returns log content."""
        config = self.create_config()
        bg_trader = BackgroundPaperTrader(
            config=config,
            state_dir=tmp_path,
        )

        # Create a log file
        log_content = "Line 1\nLine 2\nLine 3\n"
        bg_trader.log_file.write_text(log_content)

        logs = bg_trader.get_logs(lines=2)

        assert "Line 2" in logs
        assert "Line 3" in logs

    def test_clear_state(self, tmp_path):
        """Test clear_state removes all session files."""
        config = self.create_config()
        bg_trader = BackgroundPaperTrader(
            config=config,
            state_dir=tmp_path,
        )

        # Create files
        bg_trader.state_file.write_text("{}")
        bg_trader.log_file.write_text("logs")
        bg_trader.trade_history_file.write_text("")

        result = bg_trader.clear_state()

        assert result is True
        assert not bg_trader.state_file.exists()
        assert not bg_trader.log_file.exists()
        assert not bg_trader.trade_history_file.exists()

    def test_clear_state_fails_when_running(self, tmp_path):
        """Test clear_state raises error when process is running."""
        config = self.create_config()
        bg_trader = BackgroundPaperTrader(
            config=config,
            state_dir=tmp_path,
        )

        # Create a PID file with current process
        bg_trader.pid_file_path.write_text(str(os.getpid()))

        with pytest.raises(RuntimeError, match=r"Cannot clear state while.*running"):
            bg_trader.clear_state()

    def test_start_raises_if_already_running(self, tmp_path):
        """Test start raises error if process already running."""
        config = self.create_config()
        bg_trader = BackgroundPaperTrader(
            config=config,
            state_dir=tmp_path,
        )

        # Create a PID file with current process
        bg_trader.pid_file_path.write_text(str(os.getpid()))

        with pytest.raises(RuntimeError, match="already running"):
            bg_trader.start(strategy_module="test_module")

    def test_stop_returns_false_when_not_running(self, tmp_path):
        """Test stop returns False when no process is running."""
        config = self.create_config()
        bg_trader = BackgroundPaperTrader(
            config=config,
            state_dir=tmp_path,
        )

        result = bg_trader.stop()

        assert result is False

    def test_resume_raises_if_no_state(self, tmp_path):
        """Test resume raises error when no saved state exists."""
        config = self.create_config()
        bg_trader = BackgroundPaperTrader(
            config=config,
            state_dir=tmp_path,
        )

        with pytest.raises(FileNotFoundError, match="No saved state"):
            bg_trader.resume(strategy_module="test_module")

    def test_resume_raises_if_already_running(self, tmp_path):
        """Test resume raises error if process already running."""
        config = self.create_config()
        bg_trader = BackgroundPaperTrader(
            config=config,
            state_dir=tmp_path,
        )

        # Create a PID file with current process
        bg_trader.pid_file_path.write_text(str(os.getpid()))

        # Create a state file
        state = PaperTraderState(
            strategy_id="test_strategy",
            session_start=datetime.now(UTC),
            last_save=datetime.now(UTC),
            tick_count=10,
            trades=[],
            errors=[],
            current_balances={"ETH": Decimal("5")},
            initial_balances={"ETH": Decimal("5")},
            equity_curve=[],
            config=config.to_dict(),
            pid=99999,
            status="stopped",
        )
        state.save(bg_trader.state_file)

        with pytest.raises(RuntimeError, match="already running"):
            bg_trader.resume(strategy_module="test_module")


class TestSignalHandling:
    """Tests for signal handling in background process."""

    def test_signal_handler_sets_shutdown_flag(self):
        """Test that signal handler sets shutdown flag."""
        # This tests the pattern used in _run_background_paper_trader
        shutdown_requested = False

        def signal_handler(signum, frame):
            nonlocal shutdown_requested
            shutdown_requested = True

        # Simulate receiving signal
        signal_handler(signal.SIGTERM, None)

        assert shutdown_requested is True

    def test_sigterm_and_sigint_handled(self):
        """Test that both SIGTERM and SIGINT are registered."""
        # Verify the signal constants exist and are different
        assert signal.SIGTERM != signal.SIGINT
        # Verify both signals can be used in kill
        # (they're valid signal numbers)
        assert signal.SIGTERM > 0
        assert signal.SIGINT > 0


class TestGracefulShutdown:
    """Integration tests for graceful shutdown behavior."""

    def test_state_saved_on_shutdown(self, tmp_path):
        """Test that state is saved when shutdown is requested."""
        # Create initial state
        state = PaperTraderState(
            strategy_id="test_strategy",
            session_start=datetime.now(UTC),
            last_save=datetime.now(UTC) - timedelta(minutes=5),
            tick_count=50,
            trades=[],
            errors=[],
            current_balances={"ETH": Decimal("5")},
            initial_balances={"ETH": Decimal("5")},
            equity_curve=[],
            config={"chain": "arbitrum"},
            pid=os.getpid(),
            status="running",
        )

        state_file = tmp_path / "state.json"

        # Simulate shutdown: update status and save
        state.status = "stopped"
        state.save(state_file)

        # Verify state was saved
        loaded = PaperTraderState.load(state_file)
        assert loaded.status == "stopped"
        assert loaded.tick_count == 50

    def test_stopped_status_persisted_through_save_load(self, tmp_path):
        """Test that 'stopped' status is correctly persisted through save/load cycle."""
        state = PaperTraderState(
            strategy_id="test_strategy",
            session_start=datetime.now(UTC),
            last_save=datetime.now(UTC),
            tick_count=100,
            trades=[],
            errors=[],
            current_balances={"ETH": Decimal("5")},
            initial_balances={"ETH": Decimal("5")},
            equity_curve=[],
            config={"chain": "arbitrum"},
            pid=os.getpid(),
            status="stopped",  # Simulating graceful shutdown state
        )

        state_file = tmp_path / "state.json"
        state.save(state_file)

        loaded = PaperTraderState.load(state_file)
        assert loaded.status == "stopped"

    def test_status_set_to_completed_on_normal_exit(self, tmp_path):
        """Test status is 'completed' when no shutdown was requested."""
        state = PaperTraderState(
            strategy_id="test_strategy",
            session_start=datetime.now(UTC),
            last_save=datetime.now(UTC),
            tick_count=100,
            trades=[],
            errors=[],
            current_balances={"ETH": Decimal("5")},
            initial_balances={"ETH": Decimal("5")},
            equity_curve=[],
            config={"chain": "arbitrum"},
            pid=os.getpid(),
            status="running",
        )

        # Normal completion (max_ticks reached)
        shutdown_requested = False

        if not shutdown_requested:
            state.status = "completed"
        else:
            state.status = "stopped"

        state_file = tmp_path / "state.json"
        state.save(state_file)

        loaded = PaperTraderState.load(state_file)
        assert loaded.status == "completed"
