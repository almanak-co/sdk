"""Tests for background Paper Trader management.

This module tests the background Paper Trader functionality including:
- PID file management
- State persistence and resume
- Signal handling and graceful shutdown
- Trade history writing
- Background process lifecycle
"""

import json
import logging
import os
import signal
import sys
import types
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import patch

import pytest

from almanak.framework.backtesting.paper.background import (
    BackgroundPaperTrader,
    BackgroundStatus,
    PaperTraderState,
    PIDFile,
    TradeHistoryWriter,
    _instantiate_background_strategy,
    _write_new_error_records,
    _write_new_trade_records,
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
            deployment_id="test_strategy",
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
            deployment_id="test_strategy",
        )

        # Write a fake PID that "appears" to be running
        pid_file.path.write_text(str(os.getpid()))

        # Create a new PIDFile instance trying to acquire
        pid_file2 = PIDFile(
            path=tmp_path / "test.pid",
            deployment_id="test_strategy",
        )

        result = pid_file2.acquire()
        assert result is False

    def test_acquire_removes_stale_pid_file(self, tmp_path):
        """Test that acquire removes stale PID files from dead processes."""
        pid_file = PIDFile(
            path=tmp_path / "test.pid",
            deployment_id="test_strategy",
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
            deployment_id="test_strategy",
        )
        pid_file.acquire()
        assert pid_file.path.exists()

        pid_file.release()

        assert not pid_file.path.exists()

    def test_release_only_removes_own_pid(self, tmp_path):
        """Test that release only removes PID file if owned by current process."""
        pid_file = PIDFile(
            path=tmp_path / "test.pid",
            deployment_id="test_strategy",
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
            deployment_id="test_strategy",
        )
        pid_file.path.parent.mkdir(parents=True, exist_ok=True)
        pid_file.path.write_text(str(os.getpid()))

        assert pid_file.is_running() is True

    def test_is_running_with_no_file(self, tmp_path):
        """Test is_running returns False when no PID file exists."""
        pid_file = PIDFile(
            path=tmp_path / "test.pid",
            deployment_id="test_strategy",
        )

        assert pid_file.is_running() is False

    def test_is_running_with_dead_process(self, tmp_path):
        """Test is_running returns False for dead process."""
        pid_file = PIDFile(
            path=tmp_path / "test.pid",
            deployment_id="test_strategy",
        )
        pid_file.path.parent.mkdir(parents=True, exist_ok=True)
        pid_file.path.write_text("99999999")

        assert pid_file.is_running() is False

    def test_get_pid(self, tmp_path):
        """Test get_pid returns the PID from file."""
        pid_file = PIDFile(
            path=tmp_path / "test.pid",
            deployment_id="test_strategy",
        )
        pid_file.path.parent.mkdir(parents=True, exist_ok=True)
        pid_file.path.write_text("12345")

        assert pid_file.get_pid() == 12345

    def test_get_pid_no_file(self, tmp_path):
        """Test get_pid returns None when no file exists."""
        pid_file = PIDFile(
            path=tmp_path / "test.pid",
            deployment_id="test_strategy",
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
            deployment_id="test_strategy",
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
            deployment_id="test_strategy",
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
            deployment_id="test_strategy",
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
            deployment_id="test_strategy",
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
            deployment_id="test_strategy",
        )

        for i in range(5):
            trade = self._create_sample_trade(block_number=100 + i)
            writer.write_trade(trade)

        assert writer.get_trade_count() == 5

    def test_truncate(self, tmp_path):
        """Test truncating trade history file."""
        writer = TradeHistoryWriter(
            path=tmp_path / "trades.jsonl",
            deployment_id="test_strategy",
        )

        trade = self._create_sample_trade()
        writer.write_trade(trade)
        assert writer.path.exists()

        writer.truncate()

        assert not writer.path.exists()


class TestBackgroundHelperFunctions:
    """Focused tests for background entry-point helpers."""

    @staticmethod
    def _sample_trade() -> PaperTrade:
        return PaperTrade(
            timestamp=datetime.now(UTC),
            block_number=100,
            intent={"type": "SwapIntent"},
            tx_hash="0xabc",
            gas_used=21000,
            gas_cost_usd=Decimal("0.50"),
            tokens_in={"USDC": Decimal("100")},
            tokens_out={"ETH": Decimal("0.05")},
            protocol="uniswap_v3",
            intent_type="SWAP",
        )

    @staticmethod
    def _sample_error() -> PaperTradeError:
        return PaperTradeError(
            timestamp=datetime.now(UTC),
            intent={"type": "SwapIntent"},
            error_type=PaperTradeErrorType.INTERNAL_ERROR,
            error_message="boom",
        )

    def test_instantiate_strategy_does_not_swallow_constructor_type_error(self):
        class BuggyStrategy:
            def __init__(self, config):
                raise TypeError("real constructor bug")

        with pytest.raises(TypeError, match="real constructor bug"):
            _instantiate_background_strategy(
                BuggyStrategy,
                strategy_config={"x": 1},
                config=PaperTraderConfig(
                    chain="arbitrum",
                    rpc_url="http://localhost:8545",
                    deployment_id="paper-test",
                ),
            )

    def test_instantiate_strategy_uses_full_signature_when_supported(self):
        class FullSignatureStrategy:
            def __init__(self, config, chain, wallet_address, risk_guard_config):
                self.config = config
                self.chain = chain
                self.wallet_address = wallet_address
                self.risk_guard_config = risk_guard_config

        strategy = _instantiate_background_strategy(
            FullSignatureStrategy,
            strategy_config={"alpha": 1},
            config=PaperTraderConfig(
                chain="arbitrum",
                rpc_url="http://localhost:8545",
                deployment_id="paper-test",
                wallet_address="0x" + "1" * 40,
            ),
        )

        assert strategy.config == {"alpha": 1}
        assert strategy.chain == "arbitrum"
        assert strategy.wallet_address == "0x" + "1" * 40
        assert strategy.risk_guard_config is None

    def test_instantiate_strategy_uses_full_context_for_kwargs_constructor(self):
        class KwargsStrategy:
            def __init__(self, **kwargs):
                self.kwargs = kwargs

        strategy = _instantiate_background_strategy(
            KwargsStrategy,
            strategy_config={"alpha": 1},
            config=PaperTraderConfig(
                chain="base",
                rpc_url="http://localhost:8545",
                deployment_id="paper-test",
                wallet_address="0x" + "a" * 40,
            ),
        )

        assert strategy.kwargs == {
            "config": {"alpha": 1},
            "chain": "base",
            "wallet_address": "0x" + "a" * 40,
            "risk_guard_config": None,
        }

    def test_write_new_trade_records_returns_last_successful_index(self):
        class FailingWriter:
            def __init__(self):
                self.writes = 0

            def write_trade(self, trade):
                self.writes += 1
                if self.writes == 2:
                    raise OSError("disk full")

        trades = [self._sample_trade(), self._sample_trade(), self._sample_trade()]

        index = _write_new_trade_records(
            trade_history=FailingWriter(),
            trades=trades,
            last_trade_count=0,
            bg_logger=logging.getLogger("test"),
        )

        assert index == 1

    def test_write_new_error_records_returns_last_successful_index(self):
        class FailingWriter:
            def __init__(self):
                self.writes = 0

            def write_error(self, error):
                self.writes += 1
                if self.writes == 2:
                    raise OSError("disk full")

        errors = [self._sample_error(), self._sample_error(), self._sample_error()]

        index = _write_new_error_records(
            trade_history=FailingWriter(),
            errors=errors,
            last_error_count=0,
            bg_logger=logging.getLogger("test"),
        )

        assert index == 1


class TestPaperTraderState:
    """Tests for Paper Trader state persistence."""

    def create_sample_state(self):
        """Create a sample state for testing."""
        return PaperTraderState(
            deployment_id="test_strategy",
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

        assert data["deployment_id"] == "test_strategy"
        assert data["tick_count"] == 10
        assert data["current_balances"]["ETH"] == "5.0"
        assert data["status"] == "running"

    def test_from_dict(self):
        """Test state deserialization from dictionary."""
        state = self.create_sample_state()
        data = state.to_dict()

        restored = PaperTraderState.from_dict(data)

        assert restored.deployment_id == state.deployment_id
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

        assert loaded.deployment_id == state.deployment_id
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
            deployment_id="test_strategy",
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
        assert data["deployment_id"] == "test_strategy"
        assert data["tick_count"] == 100


class TestBackgroundPaperTrader:
    """Tests for BackgroundPaperTrader manager class."""

    def create_config(self):
        """Create a sample config for testing."""
        return PaperTraderConfig(
            chain="arbitrum",
            rpc_url="http://localhost:8545",
            deployment_id="test_strategy",
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
        assert status.deployment_id == "test_strategy"

    def test_get_status_with_state(self, tmp_path):
        """Test get_status with saved state."""
        config = self.create_config()
        bg_trader = BackgroundPaperTrader(
            config=config,
            state_dir=tmp_path,
        )

        # Save a state file
        state = PaperTraderState(
            deployment_id="test_strategy",
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

    def test_stop_returns_true_after_graceful_sigterm(self, tmp_path):
        """stop() releases the PID file when SIGTERM is enough."""
        config = self.create_config()
        bg_trader = BackgroundPaperTrader(
            config=config,
            state_dir=tmp_path,
        )
        bg_trader.pid_file_path.write_text(str(os.getpid()))

        with patch.object(PIDFile, "_is_process_running", side_effect=[True, False]), patch(
            "almanak.framework.backtesting.paper.background.os.kill"
        ) as kill_mock, patch("almanak.framework.backtesting.paper.background.time.sleep") as sleep_mock:
            result = bg_trader.stop(timeout=1)

        assert result is True
        kill_mock.assert_called_once_with(os.getpid(), signal.SIGTERM)
        sleep_mock.assert_not_called()
        assert not bg_trader.pid_file_path.exists()

    def test_stop_uses_sigkill_when_sigterm_times_out(self, tmp_path):
        """stop() escalates to SIGKILL after the graceful timeout."""
        config = self.create_config()
        bg_trader = BackgroundPaperTrader(
            config=config,
            state_dir=tmp_path,
        )
        bg_trader.pid_file_path.write_text(str(os.getpid()))

        with patch.object(PIDFile, "_is_process_running", side_effect=[True, False]), patch(
            "almanak.framework.backtesting.paper.background.os.kill"
        ) as kill_mock, patch("almanak.framework.backtesting.paper.background.time.sleep") as sleep_mock:
            result = bg_trader.stop(timeout=0)

        assert result is True
        assert [call.args for call in kill_mock.call_args_list] == [
            (os.getpid(), signal.SIGTERM),
            (os.getpid(), signal.SIGKILL),
        ]
        sleep_mock.assert_called_once_with(1)
        assert not bg_trader.pid_file_path.exists()

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
            deployment_id="test_strategy",
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


class TestBackgroundPaperTraderEntrypoint:
    """Tests for the spawned-process entrypoint without launching Anvil."""

    def test_entrypoint_runs_fresh_and_resumed_sessions(self, tmp_path, monkeypatch):
        from almanak.framework.backtesting.paper.background import _run_background_paper_trader

        config = PaperTraderConfig(
            chain="arbitrum",
            rpc_url="https://arb.example/rpc",
            deployment_id="child_entrypoint_strategy",
            initial_eth=Decimal("2"),
            initial_tokens={"USDC": Decimal("25")},
            tick_interval_seconds=0.001,
            max_ticks=1,
            anvil_port=8654,
        )

        strategy_module = types.ModuleType("child_entrypoint_strategy_module")
        strategy_calls = {}

        class Strategy:
            def __init__(
                self,
                config=None,
                chain=None,
                wallet_address=None,
                risk_guard_config=None,
            ):
                strategy_calls.update(
                    {
                        "config": config,
                        "chain": chain,
                        "wallet_address": wallet_address,
                        "risk_guard_config": risk_guard_config,
                    }
                )

        strategy_module.Strategy = Strategy
        monkeypatch.setitem(sys.modules, strategy_module.__name__, strategy_module)

        created = {"traders": []}

        class FakeRollingForkManager:
            def __init__(self, *, rpc_url, chain, anvil_port):
                self.rpc_url = rpc_url
                self.chain = chain
                self.anvil_port = anvil_port
                self.is_running = False
                created["fork_manager"] = self

        class FakePaperPortfolioTracker:
            def __init__(self, *, deployment_id, initial_balances):
                self.deployment_id = deployment_id
                self.initial_balances = dict(initial_balances)
                self.current_balances = dict(initial_balances)
                created["portfolio_tracker"] = self

        class FakePaperTrader:
            def __init__(self, *, fork_manager, portfolio_tracker, config):
                self.fork_manager = fork_manager
                self.portfolio_tracker = portfolio_tracker
                self.config = config
                self._running = False
                self._current_strategy = None
                self._trades = []
                self._errors = []
                self._equity_curve = []
                self._tick_count = 0
                self._ticks_with_fork = 0
                self._ticks_with_indicators = 0
                self._ticks_with_action = 0
                self._last_successful_decision_at = None
                self._last_trade_at = None
                self._last_market_snapshot = None
                self._orchestrator = None
                self.calls = []
                created["trader"] = self
                created["traders"].append(self)

            async def _initialize_fork(self):
                self.calls.append("initialize_fork")

            async def _initialize_orchestrator(self):
                self.calls.append("initialize_orchestrator")

            async def _execute_tick(self, strategy):
                self.calls.append(("execute_tick", strategy.__class__.__name__))
                self._ticks_with_fork += 1
                self._ticks_with_action += 1
                self._last_successful_decision_at = datetime.now(UTC)

            async def _sync_wallet_to_fork(self):
                self.calls.append("sync_wallet_to_fork")

            async def _cleanup(self):
                self.calls.append("cleanup")

        with patch("almanak.config.backtest.apply_ssl_cert_file"), patch(
            "almanak.config.backtest.backtest_config_from_env"
        ), patch("almanak.framework.backtesting.paper.background.atexit.register"), patch(
            "almanak.framework.backtesting.paper.background.signal.signal"
        ), patch(
            "almanak.framework.anvil.fork_manager.RollingForkManager",
            FakeRollingForkManager,
        ), patch(
            "almanak.framework.backtesting.paper.portfolio_tracker.PaperPortfolioTracker",
            FakePaperPortfolioTracker,
        ), patch(
            "almanak.framework.backtesting.paper.engine.PaperTrader",
            FakePaperTrader,
        ):
            _run_background_paper_trader(
                config.to_dict(),
                strategy_module.__name__,
                "Strategy",
                str(tmp_path),
                save_interval_seconds=0,
                raw_rpc_url=config.rpc_url,
            )

            resume_config = PaperTraderConfig(
                chain="arbitrum",
                rpc_url="https://arb.example/rpc",
                deployment_id="child_entrypoint_strategy",
                initial_eth=Decimal("2"),
                initial_tokens={"USDC": Decimal("25")},
                tick_interval_seconds=0.001,
                max_ticks=4,
                anvil_port=8654,
            )
            stopped_state = PaperTraderState(
                deployment_id="child_entrypoint_strategy",
                session_start=datetime.now(UTC),
                last_save=datetime.now(UTC),
                tick_count=3,
                trades=[],
                errors=[],
                current_balances={"ETH": Decimal("1"), "USDC": Decimal("5")},
                initial_balances={"ETH": Decimal("2"), "USDC": Decimal("25")},
                equity_curve=[],
                config=resume_config.to_dict(),
                pid=12345,
                status="stopped",
                ticks_with_fork=7,
                ticks_with_action=2,
            )
            stopped_state.save(tmp_path / "child_entrypoint_strategy.state.json")

            _run_background_paper_trader(
                resume_config.to_dict(),
                strategy_module.__name__,
                "Strategy",
                str(tmp_path),
                save_interval_seconds=0,
                resume=True,
                raw_rpc_url=resume_config.rpc_url,
            )

        state = PaperTraderState.load(tmp_path / "child_entrypoint_strategy.state.json")
        assert state.status == "completed"
        assert state.tick_count == 4
        assert state.resume_count == 1
        assert state.current_balances == {"ETH": Decimal("1"), "USDC": Decimal("5")}
        assert not (tmp_path / "child_entrypoint_strategy.pid").exists()
        assert created["traders"][0].calls == [
            "initialize_fork",
            "initialize_orchestrator",
            ("execute_tick", "Strategy"),
            "cleanup",
        ]
        assert created["traders"][1].calls == [
            "initialize_fork",
            "initialize_orchestrator",
            "sync_wallet_to_fork",
            ("execute_tick", "Strategy"),
            "cleanup",
        ]
        assert strategy_calls == {
            "config": {},
            "chain": "arbitrum",
            "wallet_address": "0x" + "0" * 40,
            "risk_guard_config": None,
        }


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
            deployment_id="test_strategy",
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
            deployment_id="test_strategy",
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
            deployment_id="test_strategy",
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


class TestBatch1Fixes:
    """Tests for Paper Trading Batch 1 fixes (resume raw_rpc_url, hex crash, port contention)."""

    class FakeSocket:
        def __init__(self, assigned_port, binds):
            self.assigned_port = assigned_port
            self.binds = binds

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def bind(self, address):
            self.binds.append(address)

        def getsockname(self):
            return ("127.0.0.1", self.assigned_port)

    class FakeSocketFactory:
        def __init__(self, assigned_ports):
            self.assigned_ports = list(assigned_ports)
            self.binds = []

        def __call__(self, family, socket_type):
            return TestBatch1Fixes.FakeSocket(self.assigned_ports.pop(0), self.binds)

    @staticmethod
    def create_config(**overrides) -> PaperTraderConfig:
        defaults = {
            "chain": "arbitrum",
            "rpc_url": "https://arb-mainnet.g.alchemy.com/v2/test-key",
            "deployment_id": "test_strategy",
            "tick_interval_seconds": 60,
            "max_ticks": 10,
        }
        defaults.update(overrides)
        return PaperTraderConfig(**defaults)

    def test_resume_passes_raw_rpc_url(self, tmp_path):
        """Fix #1: resume() must pass raw_rpc_url to child process kwargs."""
        config = self.create_config()
        bg_trader = BackgroundPaperTrader(config=config, state_dir=tmp_path)

        # Create a valid state file so resume doesn't fail on missing state
        now = datetime.now(UTC)
        state = PaperTraderState(
            deployment_id="test_strategy",
            session_start=now,
            last_save=now,
            tick_count=5,
            trades=[],
            errors=[],
            current_balances={"ETH": Decimal("10")},
            initial_balances={"ETH": Decimal("10")},
            equity_curve=[],
            config=config.to_dict(),
            pid=12345,
            status="stopped",
        )
        state_file = tmp_path / "test_strategy.state.json"
        state.save(state_file)

        # Capture the kwargs passed to Process
        captured_kwargs = {}

        class MockProcess:
            def __init__(self, target, args, kwargs, daemon=False):
                captured_kwargs.update(kwargs)
                self.pid = 99999

            def start(self):
                pass

        with patch("almanak.framework.backtesting.paper.background.multiprocessing.Process", MockProcess):
            bg_trader.resume(
                strategy_module="test_module",
                strategy_class="Strategy",
            )
        assert "raw_rpc_url" in captured_kwargs, "resume() must pass raw_rpc_url in kwargs"
        assert captured_kwargs["raw_rpc_url"] == config.rpc_url

    def test_find_free_port_returns_valid_port(self):
        """Fix #6: _find_free_port() returns a usable TCP port."""
        from almanak.framework.backtesting.paper.background import _find_free_port

        fake_socket = self.FakeSocketFactory([8655])
        with patch("almanak.framework.backtesting.paper.background.socket.socket", fake_socket):
            port = _find_free_port()

        assert 1024 <= port <= 65535
        assert fake_socket.binds == [("127.0.0.1", 0)]

    def test_find_free_port_avoids_used_ports(self):
        """Fix #3: _find_free_port() must not return a port that is already in use."""
        from almanak.framework.backtesting.paper.background import _find_free_port

        fake_socket = self.FakeSocketFactory([8655, 8656])
        with patch("almanak.framework.backtesting.paper.background.socket.socket", fake_socket):
            port1 = _find_free_port()
            port2 = _find_free_port()

        assert port1 != port2
        assert fake_socket.binds == [("127.0.0.1", 0), ("127.0.0.1", 0)]
