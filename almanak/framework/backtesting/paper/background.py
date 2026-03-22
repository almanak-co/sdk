"""Background process management for Paper Trading.

This module provides functionality to run Paper Trader as a background process,
enabling long-running paper trading sessions that persist across terminal sessions.

Key Components:
    - BackgroundPaperTrader: Manages Paper Trader as a background process
    - PaperTraderState: Serializable state for persistence
    - PIDFile: PID file management to prevent multiple sessions
    - TradeHistoryWriter: Incremental trade history persistence

Features:
    - Background process execution with multiprocessing
    - PID file locking to prevent multiple sessions
    - Periodic state persistence at configurable intervals
    - Incremental trade history saved to JSONL file
    - Resume from saved state support

Example:
    from almanak.framework.backtesting.paper.background import (
        BackgroundPaperTrader,
        PaperTraderState,
    )

    # Create background trader
    bg_trader = BackgroundPaperTrader(
        config=config,
        state_dir="/path/to/state",
    )

    # Start in background
    pid = bg_trader.start(strategy)

    # Check status
    status = bg_trader.get_status()

    # Resume from saved state (if stopped)
    pid = bg_trader.resume(strategy)

    # Stop gracefully
    bg_trader.stop()
"""

import atexit
import json
import logging
import multiprocessing
import os
import signal
import sys
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from almanak.core.redaction import install_redaction
from almanak.framework.backtesting.paper.config import PaperTraderConfig
from almanak.framework.backtesting.paper.models import (
    PaperTrade,
    PaperTradeError,
)

logger = logging.getLogger(__name__)


# =============================================================================
# PID File Management
# =============================================================================


@dataclass
class PIDFile:
    """Manages PID file for background process identification.

    The PID file prevents multiple Paper Trader sessions from running
    simultaneously for the same strategy, avoiding resource conflicts
    and state corruption.

    Attributes:
        path: Path to the PID file
        strategy_id: Strategy identifier
        pid: Process ID (set when acquired)
    """

    path: Path
    strategy_id: str
    pid: int | None = None

    @classmethod
    def get_default_path(cls, strategy_id: str, state_dir: Path | None = None) -> Path:
        """Get the default PID file path for a strategy.

        Args:
            strategy_id: Strategy identifier
            state_dir: Optional state directory (defaults to ~/.almanak/paper/)

        Returns:
            Path to the PID file
        """
        if state_dir is None:
            state_dir = Path.home() / ".almanak" / "paper"
        return state_dir / f"{strategy_id}.pid"

    def acquire(self) -> bool:
        """Acquire the PID file lock.

        Creates the PID file with the current process ID. If a PID file
        already exists with a running process, acquisition fails.

        Returns:
            True if lock acquired, False if another process holds it

        Raises:
            PermissionError: If unable to create PID file
        """
        # Ensure parent directory exists
        self.path.parent.mkdir(parents=True, exist_ok=True)

        # Check if PID file exists
        if self.path.exists():
            # Read existing PID
            try:
                existing_pid = int(self.path.read_text().strip())
                # Check if process is still running
                if self._is_process_running(existing_pid):
                    logger.warning(f"Paper Trader for {self.strategy_id} already running (PID: {existing_pid})")
                    return False
                # Process not running, remove stale PID file
                logger.info(f"Removing stale PID file for {self.strategy_id}")
                self.path.unlink()
            except (ValueError, OSError) as e:
                logger.warning(f"Error reading PID file: {e}, removing it")
                try:
                    self.path.unlink()
                except OSError:
                    pass

        # Write new PID file atomically using O_EXCL to prevent races
        self.pid = os.getpid()
        try:
            fd = os.open(str(self.path), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
            try:
                os.write(fd, str(self.pid).encode())
            finally:
                os.close(fd)
            logger.debug(f"Acquired PID file {self.path} with PID {self.pid}")
            return True
        except FileExistsError:
            # Race condition: another process created the file
            logger.warning(f"PID file {self.path} created by another process during acquire")
            return False
        except OSError as e:
            logger.error(f"Failed to write PID file: {e}")
            raise PermissionError(f"Cannot create PID file at {self.path}") from e

    def release(self) -> None:
        """Release the PID file lock.

        Removes the PID file if it exists and belongs to the current process.
        Safe to call multiple times.
        """
        if not self.path.exists():
            return

        try:
            # Only remove if this process owns it
            existing_pid = int(self.path.read_text().strip())
            if existing_pid == os.getpid():
                self.path.unlink()
                logger.debug(f"Released PID file {self.path}")
            else:
                logger.warning(f"PID file {self.path} owned by different process ({existing_pid})")
        except (ValueError, OSError) as e:
            logger.warning(f"Error releasing PID file: {e}")

    def is_running(self) -> bool:
        """Check if the process holding the PID file is running.

        Returns:
            True if PID file exists and process is running
        """
        if not self.path.exists():
            return False

        try:
            pid = int(self.path.read_text().strip())
            return self._is_process_running(pid)
        except (ValueError, OSError):
            return False

    def get_pid(self) -> int | None:
        """Get the PID from the PID file.

        Returns:
            Process ID if file exists and is valid, None otherwise
        """
        if not self.path.exists():
            return None

        try:
            return int(self.path.read_text().strip())
        except (ValueError, OSError):
            return None

    @staticmethod
    def _is_process_running(pid: int) -> bool:
        """Check if a process with the given PID is running.

        Args:
            pid: Process ID to check

        Returns:
            True if process is running
        """
        try:
            os.kill(pid, 0)
            return True
        except (OSError, ProcessLookupError):
            return False


# =============================================================================
# Trade History Writer (Incremental Persistence)
# =============================================================================


@dataclass
class TradeHistoryWriter:
    """Writes trade history incrementally to a JSONL file.

    Each trade is appended as a single JSON line, ensuring no data loss
    even if the process crashes. The JSONL format allows efficient
    append-only writes and easy streaming reads.

    Attributes:
        path: Path to the trade history JSONL file
        strategy_id: Strategy identifier
        _trade_count: Number of trades written
        _error_count: Number of errors written
    """

    path: Path
    strategy_id: str
    _trade_count: int = field(default=0, init=False)
    _error_count: int = field(default=0, init=False)

    @classmethod
    def get_default_path(cls, strategy_id: str, state_dir: Path | None = None) -> Path:
        """Get the default trade history file path for a strategy.

        Args:
            strategy_id: Strategy identifier
            state_dir: Optional state directory (defaults to ~/.almanak/paper/)

        Returns:
            Path to the trade history JSONL file
        """
        if state_dir is None:
            state_dir = Path.home() / ".almanak" / "paper"
        return state_dir / f"{strategy_id}.trades.jsonl"

    def write_trade(self, trade: PaperTrade) -> None:
        """Append a trade to the history file.

        Args:
            trade: PaperTrade to write

        Raises:
            OSError: If unable to write to file
        """
        # Ensure parent directory exists
        self.path.parent.mkdir(parents=True, exist_ok=True)

        # Append as JSONL (one JSON object per line)
        record = {
            "type": "trade",
            "data": trade.to_dict(),
        }
        try:
            with open(self.path, "a") as f:
                f.write(json.dumps(record, default=str) + "\n")
            self._trade_count += 1
            logger.debug(f"Wrote trade {self._trade_count} to {self.path}")
        except OSError as e:
            logger.error(f"Failed to write trade to history: {e}")
            raise

    def write_error(self, error: PaperTradeError) -> None:
        """Append an error to the history file.

        Args:
            error: PaperTradeError to write

        Raises:
            OSError: If unable to write to file
        """
        # Ensure parent directory exists
        self.path.parent.mkdir(parents=True, exist_ok=True)

        # Append as JSONL
        record = {
            "type": "error",
            "data": error.to_dict(),
        }
        try:
            with open(self.path, "a") as f:
                f.write(json.dumps(record, default=str) + "\n")
            self._error_count += 1
            logger.debug(f"Wrote error {self._error_count} to {self.path}")
        except OSError as e:
            logger.error(f"Failed to write error to history: {e}")
            raise

    def read_all(self) -> tuple[list[PaperTrade], list[PaperTradeError]]:
        """Read all trades and errors from the history file.

        Returns:
            Tuple of (trades list, errors list)
        """
        trades: list[PaperTrade] = []
        errors: list[PaperTradeError] = []

        if not self.path.exists():
            return trades, errors

        try:
            with open(self.path) as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        record = json.loads(line)
                        if record.get("type") == "trade":
                            trades.append(PaperTrade.from_dict(record["data"]))
                        elif record.get("type") == "error":
                            errors.append(PaperTradeError.from_dict(record["data"]))
                    except (json.JSONDecodeError, KeyError, ValueError) as e:
                        logger.warning(f"Skipping invalid line {line_num} in {self.path}: {e}")
        except OSError as e:
            logger.error(f"Failed to read trade history: {e}")

        return trades, errors

    def get_trade_count(self) -> int:
        """Get the number of trades in the history file.

        Returns:
            Number of trade records in the file
        """
        if not self.path.exists():
            return 0

        count = 0
        try:
            with open(self.path) as f:
                for line in f:
                    if '"type": "trade"' in line or '"type":"trade"' in line:
                        count += 1
        except OSError:
            pass
        return count

    def truncate(self) -> None:
        """Clear the trade history file.

        Warning: This will delete all recorded trades!
        """
        if self.path.exists():
            self.path.unlink()
            logger.info(f"Cleared trade history at {self.path}")
        self._trade_count = 0
        self._error_count = 0


# =============================================================================
# Paper Trader State
# =============================================================================


@dataclass
class PaperTraderState:
    """Serializable state of a Paper Trader session.

    This dataclass captures the complete state of a running Paper Trader
    session, allowing it to be persisted to disk and resumed later.

    Attributes:
        strategy_id: Strategy identifier
        session_start: When the session started
        last_save: When state was last saved
        tick_count: Number of ticks executed
        trades: List of successful trades
        errors: List of trade errors
        current_balances: Current token balances
        initial_balances: Initial token balances
        equity_curve: List of equity points (timestamp, value)
        config: Paper trader configuration dict
        pid: Process ID of the background process
        status: Current status (running, stopped, error, resumed)
        is_resumed: Whether this session was resumed from saved state
        resume_count: Number of times this session has been resumed
        last_resume_time: When the session was last resumed (if resumed)
    """

    strategy_id: str
    session_start: datetime
    last_save: datetime
    tick_count: int
    trades: list[PaperTrade]
    errors: list[PaperTradeError]
    current_balances: dict[str, Decimal]
    initial_balances: dict[str, Decimal]
    equity_curve: list[tuple[datetime, Decimal, Decimal | None]]  # (timestamp, value_usd, eth_price_usd)
    config: dict[str, Any]
    pid: int
    status: str = "running"  # running, stopped, error, resumed
    is_resumed: bool = False
    resume_count: int = 0
    last_resume_time: datetime | None = None

    def to_dict(self) -> dict[str, Any]:
        """Serialize state to dictionary.

        Returns:
            Dictionary representation suitable for JSON serialization
        """
        result = {
            "strategy_id": self.strategy_id,
            "session_start": self.session_start.isoformat(),
            "last_save": self.last_save.isoformat(),
            "tick_count": self.tick_count,
            "trades": [t.to_dict() for t in self.trades],
            "errors": [e.to_dict() for e in self.errors],
            "current_balances": {k: str(v) for k, v in self.current_balances.items()},
            "initial_balances": {k: str(v) for k, v in self.initial_balances.items()},
            "equity_curve": [
                {
                    "timestamp": ts.isoformat(),
                    "value": str(val),
                    **({"eth_price_usd": str(ep)} if ep is not None else {}),
                }
                for ts, val, ep in self.equity_curve
            ],
            "config": self.config,
            "pid": self.pid,
            "status": self.status,
            "is_resumed": self.is_resumed,
            "resume_count": self.resume_count,
        }
        # Optional field
        if self.last_resume_time is not None:
            result["last_resume_time"] = self.last_resume_time.isoformat()
        return result

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PaperTraderState":
        """Deserialize state from dictionary.

        Args:
            data: Dictionary with serialized state data

        Returns:
            PaperTraderState instance
        """
        # Parse optional last_resume_time
        last_resume_time = None
        if data.get("last_resume_time"):
            last_resume_time = datetime.fromisoformat(data["last_resume_time"])

        return cls(
            strategy_id=data["strategy_id"],
            session_start=datetime.fromisoformat(data["session_start"]),
            last_save=datetime.fromisoformat(data["last_save"]),
            tick_count=data["tick_count"],
            trades=[PaperTrade.from_dict(t) for t in data.get("trades", [])],
            errors=[PaperTradeError.from_dict(e) for e in data.get("errors", [])],
            current_balances={k: Decimal(v) for k, v in data.get("current_balances", {}).items()},
            initial_balances={k: Decimal(v) for k, v in data.get("initial_balances", {}).items()},
            equity_curve=[
                (
                    datetime.fromisoformat(p["timestamp"]),
                    Decimal(p["value"]),
                    Decimal(p["eth_price_usd"]) if "eth_price_usd" in p else None,
                )
                for p in data.get("equity_curve", [])
            ],
            config=data.get("config", {}),
            pid=data["pid"],
            status=data.get("status", "running"),
            is_resumed=data.get("is_resumed", False),
            resume_count=data.get("resume_count", 0),
            last_resume_time=last_resume_time,
        )

    def save(self, path: Path) -> None:
        """Save state to file.

        Args:
            path: Path to save state file

        Raises:
            OSError: If unable to write file
        """
        # Ensure parent directory exists
        path.parent.mkdir(parents=True, exist_ok=True)

        # Update last_save timestamp
        self.last_save = datetime.now(UTC)

        # Write atomically via temp file
        temp_path = path.with_suffix(".tmp")
        try:
            with open(temp_path, "w") as f:
                json.dump(self.to_dict(), f, indent=2, default=str)
            temp_path.replace(path)
            logger.debug(f"Saved state to {path}")
        except OSError as e:
            logger.error(f"Failed to save state: {e}")
            raise

    @classmethod
    def load(cls, path: Path) -> "PaperTraderState":
        """Load state from file.

        Args:
            path: Path to state file

        Returns:
            PaperTraderState instance

        Raises:
            FileNotFoundError: If state file doesn't exist
            ValueError: If state file is invalid
        """
        if not path.exists():
            raise FileNotFoundError(f"State file not found: {path}")

        try:
            with open(path) as f:
                data = json.load(f)
            return cls.from_dict(data)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid state file: {e}") from e

    def can_resume(self) -> bool:
        """Check if this state can be resumed.

        A state can be resumed if:
        - Status is 'stopped' or 'error' (not 'running' or 'completed')
        - Has valid balances and config

        Returns:
            True if the state can be resumed
        """
        # Can only resume if not currently running
        if self.status == "running":
            return False
        # Cannot resume a completed session
        if self.status == "completed":
            return False
        # Must have valid balances
        if not self.current_balances:
            return False
        # Must have valid config
        if not self.config:
            return False
        return True

    def prepare_for_resume(self, new_pid: int) -> None:
        """Prepare state for resumption.

        Updates resume tracking fields and status.

        Args:
            new_pid: Process ID of the resuming process
        """
        self.is_resumed = True
        self.resume_count += 1
        self.last_resume_time = datetime.now(UTC)
        self.pid = new_pid
        self.status = "running"
        logger.info(
            f"Prepared state for resume: tick_count={self.tick_count}, "
            f"trades={len(self.trades)}, resume_count={self.resume_count}"
        )


# =============================================================================
# Background Process Status
# =============================================================================


@dataclass
class BackgroundStatus:
    """Status of a background Paper Trader process.

    Attributes:
        is_running: Whether the process is running
        pid: Process ID if running
        strategy_id: Strategy identifier
        session_start: When session started
        tick_count: Number of ticks executed
        trade_count: Number of successful trades
        error_count: Number of errors
        last_save: When state was last saved
        uptime: Duration since session start
        is_resumed: Whether this session was resumed
        resume_count: Number of times this session has been resumed
        can_resume: Whether this session can be resumed (if stopped)
    """

    is_running: bool
    pid: int | None = None
    strategy_id: str | None = None
    session_start: datetime | None = None
    tick_count: int = 0
    trade_count: int = 0
    error_count: int = 0
    last_save: datetime | None = None
    status: str = "unknown"
    is_resumed: bool = False
    resume_count: int = 0
    can_resume: bool = False

    @property
    def uptime(self) -> timedelta | None:
        """Get session uptime."""
        if self.session_start is None:
            return None
        return datetime.now(UTC) - self.session_start

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "is_running": self.is_running,
            "pid": self.pid,
            "strategy_id": self.strategy_id,
            "session_start": self.session_start.isoformat() if self.session_start else None,
            "tick_count": self.tick_count,
            "trade_count": self.trade_count,
            "error_count": self.error_count,
            "last_save": self.last_save.isoformat() if self.last_save else None,
            "status": self.status,
            "uptime_seconds": self.uptime.total_seconds() if self.uptime else None,
            "is_resumed": self.is_resumed,
            "resume_count": self.resume_count,
            "can_resume": self.can_resume,
        }


# =============================================================================
# Background Paper Trader Manager
# =============================================================================


@dataclass
class BackgroundPaperTrader:
    """Manages Paper Trader as a background process.

    This class handles starting, stopping, and monitoring Paper Trader
    sessions running in the background. It uses multiprocessing for
    process isolation and PID files for coordination.

    Attributes:
        config: Paper trader configuration
        state_dir: Directory for state files (default: ~/.almanak/paper/)
        save_interval_seconds: Interval between state saves (default: 60)

    Example:
        bg_trader = BackgroundPaperTrader(
            config=config,
            state_dir=Path("/path/to/state"),
        )

        # Start background process
        pid = bg_trader.start(strategy_module="my_strategy")

        # Check status
        status = bg_trader.get_status()
        print(f"Running: {status.is_running}, Trades: {status.trade_count}")

        # Stop gracefully
        bg_trader.stop()
    """

    config: PaperTraderConfig
    state_dir: Path = field(default_factory=lambda: Path.home() / ".almanak" / "paper")
    save_interval_seconds: int = 60

    # Internal state
    _pid_file: PIDFile | None = field(default=None, init=False, repr=False)
    _process: multiprocessing.Process | None = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        """Initialize after dataclass creation."""
        # Ensure state directory exists
        self.state_dir.mkdir(parents=True, exist_ok=True)

    @property
    def state_file(self) -> Path:
        """Get the state file path for this strategy."""
        return self.state_dir / f"{self.config.strategy_id}.state.json"

    @property
    def pid_file_path(self) -> Path:
        """Get the PID file path for this strategy."""
        return PIDFile.get_default_path(self.config.strategy_id, self.state_dir)

    @property
    def log_file(self) -> Path:
        """Get the log file path for this strategy."""
        return self.state_dir / f"{self.config.strategy_id}.log"

    def start(
        self,
        strategy_module: str,
        strategy_class: str = "Strategy",
        strategy_config: dict[str, Any] | None = None,
    ) -> int:
        """Start Paper Trader as a background process.

        Args:
            strategy_module: Module path to import strategy from
            strategy_class: Class name of the strategy (default: "Strategy")
            strategy_config: Strategy configuration dict (from config.json).
                Required for IntentStrategy subclasses that need (config, chain, wallet_address).

        Returns:
            Process ID of the background process

        Raises:
            RuntimeError: If a process is already running for this strategy
        """
        # Check if already running
        pid_file = PIDFile(
            path=self.pid_file_path,
            strategy_id=self.config.strategy_id,
        )

        if pid_file.is_running():
            existing_pid = pid_file.get_pid()
            raise RuntimeError(f"Paper Trader for {self.config.strategy_id} already running (PID: {existing_pid})")

        # Fix SSL cert resolution for spawned subprocess (macOS multiprocessing)
        import os

        if "SSL_CERT_FILE" not in os.environ:
            for cert_path in ["/private/etc/ssl/cert.pem", "/etc/ssl/cert.pem"]:
                if os.path.exists(cert_path):
                    os.environ["SSL_CERT_FILE"] = cert_path
                    break

        # Create the background process
        # Pass raw rpc_url separately since to_dict() masks it for display
        self._process = multiprocessing.Process(
            target=_run_background_paper_trader,
            args=(
                self.config.to_dict(),
                strategy_module,
                strategy_class,
                str(self.state_dir),
                self.save_interval_seconds,
            ),
            kwargs={"strategy_config": strategy_config, "raw_rpc_url": self.config.rpc_url},
            daemon=False,  # Not daemon so it can continue after parent exits
        )

        # Start the process
        self._process.start()

        if self._process.pid is None:
            raise RuntimeError("Failed to start background process")

        logger.info(f"Started background Paper Trader for {self.config.strategy_id} (PID: {self._process.pid})")

        return self._process.pid

    def stop(self, timeout: float = 30.0) -> bool:
        """Stop the background Paper Trader process.

        Sends SIGTERM for graceful shutdown, then SIGKILL if needed.

        Args:
            timeout: Seconds to wait for graceful shutdown before SIGKILL

        Returns:
            True if process was stopped, False if no process was running
        """
        pid_file = PIDFile(
            path=self.pid_file_path,
            strategy_id=self.config.strategy_id,
        )

        pid = pid_file.get_pid()
        if pid is None or not pid_file.is_running():
            logger.info(f"No running Paper Trader for {self.config.strategy_id}")
            return False

        logger.info(f"Stopping Paper Trader (PID: {pid})...")

        try:
            # Send SIGTERM for graceful shutdown
            os.kill(pid, signal.SIGTERM)

            # Wait for process to exit
            start = time.time()
            while time.time() - start < timeout:
                if not PIDFile._is_process_running(pid):
                    logger.info(f"Paper Trader (PID: {pid}) stopped gracefully")
                    return True
                time.sleep(0.5)

            # Process didn't exit, send SIGKILL
            logger.warning(f"Paper Trader (PID: {pid}) didn't respond to SIGTERM, sending SIGKILL")
            os.kill(pid, signal.SIGKILL)

            # Brief wait for SIGKILL
            time.sleep(1)

            if PIDFile._is_process_running(pid):
                logger.error(f"Failed to stop Paper Trader (PID: {pid})")
                return False

            logger.info(f"Paper Trader (PID: {pid}) killed")
            return True

        except (OSError, ProcessLookupError) as e:
            logger.warning(f"Error stopping process: {e}")
            return False
        finally:
            # Clean up PID file
            pid_file.release()

    def get_status(self) -> BackgroundStatus:
        """Get the status of the background Paper Trader.

        Returns:
            BackgroundStatus with process and session information
        """
        pid_file = PIDFile(
            path=self.pid_file_path,
            strategy_id=self.config.strategy_id,
        )

        pid = pid_file.get_pid()
        is_running = pid is not None and pid_file.is_running()

        status = BackgroundStatus(
            is_running=is_running,
            pid=pid,
            strategy_id=self.config.strategy_id,
            status="running" if is_running else "stopped",
        )

        # Try to load state for additional info
        if self.state_file.exists():
            try:
                state = PaperTraderState.load(self.state_file)
                status.session_start = state.session_start
                status.tick_count = state.tick_count
                status.trade_count = len(state.trades)
                status.error_count = len(state.errors)
                status.last_save = state.last_save
                status.status = state.status
                status.is_resumed = state.is_resumed
                status.resume_count = state.resume_count
                # Can resume if not running and state allows it
                status.can_resume = not is_running and state.can_resume()
            except (ValueError, FileNotFoundError) as e:
                logger.warning(f"Error loading state: {e}")

        return status

    def get_state(self) -> PaperTraderState | None:
        """Get the current state of the Paper Trader.

        Returns:
            PaperTraderState if state file exists, None otherwise
        """
        if not self.state_file.exists():
            return None

        try:
            return PaperTraderState.load(self.state_file)
        except (ValueError, FileNotFoundError) as e:
            logger.warning(f"Error loading state: {e}")
            return None

    def get_logs(self, lines: int = 100) -> str:
        """Get recent log entries from the background process.

        Args:
            lines: Number of lines to return (default: 100)

        Returns:
            String containing recent log entries
        """
        if not self.log_file.exists():
            return f"No log file found at {self.log_file}"

        try:
            with open(self.log_file) as f:
                all_lines = f.readlines()
                return "".join(all_lines[-lines:])
        except OSError as e:
            return f"Error reading log file: {e}"

    @property
    def trade_history_file(self) -> Path:
        """Get the trade history JSONL file path for this strategy."""
        return TradeHistoryWriter.get_default_path(self.config.strategy_id, self.state_dir)

    def resume(
        self,
        strategy_module: str,
        strategy_class: str = "Strategy",
        strategy_config: dict[str, Any] | None = None,
    ) -> int:
        """Resume Paper Trader from saved state.

        Continues a stopped session from where it left off, preserving
        tick count, trades, errors, and current balances.

        Args:
            strategy_module: Module path to import strategy from
            strategy_class: Class name of the strategy (default: "Strategy")
            strategy_config: Strategy configuration dict (from config.json).

        Returns:
            Process ID of the background process

        Raises:
            RuntimeError: If process is already running or state cannot be resumed
            FileNotFoundError: If no saved state exists
        """
        # Check if already running
        pid_file = PIDFile(
            path=self.pid_file_path,
            strategy_id=self.config.strategy_id,
        )

        if pid_file.is_running():
            existing_pid = pid_file.get_pid()
            raise RuntimeError(f"Paper Trader for {self.config.strategy_id} already running (PID: {existing_pid})")

        # Check if state exists and can be resumed
        if not self.state_file.exists():
            raise FileNotFoundError(f"No saved state found at {self.state_file}")

        state = PaperTraderState.load(self.state_file)
        if not state.can_resume():
            raise RuntimeError(
                f"Cannot resume session: status={state.status}, "
                f"balances={len(state.current_balances)}, config={'present' if state.config else 'missing'}"
            )

        logger.info(
            f"Resuming Paper Trader for {self.config.strategy_id} "
            f"(tick_count={state.tick_count}, trades={len(state.trades)})"
        )

        # Create the background process with resume flag
        self._process = multiprocessing.Process(
            target=_run_background_paper_trader,
            args=(
                self.config.to_dict(),
                strategy_module,
                strategy_class,
                str(self.state_dir),
                self.save_interval_seconds,
            ),
            kwargs={"resume": True, "strategy_config": strategy_config},
            daemon=False,
        )

        # Start the process
        self._process.start()

        if self._process.pid is None:
            raise RuntimeError("Failed to start background process")

        logger.info(f"Resumed background Paper Trader for {self.config.strategy_id} (PID: {self._process.pid})")

        return self._process.pid

    def clear_state(self) -> bool:
        """Clear all saved state for this strategy.

        Warning: This will delete all saved state, trade history, and logs!

        Returns:
            True if any files were cleared, False if no files existed
        """
        # Check if running
        pid_file = PIDFile(
            path=self.pid_file_path,
            strategy_id=self.config.strategy_id,
        )
        if pid_file.is_running():
            raise RuntimeError(f"Cannot clear state while Paper Trader is running (PID: {pid_file.get_pid()})")

        cleared = False

        # Remove state file
        if self.state_file.exists():
            self.state_file.unlink()
            logger.info(f"Cleared state file: {self.state_file}")
            cleared = True

        # Remove trade history
        if self.trade_history_file.exists():
            self.trade_history_file.unlink()
            logger.info(f"Cleared trade history: {self.trade_history_file}")
            cleared = True

        # Remove log file
        if self.log_file.exists():
            self.log_file.unlink()
            logger.info(f"Cleared log file: {self.log_file}")
            cleared = True

        # Remove PID file (in case it's stale)
        if self.pid_file_path.exists():
            self.pid_file_path.unlink()
            logger.info(f"Cleared PID file: {self.pid_file_path}")
            cleared = True

        return cleared


# =============================================================================
# Background Process Entry Point
# =============================================================================


def _run_background_paper_trader(
    config_dict: dict[str, Any],
    strategy_module: str,
    strategy_class: str,
    state_dir: str,
    save_interval_seconds: int,
    resume: bool = False,
    strategy_config: dict[str, Any] | None = None,
    raw_rpc_url: str | None = None,
) -> None:
    """Entry point for the background Paper Trader process.

    This function runs in a separate process and handles:
    1. PID file acquisition
    2. Signal handling for graceful shutdown
    3. Periodic state persistence
    4. Incremental trade history saving
    5. Main trading loop
    6. Resume from saved state

    Args:
        config_dict: Serialized PaperTraderConfig
        strategy_module: Module path for strategy import
        strategy_class: Class name of the strategy
        state_dir: Directory for state and log files
        save_interval_seconds: Interval between state saves
        resume: Whether to resume from saved state (default: False)
        strategy_config: Strategy configuration dict (from config.json).
        raw_rpc_url: Unmasked RPC URL (to_dict masks it for display).
    """
    import asyncio
    import os

    # Fix SSL cert resolution in spawned subprocess (macOS multiprocessing issue)
    if "SSL_CERT_FILE" not in os.environ:
        for cert_path in ["/private/etc/ssl/cert.pem", "/etc/ssl/cert.pem"]:
            if os.path.exists(cert_path):
                os.environ["SSL_CERT_FILE"] = cert_path
                break

    # Convert paths
    state_path = Path(state_dir)
    # Restore unmasked RPC URL before deserializing (to_dict masks it for display)
    if raw_rpc_url:
        config_dict["rpc_url"] = raw_rpc_url
    config = PaperTraderConfig.from_dict(config_dict)

    # Set up logging to file
    log_file = state_path / f"{config.strategy_id}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout),
        ],
    )

    # Install centralized secret redaction
    install_redaction()

    bg_logger = logging.getLogger("almanak.framework.backtesting.paper.background")
    bg_logger.info(f"Starting background Paper Trader for {config.strategy_id}")

    # Acquire PID file
    pid_file = PIDFile(
        path=PIDFile.get_default_path(config.strategy_id, state_path),
        strategy_id=config.strategy_id,
    )

    if not pid_file.acquire():
        bg_logger.error("Failed to acquire PID file, exiting")
        sys.exit(1)

    # Register cleanup
    def cleanup() -> None:
        pid_file.release()
        bg_logger.info("Cleanup complete")

    atexit.register(cleanup)

    # Signal handling for graceful shutdown
    shutdown_requested = False

    def signal_handler(signum: int, frame: Any) -> None:
        nonlocal shutdown_requested
        bg_logger.info(f"Received signal {signum}, requesting shutdown...")
        shutdown_requested = True

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    # Import and instantiate strategy
    _BACKTEST_WALLET = "0x" + "0" * 40
    try:
        import importlib

        module = importlib.import_module(strategy_module)
        cls = getattr(module, strategy_class)
        strat_cfg = strategy_config or {}

        # Try IntentStrategy signature: (config, chain, wallet_address, risk_guard_config)
        try:
            strategy = cls(
                config=strat_cfg,
                chain=config.chain,
                wallet_address=config.wallet_address or _BACKTEST_WALLET,
                risk_guard_config=None,
            )
        except TypeError:
            # Fall back to simple signature: (config,)
            try:
                strategy = cls(strat_cfg)
            except TypeError:
                # Fall back to no-arg constructor (mock strategies)
                strategy = cls()

        bg_logger.info(f"Loaded strategy {strategy_class} from {strategy_module}")
    except (ImportError, AttributeError) as e:
        bg_logger.error(f"Failed to import strategy: {e}")
        sys.exit(1)

    # Initialize state and trade history writer
    state_file = state_path / f"{config.strategy_id}.state.json"
    trade_history_file = TradeHistoryWriter.get_default_path(config.strategy_id, state_path)
    trade_history = TradeHistoryWriter(path=trade_history_file, strategy_id=config.strategy_id)

    # Handle resume vs fresh start
    if resume and state_file.exists():
        # Load existing state
        try:
            state = PaperTraderState.load(state_file)
            if not state.can_resume():
                bg_logger.error(f"State cannot be resumed: status={state.status}")
                sys.exit(1)

            # Prepare state for resume
            state.prepare_for_resume(os.getpid())
            bg_logger.info(
                f"Resuming session: tick_count={state.tick_count}, "
                f"trades={len(state.trades)}, resume_count={state.resume_count}"
            )
        except (ValueError, FileNotFoundError) as e:
            bg_logger.error(f"Failed to load state for resume: {e}")
            sys.exit(1)
    else:
        # Create fresh state
        session_start = datetime.now(UTC)
        state = PaperTraderState(
            strategy_id=config.strategy_id,
            session_start=session_start,
            last_save=session_start,
            tick_count=0,
            trades=[],
            errors=[],
            current_balances=config.get_initial_balances(),
            initial_balances=config.get_initial_balances(),
            equity_curve=[],
            config=config_dict,
            pid=os.getpid(),
            status="running",
        )
        # Clear trade history for fresh start
        trade_history.truncate()
        bg_logger.info("Starting fresh session")

    # Save initial/resumed state
    state.save(state_file)

    async def run_paper_trader() -> None:
        """Run the paper trading loop."""
        nonlocal shutdown_requested

        # Import here to avoid circular imports
        from almanak.framework.anvil.fork_manager import RollingForkManager
        from almanak.framework.backtesting.paper.engine import PaperTrader
        from almanak.framework.backtesting.paper.portfolio_tracker import PaperPortfolioTracker

        # Create components
        fork_manager = RollingForkManager(
            rpc_url=config.rpc_url,
            chain=config.chain,
            anvil_port=config.anvil_port,
        )

        portfolio_tracker = PaperPortfolioTracker(
            strategy_id=config.strategy_id,
            initial_balances=config.get_initial_balances(),
        )

        # Create paper trader
        trader = PaperTrader(
            fork_manager=fork_manager,
            portfolio_tracker=portfolio_tracker,
            config=config,
        )

        bg_logger.info("Initializing Paper Trader...")

        try:
            # Initialize fork
            await trader._initialize_fork()
            await trader._initialize_orchestrator()

            trader._running = True
            trader._current_strategy = strategy

            # Restore state if resuming, otherwise start fresh
            if state.is_resumed:
                trader._tick_count = state.tick_count
                trader._trades = list(state.trades)  # Copy to avoid mutations
                trader._errors = list(state.errors)
                # Restore balances to portfolio tracker
                for token, amount in state.current_balances.items():
                    portfolio_tracker.current_balances[token] = amount
                bg_logger.info(
                    f"Restored state: tick_count={trader._tick_count}, "
                    f"trades={len(trader._trades)}, balances={len(state.current_balances)}"
                )
                # Re-sync on-chain wallet to match resumed tracker balances
                # (_initialize_fork funded with initial config; now correct to actual)
                await trader._sync_wallet_to_fork()
            else:
                trader._tick_count = 0
                trader._trades = []
                trader._errors = []

            last_save_time = time.time()
            # Track number of trades/errors for incremental saving
            last_trade_count = len(trader._trades)
            last_error_count = len(trader._errors)

            bg_logger.info("Starting trading loop...")

            while trader._running and not shutdown_requested:
                # Check tick limit
                if config.max_ticks is not None and trader._tick_count >= config.max_ticks:
                    bg_logger.info(f"Max ticks ({config.max_ticks}) reached")
                    break

                # Execute tick
                try:
                    await trader._execute_tick(strategy)
                    trader._tick_count += 1
                except Exception as e:
                    bg_logger.error(f"Error during tick: {e}")

                # Write new trades incrementally to JSONL
                current_trade_count = len(trader._trades)
                if current_trade_count > last_trade_count:
                    for i in range(last_trade_count, current_trade_count):
                        try:
                            trade_history.write_trade(trader._trades[i])
                        except OSError as e:
                            bg_logger.error(f"Failed to write trade to history: {e}")
                    last_trade_count = current_trade_count

                # Write new errors incrementally to JSONL
                current_error_count = len(trader._errors)
                if current_error_count > last_error_count:
                    for i in range(last_error_count, current_error_count):
                        try:
                            trade_history.write_error(trader._errors[i])
                        except OSError as e:
                            bg_logger.error(f"Failed to write error to history: {e}")
                    last_error_count = current_error_count

                # Update state
                state.tick_count = trader._tick_count
                state.trades = trader._trades
                state.errors = trader._errors
                state.current_balances = dict(portfolio_tracker.current_balances)
                state.equity_curve = [
                    (p.timestamp, p.value_usd, getattr(p, "eth_price_usd", None)) for p in trader._equity_curve
                ]

                # Periodic save of full state
                current_time = time.time()
                if current_time - last_save_time >= save_interval_seconds:
                    state.save(state_file)
                    last_save_time = current_time
                    bg_logger.debug(f"Saved state: tick={state.tick_count}, trades={len(state.trades)}")

                # Sleep until next tick
                await asyncio.sleep(config.tick_interval_seconds)

        except asyncio.CancelledError:
            bg_logger.info("Trading loop cancelled")
        except Exception as e:
            bg_logger.exception(f"Trading loop error: {e}")
            state.status = "error"
        finally:
            # Final cleanup
            trader._running = False
            await trader._cleanup()

            # Save final state
            if not shutdown_requested:
                state.status = "completed"
            else:
                state.status = "stopped"
            state.save(state_file)

            bg_logger.info(
                f"Trading session ended: {state.tick_count} ticks, "
                f"{len(state.trades)} trades, resume_count={state.resume_count}"
            )

    # Run the async loop
    try:
        asyncio.run(run_paper_trader())
    except KeyboardInterrupt:
        bg_logger.info("Interrupted by user")
    finally:
        cleanup()


__all__ = [
    "PIDFile",
    "TradeHistoryWriter",
    "PaperTraderState",
    "BackgroundStatus",
    "BackgroundPaperTrader",
]
