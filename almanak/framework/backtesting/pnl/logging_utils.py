"""Logging utilities for PnL backtesting with JSON format and phase timing.

This module provides structured logging utilities for backtesting:
    - JSONLogFormatter: JSON-formatted log output for machine parsing
    - PhaseTimer: Context manager for tracking phase execution timing
    - BacktestLogger: Enhanced logger with phase timing and JSON support

Example:
    from almanak.framework.backtesting.pnl.logging_utils import (
        JSONLogFormatter,
        PhaseTimer,
        BacktestLogger,
    )

    # Use JSON formatter
    handler = logging.StreamHandler()
    handler.setFormatter(JSONLogFormatter())
    logger.addHandler(handler)

    # Use phase timer
    with PhaseTimer("data_loading", backtest_id="abc-123") as timer:
        data = load_data()
    print(f"Duration: {timer.duration_seconds}s")

    # Use backtest logger
    bt_logger = BacktestLogger(backtest_id="abc-123", json_format=True)
    with bt_logger.phase("initialization"):
        initialize_portfolio()
"""

import json
import logging
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any


class JSONLogFormatter(logging.Formatter):
    """JSON formatter for structured log output.

    Produces machine-parseable JSON log lines with consistent fields:
    - timestamp: ISO format timestamp
    - level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    - logger: Logger name
    - message: Log message
    - backtest_id: Correlation ID (if present in record)
    - phase: Current phase name (if present in record)
    - duration_seconds: Phase duration (if present in record)
    - extra: Any additional fields

    Example output:
        {"timestamp": "2024-01-15T10:30:00Z", "level": "INFO", "message": "Starting backtest"}
    """

    def format(self, record: logging.LogRecord) -> str:
        """Format a log record as JSON.

        Args:
            record: Log record to format

        Returns:
            JSON-formatted log line
        """
        log_data: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Add backtest_id if present
        if hasattr(record, "backtest_id"):
            log_data["backtest_id"] = record.backtest_id

        # Add phase info if present
        if hasattr(record, "phase"):
            log_data["phase"] = record.phase

        # Add duration if present
        if hasattr(record, "duration_seconds"):
            log_data["duration_seconds"] = record.duration_seconds

        # Add any extra fields from the record
        if hasattr(record, "extra") and record.extra:
            log_data["extra"] = record.extra

        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_data)


@dataclass
class PhaseTimer:
    """Context manager for timing backtest phases.

    Tracks start/end time of a named phase and calculates duration.
    Can log phase timing automatically when used with a logger.

    Attributes:
        phase_name: Name of the phase being timed
        backtest_id: Correlation ID for the backtest run
        logger: Optional logger to emit timing logs
        json_format: Whether to emit JSON-formatted logs
        start_time: When the phase started (set on enter)
        end_time: When the phase ended (set on exit)
        duration_seconds: Phase duration in seconds (set on exit)

    Example:
        with PhaseTimer("data_loading", backtest_id="abc-123", logger=logger) as timer:
            data = load_historical_data()
        # Logs: "Phase 'data_loading' completed in 5.234s"
    """

    phase_name: str
    backtest_id: str = ""
    logger: logging.Logger | None = None
    json_format: bool = False
    start_time: float = field(default=0.0, init=False)
    end_time: float = field(default=0.0, init=False)
    duration_seconds: float = field(default=0.0, init=False)
    _start_datetime: datetime = field(default_factory=lambda: datetime.now(UTC), init=False)

    def __enter__(self) -> "PhaseTimer":
        """Start timing the phase."""
        self.start_time = time.perf_counter()
        self._start_datetime = datetime.now(UTC)

        if self.logger:
            self._log_phase_start()

        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """End timing and log the result."""
        self.end_time = time.perf_counter()
        self.duration_seconds = self.end_time - self.start_time

        if self.logger:
            self._log_phase_end(error=exc_val if exc_type else None)

    def _log_phase_start(self) -> None:
        """Log the start of a phase."""
        if not self.logger:
            return

        if self.json_format:
            # Use extra dict for structured data
            self.logger.info(
                f"Phase '{self.phase_name}' started",
                extra={
                    "backtest_id": self.backtest_id,
                    "phase": self.phase_name,
                    "phase_event": "start",
                },
            )
        else:
            prefix = f"[{self.backtest_id}] " if self.backtest_id else ""
            self.logger.info(f"{prefix}Phase '{self.phase_name}' started")

    def _log_phase_end(self, error: Exception | None = None) -> None:
        """Log the end of a phase with duration."""
        if not self.logger:
            return

        if self.json_format:
            extra_data: dict[str, Any] = {
                "backtest_id": self.backtest_id,
                "phase": self.phase_name,
                "phase_event": "end",
                "duration_seconds": round(self.duration_seconds, 4),
            }
            if error:
                extra_data["error"] = str(error)
                self.logger.error(
                    f"Phase '{self.phase_name}' failed after {self.duration_seconds:.4f}s",
                    extra=extra_data,
                )
            else:
                self.logger.info(
                    f"Phase '{self.phase_name}' completed in {self.duration_seconds:.4f}s",
                    extra=extra_data,
                )
        else:
            prefix = f"[{self.backtest_id}] " if self.backtest_id else ""
            if error:
                self.logger.error(
                    f"{prefix}Phase '{self.phase_name}' failed after {self.duration_seconds:.4f}s: {error}"
                )
            else:
                self.logger.info(f"{prefix}Phase '{self.phase_name}' completed in {self.duration_seconds:.4f}s")


@dataclass
class PhaseTiming:
    """Record of a completed phase's timing.

    Attributes:
        phase_name: Name of the phase
        start_time: When the phase started
        end_time: When the phase ended
        duration_seconds: How long the phase took
        error: Any error that occurred during the phase
    """

    phase_name: str
    start_time: datetime
    end_time: datetime
    duration_seconds: float
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization.

        Returns:
            Dict with all timing fields
        """
        return {
            "phase_name": self.phase_name,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat(),
            "duration_seconds": round(self.duration_seconds, 4),
            "error": self.error,
        }


@dataclass
class BacktestLogger:
    """Enhanced logger for backtest operations with phase timing.

    Provides structured logging with:
    - Automatic backtest_id correlation
    - Phase timing via context manager
    - Optional JSON formatting
    - Phase timing history

    Attributes:
        backtest_id: Correlation ID for all log messages
        json_format: Whether to use JSON log format
        logger: Underlying Python logger
        phase_timings: List of completed phase timings

    Example:
        bt_logger = BacktestLogger(backtest_id="abc-123", json_format=True)

        with bt_logger.phase("initialization"):
            portfolio = initialize_portfolio()

        with bt_logger.phase("simulation"):
            run_simulation()

        # Access timing history
        for timing in bt_logger.phase_timings:
            print(f"{timing.phase_name}: {timing.duration_seconds}s")
    """

    backtest_id: str
    json_format: bool = False
    logger: logging.Logger = field(default_factory=lambda: logging.getLogger(__name__))
    phase_timings: list[PhaseTiming] = field(default_factory=list)
    _current_phase: PhaseTimer | None = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        """Configure logger with JSON formatter if requested."""
        if self.json_format:
            # Check if handler already has JSON formatter
            for handler in self.logger.handlers:
                if isinstance(handler.formatter, JSONLogFormatter):
                    return

            # Only configure root logger's handlers if this logger uses them
            # Don't modify other loggers' handlers
            pass

    def phase(self, phase_name: str) -> PhaseTimer:
        """Create a phase timer context manager.

        Args:
            phase_name: Name of the phase to time

        Returns:
            PhaseTimer context manager

        Example:
            with bt_logger.phase("data_loading"):
                load_data()
        """
        timer = PhaseTimer(
            phase_name=phase_name,
            backtest_id=self.backtest_id,
            logger=self.logger,
            json_format=self.json_format,
        )
        self._current_phase = timer
        return _PhaseTimerWithCallback(timer, self._on_phase_complete)

    def _on_phase_complete(self, timer: PhaseTimer, error: Exception | None) -> None:
        """Record completed phase timing."""
        timing = PhaseTiming(
            phase_name=timer.phase_name,
            start_time=timer._start_datetime,
            end_time=datetime.now(UTC),
            duration_seconds=timer.duration_seconds,
            error=str(error) if error else None,
        )
        self.phase_timings.append(timing)
        self._current_phase = None

    def info(self, message: str, **extra: Any) -> None:
        """Log an info message with backtest_id.

        Args:
            message: Message to log
            **extra: Additional fields to include
        """
        self._log(logging.INFO, message, extra)

    def debug(self, message: str, **extra: Any) -> None:
        """Log a debug message with backtest_id.

        Args:
            message: Message to log
            **extra: Additional fields to include
        """
        self._log(logging.DEBUG, message, extra)

    def warning(self, message: str, **extra: Any) -> None:
        """Log a warning message with backtest_id.

        Args:
            message: Message to log
            **extra: Additional fields to include
        """
        self._log(logging.WARNING, message, extra)

    def error(self, message: str, **extra: Any) -> None:
        """Log an error message with backtest_id.

        Args:
            message: Message to log
            **extra: Additional fields to include
        """
        self._log(logging.ERROR, message, extra)

    def _log(self, level: int, message: str, extra: dict[str, Any]) -> None:
        """Internal logging with structured data.

        Args:
            level: Log level
            message: Message to log
            extra: Additional fields
        """
        if self.json_format:
            # For JSON format, pass extra data through record
            log_extra = {
                "backtest_id": self.backtest_id,
                "extra": extra if extra else None,
            }
            self.logger.log(level, message, extra=log_extra)
        else:
            # For text format, include backtest_id in message
            prefix = f"[{self.backtest_id}] " if self.backtest_id else ""
            self.logger.log(level, f"{prefix}{message}")

    def get_phase_summary(self) -> dict[str, Any]:
        """Get a summary of all phase timings.

        Returns:
            Dict with total duration and individual phase timings
        """
        total_duration = sum(t.duration_seconds for t in self.phase_timings)
        return {
            "backtest_id": self.backtest_id,
            "total_duration_seconds": round(total_duration, 4),
            "phases": [t.to_dict() for t in self.phase_timings],
        }


class _PhaseTimerWithCallback(PhaseTimer):
    """Internal wrapper that calls back when phase completes."""

    def __init__(
        self,
        timer: PhaseTimer,
        callback: Any,
    ) -> None:
        """Initialize with wrapped timer and callback.

        Args:
            timer: PhaseTimer to wrap
            callback: Function to call on phase completion
        """
        # Copy all fields from the timer
        self.phase_name = timer.phase_name
        self.backtest_id = timer.backtest_id
        self.logger = timer.logger
        self.json_format = timer.json_format
        self.start_time = timer.start_time
        self.end_time = timer.end_time
        self.duration_seconds = timer.duration_seconds
        self._start_datetime = timer._start_datetime
        self._callback = callback
        self._wrapped_timer = timer
        self._error: Exception | None = None

    def __enter__(self) -> "PhaseTimer":
        """Start timing."""
        self._wrapped_timer.__enter__()
        # Copy start time from wrapped timer
        self.start_time = self._wrapped_timer.start_time
        self._start_datetime = self._wrapped_timer._start_datetime
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """End timing and call back."""
        self._wrapped_timer.__exit__(exc_type, exc_val, exc_tb)
        # Copy end time and duration from wrapped timer
        self.end_time = self._wrapped_timer.end_time
        self.duration_seconds = self._wrapped_timer.duration_seconds
        self._error = exc_val if exc_type else None
        self._callback(self._wrapped_timer, self._error)


def configure_json_logging(
    logger: logging.Logger | None = None,
    level: int = logging.INFO,
) -> logging.Handler:
    """Configure a logger to use JSON formatting.

    Creates a StreamHandler with JSONLogFormatter and adds it to the logger.

    Args:
        logger: Logger to configure (defaults to root logger)
        level: Log level for the handler

    Returns:
        The configured handler

    Example:
        configure_json_logging(logging.getLogger("almanak.framework.backtesting"))
    """
    if logger is None:
        logger = logging.getLogger()

    handler = logging.StreamHandler()
    handler.setFormatter(JSONLogFormatter())
    handler.setLevel(level)
    logger.addHandler(handler)

    return handler


def configure_backtest_logging(
    verbose: bool = False,
    json_format: bool = False,
) -> None:
    """Configure logging for backtesting with appropriate levels.

    Sets up the logging configuration for backtest runs:
    - In verbose mode (--verbose flag): DEBUG level for detailed trade logs
    - In normal mode: INFO level and above only

    This should be called at the start of a backtest CLI command.

    Args:
        verbose: If True, enable DEBUG level logging for detailed output
        json_format: If True, use JSON formatter for machine-parseable output

    Example:
        # At start of CLI command
        configure_backtest_logging(verbose=ctx.verbose)

        # Then run backtest - all trade execution will be logged at DEBUG level
        result = await backtester.backtest(strategy, config)
    """
    # Get the backtesting logger
    bt_logger = logging.getLogger("almanak.framework.backtesting")

    # Set level based on verbose flag
    level = logging.DEBUG if verbose else logging.INFO
    bt_logger.setLevel(level)

    # Also set on PnL engine specifically
    pnl_logger = logging.getLogger("almanak.framework.backtesting.pnl.engine")
    pnl_logger.setLevel(level)

    # Check if handler already exists
    if not bt_logger.handlers:
        handler = logging.StreamHandler()

        if json_format:
            handler.setFormatter(JSONLogFormatter())
        else:
            # Use a simple format for text output
            formatter = logging.Formatter(
                "%(asctime)s [%(levelname)s] %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
            handler.setFormatter(formatter)

        handler.setLevel(level)
        bt_logger.addHandler(handler)

    # Update existing handlers' levels
    for existing_handler in bt_logger.handlers:
        existing_handler.setLevel(level)


def log_trade_execution(
    logger: logging.Logger,
    backtest_id: str,
    timestamp: datetime,
    intent_type: str,
    protocol: str,
    tokens: list[str],
    amount_usd: Any,
    fee_usd: Any,
    slippage_usd: Any,
    gas_cost_usd: Any,
    executed_price: Any | None = None,
    mev_cost_usd: Any | None = None,
    json_format: bool = False,
) -> None:
    """Log detailed trade execution information.

    This function provides structured logging for trade executions,
    outputting detailed information at DEBUG level for verbose mode.

    Args:
        logger: Logger to use
        backtest_id: Correlation ID for the backtest run
        timestamp: Time of trade execution
        intent_type: Type of intent (SWAP, LP_OPEN, etc.)
        protocol: Protocol used (uniswap_v3, aave_v3, etc.)
        tokens: List of tokens involved
        amount_usd: Trade amount in USD
        fee_usd: Fee paid in USD
        slippage_usd: Slippage cost in USD
        gas_cost_usd: Gas cost in USD
        executed_price: Price at which trade was executed
        mev_cost_usd: Optional MEV cost if simulation enabled
        json_format: If True, log as JSON for machine parsing
    """
    if json_format:
        log_data = {
            "event": "trade_execution",
            "backtest_id": backtest_id,
            "timestamp": timestamp.isoformat() if timestamp else None,
            "intent_type": intent_type,
            "protocol": protocol,
            "tokens": tokens,
            "amount_usd": str(amount_usd),
            "fee_usd": str(fee_usd),
            "slippage_usd": str(slippage_usd),
            "gas_cost_usd": str(gas_cost_usd),
            "executed_price": str(executed_price) if executed_price else None,
            "mev_cost_usd": str(mev_cost_usd) if mev_cost_usd else None,
        }
        logger.debug(json.dumps(log_data))
    else:
        # Format tokens for display
        tokens_str = " -> ".join(tokens) if tokens else "N/A"

        # Build the log message
        msg_parts = [
            f"[{backtest_id[:8]}]" if backtest_id else "",
            f"TRADE at {timestamp.strftime('%Y-%m-%d %H:%M:%S') if timestamp else 'N/A'}:",
            f"type={intent_type}",
            f"protocol={protocol or 'default'}",
            f"tokens={tokens_str}",
            f"amount=${float(amount_usd):,.2f}",
            f"fee=${float(fee_usd):,.2f}",
            f"slippage=${float(slippage_usd):,.2f}",
            f"gas=${float(gas_cost_usd):,.2f}",
        ]

        if executed_price:
            msg_parts.append(f"price={float(executed_price):,.6f}")

        if mev_cost_usd:
            msg_parts.append(f"mev=${float(mev_cost_usd):,.2f}")

        logger.debug(" ".join(msg_parts))
