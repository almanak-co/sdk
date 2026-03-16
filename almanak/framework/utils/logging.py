"""Structured logging configuration for the Almanak Strategy Framework.

This module provides structured logging via structlog with:
- JSON output for production (machine-readable)
- ConsoleRenderer for local development (human-readable)
- Standard fields: timestamp, level, logger, message
- Context fields: strategy_id, chain, tx_hash, correlation_id

Usage:
    # Configure logging at application startup
    from almanak.framework.utils.logging import configure_logging, get_logger, LogLevel, LogFormat

    # For local development (human-readable)
    configure_logging(level=LogLevel.DEBUG, format=LogFormat.CONSOLE)

    # For production (JSON output)
    configure_logging(level=LogLevel.INFO, format=LogFormat.JSON)

    # Get a logger in any module
    logger = get_logger(__name__)

    # Log with context
    logger.info("Processing intent", intent_type="swap", token_in="WETH")

    # Add persistent context for a correlation_id
    from almanak.framework.utils.logging import add_context
    add_context(correlation_id="abc-123", strategy_id="momentum_v1")

Example:
    >>> configure_logging(level=LogLevel.INFO, format=LogFormat.JSON)
    >>> logger = get_logger("my_module")
    >>> logger.info("transaction_confirmed", tx_hash="0x123...", gas_used=21000)
    {"timestamp": "2025-01-15T10:00:00Z", "level": "info", "logger": "my_module",
     "message": "transaction_confirmed", "tx_hash": "0x123...", "gas_used": 21000}
"""

import logging
import sys
from contextlib import AbstractContextManager
from contextvars import ContextVar
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any

import structlog
from structlog.types import EventDict, Processor, WrappedLogger


class LogLevel(StrEnum):
    """Log level enumeration."""

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class LogFormat(StrEnum):
    """Log format enumeration."""

    JSON = "json"
    CONSOLE = "console"


# Context variable for storing additional context
_log_context: ContextVar[dict[str, Any] | None] = ContextVar("log_context", default=None)

# Track if logging has been configured
_logging_configured: bool = False


def _add_timestamp(logger: WrappedLogger, method_name: str, event_dict: EventDict) -> EventDict:
    """Add ISO 8601 timestamp to log event.

    Args:
        logger: The wrapped logger instance
        method_name: The logging method name (info, error, etc.)
        event_dict: The event dictionary

    Returns:
        Event dictionary with timestamp added
    """
    event_dict["timestamp"] = datetime.now(UTC).isoformat()
    return event_dict


def _add_log_level(logger: WrappedLogger, method_name: str, event_dict: EventDict) -> EventDict:
    """Add log level to event dictionary.

    Args:
        logger: The wrapped logger instance
        method_name: The logging method name (info, error, etc.)
        event_dict: The event dictionary

    Returns:
        Event dictionary with level added
    """
    event_dict["level"] = method_name
    return event_dict


def _add_context_vars(logger: WrappedLogger, method_name: str, event_dict: EventDict) -> EventDict:
    """Add context variables to log event.

    Args:
        logger: The wrapped logger instance
        method_name: The logging method name (info, error, etc.)
        event_dict: The event dictionary

    Returns:
        Event dictionary with context variables added
    """
    ctx = _log_context.get()
    if ctx:
        # Context vars have lower precedence than explicit kwargs
        for key, value in ctx.items():
            if key not in event_dict:
                event_dict[key] = value
    return event_dict


def _rename_event_to_message(logger: WrappedLogger, method_name: str, event_dict: EventDict) -> EventDict:
    """Rename 'event' key to 'message' for clarity.

    Args:
        logger: The wrapped logger instance
        method_name: The logging method name (info, error, etc.)
        event_dict: The event dictionary

    Returns:
        Event dictionary with event renamed to message
    """
    if "event" in event_dict:
        event_dict["message"] = event_dict.pop("event")
    return event_dict


def _order_keys(logger: WrappedLogger, method_name: str, event_dict: EventDict) -> EventDict:
    """Order keys for consistent output.

    Standard order: timestamp, level, logger, message, then alphabetical extras.

    Args:
        logger: The wrapped logger instance
        method_name: The logging method name (info, error, etc.)
        event_dict: The event dictionary

    Returns:
        Ordered event dictionary
    """
    ordered: dict[str, Any] = {}
    standard_keys = ["timestamp", "level", "logger", "message"]

    # Add standard keys first in order
    for key in standard_keys:
        if key in event_dict:
            ordered[key] = event_dict[key]

    # Add remaining keys alphabetically
    for key in sorted(event_dict.keys()):
        if key not in standard_keys:
            ordered[key] = event_dict[key]

    return ordered


def _load_plugin_processors() -> list[Processor]:
    """Discover extra structlog processors from platform plugins.

    Loads ALL ``almanak.logging`` entry points named ``processors`` and
    merges their results.  Returns an empty list when no plugin is
    installed.  Logs a warning on load failure so operators notice when
    the severity mapping is silently inactive.
    """
    from importlib.metadata import entry_points

    eps = entry_points(group="almanak.logging", name="processors")
    processors: list[Processor] = []
    for ep in eps:
        try:
            get_processors = ep.load()
            processors.extend(get_processors())
        except Exception as exc:
            logging.getLogger(__name__).warning("Failed to load almanak.logging plugin %s: %s", ep.value, exc)
    return processors


def _get_shared_processors() -> list[Processor]:
    """Get processors shared between stdlib and structlog.

    Returns:
        List of shared processors
    """
    return [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        _add_context_vars,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.UnicodeDecoder(),
        *_load_plugin_processors(),
    ]


def configure_logging(
    level: LogLevel = LogLevel.INFO,
    format: LogFormat = LogFormat.JSON,
    stream: Any | None = None,
) -> None:
    """Configure structured logging for the application.

    This should be called once at application startup. Subsequent calls
    will reconfigure logging (useful for testing).

    Args:
        level: The minimum log level to output
        format: The output format (JSON for production, CONSOLE for development)
        stream: Output stream (defaults to sys.stdout)

    Example:
        # Local development
        configure_logging(level=LogLevel.DEBUG, format=LogFormat.CONSOLE)

        # Production
        configure_logging(level=LogLevel.INFO, format=LogFormat.JSON)
    """
    global _logging_configured

    if stream is None:
        stream = sys.stdout

    # Convert LogLevel enum to logging constant
    log_level = getattr(logging, level.value)

    # Shared processors for both stdlib and structlog
    shared_processors = _get_shared_processors()

    if format == LogFormat.JSON:
        # JSON renderer for production
        renderer: Processor = structlog.processors.JSONRenderer()
    else:
        # Console renderer for development
        use_colors = hasattr(stream, "isatty") and stream.isatty()
        renderer = structlog.dev.ConsoleRenderer(
            colors=use_colors,
            exception_formatter=structlog.dev.plain_traceback,
        )

    # Configure structlog
    structlog.configure(
        processors=shared_processors
        + [
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    # Configure stdlib logging to use structlog formatting
    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=shared_processors,
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            renderer,
        ],
    )

    handler = logging.StreamHandler(stream)
    handler.setFormatter(formatter)

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.setLevel(log_level)

    # Reduce noise from third-party loggers
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)
    logging.getLogger("aiohttp").setLevel(logging.WARNING)
    logging.getLogger("web3").setLevel(logging.WARNING)

    _logging_configured = True


def add_file_handler(
    log_file: str,
    level: LogLevel = LogLevel.DEBUG,
) -> None:
    """Add a JSON file handler to the root logger.

    Writes machine-readable JSON logs to the specified file, suitable for
    post-hoc analysis by AI agents or log aggregation systems. This is
    additive -- the existing console handler is preserved.

    Args:
        log_file: Path to the log file (created if it doesn't exist)
        level: Minimum log level for the file handler (default: DEBUG)
    """
    shared_processors = _get_shared_processors()

    json_formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=shared_processors,
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            structlog.processors.JSONRenderer(),
        ],
    )

    Path(log_file).parent.mkdir(parents=True, exist_ok=True)
    handler = logging.FileHandler(log_file, mode="a", encoding="utf-8")
    handler.setFormatter(json_formatter)
    handler.setLevel(getattr(logging, level.value))

    logging.getLogger().addHandler(handler)


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """Get a structured logger for the given module name.

    This returns a structlog BoundLogger that wraps the stdlib logger.
    If logging hasn't been configured, it will use sensible defaults.

    Args:
        name: The logger name (typically __name__)

    Returns:
        A structured logger instance

    Example:
        logger = get_logger(__name__)
        logger.info("processing_started", strategy_id="momentum_v1")
    """
    if not _logging_configured:
        # Configure with defaults if not yet configured
        configure_logging()

    return structlog.get_logger(name)


def add_context(**kwargs: Any) -> None:
    """Add persistent context to all subsequent log messages.

    Context is stored in a context variable and automatically added
    to all log messages. Use this for values like correlation_id or
    strategy_id that should appear in all logs.

    Args:
        **kwargs: Key-value pairs to add to context

    Example:
        add_context(correlation_id="abc-123", strategy_id="momentum_v1")
        logger.info("processing")  # Will include correlation_id and strategy_id
    """
    structlog.contextvars.bind_contextvars(**kwargs)


def clear_context() -> None:
    """Clear all context variables.

    Call this to reset the logging context, typically at the start
    of a new request or iteration.

    Example:
        clear_context()
        add_context(correlation_id="new-123")
    """
    structlog.contextvars.clear_contextvars()


def bind_context(**kwargs: Any) -> None:
    """Alias for add_context for compatibility.

    Args:
        **kwargs: Key-value pairs to add to context
    """
    add_context(**kwargs)


def unbind_context(*keys: str) -> None:
    """Remove specific keys from the logging context.

    Args:
        *keys: Keys to remove from context

    Example:
        unbind_context("tx_hash", "nonce")
    """
    structlog.contextvars.unbind_contextvars(*keys)


def with_context(**kwargs: Any) -> AbstractContextManager[None]:
    """Context manager for temporary logging context.

    Adds context for the duration of a block, then removes it.

    Args:
        **kwargs: Key-value pairs to add temporarily

    Example:
        with with_context(tx_hash="0x123"):
            logger.info("signing")  # Includes tx_hash
            logger.info("submitting")  # Includes tx_hash
        logger.info("done")  # Does not include tx_hash

    Returns:
        Context manager
    """
    return structlog.contextvars.tmp_bind_contextvars(**kwargs)  # type: ignore[attr-defined]


# Convenience function for getting a stdlib logger with structlog formatting
def get_stdlib_logger(name: str) -> logging.Logger:
    """Get a standard library logger.

    This is useful when interfacing with code that expects a stdlib logger
    but you want structlog formatting. The returned logger will use
    structlog's formatting when logging is configured.

    Args:
        name: The logger name (typically __name__)

    Returns:
        A standard library Logger instance

    Example:
        logger = get_stdlib_logger(__name__)
        logger.info("legacy code", extra={"key": "value"})
    """
    if not _logging_configured:
        configure_logging()

    return logging.getLogger(name)
