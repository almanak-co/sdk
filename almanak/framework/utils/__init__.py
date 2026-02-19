"""Utility modules for the Almanak Strategy Framework.

This package provides common utilities used throughout the framework:
- logging: Structured logging configuration with structlog
- retry: Retry decorators with exponential backoff for external API calls
"""

from .logging import (
    LogFormat,
    LogLevel,
    add_context,
    clear_context,
    configure_logging,
    get_logger,
)
from .retry import (
    DEFAULT_RETRY_CONFIG,
    RetryConfig,
    RetryContext,
    calculate_backoff_delay,
    retry_with_backoff,
)

__all__ = [
    # Logging
    "configure_logging",
    "get_logger",
    "add_context",
    "clear_context",
    "LogLevel",
    "LogFormat",
    # Retry
    "RetryConfig",
    "RetryContext",
    "calculate_backoff_delay",
    "retry_with_backoff",
    "DEFAULT_RETRY_CONFIG",
]
