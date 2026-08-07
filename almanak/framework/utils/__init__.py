"""Utility modules for the Almanak Strategy Framework.

This package provides common utilities used throughout the framework:
- logging: Structured logging configuration with structlog
- retry: DEPRECATED (removal in 3.0.0) — see :mod:`almanak.framework.utils.retry`
"""

from typing import TYPE_CHECKING, Any

from .logging import (
    LogFormat,
    LogLevel,
    add_context,
    clear_context,
    configure_logging,
    get_logger,
)

if TYPE_CHECKING:
    from .retry import (  # noqa: F401
        DEFAULT_RETRY_CONFIG,
        RetryConfig,
        RetryContext,
        calculate_backoff_delay,
        retry_with_backoff,
    )

# Deprecated names still re-exported from this package. They have no callers
# inside the SDK, but `.syncinclude` publishes `almanak/` wholesale, so they are
# importable public surface on the released package (2.24.0) and cannot vanish
# without a deprecation cycle. Resolving one imports `.retry` lazily, which
# emits a one-shot DeprecationWarning. Removal target: 3.0.0 (ALM-3197 owns the
# unified retry home that replaces them).
_DEPRECATED_RETRY_NAMES = frozenset(
    {
        "DEFAULT_RETRY_CONFIG",
        "RetryConfig",
        "RetryContext",
        "calculate_backoff_delay",
        "retry_with_backoff",
    }
)


def __getattr__(name: str) -> Any:
    """Resolve deprecated retry names lazily (PEP 562)."""
    if name in _DEPRECATED_RETRY_NAMES:
        # Lazy by design: importing `.retry` is what emits the warning. The
        # explicit call covers the case where something already imported the
        # module, which would otherwise resolve the name silently. It is
        # one-shot, so the two paths never double-warn.
        from . import retry

        retry._emit_deprecation()
        return getattr(retry, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    """Include the lazily-resolved deprecated names in ``dir()``."""
    return sorted(set(globals()) | _DEPRECATED_RETRY_NAMES)


__all__ = [
    # Logging
    "configure_logging",
    "get_logger",
    "add_context",
    "clear_context",
    "LogLevel",
    "LogFormat",
    # Retry — DEPRECATED, removal in 3.0.0
    "RetryConfig",
    "RetryContext",
    "calculate_backoff_delay",
    "retry_with_backoff",
    "DEFAULT_RETRY_CONFIG",
]
