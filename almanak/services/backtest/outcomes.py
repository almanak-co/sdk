"""Shared outcome handling for backtest service boundaries.

The PnL engine reports fatal simulation failures as a returned
``BacktestResult``. Callers must therefore inspect ``result.success`` instead
of treating a normal coroutine return as successful completion.
"""

from __future__ import annotations

import re
from collections.abc import Callable

from almanak.framework.backtesting.models import BacktestResult

DEFAULT_BACKTEST_FAILURE_MESSAGE = "Backtest engine returned a failed result."
MAX_BACKTEST_FAILURE_MESSAGE_LENGTH = 5000

_URL_CREDENTIAL_RE = re.compile(r"(https?://)[^@/\s]+@", re.IGNORECASE)
_AUTH_TOKEN_RE = re.compile(r"(x-access-token:)[^@/\s]+(@)", re.IGNORECASE)
_QUERY_CREDENTIAL_RE = re.compile(r"([?&](?:api_key|access_token)=)([^&#\s]+)", re.IGNORECASE)
_BEARER_CREDENTIAL_RE = re.compile(r"(\bauthorization\s*:\s*bearer\s+)([^\s,;]+)", re.IGNORECASE)


def redact_backtest_error_message(message: str) -> str:
    """Remove credentials commonly embedded in provider and clone URLs."""
    text = _URL_CREDENTIAL_RE.sub(r"\1***@", message)
    text = _AUTH_TOKEN_RE.sub(r"\1***\2", text)
    text = _QUERY_CREDENTIAL_RE.sub(r"\1***", text)
    return _BEARER_CREDENTIAL_RE.sub(r"\1***", text)


def backtest_failure_message(
    result: BacktestResult,
    *,
    sanitize: Callable[[str], str] = redact_backtest_error_message,
) -> str | None:
    """Return a bounded public failure message, or ``None`` for success.

    ``BacktestResult.success`` deliberately checks ``error is None``. An empty
    error string is still a failed result and receives a stable fallback.
    Sanitization happens before truncation so a credential at the message
    boundary cannot be exposed by cutting its URL before redaction.
    """
    if result.success:
        return None

    raw_error = str(result.error).strip() if result.error is not None else ""
    message = raw_error or DEFAULT_BACKTEST_FAILURE_MESSAGE
    return sanitize(message)[:MAX_BACKTEST_FAILURE_MESSAGE_LENGTH]
