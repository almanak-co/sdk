"""Centralized secret redaction for all logging and output.

Provides zero-touch redaction of sensitive values (API keys, private keys, tokens)
across all logging channels. Once installed via ``install_redaction()``, all log
output is automatically filtered -- no per-call-site changes needed.

Usage::

    from almanak.core.redaction import install_redaction

    # Call once at startup (CLI entry, gateway boot, strategy runner init)
    install_redaction()

Partial reveal format: first 2 + last 2 characters shown.
    ``QuiTw3JuH0VUc8CpUmacvhSIFIsSHuQZ`` -> ``Qu***QZ``
    ``0xabcdef1234567890`` -> ``0x***90``
    Secrets <= 4 chars -> ``***``
"""

from __future__ import annotations

import logging
import os
import re
from typing import TextIO

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Env var names whose values are always treated as secrets.
_EXPLICIT_SECRET_VARS: set[str] = {
    "ALCHEMY_API_KEY",
    "ALMANAK_API_KEY",
    "ALMANAK_PRIVATE_KEY",
    "ENSO_API_KEY",
}

# Suffix patterns -- any env var ending with these is treated as a secret.
_SECRET_SUFFIXES: tuple[str, ...] = (
    "_KEY",
    "_SECRET",
    "_TOKEN",
    "_PASSWORD",
    "_PRIVATE_KEY",
)

# Minimum secret value length to avoid false positives (e.g. ``HOME=/Users``).
_MIN_SECRET_LENGTH = 6

# Values that look like secrets but are benign config.
_IGNORE_VALUES: frozenset[str] = frozenset(
    {
        "true",
        "false",
        "1",
        "0",
        "yes",
        "no",
        "none",
        "null",
        "localhost",
        "mainnet",
        "anvil",
        "sepolia",
        "prod",
        "stage",
        "development",
        "production",
    }
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Core redaction engine
# ---------------------------------------------------------------------------

# Compiled regex built lazily on first ``redact()`` call and rebuilt on
# ``install_redaction()`` to pick up env changes.
_secret_pattern: re.Pattern[str] | None = None
# Map of compiled patterns to their partial-reveal replacements.
_secret_replacements: list[tuple[re.Pattern[str], str]] = []


def _partial_reveal(value: str) -> str:
    """Return a partially-revealed version of *value*.

    Shows first 2 + last 2 characters with ``***`` in between.
    Values <= 4 chars are fully redacted to ``***``.
    """
    if len(value) <= 4:
        return "***"
    return f"{value[:2]}***{value[-2:]}"


def _collect_secrets() -> list[tuple[str, str]]:
    """Collect (raw_value, replacement) pairs from current environment."""
    secrets: list[tuple[str, str]] = []
    seen_values: set[str] = set()

    for name, value in os.environ.items():
        if not value or len(value) < _MIN_SECRET_LENGTH:
            continue
        if value.lower() in _IGNORE_VALUES:
            continue

        is_secret = name in _EXPLICIT_SECRET_VARS or any(name.endswith(suffix) for suffix in _SECRET_SUFFIXES)
        if not is_secret:
            continue
        if value in seen_values:
            continue
        seen_values.add(value)
        secrets.append((value, _partial_reveal(value)))

    # Sort longest-first so longer secrets are matched before substrings.
    secrets.sort(key=lambda pair: len(pair[0]), reverse=True)
    return secrets


def _build_patterns(secrets: list[tuple[str, str]]) -> tuple[re.Pattern[str] | None, list[tuple[re.Pattern[str], str]]]:
    """Build regex patterns for all collected secrets."""
    if not secrets:
        return None, []

    replacements: list[tuple[re.Pattern[str], str]] = []
    for raw_value, replacement in secrets:
        try:
            pattern = re.compile(re.escape(raw_value))
            replacements.append((pattern, replacement))
        except re.error:
            logger.warning("Failed to compile regex for a secret value, skipping.", exc_info=True)
            continue

    # Also build a single combined pattern for fast "any match?" checks.
    escaped = [re.escape(s[0]) for s in secrets]
    try:
        combined = re.compile("|".join(escaped))
    except re.error:
        logger.warning("Failed to compile combined secret pattern. Fast-path check disabled.", exc_info=True)
        combined = None

    return combined, replacements


def _rebuild() -> None:
    """Rebuild the internal secret patterns from the current environment."""
    global _secret_pattern, _secret_replacements
    secrets = _collect_secrets()
    _secret_pattern, _secret_replacements = _build_patterns(secrets)


def redact(message: str) -> str:
    """Replace all known secret values in *message* with partial reveals.

    This is the core redaction function. It scans *message* for any env var
    value that matches a secret pattern and replaces it.

    Thread-safe: reads from module-level compiled patterns (immutable after
    ``install_redaction()``).
    """
    if not _secret_replacements:
        return message
    # Fast path: if no secret substring exists, skip replacement loop.
    if _secret_pattern and not _secret_pattern.search(message):
        return message
    result = message
    for pattern, replacement in _secret_replacements:
        result = pattern.sub(replacement, result)
    return result


# ---------------------------------------------------------------------------
# URL masking (replaces 6+ scattered helpers)
# ---------------------------------------------------------------------------


def mask_url(url: str | None) -> str | None:
    """Mask sensitive parts of a URL for safe logging.

    Handles common patterns:
    - Alchemy/Infura path keys: ``https://...alchemy.com/v2/KEY`` -> ``https://...alchemy.com/v2/***``
    - Query param keys: ``?api_key=KEY`` -> ``?api_key=***``
    - Credentials in URL: ``https://user:pass@host`` -> ``https://***@host``

    Also applies the general ``redact()`` pass to catch any remaining secrets.
    """
    if not url:
        return url

    masked = url

    # 1. Credentials in URL (user:pass@host)
    if "@" in masked:
        match = re.match(r"(https?://)([^@]+)@(.+)", masked)
        if match:
            masked = f"{match.group(1)}***@{match.group(3)}"

    # 2. API keys in query params
    masked = re.sub(
        r"(api[_-]?key|apikey|key|token|secret)=([^&]+)",
        r"\1=***",
        masked,
        flags=re.IGNORECASE,
    )

    # 3. Long path segments that look like API keys (20+ alphanumeric chars)
    masked = re.sub(r"/([a-zA-Z0-9_-]{20,})(/|$)", r"/***\2", masked)

    # 4. Apply general secret redaction
    masked = redact(masked)

    return masked


# ---------------------------------------------------------------------------
# Logging filter
# ---------------------------------------------------------------------------


class RedactionFilter(logging.Filter):
    """Logging filter that redacts secret values from all log records.

    Attach to the root logger to automatically redact secrets from all
    log output without any per-call-site changes::

        logging.getLogger().addFilter(RedactionFilter())
    """

    def filter(self, record: logging.LogRecord) -> bool:
        """Redact secrets from the log record message and args."""
        # Redact the formatted message
        if isinstance(record.msg, str):
            record.msg = redact(record.msg)

        # Redact string args (used in %-formatting)
        if record.args:
            if isinstance(record.args, dict):
                record.args = {k: redact(v) if isinstance(v, str) else v for k, v in record.args.items()}
            elif isinstance(record.args, tuple):
                record.args = tuple(redact(a) if isinstance(a, str) else a for a in record.args)

        return True  # Always allow the record through


# ---------------------------------------------------------------------------
# Stream wrapper (optional, for stdout/stderr)
# ---------------------------------------------------------------------------


class RedactingStream:
    """Wrapper around a text stream that redacts secrets on write.

    Usage::

        import sys
        sys.stdout = RedactingStream(sys.stdout)
    """

    def __init__(self, stream: TextIO) -> None:
        self._stream = stream

    def write(self, message: str) -> int:
        return self._stream.write(redact(message))

    def flush(self) -> None:
        self._stream.flush()

    def fileno(self) -> int:
        return self._stream.fileno()

    def isatty(self) -> bool:
        return self._stream.isatty()

    def __getattr__(self, name: str):
        """Delegate all other attributes to the wrapped stream."""
        return getattr(self._stream, name)


# ---------------------------------------------------------------------------
# Installation
# ---------------------------------------------------------------------------

_installed = False


def install_redaction() -> None:
    """Install centralized secret redaction on all logging channels.

    Call once at application startup (CLI entry, gateway boot, strategy runner).
    Safe to call multiple times -- subsequent calls rebuild patterns but don't
    add duplicate filters.

    Controlled by ``ALMANAK_REDACT_SECRETS`` env var (default: ``true``).
    Set to ``false`` to disable redaction (useful for local debugging).
    """
    global _installed

    enabled = os.environ.get("ALMANAK_REDACT_SECRETS", "true").lower() not in (
        "false",
        "0",
        "no",
    )

    # Always rebuild patterns (env may have changed since last call).
    _rebuild()

    if not enabled:
        return

    if not _installed:
        # Attach filter to root logger -- covers all child loggers.
        root = logging.getLogger()
        # Avoid duplicates if called more than once.
        if not any(isinstance(f, RedactionFilter) for f in root.filters):
            root.addFilter(RedactionFilter())
        _installed = True

    secret_count = len(_secret_replacements)
    if secret_count > 0:
        logger.debug("Secret redaction active: %d secret(s) registered", secret_count)
