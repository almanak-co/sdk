"""Config-service helpers for dynamic redaction state."""

from __future__ import annotations


def secret_environment_items() -> tuple[tuple[str, str], ...]:
    """Return a snapshot of the current environment items."""
    import os

    return tuple(os.environ.items())


__all__ = [
    "secret_environment_items",
]
