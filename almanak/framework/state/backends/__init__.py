"""State backend implementations.

This package provides different storage backends for the StateManager:
- SQLiteStore: Local SQLite database for development and lightweight deployments
- PostgresStore: Production-grade PostgreSQL storage (in parent module)

All backends implement the same interface for consistent state operations.
"""

from .sqlite import (
    SQLiteConfig,
    SQLiteStore,
)

__all__ = [
    "SQLiteStore",
    "SQLiteConfig",
]
