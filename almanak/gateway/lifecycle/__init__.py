"""Lifecycle store for agent state and command management (V2 deployment).

Provides factory, singleton accessor, and plugin discovery for the
LifecycleStore system. SQLite backend ships in the public SDK;
PostgreSQL backend is loaded via entry points from the platform plugin.
"""

import logging
import threading

from .sqlite_store import SQLiteLifecycleStore
from .store import AgentCommand, AgentState, LifecycleStore

logger = logging.getLogger(__name__)

__all__ = [
    "AgentCommand",
    "AgentState",
    "LifecycleStore",
    "SQLiteLifecycleStore",
    "create_lifecycle_store",
    "get_lifecycle_store",
    "reset_lifecycle_store",
]


def create_lifecycle_store(
    database_url: str | None = None,
    sqlite_path: str | None = None,
) -> LifecycleStore:
    """Factory for lifecycle store -- checks for platform plugin first.

    Args:
        database_url: PostgreSQL connection URL (platform deployments).
            If set, attempts to load PostgresLifecycleStore via entry points.
        sqlite_path: Path to SQLite DB file (local development).
            Defaults to ~/.config/almanak/gateway.db if neither is provided.
    """
    if database_url:
        # Check for platform plugin via entry points
        try:
            from importlib.metadata import entry_points

            eps = entry_points(group="almanak.lifecycle", name="postgres")
            ep_list = list(eps) if hasattr(eps, "__iter__") else []
            if not ep_list:
                raise RuntimeError(
                    "ALMANAK_GATEWAY_DATABASE_URL is set but no 'almanak.lifecycle:postgres' plugin is installed. "
                    "Install almanak-platform-plugins or remove the DATABASE_URL to use SQLite."
                )
            store_cls = ep_list[0].load()
            logger.info("Using platform lifecycle store: %s", store_cls.__name__)
            return store_cls(database_url=database_url)
        except RuntimeError:
            raise
        except Exception as exc:
            logger.exception("Failed to load platform lifecycle store plugin")
            raise RuntimeError("Failed to load 'almanak.lifecycle:postgres' plugin") from exc

    # Default: SQLite for local development
    if sqlite_path is None:
        from almanak.gateway.core.settings import DEFAULT_GATEWAY_DB_PATH

        sqlite_path = DEFAULT_GATEWAY_DB_PATH

    return SQLiteLifecycleStore(db_path=sqlite_path)


# ---------------------------------------------------------------------------
# Singleton accessor (same pattern as TimelineStore and InstanceRegistry)
# ---------------------------------------------------------------------------

_lifecycle_store: LifecycleStore | None = None
_lifecycle_store_lock = threading.Lock()


def get_lifecycle_store(
    database_url: str | None = None,
    sqlite_path: str | None = None,
) -> LifecycleStore:
    """Get the default lifecycle store (singleton).

    Thread-safe via double-checked locking.

    Args:
        database_url: PostgreSQL URL. Only used on first call.
        sqlite_path: SQLite path. Only used on first call.
    """
    global _lifecycle_store
    if _lifecycle_store is None:
        with _lifecycle_store_lock:
            if _lifecycle_store is None:
                store = create_lifecycle_store(
                    database_url=database_url,
                    sqlite_path=sqlite_path,
                )
                store.initialize()
                _lifecycle_store = store
    return _lifecycle_store


def reset_lifecycle_store() -> None:
    """Reset the lifecycle store singleton. Useful for testing."""
    global _lifecycle_store
    if _lifecycle_store is not None:
        _lifecycle_store.close()
        _lifecycle_store = None
