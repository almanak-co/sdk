"""Strategy Teardown System.

Provides safe, resumable strategy teardown with position-aware loss caps,
escalating slippage with human approval checkpoints, and auto-protect monitoring.

Two user-facing modes:
- Graceful Shutdown (SOFT): Takes 15-30 minutes, minimizes costs
- Safe Emergency Exit (HARD): Takes 1-3 minutes, prioritizes speed

Core invariants (never violated):
- Position-aware loss cap enforced
- MEV protection on all swaps
- 10-second cancel window
- Simulation before execution
- Atomic bundling for Safe wallets
- Post-execution verification
- Resumable state across restarts

Backend topology:

- Local SDK: SQLite, keyed by ``TeardownRequest.deployment_id``.
- Hosted strategy runtime: gateway gRPC, with PostgreSQL access kept inside
  the gateway process.
- Hosted gateway process: PostgreSQL, keyed by ``deployment_id`` (mapped from
  ``deployment_id`` at the write/read boundary in the Postgres backend).
"""

from __future__ import annotations

import logging
import threading
from pathlib import Path

from almanak.framework.teardown.cancel_window import CancelWindowManager
from almanak.framework.teardown.config import (
    ChainConsolidationConfig,
    TeardownConfig,
    TokenConsolidationConfig,
)
from almanak.framework.teardown.models import (
    ApprovalRequest,
    ApprovalResponse,
    EscalationLevel,
    PositionInfo,
    PositionType,
    TeardownAssetPolicy,
    TeardownMode,
    TeardownPhase,
    TeardownPositionSummary,
    TeardownPreview,
    TeardownProfile,
    TeardownRequest,
    TeardownResult,
    TeardownState,
    TeardownStatus,
    calculate_max_acceptable_loss,
)
from almanak.framework.teardown.post_conditions import (
    ClosureCheckResult,
    TeardownPostCondition,
    get_teardown_post_condition,
    has_teardown_post_condition,
    register_teardown_post_condition,
)
from almanak.framework.teardown.safety_guard import SafetyGuard
from almanak.framework.teardown.slippage_manager import EscalatingSlippageManager
from almanak.framework.teardown.state_manager import (
    SQLiteTeardownStateAdapter,
    SQLiteTeardownStateManager,
    TeardownStateAdapter,
    TeardownStateAdapterProtocol,
    TeardownStateManager,
    TeardownStateManagerProtocol,
)
from almanak.framework.teardown.teardown_manager import TeardownManager

logger = logging.getLogger(__name__)

__all__ = [
    # Models - Core
    "TeardownMode",
    "TeardownPhase",
    "TeardownAssetPolicy",
    "PositionType",
    "PositionInfo",
    "TeardownPositionSummary",
    "TeardownPreview",
    "TeardownResult",
    "TeardownState",
    "TeardownStatus",
    "TeardownRequest",
    "TeardownProfile",
    # Models - Escalation
    "EscalationLevel",
    "ApprovalRequest",
    "ApprovalResponse",
    # Functions
    "calculate_max_acceptable_loss",
    # Config
    "TeardownConfig",
    "TokenConsolidationConfig",
    "ChainConsolidationConfig",
    # Safety
    "SafetyGuard",
    # Managers
    "EscalatingSlippageManager",
    "CancelWindowManager",
    "TeardownManager",
    "TeardownStateManager",
    "TeardownStateAdapter",
    "SQLiteTeardownStateManager",
    "SQLiteTeardownStateAdapter",
    "TeardownStateManagerProtocol",
    "TeardownStateAdapterProtocol",
    "get_teardown_state_manager",
    "get_teardown_state_manager_for_runtime",
    "create_teardown_state_manager",
    "create_teardown_state_adapter",
    "create_teardown_state_adapter_for_runtime",
    "reset_teardown_state_manager",
    # Post-conditions (VIB-3742)
    "ClosureCheckResult",
    "TeardownPostCondition",
    "get_teardown_post_condition",
    "has_teardown_post_condition",
    "register_teardown_post_condition",
]


# ---------------------------------------------------------------------------
# Direct backend factories + local singleton
# ---------------------------------------------------------------------------
#
# ``create_*`` are direct backend constructors used by gateway internals and
# local tooling. Hosted strategy runtime code must use the ``*_for_runtime``
# helpers below so it never sees the gateway's database URL.

_DB_URL_ENV_VAR = "ALMANAK_GATEWAY_DATABASE_URL"


def create_teardown_state_manager(
    database_url: str | None = None,
    sqlite_path: str | Path | None = None,
) -> TeardownStateManagerProtocol:
    """Factory for a direct teardown-request store.

    Picks the Postgres backend when ``database_url`` is set, regardless of
    whether ``is_hosted()`` reports hosted mode. Gateway internals, dashboards,
    API processes, and Postgres test fixtures all need this direct store.

    When ``is_hosted()`` is true but ``database_url`` is unset, this raises:
    hosted mode REQUIRES the Postgres backend, and silently falling back to
    SQLite would re-create the April 30 silent-failure scenario.

    The Postgres plugin is loaded via the ``almanak.teardown:postgres`` entry
    point — see ``platform-plugins/pyproject.toml``.
    """
    from almanak.framework.deployment import is_hosted

    if is_hosted() and not database_url:
        # Hosted mode without a DB URL is unrecoverable — abort at boot rather
        # than let writes land in a local SQLite file the runner never reads.
        # See VIB-4049 PR2 brief and CLAUDE.md "Schema-contract check at boot".
        raise RuntimeError(
            f"{_DB_URL_ENV_VAR} is unset in hosted mode (ALMANAK_IS_HOSTED is set). "
            "The platform teardown backend requires a Postgres database URL — "
            "without it, teardown requests would silently disappear into a "
            "local SQLite file the runner never reads."
        )

    if database_url:
        try:
            from importlib.metadata import entry_points

            eps = entry_points(group="almanak.teardown", name="postgres")
            ep_list = list(eps) if hasattr(eps, "__iter__") else []
            if not ep_list:
                raise RuntimeError(
                    f"{_DB_URL_ENV_VAR} is set but no 'almanak.teardown:postgres' "
                    "plugin is installed. Install almanak-platform-plugins or "
                    "unset the env var."
                )
            store_cls = ep_list[0].load()
            logger.info("Using platform teardown state manager: %s", store_cls.__name__)
            return store_cls(database_url=database_url)
        except RuntimeError:
            raise
        except Exception as exc:
            logger.exception("Failed to load 'almanak.teardown:postgres' plugin")
            raise RuntimeError("Failed to load 'almanak.teardown:postgres' plugin") from exc

    return SQLiteTeardownStateManager(db_path=sqlite_path)


def create_teardown_state_adapter(
    database_url: str | None = None,
    sqlite_path: str | Path | None = None,
) -> TeardownStateAdapterProtocol:
    """Factory for the teardown-execution-state + approval-channel store.

    Same entry-point pattern as :func:`create_teardown_state_manager`, but
    resolves the sibling plugin ``almanak.teardown:postgres_adapter``.
    """
    from almanak.framework.deployment import is_hosted

    if is_hosted() and not database_url:
        # Same fail-closed contract as create_teardown_state_manager — hosted
        # mode without a DB URL must abort, not silently fall back to a SQLite
        # approval channel no other process can see.
        raise RuntimeError(
            f"{_DB_URL_ENV_VAR} is unset in hosted mode (ALMANAK_IS_HOSTED is set). "
            "The platform teardown state adapter requires a Postgres database "
            "URL — without it, slippage-escalation approval responses would "
            "land in a SQLite file invisible to the dashboard / CLI writer."
        )

    if database_url:
        try:
            from importlib.metadata import entry_points

            eps = entry_points(group="almanak.teardown", name="postgres_adapter")
            ep_list = list(eps) if hasattr(eps, "__iter__") else []
            if not ep_list:
                raise RuntimeError(
                    f"{_DB_URL_ENV_VAR} is set but no 'almanak.teardown:postgres_adapter' "
                    "plugin is installed. Install almanak-platform-plugins or "
                    "unset the env var."
                )
            adapter_cls = ep_list[0].load()
            logger.info("Using platform teardown state adapter: %s", adapter_cls.__name__)
            return adapter_cls(database_url=database_url)
        except RuntimeError:
            raise
        except Exception as exc:
            logger.exception("Failed to load 'almanak.teardown:postgres_adapter' plugin")
            raise RuntimeError("Failed to load 'almanak.teardown:postgres_adapter' plugin") from exc

    return SQLiteTeardownStateAdapter(db_path=sqlite_path)


_state_manager: TeardownStateManagerProtocol | None = None
_state_manager_lock = threading.Lock()


def get_teardown_state_manager(
    db_path: str | Path | None = None,
) -> TeardownStateManagerProtocol:
    """Return the singleton local teardown state manager.

    Thread-safe via double-checked locking. The ``db_path`` argument is
    honoured only on the first call. Hosted strategy runtime code should call
    :func:`get_teardown_state_manager_for_runtime`; this helper intentionally
    has no access to the gateway database URL.
    """
    global _state_manager
    if _state_manager is None:
        with _state_manager_lock:
            if _state_manager is None:
                _state_manager = create_teardown_state_manager(
                    database_url=None,
                    sqlite_path=db_path,
                )
    return _state_manager


def get_teardown_state_manager_for_runtime(
    gateway_client: object | None = None,
    db_path: str | Path | None = None,
) -> TeardownStateManagerProtocol:
    """Return the teardown request store for strategy runtime code.

    Hosted strategy containers must route through the gateway so the Postgres
    DSN stays out of the strategy process. Local mode keeps the existing
    SQLite behaviour.
    """
    from almanak.framework.deployment import is_hosted

    if is_hosted():
        if gateway_client is None:
            raise RuntimeError("Hosted teardown state requires a connected gateway client")
        from almanak.framework.teardown.gateway_client import GatewayTeardownStateManager

        return GatewayTeardownStateManager(gateway_client)  # type: ignore[arg-type]

    return get_teardown_state_manager(db_path=db_path)


def create_teardown_state_adapter_for_runtime(
    gateway_client: object | None = None,
    sqlite_path: str | Path | None = None,
) -> TeardownStateAdapterProtocol:
    """Return the teardown execution-state adapter for strategy runtime code."""
    from almanak.framework.deployment import is_hosted

    if is_hosted():
        if gateway_client is None:
            raise RuntimeError("Hosted teardown execution state requires a connected gateway client")
        from almanak.framework.teardown.gateway_client import GatewayTeardownStateAdapter

        return GatewayTeardownStateAdapter(gateway_client)  # type: ignore[arg-type]

    return create_teardown_state_adapter(database_url=None, sqlite_path=sqlite_path)


def reset_teardown_state_manager() -> None:
    """Reset the singleton. Useful for tests that toggle deployment mode."""
    global _state_manager
    _state_manager = None
