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

Backend topology (VIB-4049):

- Local SDK: SQLite, keyed by ``TeardownRequest.strategy_id``.
- Hosted platform: PostgreSQL, keyed by ``agent_id`` (mapped from
  ``strategy_id`` at the write/read boundary in the Postgres backend).

The factory below picks the right backend based on
``framework/deployment/mode.py:is_hosted()`` plus the platform-injected
``ALMANAK_GATEWAY_DATABASE_URL``. Callers do NOT branch on the mode
themselves — they call :func:`get_teardown_state_manager` and trust it.
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
    "create_teardown_state_manager",
    "create_teardown_state_adapter",
    "reset_teardown_state_manager",
    # Post-conditions (VIB-3742)
    "ClosureCheckResult",
    "TeardownPostCondition",
    "get_teardown_post_condition",
    "has_teardown_post_condition",
    "register_teardown_post_condition",
]


# ---------------------------------------------------------------------------
# Factory + singleton (VIB-4049 PR2 §4)
# ---------------------------------------------------------------------------
#
# Mirrors the lifecycle store's pattern (``gateway/lifecycle/__init__.py``).
# The factory is the ONE place that decides between SQLite and Postgres; every
# call site reads ``get_teardown_state_manager()`` and gets the right backend
# for the mode. The four ``is_hosted()`` short-circuits at the legacy call
# sites (runner_gateway / intent_strategy / _teardown_helpers) are collapsed
# in PR2 §7 — they all now go through this factory.

_DB_URL_ENV_VAR = "ALMANAK_GATEWAY_DATABASE_URL"


def create_teardown_state_manager(
    database_url: str | None = None,
    sqlite_path: str | Path | None = None,
) -> TeardownStateManagerProtocol:
    """Factory for the teardown-request store.

    Picks the Postgres backend when ``database_url`` is set, regardless of
    whether ``is_hosted()`` reports hosted mode. This matches the locked
    VIB-4049 design: dashboards, API processes, and Postgres test fixtures
    all need to share the teardown store with the runner — gating on
    ``is_hosted()`` would let those local-but-Postgres processes silently
    write to SQLite while the runner reads Postgres, dropping requests on
    the floor.

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
            f"{_DB_URL_ENV_VAR} is unset in hosted mode (AGENT_ID is set). "
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

    Same mode + entry-point pattern as :func:`create_teardown_state_manager`,
    but resolves the sibling plugin
    ``almanak.teardown:postgres_adapter`` so the platform package can choose to
    publish the two halves as separate classes.
    """
    from almanak.framework.deployment import is_hosted

    if is_hosted() and not database_url:
        # Same fail-closed contract as create_teardown_state_manager — hosted
        # mode without a DB URL must abort, not silently fall back to a SQLite
        # approval channel no other process can see.
        raise RuntimeError(
            f"{_DB_URL_ENV_VAR} is unset in hosted mode (AGENT_ID is set). "
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
    """Return the singleton teardown state manager for the current mode.

    Thread-safe via double-checked locking. The ``db_path`` argument is
    honoured only on the first call (and only in local SQLite mode). In
    hosted mode the factory consults :func:`is_hosted` plus
    ``ALMANAK_GATEWAY_DATABASE_URL`` (read via :class:`GatewaySettings`,
    the canonical env boundary — ``_DB_URL_ENV_VAR`` is retained only for
    error-message text) — the path argument is ignored.
    """
    from almanak.gateway.core.settings import GatewaySettings

    global _state_manager
    if _state_manager is None:
        with _state_manager_lock:
            if _state_manager is None:
                database_url = GatewaySettings().database_url or None
                _state_manager = create_teardown_state_manager(
                    database_url=database_url,
                    sqlite_path=db_path,
                )
    return _state_manager


def reset_teardown_state_manager() -> None:
    """Reset the singleton. Useful for tests that toggle deployment mode."""
    global _state_manager
    _state_manager = None
