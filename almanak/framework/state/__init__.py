"""State management package for tiered persistence.

This package provides the StateManager class for managing strategy state
across two tiers:
- HOT: In-memory cache with <1ms access
- WARM: PostgreSQL or SQLite with <10ms access

Uses CAS (Compare-And-Swap) semantics via version field for safe concurrent updates.

Also provides state schema migrations for evolving state structure over time.

Backends:
- SQLiteStore: Local SQLite database for development and lightweight deployments
- PostgresStore: Production-grade PostgreSQL storage (in state_manager module)
"""

from .backends import (
    SQLiteConfig,
    SQLiteStore,
)
from .in_flight import (
    InFlightAsset,
    InFlightExposureConfig,
    InFlightExposureError,
    InFlightExposureTracker,
    InFlightLimitExceededError,
    InFlightStatus,
    InFlightSummary,
    TransferNotFoundError,
)
from .migrations import (
    MigrationError,
    MigrationFunction,
    MigrationNotFoundError,
    MigrationRegistry,
    MigrationResult,
    RollbackInfo,
    RollbackNotSafeError,
    StateMigration,
    auto_migrate,
    check_rollback_safety,
    get_registry,
    get_rollback_safe_version,
    migrate,
    migrate_state_data,
    migration,
    needs_migration,
    register_migration,
)
from .position import (
    ChainNotFoundError,
    PositionManager,
    PositionRecord,
    PositionType,
)
from .state_manager import (
    PostgresConfig,
    SQLiteConfigLight,
    StateConflictError,
    StateData,
    StateManager,
    StateManagerConfig,
    StateNotFoundError,
    StateTier,
    TierMetrics,
    WarmBackendType,
    WarmStore,
)

__all__ = [
    # State manager
    "StateManager",
    "StateTier",
    "StateData",
    "StateConflictError",
    "StateNotFoundError",
    "TierMetrics",
    "StateManagerConfig",
    "PostgresConfig",
    "WarmBackendType",
    "WarmStore",
    "SQLiteConfigLight",
    # Backends
    "SQLiteStore",
    "SQLiteConfig",
    # Migrations
    "StateMigration",
    "MigrationResult",
    "MigrationRegistry",
    "MigrationError",
    "MigrationNotFoundError",
    "RollbackNotSafeError",
    "RollbackInfo",
    "MigrationFunction",
    "get_registry",
    "register_migration",
    "migration",
    "migrate",
    "auto_migrate",
    "migrate_state_data",
    "needs_migration",
    "check_rollback_safety",
    "get_rollback_safe_version",
    # Position management (chain dimension support)
    "PositionRecord",
    "PositionType",
    "PositionManager",
    "ChainNotFoundError",
    # In-flight exposure tracking
    "InFlightStatus",
    "InFlightAsset",
    "InFlightExposureConfig",
    "InFlightSummary",
    "InFlightExposureTracker",
    "InFlightExposureError",
    "TransferNotFoundError",
    "InFlightLimitExceededError",
]
