"""State schema migrations for strategy state evolution.

This module provides:
- StateMigration: Defines individual state migrations with version and rollback info
- MigrationRegistry: Tracks all registered migrations
- migrate: Applies migrations sequentially to bring state to target version
- Auto-migration on load when schema version doesn't match current

Migrations are applied in order from current schema_version to target version.
Each migration can specify a rollback_safe_until_version indicating how far
back it's safe to rollback without data loss.
"""

import copy
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

logger = logging.getLogger(__name__)


# =============================================================================
# TYPE ALIASES
# =============================================================================

# Migration function signature: takes state dict, returns migrated state dict
MigrationFunction = Callable[[dict[str, Any]], dict[str, Any]]


# =============================================================================
# EXCEPTIONS
# =============================================================================


class MigrationError(Exception):
    """Raised when a migration fails."""

    def __init__(
        self,
        from_version: int,
        to_version: int,
        message: str | None = None,
        cause: Exception | None = None,
    ) -> None:
        self.from_version = from_version
        self.to_version = to_version
        self.cause = cause
        super().__init__(
            message or f"Migration failed from version {from_version} to {to_version}" + (f": {cause}" if cause else "")
        )


class RollbackNotSafeError(Exception):
    """Raised when attempting to rollback past the safe version."""

    def __init__(
        self,
        current_version: int,
        target_version: int,
        safe_until_version: int,
        message: str | None = None,
    ) -> None:
        self.current_version = current_version
        self.target_version = target_version
        self.safe_until_version = safe_until_version
        super().__init__(
            message
            or f"Cannot safely rollback from version {current_version} to {target_version}. "
            f"Rollback is only safe until version {safe_until_version}"
        )


class MigrationNotFoundError(Exception):
    """Raised when a required migration is not registered."""

    def __init__(self, version: int, message: str | None = None) -> None:
        self.version = version
        super().__init__(message or f"No migration registered for version {version}")


# =============================================================================
# DATA CLASSES
# =============================================================================


@dataclass
class StateMigration:
    """Defines a single state migration.

    Attributes:
        version: The schema version this migration upgrades TO
        migration_fn: Function that transforms state from version-1 to version
        description: Human-readable description of what this migration does
        rollback_safe_until_version: Minimum version that can safely rollback to this version
            (i.e., versions >= this can rollback without data loss)
        created_at: When this migration was defined
    """

    version: int
    migration_fn: MigrationFunction
    description: str = ""
    rollback_safe_until_version: int = 1
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def __post_init__(self) -> None:
        """Validate migration."""
        if self.version < 2:
            raise ValueError("Migration version must be >= 2 (version 1 is initial)")
        if self.rollback_safe_until_version < 1:
            raise ValueError("rollback_safe_until_version must be >= 1")
        if self.rollback_safe_until_version > self.version:
            raise ValueError("rollback_safe_until_version cannot exceed version")

    def apply(self, state: dict[str, Any]) -> dict[str, Any]:
        """Apply this migration to state.

        Creates a deep copy to avoid mutating original state.

        Args:
            state: The state dict to migrate

        Returns:
            Migrated state dict (new copy)
        """
        # Deep copy to avoid mutation
        state_copy = copy.deepcopy(state)
        return self.migration_fn(state_copy)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "version": self.version,
            "description": self.description,
            "rollback_safe_until_version": self.rollback_safe_until_version,
            "created_at": self.created_at.isoformat(),
        }


@dataclass
class MigrationResult:
    """Result of applying migrations.

    Attributes:
        success: Whether all migrations succeeded
        from_version: Starting schema version
        to_version: Ending schema version
        migrations_applied: List of migration versions applied
        state: The migrated state (or original if failed)
        error: Error message if migration failed
        duration_ms: Total migration time in milliseconds
    """

    success: bool
    from_version: int
    to_version: int
    migrations_applied: list[int]
    state: dict[str, Any]
    error: str | None = None
    duration_ms: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "success": self.success,
            "from_version": self.from_version,
            "to_version": self.to_version,
            "migrations_applied": self.migrations_applied,
            "error": self.error,
            "duration_ms": self.duration_ms,
        }


@dataclass
class RollbackInfo:
    """Information about rollback safety for a schema version.

    Attributes:
        current_version: Current schema version
        safe_rollback_versions: List of versions safe to rollback to
        unsafe_rollback_versions: List of versions that would cause data loss
    """

    current_version: int
    safe_rollback_versions: list[int]
    unsafe_rollback_versions: list[int]

    def is_safe_to_rollback(self, target_version: int) -> bool:
        """Check if it's safe to rollback to target version."""
        return target_version in self.safe_rollback_versions


# =============================================================================
# MIGRATION REGISTRY
# =============================================================================


class MigrationRegistry:
    """Registry of all state migrations.

    Tracks migrations and provides version validation and lookup.
    """

    def __init__(self) -> None:
        self._migrations: dict[int, StateMigration] = {}
        self._current_version: int = 1  # Default schema version

    def register(self, migration: StateMigration) -> None:
        """Register a migration.

        Args:
            migration: The migration to register

        Raises:
            ValueError: If a migration for this version already exists
        """
        if migration.version in self._migrations:
            raise ValueError(f"Migration for version {migration.version} already registered")

        self._migrations[migration.version] = migration

        # Update current version if this is newer
        if migration.version > self._current_version:
            self._current_version = migration.version

        logger.info(f"Registered migration to version {migration.version}: {migration.description}")

    def get(self, version: int) -> StateMigration | None:
        """Get migration for a specific version.

        Args:
            version: Target version

        Returns:
            StateMigration or None if not found
        """
        return self._migrations.get(version)

    def get_migrations_path(
        self,
        from_version: int,
        to_version: int,
    ) -> list[StateMigration]:
        """Get list of migrations needed to go from one version to another.

        Args:
            from_version: Starting version
            to_version: Target version

        Returns:
            List of migrations to apply (in order)

        Raises:
            MigrationNotFoundError: If any required migration is missing
        """
        if from_version >= to_version:
            return []

        migrations = []
        for v in range(from_version + 1, to_version + 1):
            migration = self._migrations.get(v)
            if migration is None:
                raise MigrationNotFoundError(v)
            migrations.append(migration)

        return migrations

    def get_rollback_info(self, current_version: int) -> RollbackInfo:
        """Get rollback safety information for a version.

        Args:
            current_version: Current schema version

        Returns:
            RollbackInfo with safe and unsafe rollback targets
        """
        safe_versions = []
        unsafe_versions = []

        # Find the minimum safe version based on all migrations up to current
        min_safe_version = 1
        for v in range(2, current_version + 1):
            migration = self._migrations.get(v)
            if migration:
                min_safe_version = max(min_safe_version, migration.rollback_safe_until_version)

        # Categorize all versions
        for v in range(1, current_version):
            if v >= min_safe_version:
                safe_versions.append(v)
            else:
                unsafe_versions.append(v)

        return RollbackInfo(
            current_version=current_version,
            safe_rollback_versions=safe_versions,
            unsafe_rollback_versions=unsafe_versions,
        )

    @property
    def current_version(self) -> int:
        """Get the current (latest) schema version."""
        return self._current_version

    @property
    def migrations(self) -> dict[int, StateMigration]:
        """Get all registered migrations."""
        return self._migrations.copy()

    def clear(self) -> None:
        """Clear all registered migrations (mainly for testing)."""
        self._migrations.clear()
        self._current_version = 1


# Global registry instance
_registry = MigrationRegistry()


def get_registry() -> MigrationRegistry:
    """Get the global migration registry."""
    return _registry


def register_migration(
    version: int,
    migration_fn: MigrationFunction,
    description: str = "",
    rollback_safe_until_version: int = 1,
) -> StateMigration:
    """Register a migration with the global registry.

    This is a convenience function that creates a StateMigration and registers it.

    Args:
        version: Target schema version
        migration_fn: Function to transform state
        description: Human-readable description
        rollback_safe_until_version: Minimum safe rollback version

    Returns:
        The created StateMigration
    """
    migration = StateMigration(
        version=version,
        migration_fn=migration_fn,
        description=description,
        rollback_safe_until_version=rollback_safe_until_version,
    )
    _registry.register(migration)
    return migration


def migration(
    version: int,
    description: str = "",
    rollback_safe_until_version: int = 1,
) -> Callable[[MigrationFunction], MigrationFunction]:
    """Decorator for registering a migration function.

    Example:
        @migration(version=2, description="Add field_x to state")
        def migrate_v1_to_v2(state: dict) -> dict:
            state["field_x"] = "default_value"
            return state

    Args:
        version: Target schema version
        description: Human-readable description
        rollback_safe_until_version: Minimum safe rollback version

    Returns:
        Decorator function
    """

    def decorator(fn: MigrationFunction) -> MigrationFunction:
        register_migration(
            version=version,
            migration_fn=fn,
            description=description or fn.__doc__ or "",
            rollback_safe_until_version=rollback_safe_until_version,
        )
        return fn

    return decorator


# =============================================================================
# MIGRATION FUNCTIONS
# =============================================================================


def migrate(
    state: dict[str, Any],
    from_version: int,
    to_version: int | None = None,
    registry: MigrationRegistry | None = None,
) -> MigrationResult:
    """Apply migrations to bring state from one version to another.

    Migrations are applied sequentially in order. If any migration fails,
    the process stops and returns the state at that point.

    Args:
        state: The state dict to migrate
        from_version: Current schema version of the state
        to_version: Target schema version (defaults to current/latest)
        registry: Migration registry to use (defaults to global)

    Returns:
        MigrationResult with migrated state and metadata

    Example:
        result = migrate(state, from_version=1, to_version=3)
        if result.success:
            new_state = result.state
    """
    import time

    start_time = time.perf_counter()
    reg = registry or _registry

    # Default to current version
    if to_version is None:
        to_version = reg.current_version

    # Nothing to do
    if from_version >= to_version:
        return MigrationResult(
            success=True,
            from_version=from_version,
            to_version=from_version,
            migrations_applied=[],
            state=state,
            duration_ms=0.0,
        )

    # Get migration path
    try:
        migrations = reg.get_migrations_path(from_version, to_version)
    except MigrationNotFoundError as e:
        return MigrationResult(
            success=False,
            from_version=from_version,
            to_version=to_version,
            migrations_applied=[],
            state=state,
            error=str(e),
            duration_ms=(time.perf_counter() - start_time) * 1000,
        )

    # Apply migrations sequentially
    current_state = state
    applied: list[int] = []

    for mig in migrations:
        try:
            logger.info(f"Applying migration to version {mig.version}: {mig.description}")
            current_state = mig.apply(current_state)
            applied.append(mig.version)
            logger.info(f"Successfully applied migration to version {mig.version}")
        except Exception as e:
            logger.error(f"Migration to version {mig.version} failed: {e}")
            return MigrationResult(
                success=False,
                from_version=from_version,
                to_version=to_version,
                migrations_applied=applied,
                state=current_state,
                error=f"Migration to version {mig.version} failed: {e}",
                duration_ms=(time.perf_counter() - start_time) * 1000,
            )

    duration_ms = (time.perf_counter() - start_time) * 1000
    logger.info(
        f"Successfully applied {len(applied)} migrations from version {from_version} "
        f"to {to_version} in {duration_ms:.2f}ms"
    )

    return MigrationResult(
        success=True,
        from_version=from_version,
        to_version=to_version,
        migrations_applied=applied,
        state=current_state,
        duration_ms=duration_ms,
    )


def check_rollback_safety(
    current_version: int,
    target_version: int,
    registry: MigrationRegistry | None = None,
) -> bool:
    """Check if rolling back from current version to target is safe.

    Args:
        current_version: Current schema version
        target_version: Target version to rollback to
        registry: Migration registry to use (defaults to global)

    Returns:
        True if rollback is safe, False otherwise
    """
    reg = registry or _registry
    rollback_info = reg.get_rollback_info(current_version)
    return rollback_info.is_safe_to_rollback(target_version)


def get_rollback_safe_version(
    current_version: int,
    registry: MigrationRegistry | None = None,
) -> int:
    """Get the minimum version that is safe to rollback to.

    Args:
        current_version: Current schema version
        registry: Migration registry to use (defaults to global)

    Returns:
        Minimum safe rollback version
    """
    reg = registry or _registry
    rollback_info = reg.get_rollback_info(current_version)

    if rollback_info.safe_rollback_versions:
        return min(rollback_info.safe_rollback_versions)
    return current_version  # Can't rollback at all


# =============================================================================
# AUTO-MIGRATION HELPERS
# =============================================================================


def needs_migration(
    state_schema_version: int,
    target_version: int | None = None,
    registry: MigrationRegistry | None = None,
) -> bool:
    """Check if state needs migration.

    Args:
        state_schema_version: Current schema version of state
        target_version: Target version (defaults to current/latest)
        registry: Migration registry to use (defaults to global)

    Returns:
        True if state needs migration
    """
    reg = registry or _registry
    target = target_version or reg.current_version
    return state_schema_version < target


def auto_migrate(
    state: dict[str, Any],
    schema_version_key: str = "schema_version",
    registry: MigrationRegistry | None = None,
) -> MigrationResult:
    """Auto-migrate state to latest version.

    Reads schema version from state dict and migrates if needed.

    Args:
        state: State dict with schema_version field
        schema_version_key: Key in state dict for schema version
        registry: Migration registry to use (defaults to global)

    Returns:
        MigrationResult with migrated state
    """
    current_version = state.get(schema_version_key, 1)
    reg = registry or _registry

    if not needs_migration(current_version, registry=reg):
        return MigrationResult(
            success=True,
            from_version=current_version,
            to_version=current_version,
            migrations_applied=[],
            state=state,
            duration_ms=0.0,
        )

    result = migrate(
        state=state,
        from_version=current_version,
        to_version=reg.current_version,
        registry=reg,
    )

    # Update schema version in state if successful
    if result.success:
        result.state[schema_version_key] = result.to_version

    return result


def migrate_state_data(
    state_data: Any,  # StateData type, using Any to avoid circular import
    registry: MigrationRegistry | None = None,
) -> tuple[Any, MigrationResult]:
    """Migrate StateData object to latest schema version.

    This is the primary integration point with StateManager.

    Args:
        state_data: StateData object to migrate
        registry: Migration registry to use (defaults to global)

    Returns:
        Tuple of (migrated StateData, MigrationResult)
    """
    from .state_manager import StateData

    reg = registry or _registry

    if not needs_migration(state_data.schema_version, registry=reg):
        return state_data, MigrationResult(
            success=True,
            from_version=state_data.schema_version,
            to_version=state_data.schema_version,
            migrations_applied=[],
            state=state_data.state,
            duration_ms=0.0,
        )

    result = migrate(
        state=state_data.state,
        from_version=state_data.schema_version,
        to_version=reg.current_version,
        registry=reg,
    )

    if result.success:
        # Create new StateData with migrated state
        migrated_state_data = StateData(
            strategy_id=state_data.strategy_id,
            version=state_data.version,
            state=result.state,
            schema_version=result.to_version,
            created_at=state_data.created_at,
            loaded_from=state_data.loaded_from,
        )
        return migrated_state_data, result
    else:
        # Return original if migration failed
        return state_data, result


# =============================================================================
# BUILT-IN EXAMPLE MIGRATIONS
# =============================================================================

# Example migrations are commented out - uncomment to register them:

# @migration(version=2, description="Add 'metadata' field to state")
# def migrate_v1_to_v2(state: dict[str, Any]) -> dict[str, Any]:
#     """Add metadata field with default empty dict."""
#     if "metadata" not in state:
#         state["metadata"] = {}
#     return state

# @migration(version=3, description="Rename 'pnl' to 'pnl_usd'", rollback_safe_until_version=2)
# def migrate_v2_to_v3(state: dict[str, Any]) -> dict[str, Any]:
#     """Rename pnl field to pnl_usd for clarity."""
#     if "pnl" in state:
#         state["pnl_usd"] = state.pop("pnl")
#     return state
