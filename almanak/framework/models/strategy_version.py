"""Strategy versioning for tracking code and config changes.

This module provides version management for strategies, enabling:
- Tracking code hashes and versions across deployments
- Snapshotting configuration at each deployment
- Recording connector versions for reproducibility
- Rolling back to previous versions when needed
- Tracking performance metrics per version

Usage:
    from almanak.framework.models.strategy_version import StrategyVersion, VersionManager

    # Create a version manager
    manager = VersionManager(strategy_id="my_strategy")

    # Deploy a new version
    version = manager.deploy_version(
        code_hash="abc123",
        code_version="1.0.0",
        config_snapshot={"max_slippage": "0.005"},
        connector_versions={"uniswap_v3": "2.0.0"},
        created_by="operator@example.com",
    )

    # List versions
    versions = manager.list_versions()

    # Rollback to a previous version
    result = manager.rollback(version_id="v_123")
"""

import hashlib
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class PerformanceMetrics:
    """Performance metrics for a strategy version.

    These metrics help evaluate version performance over time and
    compare different versions for rollback decisions.

    Attributes:
        total_pnl_usd: Total profit/loss in USD
        net_pnl_usd: Net profit/loss after fees/gas in USD
        sharpe_ratio: Risk-adjusted return metric
        max_drawdown: Maximum peak-to-trough decline as decimal
        win_rate: Percentage of profitable trades (0-1)
        total_trades: Total number of trades executed
        total_gas_usd: Total gas costs in USD
        avg_trade_size_usd: Average trade size in USD
        uptime_seconds: Total active running time
        measurement_start: When metrics collection started
        measurement_end: When metrics collection ended (None if ongoing)
    """

    total_pnl_usd: Decimal = Decimal("0")
    net_pnl_usd: Decimal = Decimal("0")
    sharpe_ratio: Decimal | None = None
    max_drawdown: Decimal = Decimal("0")
    win_rate: Decimal | None = None
    total_trades: int = 0
    total_gas_usd: Decimal = Decimal("0")
    avg_trade_size_usd: Decimal = Decimal("0")
    uptime_seconds: int = 0
    measurement_start: datetime | None = None
    measurement_end: datetime | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert metrics to a dictionary for serialization."""
        return {
            "total_pnl_usd": str(self.total_pnl_usd),
            "net_pnl_usd": str(self.net_pnl_usd),
            "sharpe_ratio": str(self.sharpe_ratio) if self.sharpe_ratio is not None else None,
            "max_drawdown": str(self.max_drawdown),
            "win_rate": str(self.win_rate) if self.win_rate is not None else None,
            "total_trades": self.total_trades,
            "total_gas_usd": str(self.total_gas_usd),
            "avg_trade_size_usd": str(self.avg_trade_size_usd),
            "uptime_seconds": self.uptime_seconds,
            "measurement_start": self.measurement_start.isoformat() if self.measurement_start else None,
            "measurement_end": self.measurement_end.isoformat() if self.measurement_end else None,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PerformanceMetrics":
        """Create metrics from a dictionary."""
        return cls(
            total_pnl_usd=Decimal(data.get("total_pnl_usd", "0")),
            net_pnl_usd=Decimal(data.get("net_pnl_usd", "0")),
            sharpe_ratio=Decimal(data["sharpe_ratio"]) if data.get("sharpe_ratio") else None,
            max_drawdown=Decimal(data.get("max_drawdown", "0")),
            win_rate=Decimal(data["win_rate"]) if data.get("win_rate") else None,
            total_trades=data.get("total_trades", 0),
            total_gas_usd=Decimal(data.get("total_gas_usd", "0")),
            avg_trade_size_usd=Decimal(data.get("avg_trade_size_usd", "0")),
            uptime_seconds=data.get("uptime_seconds", 0),
            measurement_start=datetime.fromisoformat(data["measurement_start"])
            if data.get("measurement_start")
            else None,
            measurement_end=datetime.fromisoformat(data["measurement_end"]) if data.get("measurement_end") else None,
        )


@dataclass
class StrategyVersion:
    """A versioned snapshot of a strategy deployment.

    Each deployment of a strategy creates a new version that captures:
    - Code hash and version for reproducibility
    - Configuration snapshot at deployment time
    - Connector versions for dependency tracking
    - Who deployed and when
    - Optional performance metrics over time

    Attributes:
        version_id: Unique identifier for this version (format: "v_{strategy_id}_{timestamp}")
        strategy_id: ID of the strategy this version belongs to
        code_hash: Hash of the strategy code for integrity verification
        code_version: Semantic version string (e.g., "1.0.0", "2.1.3-beta")
        config_snapshot: Configuration values at deployment time
        connector_versions: Dict of connector name to version string
        created_at: When this version was deployed
        created_by: Who deployed this version (email or system identifier)
        performance_metrics: Optional metrics collected over this version's lifetime
        is_active: Whether this version is currently deployed
        rollback_from: If this version was created by rollback, the version ID rolled back from
        notes: Optional deployment notes or change description
    """

    version_id: str
    strategy_id: str
    code_hash: str
    code_version: str
    config_snapshot: dict[str, Any]
    connector_versions: dict[str, str] = field(default_factory=dict)
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    created_by: str = "system"
    performance_metrics: PerformanceMetrics | None = None
    is_active: bool = False
    rollback_from: str | None = None
    notes: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert the version to a dictionary for serialization."""
        return {
            "version_id": self.version_id,
            "strategy_id": self.strategy_id,
            "code_hash": self.code_hash,
            "code_version": self.code_version,
            "config_snapshot": self.config_snapshot,
            "connector_versions": self.connector_versions,
            "created_at": self.created_at.isoformat(),
            "created_by": self.created_by,
            "performance_metrics": self.performance_metrics.to_dict() if self.performance_metrics else None,
            "is_active": self.is_active,
            "rollback_from": self.rollback_from,
            "notes": self.notes,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "StrategyVersion":
        """Create a version from a dictionary.

        Args:
            data: Dictionary with version data

        Returns:
            StrategyVersion instance
        """
        metrics = None
        if data.get("performance_metrics"):
            metrics = PerformanceMetrics.from_dict(data["performance_metrics"])

        return cls(
            version_id=data["version_id"],
            strategy_id=data["strategy_id"],
            code_hash=data["code_hash"],
            code_version=data["code_version"],
            config_snapshot=data.get("config_snapshot", {}),
            connector_versions=data.get("connector_versions", {}),
            created_at=datetime.fromisoformat(data["created_at"]),
            created_by=data.get("created_by", "system"),
            performance_metrics=metrics,
            is_active=data.get("is_active", False),
            rollback_from=data.get("rollback_from"),
            notes=data.get("notes"),
        )


@dataclass
class DeploymentResult:
    """Result of a version deployment or rollback operation.

    Attributes:
        success: Whether the operation succeeded
        version: The deployed/rolled back version (if successful)
        error: Error message if the operation failed
        previous_version_id: ID of the version that was replaced
    """

    success: bool
    version: StrategyVersion | None = None
    error: str | None = None
    previous_version_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert the result to a dictionary for serialization."""
        return {
            "success": self.success,
            "version": self.version.to_dict() if self.version else None,
            "error": self.error,
            "previous_version_id": self.previous_version_id,
        }


# Type alias for version deployment callbacks
VersionDeployCallback = Callable[[StrategyVersion], None]
VersionRollbackCallback = Callable[[StrategyVersion, StrategyVersion], None]


class VersionManager:
    """Manages strategy versions for deployment, rollback, and tracking.

    The VersionManager maintains a history of all deployed versions and provides
    operations for deploying new versions, rolling back to previous versions,
    and querying version history.

    In production, versions would be persisted to a database. This implementation
    uses an in-memory store for demonstration.

    Attributes:
        strategy_id: The ID of the strategy being managed
    """

    def __init__(
        self,
        strategy_id: str,
        on_deploy: VersionDeployCallback | None = None,
        on_rollback: VersionRollbackCallback | None = None,
    ) -> None:
        """Initialize the version manager.

        Args:
            strategy_id: ID of the strategy to manage versions for
            on_deploy: Optional callback called after successful deployment
            on_rollback: Optional callback called after successful rollback
        """
        self.strategy_id = strategy_id
        self._versions: dict[str, StrategyVersion] = {}
        self._version_order: list[str] = []  # Ordered list of version IDs
        self._on_deploy = on_deploy
        self._on_rollback = on_rollback

    @staticmethod
    def compute_code_hash(code: str) -> str:
        """Compute a hash of strategy code for integrity verification.

        Args:
            code: The strategy source code as a string

        Returns:
            SHA-256 hash of the code
        """
        return hashlib.sha256(code.encode("utf-8")).hexdigest()

    @staticmethod
    def generate_version_id(strategy_id: str, timestamp: datetime | None = None) -> str:
        """Generate a unique version ID.

        Args:
            strategy_id: ID of the strategy
            timestamp: Optional timestamp (defaults to now)

        Returns:
            Version ID in format "v_{strategy_id}_{timestamp}"
        """
        ts = timestamp or datetime.now(UTC)
        ts_str = ts.strftime("%Y%m%d%H%M%S")
        return f"v_{strategy_id}_{ts_str}"

    def get_active_version(self) -> StrategyVersion | None:
        """Get the currently active version.

        Returns:
            The active StrategyVersion, or None if no version is active
        """
        for version in self._versions.values():
            if version.is_active:
                return version
        return None

    def get_version(self, version_id: str) -> StrategyVersion | None:
        """Get a specific version by ID.

        Args:
            version_id: The version ID to retrieve

        Returns:
            The StrategyVersion, or None if not found
        """
        return self._versions.get(version_id)

    def deploy_version(
        self,
        code_hash: str,
        code_version: str,
        config_snapshot: dict[str, Any],
        connector_versions: dict[str, str] | None = None,
        created_by: str = "system",
        notes: str | None = None,
    ) -> DeploymentResult:
        """Deploy a new strategy version.

        Creates a new version record and marks it as active, deactivating
        any previously active version.

        Args:
            code_hash: Hash of the strategy code
            code_version: Semantic version string
            config_snapshot: Configuration at deployment time
            connector_versions: Optional dict of connector versions
            created_by: Who is deploying (email or system ID)
            notes: Optional deployment notes

        Returns:
            DeploymentResult with the new version or error
        """
        # Validate inputs
        if not code_hash:
            return DeploymentResult(success=False, error="code_hash is required")
        if not code_version:
            return DeploymentResult(success=False, error="code_version is required")

        # Get the current active version (if any)
        previous_version = self.get_active_version()
        previous_version_id = previous_version.version_id if previous_version else None

        # Generate version ID
        created_at = datetime.now(UTC)
        version_id = self.generate_version_id(self.strategy_id, created_at)

        # Create the new version
        new_version = StrategyVersion(
            version_id=version_id,
            strategy_id=self.strategy_id,
            code_hash=code_hash,
            code_version=code_version,
            config_snapshot=config_snapshot,
            connector_versions=connector_versions or {},
            created_at=created_at,
            created_by=created_by,
            is_active=True,
            notes=notes,
        )

        # Deactivate the previous version
        if previous_version:
            previous_version.is_active = False

        # Store the new version
        self._versions[version_id] = new_version
        self._version_order.append(version_id)

        logger.info(
            f"Deployed version {version_id} for strategy {self.strategy_id} "
            f"(code: {code_version}, hash: {code_hash[:8]}...)"
        )

        # Call deployment callback
        if self._on_deploy:
            try:
                self._on_deploy(new_version)
            except Exception as e:
                logger.error(f"Deployment callback failed: {e}")

        return DeploymentResult(
            success=True,
            version=new_version,
            previous_version_id=previous_version_id,
        )

    def rollback(self, version_id: str, rolled_back_by: str = "system") -> DeploymentResult:
        """Rollback to a previous version.

        Creates a new version entry that references the rolled-back-from version,
        restoring the code hash, code version, and config snapshot from the
        target version.

        Args:
            version_id: ID of the version to rollback to
            rolled_back_by: Who is performing the rollback

        Returns:
            DeploymentResult with the new version or error
        """
        # Find the target version
        target_version = self.get_version(version_id)
        if not target_version:
            return DeploymentResult(
                success=False,
                error=f"Version {version_id} not found",
            )

        # Get the current active version
        current_version = self.get_active_version()
        if not current_version:
            return DeploymentResult(
                success=False,
                error="No active version to rollback from",
            )

        if current_version.version_id == version_id:
            return DeploymentResult(
                success=False,
                error=f"Cannot rollback to the currently active version {version_id}",
            )

        # Create a new version for the rollback
        created_at = datetime.now(UTC)
        rollback_version_id = self.generate_version_id(self.strategy_id, created_at)

        rollback_version = StrategyVersion(
            version_id=rollback_version_id,
            strategy_id=self.strategy_id,
            code_hash=target_version.code_hash,
            code_version=target_version.code_version,
            config_snapshot=target_version.config_snapshot.copy(),
            connector_versions=target_version.connector_versions.copy(),
            created_at=created_at,
            created_by=rolled_back_by,
            is_active=True,
            rollback_from=current_version.version_id,
            notes=f"Rollback from {current_version.version_id} to {version_id}",
        )

        # Deactivate the current version
        current_version.is_active = False

        # Store the rollback version
        self._versions[rollback_version_id] = rollback_version
        self._version_order.append(rollback_version_id)

        logger.info(
            f"Rolled back strategy {self.strategy_id} from {current_version.version_id} "
            f"to {version_id} (new version: {rollback_version_id})"
        )

        # Call rollback callback
        if self._on_rollback:
            try:
                self._on_rollback(current_version, rollback_version)
            except Exception as e:
                logger.error(f"Rollback callback failed: {e}")

        return DeploymentResult(
            success=True,
            version=rollback_version,
            previous_version_id=current_version.version_id,
        )

    def list_versions(
        self,
        limit: int = 50,
        offset: int = 0,
        include_metrics: bool = True,
    ) -> list[StrategyVersion]:
        """List versions for this strategy.

        Returns versions in reverse chronological order (newest first).

        Args:
            limit: Maximum number of versions to return
            offset: Number of versions to skip
            include_metrics: Whether to include performance metrics

        Returns:
            List of StrategyVersion objects
        """
        # Get version IDs in reverse order (newest first)
        version_ids = list(reversed(self._version_order))

        # Apply pagination
        paginated_ids = version_ids[offset : offset + limit]

        # Build result list
        versions: list[StrategyVersion] = []
        for vid in paginated_ids:
            version = self._versions.get(vid)
            if version:
                if not include_metrics:
                    # Create a copy without metrics
                    version = StrategyVersion(
                        version_id=version.version_id,
                        strategy_id=version.strategy_id,
                        code_hash=version.code_hash,
                        code_version=version.code_version,
                        config_snapshot=version.config_snapshot,
                        connector_versions=version.connector_versions,
                        created_at=version.created_at,
                        created_by=version.created_by,
                        performance_metrics=None,
                        is_active=version.is_active,
                        rollback_from=version.rollback_from,
                        notes=version.notes,
                    )
                versions.append(version)

        return versions

    def get_version_count(self) -> int:
        """Get the total number of versions.

        Returns:
            Total count of versions
        """
        return len(self._versions)

    def update_metrics(
        self,
        version_id: str,
        metrics: PerformanceMetrics,
    ) -> bool:
        """Update performance metrics for a version.

        Args:
            version_id: ID of the version to update
            metrics: New performance metrics

        Returns:
            True if update succeeded, False if version not found
        """
        version = self.get_version(version_id)
        if not version:
            return False

        version.performance_metrics = metrics
        logger.debug(f"Updated metrics for version {version_id}")
        return True

    def compare_versions(
        self,
        version_id_a: str,
        version_id_b: str,
    ) -> dict[str, Any] | None:
        """Compare two versions.

        Args:
            version_id_a: First version ID
            version_id_b: Second version ID

        Returns:
            Comparison dict with differences, or None if either version not found
        """
        version_a = self.get_version(version_id_a)
        version_b = self.get_version(version_id_b)

        if not version_a or not version_b:
            return None

        # Compare code
        code_changed = version_a.code_hash != version_b.code_hash
        code_version_changed = version_a.code_version != version_b.code_version

        # Compare config
        config_a = version_a.config_snapshot
        config_b = version_b.config_snapshot
        config_added = set(config_b.keys()) - set(config_a.keys())
        config_removed = set(config_a.keys()) - set(config_b.keys())
        config_changed = {
            k: {"old": config_a[k], "new": config_b[k]}
            for k in set(config_a.keys()) & set(config_b.keys())
            if config_a[k] != config_b[k]
        }

        # Compare connectors
        connectors_a = version_a.connector_versions
        connectors_b = version_b.connector_versions
        connector_changes = {}
        all_connectors = set(connectors_a.keys()) | set(connectors_b.keys())
        for connector in all_connectors:
            old_ver = connectors_a.get(connector)
            new_ver = connectors_b.get(connector)
            if old_ver != new_ver:
                connector_changes[connector] = {"old": old_ver, "new": new_ver}

        # Compare metrics if available
        metrics_comparison = None
        if version_a.performance_metrics and version_b.performance_metrics:
            metrics_a = version_a.performance_metrics
            metrics_b = version_b.performance_metrics
            metrics_comparison = {
                "net_pnl_diff_usd": str(metrics_b.net_pnl_usd - metrics_a.net_pnl_usd),
                "sharpe_diff": str((metrics_b.sharpe_ratio or Decimal("0")) - (metrics_a.sharpe_ratio or Decimal("0"))),
                "max_drawdown_diff": str(metrics_b.max_drawdown - metrics_a.max_drawdown),
            }

        return {
            "version_a": version_id_a,
            "version_b": version_id_b,
            "code_changed": code_changed,
            "code_version_changed": code_version_changed,
            "config_changes": {
                "added": list(config_added),
                "removed": list(config_removed),
                "changed": config_changed,
            },
            "connector_changes": connector_changes,
            "metrics_comparison": metrics_comparison,
        }

    def clear_all_versions(self) -> None:
        """Clear all versions (for testing purposes)."""
        self._versions.clear()
        self._version_order.clear()
        logger.warning(f"Cleared all versions for strategy {self.strategy_id}")

    def to_dict(self) -> dict[str, Any]:
        """Export the version manager state for persistence.

        Returns:
            Dictionary containing all version data
        """
        return {
            "strategy_id": self.strategy_id,
            "versions": [v.to_dict() for v in self._versions.values()],
            "version_order": self._version_order,
        }

    @classmethod
    def from_dict(
        cls,
        data: dict[str, Any],
        on_deploy: VersionDeployCallback | None = None,
        on_rollback: VersionRollbackCallback | None = None,
    ) -> "VersionManager":
        """Restore a version manager from persisted state.

        Args:
            data: Dictionary with version manager data
            on_deploy: Optional deployment callback
            on_rollback: Optional rollback callback

        Returns:
            VersionManager instance with restored state
        """
        manager = cls(
            strategy_id=data["strategy_id"],
            on_deploy=on_deploy,
            on_rollback=on_rollback,
        )

        # Restore versions
        for version_data in data.get("versions", []):
            version = StrategyVersion.from_dict(version_data)
            manager._versions[version.version_id] = version

        # Restore order
        manager._version_order = data.get("version_order", [])

        return manager
