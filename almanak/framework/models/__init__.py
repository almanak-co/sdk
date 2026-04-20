"""Almanak Strategy Framework v2.0 - Models"""

from .actions import AvailableAction, SuggestedAction
from .hot_reload_config import (
    ConfigUpdateResult,
    HotReloadableConfig,
)
from .operator_card import (
    AutoRemediation,
    EventType,
    OperatorCard,
    PositionSummary,
    Severity,
)
from .reproduction_bundle import (
    ActionBundle,
    FailureContext,
    FailureHookFn,
    MarketData,
    ReproductionBundle,
    TimelineEventSnapshot,
    TransactionReceipt,
    clear_failure_hooks,
    get_failure_hooks,
    on_failure,
    register_failure_hook,
    unregister_failure_hook,
)
from .strategy_version import (
    DeploymentResult,
    PerformanceMetrics,
    StrategyVersion,
    VersionDeployCallback,
    VersionManager,
    VersionRollbackCallback,
)
from .stuck_reason import (
    AUTO_REMEDIABLE,
    NEEDS_HUMAN,
    REMEDIATION_MAP,
    StuckReason,
)

__all__ = [
    # Actions
    "AvailableAction",
    "SuggestedAction",
    # Stuck reasons
    "StuckReason",
    "REMEDIATION_MAP",
    "AUTO_REMEDIABLE",
    "NEEDS_HUMAN",
    # Operator card
    "OperatorCard",
    "EventType",
    "Severity",
    "PositionSummary",
    "AutoRemediation",
    # Hot-reload config
    "HotReloadableConfig",
    "ConfigUpdateResult",
    # Reproduction bundle
    "ReproductionBundle",
    "TransactionReceipt",
    "ActionBundle",
    "MarketData",
    "TimelineEventSnapshot",
    "FailureContext",
    "FailureHookFn",
    "register_failure_hook",
    "unregister_failure_hook",
    "get_failure_hooks",
    "clear_failure_hooks",
    "on_failure",
    # Strategy versioning
    "StrategyVersion",
    "PerformanceMetrics",
    "DeploymentResult",
    "VersionManager",
    "VersionDeployCallback",
    "VersionRollbackCallback",
]
