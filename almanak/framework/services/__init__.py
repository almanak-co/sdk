"""Almanak Strategy Framework v2.0 - Services"""

from .auto_redemption import (
    AutoRedemptionService,
    MarketResolvedEvent,
    RedemptionAttempt,
    RedemptionCallback,
    RedemptionStatus,
)
from .base import (
    HealthCheckResult,
    Service,
    ServiceError,
    ServiceNotRunningError,
    ServiceStartError,
    ServiceStatus,
    ServiceStopError,
)
from .emergency_manager import (
    BorrowPosition,
    EmergencyManager,
    EmergencyResult,
    FullPositionSummary,
    GetPositionCallback,
    LPPositionInfo,
    PauseStrategyCallback,
    TokenPosition,
    create_emergency_manager,
)
from .operator_card_generator import OperatorCardGenerator
from .prediction_monitor import (
    EventCallback,
    MonitoredPosition,
    MonitoringResult,
    PositionSnapshot,
    PredictionEvent,
    PredictionExitConditions,
    PredictionPositionMonitor,
)
from .stuck_detector import (
    AllowanceInfo,
    BalanceInfo,
    PendingTransaction,
    StrategySnapshot,
    StuckDetectionResult,
    StuckDetector,
)

__all__ = [
    # Base service classes
    "Service",
    "ServiceStatus",
    "HealthCheckResult",
    "ServiceError",
    "ServiceNotRunningError",
    "ServiceStartError",
    "ServiceStopError",
    # Services
    "AllowanceInfo",
    "AutoRedemptionService",
    "BalanceInfo",
    "BorrowPosition",
    "EmergencyManager",
    "EmergencyResult",
    "EventCallback",
    "FullPositionSummary",
    "GetPositionCallback",
    "LPPositionInfo",
    "MarketResolvedEvent",
    "MonitoredPosition",
    "MonitoringResult",
    "OperatorCardGenerator",
    "PauseStrategyCallback",
    "PendingTransaction",
    "PositionSnapshot",
    "PredictionEvent",
    "PredictionExitConditions",
    "PredictionPositionMonitor",
    "RedemptionAttempt",
    "RedemptionCallback",
    "RedemptionStatus",
    "StrategySnapshot",
    "StuckDetectionResult",
    "StuckDetector",
    "TokenPosition",
    "create_emergency_manager",
]
