"""Copy-trading services (see docs/internal/blueprints/24-copy-trading-institutional.md).

Re-exports the names external callers use; submodules remain individually
importable for narrower symbols.
"""

from almanak.framework.services.copy_trading.copy_circuit_breaker import CopyCircuitBreaker
from almanak.framework.services.copy_trading.copy_intent_builder import CopyIntentBuilder
from almanak.framework.services.copy_trading.copy_ledger import CopyLedger
from almanak.framework.services.copy_trading.copy_performance_tracker import CopyPerformanceTracker
from almanak.framework.services.copy_trading.copy_policy_engine import CopyPolicyEngine
from almanak.framework.services.copy_trading.copy_reporting import CopyReportGenerator
from almanak.framework.services.copy_trading.copy_signal_engine import CopySignalEngine
from almanak.framework.services.copy_trading.copy_sizer import CopySizer, CopySizingConfig
from almanak.framework.services.copy_trading.copy_trading_models import (
    CopyDecision,
    CopyExecutionRecord,
    CopySignal,
    CopyTradingConfig,
    CopyTradingConfigError,
    CopyTradingConfigV2,
    LeaderEvent,
    LendingPayload,
    LPPayload,
    PerpPayload,
    SizingMode,
    SwapPayload,
)

__all__ = [
    "CopyCircuitBreaker",
    "CopyDecision",
    "CopyExecutionRecord",
    "CopyIntentBuilder",
    "CopyLedger",
    "CopyPerformanceTracker",
    "CopyPolicyEngine",
    "CopyReportGenerator",
    "CopySignal",
    "CopySignalEngine",
    "CopySizer",
    "CopySizingConfig",
    "CopyTradingConfig",
    "CopyTradingConfigError",
    "CopyTradingConfigV2",
    "LeaderEvent",
    "LendingPayload",
    "LPPayload",
    "PerpPayload",
    "SizingMode",
    "SwapPayload",
]
