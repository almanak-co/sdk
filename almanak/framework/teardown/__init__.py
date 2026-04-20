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
"""

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
from almanak.framework.teardown.safety_guard import SafetyGuard
from almanak.framework.teardown.slippage_manager import EscalatingSlippageManager
from almanak.framework.teardown.state_manager import TeardownStateManager, get_teardown_state_manager
from almanak.framework.teardown.teardown_manager import TeardownManager

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
    "get_teardown_state_manager",
]
