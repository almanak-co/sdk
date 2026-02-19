"""Almanak Strategy Framework v2.0 - API Endpoints"""

from .actions import (
    ActionResponse,
    BumpGasRequest,
    CancelTxRequest,
    ConfigUpdateRequest,
    ConfigUpdateResponse,
)
from .actions import (
    router as actions_router,
)
from .health import (
    HealthResponse,
    get_running_strategies,
    register_running_strategy,
    unregister_running_strategy,
    update_strategy_status,
)
from .health import (
    router as health_router,
)
from .teardown import (
    CancelResponse,
    ClosePreviewResponse,
    CloseStartedResponse,
    CloseStatusResponse,
)
from .teardown import (
    router as teardown_router,
)
from .timeline import (
    TimelineEvent,
    TimelineEventType,
    TimelineResponse,
)
from .timeline import (
    router as timeline_router,
)

__all__ = [
    # Actions API
    "actions_router",
    "ActionResponse",
    "BumpGasRequest",
    "CancelTxRequest",
    "ConfigUpdateRequest",
    "ConfigUpdateResponse",
    # Timeline API
    "timeline_router",
    "TimelineEvent",
    "TimelineEventType",
    "TimelineResponse",
    # Teardown API
    "teardown_router",
    "ClosePreviewResponse",
    "CloseStartedResponse",
    "CloseStatusResponse",
    "CancelResponse",
    # Health API
    "health_router",
    "HealthResponse",
    "register_running_strategy",
    "unregister_running_strategy",
    "update_strategy_status",
    "get_running_strategies",
]
