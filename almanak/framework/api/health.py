"""Health API Endpoint for system status checking.

This module provides a health endpoint that the dashboard can use to:
1. Check if the API server is running
2. Check if any strategy runners are active
3. Determine which features are available (live actions vs preview-only)
"""

from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel


class RunningStrategy(BaseModel):
    """Info about a running strategy."""

    strategy_id: str
    status: str  # "running", "paused", "stuck"
    started_at: str
    last_iteration_at: str | None = None


class HealthResponse(BaseModel):
    """Health check response."""

    # Basic health
    status: str  # "healthy", "degraded", "unhealthy"
    timestamp: str

    # API status
    api_running: bool = True
    api_version: str = "2.0.0"

    # Runner status
    runners_active: int = 0
    running_strategies: list[RunningStrategy] = []

    # Feature availability
    features: dict[str, bool] = {}


# In-memory tracking of running strategies
# This would be populated by the strategy runner when it starts/stops
_running_strategies: dict[str, dict[str, Any]] = {}


def register_running_strategy(
    strategy_id: str,
    status: str = "running",
) -> None:
    """Register a strategy as running (called by StrategyRunner).

    Args:
        strategy_id: The strategy ID
        status: Current status
    """
    _running_strategies[strategy_id] = {
        "strategy_id": strategy_id,
        "status": status,
        "started_at": datetime.now(UTC).isoformat(),
        "last_iteration_at": datetime.now(UTC).isoformat(),
    }


def unregister_running_strategy(strategy_id: str) -> None:
    """Unregister a strategy (called when runner stops).

    Args:
        strategy_id: The strategy ID to unregister
    """
    _running_strategies.pop(strategy_id, None)


def update_strategy_status(strategy_id: str, status: str) -> None:
    """Update a running strategy's status.

    Args:
        strategy_id: The strategy ID
        status: New status
    """
    if strategy_id in _running_strategies:
        _running_strategies[strategy_id]["status"] = status
        _running_strategies[strategy_id]["last_iteration_at"] = datetime.now(UTC).isoformat()


def get_running_strategies() -> dict[str, dict[str, Any]]:
    """Get all running strategies."""
    return _running_strategies.copy()


# FastAPI Router
router = APIRouter(tags=["health"])


@router.get("/api/health")
async def health_check() -> HealthResponse:
    """Check system health and feature availability.

    Returns information about:
    - API server status
    - Running strategy runners
    - Available features based on system state

    The dashboard uses this to:
    - Show connection status
    - Enable/disable action buttons
    - Display appropriate warnings
    """
    running = list(_running_strategies.values())
    runners_active = len(running)

    # Determine feature availability
    # Live actions require a running strategy
    features = {
        # Always available (preview/read-only)
        "view_strategies": True,
        "view_timeline": True,
        "view_config": True,
        "preview_teardown": True,
        # Require API but not necessarily running strategy
        "api_available": True,
        # Require running strategy runner
        "pause_resume": runners_active > 0,
        "bump_gas": runners_active > 0,
        "cancel_tx": runners_active > 0,
        "execute_teardown": runners_active > 0,
        "hot_reload_config": runners_active > 0,
    }

    # Determine overall status
    if runners_active > 0:
        status = "healthy"
    else:
        status = "degraded"  # API running but no strategies

    return HealthResponse(
        status=status,
        timestamp=datetime.now(UTC).isoformat(),
        api_running=True,
        api_version="2.0.0",
        runners_active=runners_active,
        running_strategies=[RunningStrategy(**s) for s in running],
        features=features,
    )


@router.get("/api/health/strategies/{strategy_id}")
async def strategy_health(strategy_id: str) -> dict[str, Any]:
    """Check if a specific strategy is running.

    Args:
        strategy_id: The strategy to check

    Returns:
        Status information for the strategy
    """
    if strategy_id in _running_strategies:
        return {
            "running": True,
            "strategy_id": strategy_id,
            **_running_strategies[strategy_id],
            "features": {
                "pause_resume": True,
                "bump_gas": True,
                "cancel_tx": True,
                "execute_teardown": True,
                "hot_reload_config": True,
            },
        }
    else:
        return {
            "running": False,
            "strategy_id": strategy_id,
            "features": {
                "pause_resume": False,
                "bump_gas": False,
                "cancel_tx": False,
                "execute_teardown": False,
                "hot_reload_config": False,
            },
            "message": "Strategy runner not active. Start the CLI to enable live actions.",
        }
