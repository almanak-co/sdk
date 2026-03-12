"""Health endpoint for the BacktestService."""

from __future__ import annotations

from collections.abc import Callable

from fastapi import APIRouter

from almanak.services.backtest.models import HealthResponse

router = APIRouter(prefix="/api/v1", tags=["health"])

# These will be set by app.py at startup via the lifespan
_version: str = "0.1.0"
_start_time: float = 0.0
_get_active_backtest_jobs: Callable[[], int] = lambda: 0  # noqa: E731
_get_active_paper_sessions: Callable[[], int] = lambda: 0  # noqa: E731


def configure(
    *,
    version: str,
    start_time: float,
    get_active_backtest_jobs: Callable[[], int],
    get_active_paper_sessions: Callable[[], int],
) -> None:
    """Configure health endpoint with runtime dependencies.

    Called once at app startup from the lifespan context manager.
    """
    global _version, _start_time, _get_active_backtest_jobs, _get_active_paper_sessions
    _version = version
    _start_time = start_time
    _get_active_backtest_jobs = get_active_backtest_jobs
    _get_active_paper_sessions = get_active_paper_sessions


@router.get("/health")
async def health() -> HealthResponse:
    """Service health and capacity."""
    import time

    return HealthResponse(
        status="ok",
        version=_version,
        active_backtest_jobs=_get_active_backtest_jobs(),
        active_paper_sessions=_get_active_paper_sessions(),
        uptime_seconds=round(time.time() - _start_time, 1) if _start_time else 0.0,
    )
