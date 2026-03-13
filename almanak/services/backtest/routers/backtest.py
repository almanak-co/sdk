"""Backtest endpoints: async submit/poll and synchronous quick check."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from decimal import Decimal

from fastapi import APIRouter, BackgroundTasks, HTTPException

from almanak.services.backtest.models import (
    BacktestJobResponse,
    BacktestMetricsResponse,
    BacktestRequest,
    BacktestResultResponse,
    JobStatus,
    QuickBacktestRequest,
    QuickBacktestResponse,
    StrategyListResponse,
)
from almanak.services.backtest.services.backtest_runner import (
    create_backtester,
    list_available_strategies,
    resolve_strategy,
    run_backtest_job,
)
from almanak.services.backtest.services.job_manager import JobManager

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["backtest"])

# Will be set by app.py at startup
_job_manager: JobManager | None = None


def configure(*, job_manager: JobManager) -> None:
    """Wire in the job manager dependency from app lifespan."""
    global _job_manager
    _job_manager = job_manager


def _get_job_manager() -> JobManager:
    if _job_manager is None:
        raise RuntimeError("BacktestService not initialized")
    return _job_manager


@router.post("/backtest", status_code=202)
async def submit_backtest(
    request: BacktestRequest,
    background_tasks: BackgroundTasks,
) -> BacktestJobResponse:
    """Submit an async backtest job.

    Two ways to specify a strategy:
    - ``strategy_name``: name of a registered SDK strategy (e.g. "demo_uniswap_rsi")
    - ``strategy_spec``: declarative spec (protocol + action + params)

    Returns immediately with a job_id. Poll GET /backtest/{job_id} for results.
    """
    jm = _get_job_manager()

    try:
        job_id = jm.create_job()
    except RuntimeError as e:
        raise HTTPException(status_code=429, detail=str(e)) from e

    background_tasks.add_task(
        run_backtest_job,
        job_id=job_id,
        request=request,
        job_manager=jm,
    )

    job = jm.get_job(job_id)
    return BacktestJobResponse(
        job_id=job_id,
        status=JobStatus.PENDING,
        created_at=job.created_at if job else datetime.now(UTC),
    )


@router.get("/backtest/{job_id}")
async def get_backtest_status(job_id: str) -> BacktestJobResponse:
    """Poll status and results of a backtest job."""
    jm = _get_job_manager()
    job = jm.get_job(job_id)

    if job is None:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

    result_response = None
    if job.status == JobStatus.COMPLETE and job.result:
        metrics_data = job.result.get("metrics", {})
        result_response = BacktestResultResponse(
            metrics=BacktestMetricsResponse.model_validate(metrics_data),
            equity_curve=job.result.get("equity_curve", []),
            trades=job.result.get("trades", []),
            duration_seconds=job.result.get("duration_seconds", 0.0),
        )

    return BacktestJobResponse(
        job_id=job.job_id,
        status=job.status,
        progress=job.progress,
        result=result_response,
        error=job.error,
        created_at=job.created_at,
        completed_at=job.completed_at,
    )


@router.post("/backtest/quick")
async def quick_backtest(request: QuickBacktestRequest) -> QuickBacktestResponse:
    """Synchronous quick eligibility check (<30s).

    Uses a 7-day window with simplified fees for fast signal validation.
    Supports both strategy_name and strategy_spec.
    """
    import time

    start_time = time.monotonic()

    try:
        strategy, config = resolve_strategy(request, quick=True)

        backtester = create_backtester()
        try:
            result = await backtester.backtest(strategy, config)
        finally:
            await backtester.close()

        metrics = result.metrics
        duration = time.monotonic() - start_time

        return QuickBacktestResponse(
            eligible=metrics.sharpe_ratio > 0 and metrics.max_drawdown_pct < Decimal("0.5"),
            metrics=BacktestMetricsResponse.model_validate(metrics.to_dict()),
            duration_seconds=round(duration, 2),
        )

    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e)) from None
    except Exception:
        logger.exception("Quick backtest failed")
        raise HTTPException(status_code=500, detail="Quick backtest failed. Check server logs for details.") from None


@router.get("/strategies")
async def list_strategies() -> StrategyListResponse:
    """List all registered SDK strategies available for backtesting.

    Edge can use any of these names in the ``strategy_name`` field of
    POST /backtest or POST /backtest/quick.
    """
    strategies = list_available_strategies()
    return StrategyListResponse(strategies=sorted(strategies), count=len(strategies))
