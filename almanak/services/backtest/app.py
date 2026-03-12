"""FastAPI application assembly for the BacktestService.

This is the main entry point. It creates the FastAPI app,
mounts all routers, and manages service-level state via lifespan.
"""

from __future__ import annotations

import time
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from almanak.services.backtest.config import BacktestServiceConfig
from almanak.services.backtest.routers import backtest as backtest_router
from almanak.services.backtest.routers import fee_models as fee_models_router
from almanak.services.backtest.routers import health as health_router
from almanak.services.backtest.routers import paper_trade as paper_trade_router
from almanak.services.backtest.services.job_manager import JobManager
from almanak.services.backtest.services.paper_trade_manager import PaperTradeManager

VERSION = "0.1.0"

# Service-level singletons (initialized in lifespan)
job_manager: JobManager | None = None
paper_trade_manager: PaperTradeManager | None = None
config: BacktestServiceConfig | None = None


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
    """App lifespan: initialize and tear down service state."""
    global job_manager, paper_trade_manager, config

    config = BacktestServiceConfig.from_env()
    job_manager = JobManager(max_concurrent=config.max_concurrent_backtest_jobs)
    paper_trade_manager = PaperTradeManager(max_sessions=config.max_concurrent_paper_sessions)

    # Configure routers with runtime dependencies
    backtest_router.configure(job_manager=job_manager)
    paper_trade_router.configure(paper_trade_manager=paper_trade_manager)
    health_router.configure(
        version=VERSION,
        start_time=time.time(),
        get_active_backtest_jobs=lambda: job_manager.active_count if job_manager else 0,
        get_active_paper_sessions=lambda: paper_trade_manager.active_count if paper_trade_manager else 0,
    )

    yield

    # Cleanup
    job_manager = None
    paper_trade_manager = None
    config = None


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    app = FastAPI(
        title="Almanak BacktestService",
        description="HTTP API for backtesting, paper trading, and fee model queries.",
        version=VERSION,
        lifespan=lifespan,
    )

    # Mount routers
    app.include_router(health_router.router)
    app.include_router(backtest_router.router)
    app.include_router(paper_trade_router.router)
    app.include_router(fee_models_router.router)

    return app


# Default app instance for `uvicorn almanak.services.backtest.app:app`
app = create_app()
