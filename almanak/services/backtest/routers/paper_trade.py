"""Paper trading session endpoints: start, poll, and stop."""

from __future__ import annotations

import logging
from datetime import UTC, datetime

from fastapi import APIRouter, BackgroundTasks, HTTPException

from almanak.services.backtest.models import (
    PaperTradeRequest,
    PaperTradeSessionResponse,
    PaperTradeSessionStatus,
)
from almanak.services.backtest.services.paper_trade_manager import PaperTradeManager

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["paper-trade"])

# Will be set by app.py at startup
_paper_trade_manager: PaperTradeManager | None = None


def configure(*, paper_trade_manager: PaperTradeManager) -> None:
    """Wire in the paper trade manager dependency from app lifespan."""
    global _paper_trade_manager
    _paper_trade_manager = paper_trade_manager


def _get_manager() -> PaperTradeManager:
    if _paper_trade_manager is None:
        raise RuntimeError("BacktestService not initialized")
    return _paper_trade_manager


async def _run_paper_session(
    session_id: str,
    request: PaperTradeRequest,
    manager: PaperTradeManager,
) -> None:
    """Start and monitor a paper trading session in the background.

    v1 limitation: BackgroundPaperTrader.start() requires a Python module path
    on disk (strategy_module + strategy_class) because it spawns a
    multiprocessing.Process that imports the strategy at runtime. HTTP-submitted
    StrategySpec objects cannot be serialised into a loadable module path.

    Full HTTP paper trading requires one of:
      (a) An in-process PaperTrader adapter (no subprocess) — see PaperTrader.run()
      (b) A strategy registry that maps spec -> module path
    Both are planned for v2. For now, the PnL backtest endpoints provide the
    primary simulation capability; paper trading is available via CLI only.
    """
    try:
        manager.update_progress(session_id, 5.0, "Validating paper trading request...")

        manager.mark_failed(
            session_id,
            "Paper trading via HTTP is not yet supported. "
            "BackgroundPaperTrader requires a strategy module path on disk, "
            "which HTTP-submitted StrategySpec objects cannot provide. "
            "Use CLI `almanak strat backtest paper -d <strategy_dir>` instead. "
            "HTTP paper trading is planned for v2 (in-process PaperTrader adapter).",
        )

    except Exception as e:
        logger.exception("Paper trading session %s failed", session_id)
        manager.mark_failed(session_id, str(e))


@router.post("/paper-trade", status_code=202)
async def start_paper_trade(
    request: PaperTradeRequest,
    background_tasks: BackgroundTasks,
) -> PaperTradeSessionResponse:
    """Start a new paper trading session.

    Returns immediately with a session_id. Poll GET /paper-trade/{session_id}
    for status and live metrics.
    """
    mgr = _get_manager()

    try:
        session_id = mgr.create_session(
            strategy_id=f"spec_{request.strategy_spec.protocol}_{request.strategy_spec.action}",
            chain=request.chain,
        )
    except RuntimeError as e:
        raise HTTPException(status_code=429, detail=str(e)) from e

    background_tasks.add_task(_run_paper_session, session_id, request, mgr)

    session = mgr.get_session(session_id)
    return PaperTradeSessionResponse(
        session_id=session_id,
        status=PaperTradeSessionStatus.STARTING,
        created_at=session.created_at if session else datetime.now(UTC),
    )


@router.get("/paper-trade/{session_id}")
async def get_paper_trade_status(session_id: str) -> PaperTradeSessionResponse:
    """Poll status and live metrics of a paper trading session."""
    mgr = _get_manager()
    session = mgr.get_session(session_id)

    if session is None:
        raise HTTPException(status_code=404, detail=f"Session {session_id} not found")

    return PaperTradeSessionResponse(
        session_id=session.session_id,
        status=session.status,
        progress=session.progress,
        metrics=session.metrics,
        created_at=session.created_at,
        stopped_at=session.stopped_at,
    )


@router.delete("/paper-trade/{session_id}")
async def stop_paper_trade(session_id: str) -> PaperTradeSessionResponse:
    """Stop a paper trading session gracefully."""
    mgr = _get_manager()
    session = mgr.get_session(session_id)

    if session is None:
        raise HTTPException(status_code=404, detail=f"Session {session_id} not found")

    if session.status not in (PaperTradeSessionStatus.STARTING, PaperTradeSessionStatus.RUNNING):
        raise HTTPException(
            status_code=409,
            detail=f"Session {session_id} is already {session.status}",
        )

    mgr.mark_stopped(session_id)

    return PaperTradeSessionResponse(
        session_id=session.session_id,
        status=PaperTradeSessionStatus.STOPPED,
        progress=session.progress,
        metrics=session.metrics,
        created_at=session.created_at,
        stopped_at=session.stopped_at,
    )
