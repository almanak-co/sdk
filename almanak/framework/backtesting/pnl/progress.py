"""Optional run-scoped observation; transport belongs to the caller."""

import logging
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, replace
from typing import Literal

Phase = Literal["preparing", "loading_data", "simulating", "calculating_results", "saving_results"]


LoadingStage = Literal["prices", "pool_state", "pool_ohlcv", "funding"]


@dataclass(frozen=True)
class LoadingProgress:
    stage: LoadingStage
    completed_batches: int
    total_batches: int
    elapsed_ms: int


@dataclass(frozen=True)
class BacktestProgress:
    phase: Phase
    completed_ticks: int = 0
    total_ticks: int = 0
    simulation_elapsed_ms: int = 0
    loading: LoadingProgress | None = None


@dataclass
class _Observer:
    callback: Callable[[BacktestProgress], None]
    simulation_started: float | None = None
    progress: BacktestProgress = BacktestProgress("preparing")


_observer: ContextVar[_Observer | None] = ContextVar("backtest_progress_observer", default=None)


@contextmanager
def progress_scope(callback: Callable[[BacktestProgress], None]) -> Iterator[None]:
    """Observe this async context without changing standalone SDK behavior."""
    token = _observer.set(_Observer(callback))
    try:
        yield
    finally:
        _observer.reset(token)


def report_progress(phase: Phase, completed_ticks: int = 0, total_ticks: int = 0) -> None:
    observer = _observer.get()
    if observer is None:
        return
    now = time.monotonic()
    if phase == "simulating" and observer.simulation_started is None:
        observer.simulation_started = now
    elapsed = observer.progress.simulation_elapsed_ms
    if observer.simulation_started is not None and (phase == "simulating" or observer.progress.phase == "simulating"):
        # Include final pending-intent execution up to the phase transition,
        # then freeze the simulation clock throughout calculation/publication.
        elapsed = max(0, int((now - observer.simulation_started) * 1000))
    if phase == "simulating":
        observer.progress = BacktestProgress(
            phase, completed_ticks, max(observer.progress.total_ticks, total_ticks, completed_ticks), elapsed
        )
    else:
        observer.progress = replace(observer.progress, phase=phase, simulation_elapsed_ms=elapsed, loading=None)
    try:
        observer.callback(observer.progress)
    except Exception:
        logging.getLogger(__name__).debug("Backtest progress observer failed", exc_info=True)


@contextmanager
def loading_batches(stage: LoadingStage, total: int) -> Iterator[Callable[[], None]]:
    """Measure one loading operation; retries count only after a logical batch succeeds.

    Provider calls during simulation must never replace simulation progress.
    Totals describe this operation, not all data needed by the run.
    """
    observer = _observer.get()
    if observer is None or observer.progress.phase != "loading_data" or total <= 0:
        yield lambda: None
        return
    started = time.monotonic()
    completed = 0
    latest: LoadingProgress | None = None

    def publish() -> None:
        nonlocal latest
        if observer.progress.phase != "loading_data":
            return
        latest = LoadingProgress(stage, completed, total, max(0, int((time.monotonic() - started) * 1000)))
        observer.progress = replace(observer.progress, loading=latest)
        try:
            observer.callback(observer.progress)
        except Exception:
            logging.getLogger(__name__).debug("Backtest loading observer failed", exc_info=True)

    def advance() -> None:
        nonlocal completed
        completed = min(total, completed + 1)
        publish()

    publish()
    try:
        yield advance
    finally:
        if observer.progress.loading is latest:
            observer.progress = replace(observer.progress, loading=None)
            try:
                observer.callback(observer.progress)
            except Exception:
                logging.getLogger(__name__).debug("Backtest loading observer failed", exc_info=True)
