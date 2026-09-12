"""Retain failed-client cleanup independently of an RPC caller's deadline."""

import asyncio
import logging
import threading
from collections.abc import Awaitable, Callable
from typing import Any

logger = logging.getLogger(__name__)

_DISCONNECT_TIMEOUT_SECONDS = 1.0
_tasks: set[asyncio.Task[None]] = set()
_tasks_lock = threading.Lock()


async def _disconnect(disconnect: Callable[[], Awaitable[None]]) -> None:
    async with asyncio.timeout(_DISCONNECT_TIMEOUT_SECONDS):
        await disconnect()


def _cleanup_finished(task: asyncio.Task[None]) -> None:
    with _tasks_lock:
        _tasks.discard(task)
    if task.cancelled():
        logger.error("Async Web3 initialization cleanup cancelled before completion")
        return
    error = task.exception()
    if error is not None:
        logger.error("Async Web3 initialization cleanup failed error_type=%s", type(error).__name__)


def schedule_failed_client_cleanup(provider: Any) -> None:
    """Keep cleanup alive without delaying or replacing the original failure.

    web3 6 HTTP providers expose no disconnect method and own sessions through
    their library-global cache. Only providers offering an owned disconnect
    operation can be disposed here.
    """
    disconnect = getattr(provider, "disconnect", None)
    if disconnect is None:
        return
    task = asyncio.create_task(_disconnect(disconnect), name="failed-web3-client-cleanup")
    with _tasks_lock:
        _tasks.add(task)
    task.add_done_callback(_cleanup_finished)


async def drain_failed_client_cleanup(timeout: float = 1.1) -> None:
    """Wait for this gateway loop's retained cleanup within a shutdown budget.

    A cancelled drain does not cancel the retained tasks. Other gateway loops
    own their own cleanup; tasks that outlast shutdown remain retained and emit
    an explicit diagnostic instead of being silently discarded.
    """
    loop = asyncio.get_running_loop()
    with _tasks_lock:
        tasks = {task for task in _tasks if task.get_loop() is loop}
    if not tasks:
        return
    _, pending = await asyncio.wait(tasks, timeout=max(0.0, timeout))
    if pending:
        logger.error("Async Web3 initialization cleanup still pending at shutdown count=%d", len(pending))
