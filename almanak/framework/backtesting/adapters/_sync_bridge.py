"""Synchronous bridging helpers for async data-provider calls.

Backtest adapters expose synchronous interfaces (``update_position``,
``execute_intent``) while the data providers they consume are async. These
helpers centralize the event-loop bridging policy so adapters do not
re-implement it inline:

- :func:`in_running_event_loop_task` detects the one situation where blocking
  is forbidden: the caller is *inside* a task on a running event loop.
- :func:`run_coroutine_blocking` runs a coroutine to completion from
  synchronous code, either by scheduling it thread-safely on an already
  running loop or by spinning up a private loop.

Callers must check :func:`in_running_event_loop_task` first and degrade (or
raise, in strict-historical mode) instead of calling
:func:`run_coroutine_blocking`, which would otherwise block the loop thread
until the timeout.
"""

import asyncio
from collections.abc import Callable, Coroutine
from typing import Any


def in_running_event_loop_task() -> bool:
    """Return True iff called from inside a task on a running event loop.

    A running loop with no current task (e.g. a plain ``call_soon`` callback)
    returns False; blocking there stalls the loop, but that matches the
    adapters' historical behaviour and is preserved deliberately.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return False
    return asyncio.current_task() is not None


def run_coroutine_blocking[T](
    coro_factory: Callable[[], Coroutine[Any, Any, T]],
    timeout: float,
) -> T:
    """Run ``coro_factory()`` to completion from synchronous code.

    With a running loop on this thread, the coroutine is scheduled
    thread-safely and awaited with ``timeout``; the future is cancelled before
    *any* exception propagates -- a timeout, a ``KeyboardInterrupt`` or other
    ``BaseException`` raised in the waiting thread, or a cancellation -- so a
    still-running coroutine is never left orphaned on the loop after the wait
    aborts. (On the normal path where the coroutine itself raised, the future
    is already done and ``cancel()`` is a harmless no-op.) Without a running
    loop, a private loop is created, used, and closed.

    A factory is taken instead of a coroutine object so that no coroutine is
    created (and left un-awaited) if loop acquisition itself fails.
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(coro_factory())
        finally:
            loop.close()
    future = asyncio.run_coroutine_threadsafe(coro_factory(), loop)
    try:
        return future.result(timeout=timeout)
    except BaseException:
        future.cancel()
        raise
