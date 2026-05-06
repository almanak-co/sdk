"""Single async-to-sync bridge for VIB-4062.

PRD §4.1 — replaces ad-hoc ``_run_async()`` helpers scattered across the two
legacy MarketSnapshot copies. Centralizing the bridge makes timeout policy,
error handling, and cancel semantics consistent.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import threading
from collections.abc import Awaitable
from typing import TypeVar

T = TypeVar("T")

DEFAULT_TIMEOUT_SEC: float = 30.0


def run_sync(
    coro: Awaitable[T],
    *,
    timeout_sec: float = DEFAULT_TIMEOUT_SEC,
) -> T:
    """Run ``coro`` to completion synchronously, raising on timeout.

    If a running asyncio loop exists in the current thread (e.g., we are
    inside an async test), the coroutine is executed in a worker thread so we
    do not deadlock the running loop. Otherwise we use ``asyncio.run`` in
    this thread.
    """
    try:
        running = asyncio.get_running_loop()
    except RuntimeError:
        running = None

    if running is None:
        # No running loop in this thread — drive the coroutine here.
        return asyncio.run(_run_with_timeout(coro, timeout_sec))

    # Running loop present — push to a worker thread.
    result_holder: dict[str, object] = {}

    def _runner() -> None:
        try:
            result_holder["value"] = asyncio.run(_run_with_timeout(coro, timeout_sec))
        except BaseException as exc:  # noqa: BLE001 — propagate via holder
            result_holder["error"] = exc

    t = threading.Thread(target=_runner, name="market-sync-bridge", daemon=True)
    t.start()
    t.join(timeout=timeout_sec + 1.0)
    if t.is_alive():
        raise concurrent.futures.TimeoutError(f"sync_bridge: thread did not finish in {timeout_sec}s")
    if "error" in result_holder:
        raise result_holder["error"]  # type: ignore[misc]
    return result_holder["value"]  # type: ignore[return-value]


async def _run_with_timeout(coro: Awaitable[T], timeout_sec: float) -> T:
    return await asyncio.wait_for(coro, timeout=timeout_sec)
