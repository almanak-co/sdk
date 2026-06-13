"""Tests for the sync/async event-loop bridging helpers.

These pin the exact bridging semantics that backtest adapters rely on when
calling async data providers from synchronous code:

- refuse-detection (inside a task on a running loop) vs the two runnable
  states (no loop at all; running loop but no current task),
- private-loop lifecycle (created, used, closed),
- thread-safe scheduling on an already running loop,
- future cancellation when the timeout fires or any other exception aborts the
  wait.
"""

import asyncio
import threading
from collections.abc import Iterator

import pytest

from almanak.framework.backtesting.adapters._sync_bridge import (
    in_running_event_loop_task,
    run_coroutine_blocking,
)


class TestInRunningEventLoopTask:
    def test_false_without_running_loop(self) -> None:
        assert in_running_event_loop_task() is False

    def test_true_inside_task(self) -> None:
        async def probe() -> bool:
            return in_running_event_loop_task()

        assert asyncio.run(probe()) is True

    def test_false_in_loop_callback_without_task(self) -> None:
        """A running loop with no current task is the subtle third state."""
        loop = asyncio.new_event_loop()
        observed: list[bool] = []
        try:

            def callback() -> None:
                observed.append(in_running_event_loop_task())
                loop.stop()

            loop.call_soon(callback)
            loop.run_forever()
        finally:
            loop.close()

        assert observed == [False]


@pytest.fixture
def background_loop() -> Iterator[asyncio.AbstractEventLoop]:
    """An event loop running in a daemon thread, stopped and closed on exit."""
    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=loop.run_forever, daemon=True)
    thread.start()
    try:
        yield loop
    finally:
        loop.call_soon_threadsafe(loop.stop)
        thread.join(timeout=5)
        assert not thread.is_alive(), "background_loop thread failed to stop before close()"
        loop.close()


class TestRunCoroutineBlocking:
    def test_without_loop_runs_and_closes_private_loop(self, monkeypatch: pytest.MonkeyPatch) -> None:
        created: list[asyncio.AbstractEventLoop] = []
        real_new_event_loop = asyncio.new_event_loop

        def tracking_new_event_loop() -> asyncio.AbstractEventLoop:
            loop = real_new_event_loop()
            created.append(loop)
            return loop

        monkeypatch.setattr(asyncio, "new_event_loop", tracking_new_event_loop)

        async def coro() -> int:
            return 42

        assert run_coroutine_blocking(coro, timeout=5) == 42
        assert len(created) == 1
        assert created[0].is_closed()

    def test_with_running_loop_schedules_threadsafe(
        self,
        monkeypatch: pytest.MonkeyPatch,
        background_loop: asyncio.AbstractEventLoop,
    ) -> None:
        monkeypatch.setattr(asyncio, "get_running_loop", lambda: background_loop)

        async def coro() -> str:
            return "via-threadsafe"

        assert run_coroutine_blocking(coro, timeout=5) == "via-threadsafe"

    def test_timeout_cancels_future(
        self,
        monkeypatch: pytest.MonkeyPatch,
        background_loop: asyncio.AbstractEventLoop,
    ) -> None:
        monkeypatch.setattr(asyncio, "get_running_loop", lambda: background_loop)
        cancelled = threading.Event()

        async def slow() -> None:
            try:
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                cancelled.set()
                raise

        with pytest.raises(TimeoutError):
            run_coroutine_blocking(slow, timeout=0.05)

        assert cancelled.wait(timeout=5)

    def test_cancels_future_on_non_timeout_exception(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Any BaseException from future.result -- not only TimeoutError -- cancels the future.

        Guards the leak the bare ``except TimeoutError`` left open: a
        KeyboardInterrupt (or any other BaseException) raised in the waiting
        thread must still cancel the scheduled coroutine instead of orphaning
        it on the loop.
        """

        class Boom(BaseException):
            pass

        class FakeFuture:
            def __init__(self) -> None:
                self.cancelled = False

            def result(self, timeout: float | None = None) -> object:
                raise Boom

            def cancel(self) -> bool:
                self.cancelled = True
                return True

        fake = FakeFuture()

        def fake_schedule(coro: object, loop: object) -> FakeFuture:
            coro.close()  # the coroutine is never scheduled; close it to avoid a warning
            return fake

        monkeypatch.setattr(asyncio, "get_running_loop", lambda: object())
        monkeypatch.setattr(asyncio, "run_coroutine_threadsafe", fake_schedule)

        async def coro() -> None:
            return None

        with pytest.raises(Boom):
            run_coroutine_blocking(coro, timeout=5)

        assert fake.cancelled is True
