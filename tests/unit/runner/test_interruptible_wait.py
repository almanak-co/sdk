"""Regression test for VIB-5528: _interruptible_wait returns early on a stop signal.

Before the fix, the inter-iteration sleep was a bare asyncio.sleep(interval).
A queued stop would not be detected until the sleep ended — up to the full
--interval (production default 60-180s). After the fix, _interruptible_wait
polls every _WAIT_POLL_SLICE_SECONDS and exits immediately on a STOP command or
a pending teardown request.

Both polls are synchronous gateway I/O and must run via asyncio.to_thread so the
event loop is never blocked; the tests patch to_thread with a synchronous
delegate that records which callables were dispatched off-loop.
"""

from contextlib import contextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.runner.strategy_runner import _WAIT_POLL_SLICE_SECONDS, StrategyRunner


def _make_runner() -> StrategyRunner:
    config = MagicMock()
    config.max_consecutive_errors = 3
    config.default_interval_seconds = 60
    runner = StrategyRunner.__new__(StrategyRunner)
    runner.config = config
    runner._shutdown_requested = False
    return runner


def _make_strategy(deployment_id: str = "deployment:test0000abcd") -> MagicMock:
    strategy = MagicMock()
    strategy.deployment_id = deployment_id
    # Default: no pending teardown. A bare MagicMock attribute would auto-return
    # a truthy MagicMock from should_teardown(), spuriously tripping the
    # teardown probe in _interruptible_wait; pin it False so each test opts in.
    strategy.should_teardown = MagicMock(return_value=False)
    return strategy


@contextmanager
def _patched_io(sleep_calls: list[float], to_thread_fns: list | None = None):
    """Patch asyncio.sleep (no real sleep) and asyncio.to_thread (synchronous delegate).

    to_thread is replaced with a coroutine that calls the dispatched function
    inline and records it, so tests stay deterministic (no executor threads) AND
    can assert that the synchronous gateway polls were dispatched off the event
    loop rather than called directly.
    """

    async def fake_to_thread(fn, *args, **kwargs):
        if to_thread_fns is not None:
            to_thread_fns.append(fn)
        return fn(*args, **kwargs)

    with (
        patch(
            "almanak.framework.runner.strategy_runner.asyncio.sleep",
            new_callable=AsyncMock,
            side_effect=lambda d: sleep_calls.append(d) or None,
        ),
        patch(
            "almanak.framework.runner.strategy_runner.asyncio.to_thread",
            new=fake_to_thread,
        ),
    ):
        yield


class TestInterruptibleWait:
    """_interruptible_wait exits within one poll slice when a stop signal arrives."""

    @pytest.mark.asyncio
    async def test_stop_command_during_wait_returns_early(self):
        """A STOP queued after the first poll slice causes early return.

        With interval=120 and _WAIT_POLL_SLICE_SECONDS=15, a bare sleep would
        take 120s. With the fix, the wait returns after one poll slice (15s
        in production; 0s in the test because asyncio.sleep is mocked).
        """
        runner = _make_runner()
        strategy = _make_strategy()
        deployment_id = strategy.deployment_id

        sleep_calls: list[float] = []
        to_thread_fns: list = []

        # First poll returns None (no command yet), second returns "STOP".
        runner._lifecycle_poll_command = MagicMock(side_effect=[None, "STOP"])

        handle_lc_calls: list[tuple] = []

        async def fake_handle_lifecycle_command(r, s, dep_id, cmd):
            handle_lc_calls.append((dep_id, cmd))

        with (
            _patched_io(sleep_calls, to_thread_fns),
            patch(
                "almanak.framework.runner._run_loop_helpers.handle_lifecycle_command",
                new=fake_handle_lifecycle_command,
            ),
        ):
            await runner._interruptible_wait(deployment_id, 120, strategy)

        # Should have slept twice: once before the None poll, once before the STOP poll.
        assert len(sleep_calls) == 2
        # Each slice should be _WAIT_POLL_SLICE_SECONDS (120 >> 15).
        assert all(s == _WAIT_POLL_SLICE_SECONDS for s in sleep_calls)
        # The STOP command must have been routed to handle_lifecycle_command.
        assert len(handle_lc_calls) == 1
        assert handle_lc_calls[0] == (deployment_id, "STOP")
        # _lifecycle_poll_command called twice (once per sleep slice).
        assert runner._lifecycle_poll_command.call_count == 2
        # The synchronous gRPC poll must be dispatched off-loop via asyncio.to_thread.
        assert any(fn is runner._lifecycle_poll_command for fn in to_thread_fns)

    @pytest.mark.asyncio
    async def test_non_stop_command_does_not_break_wait(self):
        """A retired PAUSE / unknown command is handled but does NOT end the wait early.

        handle_lifecycle_command processes-and-ignores PAUSE/RESUME/unknown; the wait
        must keep sleeping the remaining slices rather than waking the runner up.
        """
        runner = _make_runner()
        strategy = _make_strategy()
        deployment_id = strategy.deployment_id

        sleep_calls: list[float] = []
        # PAUSE on the first slice, then None for the rest. interval=45 → 3 slices.
        runner._lifecycle_poll_command = MagicMock(side_effect=["PAUSE", None, None])

        handle_lc_calls: list[tuple] = []

        async def fake_handle_lifecycle_command(r, s, dep_id, cmd):
            handle_lc_calls.append((dep_id, cmd))

        with (
            _patched_io(sleep_calls),
            patch(
                "almanak.framework.runner._run_loop_helpers.handle_lifecycle_command",
                new=fake_handle_lifecycle_command,
            ),
        ):
            await runner._interruptible_wait(deployment_id, 45, strategy)

        # PAUSE was routed to the handler...
        assert (deployment_id, "PAUSE") in handle_lc_calls
        # ...but the wait did NOT return early: all 3 slices were slept.
        assert len(sleep_calls) == 3
        assert runner._lifecycle_poll_command.call_count == 3

    @pytest.mark.asyncio
    async def test_no_command_sleeps_full_interval_in_slices(self):
        """When no command arrives, the full interval is slept in poll-slice chunks."""
        runner = _make_runner()
        strategy = _make_strategy()
        deployment_id = strategy.deployment_id

        sleep_calls: list[float] = []
        # interval=45 → 3 slices of 15s each
        runner._lifecycle_poll_command = MagicMock(return_value=None)

        with _patched_io(sleep_calls):
            await runner._interruptible_wait(deployment_id, 45, strategy)

        assert len(sleep_calls) == 3
        assert all(s == _WAIT_POLL_SLICE_SECONDS for s in sleep_calls)

    @pytest.mark.asyncio
    async def test_interval_smaller_than_slice_uses_single_sleep(self):
        """For interval <= _WAIT_POLL_SLICE_SECONDS, one sleep of exactly interval."""
        runner = _make_runner()
        strategy = _make_strategy()
        deployment_id = strategy.deployment_id

        sleep_calls: list[float] = []
        runner._lifecycle_poll_command = MagicMock(return_value=None)

        with _patched_io(sleep_calls):
            await runner._interruptible_wait(deployment_id, 7, strategy)

        assert sleep_calls == [7]

    @pytest.mark.asyncio
    async def test_shutdown_requested_skips_remaining_slices(self):
        """If _shutdown_requested is set between slices, exit without sleeping more."""
        runner = _make_runner()
        strategy = _make_strategy()
        deployment_id = strategy.deployment_id

        sleep_calls: list[float] = []

        def poll_and_shutdown(_deployment_id):
            runner._shutdown_requested = True
            return None

        runner._lifecycle_poll_command = MagicMock(side_effect=poll_and_shutdown)

        with _patched_io(sleep_calls):
            await runner._interruptible_wait(deployment_id, 120, strategy)

        # One sleep before the poll that sets shutdown, then exit.
        assert len(sleep_calls) == 1

    @pytest.mark.asyncio
    async def test_teardown_request_during_wait_returns_early(self):
        """A direct teardown request (not a lifecycle STOP) also exits the wait early.

        The dashboard STOP lane and the `almanak strat teardown request` lane must
        both honor the ~15s SLA. should_teardown() flips True on the second poll;
        the wait must return without sleeping the full --interval.
        """
        runner = _make_runner()
        strategy = _make_strategy()
        deployment_id = strategy.deployment_id

        sleep_calls: list[float] = []
        to_thread_fns: list = []
        # No lifecycle command on either slice.
        runner._lifecycle_poll_command = MagicMock(return_value=None)
        # should_teardown: False on the first slice, True on the second.
        strategy.should_teardown = MagicMock(side_effect=[False, True])

        with _patched_io(sleep_calls, to_thread_fns):
            await runner._interruptible_wait(deployment_id, 120, strategy)

        # Two slices: one before the False probe, one before the True probe → return.
        assert len(sleep_calls) == 2
        assert all(s == _WAIT_POLL_SLICE_SECONDS for s in sleep_calls)
        assert strategy.should_teardown.call_count == 2
        # The teardown probe must be dispatched off-loop via asyncio.to_thread.
        # (Bound methods compare by ==; MagicMock compares == by identity, so the
        # lifecycle-poll mock in the same list will not false-match.)
        assert any(fn == runner._pending_teardown_signal for fn in to_thread_fns)

    @pytest.mark.asyncio
    async def test_teardown_probe_error_is_swallowed(self):
        """A raising should_teardown() must not crash the wait (Step 0a is authoritative)."""
        runner = _make_runner()
        strategy = _make_strategy()
        deployment_id = strategy.deployment_id

        sleep_calls: list[float] = []
        runner._lifecycle_poll_command = MagicMock(return_value=None)
        strategy.should_teardown = MagicMock(side_effect=RuntimeError("gateway hiccup"))

        with _patched_io(sleep_calls):
            # interval=30 → 2 slices; the probe raises each slice but is swallowed,
            # so the wait runs to completion rather than blowing up.
            await runner._interruptible_wait(deployment_id, 30, strategy)

        assert len(sleep_calls) == 2

    @pytest.mark.asyncio
    async def test_zero_interval_yields_once_without_slicing(self):
        """interval <= 0 ('no inter-iteration delay') preserves a single cooperative yield.

        The pre-fix code ran `await asyncio.sleep(interval)`; for interval=0 that is a
        single yield to the event loop. The slice loop's `while remaining > 0` guard
        would otherwise skip the body entirely and return with no await, risking
        event-loop starvation in a tight interval=0 loop.
        """
        runner = _make_runner()
        strategy = _make_strategy()
        deployment_id = strategy.deployment_id

        sleep_calls: list[float] = []
        runner._lifecycle_poll_command = MagicMock(return_value=None)
        strategy.should_teardown = MagicMock(return_value=False)

        with _patched_io(sleep_calls):
            await runner._interruptible_wait(deployment_id, 0, strategy)

        # Exactly one yield (sleep(0)); no command/teardown polling for the zero case.
        assert sleep_calls == [0]
        runner._lifecycle_poll_command.assert_not_called()
        strategy.should_teardown.assert_not_called()
