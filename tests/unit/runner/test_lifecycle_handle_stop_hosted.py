"""Tests for runner_gateway.lifecycle_handle_stop hosted-mode behavior.

When the platform sends a STOP command via the gateway lifecycle channel
(read by the runner each iteration), the local-mode path writes a teardown
request to the SQLite approval channel so the next iteration unwinds
positions then shuts down.

Hosted mode has no SQLite approval channel — the gateway/Postgres owns
teardown lifecycle (VIB-3777). Without a hosted-aware short-circuit,
``lifecycle_handle_stop`` falls into the generic except, logs ERROR
("Failed to create teardown request..."), and sets ``_shutdown_requested``
anyway — but with a confusing operator-facing error and no graceful path.

The fix: in hosted mode, STOP becomes a clean shutdown request without
attempting to construct ``TeardownStateManager``. Position unwind in
hosted mode is a separate channel.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from almanak.framework.runner import runner_gateway


def _make_runner_stub() -> MagicMock:
    """Minimal stand-in for StrategyRunner with the lifecycle surface used
    by lifecycle_handle_stop. Tests inspect _shutdown_requested and the
    _lifecycle_write_state call."""
    runner = MagicMock()
    runner._shutdown_requested = False
    runner._lifecycle_write_state = MagicMock()
    return runner


class TestLifecycleHandleStopHosted:
    """Hosted-mode STOP must not call get_teardown_state_manager (which
    raises LocalPathError) and must not log a misleading ERROR."""

    def test_hosted_mode_clean_shutdown(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("AGENT_ID", "test-agent-id")

        # Tripwire: must not construct the local teardown state manager.
        from almanak.framework import teardown as teardown_pkg

        def _trip(*_args, **_kwargs):
            raise AssertionError(
                "hosted lifecycle STOP must not construct TeardownStateManager"
            )

        monkeypatch.setattr(teardown_pkg, "get_teardown_state_manager", _trip)

        runner = _make_runner_stub()
        runner_gateway.lifecycle_handle_stop(runner, "strat-1:hash", strategy=None)

        # STOPPING state written + clean shutdown requested.
        runner._lifecycle_write_state.assert_called_once_with("strat-1:hash", "STOPPING")
        assert runner._shutdown_requested is True

    def test_hosted_mode_no_error_log(
        self, monkeypatch: pytest.MonkeyPatch, caplog
    ) -> None:
        """The user-visible bug: rc8/rc9/rc10 hosted STOP emitted ERROR
        ("Failed to create teardown request: local-path helper called in
        hosted mode...") because the generic except wrapped LocalPathError.
        Pin that no ERROR fires in the hosted-mode path."""
        import logging

        monkeypatch.setenv("AGENT_ID", "test-agent-id")
        caplog.set_level(logging.DEBUG)

        runner = _make_runner_stub()
        runner_gateway.lifecycle_handle_stop(runner, "strat-1:hash", strategy=None)

        errors = [
            r for r in caplog.records
            if r.levelno >= logging.ERROR
            and r.name.startswith("almanak.")
        ]
        assert errors == [], (
            f"hosted STOP must not log ERROR; got: "
            f"{[(r.name, r.getMessage()) for r in errors]}"
        )


class TestLifecycleHandleStopLocal:
    """Local mode keeps the existing behavior: writes a teardown request
    to the SQLite approval channel so the next iteration unwinds positions."""

    def test_local_mode_creates_teardown_request(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("AGENT_ID", raising=False)

        created: list = []
        from almanak.framework import teardown as teardown_pkg

        class _FakeManager:
            def create_request(self, request):
                created.append(request)

        monkeypatch.setattr(teardown_pkg, "get_teardown_state_manager", lambda: _FakeManager())

        runner = _make_runner_stub()
        runner_gateway.lifecycle_handle_stop(runner, "strat-1:hash", strategy=None)

        runner._lifecycle_write_state.assert_called_once_with("strat-1:hash", "STOPPING")
        assert len(created) == 1
        assert created[0].strategy_id == "strat-1:hash"
        assert created[0].requested_by == "lifecycle"
        # Local mode does NOT immediately set _shutdown_requested on success —
        # the next iteration's _check_teardown_requested handles the unwind
        # and then calls request_shutdown.
        assert runner._shutdown_requested is False

    def test_local_mode_create_failure_falls_through_to_hard_stop(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """If create_request raises (e.g., SQLite full), local mode falls
        through to hard-stop and logs ERROR. Behavior unchanged from before
        the hosted-mode short-circuit was added."""
        monkeypatch.delenv("AGENT_ID", raising=False)

        from almanak.framework import teardown as teardown_pkg

        class _BrokenManager:
            def create_request(self, _request):
                raise RuntimeError("simulated SQLite failure")

        monkeypatch.setattr(teardown_pkg, "get_teardown_state_manager", lambda: _BrokenManager())

        runner = _make_runner_stub()
        runner_gateway.lifecycle_handle_stop(runner, "strat-1:hash", strategy=None)

        assert runner._shutdown_requested is True
