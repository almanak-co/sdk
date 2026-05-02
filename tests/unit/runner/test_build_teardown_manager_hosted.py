"""Tests for _teardown_helpers hosted-mode behavior.

Two fixes covered:

1. ``build_teardown_manager`` — ``TeardownStateAdapter`` is SQLite-backed
   and constructs via ``local_strategy_db_path()`` which raises
   ``LocalPathError`` in hosted mode (AGENT_ID set). The fix returns
   ``adapter=None`` in hosted mode and passes it through to
   ``TeardownManager`` (which already supports state_manager=None via
   ``if self.state_manager:`` guards on every method).

2. ``execute_and_verify`` — defensive fail-fast for the (currently
   unreachable, but VIB-3777 could change that) hosted-manual-teardown
   path. The approval callback dereferences SQLite-only methods on the
   adapter; if a future change ever lands us here with adapter=None and
   not is_auto_mode, raise a clear hosted-not-supported error rather
   than AttributeError-ing inside the callback.
"""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

import pytest

from almanak.framework.runner import _teardown_helpers


def _make_runner_stub() -> MagicMock:
    runner = MagicMock()
    runner.execution_orchestrator = MagicMock()
    runner.alert_manager = MagicMock()
    return runner


class TestBuildTeardownManagerHosted:
    def test_hosted_mode_returns_adapter_none(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("AGENT_ID", "test-agent-id")

        # Tripwire: must not construct TeardownStateAdapter in hosted mode.
        from almanak.framework.teardown import state_manager as sm_mod

        def _trip(*_args, **_kwargs):
            raise AssertionError(
                "hosted build_teardown_manager must not construct TeardownStateAdapter"
            )

        monkeypatch.setattr(sm_mod, "TeardownStateAdapter", _trip)

        runner = _make_runner_stub()
        compiler = MagicMock()
        state_manager = MagicMock()

        teardown_mgr, adapter = _teardown_helpers.build_teardown_manager(
            runner, compiler, state_manager
        )

        assert adapter is None
        # TeardownManager constructed with state_manager=None — its methods
        # already short-circuit on this (every callsite is guarded by
        # `if self.state_manager:`).
        assert teardown_mgr.state_manager is None

    def test_local_mode_constructs_adapter_and_threads_db_path(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path
    ) -> None:
        """Local mode must thread ``state_manager.db_path`` into the adapter
        constructor — silently falling back to the default resolution would
        diverge runner-write and dashboard-read DBs (the May 1 silent-failure
        class). Assert the constructor arg, not just adapter existence."""
        monkeypatch.delenv("AGENT_ID", raising=False)
        explicit_db = tmp_path / "explicit-state.db"
        # Ensure the default resolver wouldn't accidentally land on this same
        # path — point ALMANAK_STATE_DB elsewhere so an unwired adapter would
        # produce a different db_path.
        monkeypatch.setenv("ALMANAK_STATE_DB", str(tmp_path / "default-state.db"))

        runner = _make_runner_stub()
        compiler = MagicMock()
        state_manager = MagicMock()
        state_manager.db_path = str(explicit_db)

        teardown_mgr, adapter = _teardown_helpers.build_teardown_manager(
            runner, compiler, state_manager
        )

        assert adapter is not None
        assert teardown_mgr.state_manager is adapter
        # The wiring contract: state_manager.db_path becomes the adapter's
        # db_path. If this regresses (e.g., the helper stops forwarding the
        # path), the adapter would silently fall back to the env default
        # `default-state.db` and runner/dashboard would read different DBs.
        assert str(adapter.db_path) == str(explicit_db)


class TestExecuteAndVerifyHostedManualGuard:
    """Defence-in-depth: if a future change ever routes a hosted manual
    teardown through ``execute_and_verify`` with ``adapter=None``, raise a
    clear error rather than AttributeError-ing inside the SQLite-only
    approval callback. Today this path is unreachable because hosted mode
    always derives ``is_auto_mode=True`` from ``request=None`` — the guard
    documents the invariant."""

    def test_hosted_manual_teardown_raises_clearly(self) -> None:
        # We invoke execute_and_verify just far enough to hit the guard
        # (the early build_approval block) — no need to mock the full
        # downstream pipeline.
        runner = MagicMock()
        teardown_mgr = MagicMock()
        teardown_state = MagicMock()
        teardown_state.teardown_id = "td_abc"
        strategy = MagicMock()
        strategy.strategy_id = "strat-1:hash"

        with pytest.raises(RuntimeError, match="not yet supported in hosted mode"):
            asyncio.run(
                _teardown_helpers.execute_and_verify(
                    runner=runner,
                    teardown_mgr=teardown_mgr,
                    teardown_state_adapter=None,  # hosted mode
                    teardown_state=teardown_state,
                    strategy=strategy,
                    teardown_intents=[],
                    positions=[],
                    teardown_mode=MagicMock(),
                    teardown_market=None,
                    is_auto_mode=False,  # manual teardown — the dangerous combination
                    price_oracle=None,
                    request=MagicMock(),
                    state_manager=MagicMock(),
                )
            )
