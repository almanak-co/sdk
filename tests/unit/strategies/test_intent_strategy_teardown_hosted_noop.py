"""Tests for IntentStrategy teardown polling no-op behavior in hosted mode.

`_check_teardown_request` is called every iteration by the runner via
`should_teardown()`. Before this guard, every hosted-mode iteration tried
to construct `TeardownStateAdapter`, which calls `local_strategy_db_path()`,
which raises `LocalPathError` because hosted mode has no local DB. That
exception was caught by `except Exception` and emitted as a per-iteration
WARNING — visible spam in production logs even though everything was
working as intended (hosted teardowns flow through a different channel,
VIB-3777).

These tests pin the contract: in hosted mode, both teardown polling
methods short-circuit cleanly without instantiating the local adapter.
"""

from dataclasses import dataclass

import pytest

from almanak import IntentStrategy


@dataclass
class _Config:
    strategy_id: str = "test"
    strategy_name: str = "test"
    chain: str = "base"

    def to_dict(self) -> dict:
        return {k: v for k, v in self.__dict__.items() if k not in {"strategy_id", "strategy_name", "chain"}}

    def update(self, **kwargs) -> None:
        for k, v in kwargs.items():
            if hasattr(self, k):
                setattr(self, k, v)


class _Strat(IntentStrategy):
    STRATEGY_NAME = "test"

    def decide(self, market):
        return None

    def get_open_positions(self):
        from almanak.framework.teardown.models import TeardownPositionSummary
        return TeardownPositionSummary.empty("test")

    def generate_teardown_intents(self, mode=None, market=None):
        return []


def _make_strategy() -> _Strat:
    s = object.__new__(_Strat)
    s.config = _Config()
    s._chain = "base"
    s._strategy_id = "test:abcd"
    return s


class TestCheckTeardownRequestHostedNoOp:
    """`_check_teardown_request` in hosted mode is a no-op — no SQLite
    adapter construction, no LocalPathError, no per-iteration WARNING."""

    def test_returns_none_without_constructing_adapter(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("AGENT_ID", "test-agent-id")

        # Tripwire: if hosted mode falls through to the SQLite path, this
        # blows up with a clear test failure rather than the silent
        # LocalPathError → WARNING degradation we're trying to eliminate.
        from almanak.framework import teardown as teardown_pkg

        def _trip(*_args, **_kwargs):
            raise AssertionError(
                "hosted mode must not construct a TeardownStateManager"
            )

        monkeypatch.setattr(teardown_pkg, "get_teardown_state_manager", _trip)

        strat = _make_strategy()
        result = strat._check_teardown_request()
        assert result is None

    def test_local_mode_still_constructs_adapter(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path
    ) -> None:
        """Sanity: local mode still goes through the manager (returning None
        from get_active_request because the DB is fresh)."""
        monkeypatch.delenv("AGENT_ID", raising=False)
        monkeypatch.setenv("ALMANAK_STATE_DB", str(tmp_path / "state.db"))

        called = {"flag": False}
        # Patch on the package, not the submodule — the method does
        # ``from almanak.framework.teardown import get_teardown_state_manager``.
        from almanak.framework import teardown as teardown_pkg

        class _FakeManager:
            def get_active_request(self, _strategy_id):
                called["flag"] = True
                return None

        monkeypatch.setattr(teardown_pkg, "get_teardown_state_manager", lambda: _FakeManager())

        strat = _make_strategy()
        result = strat._check_teardown_request()
        assert result is None
        assert called["flag"] is True


class TestAcknowledgeTeardownRequestHostedNoOp:
    """`acknowledge_teardown_request` in hosted mode mirrors the check —
    short-circuit to False, no adapter construction."""

    def test_returns_false_without_constructing_adapter(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("AGENT_ID", "test-agent-id")

        from almanak.framework import teardown as teardown_pkg

        def _trip(*_args, **_kwargs):
            raise AssertionError(
                "hosted mode must not construct a TeardownStateManager for ack"
            )

        monkeypatch.setattr(teardown_pkg, "get_teardown_state_manager", _trip)

        strat = _make_strategy()
        assert strat.acknowledge_teardown_request() is False


class TestShouldTeardownHostedClean:
    """`should_teardown()` chains through `_check_teardown_request`. With the
    hosted-mode short-circuit it returns False cleanly with no log noise."""

    def test_returns_false_without_warnings(
        self, monkeypatch: pytest.MonkeyPatch, caplog
    ) -> None:
        import logging

        monkeypatch.setenv("AGENT_ID", "test-agent-id")
        caplog.set_level(logging.WARNING, logger="almanak.framework.strategies.intent_strategy")

        strat = _make_strategy()
        assert strat.should_teardown() is False

        # The per-iteration noise we're eliminating: ensure no WARNING fires
        # from the teardown poller in hosted mode.
        teardown_warnings = [
            r for r in caplog.records
            if r.levelno >= logging.WARNING
            and "teardown" in r.getMessage().lower()
        ]
        assert teardown_warnings == [], (
            f"hosted should_teardown() must be silent; saw warnings: "
            f"{[r.getMessage() for r in teardown_warnings]}"
        )
