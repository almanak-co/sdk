"""Boot-time teardown-state posture WARNING (VIB-5464 / TD-06).

``IntentStrategy.set_state_manager`` (called once at runner boot) emits a loud,
one-time WARNING when a strategy declares no teardown-state posture, so the
formerly-silent ``save_state()`` empty-``{}`` default becomes audible. This is the
runtime backstop for strategies the CI lint never scans (hosted / incubating /
user code). These tests pin each branch.
"""

from __future__ import annotations

import logging

from almanak import IntentStrategy
from almanak.framework.strategies.stateless_strategy import StatelessStrategy
from almanak.framework.teardown.models import TeardownPositionSummary


class _NoPosture(IntentStrategy):
    def decide(self, market):  # pragma: no cover - not exercised
        return None

    def get_open_positions(self):  # pragma: no cover
        return TeardownPositionSummary.empty("test")

    def generate_teardown_intents(self, mode=None, market=None):  # pragma: no cover
        return []


class _Persisted(_NoPosture):
    def get_persistent_state(self):
        return {"x": 1}

    def load_persistent_state(self, state):
        self._x = state.get("x")


class _SaveOnly(_NoPosture):
    def get_persistent_state(self):
        return {"x": 1}


class _ChainDerived(_NoPosture):
    teardown_state_derived_from_chain = True


class _Stateless(StatelessStrategy):
    def decide(self, market):  # pragma: no cover
        return None


def _boot(strategy_cls: type) -> str | None:
    """Instantiate (bypassing __init__) and run the boot posture check; return
    the WARNING message text if one was emitted, else None."""
    strategy = object.__new__(strategy_cls)
    with _capture() as records:
        strategy.set_state_manager(object(), "deployment:test")
    warnings = [r.getMessage() for r in records if r.levelno == logging.WARNING]
    return warnings[0] if warnings else None


class _capture:
    """Minimal log capturer for the intent_strategy logger."""

    def __init__(self) -> None:
        self._records: list[logging.LogRecord] = []
        self._handler = logging.Handler()
        self._handler.emit = self._records.append  # type: ignore[method-assign]
        self._logger = logging.getLogger("almanak.framework.strategies.intent_strategy")

    def __enter__(self) -> list[logging.LogRecord]:
        self._prev_level = self._logger.level
        self._logger.setLevel(logging.WARNING)
        self._logger.addHandler(self._handler)
        return self._records

    def __exit__(self, *exc) -> None:
        self._logger.removeHandler(self._handler)
        self._logger.setLevel(self._prev_level)


def test_no_posture_warns_at_boot() -> None:
    msg = _boot(_NoPosture)
    assert msg is not None
    assert "no teardown-state posture" in msg
    assert "_NoPosture" in msg


def test_persisted_posture_does_not_warn() -> None:
    assert _boot(_Persisted) is None


def test_save_only_override_warns() -> None:
    msg = _boot(_SaveOnly)
    assert msg is not None
    assert "load_persistent_state" in msg


def test_chain_derived_posture_does_not_warn() -> None:
    assert _boot(_ChainDerived) is None


def test_stateless_does_not_warn() -> None:
    assert _boot(_Stateless) is None


def test_warns_only_once() -> None:
    strategy = object.__new__(_NoPosture)
    with _capture() as records:
        strategy.set_state_manager(object(), "deployment:test")
        strategy.set_state_manager(object(), "deployment:test")
    warnings = [r for r in records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
