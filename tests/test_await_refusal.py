"""The reverse-fault driver fails closed when the injected fault is bypassed."""

from types import SimpleNamespace

import pytest

from tests.strategy_fork import supervised as driver


def test_await_refusal_fails_closed_on_completed_close(monkeypatch, tmp_path):
    monkeypatch.setattr(
        driver, "loaded_checkpoint", lambda database: SimpleNamespace(mode="closed")
    )
    with pytest.raises(AssertionError, match="fault was not exercised"):
        driver.await_refusal(tmp_path)


def test_await_refusal_returns_the_refused_teardown_state(monkeypatch, tmp_path):
    state = SimpleNamespace(mode="teardown")
    monkeypatch.setattr(driver, "loaded_checkpoint", lambda database: state)
    monkeypatch.setattr(driver, "read_workspace", lambda root: {"processes": {"coordinator": 0}})
    monkeypatch.setattr(driver, "running", lambda pid: False)
    assert driver.await_refusal(tmp_path) is state


def test_await_refusal_raises_when_the_coordinator_never_refuses(monkeypatch, tmp_path):
    monkeypatch.setattr(driver, "loaded_checkpoint", lambda database: None)
    monkeypatch.setattr(driver, "PHASE_TIMEOUT", 0)
    with pytest.raises(AssertionError, match="was not refused"):
        driver.await_refusal(tmp_path)
