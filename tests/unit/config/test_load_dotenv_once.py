"""Tests for ``_load_dotenv_once`` — process-wide load-once semantics.

Gemini found a real bug in the previous ``@cache``-decorated implementation:
when ``load_config(dotenv_path="custom.env")`` runs, it calls
``_load_dotenv_once("custom.env")`` and then internally invokes
``gateway_config_from_env()`` which calls ``_load_dotenv_once()`` with no
argument — ``functools.cache`` keys on args, so the second call is a cache
miss and triggers a *second* ``load_dotenv()`` against the cwd's default
``.env``. Under pydotenv's ``override=False`` default this silently merges
unwanted values from the cwd file into the live env.

The fix moves to a module-level boolean guard so a single load-once
invariant holds regardless of the argument shape across calls. These tests
pin that contract.
"""

from __future__ import annotations

import importlib

import pytest


@pytest.fixture
def fresh_env_module(monkeypatch: pytest.MonkeyPatch):
    """Reload ``almanak.config.env`` so the module-level flag starts False."""
    # Drop any cached import so the ``_DOTENV_LOADED = False`` initialisation
    # runs fresh; tests that share interpreter state would otherwise see the
    # flag set by a prior test or by application code.
    import almanak.config.env as env_module

    importlib.reload(env_module)
    yield env_module
    importlib.reload(env_module)


def test_first_call_invokes_load_dotenv(fresh_env_module, monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple] = []

    def fake_load_dotenv(*args, **kwargs):
        calls.append((args, kwargs))

    monkeypatch.setattr(fresh_env_module, "load_dotenv", fake_load_dotenv)
    fresh_env_module._load_dotenv_once()
    assert len(calls) == 1, f"first call should trigger one load_dotenv, got {calls}"


def test_subsequent_calls_are_noop(fresh_env_module, monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple] = []
    monkeypatch.setattr(fresh_env_module, "load_dotenv", lambda *a, **k: calls.append((a, k)))

    fresh_env_module._load_dotenv_once()
    fresh_env_module._load_dotenv_once()
    fresh_env_module._load_dotenv_once()

    assert len(calls) == 1, "load_dotenv must run at most once per process"


def test_path_then_default_does_not_double_load(
    fresh_env_module, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Regression test for Gemini's finding on PR #2107.

    Sequence: ``_load_dotenv_once("custom.env")`` then
    ``_load_dotenv_once()`` (no arg, as ``gateway_config_from_env`` does).
    Under the old ``@cache`` implementation the no-arg call was a cache
    miss and triggered a second ``load_dotenv`` against the cwd's
    ``.env``. The flag-based implementation must not re-load.
    """
    calls: list[tuple] = []
    monkeypatch.setattr(fresh_env_module, "load_dotenv", lambda *a, **k: calls.append((a, k)))

    fresh_env_module._load_dotenv_once("custom.env")
    fresh_env_module._load_dotenv_once()

    assert len(calls) == 1, (
        "second call (no arg) must not trigger a second load_dotenv when "
        "the first call already loaded a custom path; otherwise the cwd's "
        f".env silently merges in. calls={calls}"
    )
    args, _ = calls[0]
    assert args == ("custom.env",), (
        f"first call's path must be honoured; got {args!r}"
    )


def test_default_then_path_does_not_double_load(
    fresh_env_module, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Inverse direction: the default-first sequence is also a single load."""
    calls: list[tuple] = []
    monkeypatch.setattr(fresh_env_module, "load_dotenv", lambda *a, **k: calls.append((a, k)))

    fresh_env_module._load_dotenv_once()
    fresh_env_module._load_dotenv_once("custom.env")

    assert len(calls) == 1, "second call (with arg) must not double-load"
    args, _ = calls[0]
    assert args == (), f"first call's default-path semantics must be honoured; got {args!r}"
