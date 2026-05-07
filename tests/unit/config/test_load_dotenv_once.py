"""Tests for ``_load_dotenv_once`` — path-aware load-once semantics.

Two contracts pinned together:

1. Gemini's PR #2107 finding — ``load_config(dotenv_path="custom.env")``
   followed by an inner no-arg call (from ``gateway_config_from_env``)
   must NOT trigger a second ``load_dotenv()`` against the cwd default,
   because under pydotenv's ``override=False`` that silently merges
   unwanted values from the dev's working directory.

2. PR #2152 review — the SDK genuinely has multiple dotenv sources per
   process: the Click main group loads the cwd default, then a
   per-strategy command (``strat run``, ``strat test``, ``ax``, teardown,
   permissions, paper) layers a strategy-folder ``.env`` on top. The
   earlier process-wide boolean made the second load a silent no-op.

The current implementation tracks loaded *paths*: each distinct absolute
path is loaded once, and the no-arg cwd default is load-once *and*
suppressed after any explicit path has been loaded.
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


def test_path_then_default_does_not_double_load(fresh_env_module, monkeypatch: pytest.MonkeyPatch) -> None:
    """Regression test for Gemini's finding on PR #2107.

    Sequence: ``_load_dotenv_once("custom.env")`` then
    ``_load_dotenv_once()`` (no arg, as ``gateway_config_from_env`` does).
    The no-arg call must be suppressed once any explicit path has been
    loaded — re-reading the cwd default would silently merge unwanted
    values from the dev's working directory into the live env.
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
    assert args == ("custom.env",), f"first call's path must be honoured; got {args!r}"


def test_default_then_explicit_path_loads_both(fresh_env_module, monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    """Default-first then explicit path loads both — PR #2152 review.

    The Click main group loads the cwd default; a per-strategy command
    layers a strategy-folder ``.env`` on top. Both must load (path-aware
    contract); the earlier process-wide boolean broke this by silently
    skipping the second call.
    """
    calls: list[tuple] = []
    monkeypatch.setattr(fresh_env_module, "load_dotenv", lambda *a, **k: calls.append((a, k)))

    strat_env = tmp_path / "strat.env"
    strat_env.write_text("ALMANAK_PRIVATE_KEY=local\n")

    fresh_env_module._load_dotenv_once()
    fresh_env_module._load_dotenv_once(str(strat_env))

    assert len(calls) == 2, f"both default + explicit path must load (path-aware contract); got {calls}"
    assert calls[0][0] == (), f"first call is the cwd default; got {calls[0][0]!r}"
    assert calls[1][0] == (str(strat_env),), (
        f"second call must use the explicit strategy-folder path; got {calls[1][0]!r}"
    )


def test_distinct_explicit_paths_load_independently(
    fresh_env_module, monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    """Two distinct explicit paths each load once (e.g. ``ax``'s ``.env`` + ``.power-env``)."""
    calls: list[tuple] = []
    monkeypatch.setattr(fresh_env_module, "load_dotenv", lambda *a, **k: calls.append((a, k)))

    a = tmp_path / ".env"
    a.write_text("A=1\n")
    b = tmp_path / ".power-env"
    b.write_text("B=2\n")

    fresh_env_module._load_dotenv_once(str(a))
    fresh_env_module._load_dotenv_once(str(b))
    fresh_env_module._load_dotenv_once(str(a))  # second time, must be no-op
    fresh_env_module._load_dotenv_once(str(b))  # second time, must be no-op

    assert len(calls) == 2, f"each distinct path loads exactly once; got {calls}"
    assert {calls[0][0], calls[1][0]} == {(str(a),), (str(b),)}


def test_repeated_path_with_different_spelling_resolves_to_same_key(
    fresh_env_module, monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    """Path normalisation: ``./.env`` and the resolved absolute path are one key."""
    calls: list[tuple] = []
    monkeypatch.setattr(fresh_env_module, "load_dotenv", lambda *a, **k: calls.append((a, k)))

    env_file = tmp_path / ".env"
    env_file.write_text("X=1\n")

    monkeypatch.chdir(tmp_path)
    fresh_env_module._load_dotenv_once("./.env")
    fresh_env_module._load_dotenv_once(str(env_file.resolve()))

    assert len(calls) == 1, f"two spellings of the same path must collapse to one load; got {calls}"


def test_real_env_layering_first_wins_per_key(fresh_env_module, monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    """Real ``os.environ`` values: with ``override=False`` first-loaded wins per key.

    The docstring's "no-arg suppression after explicit" invariant is a
    *load* contract (don't re-read the cwd ``.env`` after an explicit
    path). This test pins the orthogonal *value-precedence* contract that
    callers depend on: when the cwd default loads first and a strategy
    ``.env`` layers on top, keys present in both keep the cwd value, and
    keys only in the strategy file are added (PR #2152 review).

    Don't change this without also updating the docstring — the override
    semantic is what makes shell exports always beat any ``.env`` source.
    """
    cwd_env = tmp_path / "cwd.env"
    cwd_env.write_text("SHARED_KEY=from_cwd\nCWD_ONLY=cwd_value\n")
    strat_env = tmp_path / "strat.env"
    strat_env.write_text("SHARED_KEY=from_strat\nSTRAT_ONLY=strat_value\n")

    monkeypatch.delenv("SHARED_KEY", raising=False)
    monkeypatch.delenv("CWD_ONLY", raising=False)
    monkeypatch.delenv("STRAT_ONLY", raising=False)

    fresh_env_module._load_dotenv_once(str(cwd_env))
    fresh_env_module._load_dotenv_once(str(strat_env))

    import os

    # First-loaded wins for overlapping keys.
    assert os.environ.get("SHARED_KEY") == "from_cwd", "cwd loaded first, override=False -> cwd value should stick"
    # Distinct keys from each file are both present (additive layering).
    assert os.environ.get("CWD_ONLY") == "cwd_value"
    assert os.environ.get("STRAT_ONLY") == "strat_value"


def test_real_env_shell_export_beats_dotenv(fresh_env_module, monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    """A shell export beats every dotenv source.

    With ``override=False`` (the default), values already in
    ``os.environ`` are never overwritten by a dotenv load. Keeping this
    behaviour is the reason explicit-path loads also stay
    ``override=False``: switching to ``override=True`` would let a
    strategy-folder ``.env`` silently overwrite a deliberately exported
    shell secret (PR #2152 review)."""
    env_file = tmp_path / ".env"
    env_file.write_text("ALMANAK_TEST_KEY=from_dotenv\n")

    monkeypatch.setenv("ALMANAK_TEST_KEY", "from_shell")

    fresh_env_module._load_dotenv_once(str(env_file))

    import os

    assert os.environ.get("ALMANAK_TEST_KEY") == "from_shell", "shell export must beat dotenv even after a fresh load"
