"""Regression tests for gateway local-DB path resolution (VIB-3761/-3835).

Closes the May 2 dashboard miscount class: a gateway started inside a
strategy folder MUST pin to that folder's ``almanak_state.db`` and MUST
NOT fall through to ``~/.local/share/almanak/utility/almanak_state.db``.

The fixture pattern manipulates env vars (``ALMANAK_STRATEGY_FOLDER``,
``ALMANAK_STATE_DB``) to model the operator's three deployment shapes:

1. STRATEGY-PINNED  — ``ALMANAK_STRATEGY_FOLDER`` resolves; strict
   resolver returns ``<folder>/almanak_state.db``.
2. STANDALONE       — explicit opt-in; lenient resolver returns the
   utility-DB fallback (``almanak ax`` use case).
3. HARD-FAIL        — neither folder nor standalone; strict resolver
   raises ``LocalPathError`` instead of silently writing to utility DB.

Hosted mode (``AGENT_ID`` set) is out of scope for these tests because
the gateway uses Postgres there; the local-path branch never executes.
"""

from __future__ import annotations

import os

import pytest

from almanak.framework.local_paths import (
    LocalPathError,
    auto_detect_strategy_folder,
    looks_like_strategy_folder,
)
from almanak.gateway._server_start_helpers import resolve_gateway_local_db_path
from almanak.gateway.core.settings import GatewaySettings


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    """Strip strategy/DB-related env vars so each test starts clean.

    Without this, a CI runner with ``ALMANAK_STATE_DB`` exported in the
    job env can mask a regression — the lenient fallback would pick up
    the explicit override and the test would pass for the wrong reason.
    """
    for var in (
        "ALMANAK_STATE_DB",
        "ALMANAK_STRATEGY_FOLDER",
        "ALMANAK_GATEWAY_DB_PATH",
        "AGENT_ID",
    ):
        monkeypatch.delenv(var, raising=False)


def _make_strategy_folder(tmp_path):
    """Create a directory that ``looks_like_strategy_folder`` recognises."""
    folder = tmp_path / "strategies" / "accounting" / "lp"
    folder.mkdir(parents=True)
    (folder / "config.json").write_text("{}")
    (folder / "strategy.py").write_text("# placeholder\n")
    return folder


# ─── looks_like_strategy_folder ──────────────────────────────────────────


def test_looks_like_strategy_folder_recognises_config_json(tmp_path):
    folder = tmp_path / "strat"
    folder.mkdir()
    (folder / "config.json").write_text("{}")
    assert looks_like_strategy_folder(folder) is True


def test_looks_like_strategy_folder_recognises_strategy_py(tmp_path):
    folder = tmp_path / "strat"
    folder.mkdir()
    (folder / "strategy.py").write_text("# decorator-driven\n")
    assert looks_like_strategy_folder(folder) is True


def test_looks_like_strategy_folder_rejects_empty_dir(tmp_path):
    folder = tmp_path / "empty"
    folder.mkdir()
    assert looks_like_strategy_folder(folder) is False


def test_looks_like_strategy_folder_rejects_missing_path(tmp_path):
    assert looks_like_strategy_folder(tmp_path / "does-not-exist") is False


# ─── auto_detect_strategy_folder ─────────────────────────────────────────


def test_auto_detect_returns_existing_env_var(tmp_path, monkeypatch):
    folder = _make_strategy_folder(tmp_path)
    monkeypatch.setenv("ALMANAK_STRATEGY_FOLDER", str(folder))
    # cwd is irrelevant when env var already resolves
    monkeypatch.chdir(tmp_path)

    detected = auto_detect_strategy_folder()
    assert detected == folder


def test_auto_detect_exports_cwd_when_unset(tmp_path, monkeypatch):
    folder = _make_strategy_folder(tmp_path)
    monkeypatch.chdir(folder)

    detected = auto_detect_strategy_folder()
    assert detected == folder
    assert os.environ["ALMANAK_STRATEGY_FOLDER"] == str(folder)


def test_auto_detect_returns_none_outside_strategy_folder(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)  # plain dir, no config.json
    assert auto_detect_strategy_folder() is None
    assert "ALMANAK_STRATEGY_FOLDER" not in os.environ


# ─── resolve_gateway_local_db_path ───────────────────────────────────────


def test_resolve_strategy_pinned(tmp_path, monkeypatch):
    """Default settings + strategy-folder env var → strict resolver wins."""
    folder = _make_strategy_folder(tmp_path)
    monkeypatch.setenv("ALMANAK_STRATEGY_FOLDER", str(folder))

    settings = GatewaySettings(standalone=False)
    db_path = resolve_gateway_local_db_path(settings)

    assert db_path == folder / "almanak_state.db"
    # Hard guarantee: the utility-DB path is NOT what we resolved to.
    assert "utility" not in str(db_path)


def test_resolve_hard_fails_outside_strategy_folder(tmp_path, monkeypatch):
    """No folder, no --standalone → LocalPathError, not silent utility-DB."""
    monkeypatch.chdir(tmp_path)
    settings = GatewaySettings(standalone=False)

    with pytest.raises(LocalPathError) as exc_info:
        resolve_gateway_local_db_path(settings)

    msg = str(exc_info.value)
    assert "no strategy folder resolved" in msg
    # Remediation hint for the operator
    assert "config.json" in msg


def test_resolve_standalone_allows_utility_db(tmp_path, monkeypatch):
    """--standalone explicitly opts into the lenient utility-DB path."""
    monkeypatch.chdir(tmp_path)  # no config.json
    settings = GatewaySettings(standalone=True)

    # Should resolve without raising — falls through to utility path.
    db_path = resolve_gateway_local_db_path(settings)
    assert db_path.name == "almanak_state.db"
    # Standalone path lands in the utility directory by design.
    assert "utility" in str(db_path) or "almanak" in str(db_path.parent)


def test_resolve_standalone_still_honours_explicit_state_db(tmp_path, monkeypatch):
    """``ALMANAK_STATE_DB`` always wins, even in standalone mode."""
    explicit = tmp_path / "custom.db"
    monkeypatch.setenv("ALMANAK_STATE_DB", str(explicit))
    settings = GatewaySettings(standalone=True)

    db_path = resolve_gateway_local_db_path(settings)
    assert db_path == explicit


def test_resolve_strategy_folder_wins_over_standalone_env(tmp_path, monkeypatch):
    """If the operator both sets the strategy folder AND passes --standalone,
    the lenient resolver still picks the strategy folder first because the
    underlying ``local_db_path`` resolution prefers it.
    """
    folder = _make_strategy_folder(tmp_path)
    monkeypatch.setenv("ALMANAK_STRATEGY_FOLDER", str(folder))
    settings = GatewaySettings(standalone=True)

    db_path = resolve_gateway_local_db_path(settings)
    assert db_path == folder / "almanak_state.db"
