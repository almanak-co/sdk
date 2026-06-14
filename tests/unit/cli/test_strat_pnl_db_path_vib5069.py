"""VIB-5069 — bare `strat pnl` must agree with `strat run` on which DB to read.

`strat run` folder-scopes its SQLite DB off the cwd, but the non-strict resolver
behind `local_db_path` does not consult the cwd, so a bare `strat pnl` (no
`--db`, no env override) silently read the per-user *utility* DB. The fix pins a
cwd-detected strategy folder first and announces a genuine utility-DB fallback.
"""

from __future__ import annotations

import pytest

from almanak.framework.cli import strat_pnl

_DB_ENV = ("ALMANAK_STATE_DB", "ALMANAK_STRATEGY_FOLDER", "ALMANAK_GATEWAY_DB_PATH", "ALMANAK_IS_HOSTED")


def _isolate(monkeypatch: pytest.MonkeyPatch, tmp_path) -> list[str]:
    """Clear all DB-resolution env, pin XDG under tmp, capture stderr echoes."""
    for k in _DB_ENV:
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "xdg"))
    errs: list[str] = []
    monkeypatch.setattr(
        "click.echo",
        lambda *a, **k: errs.append(a[0] if a else "") if k.get("err") else None,
    )
    return errs


def test_cwd_strategy_folder_used_without_warning(tmp_path, monkeypatch):
    errs = _isolate(monkeypatch, tmp_path)
    (tmp_path / "config.json").write_text("{}", encoding="utf-8")
    monkeypatch.chdir(tmp_path)

    db = strat_pnl._default_db_path()

    assert db == str(tmp_path / "almanak_state.db")
    assert errs == []  # a real strategy folder: no utility warning


def test_utility_fallback_is_announced(tmp_path, monkeypatch):
    errs = _isolate(monkeypatch, tmp_path)
    nonstrat = tmp_path / "not-a-strategy"
    nonstrat.mkdir()
    monkeypatch.chdir(nonstrat)

    db = strat_pnl._default_db_path()

    assert db.endswith("/almanak/utility/almanak_state.db")
    assert any("Using utility DB" in e for e in errs), errs


def test_state_db_env_wins_unchanged(tmp_path, monkeypatch):
    """ALMANAK_STATE_DB precedence is preserved, even from a non-strategy cwd,
    and no utility warning fires (it is not the utility DB)."""
    errs = _isolate(monkeypatch, tmp_path)
    explicit = tmp_path / "explicit_state.db"
    monkeypatch.setenv("ALMANAK_STATE_DB", str(explicit))
    monkeypatch.chdir(tmp_path)  # not a strategy folder

    db = strat_pnl._default_db_path()

    assert db == str(explicit)
    assert errs == []


def test_strategy_folder_env_wins_over_cwd(tmp_path, monkeypatch):
    """ALMANAK_STRATEGY_FOLDER wins; cwd detection does not override it."""
    errs = _isolate(monkeypatch, tmp_path)
    folder = tmp_path / "pinned"
    folder.mkdir()
    (folder / "config.json").write_text("{}", encoding="utf-8")
    monkeypatch.setenv("ALMANAK_STRATEGY_FOLDER", str(folder))
    other = tmp_path / "elsewhere"
    other.mkdir()
    monkeypatch.chdir(other)  # different from the pinned env folder

    db = strat_pnl._default_db_path()

    assert db == str(folder / "almanak_state.db")
    assert errs == []
