"""Unit tests for ``_find_state_db`` (issue #1713 + VIB-3761).

Covers the deterministic-first resolution order:

1. ``ALMANAK_STATE_DB`` env var wins unconditionally when the file exists
   (delegated to ``almanak.framework.local_paths.local_db_path``).
2. The cwd-relative ``./almanak_state.db`` legacy default is **removed**
   in VIB-3761 (April 29 silent-failure root cause). Callers without an
   explicit env var fall through to the per-deployment lookup below.
3. Per-deployment-id lookup (``~/.almanak/state/<id>/state.db``) is
   preferred over legacy flat locations, and the full deployment id is
   checked before the base strategy name when the id contains a colon.
4. Multiple legacy flat locations present -> warning logged listing
   every match; first candidate still returned so the dashboard stays
   usable.
5. No candidate exists -> ``None``.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

import pytest

from almanak.framework.dashboard.pages.detail import _find_state_db


@pytest.fixture
def isolated_cwd(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Run the resolver in a clean cwd + a clean fake ``$HOME``.

    Without this fixture the tests inherit the developer's real
    ``./almanak_state.db`` and ``~/.almanak/state/...`` layout, which would
    make the assertions flaky on every machine. We point both ``cwd`` and
    ``HOME`` at a throwaway temp directory so each test sees a pristine
    filesystem. ``Path.home`` is monkey-patched too because the canonical
    ``local_db_path`` resolver consults it (not just ``$HOME``).
    """
    fake_home = tmp_path / "home"
    fake_home.mkdir()
    monkeypatch.setenv("HOME", str(fake_home))
    # On some platforms ``os.path.expanduser`` consults ``USERPROFILE`` too.
    monkeypatch.setenv("USERPROFILE", str(fake_home))
    monkeypatch.setattr(Path, "home", lambda: fake_home)
    # VIB-3761: clear all path env vars so the resolver returns the
    # per-user utility default, which lands under our fake home.
    monkeypatch.delenv("AGENT_ID", raising=False)
    monkeypatch.delenv("ALMANAK_STATE_DB", raising=False)
    monkeypatch.delenv("ALMANAK_STRATEGY_FOLDER", raising=False)
    monkeypatch.delenv("ALMANAK_GATEWAY_DB_PATH", raising=False)
    monkeypatch.delenv("XDG_DATA_HOME", raising=False)
    monkeypatch.chdir(tmp_path)
    return tmp_path


def test_find_state_db_env_var_wins_over_every_other_candidate(
    isolated_cwd: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Explicit ``ALMANAK_STATE_DB`` always wins when pointing at an existing file."""
    env_db = isolated_cwd / "custom_path" / "state.db"
    env_db.parent.mkdir()
    env_db.touch()
    # Also create a CLI-default file to prove the env var wins over it.
    (isolated_cwd / "almanak_state.db").touch()

    monkeypatch.setenv("ALMANAK_STATE_DB", str(env_db))

    assert _find_state_db("AaveYieldStrategy:abc123") == str(env_db)


def test_find_state_db_env_var_missing_file_falls_through_to_deployment_lookup(
    isolated_cwd: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``ALMANAK_STATE_DB`` set to a non-existent file falls through to the
    per-deployment lookup.

    VIB-3761: a stray ``./almanak_state.db`` is no longer a candidate —
    the cwd-relative legacy default was removed. With no other DB
    present, the resolver returns ``None`` and the dashboard renders the
    no-data state.
    """
    # A cwd-relative file used to win here; under VIB-3761 it must be
    # ignored because relying on cwd was the April 29 silent-failure
    # root cause.
    (isolated_cwd / "almanak_state.db").touch()
    monkeypatch.setenv("ALMANAK_STATE_DB", str(isolated_cwd / "does_not_exist.db"))

    result = _find_state_db("AaveYieldStrategy:abc123")

    assert result is None


def test_find_state_db_deployment_lookup_wins_over_cwd_legacy(isolated_cwd: Path) -> None:
    """Per-deployment ``~/.almanak/state/<id>/state.db`` wins; the cwd
    legacy ``./almanak_state.db`` is no longer a candidate (VIB-3761)."""
    # The cwd-relative file must NOT be picked up.
    (isolated_cwd / "almanak_state.db").touch()

    home_dir = isolated_cwd / "home" / ".almanak" / "state" / "AaveYieldStrategy:abc123"
    home_dir.mkdir(parents=True)
    (home_dir / "state.db").touch()

    result = _find_state_db("AaveYieldStrategy:abc123")

    assert result == str(home_dir / "state.db")


def test_find_state_db_deployment_id_lookup_preferred_over_base_name(isolated_cwd: Path) -> None:
    """Exact deployment-id match wins over base-strategy-name match."""
    strategy_id = "AaveYieldStrategy:abc123"
    home_state = isolated_cwd / "home" / ".almanak" / "state"
    home_state.mkdir(parents=True)
    # Base-name match (lower priority).
    base_dir = home_state / "AaveYieldStrategy"
    base_dir.mkdir()
    (base_dir / "state.db").touch()
    # Deployment-id match (higher priority).
    deployment_dir = home_state / strategy_id
    deployment_dir.mkdir()
    (deployment_dir / "state.db").touch()

    result = _find_state_db(strategy_id)

    # The deployment-id path wins.
    assert result == str(deployment_dir / "state.db")


def test_find_state_db_base_name_lookup_used_when_only_base_exists(isolated_cwd: Path) -> None:
    """When only the base-name directory exists, it is returned as a fallback."""
    strategy_id = "AaveYieldStrategy:abc123"
    home_state = isolated_cwd / "home" / ".almanak" / "state"
    home_state.mkdir(parents=True)
    base_dir = home_state / "AaveYieldStrategy"
    base_dir.mkdir()
    (base_dir / "state.db").touch()

    result = _find_state_db(strategy_id)

    assert result == str(base_dir / "state.db")


def test_find_state_db_warns_on_multiple_legacy_matches(
    isolated_cwd: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Two legacy flat locations match -> warning logged, first match returned."""
    # Both legacy flat locations exist; neither deployment-id nor cli-default do.
    legacy_home = isolated_cwd / "home" / ".almanak" / "state" / "state.db"
    legacy_home.parent.mkdir(parents=True)
    legacy_home.touch()
    legacy_cwd = isolated_cwd / ".almanak" / "state.db"
    legacy_cwd.parent.mkdir(parents=True)
    legacy_cwd.touch()

    with caplog.at_level(logging.WARNING, logger="almanak.framework.dashboard.pages.detail"):
        result = _find_state_db("AaveYieldStrategy:abc123")

    # First candidate (home legacy) still wins so the dashboard stays functional.
    assert result == str(legacy_home)
    # But the ambiguity must be surfaced.
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert any("Multiple legacy state DB candidates" in r.getMessage() for r in warnings)


def test_find_state_db_no_candidates_returns_none(isolated_cwd: Path) -> None:
    """No DB anywhere -> ``None`` (caller uses this as the no-data sentinel)."""
    assert _find_state_db("AaveYieldStrategy:abc123") is None


def test_find_state_db_strategy_id_without_colon_uses_id_as_deployment_folder(
    isolated_cwd: Path,
) -> None:
    """Legacy strategy ids (no deployment suffix) still resolve via the canonical folder."""
    strategy_id = "AaveYieldStrategy"  # No colon.
    home_state = isolated_cwd / "home" / ".almanak" / "state" / strategy_id
    home_state.mkdir(parents=True)
    (home_state / "state.db").touch()

    result = _find_state_db(strategy_id)

    assert result == str(home_state / "state.db")
