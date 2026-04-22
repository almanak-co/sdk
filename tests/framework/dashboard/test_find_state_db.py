"""Unit tests for ``_find_state_db`` (issue #1713).

Covers the deterministic-first resolution order introduced by the dashboard
latent-bug-bundle fix:

1. ``ALMANAK_STATE_DB`` env var wins unconditionally when the file exists.
2. ``./almanak_state.db`` (CLI default) is preferred over any ``~/.almanak/..``
   candidate when present.
3. Per-deployment-id lookup (``~/.almanak/state/<id>/state.db``) is preferred
   over legacy flat locations, and the full deployment id is checked before
   the base strategy name when the id contains a colon.
4. Multiple legacy flat locations present -> warning logged listing every
   match; first candidate still returned so the dashboard stays usable.
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
    filesystem.
    """
    fake_home = tmp_path / "home"
    fake_home.mkdir()
    monkeypatch.setenv("HOME", str(fake_home))
    # On some platforms ``os.path.expanduser`` consults ``USERPROFILE`` too.
    monkeypatch.setenv("USERPROFILE", str(fake_home))
    monkeypatch.delenv("ALMANAK_STATE_DB", raising=False)
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


def test_find_state_db_env_var_missing_file_falls_through_to_default(
    isolated_cwd: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``ALMANAK_STATE_DB`` set to a non-existent file falls through."""
    (isolated_cwd / "almanak_state.db").touch()
    monkeypatch.setenv("ALMANAK_STATE_DB", str(isolated_cwd / "does_not_exist.db"))

    result = _find_state_db("AaveYieldStrategy:abc123")

    assert result == os.path.join(".", "almanak_state.db")


def test_find_state_db_cli_default_is_preferred_over_home_locations(isolated_cwd: Path) -> None:
    """The canonical CLI default path wins over any ``~/.almanak/...`` candidate."""
    (isolated_cwd / "almanak_state.db").touch()

    # Also populate the per-deployment location - cli default still wins.
    home_dir = isolated_cwd / "home" / ".almanak" / "state" / "AaveYieldStrategy:abc123"
    home_dir.mkdir(parents=True)
    (home_dir / "state.db").touch()

    result = _find_state_db("AaveYieldStrategy:abc123")

    assert result == os.path.join(".", "almanak_state.db")


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
