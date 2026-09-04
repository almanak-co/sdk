"""Path-resolution tests for the Accountant Test CLI."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from almanak.framework.cli.accountant_test_cli import _resolve_db_path

_DB_ENV = (
    "ALMANAK_IS_HOSTED",
    "ALMANAK_DEPLOYMENT_ID",
    "ALMANAK_STATE_DB",
    "ALMANAK_STRATEGY_FOLDER",
    "ALMANAK_GATEWAY_DB_PATH",
    "XDG_DATA_HOME",
)


@pytest.fixture
def isolated_db_env(monkeypatch: pytest.MonkeyPatch) -> pytest.MonkeyPatch:
    for variable in _DB_ENV:
        monkeypatch.delenv(variable, raising=False)
    return monkeypatch


def test_explicit_db_expands_user_path_before_hosted_resolution(
    tmp_path: Path,
    isolated_db_env: pytest.MonkeyPatch,
) -> None:
    home = tmp_path / "home"
    home.mkdir()
    db = home / "state.db"
    db.touch()
    isolated_db_env.setenv("HOME", str(home))
    isolated_db_env.setenv("ALMANAK_IS_HOSTED", "true")

    assert _resolve_db_path(None, "~/state.db") == db.resolve()


def test_explicit_db_rejects_directory(tmp_path: Path, isolated_db_env: pytest.MonkeyPatch) -> None:
    directory = tmp_path / "state.db"
    directory.mkdir()

    with pytest.raises(SystemExit) as exc_info:
        _resolve_db_path(None, str(directory))

    assert str(exc_info.value) == f"--db must point at a regular file, got: {directory.resolve()}"


def test_explicit_db_rejects_missing_file(tmp_path: Path, isolated_db_env: pytest.MonkeyPatch) -> None:
    missing = tmp_path / "missing.db"

    with pytest.raises(SystemExit) as exc_info:
        _resolve_db_path(None, str(missing))

    assert str(exc_info.value) == f"DB file does not exist: {missing.resolve()}"


def test_working_directory_selects_its_db_and_restores_prior_env(
    tmp_path: Path,
    isolated_db_env: pytest.MonkeyPatch,
) -> None:
    folder = tmp_path / "strategy"
    folder.mkdir()
    db = folder / "almanak_state.db"
    db.touch()
    isolated_db_env.setenv("ALMANAK_STRATEGY_FOLDER", "prior-folder")

    assert _resolve_db_path(str(folder), None) == db
    assert os.environ["ALMANAK_STRATEGY_FOLDER"] == "prior-folder"


def test_state_db_env_precedes_working_directory_and_restores_prior_env(
    tmp_path: Path,
    isolated_db_env: pytest.MonkeyPatch,
) -> None:
    state_db = tmp_path / "state-env.db"
    state_db.touch()
    folder = tmp_path / "strategy"
    folder.mkdir()
    (folder / "almanak_state.db").touch()
    isolated_db_env.setenv("ALMANAK_STATE_DB", str(state_db))
    isolated_db_env.setenv("ALMANAK_STRATEGY_FOLDER", "prior-folder")

    assert _resolve_db_path(str(folder), None) == state_db
    assert os.environ["ALMANAK_STRATEGY_FOLDER"] == "prior-folder"


def test_state_db_env_selects_existing_file(tmp_path: Path, isolated_db_env: pytest.MonkeyPatch) -> None:
    db = tmp_path / "state-env.db"
    db.touch()
    isolated_db_env.setenv("ALMANAK_STATE_DB", str(db))

    assert _resolve_db_path(None, None) == db


def test_strategy_folder_env_selects_folder_db(
    tmp_path: Path,
    isolated_db_env: pytest.MonkeyPatch,
) -> None:
    folder = tmp_path / "strategy"
    folder.mkdir()
    db = folder / "almanak_state.db"
    db.touch()
    isolated_db_env.setenv("ALMANAK_STRATEGY_FOLDER", str(folder))

    assert _resolve_db_path(None, None) == db


def test_cwd_strategy_config_selects_folder_db_without_exporting_env(
    tmp_path: Path,
    isolated_db_env: pytest.MonkeyPatch,
) -> None:
    (tmp_path / "config.json").write_text("{}", encoding="utf-8")
    db = tmp_path / "almanak_state.db"
    db.touch()
    isolated_db_env.chdir(tmp_path)

    assert _resolve_db_path(None, None) == db
    assert "ALMANAK_STRATEGY_FOLDER" not in os.environ


def test_hosted_resolution_reports_canonical_refusal(
    tmp_path: Path,
    isolated_db_env: pytest.MonkeyPatch,
) -> None:
    db = tmp_path / "state.db"
    db.touch()
    isolated_db_env.setenv("ALMANAK_IS_HOSTED", "true")
    isolated_db_env.setenv("ALMANAK_STATE_DB", str(db))

    with pytest.raises(SystemExit) as exc_info:
        _resolve_db_path(None, None)

    assert str(exc_info.value) == (
        "Cannot resolve strategy DB path: local-path helper called in hosted mode (ALMANAK_IS_HOSTED set). "
        "Hosted mode uses Postgres via ALMANAK_GATEWAY_DATABASE_URL.\n"
        "Pass --db <path>, set ALMANAK_STATE_DB, or use -d/--working-dir."
    )


def test_missing_working_directory_reports_resolution_error_and_restores_env(
    tmp_path: Path,
    isolated_db_env: pytest.MonkeyPatch,
) -> None:
    cwd = tmp_path / "cwd"
    cwd.mkdir()
    isolated_db_env.chdir(cwd)
    isolated_db_env.setenv("ALMANAK_STRATEGY_FOLDER", "prior-folder")

    with pytest.raises(SystemExit) as exc_info:
        _resolve_db_path(str(tmp_path / "missing-strategy"), None)

    assert str(exc_info.value) == (
        "Cannot resolve strategy DB path: no strategy folder resolved.\n"
        "  Pass --working-dir / -d <path>, or run from a strategy folder.\n"
        "  A strategy folder must contain config.json, config.yaml, config.yml, or strategy.py.\n"
        "Pass --db <path>, set ALMANAK_STATE_DB, or use -d/--working-dir."
    )
    assert os.environ["ALMANAK_STRATEGY_FOLDER"] == "prior-folder"


def test_resolved_folder_path_rejects_directory(
    tmp_path: Path,
    isolated_db_env: pytest.MonkeyPatch,
) -> None:
    folder = tmp_path / "strategy"
    folder.mkdir()
    db = folder / "almanak_state.db"
    db.mkdir()

    with pytest.raises(SystemExit) as exc_info:
        _resolve_db_path(str(folder), None)

    assert str(exc_info.value) == (
        f"Folder-scoped DB path is not a regular file: {db}\n"
        "Pass --db <path> or run from inside the strategy folder, or use -d/--working-dir."
    )


def test_resolved_folder_path_rejects_missing_file(
    tmp_path: Path,
    isolated_db_env: pytest.MonkeyPatch,
) -> None:
    folder = tmp_path / "strategy"
    folder.mkdir()
    db = folder / "almanak_state.db"

    with pytest.raises(SystemExit) as exc_info:
        _resolve_db_path(str(folder), None)

    assert str(exc_info.value) == (
        f"DB file does not exist at folder-scoped path: {db}\n"
        "Pass --db <path> or run from inside the strategy folder, or use -d/--working-dir."
    )
