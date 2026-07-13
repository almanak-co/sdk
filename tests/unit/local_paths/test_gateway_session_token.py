"""Unit tests for the managed-gateway session-token file handoff (VIB-4047).

The managed gateway persists its ephemeral session token to a 0600 file sibling
to the folder-scoped DB so a separately-launched ``almanak dashboard`` can
authenticate without an env-var dance. Hosted mode must never write it, and the
env var must always win over the file.
"""

from __future__ import annotations

import os
import stat
from pathlib import Path

import pytest

from almanak.framework import local_paths


@pytest.fixture
def strategy_folder(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv("ALMANAK_STATE_DB", str(tmp_path / "almanak_state.db"))
    monkeypatch.delenv("ALMANAK_IS_HOSTED", raising=False)
    return tmp_path


def test_write_then_read_round_trip(strategy_folder: Path) -> None:
    path = local_paths.write_gateway_session_token("deadbeefcafe")
    assert path is not None
    assert path == strategy_folder / "almanak_state.db.gw.session"
    assert local_paths.read_gateway_session_token() == "deadbeefcafe"


def test_written_file_is_0600(strategy_folder: Path) -> None:
    path = local_paths.write_gateway_session_token("tok")
    assert path is not None
    assert stat.S_IMODE(os.stat(path).st_mode) == 0o600


def test_write_overwrites_stale_token(strategy_folder: Path) -> None:
    local_paths.write_gateway_session_token("old")
    local_paths.write_gateway_session_token("new")
    assert local_paths.read_gateway_session_token() == "new"


def test_read_missing_returns_none(strategy_folder: Path) -> None:
    assert local_paths.read_gateway_session_token() is None


def test_read_empty_file_returns_none(strategy_folder: Path) -> None:
    (strategy_folder / "almanak_state.db.gw.session").write_text("   \n")
    assert local_paths.read_gateway_session_token() is None


def test_clear_removes_file(strategy_folder: Path) -> None:
    local_paths.write_gateway_session_token("tok")
    local_paths.clear_gateway_session_token()
    assert local_paths.read_gateway_session_token() is None
    assert not (strategy_folder / "almanak_state.db.gw.session").exists()


def test_clear_is_idempotent(strategy_folder: Path) -> None:
    # No file yet — must not raise.
    local_paths.clear_gateway_session_token()


def test_hosted_mode_never_writes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ALMANAK_STATE_DB", str(tmp_path / "almanak_state.db"))
    monkeypatch.setenv("ALMANAK_IS_HOSTED", "1")
    assert local_paths.write_gateway_session_token("tok") is None
    assert local_paths.read_gateway_session_token() is None
    assert not (tmp_path / "almanak_state.db.gw.session").exists()


def test_empty_token_not_written(strategy_folder: Path) -> None:
    assert local_paths.write_gateway_session_token("") is None
    assert not (strategy_folder / "almanak_state.db.gw.session").exists()


def test_no_strategy_folder_is_noop(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # No ALMANAK_STATE_DB / ALMANAK_STRATEGY_FOLDER resolvable → strict resolve
    # raises internally and the helpers degrade to None (never crash the gateway).
    monkeypatch.delenv("ALMANAK_STATE_DB", raising=False)
    monkeypatch.delenv("ALMANAK_STRATEGY_FOLDER", raising=False)
    monkeypatch.delenv("ALMANAK_IS_HOSTED", raising=False)
    monkeypatch.chdir(tmp_path)  # an empty dir is not a strategy folder
    assert local_paths.write_gateway_session_token("tok") is None
    assert local_paths.read_gateway_session_token() is None


def test_cli_runtime_precedence_env_wins_over_file(
    strategy_folder: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from almanak.config.cli_runtime import cli_runtime_config_from_env

    local_paths.write_gateway_session_token("file-token")

    monkeypatch.setenv("ALMANAK_GATEWAY_AUTH_TOKEN", "env-token")
    assert cli_runtime_config_from_env().gateway_client_auth_token_resolved == "env-token"

    monkeypatch.delenv("ALMANAK_GATEWAY_AUTH_TOKEN", raising=False)
    assert cli_runtime_config_from_env().gateway_client_auth_token_resolved == "file-token"
