"""Unit tests for ``almanak.framework.local_paths`` — VIB-3761, plan §B.

The local SDK is folder-scoped. These tests pin the resolution priority
order (explicit override → strategy folder → utility default), the
hosted-mode refusal, the flock collision behaviour, and the legacy-cwd
migration warning.

Test IDs T-3761-1..T-3761-12.
"""

from __future__ import annotations

import logging
import os
import re
import subprocess
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from almanak.framework.local_paths import (
    LOCAL_DB_FILENAME,
    LocalDbLockError,
    LocalPathError,
    acquire_local_db_lock,
    local_db_path,
    local_log_path,
    release_local_db_lock,
    warn_if_legacy_cwd_db_exists,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
@pytest.fixture
def clean_env(monkeypatch: pytest.MonkeyPatch) -> pytest.MonkeyPatch:
    """Clear every env var the resolver consults so each test starts neutral."""
    for var in (
        "AGENT_ID",
        "ALMANAK_STATE_DB",
        "ALMANAK_STRATEGY_FOLDER",
        "ALMANAK_GATEWAY_DB_PATH",
        "XDG_DATA_HOME",
    ):
        monkeypatch.delenv(var, raising=False)
    return monkeypatch


# ---------------------------------------------------------------------------
# T-3761-1..4: resolution priority order
# ---------------------------------------------------------------------------
def test_t_3761_1_explicit_state_db_wins(clean_env, tmp_path) -> None:
    """T-3761-1: ALMANAK_STATE_DB beats every other resolver branch."""
    explicit = tmp_path / "explicit.db"
    other = tmp_path / "other.db"
    clean_env.setenv("ALMANAK_STATE_DB", str(explicit))
    clean_env.setenv("ALMANAK_STRATEGY_FOLDER", str(tmp_path / "strategy"))
    clean_env.setenv("ALMANAK_GATEWAY_DB_PATH", str(other))
    assert local_db_path() == explicit.resolve()


def test_t_3761_2_strategy_folder_wins_over_utility(clean_env, tmp_path) -> None:
    """T-3761-2: ALMANAK_STRATEGY_FOLDER beats ALMANAK_GATEWAY_DB_PATH."""
    folder = tmp_path / "strategy"
    folder.mkdir()
    clean_env.setenv("ALMANAK_STRATEGY_FOLDER", str(folder))
    clean_env.setenv("ALMANAK_GATEWAY_DB_PATH", str(tmp_path / "utility.db"))
    assert local_db_path() == folder.resolve() / LOCAL_DB_FILENAME


def test_t_3761_3_gateway_db_path_used_when_no_strategy_folder(
    clean_env, tmp_path
) -> None:
    """T-3761-3: ALMANAK_GATEWAY_DB_PATH wins over the utility default."""
    explicit_gw = tmp_path / "gateway.db"
    clean_env.setenv("ALMANAK_GATEWAY_DB_PATH", str(explicit_gw))
    assert local_db_path() == explicit_gw.resolve()


def test_t_3761_4_utility_default_when_nothing_set(clean_env, tmp_path) -> None:
    """T-3761-4: with no env vars, falls back to ~/.local/share/...

    Honours XDG_DATA_HOME so a CI environment with a custom XDG layout
    isn't surprised.
    """
    clean_env.setenv("XDG_DATA_HOME", str(tmp_path))
    expected = tmp_path / "almanak" / "utility" / LOCAL_DB_FILENAME
    assert local_db_path() == expected


# ---------------------------------------------------------------------------
# T-3761-5: cwd-relative is GONE
# ---------------------------------------------------------------------------
def test_t_3761_5_no_cwd_relative_default(clean_env, tmp_path, monkeypatch) -> None:
    """T-3761-5: with all env vars unset and XDG unset, the path must NOT be
    cwd-relative. The cwd-relative ``./almanak_state.db`` legacy default
    was the April 29 silent-failure root cause and is removed.
    """
    monkeypatch.chdir(tmp_path)
    resolved = local_db_path()
    assert resolved.is_absolute(), f"resolver returned a relative path: {resolved}"
    assert resolved != Path.cwd() / LOCAL_DB_FILENAME, (
        "resolver fell back to cwd-relative ./almanak_state.db — VIB-3761 forbids this"
    )


# ---------------------------------------------------------------------------
# T-3761-6: empty / whitespace env values are ignored
# ---------------------------------------------------------------------------
def test_t_3761_6_empty_env_values_are_ignored(clean_env, tmp_path) -> None:
    """T-3761-6: blank env values must not change the resolution branch.

    Catches the shortcut ``"X" in os.environ`` which would treat empty
    values as set and pick the wrong branch.
    """
    folder = tmp_path / "strategy"
    folder.mkdir()
    clean_env.setenv("ALMANAK_STATE_DB", "")
    clean_env.setenv("ALMANAK_STRATEGY_FOLDER", str(folder))
    clean_env.setenv("ALMANAK_GATEWAY_DB_PATH", "")
    # Empty STATE_DB / GATEWAY_DB_PATH must be skipped; STRATEGY_FOLDER wins.
    assert local_db_path() == folder.resolve() / LOCAL_DB_FILENAME


# ---------------------------------------------------------------------------
# T-3761-7: hosted mode refuses the helper
# ---------------------------------------------------------------------------
def test_t_3761_7_hosted_mode_refuses(clean_env) -> None:
    """T-3761-7: in hosted mode the local-path helpers raise LocalPathError.

    Hosted mode reads/writes Postgres; calling local_db_path() from there
    is a programmer error and must surface, not silently return a path.
    """
    clean_env.setenv("AGENT_ID", "agent-test")
    with pytest.raises(LocalPathError, match=r"hosted mode"):
        local_db_path()
    with pytest.raises(LocalPathError, match=r"hosted mode"):
        local_log_path("gateway")


# ---------------------------------------------------------------------------
# T-3761-8: log path mirrors DB folder
# ---------------------------------------------------------------------------
def test_t_3761_8_log_path_mirrors_db_folder(clean_env, tmp_path) -> None:
    """T-3761-8: logs sit alongside the DB so ops grep one location."""
    folder = tmp_path / "strategy"
    folder.mkdir()
    clean_env.setenv("ALMANAK_STRATEGY_FOLDER", str(folder))
    assert local_log_path("gateway") == folder.resolve() / "gateway.log"


def test_log_path_rejects_empty_name(clean_env, tmp_path) -> None:
    folder = tmp_path / "strategy"
    folder.mkdir()
    clean_env.setenv("ALMANAK_STRATEGY_FOLDER", str(folder))
    with pytest.raises(LocalPathError, match=r"non-empty"):
        local_log_path("")


# ---------------------------------------------------------------------------
# T-3761-9 / T-3761-10: flock collision behavior
# ---------------------------------------------------------------------------
def test_t_3761_9_flock_acquire_release_roundtrip(clean_env, tmp_path) -> None:
    """T-3761-9: a single acquire+release must not raise."""
    db_path = tmp_path / "lock-test.db"
    handle = acquire_local_db_lock(db_path)
    assert handle is not None
    release_local_db_lock(handle)


def test_t_3761_10_second_acquire_blocks(clean_env, tmp_path) -> None:
    """T-3761-10: a second non-blocking acquire raises LocalDbLockError.

    Plan §B: 1 strategy = 1 DB = 1 gateway. The flock is the OS-level
    enforcement of that invariant. Without this test, two gateway
    processes can race on the same DB path again — the April 29 root
    cause.
    """
    db_path = tmp_path / "collision.db"
    first = acquire_local_db_lock(db_path)
    try:
        with pytest.raises(LocalDbLockError, match=r"Another gateway already holds"):
            acquire_local_db_lock(db_path)
    finally:
        release_local_db_lock(first)
    # After release, another acquire works.
    second = acquire_local_db_lock(db_path)
    release_local_db_lock(second)


# ---------------------------------------------------------------------------
# T-3761-11: legacy cwd-DB warning
# ---------------------------------------------------------------------------
def test_t_3761_11_warn_legacy_cwd_db(clean_env, tmp_path, monkeypatch) -> None:
    """T-3761-11: when ./almanak_state.db sits in cwd, log an instruction.

    Plan §7 R1: emit operator instructions only — no auto-mv.
    """
    # Place a sentinel cwd DB and pretend cwd is here.
    monkeypatch.chdir(tmp_path)
    legacy = tmp_path / LOCAL_DB_FILENAME
    legacy.write_bytes(b"")
    # Set XDG so canonical resolves to a known place in tmp.
    clean_env.setenv("XDG_DATA_HOME", str(tmp_path / "xdg"))

    captured = MagicMock()
    warn_if_legacy_cwd_db_exists(captured)

    assert captured.warning.called, "expected a warning when legacy cwd DB exists"
    msg = captured.warning.call_args.args[0] if captured.warning.call_args.args else ""
    assert "Legacy ./almanak_state.db detected" in msg


def test_warn_legacy_cwd_db_silent_when_absent(clean_env, tmp_path, monkeypatch) -> None:
    """When no legacy DB sits in cwd, the helper is silent."""
    monkeypatch.chdir(tmp_path)
    captured = MagicMock()
    warn_if_legacy_cwd_db_exists(captured)
    assert not captured.warning.called


# ---------------------------------------------------------------------------
# T-3761-12: anti-gaming guard — no cwd-relative ./almanak_state.db reads
# remain in production code.
# ---------------------------------------------------------------------------
def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def test_t_3761_12_no_cwd_relative_db_default_remains() -> None:
    """T-3761-12: zero ``"./almanak_state.db"`` literals in production code.

    Anti-gaming guard. The whole point of §B is that this string is
    forbidden — it was the April 29 silent-failure root cause. Future
    PRs can reintroduce it by accident; this test catches it at PR time.

    Allowlist:
      * ``almanak/framework/local_paths.py`` defines ``LOCAL_DB_FILENAME``
        but never the ``./`` form.
      * Comments and docstrings that REFERENCE the legacy default for
        historical context are stripped before the regex check.
    """
    root = _repo_root()
    pattern = re.compile(r'["\']\./almanak_state\.db["\']')

    completed = subprocess.run(
        ["git", "ls-files", "--", "*.py"],
        cwd=root,
        capture_output=True,
        text=True,
        check=True,
    )
    candidate_paths = [Path(line) for line in completed.stdout.splitlines() if line.strip()]

    PRODUCTION_PREFIXES = ("almanak/", "platform-plugins/")
    TEST_DIR_TOKENS = ("/tests/", "tests/")

    offenders: list[str] = []
    for rel in candidate_paths:
        rel_str = str(rel)
        if not rel_str.startswith(PRODUCTION_PREFIXES):
            continue
        if any(token in rel_str for token in TEST_DIR_TOKENS):
            continue
        try:
            content = (root / rel).read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        # Strip line comments and triple-quoted blocks; only code-level
        # occurrences count as offenders.
        code_only = re.sub(r"#[^\n]*", "", content)
        code_only = re.sub(r"\"\"\"[\s\S]*?\"\"\"", "", code_only)
        code_only = re.sub(r"'''[\s\S]*?'''", "", code_only)
        if pattern.search(code_only):
            offenders.append(rel_str)

    assert not offenders, (
        "Found cwd-relative './almanak_state.db' literals in production code.\n"
        "VIB-3761 §B forbids this — it was the April 29 silent-failure root cause.\n"
        "Use almanak.framework.local_paths.local_db_path() instead.\n"
        "Offenders:\n  - " + "\n  - ".join(offenders)
    )
