"""Unit tests for ``almanak.framework.local_paths`` — VIB-3761, plan §B.

The local SDK is folder-scoped. These tests pin the resolution priority
order (explicit override → strategy folder → utility default), the
hosted-mode refusal, the flock collision behaviour, and the legacy-cwd
migration warning.

Test IDs T-3761-1..T-3761-12.
"""

from __future__ import annotations

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
    acquire_local_db_lock_with_utility_fallback,
    local_db_path,
    local_log_path,
    local_strategy_db_path,
    release_local_db_lock,
    set_strategy_folder,
    utility_db_path,
    warn_if_legacy_cwd_db_exists,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
@pytest.fixture
def clean_env(monkeypatch: pytest.MonkeyPatch) -> pytest.MonkeyPatch:
    """Clear every env var the resolver consults so each test starts neutral."""
    for var in (
        "ALMANAK_IS_HOSTED",
        "ALMANAK_DEPLOYMENT_ID",
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


def test_t_3761_3_gateway_db_path_used_when_no_strategy_folder(clean_env, tmp_path) -> None:
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
    clean_env.setenv("ALMANAK_IS_HOSTED", "true")
    clean_env.setenv("ALMANAK_DEPLOYMENT_ID", "agent-test")
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
# VIB-5550: utility-DB flock contention tolerance
# ---------------------------------------------------------------------------
def test_vib_5550_retry_succeeds_when_lock_released_within_budget(clean_env, tmp_path) -> None:
    """A bounded-retry acquire succeeds once the holder releases.

    Simulated contention: one handle holds the lock; a timer thread
    releases it mid-budget; the retrying acquire must succeed within
    the budget instead of dying on the first attempt.
    """
    import threading
    import time as _time

    db_path = tmp_path / "retry.db"
    holder = acquire_local_db_lock(db_path)
    threading.Timer(0.3, release_local_db_lock, args=(holder,)).start()

    start = _time.monotonic()
    handle = acquire_local_db_lock(db_path, timeout=3.0, poll_interval=0.05)
    elapsed = _time.monotonic() - start
    try:
        assert handle is not None
        assert elapsed < 3.0, f"retry should succeed within budget, took {elapsed:.2f}s"
    finally:
        release_local_db_lock(handle)


def test_vib_5550_retry_exhausted_still_raises(clean_env, tmp_path) -> None:
    """An exhausted retry budget still raises LocalDbLockError — never
    silently proceed without the lock."""
    import time as _time

    db_path = tmp_path / "exhaust.db"
    holder = acquire_local_db_lock(db_path)
    try:
        start = _time.monotonic()
        with pytest.raises(LocalDbLockError, match=r"Another gateway already holds"):
            acquire_local_db_lock(db_path, timeout=0.3, poll_interval=0.05)
        assert _time.monotonic() - start >= 0.3
    finally:
        release_local_db_lock(holder)


def test_vib_5550_default_acquire_remains_single_attempt(clean_env, tmp_path) -> None:
    """timeout=0.0 default preserves the original fail-fast semantics
    (strategy-DB callers must not wait — two gateways on one strategy DB
    is a hard invariant violation)."""
    import time as _time

    db_path = tmp_path / "failfast.db"
    holder = acquire_local_db_lock(db_path)
    try:
        start = _time.monotonic()
        with pytest.raises(LocalDbLockError):
            acquire_local_db_lock(db_path)
        assert _time.monotonic() - start < 0.5
    finally:
        release_local_db_lock(holder)


def test_vib_5550_non_blocking_path_never_sleeps(clean_env, tmp_path, monkeypatch) -> None:
    """A non-positive ``timeout`` must be *structurally* single-attempt.

    Gemini review (PR #3213): the retry loop must never reach ``time.sleep``
    on the ``timeout<=0`` path — coarse ``monotonic`` resolution or a future
    ``>=``→``>`` edit must not be able to turn the fail-fast default into a
    poll/wait. Assert ``time.sleep`` is never invoked when the lock is
    contended at ``timeout=0.0``. A positive budget, by contrast, DOES sleep
    between attempts.
    """
    sleep_spy = MagicMock()
    monkeypatch.setattr("almanak.framework.local_paths.time.sleep", sleep_spy)

    db_path = tmp_path / "nonblocking.db"
    holder = acquire_local_db_lock(db_path)
    try:
        with pytest.raises(LocalDbLockError):
            acquire_local_db_lock(db_path)  # timeout=0.0 default
        sleep_spy.assert_not_called()

        # Sanity: a positive budget takes the retry branch and DOES sleep, so
        # the assertion above is meaningful (not vacuously true because the
        # loop can never sleep at all).
        with pytest.raises(LocalDbLockError):
            acquire_local_db_lock(db_path, timeout=0.05, poll_interval=0.01)
        assert sleep_spy.called
    finally:
        release_local_db_lock(holder)


def test_vib_5550_utility_fallback_allocates_session_db(clean_env, tmp_path) -> None:
    """Contended canonical utility DB → per-session fallback DB.

    The fallback must: land under utility/sessions/, hold its own lock,
    export ALMANAK_GATEWAY_DB_PATH so later lenient resolution follows,
    and warn loudly (never silent).
    """
    clean_env.setenv("XDG_DATA_HOME", str(tmp_path))
    canonical = utility_db_path()
    holder = acquire_local_db_lock(canonical)
    log = MagicMock()
    try:
        handle, effective = acquire_local_db_lock_with_utility_fallback(canonical, log, retry_budget_s=0.05)
        try:
            assert effective != canonical
            assert effective.parent.parent == canonical.parent / "sessions"
            assert os.environ["ALMANAK_GATEWAY_DB_PATH"] == str(effective)
            # Lenient resolution now follows the session DB (with the
            # ALMANAK_GATEWAY_DB_PATH branch's .resolve() applied).
            assert local_db_path() == effective.resolve()
            log.warning.assert_called_once()
            # The session lock is genuinely held.
            with pytest.raises(LocalDbLockError):
                acquire_local_db_lock(effective)
        finally:
            release_local_db_lock(handle)
    finally:
        release_local_db_lock(holder)
        os.environ.pop("ALMANAK_GATEWAY_DB_PATH", None)


def test_vib_5550_utility_uncontended_keeps_canonical(clean_env, tmp_path) -> None:
    """No contention → the canonical utility DB is used and no env export
    happens (warm per-user caches preserved for the common case)."""
    clean_env.setenv("XDG_DATA_HOME", str(tmp_path))
    canonical = utility_db_path()
    log = MagicMock()
    handle, effective = acquire_local_db_lock_with_utility_fallback(canonical, log, retry_budget_s=0.05)
    try:
        assert effective == canonical
        assert "ALMANAK_GATEWAY_DB_PATH" not in os.environ
        log.warning.assert_not_called()
    finally:
        release_local_db_lock(handle)


def test_vib_5550_pinned_path_never_falls_back(clean_env, tmp_path) -> None:
    """A non-utility (explicitly pinned) path keeps hard-fail semantics —
    two gateways sharing a pinned path IS the invariant violation."""
    clean_env.setenv("XDG_DATA_HOME", str(tmp_path))
    pinned = tmp_path / "pinned" / LOCAL_DB_FILENAME
    holder = acquire_local_db_lock(pinned)
    log = MagicMock()
    try:
        with pytest.raises(LocalDbLockError, match=r"Another gateway already holds"):
            acquire_local_db_lock_with_utility_fallback(pinned, log, retry_budget_s=0.05)
        assert "ALMANAK_GATEWAY_DB_PATH" not in os.environ
        log.warning.assert_not_called()
    finally:
        release_local_db_lock(holder)


def test_vib_5550_env_pinned_to_canonical_path_never_falls_back(clean_env, tmp_path) -> None:
    """An env var explicitly pinned to a path that HAPPENS to equal the
    canonical utility DB is still a pin — contention must hard-fail, not
    silently reroute the operator's chosen path to a session DB."""
    clean_env.setenv("XDG_DATA_HOME", str(tmp_path))
    canonical = utility_db_path()
    clean_env.setenv("ALMANAK_GATEWAY_DB_PATH", str(canonical))
    holder = acquire_local_db_lock(canonical)
    log = MagicMock()
    try:
        with pytest.raises(LocalDbLockError, match=r"Another gateway already holds"):
            acquire_local_db_lock_with_utility_fallback(canonical, log, retry_budget_s=0.05)
        assert os.environ["ALMANAK_GATEWAY_DB_PATH"] == str(canonical)  # untouched
        log.warning.assert_not_called()
    finally:
        release_local_db_lock(holder)


def test_vib_5550_concurrent_fallbacks_get_distinct_session_dbs(clean_env, tmp_path) -> None:
    """Two contenders falling back concurrently must not collide with
    each other — every session DB path is unique.

    The env export is process-scoped in production; simulate the second
    contender's separate process by clearing the first export before the
    second call (in-process, the export IS the pin — a restarting managed
    gateway resolves straight to its own session DB, no second fallback).
    """
    clean_env.setenv("XDG_DATA_HOME", str(tmp_path))
    canonical = utility_db_path()
    holder = acquire_local_db_lock(canonical)
    log = MagicMock()
    try:
        h1, p1 = acquire_local_db_lock_with_utility_fallback(canonical, log, retry_budget_s=0.05)
        os.environ.pop("ALMANAK_GATEWAY_DB_PATH", None)  # contender #2 = fresh process env
        h2, p2 = acquire_local_db_lock_with_utility_fallback(canonical, log, retry_budget_s=0.05)
        try:
            assert p1 != p2
            assert p1 != canonical and p2 != canonical
        finally:
            release_local_db_lock(h1)
            release_local_db_lock(h2)
    finally:
        release_local_db_lock(holder)
        os.environ.pop("ALMANAK_GATEWAY_DB_PATH", None)


def test_vib_5550_stale_session_dirs_are_swept(clean_env, tmp_path) -> None:
    """Session dirs whose owner exited (lock acquirable) are GC'd on the
    next utility acquisition; live and freshly created sessions are left
    alone (the age guard protects a contender mid-boot whose flock isn't
    taken yet)."""
    import time as _time

    clean_env.setenv("XDG_DATA_HOME", str(tmp_path))
    canonical = utility_db_path()
    sessions = canonical.parent / "sessions"

    # Stale session: dir + lock file exist, nobody holds the lock, old
    # enough to clear the anti-race age guard.
    stale = sessions / "gw-99999-deadbeef"
    stale.mkdir(parents=True)
    (stale / LOCAL_DB_FILENAME).touch()
    (stale / f"{LOCAL_DB_FILENAME}.gw.lock").touch()
    backdated = _time.time() - 3600
    os.utime(stale, (backdated, backdated))

    # Fresh session: unheld lock but created just now — mid-boot window.
    fresh = sessions / "gw-88888-feedface"
    fresh.mkdir(parents=True)
    (fresh / f"{LOCAL_DB_FILENAME}.gw.lock").touch()

    # Live session: its lock is held (backdated so only the flock saves it).
    live_db = sessions / "gw-11111-cafecafe" / LOCAL_DB_FILENAME
    live_holder = acquire_local_db_lock(live_db)
    os.utime(live_db.parent, (backdated, backdated))

    log = MagicMock()
    try:
        handle, effective = acquire_local_db_lock_with_utility_fallback(canonical, log, retry_budget_s=0.05)
        try:
            assert effective == canonical  # uncontended canonical
            assert not stale.exists(), "stale session dir should be GC'd"
            assert fresh.exists(), "fresh session dir must survive (age guard)"
            assert live_db.parent.exists(), "live session dir must survive the sweep"
        finally:
            release_local_db_lock(handle)
    finally:
        release_local_db_lock(live_holder)


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


# ---------------------------------------------------------------------------
# VIB-3835: local_strategy_db_path — strict, no utility-DB fallback
# ---------------------------------------------------------------------------
def test_strategy_db_path_uses_strategy_folder(clean_env, tmp_path) -> None:
    """VIB-3835: ALMANAK_STRATEGY_FOLDER selects the strategy-folder DB."""
    folder = tmp_path / "strategy"
    folder.mkdir()
    clean_env.setenv("ALMANAK_STRATEGY_FOLDER", str(folder))
    assert local_strategy_db_path() == folder.resolve() / LOCAL_DB_FILENAME


def test_strategy_db_path_honors_explicit_state_db(clean_env, tmp_path) -> None:
    """VIB-3835: ALMANAK_STATE_DB beats every other branch (test escape hatch)."""
    explicit = tmp_path / "explicit.db"
    clean_env.setenv("ALMANAK_STATE_DB", str(explicit))
    clean_env.setenv("ALMANAK_STRATEGY_FOLDER", str(tmp_path / "strategy"))
    assert local_strategy_db_path() == explicit.resolve()


def test_strategy_db_path_raises_when_no_folder(clean_env) -> None:
    """VIB-3835: strict resolver hard-fails instead of falling back to utility DB.

    The May 1 mainnet teardown surfaced the silent-fallback failure mode:
    ``teardown request`` ran from a second shell with no ``ALMANAK_STRATEGY_FOLDER``,
    fell through to ``~/.local/share/almanak/utility/almanak_state.db``, and the
    runner (polling the strategy-folder DB) never saw the request. The strict
    resolver eliminates that branch.
    """
    with pytest.raises(LocalPathError, match=r"no strategy folder resolved"):
        local_strategy_db_path()


def test_strategy_db_path_ignores_gateway_db_path_env(clean_env, tmp_path) -> None:
    """VIB-3835: ALMANAK_GATEWAY_DB_PATH is a gateway-only override; strategy
    operations must not honour it.

    The utility-DB path (whether default or via ``ALMANAK_GATEWAY_DB_PATH``) is
    explicitly out of scope for strategy-scoped writers. Folding it back in
    would re-open the silent-DB-mismatch class.
    """
    clean_env.setenv("ALMANAK_GATEWAY_DB_PATH", str(tmp_path / "gateway-only.db"))
    with pytest.raises(LocalPathError, match=r"no strategy folder resolved"):
        local_strategy_db_path()


def test_strategy_db_path_hosted_mode_refuses(clean_env) -> None:
    """VIB-3835: like local_db_path, the strict variant refuses in hosted mode."""
    clean_env.setenv("ALMANAK_IS_HOSTED", "true")
    clean_env.setenv("ALMANAK_DEPLOYMENT_ID", "agent-test")
    with pytest.raises(LocalPathError, match=r"hosted mode"):
        local_strategy_db_path()


# ---------------------------------------------------------------------------
# Phase 4c (#2100): set_strategy_folder() centralised setter
# ---------------------------------------------------------------------------
def test_set_strategy_folder_writes_env(clean_env) -> None:
    """The setter pins ``ALMANAK_STRATEGY_FOLDER`` for downstream readers.

    Phase 4c rationale: CLI handlers used to mutate ``os.environ`` inline
    in three places. Routing every CLI write through this setter keeps the
    mutation in the single allowlisted file (``local_paths.py``) instead.
    """
    set_strategy_folder("/tmp/foo")
    assert os.environ["ALMANAK_STRATEGY_FOLDER"] == "/tmp/foo"


def test_set_strategy_folder_accepts_path_object(clean_env) -> None:
    """The setter coerces ``Path`` to ``str`` so callers don't have to."""
    set_strategy_folder(Path("/tmp/foo"))
    assert os.environ["ALMANAK_STRATEGY_FOLDER"] == str(Path("/tmp/foo"))
