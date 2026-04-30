"""Local SDK file-path resolver — VIB-3761, plan §B.

The local SDK is folder-scoped: 1 strategy = 1 folder = 1 DB = 1 gateway.
This module is the **single source of truth** for where local files live;
every other module that needs a local DB or log path must call into here.

Three resolution shapes — pick by which env var is set, in priority order:

1. **Explicit override** (``ALMANAK_STATE_DB`` is set):
   - DB: that path verbatim. Reserved for tests and one-off operator
     workflows; production runs should not rely on it.
2. **Strategy-anchored** (``ALMANAK_STRATEGY_FOLDER`` is set, set by the
   strategy CLI when launching a companion gateway):
   - DB:   ``<folder>/almanak_state.db``
   - Logs: ``<folder>/<name>.log``
3. **Utility / standalone** (no strategy folder, typical for ``almanak ax``,
   intent tests, ad-hoc gateway sessions):
   - DB:   ``ALMANAK_GATEWAY_DB_PATH`` if set; else
           ``$XDG_DATA_HOME/almanak/utility/almanak_state.db``; else
           ``~/.local/share/almanak/utility/almanak_state.db``
   - Logs: same directory.

The cwd-relative ``./almanak_state.db`` legacy default is **removed**. It
was the proximate cause of April 29's silent accounting failure — 10
strategies launched from the same cwd colliding on a single DB file.

Hosted mode (``AGENT_ID`` set) has no local DB; calling these helpers in
hosted mode is a programmer error and raises :class:`LocalPathError`.
"""

from __future__ import annotations

import errno
import os
import shlex
from pathlib import Path

LOCAL_DB_FILENAME = "almanak_state.db"


class LocalPathError(RuntimeError):
    """Raised when a local-path helper is called in hosted mode, or with
    an obviously-bad argument.
    """


def _utility_data_dir() -> Path:
    """Stable per-user directory for non-strategy-anchored sessions.

    Honours ``XDG_DATA_HOME`` so Linux users with a custom XDG layout
    don't get files dumped in ``~/.local/share`` regardless.
    """
    xdg = os.environ.get("XDG_DATA_HOME")
    if xdg and xdg.strip():
        return Path(xdg.strip()).expanduser() / "almanak" / "utility"
    return Path.home() / ".local" / "share" / "almanak" / "utility"


def _strategy_folder() -> Path | None:
    raw = os.environ.get("ALMANAK_STRATEGY_FOLDER")
    if not raw or not raw.strip():
        return None
    return Path(raw.strip()).expanduser().resolve()


def _ensure_local() -> None:
    """Refuse to be used in hosted mode.

    Hosted mode reads/writes Postgres via ``ALMANAK_GATEWAY_DATABASE_URL``.
    Calling a local-path helper in hosted mode is a programmer error.
    """
    from almanak.framework.deployment import is_hosted

    if is_hosted():
        raise LocalPathError(
            "local-path helper called in hosted mode (AGENT_ID set). "
            "Hosted mode uses Postgres via ALMANAK_GATEWAY_DATABASE_URL."
        )


def _resolve_db_path() -> Path:
    """Internal: pure resolution with no filesystem side effects.

    Kept separate from :func:`local_db_path` so tests can pin the
    resolution rules without parent-dir creation interfering.
    """
    explicit = os.environ.get("ALMANAK_STATE_DB")
    if explicit and explicit.strip():
        return Path(explicit.strip()).expanduser().resolve()

    folder = _strategy_folder()
    if folder is not None:
        return folder / LOCAL_DB_FILENAME

    explicit_gw = os.environ.get("ALMANAK_GATEWAY_DB_PATH")
    if explicit_gw and explicit_gw.strip():
        return Path(explicit_gw.strip()).expanduser().resolve()

    return _utility_data_dir() / LOCAL_DB_FILENAME


def local_db_path() -> Path:
    """Resolve the SQLite DB path for this gateway / CLI session.

    See module docstring for resolution order. The parent directory is
    created (idempotent) so callers can pass the result directly to
    ``sqlite3.connect`` without an extra ``mkdir`` step. Pure-resolution
    callers that don't want this side effect can call ``_resolve_db_path``.
    """
    _ensure_local()
    path = _resolve_db_path()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
    except OSError:
        # Best-effort: if the user's filesystem is read-only, surface the
        # failure at connect time rather than here.
        pass
    return path


def local_log_path(name: str) -> Path:
    """Resolve a log-file path for this gateway / CLI session.

    Logs sit alongside the DB (same directory) so ops can grep both
    without remembering two paths. ``name`` is the log identifier
    without the extension (``"gateway"``, ``"runner"``, etc.).

    Implementation: delegate to :func:`local_db_path` so the log
    location *follows* whatever wins DB resolution — if an operator
    sets ``ALMANAK_STATE_DB=/tmp/custom.db`` the runner / gateway log
    lands next to ``/tmp/custom.db``, not in the strategy or utility
    directory we never opened. Reusing ``local_db_path`` also picks up
    its parent-dir creation, so a log-only ``almanak ax`` invocation
    on a fresh utility install does not blow up trying to write into
    a non-existent directory.
    """
    _ensure_local()
    if not name or not name.strip():
        raise LocalPathError("local_log_path() requires a non-empty name")
    safe = name.strip()
    # Reject path components — ``../shared/runner`` or ``subdir/gateway``
    # could escape the canonical DB parent and defeat the
    # folder-scoped invariant. We only accept simple file stems.
    candidate = Path(safe)
    if candidate.is_absolute() or candidate.name != safe or safe in (".", ".."):
        raise LocalPathError(
            f"local_log_path() name must be a simple file stem (no path components, no separators); got {name!r}"
        )
    return local_db_path().parent / f"{safe}.log"


def warn_if_legacy_cwd_db_exists(logger) -> None:
    """If a legacy ``./almanak_state.db`` sits in cwd, log a one-time
    operator instruction with the exact ``mv`` command.

    Idempotent. Plan §7 R1: instructions only — no auto-mv (could be
    destructive on a multi-strategy operator's machine).
    """
    legacy = Path.cwd() / LOCAL_DB_FILENAME
    if not legacy.exists():
        return
    try:
        canonical = local_db_path()
    except LocalPathError:
        # In hosted mode there is no local target. Nothing to warn about.
        return
    try:
        if legacy.resolve() == canonical.resolve():
            return  # operator pointed ALMANAK_STATE_DB at cwd; nothing to migrate
    except OSError:
        pass
    # Shell-quote the substitutions so the command stays valid when
    # paths contain spaces (e.g. macOS "Library/Application Support")
    # or other shell metacharacters. The bare paths in the leading
    # %s slots are still rendered without quoting because they appear
    # in human-readable prose, not shell syntax.
    logger.warning(
        "Legacy ./almanak_state.db detected at %s. The new local-DB location "
        "for this session is %s. Move the file with: "
        "mkdir -p %s && mv %s %s. Future runs will use the new location "
        "regardless; the file at %s will be ignored.",
        legacy,
        canonical,
        shlex.quote(str(canonical.parent)),
        shlex.quote(str(legacy)),
        shlex.quote(str(canonical)),
        legacy,
    )


# ---------------------------------------------------------------------------
# Single-writer flock (plan §B "1 strategy = 1 DB = 1 gateway")
# ---------------------------------------------------------------------------
class LocalDbLockError(RuntimeError):
    """A second process is already holding the local DB lock for this path."""


def _lock_file_path(db_path: Path) -> Path:
    """Sibling lock file. ``.gw.lock`` rather than ``.lock`` so it doesn't
    collide with anything SQLite or another tool already uses.
    """
    return db_path.with_suffix(db_path.suffix + ".gw.lock")


def acquire_local_db_lock(db_path: Path) -> int:
    """Acquire an exclusive, non-blocking flock on the gateway DB path.

    Returns the underlying file descriptor (an ``int``) the caller must
    keep alive for the gateway lifetime. Released by closing the
    descriptor (e.g., garbage collection at process exit, or explicit
    ``release_local_db_lock``).

    Raises :class:`LocalDbLockError` if another gateway already holds
    the lock — this is the OS-level enforcement of the 1 strategy = 1 DB
    = 1 gateway invariant. The error message names the conflicting path
    so the operator can locate the other process.
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = _lock_file_path(db_path)
    fd = os.open(lock_path, os.O_WRONLY | os.O_CREAT, 0o600)
    try:
        # Local import: ``fcntl`` is POSIX-only, but every supported
        # Almanak environment is POSIX. Importing here keeps the module
        # importable on Windows for completeness (e.g., test harnesses
        # that don't actually call this function).
        import fcntl

        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            if exc.errno in (errno.EWOULDBLOCK, errno.EAGAIN):
                os.close(fd)
                raise LocalDbLockError(
                    f"Another gateway already holds the lock on {db_path}. "
                    "Plan §B requires 1 strategy = 1 DB = 1 gateway. "
                    "Stop the other gateway or use a different "
                    "ALMANAK_STRATEGY_FOLDER / ALMANAK_GATEWAY_DB_PATH."
                ) from exc
            os.close(fd)
            raise
    except Exception:
        try:
            os.close(fd)
        except OSError:
            pass
        raise
    # Stamp the lock file with our PID for forensic clarity.
    try:
        os.write(fd, f"{os.getpid()}\n".encode())
    except OSError:
        pass
    return fd  # caller owns; release via release_local_db_lock(fd)


def release_local_db_lock(handle: int | None) -> None:
    """Release a lock acquired via :func:`acquire_local_db_lock`.

    ``handle`` is the file descriptor returned by
    :func:`acquire_local_db_lock`. Accepts ``None`` for callers that
    track the handle as ``int | None`` and may not have acquired one.
    """
    if handle is None:
        return
    try:
        import fcntl

        fcntl.flock(handle, fcntl.LOCK_UN)
    except Exception:
        pass
    try:
        os.close(handle)
    except Exception:
        pass
