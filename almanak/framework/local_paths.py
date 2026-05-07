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
    """Resolve ``ALMANAK_STRATEGY_FOLDER`` to an existing directory, or ``None``.

    Returning ``None`` for a missing / non-directory path lets the strict
    resolver hard-fail with the canonical "no strategy folder resolved"
    remediation. Without this check, a stale env var like
    ``/tmp/typo`` would silently create a fresh folder DB at that path —
    re-opening the May 1 silent-failure class the strict resolver exists
    to close.
    """
    raw = os.environ.get("ALMANAK_STRATEGY_FOLDER")
    if not raw or not raw.strip():
        return None
    path = Path(raw.strip()).expanduser().resolve()
    if not path.is_dir():
        return None
    return path


def looks_like_strategy_folder(path: Path) -> bool:
    """Return True if ``path`` contains a strategy entry point.

    A strategy folder is identified by a ``config.json`` (primary signal —
    every Almanak demo strategy ships one) or a ``config.yaml`` /
    ``config.yml`` (some incubating strategies) or a ``strategy.py``
    (decorator-driven strategies without an explicit config file).

    Used by ``almanak gateway`` and ``almanak strat teardown`` to
    auto-detect a strategy folder from cwd so the operator does not have
    to remember to set ``ALMANAK_STRATEGY_FOLDER`` for every invocation.
    """
    if not path.is_dir():
        return False
    if (path / "config.json").is_file():
        return True
    if (path / "config.yaml").is_file() or (path / "config.yml").is_file():
        return True
    return (path / "strategy.py").is_file()


def auto_detect_strategy_folder(*, export_env: bool = True) -> Path | None:
    """Auto-detect a strategy folder from cwd and (optionally) export
    ``ALMANAK_STRATEGY_FOLDER``.

    Mirrors the cwd-detection block in ``almanak strat run`` (cli/run.py)
    and ``almanak strat teardown`` (cli/teardown.py) so a gateway started
    from inside a strategy folder pins to that folder's DB instead of
    silently falling through to ``~/.local/share/almanak/utility``.

    Resolution order:
        1. If ``ALMANAK_STRATEGY_FOLDER`` is already set to a real
           directory, return it (operator override wins).
        2. If cwd looks like a strategy folder
           (:func:`looks_like_strategy_folder`), export it (when
           ``export_env`` is True) and return.
        3. Otherwise return ``None`` — the caller must decide whether
           to hard-fail or accept standalone-mode operation.

    The ``export_env`` knob lets internal callers query "is this cwd a
    strategy folder?" without mutating process-global env. CLI helpers
    that intentionally pin downstream lookups (``almanak gateway``,
    ``almanak strat teardown``) keep the default ``True``; the strict
    DB-path resolver passes ``False`` so a single ``local_strategy_db_path()``
    probe does not permanently re-anchor later resolution in the same
    process.

    Idempotent: callers can invoke it multiple times safely; the env var
    is only written when a folder is actually resolved AND ``export_env``
    is True.
    """
    existing = _strategy_folder()
    if existing is not None:
        return existing

    try:
        cwd = Path.cwd().resolve()
    except OSError:
        # cwd was deleted or is otherwise inaccessible. Fall through to
        # ``None`` so callers can decide between hard-fail and standalone
        # mode rather than letting the OSError bubble up.
        return None
    if looks_like_strategy_folder(cwd):
        if export_env:
            os.environ["ALMANAK_STRATEGY_FOLDER"] = str(cwd)
        return cwd

    return None


def set_strategy_folder(path: Path | str) -> None:
    """Pin ``ALMANAK_STRATEGY_FOLDER`` for downstream env-readers.

    Centralised setter. CLI handlers that resolve a strategy folder
    from arguments / cwd and need to make it visible to env-reading
    consumers (the gateway, runtime config, teardown helpers) call
    this instead of mutating ``os.environ`` directly. The mutation
    still happens — it just happens in this single allowlisted file
    rather than scattered across CLI handlers (issue #2100, plan
    Phase 4c).

    Idempotent: re-pinning the same path is a no-op for downstream
    readers; re-pinning a different path overwrites cleanly.

    Args:
        path: A ``Path`` or path-like ``str``. ``str(...)`` is applied
            so callers don't need to coerce.
    """
    os.environ["ALMANAK_STRATEGY_FOLDER"] = str(path)


def strategy_folder_env() -> str | None:
    """Return the raw ``ALMANAK_STRATEGY_FOLDER`` env value (or ``None``).

    The CLI surface needs the *raw* env value in three places:

    * The teardown CLI's resolution ladder, which falls through to a
      cwd check when the env value points at a non-strategy folder
      (validation already lives in :func:`_strategy_folder` — but the
      caller wants the raw string so it can decide whether to fall
      through silently or warn).
    * The teardown CLI's interrupt-resume hint, which echoes the
      configured folder verbatim back to the operator.
    * The accountant-test CLI's ``--working-dir`` save / restore
      pattern that pushes a folder onto the env, runs path
      resolution, and pops the prior value back.

    Returns:
        The raw env value (whitespace preserved, no validation), or
        ``None`` when the var is unset / empty.
    """
    raw = os.environ.get("ALMANAK_STRATEGY_FOLDER")
    if not raw:
        return None
    return raw


def state_db_env() -> str | None:
    """Return the raw ``ALMANAK_STATE_DB`` env value (or ``None``).

    Companion to :func:`strategy_folder_env`. The CLI's
    ``almanak gateway`` startup path needs to know whether the operator
    has explicitly pinned a state-DB path before deciding whether to
    refuse to start, fall back to standalone mode, or auto-detect a
    strategy folder. The full resolution lives in
    :func:`local_db_path` / :func:`local_strategy_db_path`; this helper
    just exposes the raw env-value test the boot-time refuse-to-start
    branch needs.

    Returns:
        The raw env value (whitespace preserved, no validation), or
        ``None`` when the var is unset / empty.
    """
    raw = os.environ.get("ALMANAK_STATE_DB")
    if not raw:
        return None
    return raw


def push_strategy_folder(path: Path | str) -> str | None:
    """Pin ``ALMANAK_STRATEGY_FOLDER`` and return the *prior* value (or ``None``).

    Companion to :func:`pop_strategy_folder`. Callers use the returned
    value to restore the prior env state in a ``finally`` block, so the
    CLI's transient ``--working-dir`` doesn't leak across to siblings.

    Args:
        path: A ``Path`` or path-like ``str``. Resolved via
            :func:`set_strategy_folder`.

    Returns:
        The prior value of ``ALMANAK_STRATEGY_FOLDER`` (or ``None``).
    """
    prior = os.environ.get("ALMANAK_STRATEGY_FOLDER")
    set_strategy_folder(path)
    return prior


def pop_strategy_folder(prior: str | None) -> None:
    """Restore ``ALMANAK_STRATEGY_FOLDER`` to ``prior`` (or unset it).

    Companion to :func:`push_strategy_folder`. Idempotent — calling
    with the value that was already in the environment is a no-op.

    Args:
        prior: The value returned by :func:`push_strategy_folder`. Pass
            ``None`` to unset the env var.
    """
    if prior is None:
        os.environ.pop("ALMANAK_STRATEGY_FOLDER", None)
    else:
        os.environ["ALMANAK_STRATEGY_FOLDER"] = prior


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


def _resolve_db_path(*, strict: bool = False) -> Path:
    """Internal: pure resolution with no filesystem side effects.

    Kept separate from :func:`local_db_path` so tests can pin the
    resolution rules without parent-dir creation interfering.

    When ``strict`` is True, refuse to fall back to the utility-data
    directory (or to ``ALMANAK_GATEWAY_DB_PATH``). VIB-3835: strategy-
    scoped operations (teardown CLI, runner accounting writes) MUST
    resolve to a real strategy folder — silently falling through to
    a per-user utility DB caused the May 1 mainnet teardown failure.
    """
    explicit = os.environ.get("ALMANAK_STATE_DB")
    if explicit and explicit.strip():
        return Path(explicit.strip()).expanduser().resolve()

    folder = _strategy_folder()
    if folder is not None:
        return folder / LOCAL_DB_FILENAME

    # Strict-mode fallback: try the cwd-detection path before raising so
    # entry points that don't run a CLI helper first (e.g. the gateway's
    # ``_server_start_helpers.resolve_gateway_local_db_path``) still
    # auto-pin to a strategy folder when launched from inside one.
    # ``export_env=False`` so a strict probe does not permanently
    # re-anchor later path resolution in the same process — explicit
    # CLI entry points (``almanak gateway``, ``almanak strat teardown``)
    # call ``auto_detect_strategy_folder()`` directly and keep the
    # env-export side effect.
    if strict:
        cwd_folder = auto_detect_strategy_folder(export_env=False)
        if cwd_folder is not None:
            return cwd_folder / LOCAL_DB_FILENAME
        raise LocalPathError(
            "no strategy folder resolved.\n"
            "  Pass --working-dir / -d <path>, or run from a strategy folder.\n"
            "  A strategy folder must contain config.json, config.yaml, "
            "config.yml, or strategy.py."
        )

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


def local_strategy_db_path() -> Path:
    """Resolve the SQLite DB path for a *strategy-scoped* operation.

    Same as :func:`local_db_path` but refuses the utility-DB fallback
    (VIB-3835). Use this from any code path whose semantics are tied to
    a specific strategy: teardown CLI subcommands, accounting writers,
    runner state — anywhere a silent fall-through to the per-user
    utility DB would write to the wrong place.

    Raises :class:`LocalPathError` when no strategy folder can be
    resolved (no ``ALMANAK_STATE_DB``, no ``ALMANAK_STRATEGY_FOLDER``).
    """
    _ensure_local()
    path = _resolve_db_path(strict=True)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
    except OSError:
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
