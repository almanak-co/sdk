"""``almanak strat run`` -- dashboard subprocess management.

Split from run_helpers.py; import via the run_helpers facade externally.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Any

import click

logger = logging.getLogger(
    "almanak.framework.cli.run_helpers"
)  # pinned: tests + operator filters key on the historical module path


# ---------------------------------------------------------------------------
# Dashboard helpers
# ---------------------------------------------------------------------------


def _build_dashboard_subprocess_env(
    *,
    gateway_host: str,
    gateway_port: int,
    auth_token: str | None,
    mode: str,
    deployment_id: str | None,
    strategy_working_dir: str | None,
    strategy_config: dict[str, Any] | None,
) -> dict[str, str]:
    """Build the env mapping handed to the dashboard subprocess.

    Encapsulates gateway-connection forwarding (host/port/auth-token,
    plus the stale-``GATEWAY_AUTH_TOKEN`` strip from VIB-520) AND the
    hosted-parity scoping channel (deployment_id, working_dir, and the
    optional pre-resolved runtime config). Extracted out of
    ``_start_dashboard_background`` so that function stays under the
    CRAP complexity cap as additional env channels are added.
    """
    import json as _json

    from almanak.config.cli_runtime import subprocess_env_with_overrides

    overrides: dict[str, str] = {
        "GATEWAY_HOST": gateway_host,
        "GATEWAY_PORT": str(gateway_port),
    }
    if auth_token:
        overrides["ALMANAK_GATEWAY_AUTH_TOKEN"] = auth_token

    if mode == "hosted-parity" and deployment_id and strategy_working_dir:
        # Tell app_single.py which strategy to scope to and where to find
        # its ``dashboard/ui.py`` and ``config.json``. The dashboard reads
        # these from os.environ; do NOT rely on cwd because Streamlit's
        # child cwd is not the strategy's working dir.
        overrides["ALMANAK_DASHBOARD_DEPLOYMENT_ID"] = deployment_id
        overrides["ALMANAK_DASHBOARD_WORKING_DIR"] = str(Path(strategy_working_dir).resolve())
        # Forward the RESOLVED + MUTATED runtime config (post-bootstrap)
        # so the dashboard sees the same values the running strategy sees
        # — covers ``--config`` pointing outside working_dir AND copy-
        # trading / chain runtime overrides AND the resolved deployment_id
        # field. Without this, app_single re-reads working_dir/config.json
        # and renders stale values (Codex P2 on PR #2372).
        if strategy_config is not None:
            try:
                # ``default=str`` so Decimal / datetime / Path / etc. in the
                # strategy_config serialise to a string rather than crashing
                # the subprocess at boot (strategy configs frequently carry
                # Decimal for range bounds, fee tiers, target_ltv, …).
                # Lossy: the dashboard receives strings, not the typed
                # objects — but the alternative (TypeError → fall back to
                # stale on-disk config) is worse.
                overrides["ALMANAK_DASHBOARD_STRATEGY_CONFIG"] = _json.dumps(strategy_config, default=str)
            except (TypeError, ValueError):
                logger.warning(
                    "Failed to serialise strategy_config for dashboard subprocess; "
                    "app_single will fall back to working_dir/config.json (may be stale)."
                )

    env = subprocess_env_with_overrides(overrides)
    if auth_token:
        # Drop the legacy unprefixed shape so a stale .env value can't shadow
        # the session token in the spawned child (VIB-520).
        env.pop("GATEWAY_AUTH_TOKEN", None)
    return env


def _start_dashboard_background(
    *,
    port: int,
    gateway_host: str = "127.0.0.1",
    gateway_port: int = 50051,
    auth_token: str | None = None,
    mode: str = "command-center",
    deployment_id: str | None = None,
    strategy_working_dir: str | None = None,
    strategy_config: dict[str, Any] | None = None,
) -> Any:
    """Launch the Streamlit dashboard as a background subprocess.

    Mirrors the nested ``start_dashboard_background`` previously defined
    inside ``run()``. Behavior-preserving for ``mode == "command-center"``:
    probes the requested port with a transient socket bind, falls back to
    8502-8509 if busy, and returns ``None`` on any launch failure (no
    streamlit, spawn error, no free port).

    Args:
        port: The requested dashboard port.
        gateway_host: Gateway host for the dashboard env (GATEWAY_HOST).
        gateway_port: Gateway port for the dashboard env (GATEWAY_PORT).
        auth_token: Managed-gateway session token, exported in the
            subprocess env as ``ALMANAK_GATEWAY_AUTH_TOKEN`` so the
            dashboard's ``GatewayClient`` authenticates against the same
            ephemeral token the managed gateway is enforcing on mainnet.
            Without forwarding, the subprocess inherits whatever happens
            to be in the parent's env (``ALMANAK_GATEWAY_AUTH_TOKEN`` /
            ``GATEWAY_AUTH_TOKEN`` from a stale ``.env``) — but on
            mainnet the managed gateway always rolls a fresh
            ``uuid.uuid4().hex`` (VIB-520), so the inherited value never
            matches and every dashboard gRPC call returns UNAUTHENTICATED.
        mode: ``"hosted-parity"`` (single-strategy, mirrors hosted image —
            ``app_single.py``) or ``"command-center"`` (multi-strategy
            navigation — ``app.py``). Hosted-parity requires
            ``deployment_id`` and ``strategy_working_dir``.
        deployment_id: Resolved deployment_id the dashboard scopes to. Required
            for ``mode == "hosted-parity"``; ignored otherwise.
        strategy_working_dir: Strategy folder containing ``config.json`` and
            (optionally) ``dashboard/ui.py``. Required for
            ``mode == "hosted-parity"``; ignored otherwise.
        strategy_config: Resolved + mutated runtime strategy config dict
            (post ``_load_strategy_bootstrap`` / ``_prepare_runtime_bootstrap``,
            so it reflects ``--config`` overrides AND copy-trading flags AND
            the resolved ``deployment_id``). Serialized to JSON and exported
            as ``ALMANAK_DASHBOARD_STRATEGY_CONFIG``. The dashboard prefers
            this over re-reading ``working_dir/config.json`` so custom
            dashboards see the same config the running strategy sees
            (Codex P2 on PR #2372 — fixes the case where ``--config`` points
            outside ``working_dir`` or runtime overrides have mutated the
            config since startup).

    Returns:
        A ``subprocess.Popen`` handle, or ``None`` if launch failed.
    """
    import importlib.util
    import socket
    import subprocess

    if importlib.util.find_spec("streamlit") is None:
        click.echo("Error: streamlit not found. Install with: pip install 'almanak[dashboard]'", err=True)
        return None

    def is_port_available(p: int) -> bool:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind(("localhost", p))
                return True
            except OSError:
                return False

    actual_port = port
    if not is_port_available(actual_port):
        click.echo(f"Warning: Dashboard port {actual_port} is already in use.", err=True)
        for alt_port in range(8502, 8510):
            if is_port_available(alt_port):
                actual_port = alt_port
                click.echo(f"Using alternative dashboard port: {actual_port}", err=True)
                break
        else:
            click.echo(
                f"Error: Could not find an available port for dashboard. "
                f"Please free up port {port} or specify a different port with --dashboard-port",
                err=True,
            )
            return None

    project_root = Path(__file__).parent.parent.parent.parent
    dashboard_dir = project_root / "almanak" / "framework" / "dashboard"
    if mode == "hosted-parity":
        if not deployment_id or not strategy_working_dir:
            click.echo(
                "Error: hosted-parity dashboard requires deployment_id and "
                "strategy_working_dir; falling back to Command Center.",
                err=True,
            )
            dashboard_path = dashboard_dir / "app.py"
            mode = "command-center"
        else:
            dashboard_path = dashboard_dir / "app_single.py"
    elif mode == "command-center":
        dashboard_path = dashboard_dir / "app.py"
    else:
        click.echo(f"Error: unknown dashboard mode {mode!r}", err=True)
        return None

    # Build the subprocess env (gateway connection + hosted-parity scoping
    # if applicable). Extracted to a helper to keep this function under
    # the CRAP complexity cap as new env channels are added.
    env = _build_dashboard_subprocess_env(
        gateway_host=gateway_host,
        gateway_port=gateway_port,
        auth_token=auth_token,
        mode=mode,
        deployment_id=deployment_id,
        strategy_working_dir=strategy_working_dir,
        strategy_config=strategy_config,
    )

    cmd = [
        sys.executable,
        "-m",
        "streamlit",
        "run",
        str(dashboard_path),
        "--server.port",
        str(actual_port),
        "--server.headless",
        "false",
    ]

    # VIB-5012: capture the child's stdout/stderr in a strategy-local
    # ``dashboard.log`` instead of discarding it. When the dashboard hangs
    # or dies on mainnet we need evidence; DEVNULL gave us none.
    from almanak.framework.cli import run_helpers as _rh  # local import: avoids module-level cycle

    log_handle, log_path = _rh._open_dashboard_log()  # via facade: tests monkeypatch this attribute on run_helpers
    try:
        process = subprocess.Popen(
            cmd,
            env=env,
            stdout=log_handle if log_handle is not None else subprocess.DEVNULL,
            stderr=subprocess.STDOUT if log_handle is not None else subprocess.DEVNULL,
        )
    except Exception as e:
        if log_handle is not None:
            try:
                log_handle.close()
            except Exception:  # pragma: no cover - best-effort close
                pass
        click.echo(f"Error launching dashboard: {e}", err=True)
        return None

    if log_handle is not None:
        # Spawn banner from the parent so restarts are delimited in the
        # appended log and the PID/command are on record even if the child
        # never emits a byte before hanging.
        try:
            from datetime import UTC, datetime

            banner = (
                f"--- [{datetime.now(UTC).isoformat()}] "
                f"dashboard spawn pid={getattr(process, 'pid', None)} cmd={' '.join(cmd)}\n"
            )
            log_handle.write(banner)
            log_handle.flush()
        except Exception:  # pragma: no cover - observability must not break the run
            logger.warning("Failed to write dashboard.log spawn banner", exc_info=True)
        logger.info(
            "Dashboard subprocess started (pid=%s); stdout/stderr -> %s",
            getattr(process, "pid", None),
            log_path,
        )
        # Stash the handle on the Popen object so _stop_dashboard can close
        # it on the shutdown path without changing any call-site signature.
        process._almanak_dashboard_log_handle = log_handle  # type: ignore[attr-defined]
    else:
        logger.info(
            "Dashboard subprocess started (pid=%s); output discarded (dashboard.log unavailable)",
            getattr(process, "pid", None),
        )

    click.echo(f"Dashboard started at http://localhost:{actual_port}")
    return process


def _open_dashboard_log() -> tuple[Any, Path | None]:
    """Open the strategy-local ``dashboard.log`` for appending.

    Resolves the path through :func:`local_log_path` so the log lands next
    to the strategy's folder-scoped SQLite DB (the same folder that owns
    ``config.json`` for a strategy run). Append mode so restarts don't
    clobber prior evidence.

    Returns:
        ``(handle, path)`` on success, ``(None, None)`` when the path cannot
        be resolved or the file cannot be opened (hosted mode, read-only
        disk, ...). Failure degrades to the historical DEVNULL behavior with
        a warning — observability must never break the run.
    """
    try:
        from almanak.framework.local_paths import local_log_path

        log_path = local_log_path("dashboard")
        return open(log_path, "a", encoding="utf-8"), log_path
    except Exception as exc:
        logger.warning(
            "Could not open dashboard.log for the dashboard subprocess (%s); "
            "falling back to DEVNULL — dashboard output will be discarded.",
            exc,
        )
        return None, None


def _stop_dashboard(process: Any) -> None:
    """Terminate the background dashboard process (best-effort).

    Mirrors the nested ``stop_dashboard`` previously defined inside
    ``run()``. ``None`` process is a no-op. Terminates first, falls back
    to kill on any exception during terminate/wait.

    VIB-5012 observability: logs the child's exit/return code at INFO —
    including the case where the dashboard already died silently mid-run
    (``poll()`` non-None before we ever sent a signal) — and closes the
    ``dashboard.log`` handle stashed by ``_start_dashboard_background``.
    Idempotent: ``run()`` calls this explicitly AND registers it via
    ``atexit``, so the second invocation is a no-op (no duplicate logs,
    no double-close).

    Args:
        process: The ``subprocess.Popen`` handle returned by
            ``_start_dashboard_background`` (may be ``None``).
    """
    if process is None:
        return
    if getattr(process, "_almanak_dashboard_stopped", False):
        return
    try:
        process._almanak_dashboard_stopped = True
    except Exception:  # pragma: no cover - exotic process fakes
        pass

    try:
        pid = getattr(process, "pid", None)
        try:
            already_exited = process.poll() is not None
        except Exception:  # pragma: no cover - poll is best-effort observability
            already_exited = False
        if already_exited:
            # The child died silently mid-run — surface it instead of
            # pretending the terminate below did anything.
            logger.info(
                "Dashboard subprocess (pid=%s) had already exited with returncode=%s",
                pid,
                getattr(process, "returncode", None),
            )
        else:
            try:
                process.terminate()
                process.wait(timeout=5)
                logger.info(
                    "Dashboard subprocess (pid=%s) stopped with returncode=%s",
                    pid,
                    getattr(process, "returncode", None),
                )
            except Exception:
                try:
                    process.kill()
                except Exception:
                    pass
    finally:
        log_handle = getattr(process, "_almanak_dashboard_log_handle", None)
        if log_handle is not None:
            try:
                log_handle.close()
            except Exception:  # pragma: no cover - best-effort close
                pass


def _handle_standalone_dashboard(
    *,
    working_dir: str,
    dashboard: bool,
    dashboard_port: int,
    gateway_host: str,
    gateway_port: int,
    auth_token: str | None = None,
    dashboard_mode: str = "command-center",
) -> bool:
    """Handle the standalone dashboard early-exit branch.

    Mirrors the block in ``run()``::

        if dashboard and working_dir == ".":
            <launch banner + block on Ctrl+C>
            return

    Launches the dashboard as a background subprocess, prints the banner,
    and blocks on ``process.wait()`` until interrupted. On ``KeyboardInterrupt``
    tears the dashboard down and returns ``True``. When launch fails,
    exits with status 1 (preserving original semantics).

    Args:
        working_dir: CLI ``--working-dir`` (standalone path iff ``"."``).
        dashboard: CLI ``--dashboard`` flag.
        dashboard_port: CLI ``--dashboard-port`` flag.
        gateway_host: Effective gateway host (post-``_setup_gateway``).
        gateway_port: Effective gateway port (post-``_setup_gateway``).
        auth_token: Managed-gateway session token, forwarded to the
            dashboard subprocess so it authenticates against the same
            ephemeral token the managed gateway is enforcing.

    Returns:
        ``True`` if the branch handled the request (caller must ``return``),
        ``False`` otherwise.
    """
    if not (dashboard and working_dir == "."):
        return False

    # Standalone dashboard (no strategy directory) always opens Command
    # Center — hosted-parity scoping requires a deployment id/dir context
    # that doesn't exist here. If the operator explicitly passed
    # ``--dashboard-mode=hosted-parity``, surface a one-line warning so
    # they know their flag was overridden rather than silently ignored
    # (Claude pr-auditor Important #3 on PR #2372).
    if dashboard_mode.lower() == "hosted-parity":
        click.echo(
            "Warning: --dashboard-mode=hosted-parity ignored in standalone mode "
            "(no strategy context). Opening Command Center.",
            err=True,
        )

    click.echo()
    click.echo("=" * 60)
    click.echo("LAUNCHING DASHBOARD (standalone mode)")
    click.echo("=" * 60)
    click.echo("Press Ctrl+C to stop")
    from almanak.framework.cli import run_helpers as _rh  # local import: avoids module-level cycle

    proc = _rh._start_dashboard_background(  # via facade: tests monkeypatch this attribute on run_helpers
        port=dashboard_port,
        gateway_host=gateway_host,
        gateway_port=gateway_port,
        auth_token=auth_token,
    )
    if proc is None:
        sys.exit(1)
    try:
        proc.wait()
    except KeyboardInterrupt:
        _rh._stop_dashboard(proc)  # via facade: tests monkeypatch this attribute on run_helpers
        click.echo("Dashboard stopped.")
    return True
