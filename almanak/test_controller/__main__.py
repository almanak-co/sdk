"""Test controller — HTTP service that manages the lifecycle of a single
privileged ``managed_serve`` subprocess.

Designed as a sidecar container in a Cloud Run multi-container revision.
The controller is the only process that holds privileged env (Alchemy URLs,
Etherscan keys, etc.). It does NOT run the test ladder — that lives in the
worker container alongside the strategy workspace, driven by the MCP
``run_test`` tool. The controller's contract is narrower than that:

  ``POST /start_gateway`` → "give me a working gateway pointing at this
                            workspace's chain, with anvil_funding applied."
  ``POST /stop_gateway``  → "tear it down."

Why this split: tests modify workspace files (state DB, logs, snapshots).
The workspace lives in the worker container, so test execution has to live
there too. The controller only owns the privileged subprocess. The MCP
tool orchestrates the two sides — calls /start_gateway, runs the ladder
steps locally, finally calls /stop_gateway.

Endpoints
~~~~~~~~~
``GET /health``
    Liveness probe. Always 200.

``POST /start_gateway`` ``{"workspace_path": "..."}``
    Spawn a fresh ``managed_serve`` subprocess. The subprocess reads
    chain + ``anvil_funding`` from ``<workspace_path>/config.json`` via
    the SDK's existing ``_resolve_anvil_chains_and_funding`` helper.
    Returns ``{port}`` so the caller knows where to point
    ``--no-gateway --gateway-port=N``. Returns 409 if a gateway is
    already running.

``POST /stop_gateway``
    SIGTERM the current gateway subprocess, wait, SIGKILL if needed.
    Idempotent — a stop with nothing running is a 200 no-op so the MCP
    tool's ``finally`` block is safe.

``GET /status``
    Reports whether a gateway is currently running, its port, and how
    long it's been alive. Useful for debugging stuck states.

Safety net: if a caller crashes mid-test and never calls /stop_gateway,
the controller auto-stops the gateway after ``IDLE_TIMEOUT_SECONDS``.

Run with::

    python -m almanak.test_controller
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import signal
import socket
import sys
import tempfile
import time
from collections.abc import Awaitable, Callable
from pathlib import Path

import click
import uvicorn
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field

# === Configuration ===

CONTROLLER_HOST = os.environ.get("ALMANAK_TEST_CONTROLLER_HOST", "127.0.0.1")
CONTROLLER_PORT = int(os.environ.get("ALMANAK_TEST_CONTROLLER_PORT", "9100"))

# Gateway lifecycle budgets. We compute the startup deadline per-call using
# the same helper the gateway uses to size its own ``ManagedGateway.start``
# budget (``compute_anvil_startup_timeout`` in ``framework.cli._anvil_timeout``)
# so the controller never kills a still-initializing gateway whose own budget
# hasn't elapsed yet. A small safety margin covers Python startup + the
# managed_serve gRPC bind that runs after ``ManagedGateway.start`` returns.
# Shutdown is ~1s typically; allow 10s before SIGKILL.
GATEWAY_STARTUP_SAFETY_MARGIN_SECONDS = 30.0
GATEWAY_SHUTDOWN_TIMEOUT_SECONDS = 10.0

# Safety net: if no /stop_gateway arrives within this long after the gateway
# was started, the controller tears it down automatically. Bound the worst
# case "MCP tool crashed mid-test and never told us to stop" → leaked Anvil
# subprocess chewing memory until the Cloud Run instance is recycled.
IDLE_TIMEOUT_SECONDS = 1800  # 30 min

# Default test wallet (Anvil account #0). Public test key — fine to embed
# rather than env so the controller works with zero extra config in dev.
ANVIL_DEFAULT_PRIVATE_KEY = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"

logger = logging.getLogger("almanak.test_controller")


# === Request / response models ===


class StartGatewayRequest(BaseModel):
    workspace_path: str = Field(
        description="Absolute path to the strategy workspace. Must contain "
        "strategy.py and config.json — the gateway reads chain + "
        "anvil_funding from the latter."
    )


class StartGatewayResponse(BaseModel):
    port: int = Field(
        description="Loopback port the gateway gRPC server is reachable on. "
        "Pass to ``--no-gateway --gateway-port=<this>``."
    )
    started_at_unix: float = Field(description="Wall-clock time the gateway became ready.")


class StopGatewayResponse(BaseModel):
    stopped: bool = Field(
        description="True if a gateway was running and got stopped; False if no gateway was running (idempotent no-op)."
    )
    shutdown_ms: float | None = Field(default=None, description="How long the shutdown took, in ms.")


class StatusResponse(BaseModel):
    running: bool
    port: int | None = None
    workspace_path: str | None = None
    started_at_unix: float | None = None
    age_seconds: float | None = None


# === Startup-failure cause surfacing (ALM-3274 / ALM-3264 / ALM-3266) ===

# Known-safe startup-failure causes the controller is allowed to forward to
# its (unprivileged) caller. Everything else stays on the redacted generic
# 500 — the allowlist is the second gate after the child's own redaction
# (``managed_serve._record_startup_error``), so a novel exception whose text
# embeds an RPC URL never reaches the worker container. Each pattern captures
# a bounded, module-generated message:
#   1. Managed-Anvil funding refusal (managed.py _fund_anvil_wallets) — the
#      cause behind the 2026-08-12 "Prod: gateway cannot start" cluster.
#   2. anvil_funding config validation (managed.py _parse_anvil_funding_for_chain),
#      e.g. the zero-address-as-native-key mistake.
#   3. The archive-RPC gate (managed.py _check_archive_rpc_availability) —
#      multi-line but fully module-generated (public endpoints + env var
#      NAMES only), and its "Fix —" section is the actionable part.
_STARTUP_CAUSE_SENTINELS: tuple[re.Pattern[str], ...] = (
    re.compile(r"Managed Anvil funding could not provision[^\n]*"),
    re.compile(r"anvil_funding key [^\n]*"),
    re.compile(r"Refusing to start Anvil fork\(s\) against state-pruned public RPC\(s\):.*", re.DOTALL),
)

# Bound on the cause text forwarded in the HTTP error detail.
_STARTUP_CAUSE_MAX_CHARS = 1000


def _read_startup_error_detail(path: str) -> str | None:
    """Read the child's startup-failure summary; None on absent/malformed."""
    try:
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
    except (OSError, ValueError):
        return None
    message = data.get("message") if isinstance(data, dict) else None
    return message if isinstance(message, str) and message.strip() else None


def _classify_startup_cause(summary: str | None) -> str | None:
    """Extract an allowlisted, caller-safe cause from the child's summary.

    Returns the matched (bounded) cause text, or None when no known-safe
    sentinel matches — the caller then keeps today's opaque-500 behaviour.
    """
    if not summary:
        return None
    for sentinel in _STARTUP_CAUSE_SENTINELS:
        match = sentinel.search(summary)
        if match:
            return match.group(0)[:_STARTUP_CAUSE_MAX_CHARS]
    return None


def _unlink_quiet(path: str) -> None:
    try:
        os.unlink(path)
    except OSError:
        pass


class _GatewayStartupError(RuntimeError):
    """Early managed_serve exit with a cause classified safe to surface.

    Only raised when ``_classify_startup_cause`` matched an allowlisted
    sentinel; ``cause`` is that matched text. Unclassified early exits keep
    raising plain ``RuntimeError`` so the endpoint's redacted-500 invariant
    (and its secret-leak test) is untouched.
    """

    def __init__(self, message: str, cause: str) -> None:
        super().__init__(message)
        self.cause = cause


# === Gateway lifecycle handle ===


class _Gateway:
    """Wrapper around one managed_serve subprocess + its bookkeeping."""

    def __init__(self, proc: asyncio.subprocess.Process, port: int, workspace: Path) -> None:
        self.proc = proc
        self.port = port
        self.workspace = workspace
        self.started_at = time.time()
        self._idle_task: asyncio.Task | None = None

    def schedule_idle_timeout(self, on_timeout: Callable[[], Awaitable[None]]) -> None:
        """Start a background task that calls ``on_timeout`` after IDLE_TIMEOUT_SECONDS.

        The timeout is the controller's last-resort cleanup for callers that
        crash without sending /stop_gateway. Cancelled by ``cancel_idle_timeout``
        when /stop_gateway arrives normally.
        """

        async def _watchdog() -> None:
            try:
                await asyncio.sleep(IDLE_TIMEOUT_SECONDS)
                logger.warning(
                    "gateway idle timeout (%.0fs) reached without /stop_gateway — auto-stopping PID=%d",
                    IDLE_TIMEOUT_SECONDS,
                    self.proc.pid,
                )
                await on_timeout()
            except asyncio.CancelledError:
                pass

        self._idle_task = asyncio.create_task(_watchdog())

    def cancel_idle_timeout(self) -> None:
        if self._idle_task is not None and not self._idle_task.done():
            self._idle_task.cancel()
        self._idle_task = None

    async def stop(self) -> None:
        """SIGTERM → wait → SIGKILL on timeout. Idempotent if already exited."""
        if self.proc.returncode is not None:
            return
        try:
            self.proc.send_signal(signal.SIGTERM)
            try:
                await asyncio.wait_for(self.proc.wait(), timeout=GATEWAY_SHUTDOWN_TIMEOUT_SECONDS)
                return
            except TimeoutError:
                logger.warning(
                    "managed_serve PID=%d did not exit on SIGTERM within %.0fs; sending SIGKILL",
                    self.proc.pid,
                    GATEWAY_SHUTDOWN_TIMEOUT_SECONDS,
                )
            self.proc.kill()
            await self.proc.wait()
        except ProcessLookupError:
            pass


# === Module-level state ===

# Single gateway slot. The controller serves one gateway at a time —
# multi-container concurrency=1 means there's never reason for more. A 409
# on /start_gateway if something's already running surfaces the race.
_current: _Gateway | None = None
_lifecycle_lock = asyncio.Lock()


# === Utilities ===


def _find_free_port() -> int:
    """Pick an ephemeral port. Inherent TOCTOU race with the gateway bind;
    accepted given the ephemeral port range is large.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _build_gateway_env(workspace: Path, port: int, startup_error_file: str | None = None) -> dict[str, str]:
    """Subprocess env for managed_serve: inherit ours + per-call overrides."""
    env = {
        **os.environ,
        "ALMANAK_GATEWAY_NETWORK": "anvil",
        "ALMANAK_GATEWAY_GRPC_PORT": str(port),
        "ALMANAK_STRATEGY_FOLDER": str(workspace),
        "ALMANAK_PRIVATE_KEY": ANVIL_DEFAULT_PRIVATE_KEY,
        # Controller-managed gateways are loopback-only by design — the
        # controller spawned them and the worker reaches them via the shared
        # network namespace. An auth_token between worker and gateway adds no
        # security in this topology (anything that can reach :9100 already
        # shares the netns). Pre-set allow_insecure so callers don't have to
        # provision a per-deploy ALMANAK_GATEWAY_AUTH_TOKEN to make the
        # sidecar boot.
        "ALMANAK_GATEWAY_ALLOW_INSECURE": "true",
    }
    if startup_error_file is not None:
        # Typed as GatewaySettings.startup_error_file in the child — the
        # one-shot handoff path for a redacted startup-failure summary.
        env["ALMANAK_GATEWAY_STARTUP_ERROR_FILE"] = startup_error_file
    return env


async def _wait_for_port(proc: asyncio.subprocess.Process, port: int, deadline: float) -> bool:
    """Poll the loopback ``port`` until it accepts connections or the deadline passes.

    Returns True on success. Returns False on either deadline-elapsed or the
    subprocess exiting early — caller decides how to respond.
    """
    while time.monotonic() < deadline:
        if proc.returncode is not None:
            return False
        try:
            _, writer = await asyncio.wait_for(asyncio.open_connection("127.0.0.1", port), timeout=1.0)
            writer.close()
            await writer.wait_closed()
            return True
        except (TimeoutError, ConnectionRefusedError, OSError):
            await asyncio.sleep(0.3)
    return False


def _compute_startup_budget(workspace: Path) -> float:
    """Mirror the gateway's own startup-budget calculation so we never kill a
    managed_serve subprocess whose ``ManagedGateway.start(timeout=…)`` is still
    within its budget. Uses the same helper managed_serve imports, sourced from
    ``ManagedGateway.COLD_START_SLOW_CHAINS`` so policy stays in one place.

    Falls back to a 60s minimum if chain resolution returns empty (mainnet
    network, malformed config) — covers the gRPC-bind tail without forks.
    """
    from almanak.framework.cli._anvil_timeout import compute_anvil_startup_timeout
    from almanak.framework.cli.run_helpers import _resolve_anvil_chains_and_funding

    anvil_chains, _funding = _resolve_anvil_chains_and_funding(
        working_dir=str(workspace),
        config_file=None,
        early_strategy_class=None,
        external_anvil_ports={},
    )
    base = compute_anvil_startup_timeout(anvil_chains)
    return base + GATEWAY_STARTUP_SAFETY_MARGIN_SECONDS


async def _spawn_gateway(workspace: Path, port: int) -> _Gateway:
    """Launch managed_serve and wait for the gRPC port to accept connections."""
    startup_budget = _compute_startup_budget(workspace)

    # One-shot handoff file for a redacted startup-failure summary (ALM-3274).
    # Deliberately NOT a pipe: managed_serve keeps inheriting stdout/stderr so
    # its full logs still reach Cloud Logging, and a pipe would deadlock a
    # healthy long-lived gateway once the buffer fills.
    err_fd, err_path = tempfile.mkstemp(prefix="gw_startup_err_", suffix=".json")
    os.close(err_fd)

    # The outer try/finally owns the handoff file from the moment it exists,
    # so a subprocess-creation failure (or cancellation) cannot leak it.
    try:
        proc = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "almanak.gateway.managed_serve",
            env=_build_gateway_env(workspace, port, startup_error_file=err_path),
            stdout=None,
            stderr=None,
        )
        logger.info("managed_serve subprocess: pid=%d, startup_budget=%.0fs", proc.pid, startup_budget)

        # Catch BaseException (not just Exception) so an HTTP-request cancellation
        # — which delivers asyncio.CancelledError mid-await — still kills the
        # subprocess instead of leaving it orphaned. We re-raise after cleanup so
        # the caller's exception semantics are preserved.
        try:
            deadline = time.monotonic() + startup_budget
            if await _wait_for_port(proc, port, deadline):
                return _Gateway(proc=proc, port=port, workspace=workspace)

            if proc.returncode is None:
                proc.kill()
                await proc.wait()
                raise TimeoutError(
                    f"managed_serve did not become reachable on port {port} within {startup_budget:.0f}s"
                )
            cause = _classify_startup_cause(_read_startup_error_detail(err_path))
            if cause is not None:
                raise _GatewayStartupError(
                    f"managed_serve exited early with code {proc.returncode}: {cause}",
                    cause=cause,
                )
            raise RuntimeError(f"managed_serve exited early with code {proc.returncode}")
        except BaseException:
            if proc.returncode is None:
                try:
                    proc.kill()
                    await proc.wait()
                except BaseException:
                    # Swallow secondary errors during cleanup — the original
                    # exception is what we want to surface.
                    pass
            raise
    finally:
        # The summary is only ever written during a failed startup, so the
        # file is dead weight the moment we return OR raise.
        _unlink_quiet(err_path)


# === FastAPI app ===


app = FastAPI(title="Almanak Test Controller", version="1.0.0")


@app.get("/health")
async def health() -> dict[str, bool]:
    return {"ok": True}


def _reap_stale_current() -> _Gateway | None:
    """Return ``_current`` if its subprocess is still alive; otherwise clear it.

    Without this, a silently-crashed ``managed_serve`` (OOM, segfault, etc.)
    leaves the controller wedged: ``/status`` reports ``running=true`` and
    ``/start_gateway`` returns 409 until the idle watchdog (30 min) finally
    tears it down. Checking ``returncode`` on every read shrinks that window
    to the next caller's request.
    """
    global _current
    gw = _current
    if gw is not None and gw.proc.returncode is not None:
        logger.warning(
            "managed_serve PID=%d exited unexpectedly (code=%s); clearing state",
            gw.proc.pid,
            gw.proc.returncode,
        )
        gw.cancel_idle_timeout()
        _current = None
        return None
    return gw


@app.get("/status", response_model=StatusResponse)
async def get_status() -> StatusResponse:
    gw = _reap_stale_current()
    if gw is None:
        return StatusResponse(running=False)
    return StatusResponse(
        running=True,
        port=gw.port,
        workspace_path=str(gw.workspace),
        started_at_unix=gw.started_at,
        age_seconds=time.time() - gw.started_at,
    )


def _validate_workspace(raw: str) -> Path:
    """Resolve + sanity-check the workspace path. Raises HTTP 400 on failure."""
    workspace = Path(raw).resolve()
    for required in ("strategy.py", "config.json"):
        if not (workspace / required).exists():
            raise HTTPException(400, f"workspace missing {required}: {workspace}")
    if not workspace.exists():
        raise HTTPException(400, f"workspace_path does not exist: {workspace}")
    return workspace


@app.post("/start_gateway", response_model=StartGatewayResponse)
async def start_gateway(req: StartGatewayRequest) -> StartGatewayResponse:
    global _current

    workspace = _validate_workspace(req.workspace_path)

    # Validate config.json up front, scoped to this one parse call. A schema /
    # parse failure here is user-actionable and structurally secret-free (the
    # message is just the config path + the pydantic/JSON error), so we surface
    # it as a 400 instead of letting it fall into the generic "gateway startup
    # failed" 500 below — which previously masked fixable config bugs as opaque
    # infra failures. We deliberately do NOT widen the catch to all of
    # ``_spawn_gateway``: gateway-startup ClickExceptions can carry host/RPC
    # detail, so those must stay on the redacted 500 path. ``_spawn_gateway``
    # re-parses via the same shared loader, so a config that passes here won't
    # re-raise there for config reasons.
    from almanak.framework.cli.run import parse_strategy_config_file

    try:
        parse_strategy_config_file(workspace / "config.json", warn_unknown_keys=False)
    except click.ClickException as e:
        logger.warning("gateway startup rejected: invalid strategy config: %s", e)
        raise HTTPException(400, f"invalid strategy config: {e}") from e

    async with _lifecycle_lock:
        live = _reap_stale_current()
        if live is not None:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"gateway already running on port {live.port} "
                f"(workspace={live.workspace}). "
                f"call /stop_gateway first.",
            )

        port = _find_free_port()
        try:
            gw = await _spawn_gateway(workspace, port)
        except _GatewayStartupError as e:
            # The child reported a cause that matched the caller-safe
            # allowlist (funding refusal, anvil_funding validation, archive
            # gate) — all user-actionable workspace/config problems. Surface
            # it as a 422 so the test ladder stops misreporting fixable
            # config gaps as opaque infra failures (ALM-3274/3264/3266).
            logger.exception("gateway startup failed (classified cause surfaced to caller)")
            raise HTTPException(422, f"gateway startup failed: {e.cause}") from e
        except Exception as e:
            # Privileged service — full error stays in the log; client gets
            # a generic message so internal paths/RPC URLs don't leak.
            logger.exception("gateway startup failed")
            raise HTTPException(500, "gateway startup failed") from e

        gw.schedule_idle_timeout(_auto_stop_on_idle)
        _current = gw
        logger.info("gateway started: port=%d workspace=%s pid=%d", gw.port, gw.workspace, gw.proc.pid)
        return StartGatewayResponse(port=gw.port, started_at_unix=gw.started_at)


@app.post("/stop_gateway", response_model=StopGatewayResponse)
async def stop_gateway() -> StopGatewayResponse:
    global _current

    async with _lifecycle_lock:
        gw = _current
        if gw is None:
            # No-op so the caller's finally-block is safe.
            return StopGatewayResponse(stopped=False)

        gw.cancel_idle_timeout()
        t0 = time.monotonic()
        try:
            await gw.stop()
        finally:
            _current = None
        shutdown_ms = (time.monotonic() - t0) * 1000
        logger.info("gateway stopped: shutdown_ms=%.1f", shutdown_ms)
        return StopGatewayResponse(stopped=True, shutdown_ms=shutdown_ms)


async def _auto_stop_on_idle() -> None:
    """Background-task callback for the per-gateway idle watchdog."""
    global _current
    async with _lifecycle_lock:
        gw = _current
        if gw is None:
            return
        try:
            await gw.stop()
        finally:
            _current = None
        logger.warning("auto-stopped gateway after idle timeout; caller never sent /stop_gateway")


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logger.info("Starting almanak test-controller on %s:%d", CONTROLLER_HOST, CONTROLLER_PORT)
    uvicorn.run(app, host=CONTROLLER_HOST, port=CONTROLLER_PORT, access_log=False, log_level="info")


if __name__ == "__main__":
    main()
