"""Tests for the ``test_controller`` HTTP endpoints.

Reviewer-flagged in PR #2351: the new stateful module
(``/start_gateway``, ``/stop_gateway``, ``/status``, ``/health``, idle
watchdog, subprocess startup/error branches) needs direct coverage. We
use FastAPI's TestClient and stub ``_spawn_gateway`` so no real Anvil /
gRPC server is involved.
"""

from __future__ import annotations

import asyncio
import json
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def workspace(tmp_path: Path) -> Path:
    """Minimal strategy workspace: just the two files the controller validates."""
    (tmp_path / "strategy.py").write_text("# placeholder\n")
    (tmp_path / "config.json").write_text(json.dumps({"chain": "base", "anvil_funding": {"ETH": "10"}}))
    return tmp_path


def _make_fake_gateway(port: int = 9999, returncode: int | None = None) -> MagicMock:
    """Build a stand-in for the ``_Gateway`` instance returned by ``_spawn_gateway``."""
    gw = MagicMock()
    gw.port = port
    gw.workspace = Path("/tmp/fake-workspace")
    gw.started_at = 12345.0
    gw.proc = MagicMock()
    gw.proc.pid = 4242
    gw.proc.returncode = returncode
    gw.cancel_idle_timeout = MagicMock()
    gw.schedule_idle_timeout = MagicMock()
    gw.stop = AsyncMock()
    return gw


@pytest.fixture(autouse=True)
def _reset_module_state():
    """Each test starts with no gateway registered.

    ``_current`` is module-level mutable state — without this reset, ordering
    between tests determines whether the second one sees the first one's
    fake gateway and returns the wrong status.
    """
    from almanak.test_controller import __main__ as ctrl

    ctrl._current = None
    yield
    ctrl._current = None


@pytest.fixture
def client():
    from almanak.test_controller import __main__ as ctrl

    return TestClient(ctrl.app)


# ─── /health ─────────────────────────────────────────────────────────────


def test_health_returns_ok(client: TestClient) -> None:
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"ok": True}


# ─── /status ─────────────────────────────────────────────────────────────


def test_status_when_idle(client: TestClient) -> None:
    resp = client.get("/status")
    assert resp.status_code == 200
    body = resp.json()
    assert body["running"] is False
    assert body["port"] is None


def test_status_when_running(client: TestClient) -> None:
    from almanak.test_controller import __main__ as ctrl

    ctrl._current = _make_fake_gateway(port=58000)
    resp = client.get("/status")
    body = resp.json()
    assert body["running"] is True
    assert body["port"] == 58000
    assert body["age_seconds"] is not None


def test_status_reaps_stale_dead_subprocess(client: TestClient) -> None:
    """If the subprocess exited silently, /status must clear state and report running=False."""
    from almanak.test_controller import __main__ as ctrl

    ctrl._current = _make_fake_gateway(returncode=137)  # SIGKILL'd, e.g. OOM
    resp = client.get("/status")
    body = resp.json()
    assert body["running"] is False
    assert ctrl._current is None, "stale _current must be cleared by the reap helper"


# ─── /start_gateway ──────────────────────────────────────────────────────


def test_start_gateway_happy_path(client: TestClient, workspace: Path) -> None:
    fake_gw = _make_fake_gateway(port=58111)
    with patch(
        "almanak.test_controller.__main__._spawn_gateway",
        new=AsyncMock(return_value=fake_gw),
    ):
        resp = client.post("/start_gateway", json={"workspace_path": str(workspace)})

    assert resp.status_code == 200
    body = resp.json()
    assert body["port"] == 58111
    assert body["started_at_unix"] == 12345.0
    fake_gw.schedule_idle_timeout.assert_called_once()


def test_start_gateway_missing_strategy_py_returns_400(client: TestClient, tmp_path: Path) -> None:
    (tmp_path / "config.json").write_text("{}")
    resp = client.post("/start_gateway", json={"workspace_path": str(tmp_path)})
    assert resp.status_code == 400
    assert "strategy.py" in resp.json()["detail"]


def test_start_gateway_missing_config_json_returns_400(client: TestClient, tmp_path: Path) -> None:
    (tmp_path / "strategy.py").write_text("# stub\n")
    resp = client.post("/start_gateway", json={"workspace_path": str(tmp_path)})
    assert resp.status_code == 400
    assert "config.json" in resp.json()["detail"]


def test_start_gateway_returns_409_when_already_running(client: TestClient, workspace: Path) -> None:
    """Two consecutive /start_gateway calls — second must be rejected with 409."""
    from almanak.test_controller import __main__ as ctrl

    ctrl._current = _make_fake_gateway(port=58000)
    resp = client.post("/start_gateway", json={"workspace_path": str(workspace)})
    assert resp.status_code == 409
    assert "already running" in resp.json()["detail"]


def test_start_gateway_returns_500_with_sanitized_message_on_spawn_failure(
    client: TestClient, workspace: Path
) -> None:
    """Internal exception text must NOT leak to the client (reviewer-flagged)."""
    with patch(
        "almanak.test_controller.__main__._spawn_gateway",
        new=AsyncMock(side_effect=RuntimeError("alchemy URL https://...secret-token leaked here")),
    ):
        resp = client.post("/start_gateway", json={"workspace_path": str(workspace)})

    assert resp.status_code == 500
    detail = resp.json()["detail"]
    # Generic message only — no Alchemy URL, no exception text.
    assert detail == "gateway startup failed"
    assert "alchemy" not in detail.lower()
    assert "secret" not in detail.lower()


def test_start_gateway_returns_400_with_real_message_on_invalid_config(
    client: TestClient, tmp_path: Path
) -> None:
    """A config.json that fails StrategyConfig schema validation must surface the
    actual error as a 400 — NOT get masked as the opaque 500 "gateway startup
    failed", which made the test ladder misreport fixable config bugs as
    terminal infra failures. ``pool`` is typed ``str``; a nested object trips
    pydantic. ``_spawn_gateway`` is mocked to assert it is NEVER reached — the
    400 must come from the pre-spawn config validation, and the unit test must
    not start a real Anvil/gateway even if ``StrategyConfig`` later loosens."""
    (tmp_path / "strategy.py").write_text("# stub\n")
    (tmp_path / "config.json").write_text(json.dumps({"chain": "base", "pool": {"fee_tier_bps": 30}}))

    with patch(
        "almanak.test_controller.__main__._spawn_gateway",
        new=AsyncMock(side_effect=AssertionError("_spawn_gateway must not run for invalid config")),
    ):
        resp = client.post("/start_gateway", json={"workspace_path": str(tmp_path)})

    assert resp.status_code == 400
    detail = resp.json()["detail"]
    assert "invalid strategy config" in detail
    assert "schema validation" in detail
    assert detail != "gateway startup failed"


def test_start_gateway_after_stale_subprocess_clears_state_and_succeeds(
    client: TestClient, workspace: Path
) -> None:
    """If the previous gateway crashed silently, /start_gateway should reap and proceed."""
    from almanak.test_controller import __main__ as ctrl

    ctrl._current = _make_fake_gateway(returncode=139)  # SIGSEGV

    fresh = _make_fake_gateway(port=58222)
    with patch(
        "almanak.test_controller.__main__._spawn_gateway",
        new=AsyncMock(return_value=fresh),
    ):
        resp = client.post("/start_gateway", json={"workspace_path": str(workspace)})

    assert resp.status_code == 200
    assert resp.json()["port"] == 58222


# ─── /stop_gateway ───────────────────────────────────────────────────────


def test_stop_gateway_idempotent_when_nothing_running(client: TestClient) -> None:
    """/stop_gateway must be a 200 no-op so callers' finally blocks are safe."""
    resp = client.post("/stop_gateway")
    assert resp.status_code == 200
    body = resp.json()
    assert body["stopped"] is False
    assert body["shutdown_ms"] is None


def test_stop_gateway_stops_running_gateway(client: TestClient) -> None:
    from almanak.test_controller import __main__ as ctrl

    fake_gw = _make_fake_gateway(port=58333)
    ctrl._current = fake_gw

    resp = client.post("/stop_gateway")
    assert resp.status_code == 200
    body = resp.json()
    assert body["stopped"] is True
    assert body["shutdown_ms"] is not None
    fake_gw.cancel_idle_timeout.assert_called_once()
    fake_gw.stop.assert_awaited_once()
    assert ctrl._current is None


def test_stop_gateway_clears_state_even_if_stop_raises(client: TestClient) -> None:
    """``finally`` block must zero ``_current`` so a stuck gateway doesn't block restarts."""
    from almanak.test_controller import __main__ as ctrl

    fake_gw = _make_fake_gateway(port=58444)
    fake_gw.stop = AsyncMock(side_effect=RuntimeError("anvil hung on SIGTERM"))
    ctrl._current = fake_gw

    with pytest.raises(RuntimeError, match="anvil hung"):
        client.post("/stop_gateway")

    assert ctrl._current is None, "_current must be cleared in the finally block"


# ─── _validate_workspace direct ──────────────────────────────────────────


def test_validate_workspace_resolves_relative(tmp_path: Path) -> None:
    """Smoke check that the validator resolves and checks for both files."""
    from almanak.test_controller.__main__ import _validate_workspace

    (tmp_path / "strategy.py").write_text("")
    (tmp_path / "config.json").write_text("{}")

    result = _validate_workspace(str(tmp_path))
    assert result == tmp_path.resolve()


# ─── _reap_stale_current direct ──────────────────────────────────────────


def test_reap_clears_when_subprocess_exited() -> None:
    from almanak.test_controller import __main__ as ctrl

    ctrl._current = _make_fake_gateway(returncode=0)
    result = ctrl._reap_stale_current()
    assert result is None
    assert ctrl._current is None
    ctrl._current.__dict__ if ctrl._current else None  # noqa: B018  — just dead-code linter shut-up


def test_reap_returns_gateway_when_subprocess_alive() -> None:
    from almanak.test_controller import __main__ as ctrl

    gw = _make_fake_gateway(returncode=None)
    ctrl._current = gw
    result = ctrl._reap_stale_current()
    assert result is gw
    assert ctrl._current is gw


def test_reap_when_idle_returns_none() -> None:
    from almanak.test_controller import __main__ as ctrl

    ctrl._current = None
    assert ctrl._reap_stale_current() is None


# ─── _spawn_gateway cancellation cleanup ─────────────────────────────────


@pytest.mark.asyncio
async def test_spawn_gateway_kills_subprocess_on_cancellation() -> None:
    """If the HTTP request is cancelled mid-spawn, the subprocess must not leak.

    ``asyncio.CancelledError`` is a ``BaseException`` (not ``Exception``), so the
    outer ``except Exception`` in ``start_gateway`` does NOT catch it — meaning
    cleanup has to happen inside ``_spawn_gateway`` itself.
    """
    from almanak.test_controller import __main__ as ctrl

    fake_proc = MagicMock()
    fake_proc.pid = 9999
    fake_proc.returncode = None
    fake_proc.kill = MagicMock()
    fake_proc.wait = AsyncMock(return_value=0)

    async def cancel_during_wait(*_args, **_kwargs):
        raise asyncio.CancelledError("client disconnected")

    with patch(
        "almanak.test_controller.__main__.asyncio.create_subprocess_exec",
        new=AsyncMock(return_value=fake_proc),
    ), patch("almanak.test_controller.__main__._wait_for_port", new=cancel_during_wait):
        with pytest.raises(asyncio.CancelledError):
            await ctrl._spawn_gateway(Path("/tmp/some-workspace"), 58000)

    fake_proc.kill.assert_called_once()
    fake_proc.wait.assert_awaited()


# ─── _Gateway.schedule_idle_timeout direct ───────────────────────────────


@pytest.mark.asyncio
async def test_schedule_idle_timeout_callable_runs_on_expiry(monkeypatch) -> None:
    """The idle watchdog must call ``on_timeout`` when the sleep completes."""
    from almanak.test_controller import __main__ as ctrl

    proc = MagicMock()
    proc.pid = 1234
    proc.returncode = None
    gw = ctrl._Gateway(proc=proc, port=58000, workspace=Path("/tmp/x"))

    # Make IDLE_TIMEOUT_SECONDS effectively zero so the watchdog fires immediately.
    monkeypatch.setattr(ctrl, "IDLE_TIMEOUT_SECONDS", 0.01)

    callback_ran = asyncio.Event()

    async def on_timeout() -> None:
        callback_ran.set()

    gw.schedule_idle_timeout(on_timeout)
    await asyncio.wait_for(callback_ran.wait(), timeout=1.0)


@pytest.mark.asyncio
async def test_cancel_idle_timeout_aborts_callback() -> None:
    """Cancelling the watchdog before it fires must prevent ``on_timeout`` running."""
    from almanak.test_controller import __main__ as ctrl

    proc = MagicMock()
    proc.pid = 1234
    proc.returncode = None
    gw = ctrl._Gateway(proc=proc, port=58000, workspace=Path("/tmp/x"))

    callback_ran = False

    async def on_timeout() -> None:
        nonlocal callback_ran
        callback_ran = True

    gw.schedule_idle_timeout(on_timeout)
    gw.cancel_idle_timeout()
    # Give any scheduled task a chance to (incorrectly) fire.
    await asyncio.sleep(0.05)
    assert not callback_ran


def test_build_gateway_env_preconfigures_managed_serve(tmp_path: Path) -> None:
    """``_build_gateway_env`` must pre-set every env var the controller-managed
    gateway needs to boot without per-deploy provisioning. Drift here surfaces
    as cryptic ``managed_serve exited early`` errors at /start_gateway time.
    """
    from almanak.test_controller import __main__ as ctrl

    env = ctrl._build_gateway_env(tmp_path, 12345)

    # Per-call overrides
    assert env["ALMANAK_GATEWAY_NETWORK"] == "anvil"
    assert env["ALMANAK_GATEWAY_GRPC_PORT"] == "12345"
    assert env["ALMANAK_STRATEGY_FOLDER"] == str(tmp_path)
    assert env["ALMANAK_PRIVATE_KEY"] == ctrl.ANVIL_DEFAULT_PRIVATE_KEY
    # Loopback-only sidecar shape — auth_token adds no security, allow_insecure
    # lets the gateway boot without a per-deploy ALMANAK_GATEWAY_AUTH_TOKEN.
    assert env["ALMANAK_GATEWAY_ALLOW_INSECURE"] == "true"


# silence the unrelated incubating-strategy import collected as a warning
@pytest.fixture(autouse=True)
def _suppress_unrelated_import(caplog):
    yield


_ = tempfile  # imported for future signature-compat
