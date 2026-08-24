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
    ctrl._data_gateway_proc = None
    ctrl._data_gateway_ready = False
    ctrl._data_gateway_task = None
    yield
    ctrl._current = None
    ctrl._data_gateway_proc = None
    ctrl._data_gateway_ready = False
    ctrl._data_gateway_task = None


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


def test_start_gateway_returns_500_with_sanitized_message_on_spawn_failure(client: TestClient, workspace: Path) -> None:
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


def test_start_gateway_returns_400_with_real_message_on_invalid_config(client: TestClient, tmp_path: Path) -> None:
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


def test_start_gateway_after_stale_subprocess_clears_state_and_succeeds(client: TestClient, workspace: Path) -> None:
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

    with (
        patch(
            "almanak.test_controller.__main__.asyncio.create_subprocess_exec",
            new=AsyncMock(return_value=fake_proc),
        ),
        patch("almanak.test_controller.__main__._wait_for_port", new=cancel_during_wait),
    ):
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


# ─── startup-cause surfacing (ALM-3274 / ALM-3264 / ALM-3266) ─────────────

# Verbatim shape of the prod 2026-08-12 cluster's underlying cause: the
# managed-Anvil funding refusal that the controller used to collapse into an
# opaque "HTTP 500: gateway startup failed".
_FUNDING_CAUSE = (
    "Managed Anvil funding could not provision every requested asset; "
    "refusing to start an under-funded fork. Failures: bsc: could not "
    "provision ERC-20 addresses ['0xab78b89b5bb00236be0b4b20704cbfa04efc711c']"
)


def test_start_gateway_surfaces_classified_cause_as_422(client: TestClient, workspace: Path) -> None:
    """A classified (allowlisted) startup cause must reach the caller as a 422
    with the actual reason — not the opaque 500 that made every funding gap
    look like a platform outage."""
    from almanak.test_controller import __main__ as ctrl

    err = ctrl._GatewayStartupError(
        f"managed_serve exited early with code 1: {_FUNDING_CAUSE}",
        cause=_FUNDING_CAUSE,
    )
    with patch(
        "almanak.test_controller.__main__._spawn_gateway",
        new=AsyncMock(side_effect=err),
    ):
        resp = client.post("/start_gateway", json={"workspace_path": str(workspace)})

    assert resp.status_code == 422
    detail = resp.json()["detail"]
    assert detail.startswith("gateway startup failed: ")
    assert "could not provision ERC-20 addresses" in detail
    assert "0xab78b89b" in detail


def test_classify_startup_cause_matches_funding_refusal() -> None:
    from almanak.test_controller import __main__ as ctrl

    summary = f"Managed gateway failed to start: {_FUNDING_CAUSE}"
    cause = ctrl._classify_startup_cause(summary)
    assert cause is not None
    assert cause.startswith("Managed Anvil funding could not provision")
    assert "0xab78b89b" in cause


def test_classify_startup_cause_matches_anvil_funding_key_validation() -> None:
    """The zero-address-as-native mistake (ALM-3269/3270) is rejected at parse
    time with an ``anvil_funding key …`` message — it must classify."""
    from almanak.test_controller import __main__ as ctrl

    summary = (
        "Managed gateway failed to start: anvil_funding key "
        "'0x0000000000000000000000000000000000000000' is the zero address, which is not an "
        "ERC-20 contract. To fund the chain's native gas asset, use "
        "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE as the key instead."
    )
    cause = ctrl._classify_startup_cause(summary)
    assert cause is not None
    assert cause.startswith("anvil_funding key ")
    assert "zero address" in cause


def test_classify_startup_cause_matches_archive_gate() -> None:
    from almanak.test_controller import __main__ as ctrl

    summary = (
        "Managed gateway failed to start: Refusing to start Anvil fork(s) against "
        "state-pruned public RPC(s):\n  - polygon: https://polygon-bor-rpc.publicnode.com "
        "serves ~134s of state\n\nFix — configure an archive-capable RPC"
    )
    cause = ctrl._classify_startup_cause(summary)
    assert cause is not None
    assert cause.startswith("Refusing to start Anvil fork(s)")
    assert "archive-capable RPC" in cause


def test_classify_startup_cause_rejects_unallowlisted_text() -> None:
    """Novel exception text — which could embed an RPC URL — must NOT classify;
    the endpoint then keeps today's redacted generic 500."""
    from almanak.test_controller import __main__ as ctrl

    assert ctrl._classify_startup_cause("RuntimeError: https://eth-mainnet.g.alchemy.com/v2/tok_secret") is None
    assert ctrl._classify_startup_cause("") is None
    assert ctrl._classify_startup_cause(None) is None


def test_classify_startup_cause_bounds_length() -> None:
    from almanak.test_controller import __main__ as ctrl

    summary = "Managed Anvil funding could not provision " + "x" * 5000
    cause = ctrl._classify_startup_cause(summary)
    assert cause is not None
    assert len(cause) <= ctrl._STARTUP_CAUSE_MAX_CHARS


def test_read_startup_error_detail_handles_missing_and_malformed(tmp_path: Path) -> None:
    from almanak.test_controller import __main__ as ctrl

    assert ctrl._read_startup_error_detail(str(tmp_path / "nope.json")) is None

    malformed = tmp_path / "bad.json"
    malformed.write_text("{not json")
    assert ctrl._read_startup_error_detail(str(malformed)) is None

    empty_message = tmp_path / "empty.json"
    empty_message.write_text(json.dumps({"message": "   "}))
    assert ctrl._read_startup_error_detail(str(empty_message)) is None

    ok = tmp_path / "ok.json"
    ok.write_text(json.dumps({"message": _FUNDING_CAUSE}))
    assert ctrl._read_startup_error_detail(str(ok)) == _FUNDING_CAUSE


def _early_exit_spawn_patches(ctrl, summary: str | None):
    """Patches for driving ``_spawn_gateway`` down the early-exit branch with a
    fake child that wrote ``summary`` to the handoff file (None = wrote nothing)."""

    async def fake_create_subprocess_exec(*args, env=None, **kwargs):
        if summary is not None:
            Path(env["ALMANAK_GATEWAY_STARTUP_ERROR_FILE"]).write_text(json.dumps({"message": summary}))
        proc = MagicMock()
        proc.pid = 4242
        proc.returncode = 1
        return proc

    return (
        patch(
            "almanak.test_controller.__main__.asyncio.create_subprocess_exec",
            new=fake_create_subprocess_exec,
        ),
        patch(
            "almanak.test_controller.__main__._wait_for_port",
            new=AsyncMock(return_value=False),
        ),
        patch(
            "almanak.test_controller.__main__._compute_startup_budget",
            new=MagicMock(return_value=0.1),
        ),
    )


@pytest.mark.asyncio()
async def test_spawn_gateway_early_exit_surfaces_child_summary(tmp_path: Path) -> None:
    from almanak.test_controller import __main__ as ctrl

    p1, p2, p3 = _early_exit_spawn_patches(ctrl, f"Managed gateway failed to start: {_FUNDING_CAUSE}")
    with p1, p2, p3:
        with pytest.raises(ctrl._GatewayStartupError) as excinfo:
            await ctrl._spawn_gateway(tmp_path, 55555)

    assert excinfo.value.cause.startswith("Managed Anvil funding could not provision")
    assert "exited early with code 1" in str(excinfo.value)


@pytest.mark.asyncio()
async def test_spawn_gateway_unlinks_handoff_file_when_subprocess_creation_fails(tmp_path: Path) -> None:
    """A create_subprocess_exec failure must not leak the handoff temp file."""
    import glob
    import tempfile as _tempfile

    from almanak.test_controller import __main__ as ctrl

    before = set(glob.glob(f"{_tempfile.gettempdir()}/gw_startup_err_*.json"))
    with (
        patch(
            "almanak.test_controller.__main__.asyncio.create_subprocess_exec",
            new=AsyncMock(side_effect=OSError("spawn refused")),
        ),
        patch("almanak.test_controller.__main__._compute_startup_budget", new=MagicMock(return_value=0.1)),
    ):
        with pytest.raises(OSError, match="spawn refused"):
            await ctrl._spawn_gateway(tmp_path, 55555)

    after = set(glob.glob(f"{_tempfile.gettempdir()}/gw_startup_err_*.json"))
    assert after - before == set(), "handoff file leaked on subprocess-creation failure"


@pytest.mark.asyncio()
async def test_spawn_gateway_early_exit_without_summary_stays_generic(tmp_path: Path) -> None:
    """No handoff summary (crash before the except arm, older child image) →
    the plain RuntimeError path, i.e. the redacted 500, is preserved."""
    from almanak.test_controller import __main__ as ctrl

    p1, p2, p3 = _early_exit_spawn_patches(ctrl, None)
    with p1, p2, p3:
        with pytest.raises(RuntimeError) as excinfo:
            await ctrl._spawn_gateway(tmp_path, 55555)

    assert not isinstance(excinfo.value, ctrl._GatewayStartupError)
    assert str(excinfo.value) == "managed_serve exited early with code 1"


@pytest.mark.asyncio()
async def test_spawn_gateway_early_exit_with_secret_summary_stays_generic(tmp_path: Path) -> None:
    """A summary that doesn't match the allowlist — e.g. an exception whose
    text embeds an upstream URL — must not be surfaced."""
    from almanak.test_controller import __main__ as ctrl

    p1, p2, p3 = _early_exit_spawn_patches(ctrl, "boom: https://eth-mainnet.g.alchemy.com/v2/tok_secret")
    with p1, p2, p3:
        with pytest.raises(RuntimeError) as excinfo:
            await ctrl._spawn_gateway(tmp_path, 55555)

    assert not isinstance(excinfo.value, ctrl._GatewayStartupError)
    assert "alchemy" not in str(excinfo.value)


# silence the unrelated incubating-strategy import collected as a warning
@pytest.fixture(autouse=True)
def _suppress_unrelated_import(caplog):
    yield


_ = tempfile  # imported for future signature-compat


# ─── persistent data-plane gateway ───────────────────────────────────────


def test_ready_reports_disabled_without_token(client: TestClient, monkeypatch) -> None:
    monkeypatch.delenv("ALMANAK_GATEWAY_AUTH_TOKEN", raising=False)
    resp = client.get("/ready")
    assert resp.status_code == 200
    assert resp.json() == {"ok": True, "data_gateway": "disabled"}


def test_ready_503_when_enabled_but_not_ready(client: TestClient, monkeypatch) -> None:
    monkeypatch.setenv("ALMANAK_GATEWAY_AUTH_TOKEN", "tok")
    resp = client.get("/ready")
    assert resp.status_code == 503


def test_ready_ok_when_enabled_and_ready(client: TestClient, monkeypatch) -> None:
    from almanak.test_controller import __main__ as ctrl

    monkeypatch.setenv("ALMANAK_GATEWAY_AUTH_TOKEN", "tok")
    ctrl._data_gateway_ready = True
    resp = client.get("/ready")
    assert resp.status_code == 200
    assert resp.json() == {"ok": True, "data_gateway": "ready"}


def test_status_data_gateway_fields_disabled(client: TestClient, monkeypatch) -> None:
    monkeypatch.delenv("ALMANAK_GATEWAY_AUTH_TOKEN", raising=False)
    body = client.get("/status").json()
    assert body["data_gateway_running"] is None
    assert body["data_gateway_port"] is None


def test_status_data_gateway_fields_enabled(client: TestClient, monkeypatch) -> None:
    from almanak.test_controller import __main__ as ctrl

    monkeypatch.setenv("ALMANAK_GATEWAY_AUTH_TOKEN", "tok")
    ctrl._data_gateway_ready = True
    body = client.get("/status").json()
    assert body["data_gateway_running"] is True
    assert body["data_gateway_port"] == ctrl.DATA_GATEWAY_PORT


def test_test_gateway_env_drops_inherited_auth_token(workspace: Path, monkeypatch) -> None:
    """The per-test gateway must keep exact insecure-loopback semantics even
    when the deploy put a static token in the controller's env."""
    from almanak.test_controller import __main__ as ctrl

    monkeypatch.setenv("ALMANAK_GATEWAY_AUTH_TOKEN", "tok")
    env = ctrl._build_gateway_env(workspace, 12345)
    assert "ALMANAK_GATEWAY_AUTH_TOKEN" not in env
    assert env["ALMANAK_GATEWAY_ALLOW_INSECURE"] == "true"


def test_data_gateway_env_shape(monkeypatch) -> None:
    """Mainnet, standalone, token-authed — and structurally unable to sign."""
    from almanak.test_controller import __main__ as ctrl

    monkeypatch.setenv("ALMANAK_STRATEGY_FOLDER", "/tmp/some-workspace")
    monkeypatch.setenv("ALMANAK_GATEWAY_ALLOW_INSECURE", "true")
    monkeypatch.setenv("ALMANAK_GATEWAY_STARTUP_ERROR_FILE", "/tmp/err.json")
    # Signing material in every known shape — raw keys (bare / prefixed /
    # Solana / future integrations), mnemonics, and keyless signer plumbing
    # (Safe/Zodiac, wallet registry, remote signer) — must all be stripped
    # (codex P1 on PR #3803).
    monkeypatch.setenv("ALMANAK_PRIVATE_KEY", "0xdead")
    monkeypatch.setenv("ALMANAK_GATEWAY_PRIVATE_KEY", "0xbeef")
    monkeypatch.setenv("ALMANAK_GATEWAY_SOLANA_PRIVATE_KEY", "base58stuff")
    monkeypatch.setenv("SOLANA_PRIVATE_KEY", "base58stuff")
    monkeypatch.setenv("SOME_FUTURE_PRIVATE_KEY", "0xf00d")
    monkeypatch.setenv("WALLET_MNEMONIC", "abandon abandon ...")
    monkeypatch.setenv("ALMANAK_GATEWAY_SAFE_MODE", "zodiac")
    monkeypatch.setenv("ALMANAK_GATEWAY_WALLETS", "{}")
    monkeypatch.setenv("ALMANAK_GATEWAY_SIGNER_SERVICE_URL", "https://signer")
    monkeypatch.setenv("ALMANAK_SIGNER_SERVICE_JWT", "jwt")
    monkeypatch.setenv("COINGECKO_API_KEY", "cg-key")

    env = ctrl._build_data_gateway_env(50051, "tok")

    for stripped in (
        "ALMANAK_STRATEGY_FOLDER",
        "ALMANAK_GATEWAY_ALLOW_INSECURE",
        "ALMANAK_GATEWAY_STARTUP_ERROR_FILE",
        "SOLANA_PRIVATE_KEY",
        "SOME_FUTURE_PRIVATE_KEY",
        "WALLET_MNEMONIC",
        "ALMANAK_GATEWAY_SAFE_MODE",
        "ALMANAK_GATEWAY_WALLETS",
        "ALMANAK_GATEWAY_SIGNER_SERVICE_URL",
        "ALMANAK_SIGNER_SERVICE_JWT",
    ):
        assert stripped not in env
    # The canonical key vars are pinned EMPTY, not popped: dotenv loads with
    # override=False, so a present-but-empty var can't be refilled from .env.
    assert env["ALMANAK_PRIVATE_KEY"] == ""
    assert env["ALMANAK_GATEWAY_PRIVATE_KEY"] == ""
    assert env["ALMANAK_GATEWAY_SOLANA_PRIVATE_KEY"] == ""
    assert env["ALMANAK_GATEWAY_NETWORK"] == "mainnet"
    assert env["ALMANAK_GATEWAY_GRPC_HOST"] == "127.0.0.1"
    assert env["ALMANAK_GATEWAY_GRPC_PORT"] == "50051"
    assert env["ALMANAK_GATEWAY_STANDALONE"] == "true"
    assert env["ALMANAK_GATEWAY_AUTH_TOKEN"] == "tok"
    # Provider keys must flow through — they are the point of the gateway.
    assert env["COINGECKO_API_KEY"] == "cg-key"


@pytest.mark.asyncio
async def test_data_gateway_supervisor_sigterms_child_on_cancel(monkeypatch) -> None:
    """Controller shutdown must not orphan the data-gateway subprocess."""
    import signal as _signal

    from almanak.test_controller import __main__ as ctrl

    proc = MagicMock()
    proc.pid = 777
    proc.returncode = None
    blocker = asyncio.Event()

    async def _wait() -> None:
        await blocker.wait()

    proc.wait = _wait

    def _on_sigterm(signum) -> None:
        # Child exits promptly on SIGTERM: the shutdown handler's bounded
        # wait must observe the exit and never need SIGKILL.
        proc.returncode = -15
        blocker.set()

    proc.send_signal = MagicMock(side_effect=_on_sigterm)
    proc.kill = MagicMock()

    async def _fake_spawn(*args, **kwargs):
        return proc

    async def _fake_wait_for_port(p, port, deadline) -> bool:
        return True

    monkeypatch.setattr(asyncio, "create_subprocess_exec", _fake_spawn)
    monkeypatch.setattr(ctrl, "_wait_for_port", _fake_wait_for_port)

    task = asyncio.create_task(ctrl._data_gateway_supervisor("tok"))
    for _ in range(200):
        if ctrl._data_gateway_ready:
            break
        await asyncio.sleep(0.01)
    assert ctrl._data_gateway_ready is True

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    proc.send_signal.assert_called_once_with(_signal.SIGTERM)
    proc.kill.assert_not_called()  # graceful exit observed within the bounded wait
    assert ctrl._data_gateway_ready is False
    assert ctrl._data_gateway_proc is None


@pytest.mark.asyncio
async def test_data_gateway_supervisor_restarts_after_exit(monkeypatch) -> None:
    """A crashed data gateway is respawned (with backoff), not abandoned."""
    from almanak.test_controller import __main__ as ctrl

    monkeypatch.setattr(ctrl, "DATA_GATEWAY_RESTART_BACKOFF_INITIAL_SECONDS", 0.01)
    spawned: list[MagicMock] = []

    def _make_proc() -> MagicMock:
        proc = MagicMock()
        proc.pid = 800 + len(spawned)
        proc.returncode = None

        async def _wait() -> None:
            proc.returncode = 1  # simulate crash after becoming ready

        proc.wait = _wait
        return proc

    async def _fake_spawn(*args, **kwargs):
        proc = _make_proc()
        spawned.append(proc)
        return proc

    async def _fake_wait_for_port(p, port, deadline) -> bool:
        return True

    monkeypatch.setattr(asyncio, "create_subprocess_exec", _fake_spawn)
    monkeypatch.setattr(ctrl, "_wait_for_port", _fake_wait_for_port)

    task = asyncio.create_task(ctrl._data_gateway_supervisor("tok"))
    for _ in range(500):
        if len(spawned) >= 2:
            break
        await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert len(spawned) >= 2
