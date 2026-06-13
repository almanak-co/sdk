"""Smoke tests for the hosted-parity single-strategy dashboard entrypoint (PR 1 / Problem A1).

The entrypoint's contract:

1. With a reachable gateway + a strategy that has `dashboard/ui.py`, it invokes
   `render_custom_dashboard_safe(...)` with a real gateway-backed
   `DashboardAPIClient` (not a mock, not direct SQLite).
2. With an unreachable gateway, it fails closed — surfaces a clear
   "Gateway unreachable" error via `st.error(...)` and does NOT call
   `render_custom_dashboard_safe(...)`. Substituting a mock client would
   silently invalidate the whole reason this entrypoint exists.
3. Without `ALMANAK_DASHBOARD_DEPLOYMENT_ID` / `ALMANAK_DASHBOARD_WORKING_DIR`
   it surfaces a clear "Missing strategy context" error (not a crash).

The tests stub Streamlit's render surface so the entrypoint can be driven
from a normal pytest run — Streamlit's runtime is not required for the
contract being verified here (which is "what does the entrypoint dispatch
to").
"""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from almanak.framework.dashboard import app_single
from almanak.framework.dashboard.custom.api_client import DashboardAPIClient
from almanak.framework.dashboard.gateway_client import GatewayConnectionError


class _StubStreamlit:
    """Record the streamlit calls app_single.main() makes during a smoke run.

    We don't need a real Streamlit runtime — the entrypoint's branching is
    deterministic given env + gateway state; we just need to know which
    error/caption/page-config calls fired.
    """

    def __init__(self) -> None:
        self.errors: list[str] = []
        self.warnings: list[str] = []
        self.captions: list[str] = []
        self.markdowns: list[str] = []
        self.codes: list[str] = []
        self.set_page_config_called = False
        self.session_state: dict[str, Any] = {}

    def set_page_config(self, **_kwargs: Any) -> None:
        self.set_page_config_called = True

    def error(self, msg: str) -> None:
        self.errors.append(msg)

    def warning(self, msg: str) -> None:
        self.warnings.append(msg)

    def caption(self, msg: str) -> None:
        self.captions.append(msg)

    def markdown(self, msg: str) -> None:
        self.markdowns.append(msg)

    def code(self, msg: str, **_kwargs: Any) -> None:
        self.codes.append(msg)


class _FakeGateway:
    def __init__(self, *, connected: bool, connect_raises: bool = False) -> None:
        self.is_connected = connected
        self._connect_raises = connect_raises
        self.connect_call_count = 0

    def connect(self) -> None:
        self.connect_call_count += 1
        if self._connect_raises:
            raise GatewayConnectionError("simulated unreachable gateway")
        self.is_connected = True

    @property
    def connect_called(self) -> bool:
        return self.connect_call_count > 0


@pytest.fixture
def stub_streamlit(monkeypatch) -> _StubStreamlit:
    stub = _StubStreamlit()
    monkeypatch.setattr(app_single, "st", stub)
    return stub


@pytest.fixture
def strategy_dir(tmp_path: Path) -> Path:
    """A minimal strategy folder with config.json and dashboard/ui.py."""
    config = {
        "deployment_id": "TestStrat:abc123",
        "strategy_display_name": "TestStrat:abc123",
        "chain": "arbitrum",
    }
    (tmp_path / "config.json").write_text(json.dumps(config))
    dashboard_dir = tmp_path / "dashboard"
    dashboard_dir.mkdir()
    # ui.py existence is enough — the renderer is stubbed so the body never
    # executes. Keeping it valid python avoids surprises if the stubbing
    # regresses.
    (dashboard_dir / "ui.py").write_text(
        "def render_custom_dashboard(deployment_id, strategy_config, api_client, session_state):\n"
        "    return None\n"
    )
    return tmp_path


def _set_context(monkeypatch, deployment_id: str, working_dir: Path) -> None:
    monkeypatch.setenv("ALMANAK_DASHBOARD_DEPLOYMENT_ID", deployment_id)
    monkeypatch.setenv("ALMANAK_DASHBOARD_WORKING_DIR", str(working_dir))


def test_missing_context_fails_loudly(stub_streamlit, monkeypatch) -> None:
    monkeypatch.delenv("ALMANAK_DASHBOARD_DEPLOYMENT_ID", raising=False)
    monkeypatch.delenv("ALMANAK_DASHBOARD_WORKING_DIR", raising=False)

    app_single.main()

    assert stub_streamlit.set_page_config_called is True
    assert any("Missing strategy context" in e for e in stub_streamlit.errors), stub_streamlit.errors


def test_real_api_client_passed_to_renderer(stub_streamlit, monkeypatch, strategy_dir) -> None:
    """When the gateway is reachable, the entrypoint must build a real
    DashboardAPIClient and pass it to render_custom_dashboard_safe — no
    mock, no None (which would let the renderer pick up its fallback)."""
    _set_context(monkeypatch, "TestStrat:abc123", strategy_dir)

    fake_gateway = _FakeGateway(connected=False)
    monkeypatch.setattr(
        "almanak.framework.dashboard.gateway_client.get_dashboard_client",
        lambda: fake_gateway,
    )

    captured: dict[str, Any] = {}

    def fake_render(**kwargs: Any) -> bool:
        captured.update(kwargs)
        return True

    monkeypatch.setattr(
        "almanak.framework.dashboard.custom.renderer.render_custom_dashboard_safe",
        fake_render,
    )

    app_single.main()

    assert fake_gateway.connect_called is True
    assert not stub_streamlit.errors, f"unexpected errors: {stub_streamlit.errors}"
    assert "api_client" in captured
    assert isinstance(captured["api_client"], DashboardAPIClient), (
        f"expected DashboardAPIClient, got {type(captured['api_client']).__name__}"
    )
    assert captured["deployment_id"] == "TestStrat:abc123"
    # strategy_config came from config.json, not an empty dict
    assert captured["strategy_config"].get("chain") == "arbitrum"


def test_gateway_unreachable_fails_closed_without_mock(stub_streamlit, monkeypatch, strategy_dir) -> None:
    """The whole reason this entrypoint exists: when the gateway can't be
    reached, surface the error — do NOT silently substitute a mock client.
    The presence of any call to render_custom_dashboard_safe here would
    indicate that a mock-or-otherwise-fallback client got passed through."""
    _set_context(monkeypatch, "TestStrat:abc123", strategy_dir)

    fake_gateway = _FakeGateway(connected=False, connect_raises=True)
    monkeypatch.setattr(
        "almanak.framework.dashboard.gateway_client.get_dashboard_client",
        lambda: fake_gateway,
    )

    rendered_with: list[dict[str, Any]] = []

    def must_not_be_called(**kwargs: Any) -> bool:
        rendered_with.append(kwargs)
        return True

    monkeypatch.setattr(
        "almanak.framework.dashboard.custom.renderer.render_custom_dashboard_safe",
        must_not_be_called,
    )

    app_single.main()

    assert rendered_with == [], (
        "render_custom_dashboard_safe must NOT be called when the gateway is "
        "unreachable — substituting a mock client invalidates hosted parity"
    )
    assert any("Gateway unreachable" in e for e in stub_streamlit.errors), stub_streamlit.errors


def test_missing_ui_renders_fallback_without_crashing(stub_streamlit, monkeypatch, tmp_path) -> None:
    """A strategy folder without dashboard/ui.py must render the fallback
    shell, not crash. (Mirrors the hosted-image behaviour for a strategy
    deployed without a custom dashboard.)"""
    (tmp_path / "config.json").write_text(json.dumps({"deployment_id": "Foo:1"}))
    # No dashboard/ui.py written.

    _set_context(monkeypatch, "Foo:1", tmp_path)

    fake_gateway = _FakeGateway(connected=True)
    monkeypatch.setattr(
        "almanak.framework.dashboard.gateway_client.get_dashboard_client",
        lambda: fake_gateway,
    )

    rendered_with: list[dict[str, Any]] = []

    def fake_render(**kwargs: Any) -> bool:
        rendered_with.append(kwargs)
        return True

    monkeypatch.setattr(
        "almanak.framework.dashboard.custom.renderer.render_custom_dashboard_safe",
        fake_render,
    )

    app_single.main()

    # Fallback was rendered (warning surfaced), real renderer not invoked
    assert any("No custom dashboard" in w for w in stub_streamlit.warnings), stub_streamlit.warnings
    assert rendered_with == [], (
        "render_custom_dashboard_safe must not run when dashboard/ui.py is absent"
    )


def test_connect_gateway_fail_closed_returns_none_on_connect_failure(monkeypatch) -> None:
    """Unit-level: _connect_gateway_fail_closed returns None (not a mock) on
    GatewayConnectionError so callers can render the fail-closed error."""
    fake_gateway = _FakeGateway(connected=False, connect_raises=True)
    monkeypatch.setattr(
        "almanak.framework.dashboard.gateway_client.get_dashboard_client",
        lambda: fake_gateway,
    )

    result = app_single._connect_gateway_fail_closed("any-strategy")

    assert result is None


def test_connect_gateway_always_calls_connect_to_force_health_check(monkeypatch) -> None:
    """Codex P2 on PR #2372 — ``gateway.is_connected`` only checks whether a
    gRPC channel object exists. If the gateway is killed externally after
    a successful first connect, ``is_connected`` keeps returning True
    while RPCs silently swallow errors into empty dicts/lists. The fix:
    always call ``gateway.connect()`` regardless of ``is_connected``,
    because ``GatewayDashboardClient.connect()`` always runs a
    ``health_check()`` and raises on stale connections — surfacing the
    fail-closed UX instead of silently rendering empty data."""
    # Gateway is "already connected" (channel exists from a prior render)
    # but we still expect connect() to fire so the underlying health
    # check runs.
    fake_gateway = _FakeGateway(connected=True, connect_raises=False)
    monkeypatch.setattr(
        "almanak.framework.dashboard.gateway_client.get_dashboard_client",
        lambda: fake_gateway,
    )

    result = app_single._connect_gateway_fail_closed("any-strategy")

    assert result is not None  # connect() succeeded → DashboardAPIClient returned
    assert fake_gateway.connect_call_count == 1, (
        "connect() must always be called even when is_connected reports True, "
        "so the underlying health_check runs and stale connections surface"
    )


def test_connect_gateway_catches_unexpected_exceptions_too(monkeypatch) -> None:
    """Claude pr-auditor Important #1 — the connect path must catch
    unexpected exception classes (broken stub, malformed env, gRPC
    channel surprises) not just GatewayConnectionError. Fail-closed is
    the only safe outcome for any connect-side error."""

    class _ExplodingGateway:
        is_connected = False

        def connect(self):
            raise AttributeError("simulated broken stub")

    monkeypatch.setattr(
        "almanak.framework.dashboard.gateway_client.get_dashboard_client",
        lambda: _ExplodingGateway(),
    )

    # Must not propagate the AttributeError — must fail closed with None.
    result = app_single._connect_gateway_fail_closed("any-strategy")

    assert result is None


def test_connect_gateway_fail_closed_returns_real_client_on_success(monkeypatch) -> None:
    """Unit-level: _connect_gateway_fail_closed returns a real
    DashboardAPIClient (not a mock) when the gateway connects."""
    fake_gateway = _FakeGateway(connected=False)
    monkeypatch.setattr(
        "almanak.framework.dashboard.gateway_client.get_dashboard_client",
        lambda: fake_gateway,
    )

    result = app_single._connect_gateway_fail_closed("any-strategy")

    assert isinstance(result, DashboardAPIClient)
    assert result.deployment_id == "any-strategy"


def _install_launch_stubs(monkeypatch) -> tuple[list[list[str]], list[dict[str, str]]]:
    """Patch ``subprocess.Popen`` + ``socket.socket`` so _start_dashboard_background
    records what it WOULD launch without actually spawning streamlit or binding
    a port. Returns (captured_cmds, captured_envs) — both grow by one entry
    per call to the helper."""
    import socket as socket_module
    import subprocess as subprocess_module

    captured_cmd: list[list[str]] = []
    captured_env: list[dict[str, str]] = []

    class _FakeProc:
        pass

    def fake_popen(cmd, env=None, stdout=None, stderr=None):
        captured_cmd.append(list(cmd))
        captured_env.append(dict(env or {}))
        return _FakeProc()

    monkeypatch.setattr(subprocess_module, "Popen", fake_popen)

    class _FakeSock:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def bind(self, *_args):
            return None  # always "free"

    monkeypatch.setattr(socket_module, "socket", lambda *_a, **_kw: _FakeSock())

    # VIB-5012: the launcher now routes child stdout/stderr to a
    # strategy-local dashboard.log. These tests only assert cmd/env capture,
    # so force the DEVNULL fallback instead of writing real files.
    from almanak.framework.cli import run_helpers as _run_helpers

    monkeypatch.setattr(_run_helpers, "_open_dashboard_log", lambda: (None, None))
    return captured_cmd, captured_env


def test_launcher_chooses_app_single_for_hosted_parity(monkeypatch, tmp_path) -> None:
    """When run_helpers launches with mode='hosted-parity', the subprocess
    command points at app_single.py and the env carries the deployment_id +
    working_dir overrides."""
    from almanak.framework.cli import run_helpers

    captured_cmd, captured_env = _install_launch_stubs(monkeypatch)

    proc = run_helpers._start_dashboard_background(
        port=8501,
        gateway_host="127.0.0.1",
        gateway_port=50051,
        auth_token="tok",
        mode="hosted-parity",
        deployment_id="MyStrat:42",
        strategy_working_dir=str(tmp_path),
    )

    assert proc is not None
    assert len(captured_cmd) == 1
    cmd = captured_cmd[0]
    assert any(arg.endswith("app_single.py") for arg in cmd), cmd
    env = captured_env[0]
    assert env.get("ALMANAK_DASHBOARD_DEPLOYMENT_ID") == "MyStrat:42"
    assert env.get("ALMANAK_DASHBOARD_WORKING_DIR") == str(tmp_path.resolve())
    assert env.get("ALMANAK_GATEWAY_AUTH_TOKEN") == "tok"
    # The launcher MUST also strip the legacy unprefixed GATEWAY_AUTH_TOKEN
    # (VIB-520) to prevent a stale .env shadowing the session token.
    assert "GATEWAY_AUTH_TOKEN" not in env


def test_launcher_chooses_app_for_command_center(monkeypatch) -> None:
    """When mode='command-center', the subprocess command points at app.py
    (NOT app_single.py) and skips strategy-scoping env injection."""
    from almanak.framework.cli import run_helpers

    captured_cmd, captured_env = _install_launch_stubs(monkeypatch)

    proc = run_helpers._start_dashboard_background(
        port=8501,
        gateway_host="127.0.0.1",
        gateway_port=50051,
        auth_token=None,
        mode="command-center",
    )

    assert proc is not None
    cmd = captured_cmd[0]
    assert any(arg.endswith("app.py") for arg in cmd) and not any(
        arg.endswith("app_single.py") for arg in cmd
    ), cmd
    env = captured_env[0]
    assert "ALMANAK_DASHBOARD_DEPLOYMENT_ID" not in env
    assert "ALMANAK_DASHBOARD_WORKING_DIR" not in env


def test_launcher_falls_back_to_cc_when_hosted_parity_missing_context(monkeypatch) -> None:
    """Defensive: hosted-parity without deployment_id falls back to Command
    Center rather than launching an unconfigurable app_single.py."""
    from almanak.framework.cli import run_helpers

    captured_cmd, _ = _install_launch_stubs(monkeypatch)

    proc = run_helpers._start_dashboard_background(
        port=8501,
        gateway_host="127.0.0.1",
        gateway_port=50051,
        auth_token=None,
        mode="hosted-parity",
        deployment_id=None,
        strategy_working_dir=None,
    )

    assert proc is not None
    cmd = captured_cmd[0]
    assert any(arg.endswith("app.py") for arg in cmd) and not any(
        arg.endswith("app_single.py") for arg in cmd
    ), cmd


def test_launcher_forwards_runtime_strategy_config_to_env(monkeypatch, tmp_path) -> None:
    """Codex P2 on PR #2372 — the resolved + mutated runtime config (post
    --config overrides, post-runtime-flag mutations) must be forwarded to
    the dashboard subprocess so it doesn't re-read a stale
    ``working_dir/config.json``. Asserted via the new
    ``ALMANAK_DASHBOARD_STRATEGY_CONFIG`` env var."""
    from almanak.framework.cli import run_helpers

    captured_cmd, captured_env = _install_launch_stubs(monkeypatch)

    resolved_config = {
        "deployment_id": "MyStrat:42",
        "chain": "arbitrum",
        "copy_trading": {"mode": "shadow", "strict": True},
        "param_overridden_by_cli": "live-value",
    }
    proc = run_helpers._start_dashboard_background(
        port=8501,
        gateway_host="127.0.0.1",
        gateway_port=50051,
        auth_token="tok",
        mode="hosted-parity",
        deployment_id="MyStrat:42",
        strategy_working_dir=str(tmp_path),
        strategy_config=resolved_config,
    )

    assert proc is not None
    env = captured_env[0]
    forwarded = env.get("ALMANAK_DASHBOARD_STRATEGY_CONFIG")
    assert forwarded, "ALMANAK_DASHBOARD_STRATEGY_CONFIG must be set when strategy_config is supplied"
    parsed = json.loads(forwarded)
    assert parsed == resolved_config, (
        f"forwarded config must match the resolved runtime config verbatim; got {parsed!r}"
    )


def test_launcher_skips_config_env_var_when_strategy_config_is_none(monkeypatch, tmp_path) -> None:
    """When the launcher is called without a strategy_config (e.g. legacy
    callers), the ALMANAK_DASHBOARD_STRATEGY_CONFIG env var must NOT be
    set so app_single falls through to reading working_dir/config.json."""
    from almanak.framework.cli import run_helpers

    captured_cmd, captured_env = _install_launch_stubs(monkeypatch)

    proc = run_helpers._start_dashboard_background(
        port=8501,
        gateway_host="127.0.0.1",
        gateway_port=50051,
        auth_token="tok",
        mode="hosted-parity",
        deployment_id="MyStrat:42",
        strategy_working_dir=str(tmp_path),
        strategy_config=None,
    )

    assert proc is not None
    env = captured_env[0]
    assert "ALMANAK_DASHBOARD_STRATEGY_CONFIG" not in env


def test_app_single_prefers_env_config_over_file_config(monkeypatch, tmp_path, stub_streamlit) -> None:
    """app_single should prefer ``ALMANAK_DASHBOARD_STRATEGY_CONFIG`` over
    the on-disk ``config.json``. End-to-end check: write a stale
    config.json AND set the env var to fresh values; assert the dashboard
    renders with the fresh values."""
    # Write a STALE config.json (old chain, no copy_trading)
    stale_config = {"deployment_id": "MyStrat:42", "chain": "ethereum"}
    (tmp_path / "config.json").write_text(json.dumps(stale_config))
    dashboard_dir = tmp_path / "dashboard"
    dashboard_dir.mkdir()
    (dashboard_dir / "ui.py").write_text(
        "def render_custom_dashboard(deployment_id, strategy_config, api_client, session_state):\n"
        "    return None\n"
    )
    _set_context(monkeypatch, "MyStrat:42", tmp_path)

    fresh_config = {"deployment_id": "MyStrat:42", "chain": "arbitrum", "copy_trading": {"mode": "shadow"}}
    monkeypatch.setenv("ALMANAK_DASHBOARD_STRATEGY_CONFIG", json.dumps(fresh_config))

    fake_gateway = _FakeGateway(connected=False)
    monkeypatch.setattr(
        "almanak.framework.dashboard.gateway_client.get_dashboard_client",
        lambda: fake_gateway,
    )
    captured: dict[str, Any] = {}

    def fake_render(**kwargs):
        captured.update(kwargs)
        return True

    monkeypatch.setattr(
        "almanak.framework.dashboard.custom.renderer.render_custom_dashboard_safe",
        fake_render,
    )

    app_single.main()

    assert captured.get("strategy_config") == fresh_config, (
        f"app_single must prefer env-forwarded runtime config over on-disk config.json; "
        f"got {captured.get('strategy_config')!r}"
    )


def test_app_single_falls_back_to_file_config_on_missing_env(monkeypatch, tmp_path, stub_streamlit) -> None:
    """When ``ALMANAK_DASHBOARD_STRATEGY_CONFIG`` is missing, app_single
    falls back to reading working_dir/config.json (preserves behavior for
    operators who launch app_single manually outside the runner)."""
    file_config = {"deployment_id": "MyStrat:42", "chain": "ethereum"}
    (tmp_path / "config.json").write_text(json.dumps(file_config))
    dashboard_dir = tmp_path / "dashboard"
    dashboard_dir.mkdir()
    (dashboard_dir / "ui.py").write_text(
        "def render_custom_dashboard(deployment_id, strategy_config, api_client, session_state):\n"
        "    return None\n"
    )
    _set_context(monkeypatch, "MyStrat:42", tmp_path)
    monkeypatch.delenv("ALMANAK_DASHBOARD_STRATEGY_CONFIG", raising=False)

    fake_gateway = _FakeGateway(connected=False)
    monkeypatch.setattr(
        "almanak.framework.dashboard.gateway_client.get_dashboard_client",
        lambda: fake_gateway,
    )
    captured: dict[str, Any] = {}

    def fake_render(**kwargs):
        captured.update(kwargs)
        return True

    monkeypatch.setattr(
        "almanak.framework.dashboard.custom.renderer.render_custom_dashboard_safe",
        fake_render,
    )

    app_single.main()

    assert captured.get("strategy_config") == file_config


def test_standalone_dashboard_warns_when_hosted_parity_explicitly_requested(monkeypatch) -> None:
    """Claude pr-auditor Important #3 — standalone mode (no strategy
    directory) always opens Command Center. If the operator explicitly
    passes --dashboard-mode=hosted-parity in this mode, a warning must
    surface so they know the flag was overridden rather than silently
    ignored."""
    import socket as socket_module
    import subprocess as subprocess_module

    from almanak.framework.cli import run_helpers

    # Custom subprocess stub whose .wait() raises KeyboardInterrupt so
    # the standalone branch's blocking wait completes immediately.
    class _ProcWithKeyboardInterrupt:
        def wait(self):
            raise KeyboardInterrupt

        def terminate(self):
            pass

    monkeypatch.setattr(
        subprocess_module,
        "Popen",
        lambda *_args, **_kwargs: _ProcWithKeyboardInterrupt(),
    )

    class _FakeSock:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def bind(self, *_args):
            return None

    monkeypatch.setattr(socket_module, "socket", lambda *_a, **_kw: _FakeSock())

    # VIB-5012: the standalone branch is now selected by resolved-dir
    # contents, not ``working_dir == "."``. Force "not a strategy folder"
    # so this test exercises the standalone path deterministically
    # regardless of the pytest cwd.
    monkeypatch.setattr(
        "almanak.framework.local_paths.looks_like_strategy_folder",
        lambda _path: False,
    )

    captured_errs: list[str] = []
    monkeypatch.setattr(
        "click.echo",
        lambda *args, **kwargs: captured_errs.append(args[0] if args else "")
        if kwargs.get("err")
        else None,
    )

    handled = run_helpers._handle_standalone_dashboard(
        working_dir=".",
        dashboard=True,
        dashboard_port=8501,
        gateway_host="127.0.0.1",
        gateway_port=50051,
        auth_token=None,
        dashboard_mode="hosted-parity",
    )

    assert handled is True
    combined = "\n".join(captured_errs)
    assert "hosted-parity ignored in standalone" in combined, (
        f"standalone branch must warn when hosted-parity was explicitly requested; got: {captured_errs!r}"
    )


# Helper to keep mypy/ruff quiet about unused SimpleNamespace import in some envs.
_ = SimpleNamespace
