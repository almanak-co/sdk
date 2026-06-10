"""Unit tests for the Phase 4d helpers in `almanak/framework/cli/run_helpers.py`.

Covers:
    - _start_dashboard_background   (phase 5a)
    - _stop_dashboard               (phase 5b)
    - _handle_standalone_dashboard  (phase 5c)
    - _run_once                     (phase 15)
    - _run_continuous               (phase 16)

Pattern mirrors `test_run_helpers_components.py`: CliRunner + MagicMock /
AsyncMock fakes for the runner, state manager, gateway_client, and
managed_gateway. No real subprocesses, sockets, or event loops are spun
up by these tests.
"""

from __future__ import annotations

import logging
import subprocess
import sys
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from click.testing import CliRunner

from almanak.framework.cli import run_helpers
from almanak.framework.runner.runner_models import IterationResult, IterationStatus

# ---------------------------------------------------------------------------
# Helpers: fake runner / strategy / state manager
# ---------------------------------------------------------------------------


def _make_fake_runner(
    *,
    iteration_result: IterationResult | None = None,
    teardown_result: IterationResult | None = None,
) -> MagicMock:
    """Build a runner fake covering the surface `_run_once` / `_run_continuous` touch."""
    runner = MagicMock()
    runner.setup_gateway_integration = MagicMock()
    runner.teardown_gateway_integration = MagicMock()
    runner.setup_signal_handlers = MagicMock()
    runner.request_shutdown = MagicMock()
    runner._emit_iteration_summary = MagicMock()
    runner._capture_portfolio_snapshot = AsyncMock()

    # The exit-code logic inspects these directly.
    runner._signal_received = False
    runner._successful_iterations = 0
    runner._total_iterations = 0

    runner.config = MagicMock()
    runner.config.enable_state_persistence = True

    results: list[IterationResult] = []
    if iteration_result is not None:
        results.append(iteration_result)
    if teardown_result is not None:
        results.append(teardown_result)
    runner.run_iteration = AsyncMock(side_effect=results or [_make_success_result()])

    runner.run_loop = AsyncMock()

    return runner


def _make_fake_strategy(*, with_load_state: bool = False, with_flush: bool = False) -> MagicMock:
    """Build a minimal strategy instance fake."""
    strategy = MagicMock()
    strategy.deployment_id = "test-strat"
    strategy.STRATEGY_NAME = "TestStrategy"
    strategy.chain = "arbitrum"
    # activity_provider stays None unless a test wires it up
    strategy._wallet_activity_provider = None

    # Remove load_state_async / flush_pending_saves unless requested so the
    # hasattr() checks in the helper take the false branch.
    if with_load_state:
        strategy.load_state_async = AsyncMock(return_value=False)
    else:
        # Ensure hasattr(..., "load_state_async") returns False
        del strategy.load_state_async
    if with_flush:
        strategy.flush_pending_saves = AsyncMock()
    else:
        del strategy.flush_pending_saves

    return strategy


def _make_fake_state_manager() -> MagicMock:
    state_manager = MagicMock()
    state_manager.load_state = AsyncMock(return_value=None)
    state_manager.save_state = AsyncMock()
    return state_manager


def _make_success_result() -> IterationResult:
    return IterationResult(
        status=IterationStatus.SUCCESS,
        deployment_id="test-strat",
        duration_ms=1.0,
    )


def _make_failed_result() -> IterationResult:
    return IterationResult(
        status=IterationStatus.EXECUTION_FAILED,
        error="it blew up",
        deployment_id="test-strat",
        duration_ms=1.0,
    )


def _make_teardown_result() -> IterationResult:
    return IterationResult(
        status=IterationStatus.TEARDOWN,
        deployment_id="test-strat",
        duration_ms=1.0,
    )


# ---------------------------------------------------------------------------
# _start_dashboard_background
# ---------------------------------------------------------------------------


class TestStartDashboardBackground:
    """Phase 5a helper — dashboard subprocess spawn."""

    @pytest.fixture(autouse=True)
    def _devnull_dashboard_log(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Keep the legacy spawn tests hermetic: VIB-5012 routes child output
        to a strategy-local ``dashboard.log``; these tests don't exercise the
        log path, so force the DEVNULL fallback instead of writing real files.
        ``TestDashboardLogObservability`` covers the log behaviour."""
        monkeypatch.setattr(run_helpers, "_open_dashboard_log", lambda: (None, None))

    def test_happy_path_spawns_subprocess_and_returns_popen(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # Port is available.
        class _FakeSock:
            def __enter__(self) -> _FakeSock:
                return self

            def __exit__(self, *a: Any) -> None:
                pass

            def bind(self, addr: tuple[str, int]) -> None:
                return None

        monkeypatch.setattr(run_helpers, "Path", run_helpers.Path)

        fake_popen = MagicMock(spec=subprocess.Popen)
        captured: dict[str, Any] = {}

        def _popen_factory(cmd: list[str], **kwargs: Any) -> Any:
            captured["cmd"] = cmd
            captured["kwargs"] = kwargs
            return fake_popen

        # Patch socket.socket and subprocess.Popen in the helper's context.
        # Also patch importlib.util.find_spec so the test is hermetic regardless
        # of whether streamlit is installed in the test environment.
        with (
            patch("socket.socket", return_value=_FakeSock()),
            patch("subprocess.Popen", side_effect=_popen_factory),
            patch("importlib.util.find_spec", return_value=MagicMock()),
        ):
            runner = CliRunner()
            with runner.isolation():
                proc = run_helpers._start_dashboard_background(
                    port=8501,
                    gateway_host="127.0.0.1",
                    gateway_port=50051,
                )

        assert proc is fake_popen
        # Launched via `sys.executable -m streamlit run ...` so the current
        # venv's streamlit is always picked up (not a stale PATH entry).
        assert captured["cmd"][0] == sys.executable
        assert captured["cmd"][1:3] == ["-m", "streamlit"]
        assert captured["cmd"][3] == "run"
        assert "8501" in captured["cmd"]
        # Gateway env is threaded through.
        assert captured["kwargs"]["env"]["GATEWAY_HOST"] == "127.0.0.1"
        assert captured["kwargs"]["env"]["GATEWAY_PORT"] == "50051"

    def test_port_busy_falls_back_to_alt_port(self, monkeypatch: pytest.MonkeyPatch) -> None:
        call_log: list[int] = []

        class _FakeSock:
            def __init__(self) -> None:
                self._last_port: int | None = None

            def __enter__(self) -> _FakeSock:
                return self

            def __exit__(self, *a: Any) -> None:
                pass

            def bind(self, addr: tuple[str, int]) -> None:
                port = addr[1]
                call_log.append(port)
                # Primary port 8501 is busy; first alt 8502 is busy; 8503 succeeds.
                if port in (8501, 8502):
                    raise OSError("busy")

        def _sock_factory(*a: Any, **kw: Any) -> _FakeSock:
            return _FakeSock()

        fake_popen = MagicMock(spec=subprocess.Popen)
        with (
            patch("socket.socket", side_effect=_sock_factory),
            patch("subprocess.Popen", return_value=fake_popen) as popen_mock,
            patch("importlib.util.find_spec", return_value=MagicMock()),
        ):
            runner = CliRunner()
            with runner.isolation() as (out, _err):
                proc = run_helpers._start_dashboard_background(port=8501)

        assert proc is fake_popen
        # Primary then alternates probed in order; we expect 8501 tried first.
        assert call_log[0] == 8501
        # `--server.port 8503` passes through to subprocess
        cmd = popen_mock.call_args[0][0]
        assert "8503" in cmd

    def test_no_free_port_returns_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        class _FakeSock:
            def __enter__(self) -> _FakeSock:
                return self

            def __exit__(self, *a: Any) -> None:
                pass

            def bind(self, addr: tuple[str, int]) -> None:
                raise OSError("always busy")

        with patch("socket.socket", return_value=_FakeSock()):
            runner = CliRunner()
            with runner.isolation():
                proc = run_helpers._start_dashboard_background(port=8501)

        assert proc is None

    def test_returns_none_when_streamlit_module_missing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Preflight `importlib.util.find_spec('streamlit')` returns None -> helper
        bails out with a friendly error before touching Popen.

        This replaces the previous ``FileNotFoundError``-on-Popen test: since the
        launch now uses ``sys.executable -m streamlit``, Popen never raises
        FileNotFoundError when streamlit is missing (Python itself starts fine).
        The preflight is the only guard, and this test covers it.
        """
        class _FakeSock:
            def __enter__(self) -> _FakeSock:
                return self

            def __exit__(self, *a: Any) -> None:
                pass

            def bind(self, addr: tuple[str, int]) -> None:
                return None

        popen_mock = MagicMock()
        with (
            patch("socket.socket", return_value=_FakeSock()),
            patch("subprocess.Popen", popen_mock),
            patch("importlib.util.find_spec", return_value=None) as find_spec_mock,
        ):
            runner = CliRunner(mix_stderr=False)
            with runner.isolation() as (_out, err):
                proc = run_helpers._start_dashboard_background(port=8501)
                err_value = err.getvalue()

        assert proc is None
        # Preflight was consulted for the streamlit module specifically.
        find_spec_mock.assert_any_call("streamlit")
        # Popen must never be called when the module is missing.
        popen_mock.assert_not_called()
        # Friendly error is echoed to stderr.
        assert b"streamlit not found" in err_value

    def test_unexpected_exception_returns_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        class _FakeSock:
            def __enter__(self) -> _FakeSock:
                return self

            def __exit__(self, *a: Any) -> None:
                pass

            def bind(self, addr: tuple[str, int]) -> None:
                return None

        with (
            patch("socket.socket", return_value=_FakeSock()),
            patch("subprocess.Popen", side_effect=RuntimeError("boom")),
            patch("importlib.util.find_spec", return_value=MagicMock()),
        ):
            runner = CliRunner()
            with runner.isolation():
                proc = run_helpers._start_dashboard_background(port=8501)

        assert proc is None

    def test_auth_token_is_forwarded_and_overrides_inherited_env(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Managed-gateway session token must be threaded into the dashboard
        subprocess as ``ALMANAK_GATEWAY_AUTH_TOKEN``, and any inherited
        ``GATEWAY_AUTH_TOKEN`` must be stripped so it can't shadow it.

        Regression: on mainnet ``strat run`` rolls a fresh
        ``session_auth_token = uuid.uuid4().hex`` (VIB-520). If we don't
        forward it, the subprocess inherits a stale ``GATEWAY_AUTH_TOKEN``
        from the operator's ``.env`` and every dashboard gRPC call returns
        UNAUTHENTICATED.
        """
        # Simulate a stale token already in the parent env that must NOT win.
        monkeypatch.setenv("GATEWAY_AUTH_TOKEN", "stale-from-dotenv")
        monkeypatch.delenv("ALMANAK_GATEWAY_AUTH_TOKEN", raising=False)

        class _FakeSock:
            def __enter__(self) -> _FakeSock:
                return self

            def __exit__(self, *a: Any) -> None:
                pass

            def bind(self, addr: tuple[str, int]) -> None:
                return None

        captured: dict[str, Any] = {}

        def _popen_factory(cmd: list[str], **kwargs: Any) -> Any:
            captured["env"] = kwargs["env"]
            return MagicMock(spec=subprocess.Popen)

        with (
            patch("socket.socket", return_value=_FakeSock()),
            patch("subprocess.Popen", side_effect=_popen_factory),
            patch("importlib.util.find_spec", return_value=MagicMock()),
        ):
            runner = CliRunner()
            with runner.isolation():
                run_helpers._start_dashboard_background(
                    port=8501, auth_token="fresh-session-uuid"
                )

        env = captured["env"]
        assert env["ALMANAK_GATEWAY_AUTH_TOKEN"] == "fresh-session-uuid"
        # Inherited GATEWAY_AUTH_TOKEN MUST be stripped — otherwise
        # ``ALMANAK_GATEWAY_AUTH_TOKEN or GATEWAY_AUTH_TOKEN`` in the
        # gateway client could pick the stale one.
        assert "GATEWAY_AUTH_TOKEN" not in env

    def test_no_auth_token_leaves_inherited_env_alone(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Backwards compat: when ``auth_token`` is None (e.g. anvil/sepolia
        managed-gateway path where the token is intentionally None), the
        helper must not mutate inherited env."""
        monkeypatch.setenv("GATEWAY_AUTH_TOKEN", "user-supplied")
        monkeypatch.setenv("ALMANAK_GATEWAY_AUTH_TOKEN", "user-supplied-canonical")

        class _FakeSock:
            def __enter__(self) -> _FakeSock:
                return self

            def __exit__(self, *a: Any) -> None:
                pass

            def bind(self, addr: tuple[str, int]) -> None:
                return None

        captured: dict[str, Any] = {}

        def _popen_factory(cmd: list[str], **kwargs: Any) -> Any:
            captured["env"] = kwargs["env"]
            return MagicMock(spec=subprocess.Popen)

        with (
            patch("socket.socket", return_value=_FakeSock()),
            patch("subprocess.Popen", side_effect=_popen_factory),
            patch("importlib.util.find_spec", return_value=MagicMock()),
        ):
            runner = CliRunner()
            with runner.isolation():
                run_helpers._start_dashboard_background(port=8501)

        env = captured["env"]
        assert env["GATEWAY_AUTH_TOKEN"] == "user-supplied"
        assert env["ALMANAK_GATEWAY_AUTH_TOKEN"] == "user-supplied-canonical"


# ---------------------------------------------------------------------------
# _stop_dashboard
# ---------------------------------------------------------------------------


class TestStopDashboard:
    """Phase 5b helper — dashboard subprocess shutdown."""

    def test_none_is_noop(self) -> None:
        # Should not raise.
        run_helpers._stop_dashboard(None)

    def test_alive_process_terminates_and_waits(self) -> None:
        proc = MagicMock(spec=subprocess.Popen)
        proc.poll.return_value = None  # still alive
        run_helpers._stop_dashboard(proc)
        proc.terminate.assert_called_once()
        proc.wait.assert_called_once_with(timeout=5)
        proc.kill.assert_not_called()

    def test_terminate_failure_falls_back_to_kill(self) -> None:
        proc = MagicMock(spec=subprocess.Popen)
        proc.poll.return_value = None  # still alive
        proc.terminate.side_effect = RuntimeError("nope")
        run_helpers._stop_dashboard(proc)
        proc.kill.assert_called_once()

    def test_terminate_and_kill_both_fail_silently(self) -> None:
        proc = MagicMock(spec=subprocess.Popen)
        proc.poll.return_value = None  # still alive
        proc.terminate.side_effect = RuntimeError("nope")
        proc.kill.side_effect = RuntimeError("also nope")
        # Must not propagate
        run_helpers._stop_dashboard(proc)


# ---------------------------------------------------------------------------
# Dashboard child observability (VIB-5012)
# ---------------------------------------------------------------------------


class _AvailableSock:
    """Fake socket whose bind always succeeds (port available)."""

    def __enter__(self) -> _AvailableSock:
        return self

    def __exit__(self, *a: Any) -> None:
        pass

    def bind(self, addr: tuple[str, int]) -> None:
        return None


class TestDashboardLogObservability:
    """VIB-5012 — dashboard child stdout/stderr capture + lifecycle logging.

    The dashboard subprocess used to be spawned with stdout/stderr=DEVNULL,
    so a hung/dead dashboard left zero evidence. These tests pin the new
    contract: output appends to a strategy-local ``dashboard.log``, the
    parent logs PID + log path at spawn and the return code at shutdown,
    and a failure to open the log degrades to the old DEVNULL behaviour.
    """

    _LOGGER_NAME = "almanak.framework.cli.run_helpers"

    def _spawn(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Any,
    ) -> tuple[Any, dict[str, Any], Any]:
        """Launch the helper with ``local_log_path`` anchored to ``tmp_path``.

        Returns ``(proc, captured_popen_kwargs, log_path)``.
        """
        log_path = tmp_path / "dashboard.log"
        monkeypatch.setattr(
            "almanak.framework.local_paths.local_log_path",
            lambda name: tmp_path / f"{name}.log",
        )

        captured: dict[str, Any] = {}
        fake_popen = MagicMock(spec=subprocess.Popen)
        fake_popen.pid = 4242

        def _default_factory(cmd: list[str], **kwargs: Any) -> Any:
            captured["cmd"] = cmd
            captured["kwargs"] = kwargs
            return fake_popen

        with (
            patch("socket.socket", return_value=_AvailableSock()),
            patch("subprocess.Popen", side_effect=_default_factory),
            patch("importlib.util.find_spec", return_value=MagicMock()),
        ):
            runner = CliRunner()
            with runner.isolation():
                proc = run_helpers._start_dashboard_background(port=8501)

        return proc, captured, log_path

    def test_spawn_redirects_output_to_dashboard_log(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Any,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """stdout is the dashboard.log handle (append mode, NOT DEVNULL),
        stderr folds into stdout, the spawn banner is written, the handle is
        stashed for shutdown cleanup, and PID + log path are logged at INFO."""
        with caplog.at_level(logging.INFO, logger=self._LOGGER_NAME):
            proc, captured, log_path = self._spawn(monkeypatch, tmp_path)

        assert proc is not None
        stdout = captured["kwargs"]["stdout"]
        assert stdout is not subprocess.DEVNULL
        assert stdout.name == str(log_path)
        assert stdout.mode == "a"
        assert captured["kwargs"]["stderr"] is subprocess.STDOUT

        # Spawn banner: timestamp + PID + command, flushed by the parent.
        banner = log_path.read_text()
        assert "dashboard spawn pid=4242" in banner
        assert "-m streamlit run" in banner

        # Handle is stashed on the Popen object so _stop_dashboard can close it.
        assert getattr(proc, "_almanak_dashboard_log_handle", None) is stdout

        # Parent INFO log carries the PID and the log path.
        spawn_logs = [r.message for r in caplog.records if "Dashboard subprocess started" in r.message]
        assert spawn_logs, caplog.text
        assert "4242" in spawn_logs[0]
        assert str(log_path) in spawn_logs[0]

        stdout.close()

    def test_restart_appends_instead_of_clobbering(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> None:
        """Append mode: evidence from a previous run survives a restart."""
        (tmp_path / "dashboard.log").write_text("prior-run-evidence\n")
        proc, _captured, log_path = self._spawn(monkeypatch, tmp_path)
        content = log_path.read_text()
        assert content.startswith("prior-run-evidence\n")
        assert "dashboard spawn pid=4242" in content
        proc._almanak_dashboard_log_handle.close()

    def test_open_failure_falls_back_to_devnull(
        self,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """When dashboard.log can't be opened, the run must proceed with the
        historical DEVNULL behaviour and a warning — never break the run."""

        def _boom(name: str) -> Any:
            raise OSError("read-only disk")

        monkeypatch.setattr("almanak.framework.local_paths.local_log_path", _boom)

        captured: dict[str, Any] = {}
        fake_popen = MagicMock(spec=subprocess.Popen)
        fake_popen.pid = 4242

        def _factory(cmd: list[str], **kwargs: Any) -> Any:
            captured["kwargs"] = kwargs
            return fake_popen

        with (
            patch("socket.socket", return_value=_AvailableSock()),
            patch("subprocess.Popen", side_effect=_factory),
            patch("importlib.util.find_spec", return_value=MagicMock()),
            caplog.at_level(logging.WARNING, logger=self._LOGGER_NAME),
        ):
            runner = CliRunner()
            with runner.isolation():
                proc = run_helpers._start_dashboard_background(port=8501)

        assert proc is fake_popen
        assert captured["kwargs"]["stdout"] is subprocess.DEVNULL
        assert captured["kwargs"]["stderr"] is subprocess.DEVNULL
        assert any("falling back to DEVNULL" in r.message for r in caplog.records), caplog.text

    def test_popen_failure_closes_log_handle(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> None:
        """The opened dashboard.log handle must not leak when the spawn raises."""
        fake_handle = MagicMock()
        monkeypatch.setattr(
            run_helpers,
            "_open_dashboard_log",
            lambda: (fake_handle, tmp_path / "dashboard.log"),
        )

        with (
            patch("socket.socket", return_value=_AvailableSock()),
            patch("subprocess.Popen", side_effect=RuntimeError("boom")),
            patch("importlib.util.find_spec", return_value=MagicMock()),
        ):
            runner = CliRunner()
            with runner.isolation():
                proc = run_helpers._start_dashboard_background(port=8501)

        assert proc is None
        fake_handle.close.assert_called_once()

    def test_stop_logs_returncode_and_closes_handle(self, caplog: pytest.LogCaptureFixture) -> None:
        proc = MagicMock(spec=subprocess.Popen)
        proc.pid = 4242
        proc.poll.return_value = None  # alive until terminated
        proc.returncode = -15
        log_handle = MagicMock()
        proc._almanak_dashboard_log_handle = log_handle

        with caplog.at_level(logging.INFO, logger=self._LOGGER_NAME):
            run_helpers._stop_dashboard(proc)

        proc.terminate.assert_called_once()
        log_handle.close.assert_called_once()
        stop_logs = [r.message for r in caplog.records if "stopped with returncode" in r.message]
        assert stop_logs, caplog.text
        assert "4242" in stop_logs[0]
        assert "-15" in stop_logs[0]

    def test_stop_surfaces_child_that_already_died(self, caplog: pytest.LogCaptureFixture) -> None:
        """A dashboard that died silently mid-run is surfaced at shutdown
        (poll() non-None before any signal) instead of being terminated again."""
        proc = MagicMock(spec=subprocess.Popen)
        proc.pid = 4242
        proc.poll.return_value = 1  # already exited
        proc.returncode = 1
        log_handle = MagicMock()
        proc._almanak_dashboard_log_handle = log_handle

        with caplog.at_level(logging.INFO, logger=self._LOGGER_NAME):
            run_helpers._stop_dashboard(proc)

        proc.terminate.assert_not_called()
        log_handle.close.assert_called_once()
        died_logs = [r.message for r in caplog.records if "had already exited" in r.message]
        assert died_logs, caplog.text
        assert "4242" in died_logs[0]
        assert "returncode=1" in died_logs[0]

    def test_stop_is_idempotent_for_explicit_plus_atexit_call(self) -> None:
        """run() calls _stop_dashboard explicitly AND registers it via atexit;
        the second invocation must be a no-op (no double terminate/close)."""
        proc = MagicMock(spec=subprocess.Popen)
        proc.pid = 4242
        proc.poll.return_value = None
        proc.returncode = 0
        log_handle = MagicMock()
        proc._almanak_dashboard_log_handle = log_handle

        run_helpers._stop_dashboard(proc)
        run_helpers._stop_dashboard(proc)

        proc.terminate.assert_called_once()
        log_handle.close.assert_called_once()


# ---------------------------------------------------------------------------
# _handle_standalone_dashboard
# ---------------------------------------------------------------------------


class TestHandleStandaloneDashboard:
    """Phase 5c helper — standalone dashboard early-exit branch."""

    def test_non_standalone_returns_false(self) -> None:
        # dashboard=False OR working_dir != "." -> does nothing, returns False
        assert (
            run_helpers._handle_standalone_dashboard(
                working_dir=".",
                dashboard=False,
                dashboard_port=8501,
                gateway_host="localhost",
                gateway_port=50051,
            )
            is False
        )
        assert (
            run_helpers._handle_standalone_dashboard(
                working_dir="/some/dir",
                dashboard=True,
                dashboard_port=8501,
                gateway_host="localhost",
                gateway_port=50051,
            )
            is False
        )

    def test_standalone_launches_and_waits(self, monkeypatch: pytest.MonkeyPatch) -> None:
        fake_proc = MagicMock()
        fake_proc.wait = MagicMock(return_value=None)

        monkeypatch.setattr(run_helpers, "_start_dashboard_background", lambda **_k: fake_proc)

        runner = CliRunner()
        with runner.isolation() as (out, _err):
            result = run_helpers._handle_standalone_dashboard(
                working_dir=".",
                dashboard=True,
                dashboard_port=8501,
                gateway_host="localhost",
                gateway_port=50051,
            )

        assert result is True
        fake_proc.wait.assert_called_once()

    def test_standalone_launch_failure_exits_1(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(run_helpers, "_start_dashboard_background", lambda **_k: None)
        runner = CliRunner()
        with pytest.raises(SystemExit) as exc:
            with runner.isolation():
                run_helpers._handle_standalone_dashboard(
                    working_dir=".",
                    dashboard=True,
                    dashboard_port=8501,
                    gateway_host="localhost",
                    gateway_port=50051,
                )
        assert exc.value.code == 1

    def test_keyboard_interrupt_stops_dashboard(self, monkeypatch: pytest.MonkeyPatch) -> None:
        fake_proc = MagicMock()
        fake_proc.wait = MagicMock(side_effect=KeyboardInterrupt)
        stop_calls: list[Any] = []
        monkeypatch.setattr(run_helpers, "_start_dashboard_background", lambda **_k: fake_proc)
        monkeypatch.setattr(run_helpers, "_stop_dashboard", lambda p: stop_calls.append(p))

        runner = CliRunner()
        with runner.isolation() as (out, _err):
            result = run_helpers._handle_standalone_dashboard(
                working_dir=".",
                dashboard=True,
                dashboard_port=8501,
                gateway_host="localhost",
                gateway_port=50051,
            )

        assert result is True
        assert stop_calls == [fake_proc]

    def test_auth_token_is_forwarded_to_subprocess_launcher(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The ``auth_token`` kwarg must reach
        ``_start_dashboard_background`` so the standalone-dashboard path
        also gets the managed-gateway session token."""
        captured: dict[str, Any] = {}

        def _fake_launcher(**kw: Any) -> Any:
            captured.update(kw)
            return MagicMock()

        monkeypatch.setattr(run_helpers, "_start_dashboard_background", _fake_launcher)

        runner = CliRunner()
        with runner.isolation():
            run_helpers._handle_standalone_dashboard(
                working_dir=".",
                dashboard=True,
                dashboard_port=8501,
                gateway_host="localhost",
                gateway_port=50051,
                auth_token="standalone-session-uuid",
            )

        assert captured["auth_token"] == "standalone-session-uuid"


# ---------------------------------------------------------------------------
# _run_once
# ---------------------------------------------------------------------------


class TestRunOnce:
    """Phase 15 helper — single-iteration execution + exit code."""

    @pytest.fixture(autouse=True)
    def _isolated_teardown_state_db(self, monkeypatch, tmp_path) -> None:
        """Pin ``ALMANAK_STATE_DB`` so ``_run_once`` with ``teardown_after=True``
        can build a real ``TeardownStateManager`` (VIB-3835 strict resolver
        otherwise hard-fails when no strategy folder is set). The DB sits
        in a per-test tmp file; ``create_request`` writes one row and that's it.
        """
        monkeypatch.delenv("ALMANAK_IS_HOSTED", raising=False)
        monkeypatch.delenv("ALMANAK_STRATEGY_FOLDER", raising=False)
        monkeypatch.setenv("ALMANAK_STATE_DB", str(tmp_path / "test_state.db"))
        # Reset the singleton in case a prior test already cached a manager
        # for a different path.
        from almanak.framework.teardown import state_manager as state_manager_module

        state_manager_module._default_manager = None

    def test_happy_path_returns_exit_0(self) -> None:
        runner = _make_fake_runner(iteration_result=_make_success_result())
        strategy = _make_fake_strategy()
        state_manager = _make_fake_state_manager()
        cleanup = AsyncMock()

        cli = CliRunner()
        with cli.isolation():
            code = run_helpers._run_once(
                runner=runner,
                strategy_instance=strategy,
                state_manager=state_manager,
                cleanup_fn=cleanup,
                teardown_after=False,
            )

        assert code == 0
        runner.setup_gateway_integration.assert_called_once_with(strategy)
        runner.run_iteration.assert_awaited_once_with(strategy)
        runner.teardown_gateway_integration.assert_called_once_with("test-strat")
        cleanup.assert_awaited_once()

    def test_iteration_failure_returns_exit_1(self) -> None:
        runner = _make_fake_runner(iteration_result=_make_failed_result())
        strategy = _make_fake_strategy()
        cleanup = AsyncMock()

        cli = CliRunner()
        with cli.isolation():
            code = run_helpers._run_once(
                runner=runner,
                strategy_instance=strategy,
                state_manager=_make_fake_state_manager(),
                cleanup_fn=cleanup,
                teardown_after=False,
            )

        assert code == 1
        cleanup.assert_awaited_once()

    def test_teardown_after_success_runs_second_iteration(self) -> None:
        runner = _make_fake_runner(
            iteration_result=_make_success_result(),
            teardown_result=_make_teardown_result(),
        )
        # Configure run_iteration to return results in order.
        runner.run_iteration = AsyncMock(side_effect=[_make_success_result(), _make_teardown_result()])
        strategy = _make_fake_strategy()
        cleanup = AsyncMock()

        cli = CliRunner()
        with cli.isolation():
            code = run_helpers._run_once(
                runner=runner,
                strategy_instance=strategy,
                state_manager=_make_fake_state_manager(),
                cleanup_fn=cleanup,
                teardown_after=True,
            )

        assert code == 0
        # Two iterations: main + teardown
        assert runner.run_iteration.await_count == 2

    def test_teardown_after_with_first_iteration_failure_still_runs_teardown(self) -> None:
        """The original code runs teardown unconditionally (no early exit)."""
        runner = _make_fake_runner()
        runner.run_iteration = AsyncMock(side_effect=[_make_failed_result(), _make_teardown_result()])
        strategy = _make_fake_strategy()
        cleanup = AsyncMock()

        cli = CliRunner()
        with cli.isolation():
            code = run_helpers._run_once(
                runner=runner,
                strategy_instance=strategy,
                state_manager=_make_fake_state_manager(),
                cleanup_fn=cleanup,
                teardown_after=True,
            )

        # Main failed -> exit 1 even though teardown succeeded.
        assert code == 1
        assert runner.run_iteration.await_count == 2

    def test_teardown_after_with_teardown_failure_returns_1(self) -> None:
        runner = _make_fake_runner()
        runner.run_iteration = AsyncMock(side_effect=[_make_success_result(), _make_failed_result()])
        strategy = _make_fake_strategy()
        cleanup = AsyncMock()

        cli = CliRunner()
        with cli.isolation():
            code = run_helpers._run_once(
                runner=runner,
                strategy_instance=strategy,
                state_manager=_make_fake_state_manager(),
                cleanup_fn=cleanup,
                teardown_after=True,
            )

        assert code == 1

    def test_cleanup_awaited_on_iteration_exception(self) -> None:
        runner = _make_fake_runner()
        runner.run_iteration = AsyncMock(side_effect=RuntimeError("exploded"))
        strategy = _make_fake_strategy()
        cleanup = AsyncMock()

        cli = CliRunner()
        with cli.isolation():
            code = run_helpers._run_once(
                runner=runner,
                strategy_instance=strategy,
                state_manager=_make_fake_state_manager(),
                cleanup_fn=cleanup,
                teardown_after=False,
            )

        assert code == 1
        cleanup.assert_awaited_once()
        # Gateway integration teardown still happened
        runner.teardown_gateway_integration.assert_called_once_with("test-strat")

    def test_portfolio_snapshot_called_when_persistence_enabled(self) -> None:
        runner = _make_fake_runner(iteration_result=_make_success_result())
        runner.config.enable_state_persistence = True
        strategy = _make_fake_strategy()
        cleanup = AsyncMock()

        cli = CliRunner()
        with cli.isolation():
            run_helpers._run_once(
                runner=runner,
                strategy_instance=strategy,
                state_manager=_make_fake_state_manager(),
                cleanup_fn=cleanup,
                teardown_after=False,
            )
        runner._capture_portfolio_snapshot.assert_awaited_once()

    def test_portfolio_snapshot_skipped_when_persistence_disabled(self) -> None:
        runner = _make_fake_runner(iteration_result=_make_success_result())
        runner.config.enable_state_persistence = False
        strategy = _make_fake_strategy()
        cleanup = AsyncMock()

        cli = CliRunner()
        with cli.isolation():
            run_helpers._run_once(
                runner=runner,
                strategy_instance=strategy,
                state_manager=_make_fake_state_manager(),
                cleanup_fn=cleanup,
                teardown_after=False,
            )
        runner._capture_portfolio_snapshot.assert_not_awaited()

    def test_copy_trading_restore_and_persist_order(self) -> None:
        """When activity_provider is present, load_state is called before and after iteration."""
        runner = _make_fake_runner(iteration_result=_make_success_result())
        strategy = _make_fake_strategy()
        activity = MagicMock()
        activity.get_state = MagicMock(return_value={"cursor": 42})
        activity.set_state = MagicMock()
        strategy._wallet_activity_provider = activity

        # Set up state manager to return a state on first call (restore), then None (persist path)
        existing = MagicMock()
        existing.state = {"copy_trading_state": {"cursor": 10}}
        existing.version = 3
        state_manager = MagicMock()
        state_manager.load_state = AsyncMock(side_effect=[existing, existing])
        state_manager.save_state = AsyncMock()

        cleanup = AsyncMock()

        cli = CliRunner()
        with cli.isolation():
            run_helpers._run_once(
                runner=runner,
                strategy_instance=strategy,
                state_manager=state_manager,
                cleanup_fn=cleanup,
                teardown_after=False,
            )

        activity.set_state.assert_called_once_with({"cursor": 10})
        activity.get_state.assert_called_once()
        state_manager.save_state.assert_awaited_once()

    def test_flush_pending_saves_called_when_available(self) -> None:
        runner = _make_fake_runner(iteration_result=_make_success_result())
        strategy = _make_fake_strategy(with_flush=True)
        cleanup = AsyncMock()

        cli = CliRunner()
        with cli.isolation():
            run_helpers._run_once(
                runner=runner,
                strategy_instance=strategy,
                state_manager=_make_fake_state_manager(),
                cleanup_fn=cleanup,
                teardown_after=False,
            )
        strategy.flush_pending_saves.assert_awaited_once()

    def test_cleanup_awaited_when_setup_gateway_integration_raises(self) -> None:
        """CR Major 3117799693: cleanup_fn must run even if setup fails.

        If ``setup_gateway_integration`` raises during ``run_once``, the
        inner try/finally guarantees ``cleanup_fn`` is still awaited.
        ``teardown_gateway_integration`` MUST NOT be called because setup
        never completed -- the ``gateway_integration_ready`` guard enforces
        the pairing invariant.
        """
        runner = _make_fake_runner()
        runner.setup_gateway_integration = MagicMock(side_effect=RuntimeError("setup boom"))
        strategy = _make_fake_strategy()
        cleanup = AsyncMock()

        cli = CliRunner()
        with cli.isolation():
            code = run_helpers._run_once(
                runner=runner,
                strategy_instance=strategy,
                state_manager=_make_fake_state_manager(),
                cleanup_fn=cleanup,
                teardown_after=False,
            )

        # The outer try/except in _run_once swallows the RuntimeError
        # into an exit code of 1 (matches existing iteration-exception test).
        assert code == 1
        cleanup.assert_awaited_once()
        # Setup failed -> teardown should NOT have been called.
        runner.teardown_gateway_integration.assert_not_called()

    def test_cleanup_awaited_when_teardown_gateway_integration_raises(self) -> None:
        """CR Major 3117799693: cleanup_fn must run even if teardown fails.

        Iteration completes successfully, but ``teardown_gateway_integration``
        raises inside the ``finally``. The nested try/finally guarantees
        ``cleanup_fn`` is still awaited despite the teardown exception.
        """
        runner = _make_fake_runner(iteration_result=_make_success_result())
        runner.teardown_gateway_integration = MagicMock(side_effect=RuntimeError("teardown boom"))
        strategy = _make_fake_strategy()
        cleanup = AsyncMock()

        cli = CliRunner()
        with cli.isolation():
            code = run_helpers._run_once(
                runner=runner,
                strategy_instance=strategy,
                state_manager=_make_fake_state_manager(),
                cleanup_fn=cleanup,
                teardown_after=False,
            )

        # Teardown raised after a successful iteration; the outer try/except
        # wraps this into exit code 1.
        assert code == 1
        cleanup.assert_awaited_once()
        # Teardown was attempted (and raised).
        runner.teardown_gateway_integration.assert_called_once_with("test-strat")


# ---------------------------------------------------------------------------
# _run_continuous
# ---------------------------------------------------------------------------


class TestRunContinuous:
    """Phase 16 helper — loop execution + exit code."""

    def test_happy_path_returns_exit_0(self) -> None:
        runner = _make_fake_runner()
        runner.run_loop = AsyncMock()
        runner._signal_received = False
        runner._successful_iterations = 3
        runner._total_iterations = 3
        strategy = _make_fake_strategy()
        cleanup = AsyncMock()

        cli = CliRunner()
        with cli.isolation():
            code = run_helpers._run_continuous(
                runner=runner,
                strategy_instance=strategy,
                cleanup_fn=cleanup,
                interval=30,
                max_iterations=None,
                reset_fork=False,
                managed_gateway=None,
            )

        assert code == 0
        runner.setup_signal_handlers.assert_called_once()
        runner.run_loop.assert_awaited_once()
        cleanup.assert_awaited()  # finally clause

    def test_signal_received_returns_exit_2(self) -> None:
        runner = _make_fake_runner()
        runner.run_loop = AsyncMock()
        runner._signal_received = True
        strategy = _make_fake_strategy()
        cleanup = AsyncMock()

        cli = CliRunner()
        with cli.isolation():
            code = run_helpers._run_continuous(
                runner=runner,
                strategy_instance=strategy,
                cleanup_fn=cleanup,
                interval=30,
                max_iterations=None,
                reset_fork=False,
                managed_gateway=None,
            )

        assert code == 2

    def test_max_iterations_all_failed_returns_exit_1(self) -> None:
        runner = _make_fake_runner()
        runner.run_loop = AsyncMock()
        runner._signal_received = False
        runner._successful_iterations = 0
        runner._total_iterations = 5
        strategy = _make_fake_strategy()
        cleanup = AsyncMock()

        cli = CliRunner()
        with cli.isolation():
            code = run_helpers._run_continuous(
                runner=runner,
                strategy_instance=strategy,
                cleanup_fn=cleanup,
                interval=30,
                max_iterations=5,
                reset_fork=False,
                managed_gateway=None,
            )

        assert code == 1

    def test_max_iterations_some_succeeded_returns_exit_0(self) -> None:
        runner = _make_fake_runner()
        runner.run_loop = AsyncMock()
        runner._signal_received = False
        runner._successful_iterations = 2
        runner._total_iterations = 5
        strategy = _make_fake_strategy()
        cleanup = AsyncMock()

        cli = CliRunner()
        with cli.isolation():
            code = run_helpers._run_continuous(
                runner=runner,
                strategy_instance=strategy,
                cleanup_fn=cleanup,
                interval=30,
                max_iterations=5,
                reset_fork=False,
                managed_gateway=None,
            )

        assert code == 0

    def test_keyboard_interrupt_returns_exit_0(self) -> None:
        runner = _make_fake_runner()
        runner.run_loop = AsyncMock(side_effect=KeyboardInterrupt)
        strategy = _make_fake_strategy()
        cleanup = AsyncMock()

        cli = CliRunner()
        with cli.isolation():
            code = run_helpers._run_continuous(
                runner=runner,
                strategy_instance=strategy,
                cleanup_fn=cleanup,
                interval=30,
                max_iterations=None,
                reset_fork=False,
                managed_gateway=None,
            )

        # Original semantic: KeyboardInterrupt -> exit 0 (explicit sys.exit(0))
        assert code == 0
        runner.request_shutdown.assert_called_once()

    def test_unhandled_exception_returns_exit_1(self) -> None:
        runner = _make_fake_runner()
        runner.run_loop = AsyncMock(side_effect=RuntimeError("boom"))
        strategy = _make_fake_strategy()
        cleanup = AsyncMock()

        cli = CliRunner()
        with cli.isolation():
            code = run_helpers._run_continuous(
                runner=runner,
                strategy_instance=strategy,
                cleanup_fn=cleanup,
                interval=30,
                max_iterations=None,
                reset_fork=False,
                managed_gateway=None,
            )

        assert code == 1

    def test_reset_fork_wires_pre_iteration_callback(self) -> None:
        runner = _make_fake_runner()
        runner.run_loop = AsyncMock()
        strategy = _make_fake_strategy()
        cleanup = AsyncMock()

        managed = MagicMock()
        managed.reset_anvil_forks = MagicMock(return_value=True)

        cli = CliRunner()
        with cli.isolation():
            run_helpers._run_continuous(
                runner=runner,
                strategy_instance=strategy,
                cleanup_fn=cleanup,
                interval=30,
                max_iterations=None,
                reset_fork=True,
                managed_gateway=managed,
            )

        kwargs = runner.run_loop.await_args.kwargs
        pre_cb = kwargs["pre_iteration_callback"]
        assert pre_cb is not None
        # Invoke it — should call reset_anvil_forks and not raise on success.
        pre_cb()
        managed.reset_anvil_forks.assert_called_once()

    def test_reset_fork_failure_raises_critical_callback_error(self) -> None:
        """pre_iteration_cb must raise CriticalCallbackError when reset fails."""
        from almanak.framework.runner.strategy_runner import CriticalCallbackError

        runner = _make_fake_runner()
        runner.run_loop = AsyncMock()
        strategy = _make_fake_strategy()
        cleanup = AsyncMock()

        managed = MagicMock()
        managed.reset_anvil_forks = MagicMock(return_value=False)

        cli = CliRunner()
        with cli.isolation():
            run_helpers._run_continuous(
                runner=runner,
                strategy_instance=strategy,
                cleanup_fn=cleanup,
                interval=30,
                max_iterations=None,
                reset_fork=True,
                managed_gateway=managed,
            )

        pre_cb = runner.run_loop.await_args.kwargs["pre_iteration_callback"]
        with pytest.raises(CriticalCallbackError):
            pre_cb()

    def test_reset_fork_with_none_managed_gateway_leaves_callback_none(self) -> None:
        """reset_fork=True but no managed gateway -> no callback wired."""
        runner = _make_fake_runner()
        runner.run_loop = AsyncMock()
        strategy = _make_fake_strategy()
        cleanup = AsyncMock()

        cli = CliRunner()
        with cli.isolation():
            run_helpers._run_continuous(
                runner=runner,
                strategy_instance=strategy,
                cleanup_fn=cleanup,
                interval=30,
                max_iterations=None,
                reset_fork=True,
                managed_gateway=None,
            )

        assert runner.run_loop.await_args.kwargs["pre_iteration_callback"] is None

    def test_cleanup_awaited_on_keyboard_interrupt(self) -> None:
        """KeyboardInterrupt path spins a fresh event loop for cleanup."""
        runner = _make_fake_runner()
        runner.run_loop = AsyncMock(side_effect=KeyboardInterrupt)
        strategy = _make_fake_strategy()
        cleanup_call_count = {"n": 0}

        async def _fake_cleanup() -> None:
            cleanup_call_count["n"] += 1

        cli = CliRunner()
        with cli.isolation():
            code = run_helpers._run_continuous(
                runner=runner,
                strategy_instance=strategy,
                cleanup_fn=_fake_cleanup,
                interval=30,
                max_iterations=None,
                reset_fork=False,
                managed_gateway=None,
            )

        assert code == 0
        # cleanup_fn is awaited once from run_loop_with_cleanup's finally
        # (which ran before KeyboardInterrupt propagated out of run_loop) AND
        # once from the outer except KeyboardInterrupt handler. Original
        # semantic: two calls.
        assert cleanup_call_count["n"] == 2

    def test_iteration_callback_echoes_formatted_result(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """on_iteration prints a formatted line per iteration."""
        runner = _make_fake_runner()

        # Capture the callback that run_loop is handed
        captured_cb: dict[str, Any] = {}

        async def _fake_run_loop(**kwargs: Any) -> None:
            captured_cb["cb"] = kwargs["iteration_callback"]

        runner.run_loop = AsyncMock(side_effect=_fake_run_loop)
        strategy = _make_fake_strategy()
        cleanup = AsyncMock()

        cli = CliRunner()
        with cli.isolation() as (out, _err):
            run_helpers._run_continuous(
                runner=runner,
                strategy_instance=strategy,
                cleanup_fn=cleanup,
                interval=30,
                max_iterations=None,
                reset_fork=False,
                managed_gateway=None,
            )
            # Now invoke the captured callback
            cb = captured_cb["cb"]
            cb(_make_success_result())
            out_text = out.getvalue().decode()

        # The formatted result string contains "SUCCESS"
        assert "SUCCESS" in out_text
