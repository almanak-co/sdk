"""Tests for the custom dashboard renderer helpers and render entrypoint."""

import contextlib

import pytest

from almanak.framework.dashboard.custom import renderer
from almanak.framework.dashboard.custom.discoverer import CustomDashboardInfo
from almanak.framework.dashboard.custom.loader import DashboardInterfaceError, DashboardLoadError
from almanak.framework.dashboard.custom.renderer import _resolve_api_client, _spinner_label
from almanak.framework.dashboard.gateway_client import GatewayConnectionError

# --- _spinner_label ---


def test_spinner_label_appends_suffix_when_missing() -> None:
    assert _spinner_label("Aave Looping") == "Aave Looping dashboard"


def test_spinner_label_keeps_name_when_already_ends_in_dashboard() -> None:
    assert _spinner_label("Strategy Dashboard") == "Strategy Dashboard"


def test_spinner_label_suffix_check_is_case_insensitive() -> None:
    assert _spinner_label("My DASHBOARD") == "My DASHBOARD"


def test_spinner_label_strips_trailing_whitespace() -> None:
    assert _spinner_label("Strategy Dashboard  ") == "Strategy Dashboard"
    assert _spinner_label("Aave Looping ") == "Aave Looping dashboard"


def test_spinner_label_tolerates_missing_display_name() -> None:
    assert _spinner_label(None) == "dashboard"
    assert _spinner_label("") == "dashboard"
    assert _spinner_label("   ") == "dashboard"


# --- _resolve_api_client ---


class _FakeGatewayClient:
    """Stand-in for GatewayDashboardClient covering the connect paths."""

    def __init__(self, *, connected: bool, connect_raises: bool = False, connect_connects: bool = True) -> None:
        self.is_connected = connected
        self._connect_raises = connect_raises
        self._connect_connects = connect_connects
        self.connect_called = False

    def connect(self) -> None:
        self.connect_called = True
        if self._connect_raises:
            raise GatewayConnectionError("connect failed")
        if self._connect_connects:
            self.is_connected = True


@pytest.fixture
def patched_factories(monkeypatch):
    """Patch the client factories renderer uses; yields (real, mock) sentinels."""
    real_client = object()
    mock_client = object()
    monkeypatch.setattr(renderer, "create_api_client", lambda _gw, _sid: real_client)
    monkeypatch.setattr(renderer, "create_mock_api_client", lambda: mock_client)
    return real_client, mock_client


def test_resolve_returns_caller_supplied_client(patched_factories) -> None:
    existing = object()
    assert _resolve_api_client("strat", existing, None) is existing


def test_resolve_builds_from_gateway_client(patched_factories) -> None:
    real_client, _ = patched_factories
    assert _resolve_api_client("strat", None, object()) is real_client


def test_resolve_uses_connected_dashboard_client(patched_factories, monkeypatch) -> None:
    real_client, _ = patched_factories
    gw = _FakeGatewayClient(connected=True)
    monkeypatch.setattr("almanak.framework.dashboard.gateway_client.get_dashboard_client", lambda: gw)
    assert _resolve_api_client("strat", None, None) is real_client


def test_resolve_connects_then_uses_dashboard_client(patched_factories, monkeypatch) -> None:
    real_client, _ = patched_factories
    gw = _FakeGatewayClient(connected=False, connect_connects=True)
    monkeypatch.setattr("almanak.framework.dashboard.gateway_client.get_dashboard_client", lambda: gw)
    assert _resolve_api_client("strat", None, None) is real_client
    assert gw.connect_called


def test_resolve_falls_back_to_mock_on_connect_error(patched_factories, monkeypatch) -> None:
    _, mock_client = patched_factories
    gw = _FakeGatewayClient(connected=False, connect_raises=True)
    monkeypatch.setattr("almanak.framework.dashboard.gateway_client.get_dashboard_client", lambda: gw)
    assert _resolve_api_client("strat", None, None) is mock_client


def test_resolve_falls_back_to_mock_when_still_disconnected(patched_factories, monkeypatch) -> None:
    _, mock_client = patched_factories
    gw = _FakeGatewayClient(connected=False, connect_connects=False)
    monkeypatch.setattr("almanak.framework.dashboard.gateway_client.get_dashboard_client", lambda: gw)
    assert _resolve_api_client("strat", None, None) is mock_client


def test_resolve_falls_back_to_mock_on_unexpected_error(patched_factories, monkeypatch) -> None:
    _, mock_client = patched_factories

    def _boom():
        raise RuntimeError("kaboom")

    monkeypatch.setattr("almanak.framework.dashboard.gateway_client.get_dashboard_client", _boom)
    assert _resolve_api_client("strat", None, None) is mock_client


# --- render_custom_dashboard_safe ---


class _FakeSt:
    """Minimal streamlit stand-in: context-manager UI calls work, the rest no-op."""

    def __init__(self) -> None:
        self.session_state: dict = {}

    def spinner(self, *_a, **_k):
        return contextlib.nullcontext()

    def expander(self, *_a, **_k):
        return contextlib.nullcontext()

    def columns(self, n, *_a, **_k):
        return [contextlib.nullcontext() for _ in range(n)]

    def button(self, *_a, **_k) -> bool:
        return False

    def __getattr__(self, _name):
        return lambda *a, **k: None


@pytest.fixture
def fake_st(monkeypatch):
    st = _FakeSt()
    monkeypatch.setattr(renderer, "st", st)
    return st


def _dashboard_info(tmp_path) -> CustomDashboardInfo:
    return CustomDashboardInfo(
        strategy_name="demo_strat",
        dashboard_path=tmp_path,
        display_name="Strategy Dashboard",
    )


def test_render_happy_path_invokes_render_func(fake_st, monkeypatch, tmp_path) -> None:
    calls: dict = {}
    monkeypatch.setattr(renderer, "load_dashboard_module", lambda **_k: object())
    monkeypatch.setattr(renderer, "get_dashboard_render_function", lambda _m: lambda **k: calls.update(k))
    api = object()
    ok = renderer.render_custom_dashboard_safe(
        _dashboard_info(tmp_path), api_client=api, strategy_config={"x": 1}, session_state={"s": 2}
    )
    assert ok is True
    assert calls == {
        "deployment_id": "demo_strat",
        "strategy_config": {"x": 1},
        "api_client": api,
        "session_state": {"s": 2},
    }


def test_render_applies_defaults_when_args_omitted(fake_st, monkeypatch, tmp_path) -> None:
    calls: dict = {}
    monkeypatch.setattr(renderer, "load_dashboard_module", lambda **_k: object())
    monkeypatch.setattr(renderer, "get_dashboard_render_function", lambda _m: lambda **k: calls.update(k))
    ok = renderer.render_custom_dashboard_safe(_dashboard_info(tmp_path), api_client=object())
    assert ok is True
    assert calls["deployment_id"] == "demo_strat"
    assert calls["strategy_config"] == {}
    assert calls["session_state"] == {}


def test_render_returns_false_on_load_error(fake_st, monkeypatch, tmp_path) -> None:
    def _raise(**_k):
        raise DashboardLoadError("bad module")

    monkeypatch.setattr(renderer, "load_dashboard_module", _raise)
    assert renderer.render_custom_dashboard_safe(_dashboard_info(tmp_path), api_client=object()) is False


def test_render_returns_false_on_interface_error(fake_st, monkeypatch, tmp_path) -> None:
    def _raise(**_k):
        raise DashboardInterfaceError("missing render fn")

    monkeypatch.setattr(renderer, "load_dashboard_module", _raise)
    assert renderer.render_custom_dashboard_safe(_dashboard_info(tmp_path), api_client=object()) is False


def test_render_returns_false_on_runtime_error(fake_st, monkeypatch, tmp_path) -> None:
    def _render_func(**_k):
        raise RuntimeError("kaboom")

    monkeypatch.setattr(renderer, "load_dashboard_module", lambda **_k: object())
    monkeypatch.setattr(renderer, "get_dashboard_render_function", lambda _m: _render_func)
    assert renderer.render_custom_dashboard_safe(_dashboard_info(tmp_path), api_client=object()) is False


# --- _render_runtime_error: recovery buttons on BOTH branches (VIB-4047) ---


class _ButtonRecordingSt(_FakeSt):
    """``_FakeSt`` that records the ``key`` of every ``st.button`` rendered."""

    def __init__(self) -> None:
        super().__init__()
        self.button_keys: list[str] = []

    def button(self, *_a, key=None, **_k) -> bool:
        self.button_keys.append(key)
        return False


@pytest.fixture
def recording_st(monkeypatch):
    st = _ButtonRecordingSt()
    monkeypatch.setattr(renderer, "st", st)
    return st


def test_runtime_error_gateway_branch_still_offers_recovery_buttons(recording_st, monkeypatch, tmp_path) -> None:
    # A gateway auth/unreachable error routes to the LOUD banner and used to
    # `return` early — skipping the in-pane recovery buttons that the generic
    # branch offers. Regression guard for the fix (CodeRabbit): both branches
    # must render the same two recovery actions.
    banner: dict = {}
    monkeypatch.setattr(
        "almanak.framework.dashboard.error_ui.render_gateway_error",
        lambda *a, **k: banner.setdefault("shown", True),
    )
    # Text-classified as AUTH (not OTHER) → takes the gateway branch.
    renderer._render_runtime_error(_dashboard_info(tmp_path), Exception("gRPC status = UNAUTHENTICATED"))
    assert banner.get("shown") is True
    assert recording_st.button_keys == ["runtime_error_return", "runtime_error_retry"]


def test_runtime_error_generic_branch_offers_recovery_buttons(recording_st, tmp_path) -> None:
    # An unclassified (OTHER) error takes the generic branch and must still
    # render the recovery buttons.
    renderer._render_runtime_error(_dashboard_info(tmp_path), RuntimeError("kaboom"))
    assert recording_st.button_keys == ["runtime_error_return", "runtime_error_retry"]
