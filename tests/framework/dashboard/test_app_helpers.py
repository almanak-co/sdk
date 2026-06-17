"""Focused tests for the dashboard app coordinator helpers."""

from __future__ import annotations

from datetime import datetime, timedelta
from types import SimpleNamespace
from typing import Any

import pytest

from almanak.framework.dashboard import app


class _Expander:
    def __enter__(self) -> _Expander:
        return self

    def __exit__(self, *_exc: object) -> None:
        return None


class _SessionState(dict):
    def __getattr__(self, key: str) -> Any:
        try:
            return self[key]
        except KeyError as exc:
            raise AttributeError(key) from exc

    def __setattr__(self, key: str, value: Any) -> None:
        self[key] = value


class _StubStreamlit:
    def __init__(self) -> None:
        self.query_params: dict[str, str] = {}
        self.session_state = _SessionState()
        self.captions: list[str] = []
        self.errors: list[str] = []
        self.infos: list[str] = []
        self.codes: list[str] = []
        self.rerun_count = 0

    def error(self, message: str) -> None:
        self.errors.append(message)

    def info(self, message: str) -> None:
        self.infos.append(message)

    def code(self, message: str) -> None:
        self.codes.append(message)

    def caption(self, message: str) -> None:
        self.captions.append(message)

    def expander(self, _label: str) -> _Expander:
        return _Expander()

    def rerun(self) -> None:
        self.rerun_count += 1


@pytest.fixture
def stub_st(monkeypatch: pytest.MonkeyPatch) -> _StubStreamlit:
    stub = _StubStreamlit()
    monkeypatch.setattr(app, "st", stub)
    return stub


def _strategy(deployment_id: str, name: str = "Strategy") -> SimpleNamespace:
    return SimpleNamespace(id=deployment_id, name=name)


def test_current_strategy_index_returns_matching_strategy_offset() -> None:
    strategies = [_strategy("alpha"), _strategy("bravo"), _strategy("charlie")]

    assert app._current_strategy_index(strategies, None) == 0
    assert app._current_strategy_index(strategies, "missing") == 0
    assert app._current_strategy_index(strategies, "bravo") == 2


def test_sidebar_strategy_selection_routes_unknown_page_to_detail(stub_st: _StubStreamlit) -> None:
    strategies = [_strategy("deployment:abc12345")]
    stub_st.query_params["page"] = "library"

    app._handle_sidebar_strategy_selection(strategies, 1, None)

    assert stub_st.query_params["deployment_id"] == "deployment:abc12345"
    assert stub_st.query_params["page"] == "detail"
    assert stub_st.rerun_count == 1


def test_sidebar_strategy_selection_preserves_strategy_aware_page(stub_st: _StubStreamlit) -> None:
    strategies = [_strategy("deployment:abc12345")]
    stub_st.query_params["page"] = "timeline"

    app._handle_sidebar_strategy_selection(strategies, 1, None)

    assert stub_st.query_params["deployment_id"] == "deployment:abc12345"
    assert stub_st.query_params["page"] == "timeline"
    assert stub_st.rerun_count == 1


def test_sidebar_strategy_selection_same_strategy_is_noop(stub_st: _StubStreamlit) -> None:
    strategies = [_strategy("deployment:abc12345")]
    stub_st.query_params["deployment_id"] = "deployment:abc12345"

    app._handle_sidebar_strategy_selection(strategies, 1, "deployment:abc12345")

    assert stub_st.query_params["deployment_id"] == "deployment:abc12345"
    assert stub_st.rerun_count == 0


def test_sidebar_strategy_deselect_removes_deployment_id(stub_st: _StubStreamlit) -> None:
    strategies = [_strategy("deployment:abc12345")]
    stub_st.query_params["deployment_id"] = "deployment:abc12345"

    app._handle_sidebar_strategy_selection(strategies, 0, "deployment:abc12345")

    assert "deployment_id" not in stub_st.query_params
    assert stub_st.rerun_count == 1


@pytest.mark.parametrize(
    ("route", "expected"),
    [
        ("overview", "overview"),
        ("unknown", "overview"),
        ("config", "config"),
        ("timeline", "timeline"),
        ("detail", "detail"),
        ("teardown", "teardown"),
    ],
)
def test_render_page_route_dispatches_strategy_pages(
    monkeypatch: pytest.MonkeyPatch,
    route: str,
    expected: str,
) -> None:
    calls: list[tuple[str, Any]] = []
    strategies = [_strategy("deployment:abc12345")]

    monkeypatch.setattr(app.overview, "page", lambda s: calls.append(("overview", s)))
    monkeypatch.setattr(app.config_page, "page", lambda s: calls.append(("config", s)))
    monkeypatch.setattr(app.timeline, "page", lambda s: calls.append(("timeline", s)))
    monkeypatch.setattr(app.detail, "page", lambda s: calls.append(("detail", s)))
    monkeypatch.setattr(app.teardown, "page", lambda s: calls.append(("teardown", s)))

    app._render_page_route(route, [], strategies)

    assert calls == [(expected, strategies)]


def test_render_page_route_dispatches_library(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []

    monkeypatch.setattr(app.library, "page", lambda: calls.append("library"))

    app._render_page_route("library", [], [])

    assert calls == ["library"]


def test_custom_dashboard_route_requires_dashboard_name(
    monkeypatch: pytest.MonkeyPatch,
    stub_st: _StubStreamlit,
) -> None:
    calls: list[str] = []
    monkeypatch.setattr(app.overview, "page", lambda _strategies: calls.append("overview"))

    app._render_page_route("custom_dashboard", ["custom"], ["strategy"])

    assert stub_st.errors == ["No custom dashboard specified"]
    assert calls == ["overview"]


def test_custom_dashboard_route_dispatches_named_dashboard(
    monkeypatch: pytest.MonkeyPatch,
    stub_st: _StubStreamlit,
) -> None:
    calls: list[tuple[str, list[str], list[str]]] = []
    stub_st.query_params["custom_dashboard"] = "demo"

    def _render(name: str, custom_dashboards: list[str], strategies: list[str]) -> None:
        calls.append((name, custom_dashboards, strategies))

    monkeypatch.setattr(app, "render_custom_dashboard_page", _render)

    app._render_page_route("custom_dashboard", ["custom"], ["strategy"])

    assert calls == [("demo", ["custom"], ["strategy"])]


def test_render_current_page_re_raises_streamlit_control_flow(
    monkeypatch: pytest.MonkeyPatch,
    stub_st: _StubStreamlit,
) -> None:
    class FakeRerun(Exception):
        pass

    monkeypatch.setattr(app, "RerunException", FakeRerun)

    def _raise_rerun(*_args: object, **_kwargs: object) -> None:
        raise FakeRerun()

    monkeypatch.setattr(app, "_render_page_route", _raise_rerun)

    with pytest.raises(FakeRerun):
        app._render_current_page([], [])


def test_render_current_page_falls_back_to_overview_on_render_error(
    monkeypatch: pytest.MonkeyPatch,
    stub_st: _StubStreamlit,
) -> None:
    stub_st.query_params["page"] = "detail"

    def _raise_error(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("boom")

    monkeypatch.setattr(app, "_render_page_route", _raise_error)

    app._render_current_page([], [])

    assert stub_st.errors == ["Error rendering page 'detail': boom"]
    assert stub_st.infos == ["Returning to overview page..."]
    assert stub_st.query_params["page"] == "overview"
    assert stub_st.rerun_count == 1


def test_discover_custom_dashboards_safe_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(app, "CUSTOM_DASHBOARDS_AVAILABLE", False)

    assert app._discover_custom_dashboards_safe() == []


def test_discover_custom_dashboards_safe_swallows_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(app, "CUSTOM_DASHBOARDS_AVAILABLE", True)

    def _raise() -> list:
        raise RuntimeError("boom")

    monkeypatch.setattr(app, "discover_custom_dashboards", _raise)

    assert app._discover_custom_dashboards_safe() == []


def test_auto_refresh_caption_reruns_after_interval(
    stub_st: _StubStreamlit,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sleeps: list[int] = []
    monkeypatch.setattr(app, "_sleep", lambda seconds: sleeps.append(seconds))
    stub_st.session_state.last_refresh = datetime.now() - timedelta(seconds=31)
    stub_st.session_state.refresh_interval = 30

    app._render_auto_refresh_caption()

    assert stub_st.rerun_count == 1
    assert sleeps == []
    assert "Next in: 0s" in stub_st.captions[0]
    assert datetime.now() - stub_st.session_state.last_refresh < timedelta(seconds=1)


def test_auto_refresh_caption_schedules_countdown_rerun(
    stub_st: _StubStreamlit,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sleeps: list[int] = []
    monkeypatch.setattr(app, "_sleep", lambda seconds: sleeps.append(seconds))
    last_refresh = datetime.now()
    stub_st.session_state.last_refresh = last_refresh
    stub_st.session_state.refresh_interval = 30

    app._render_auto_refresh_caption()

    assert stub_st.rerun_count == 1
    assert sleeps == [1]
    assert stub_st.session_state.last_refresh == last_refresh
    assert "Auto-refresh ON" in stub_st.captions[0]


def test_load_dashboard_strategies_returns_empty_on_error(
    monkeypatch: pytest.MonkeyPatch,
    stub_st: _StubStreamlit,
) -> None:
    def _raise() -> list:
        raise RuntimeError("load failed")

    monkeypatch.setattr(app, "get_all_strategies", _raise)

    assert app._load_dashboard_strategies() == []
    assert stub_st.errors == ["Error loading strategies: load failed"]
    assert "RuntimeError: load failed" in stub_st.codes[0]
