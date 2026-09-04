"""Rendering contract tests for the Command Center custom-dashboard page."""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import pytest

from almanak.framework.dashboard import app


class _Context:
    def __enter__(self) -> _Context:
        return self

    def __exit__(self, *_exc: object) -> None:
        return None


class _StubStreamlit:
    def __init__(self, button_results: dict[str, bool] | None = None) -> None:
        self.query_params: dict[str, str] = {}
        self.session_state: dict[str, Any] = {"operator_filter": "active"}
        self._button_results = button_results or {}
        self.buttons: list[str] = []
        self.column_specs: list[list[int]] = []
        self.errors: list[str] = []
        self.markdowns: list[str] = []
        self.warnings: list[str] = []
        self.divider_count = 0

    def button(self, label: str) -> bool:
        self.buttons.append(label)
        return self._button_results.get(label, False)

    def columns(self, spec: list[int]) -> list[_Context]:
        self.column_specs.append(spec)
        return [_Context() for _ in spec]

    def divider(self) -> None:
        self.divider_count += 1

    def error(self, message: str) -> None:
        self.errors.append(message)

    def markdown(self, message: str) -> None:
        self.markdowns.append(message)

    def warning(self, message: str) -> None:
        self.warnings.append(message)


def _dashboard(strategy_name: str, *, display_name: str = "Demo Dashboard", icon: str | None = "D") -> Any:
    return SimpleNamespace(strategy_name=strategy_name, display_name=display_name, icon=icon)


def _strategy(
    deployment_id: str,
    *,
    name: str = "Demo Strategy",
    status: str = "RUNNING",
    total_value_usd: Decimal = Decimal("123.45"),
) -> Any:
    return SimpleNamespace(
        id=deployment_id,
        name=name,
        status=SimpleNamespace(value=status),
        total_value_usd=total_value_usd,
    )


@pytest.fixture
def render_calls(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(app, "render_custom_dashboard_safe", lambda **kwargs: calls.append(kwargs))
    return calls


@pytest.mark.parametrize("has_custom_param", [False, True])
def test_missing_dashboard_return_cleans_only_custom_route_when_clicked(
    monkeypatch: pytest.MonkeyPatch,
    render_calls: list[dict[str, Any]],
    has_custom_param: bool,
) -> None:
    stub = _StubStreamlit({"Return to Overview": True})
    stub.query_params["page"] = "custom_dashboard"
    if has_custom_param:
        stub.query_params["custom_dashboard"] = "missing"
    monkeypatch.setattr(app, "st", stub)

    app.render_custom_dashboard_page("missing", [_dashboard("other")], [])

    assert stub.errors == ["Custom dashboard not found: missing"]
    assert stub.buttons == ["Return to Overview"]
    assert stub.query_params == {"page": "overview"}
    assert stub.column_specs == []
    assert stub.markdowns == []
    assert stub.divider_count == 0
    assert render_calls == []


def test_missing_dashboard_without_return_is_render_only(
    monkeypatch: pytest.MonkeyPatch,
    render_calls: list[dict[str, Any]],
) -> None:
    stub = _StubStreamlit()
    stub.query_params.update(page="custom_dashboard", custom_dashboard="missing")
    monkeypatch.setattr(app, "st", stub)

    app.render_custom_dashboard_page("missing", [], [])

    assert stub.errors == ["Custom dashboard not found: missing"]
    assert stub.query_params == {"page": "custom_dashboard", "custom_dashboard": "missing"}
    assert render_calls == []


def test_matching_dashboard_and_exact_deployment_render_full_context(
    monkeypatch: pytest.MonkeyPatch,
    render_calls: list[dict[str, Any]],
) -> None:
    stub = _StubStreamlit()
    stub.query_params.update(page="custom_dashboard", custom_dashboard="demo")
    monkeypatch.setattr(app, "st", stub)
    selected_dashboard = _dashboard("demo", display_name="Authenticated Demo", icon="A")
    selected_strategy = _strategy(
        "demo",
        name="Authenticated Strategy",
        status="PAUSED",
        total_value_usd=Decimal("42.25"),
    )

    app.render_custom_dashboard_page(
        "demo",
        [_dashboard("other"), selected_dashboard],
        [_strategy("other"), selected_strategy],
    )

    assert stub.buttons == ["← Back"]
    assert stub.column_specs == [[1, 5]]
    assert stub.markdowns == ["### A Authenticated Demo"]
    assert stub.divider_count == 1
    assert stub.query_params == {"page": "custom_dashboard", "custom_dashboard": "demo"}
    assert len(render_calls) == 1
    assert render_calls[0] == {
        "dashboard_info": selected_dashboard,
        "deployment_id": "demo",
        "strategy_config": {
            "name": "Authenticated Strategy",
            "status": "PAUSED",
            "total_value": 42.25,
        },
        "api_client": None,
        "session_state": {"operator_filter": "active"},
    }
    assert render_calls[0]["session_state"] is not stub.session_state


def test_legacy_containing_deployment_match_and_back_cleanup(
    monkeypatch: pytest.MonkeyPatch,
    render_calls: list[dict[str, Any]],
) -> None:
    stub = _StubStreamlit({"← Back": True})
    stub.query_params.update(page="custom_dashboard", custom_dashboard="demo")
    monkeypatch.setattr(app, "st", stub)
    selected_dashboard = _dashboard("demo", icon=None)

    app.render_custom_dashboard_page(
        "demo",
        [selected_dashboard],
        [_strategy("unrelated"), _strategy("DemoStrategy:demo:123", name="Legacy Demo")],
    )

    assert stub.query_params == {"page": "overview"}
    assert stub.markdowns == ["### 📊 Demo Dashboard"]
    assert render_calls[0]["deployment_id"] == "DemoStrategy:demo:123"
    assert render_calls[0]["strategy_config"]["name"] == "Legacy Demo"


def test_first_authenticated_match_wins_when_multiple_deployments_match(
    monkeypatch: pytest.MonkeyPatch,
    render_calls: list[dict[str, Any]],
) -> None:
    stub = _StubStreamlit()
    stub.query_params.update(
        page="custom_dashboard",
        custom_dashboard="demo",
        deployment_id="DemoStrategy:demo:second",
    )
    monkeypatch.setattr(app, "st", stub)

    app.render_custom_dashboard_page(
        "demo",
        [_dashboard("demo")],
        [
            _strategy("DemoStrategy:demo:first", name="First Demo"),
            _strategy("DemoStrategy:demo:second", name="Selected Demo"),
        ],
    )

    assert render_calls[0]["deployment_id"] == "DemoStrategy:demo:first"
    assert render_calls[0]["strategy_config"]["name"] == "First Demo"
    assert stub.query_params["deployment_id"] == "DemoStrategy:demo:second"


def test_unmatched_strategy_uses_dashboard_identity_and_empty_config(
    monkeypatch: pytest.MonkeyPatch,
    render_calls: list[dict[str, Any]],
) -> None:
    stub = _StubStreamlit({"← Back": True})
    monkeypatch.setattr(app, "st", stub)
    selected_dashboard = _dashboard("demo")

    app.render_custom_dashboard_page("demo", [selected_dashboard], [_strategy("unrelated")])

    assert stub.query_params == {"page": "overview"}
    assert render_calls[0]["deployment_id"] == "demo"
    assert render_calls[0]["strategy_config"] == {}


def test_unavailable_renderer_warns_after_rendering_page_shell(monkeypatch: pytest.MonkeyPatch) -> None:
    stub = _StubStreamlit()
    monkeypatch.setattr(app, "st", stub)
    monkeypatch.setattr(app, "render_custom_dashboard_safe", None)

    app.render_custom_dashboard_page("demo", [_dashboard("demo")], [])

    assert stub.markdowns == ["### D Demo Dashboard"]
    assert stub.divider_count == 1
    assert stub.warnings == ["Custom dashboard rendering not available"]


def test_renderer_error_propagates_to_page_error_boundary(monkeypatch: pytest.MonkeyPatch) -> None:
    stub = _StubStreamlit()
    monkeypatch.setattr(app, "st", stub)

    def _raise(**_kwargs: Any) -> None:
        raise RuntimeError("render failed")

    monkeypatch.setattr(app, "render_custom_dashboard_safe", _raise)

    with pytest.raises(RuntimeError, match="render failed"):
        app.render_custom_dashboard_page("demo", [_dashboard("demo")], [])

    assert stub.markdowns == ["### D Demo Dashboard"]
    assert stub.divider_count == 1
