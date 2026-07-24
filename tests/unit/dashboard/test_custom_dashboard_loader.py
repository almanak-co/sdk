"""Branch coverage for the custom dashboard loader.

Covers ``load_dashboard_module`` (cache hits, validation, spec/loader
failures, error wrapping), ``get_dashboard_render_function`` interface
checks, and the cache helpers. All against tmp_path-backed modules.
"""

from types import SimpleNamespace

import pytest

from almanak.framework.dashboard.custom.loader import (
    DashboardInterfaceError,
    DashboardLoadError,
    clear_module_cache,
    get_dashboard_render_function,
    invalidate_module,
    load_dashboard_module,
)

VALID_UI = "def render_custom_dashboard(deployment_id, strategy_config, api_client, session_state):\n    return deployment_id\n"


@pytest.fixture(autouse=True)
def _fresh_cache():
    clear_module_cache()
    yield
    clear_module_cache()


def _dashboard(tmp_path, source=VALID_UI):
    (tmp_path / "ui.py").write_text(source)
    return tmp_path


class TestLoadDashboardModule:
    def test_loads_module(self, tmp_path):
        module = load_dashboard_module(_dashboard(tmp_path), "my_strategy")
        assert callable(module.render_custom_dashboard)
        assert module.__name__ == "custom_dashboard_my_strategy"

    def test_cache_returns_same_module(self, tmp_path):
        path = _dashboard(tmp_path)
        first = load_dashboard_module(path, "my_strategy")
        second = load_dashboard_module(path, "my_strategy")
        assert first is second

    def test_cache_bypass_reloads(self, tmp_path):
        path = _dashboard(tmp_path)
        first = load_dashboard_module(path, "my_strategy")
        second = load_dashboard_module(path, "my_strategy", use_cache=False)
        assert first is not second

    def test_missing_file_raises(self, tmp_path):
        with pytest.raises(DashboardLoadError, match="not found"):
            load_dashboard_module(tmp_path, "my_strategy")

    def test_directory_named_ui_py_raises(self, tmp_path):
        (tmp_path / "ui.py").mkdir()
        with pytest.raises(DashboardLoadError, match="not a file"):
            load_dashboard_module(tmp_path, "my_strategy")

    def test_syntax_error_wrapped(self, tmp_path):
        path = _dashboard(tmp_path, source="def broken(:\n")
        with pytest.raises(DashboardLoadError, match="Syntax error"):
            load_dashboard_module(path, "my_strategy")

    def test_import_error_wrapped(self, tmp_path):
        path = _dashboard(tmp_path, source="import module_that_does_not_exist_anywhere\n")
        with pytest.raises(DashboardLoadError, match="Import error"):
            load_dashboard_module(path, "my_strategy")

    def test_runtime_error_wrapped(self, tmp_path):
        path = _dashboard(tmp_path, source="raise RuntimeError('boom at import')\n")
        with pytest.raises(DashboardLoadError, match="Error loading"):
            load_dashboard_module(path, "my_strategy")

    def test_none_spec_wrapped(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "almanak.framework.dashboard.custom.loader.importlib.util.spec_from_file_location",
            lambda name, path: None,
        )
        with pytest.raises(DashboardLoadError, match="module spec"):
            load_dashboard_module(_dashboard(tmp_path), "my_strategy")

    def test_none_loader_wrapped(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "almanak.framework.dashboard.custom.loader.importlib.util.spec_from_file_location",
            lambda name, path: SimpleNamespace(loader=None),
        )
        with pytest.raises(DashboardLoadError, match="No loader"):
            load_dashboard_module(_dashboard(tmp_path), "my_strategy")


class TestGetDashboardRenderFunction:
    def test_returns_render_function(self, tmp_path):
        module = load_dashboard_module(_dashboard(tmp_path), "my_strategy")
        render = get_dashboard_render_function(module)
        assert render("dep-1", {}, None, {}) == "dep-1"

    def test_missing_function_raises(self, tmp_path):
        module = load_dashboard_module(
            _dashboard(tmp_path, source="VALUE = 1\n"), "my_strategy"
        )
        with pytest.raises(DashboardInterfaceError, match="does not implement"):
            get_dashboard_render_function(module)

    def test_non_callable_raises(self, tmp_path):
        module = load_dashboard_module(
            _dashboard(tmp_path, source="render_custom_dashboard = 'nope'\n"),
            "my_strategy",
        )
        with pytest.raises(DashboardInterfaceError, match="not callable"):
            get_dashboard_render_function(module)


class TestCacheHelpers:
    def test_invalidate_module_evicts(self, tmp_path):
        path = _dashboard(tmp_path)
        first = load_dashboard_module(path, "my_strategy")
        invalidate_module(path)
        second = load_dashboard_module(path, "my_strategy")
        assert first is not second

    def test_invalidate_unknown_path_is_noop(self, tmp_path):
        invalidate_module(tmp_path / "never-loaded")
