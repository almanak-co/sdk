"""Tests for file-path-based strategy discovery in _try_import_strategy."""

import sys
import textwrap
from pathlib import Path
from unittest.mock import patch

import pytest

from almanak.framework.strategies import _try_import_strategy


@pytest.fixture()
def strategy_dir(tmp_path):
    """Create a temporary strategy directory with a valid strategy module."""
    pkg = tmp_path / "almanak" / "demo_strategies" / "test_strat"
    pkg.mkdir(parents=True)
    (tmp_path / "almanak" / "__init__.py").touch()
    (tmp_path / "almanak" / "demo_strategies" / "__init__.py").touch()
    (pkg / "__init__.py").touch()
    (pkg / "strategy.py").write_text(
        textwrap.dedent("""\
            LOADED = True
        """)
    )
    return tmp_path


@pytest.fixture(autouse=True)
def _cleanup_sys_modules():
    """Remove test modules from sys.modules after each test."""
    before = set(sys.modules.keys())
    yield
    for key in list(sys.modules.keys()):
        if key not in before:
            del sys.modules[key]


class TestTryImportStrategyFilePath:
    """Tests for file_path-based loading in _try_import_strategy."""

    def test_loads_module_via_file_path(self, strategy_dir):
        module_name = "almanak.demo_strategies.test_strat.strategy"
        file_path = strategy_dir / "almanak" / "demo_strategies" / "test_strat" / "strategy.py"

        _try_import_strategy(module_name, file_path=file_path)

        assert module_name in sys.modules
        assert sys.modules[module_name].LOADED is True

    def test_creates_parent_packages_in_sys_modules(self, strategy_dir):
        module_name = "almanak.demo_strategies.test_strat.strategy"
        file_path = strategy_dir / "almanak" / "demo_strategies" / "test_strat" / "strategy.py"

        _try_import_strategy(module_name, file_path=file_path)

        assert "almanak" in sys.modules
        assert "almanak.demo_strategies" in sys.modules
        assert "almanak.demo_strategies.test_strat" in sys.modules

    def test_namespace_package_without_init(self, tmp_path):
        """Parent dirs without __init__.py get namespace package placeholders."""
        pkg = tmp_path / "ns_strats" / "my_strat"
        pkg.mkdir(parents=True)
        # No __init__.py files at all
        (pkg / "strategy.py").write_text("VALUE = 42\n")

        module_name = "ns_strats.my_strat.strategy"
        file_path = pkg / "strategy.py"

        _try_import_strategy(module_name, file_path=file_path)

        assert module_name in sys.modules
        assert sys.modules[module_name].VALUE == 42
        # Parent should exist as namespace package
        assert "ns_strats" in sys.modules

    def test_cleans_up_sys_modules_on_import_failure(self, tmp_path):
        """Failed imports should not leave poisoned entries in sys.modules."""
        pkg = tmp_path / "bad_strats" / "broken"
        pkg.mkdir(parents=True)
        (pkg / "strategy.py").write_text("raise RuntimeError('broken import')\n")

        module_name = "bad_strats.broken.strategy"
        file_path = pkg / "strategy.py"

        _try_import_strategy(module_name, file_path=file_path)

        # All entries should be cleaned up (module + all parent packages)
        assert module_name not in sys.modules
        assert "bad_strats.broken" not in sys.modules
        assert "bad_strats" not in sys.modules

    def test_idempotent_when_already_loaded(self, strategy_dir):
        """If leaf module is already in sys.modules, skip re-execution."""
        module_name = "almanak.demo_strategies.test_strat.strategy"
        file_path = strategy_dir / "almanak" / "demo_strategies" / "test_strat" / "strategy.py"

        # First import loads the module
        _try_import_strategy(module_name, file_path=file_path)
        assert module_name in sys.modules
        original_module = sys.modules[module_name]

        # Mutate module to detect re-execution
        original_module._CALL_COUNT = 1

        # Second import should be a no-op (idempotent)
        _try_import_strategy(module_name, file_path=file_path)

        # Module object should be the same (not re-created)
        assert sys.modules[module_name] is original_module
        # Attribute should still be there (module was NOT re-executed)
        assert sys.modules[module_name]._CALL_COUNT == 1

    def test_fallback_to_importlib_when_no_file_path(self):
        """When file_path is None, uses importlib.import_module."""
        with patch("almanak.framework.strategies.importlib.import_module") as mock_import:
            _try_import_strategy("some.fake.module")
            mock_import.assert_called_once_with("some.fake.module")

    def test_handles_missing_spec(self, tmp_path):
        """Gracefully handles case where spec_from_file_location returns None."""
        pkg = tmp_path / "no_spec"
        pkg.mkdir()
        strategy_file = pkg / "strategy.py"
        strategy_file.write_text("X = 1\n")

        with patch("almanak.framework.strategies.importlib.util.spec_from_file_location", return_value=None):
            _try_import_strategy("no_spec.strategy", file_path=strategy_file)

        assert "no_spec.strategy" not in sys.modules
        assert "no_spec" not in sys.modules
