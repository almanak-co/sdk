"""Same-name strategy re-registration: loud when classes come from different files.

``@almanak_strategy`` registration is first-wins. Before this guard, a second
registration under an existing name was skipped at debug level regardless of
where it came from — which is how a stale ``strategies/incubating/`` twin of a
demo silently shadowed (or was shadowed by) the demo for weeks.

The distinction under test:

- same source file imported twice under different module names (cwd lane,
  strategies-dir scan, demo loader all do this) → benign, stays debug;
- two DIFFERENT source files claiming one name → WARNING naming both files.
"""

from __future__ import annotations

import importlib.util
import logging
import sys
import textwrap
from pathlib import Path

import pytest

from almanak.framework.strategies import STRATEGY_REGISTRY, unregister_strategy

STRATEGY_SOURCE = textwrap.dedent(
    '''
    """Fixture strategy used by test_registration_shadow_warning.py."""
    from almanak.framework.intents import Intent
    from almanak.framework.strategies import IntentStrategy, almanak_strategy


    @almanak_strategy(name="{name}", supported_chains=["arbitrum"], default_chain="arbitrum")
    class {class_name}(IntentStrategy):
        def decide(self, market):
            return Intent.hold(reason="fixture")
    '''
).strip()


def _import_file(path: Path, module_name: str) -> None:
    spec = importlib.util.spec_from_file_location(module_name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)


@pytest.fixture
def _cleanup():
    before_registry = set(STRATEGY_REGISTRY)
    before_modules = set(sys.modules)
    yield
    for name in set(STRATEGY_REGISTRY) - before_registry:
        unregister_strategy(name)
    for mod in set(sys.modules) - before_modules:
        if mod.startswith("_shadow_test_"):
            sys.modules.pop(mod, None)


class TestRegistrationShadowWarning:
    def test_different_files_same_name_warns_loudly(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture, _cleanup
    ) -> None:
        name = "shadow_test_two_files"
        file_a = tmp_path / "a" / "strategy.py"
        file_b = tmp_path / "b" / "strategy.py"
        for f, cls in ((file_a, "StrategyA"), (file_b, "StrategyB")):
            f.parent.mkdir()
            f.write_text(STRATEGY_SOURCE.format(name=name, class_name=cls))

        _import_file(file_a, "_shadow_test_mod_a")
        with caplog.at_level(logging.WARNING, logger="almanak.framework.strategies.metadata"):
            _import_file(file_b, "_shadow_test_mod_b")

        assert STRATEGY_REGISTRY[name].__name__ == "StrategyA", "first registration must win"
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING and name in r.getMessage()]
        assert warnings, "re-registration by a different file must warn, not debug-skip"
        message = warnings[0].getMessage()
        assert str(file_a.resolve()) in message
        assert str(file_b.resolve()) in message

    def test_same_file_reimported_stays_quiet(self, tmp_path: Path, caplog: pytest.LogCaptureFixture, _cleanup) -> None:
        name = "shadow_test_one_file"
        file_a = tmp_path / "strategy.py"
        file_a.write_text(STRATEGY_SOURCE.format(name=name, class_name="StrategyOnce"))

        _import_file(file_a, "_shadow_test_mod_first")
        with caplog.at_level(logging.WARNING, logger="almanak.framework.strategies.metadata"):
            _import_file(file_a, "_shadow_test_mod_second")

        assert name in STRATEGY_REGISTRY
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING and name in r.getMessage()]
        assert not warnings, "double-import of one file is benign and must stay debug-level"
