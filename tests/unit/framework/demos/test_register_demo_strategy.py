"""Tests for ``almanak.framework.demos.spec.register_demo_strategy``.

The backtest CLI's demo resolution lane depends on three contracts:

- a demo name (directory slug or ``demo_``-prefixed form) imports the demo so
  ``@almanak_strategy`` registers it, and the decorator name is returned;
- the imported module STAYS in ``sys.modules`` so ``inspect.getfile`` on the
  registered class keeps working (sweep's ``ProcessPoolExecutor`` workers
  re-exec the strategy from that path, VIB-5624);
- failures (unknown name, broken demo, traversal-shaped input) return ``None``
  without raising, so CLI callers fall through to their normal error path.
"""

from __future__ import annotations

import inspect
import sys
import textwrap
from pathlib import Path

import pytest

from almanak.framework.demos.spec import register_demo_strategy
from almanak.framework.strategies import STRATEGY_REGISTRY, get_strategy, unregister_strategy

STRATEGY_TEMPLATE = textwrap.dedent(
    '''
    """Fixture demo used by test_register_demo_strategy.py."""
    from almanak.framework.intents import Intent
    from almanak.framework.strategies import IntentStrategy, almanak_strategy


    @almanak_strategy(name="{name}", supported_chains=["arbitrum"], default_chain="arbitrum")
    class FixtureDemoStrategy(IntentStrategy):
        def decide(self, market):
            return Intent.hold(reason="fixture")
    '''
).strip()


@pytest.fixture
def demos_root(tmp_path: Path) -> Path:
    root = tmp_path / "demos"
    root.mkdir()
    return root


def _make_demo(root: Path, dir_name: str, *, decorator_name: str | None = None, source: str | None = None) -> Path:
    demo_dir = root / dir_name
    demo_dir.mkdir()
    body = source if source is not None else STRATEGY_TEMPLATE.format(name=decorator_name or dir_name)
    (demo_dir / "strategy.py").write_text(body)
    return demo_dir


@pytest.fixture
def _registry_cleanup():
    """Remove fixture registrations and ad-hoc demo modules after each test."""
    before = set(STRATEGY_REGISTRY)
    yield
    for name in set(STRATEGY_REGISTRY) - before:
        unregister_strategy(name)
    for mod in [m for m in sys.modules if m.startswith("_almanak_demo_strategy_")]:
        sys.modules.pop(mod, None)


class TestRegisterDemoStrategy:
    def test_bare_slug_registers_and_returns_decorator_name(self, demos_root, _registry_cleanup) -> None:
        _make_demo(demos_root, "reg_demo_fixture_a", decorator_name="demo_reg_demo_fixture_a")

        name = register_demo_strategy("reg_demo_fixture_a", root=demos_root)

        assert name == "demo_reg_demo_fixture_a"
        assert name in STRATEGY_REGISTRY

    def test_demo_prefixed_input_matches_bare_directory(self, demos_root, _registry_cleanup) -> None:
        _make_demo(demos_root, "reg_demo_fixture_b", decorator_name="demo_reg_demo_fixture_b")

        name = register_demo_strategy("demo_reg_demo_fixture_b", root=demos_root)

        assert name == "demo_reg_demo_fixture_b"
        assert name in STRATEGY_REGISTRY

    def test_module_survives_for_inspect_getfile(self, demos_root, _registry_cleanup) -> None:
        # Negative control for sweep worker re-exec: if the loader evicted its
        # module (as DemoSpec.load's metadata reader does), this raises TypeError
        # and parallel sweeps of demos would break.
        demo_dir = _make_demo(demos_root, "reg_demo_fixture_c", decorator_name="reg_demo_fixture_c")

        name = register_demo_strategy("reg_demo_fixture_c", root=demos_root)

        assert name == "reg_demo_fixture_c"
        source = Path(inspect.getfile(get_strategy(name))).resolve()
        assert source == (demo_dir / "strategy.py").resolve()

    def test_unknown_name_returns_none(self, demos_root, _registry_cleanup) -> None:
        assert register_demo_strategy("does_not_exist", root=demos_root) is None

    def test_broken_demo_returns_none_and_cleans_sys_modules(self, demos_root, _registry_cleanup) -> None:
        _make_demo(demos_root, "reg_demo_fixture_broken", source="raise RuntimeError('boom at import')")

        assert register_demo_strategy("reg_demo_fixture_broken", root=demos_root) is None
        assert not any("reg_demo_fixture_broken" in m for m in sys.modules)

    def test_demo_without_decorator_returns_none(self, demos_root, _registry_cleanup) -> None:
        _make_demo(demos_root, "reg_demo_fixture_plain", source="X = 1\n")

        assert register_demo_strategy("reg_demo_fixture_plain", root=demos_root) is None

    @pytest.mark.parametrize(
        "name",
        ["../reg_demo_fixture_a", "a/b", "..", ".", "", ".hidden", "_private", "demo_"],
    )
    def test_non_slug_names_rejected(self, demos_root, _registry_cleanup, name: str) -> None:
        # Traversal-shaped or hidden/underscore names never touch the filesystem
        # beyond the demos root — mirrors what DemoCatalog.discover surfaces.
        _make_demo(demos_root, "reg_demo_fixture_a", decorator_name="reg_demo_fixture_a")

        assert register_demo_strategy(name, root=demos_root) is None


class TestRegisterShippedDemoUsesDottedModuleName:
    def test_shipped_demo_imports_under_real_package_name(self, _registry_cleanup) -> None:
        """Shipped demos must carry a child-process-importable __module__.

        BackgroundPaperTrader's multiprocessing child (spawn start method on
        macOS/Windows = fresh interpreter) and sweep workers re-import the
        strategy via importlib.import_module(strategy_cls.__module__). A
        synthetic spec_from_file_location name only survives fork, so the
        shipped-demos lane must import under the real dotted name.
        """
        name = register_demo_strategy("spark_lender")

        assert name == "demo_spark_lender"
        cls = get_strategy(name)
        assert cls.__module__ == "almanak.demo_strategies.spark_lender.strategy"
        # The exact call the child process makes must resolve this module.
        import importlib

        module = importlib.import_module(cls.__module__)
        assert getattr(module, cls.__name__) is cls
