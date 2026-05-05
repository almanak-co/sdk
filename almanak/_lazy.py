"""Shared :pep:`562` lazy-attribute helper for package barrels.

Several ``__init__.py`` files in this codebase need to expose a flat namespace
of public names while keeping the underlying submodules off the import graph
until the names are actually accessed. This is load-bearing for the gateway
sidecar (``deploy/docker/strip-list-gateway.txt``) and for the dashboard image
(``platform/packages/backend/src/templates/strip-list-dashboard.txt``), both of
which delete heavy UI / backtesting dependencies that would otherwise be
eagerly imported via barrel re-exports. Regression guards live in
``tests/{gateway,framework/dashboard,framework/runner,framework/cli}/test_imports_lean.py``.

Before this helper, every barrel hand-rolled its own ``__getattr__`` and
``__dir__``. Three subtly-different variants drifted into the codebase
(string-vs-tuple specs, absolute-vs-relative module paths, divergent
``__dir__`` recipes) — exactly the maintenance smell this consolidation
removes. The helper was extracted as part of VIB-4048.

If a callsite needs richer dispatch (multi-table lookup, computed
fallbacks) — see ``almanak/__init__.py`` — keep its hand-rolled
``__getattr__``. This helper covers the common case.

Usage::

    # almanak/framework/dashboard/__init__.py
    from almanak._lazy import LazySpec, build_lazy_module_dispatch

    _LAZY_IMPORTS: dict[str, LazySpec] = {
        "render_pnl_section":         ".sections",
        "render_cost_stack_section":  ".sections",
        # Re-export under a different public name:
        "QuantStaleDataError":        (".exceptions", "StaleDataError"),
        # Absolute path also works (``importlib.import_module`` ignores
        # ``package=`` for absolute names):
        "BacktestRunner":             ("almanak.framework.backtesting.runner", "Runner"),
    }
    __getattr__, __dir__ = build_lazy_module_dispatch(
        _LAZY_IMPORTS, package=__name__, namespace=globals()
    )

This module is intentionally trivial. Its only imports are from the
standard library so that adding ``from almanak._lazy import ...`` to a
lean barrel does not regress the lean-import contract.
"""

from __future__ import annotations

import importlib
from collections.abc import Callable

LazySpec = str | tuple[str, str]
"""Spec for one lazy public name. Either:

- ``"submodule_path"`` — resolves to the attribute of the same name in
  ``submodule_path`` (e.g. ``"render_pnl_section": ".sections"`` →
  ``from .sections import render_pnl_section``).
- ``("submodule_path", "attr_name")`` — resolves to ``attr_name`` in
  ``submodule_path``, useful when re-exporting under a different public
  name (e.g. ``"QuantStaleDataError": (".exceptions", "StaleDataError")``).

Submodule paths may be relative (``".foo"``) or absolute
(``"almanak.framework.foo"``); ``importlib.import_module`` accepts both.
"""


def build_lazy_module_dispatch(
    lazy_imports: dict[str, LazySpec],
    *,
    package: str,
    namespace: dict[str, object],
) -> tuple[Callable[[str], object], Callable[[], list[str]]]:
    """Return ``(__getattr__, __dir__)`` for a lazy package barrel.

    Resolved attributes are cached in ``namespace`` (the caller's
    ``globals()``) so each lazy name pays the import cost at most once
    per process. Names absent from ``lazy_imports`` raise
    ``AttributeError`` with a Python-standard message so that
    ``hasattr()`` probes and ``from … import wrong_name`` failures
    behave the same as for an eagerly-defined module.

    The returned ``__dir__`` unions ``namespace`` (already-imported
    names plus dunders), ``lazy_imports`` (advertised but not yet
    resolved), and ``__all__`` if the caller defined one. This makes
    tab completion and ``inspect.getmembers`` work out of the box.
    """

    def __getattr__(name: str) -> object:
        if name not in lazy_imports:
            raise AttributeError(f"module {package!r} has no attribute {name!r}")
        spec = lazy_imports[name]
        if isinstance(spec, str):
            module_path, attr_name = spec, name
        else:
            module_path, attr_name = spec
        module = importlib.import_module(module_path, package=package)
        attr = getattr(module, attr_name)
        namespace[name] = attr
        return attr

    def __dir__() -> list[str]:
        out: set[str] = set(namespace) | set(lazy_imports)
        all_ = namespace.get("__all__")
        if isinstance(all_, list | tuple | set):
            out.update(all_)
        return sorted(out)

    return __getattr__, __dir__
