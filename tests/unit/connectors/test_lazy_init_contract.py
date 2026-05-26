"""Parametrized contract test for VIB-4835 lazy connector inits.

Phase 2 of VIB-4835 converted every protocol connector's ``__init__.py``
to the PEP 562 lazy shape: a module-level ``_LAZY`` mapping, a
``__getattr__`` resolver, and a ``_register_once()`` helper that fires
once on first strategy-side attribute access. The shape is uniform across
all 39 connectors — instead of writing the same per-connector unit test
39 times, this single parametrised test exercises the contract once per
discovered lazy connector.

Contract:

1. Every entry in ``_LAZY`` resolves via ``__getattr__`` to a non-None object
   (the keys are guaranteed by the lazy template to point at real symbols
   in submodules).
2. After the first access, the resolved attribute is cached in the
   module's ``globals()`` so subsequent ``getattr`` calls bypass
   ``__getattr__`` (a runtime-perf invariant).
3. ``__getattr__`` raises ``AttributeError`` for names not in ``_LAZY``.
4. ``_register_once()`` is idempotent: a second invocation with
   ``_registered`` already True is a no-op (no side effects, no exception).

The first-call registration behaviour itself is exercised end-to-end by
``almanak.connectors._strategy_base.registry._import_all_connectors`` and
its associated CI gate (``scripts/ci/check_connector_registry.py``); we
deliberately do NOT re-test registration here because every connector
under test has already been registered at import time by the test runner
(pytest's collection touches the modules), so a fresh call to
``_register_once`` is a no-op by design.

Why this test exists: CodeRabbit auto-review on PR #2447 requested
per-connector unit tests for the lazy-init pattern across ~14 connectors.
Rather than ship 14 near-identical test files, this single parametric
test covers the contract for every connector that follows the lazy shape.
"""

from __future__ import annotations

import importlib
import pkgutil

import pytest


def _discover_lazy_connectors() -> list[str]:
    """Connectors whose ``__init__.py`` exposes ``_LAZY`` + ``_register_once``.

    The set is computed at collection time, not hard-coded, so a future
    connector added with the lazy shape is automatically covered without
    a test edit. Underscore-prefixed packages (``_base``,
    ``_strategy_base``) are foundation, not protocol leaves; skipped.
    """
    import almanak.connectors as conn_pkg

    found: list[str] = []
    for info in pkgutil.iter_modules(conn_pkg.__path__):
        if not info.ispkg or info.name.startswith("_"):
            continue
        try:
            mod = importlib.import_module(f"almanak.connectors.{info.name}")
        except Exception:  # noqa: BLE001
            continue
        if hasattr(mod, "_LAZY") and hasattr(mod, "_register_once"):
            found.append(info.name)
    return sorted(found)


LAZY_CONNECTORS = _discover_lazy_connectors()


def test_discovery_finds_the_expected_lazy_connectors() -> None:
    """Sanity: the discovery walks the package and finds a plausible count.

    A drop here likely signals one of two things:
    * The lazy shape was reverted somewhere (regression we want to catch).
    * The package layout moved again without updating this test.

    The threshold is conservative — Phase 2 shipped ~39 lazy connectors;
    the post-merge count may grow as new connectors are added.
    """
    assert len(LAZY_CONNECTORS) >= 30, (
        f"Only {len(LAZY_CONNECTORS)} lazy connectors discovered "
        f"(expected >= 30): {LAZY_CONNECTORS}"
    )


@pytest.mark.parametrize("connector", LAZY_CONNECTORS)
def test_lazy_attr_resolves_and_is_cached(connector: str) -> None:
    """Every ``_LAZY`` key resolves via ``__getattr__`` and is cached after.

    Iterates every entry — a typo in one cell would fail only when that
    specific symbol is requested, which is exactly the trap a connector's
    consumer would otherwise hit at runtime.
    """
    mod = importlib.import_module(f"almanak.connectors.{connector}")
    lazy_map = mod._LAZY
    assert lazy_map, f"{connector}._LAZY is empty"
    for name in lazy_map:
        # Pop any cached value left by earlier tests so we exercise the
        # ``__getattr__`` path, not the globals fast-path.
        mod.__dict__.pop(name, None)
        value = getattr(mod, name)
        assert value is not None, f"{connector}.{name} resolved to None"
        # Now cached in globals — second access must NOT hit __getattr__.
        assert name in mod.__dict__, f"{connector}.{name} not cached after first access"
        # Identity check: cached value matches the freshly-resolved one.
        assert mod.__dict__[name] is value


@pytest.mark.parametrize("connector", LAZY_CONNECTORS)
def test_unknown_attr_raises_attribute_error(connector: str) -> None:
    """Accessing a name not in ``_LAZY`` raises ``AttributeError``.

    The template uses a deliberate ``raise AttributeError(...)`` rather
    than ``KeyError`` so that ``hasattr`` works correctly. This pins the
    behaviour.
    """
    mod = importlib.import_module(f"almanak.connectors.{connector}")
    with pytest.raises(AttributeError, match="has no attribute"):
        _ = mod._NonExistentSymbol_VIB4835_  # type: ignore[attr-defined]


@pytest.mark.parametrize("connector", LAZY_CONNECTORS)
def test_register_once_is_idempotent(connector: str) -> None:
    """``_register_once()`` is a no-op when ``_registered`` is already True.

    Test runner has already touched every module by the time this test
    runs (pytest collection), so ``_registered`` is True. A second call
    must not raise (no double-register error) and must not perform side
    effects.

    We restore the pre-test ``_registered`` flag at the end so this test
    is order-insensitive vs other tests that depend on the lazy machinery.
    """
    mod = importlib.import_module(f"almanak.connectors.{connector}")
    saved = mod._registered
    try:
        mod._registered = True
        # Should return immediately without raising.
        result = mod._register_once()
        assert result is None  # the function returns None
        # Flag remains True (no spurious mutation).
        assert mod._registered is True
    finally:
        mod._registered = saved
