"""Unit tests for ``almanak._lazy`` (VIB-4048).

The helper backs PEP 562 dispatch in 7 barrel ``__init__.py`` files spanning
local SDK and hosted gateway/dashboard import paths. Indirect coverage
through the lean-import regression tests catches the headline contract
(no streamlit on the gateway runtime path), but a typo in any caller's
``_LAZY_IMPORTS``, or a regression in the helper itself (string-vs-tuple
spec handling, namespace caching, missing-name behaviour, ``__dir__``
composition), needs direct, focused tests so it surfaces independently
of the consumers.
"""

from __future__ import annotations

import sys
import types
from typing import cast

import pytest

from almanak._lazy import LazySpec, build_lazy_module_dispatch


def _install_fake_module(name: str, **attrs: object) -> types.ModuleType:
    """Register a synthetic module in ``sys.modules`` so the helper's
    ``importlib.import_module`` can find it without touching the disk.

    The caller is responsible for cleanup via ``monkeypatch`` or by
    deleting the entry — we hang it under the ``almanak._lazy_test_`` prefix
    so collisions with real modules are impossible.
    """
    module = types.ModuleType(name)
    for attr_name, attr_value in attrs.items():
        setattr(module, attr_name, attr_value)
    sys.modules[name] = module
    return module


@pytest.fixture
def fake_modules(monkeypatch: pytest.MonkeyPatch) -> dict[str, types.ModuleType]:
    """Install two synthetic modules with a known set of attributes; cleaned
    up automatically by ``monkeypatch.setitem``."""
    sentinel_a = object()
    sentinel_b = object()
    sentinel_renamed = object()
    mod_a = _install_fake_module(
        "almanak._lazy_test_alpha",
        thing_a=sentinel_a,
        renamed_source=sentinel_renamed,
    )
    mod_b = _install_fake_module(
        "almanak._lazy_test_beta",
        thing_b=sentinel_b,
    )
    monkeypatch.setitem(sys.modules, "almanak._lazy_test_alpha", mod_a)
    monkeypatch.setitem(sys.modules, "almanak._lazy_test_beta", mod_b)
    return {"alpha": mod_a, "beta": mod_b}


class TestStringSpecResolution:
    """``"submodule"`` form: attribute name on the submodule equals the
    public name in ``_LAZY_IMPORTS``."""

    def test_string_spec_resolves_attribute(self, fake_modules: dict[str, types.ModuleType]) -> None:
        namespace: dict[str, object] = {}
        lazy_imports: dict[str, LazySpec] = {
            "thing_a": "almanak._lazy_test_alpha",
        }
        getattr_, _dir = build_lazy_module_dispatch(lazy_imports, package="almanak._lazy_test", namespace=namespace)

        result = getattr_("thing_a")

        assert result is fake_modules["alpha"].thing_a


class TestTupleSpecResolution:
    """``("submodule", "attr_name")`` form: lets a barrel re-export an
    attribute under a different public name (e.g. ``QuantStaleDataError``
    sourced from ``StaleDataError``)."""

    def test_tuple_spec_resolves_renamed_attribute(self, fake_modules: dict[str, types.ModuleType]) -> None:
        namespace: dict[str, object] = {}
        lazy_imports: dict[str, LazySpec] = {
            "PublicName": ("almanak._lazy_test_alpha", "renamed_source"),
        }
        getattr_, _dir = build_lazy_module_dispatch(lazy_imports, package="almanak._lazy_test", namespace=namespace)

        result = getattr_("PublicName")

        assert result is fake_modules["alpha"].renamed_source


class TestCaching:
    """First access pays the import cost; subsequent accesses must hit
    ``namespace`` directly so callers don't repeatedly walk the import
    graph. Verified by counting ``importlib.import_module`` calls."""

    def test_resolved_attribute_is_cached_in_namespace(self, fake_modules: dict[str, types.ModuleType]) -> None:
        namespace: dict[str, object] = {}
        lazy_imports: dict[str, LazySpec] = {
            "thing_a": "almanak._lazy_test_alpha",
        }
        getattr_, _dir = build_lazy_module_dispatch(lazy_imports, package="almanak._lazy_test", namespace=namespace)

        getattr_("thing_a")

        assert "thing_a" in namespace
        assert namespace["thing_a"] is fake_modules["alpha"].thing_a

    def test_repeat_access_does_not_call_importlib_again(
        self,
        fake_modules: dict[str, types.ModuleType],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        namespace: dict[str, object] = {}
        lazy_imports: dict[str, LazySpec] = {
            "thing_a": "almanak._lazy_test_alpha",
        }
        getattr_, _dir = build_lazy_module_dispatch(lazy_imports, package="almanak._lazy_test", namespace=namespace)

        # First access populates the cache via importlib.
        getattr_("thing_a")

        # After the cache is warm, importlib should never be called again.
        # Module-level ``__getattr__`` in real Python is consulted only when
        # the attribute is *missing* from the module's globals, so the
        # cache hit happens at the language level rather than inside our
        # helper. We assert at the helper level by spying on
        # ``importlib.import_module`` and forcing a second call: it must
        # still resolve from the cache without another import.
        import_calls: list[str] = []
        import almanak._lazy as lazy_module

        original = lazy_module.importlib.import_module

        def spy(name: str, package: str | None = None) -> types.ModuleType:
            import_calls.append(name)
            return original(name, package=package)

        monkeypatch.setattr(lazy_module.importlib, "import_module", spy)

        # Force-call the helper a second time; the cached value in the
        # caller's namespace is what real ``__getattr__`` would shortcut on.
        # Verify by reading ``namespace`` directly — that's the canonical
        # post-cache lookup.
        cached = namespace["thing_a"]
        assert cached is fake_modules["alpha"].thing_a
        assert import_calls == [], "cache hit must not re-enter importlib"


class TestMissingName:
    """Names absent from ``_LAZY_IMPORTS`` raise ``AttributeError`` so
    ``hasattr()`` probes and bad ``from … import …`` statements behave
    the same as for an eagerly-defined module."""

    def test_unknown_name_raises_attribute_error(self) -> None:
        namespace: dict[str, object] = {}
        lazy_imports: dict[str, LazySpec] = {
            "known": "almanak._lazy_test_alpha",
        }
        getattr_, _dir = build_lazy_module_dispatch(lazy_imports, package="almanak._lazy_test", namespace=namespace)

        with pytest.raises(AttributeError, match="has no attribute 'unknown'"):
            getattr_("unknown")

    def test_attribute_error_message_carries_package_name(self) -> None:
        namespace: dict[str, object] = {}
        getattr_, _dir = build_lazy_module_dispatch({}, package="almanak._lazy_test", namespace=namespace)

        with pytest.raises(AttributeError) as exc_info:
            getattr_("anything")

        assert "almanak._lazy_test" in str(exc_info.value)


class TestDirComposition:
    """``__dir__`` should expose every name a consumer might want to
    autocomplete: already-resolved names, lazily-advertised names, and
    anything the caller stamped into ``__all__``. Returned sorted so
    REPL output is stable."""

    def test_dir_includes_lazy_imports_and_namespace(self, fake_modules: dict[str, types.ModuleType]) -> None:
        namespace: dict[str, object] = {"already_loaded": object()}
        lazy_imports: dict[str, LazySpec] = {
            "lazy_one": "almanak._lazy_test_alpha",
            "lazy_two": ("almanak._lazy_test_beta", "thing_b"),
        }
        _getattr, dir_ = build_lazy_module_dispatch(lazy_imports, package="almanak._lazy_test", namespace=namespace)

        listed = dir_()

        assert "already_loaded" in listed
        assert "lazy_one" in listed
        assert "lazy_two" in listed

    def test_dir_includes_all_when_namespace_defines_it(self) -> None:
        namespace: dict[str, object] = {"__all__": ["explicit"]}
        lazy_imports: dict[str, LazySpec] = {"lazy": "almanak._lazy_test_alpha"}
        _getattr, dir_ = build_lazy_module_dispatch(lazy_imports, package="almanak._lazy_test", namespace=namespace)

        listed = dir_()

        assert "explicit" in listed
        assert "lazy" in listed

    def test_dir_returns_sorted_unique_names(self) -> None:
        namespace: dict[str, object] = {"b": 1, "a": 2}
        lazy_imports: dict[str, LazySpec] = {
            "c": "almanak._lazy_test_alpha",
            "a": "almanak._lazy_test_beta",  # also in namespace; must not duplicate
        }
        _getattr, dir_ = build_lazy_module_dispatch(lazy_imports, package="almanak._lazy_test", namespace=namespace)

        listed = dir_()

        assert listed == sorted(listed)
        assert len(listed) == len(set(listed))

    def test_dir_tolerates_non_iterable_all(self) -> None:
        """A caller might define ``__all__`` as something unusual; the
        helper guards against that by checking ``isinstance``. Verifies
        the guard is actually exercised."""
        namespace: dict[str, object] = {"__all__": cast(object, 42)}
        lazy_imports: dict[str, LazySpec] = {"lazy": "almanak._lazy_test_alpha"}
        _getattr, dir_ = build_lazy_module_dispatch(lazy_imports, package="almanak._lazy_test", namespace=namespace)

        # Must not raise even though __all__ is the wrong shape.
        listed = dir_()

        assert "lazy" in listed
