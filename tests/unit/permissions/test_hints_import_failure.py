"""``get_permission_hints`` must fail closed on a broken import (VIB-6018).

Before this, ``get_permission_hints`` swallowed every ``ImportError`` /
``ModuleNotFoundError`` and returned empty default hints. A typo or a broken
refactor anywhere in a connector's ``permission_hints`` import graph therefore
dropped that connector out of every derived membership set in
``synthetic_intents.py`` and produced an **empty Zodiac manifest** — the exact
VIB-5990 shape, with no error, no warning, and a green test suite.

An empty manifest is not a degraded result. It is a wrong one: every Safe-path
call for that connector reverts unauthorized, or (pre-VIB-6057) executes
completely unconstrained inside a MultiSend batch.

``get_discovery_vectors_override`` already drew the right line — module genuinely
absent → ``None``; nested import error → re-raise. These tests pin that the two
now agree, in both directions, so a connector cannot silently lose its
declarative hints while keeping its vector override.
"""

from __future__ import annotations

import importlib
import sys
import types

import pytest

from almanak.framework.permissions import hints as hints_mod
from almanak.framework.permissions.hints import (
    PermissionHints,
    PermissionHintsError,
    get_discovery_vectors_override,
    get_permission_hints,
)

_REAL_IMPORT = importlib.import_module


class TestGenuinelyAbsentModule:
    """No ``permission_hints`` module at all → empty defaults, no exception."""

    def test_unknown_protocol_returns_defaults(self) -> None:
        result = get_permission_hints("definitely_not_a_connector")
        assert isinstance(result, PermissionHints)
        assert result == PermissionHints()

    def test_unknown_protocol_has_no_vector_override(self) -> None:
        assert get_discovery_vectors_override("definitely_not_a_connector") is None


class TestNestedImportError:
    """A broken import INSIDE an existing module must surface, not degrade."""

    @staticmethod
    def _patched_import(monkeypatch: pytest.MonkeyPatch, exc: Exception) -> None:
        """Make importing curve's permission_hints raise ``exc``; leave the rest alone."""

        def fake_import(name: str, *args: object, **kwargs: object) -> object:
            if name == "almanak.connectors.curve.permission_hints":
                raise exc
            return _REAL_IMPORT(name, *args, **kwargs)

        monkeypatch.setattr(hints_mod.importlib, "import_module", fake_import)

    def test_nested_module_not_found_is_reraised(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A dependency of the hints module is missing — NOT the hints module itself."""
        self._patched_import(monkeypatch, ModuleNotFoundError("No module named 'some_dep'", name="some_dep"))
        with pytest.raises(ModuleNotFoundError):
            get_permission_hints("curve")

    def test_nested_import_error_is_reraised(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """``from x import y`` where ``y`` vanished — a renamed symbol upstream."""
        self._patched_import(monkeypatch, ImportError("cannot import name 'CURVE_POOLS'"))
        with pytest.raises(ImportError):
            get_permission_hints("curve")

    def test_missing_module_itself_still_degrades_gracefully(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The discriminator is ``exc.name``, not the exception type.

        When the *target* module is what's missing, the empty default is correct
        — most connectors have no ``permission_hints`` module and must not raise.
        """
        self._patched_import(
            monkeypatch,
            ModuleNotFoundError(
                "No module named 'almanak.connectors.curve.permission_hints'",
                name="almanak.connectors.curve.permission_hints",
            ),
        )
        assert get_permission_hints("curve") == PermissionHints()

    def test_missing_parent_package_still_degrades_gracefully(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self._patched_import(
            monkeypatch,
            ModuleNotFoundError("No module named 'almanak.connectors.curve'", name="almanak.connectors.curve"),
        )
        assert get_permission_hints("curve") == PermissionHints()


class TestAgreementWithVectorOverride:
    """The two loaders must draw the absent/broken line in the same place."""

    @pytest.mark.parametrize(
        "exc",
        [
            ModuleNotFoundError("No module named 'some_dep'", name="some_dep"),
            ModuleNotFoundError("No module named 'web3.contrib'", name="web3.contrib"),
        ],
    )
    def test_both_reraise_the_same_nested_failures(self, monkeypatch: pytest.MonkeyPatch, exc: Exception) -> None:
        def fake_import(name: str, *args: object, **kwargs: object) -> object:
            if name == "almanak.connectors.curve.permission_hints":
                raise exc
            return _REAL_IMPORT(name, *args, **kwargs)

        monkeypatch.setattr(hints_mod.importlib, "import_module", fake_import)
        with pytest.raises(ModuleNotFoundError):
            get_permission_hints("curve")
        with pytest.raises(ModuleNotFoundError):
            get_discovery_vectors_override("curve")


class TestWrongAttributeType:
    """A present-but-wrong ``PERMISSION_HINTS`` is a declaration bug, not an import one.

    **This assertion was inverted deliberately.** It previously asserted that a
    malformed export "falls back to defaults", pinning the fail-OPEN behaviour as
    though it were intended. It is not: an empty ``PermissionHints()`` drops the
    connector out of every derived membership set and yields a Zodiac manifest
    with zero core grants, so every Safe-path call for it reverts unauthorized —
    the same outcome VIB-6018 exists to prevent, reached through a different
    door. The dict shape used here is the realistic bad merge and the most
    dangerous one, because it is truthy and looks structured.

    The genuinely-absent-module case is unchanged and still returns the empty
    default; see the module-absent tests above.
    """

    def test_non_permission_hints_attribute_fails_closed(self, monkeypatch: pytest.MonkeyPatch) -> None:
        module = _REAL_IMPORT("almanak.connectors.curve.permission_hints")
        monkeypatch.setattr(module, "PERMISSION_HINTS", {"synthetic_discovery_intents": {"SWAP"}}, raising=True)
        with pytest.raises(PermissionHintsError, match="does not export a usable"):
            get_permission_hints("curve")


class TestAliasedProtocols:
    """The ``(connector, attribute)`` alias path must take the same branch."""

    def test_aliased_protocol_reraises_nested_failure(self, monkeypatch: pytest.MonkeyPatch) -> None:
        def fake_import(name: str, *args: object, **kwargs: object) -> object:
            if name == "almanak.connectors.aerodrome.permission_hints":
                raise ModuleNotFoundError("No module named 'some_dep'", name="some_dep")
            return _REAL_IMPORT(name, *args, **kwargs)

        monkeypatch.setattr(hints_mod.importlib, "import_module", fake_import)
        with pytest.raises(ModuleNotFoundError):
            get_permission_hints("aerodrome_slipstream")

    def test_aliased_protocol_resolves_its_own_attribute(self) -> None:
        """Sanity anchor: the alias path works at all, so the test above is meaningful."""
        assert "SWAP" in get_permission_hints("aerodrome_slipstream").synthetic_discovery_intents


class TestMalformedHintsFailClosed:
    """A module that EXISTS but exports no usable hints must raise, not default.

    Returning ``_DEFAULT`` here reintroduces the exact failure VIB-6018 closes,
    through a different door: the connector silently drops out of every derived
    membership set in ``synthetic_intents.py`` and its Zodiac manifest comes back
    empty, with a DEBUG line as the only trace. An empty manifest is not a
    degraded result — every Safe-path call for that connector reverts
    unauthorized.

    The "protocol genuinely has no hints module" case is different and still
    returns the empty default; that is covered by the module-absent tests above.
    """

    def _install(self, monkeypatch: pytest.MonkeyPatch, name: str, attr_value: object, sentinel: bool) -> None:
        """Register a fake connector module exporting ``attr_value`` (or nothing)."""
        module = types.ModuleType(f"almanak.connectors.{name}.permission_hints")
        if sentinel:
            module.PERMISSION_HINTS = attr_value  # type: ignore[attr-defined]
        monkeypatch.setitem(sys.modules, f"almanak.connectors.{name}.permission_hints", module)

    def test_attribute_missing_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self._install(monkeypatch, "fakeproto_missing", None, sentinel=False)
        with pytest.raises(PermissionHintsError, match="does not export a usable"):
            get_permission_hints("fakeproto_missing")

    @pytest.mark.parametrize("bogus", [{}, [], "PERMISSION_HINTS", 42, None])
    def test_attribute_wrong_type_raises(self, monkeypatch: pytest.MonkeyPatch, bogus: object) -> None:
        """A plain dict is the realistic bad merge — and the most dangerous,
        because it is truthy and looks structured."""
        self._install(monkeypatch, "fakeproto_wrong", bogus, sentinel=True)
        with pytest.raises(PermissionHintsError):
            get_permission_hints("fakeproto_wrong")

    def test_every_registered_connector_still_resolves(self) -> None:
        """Blast-radius guard: failing closed must break nobody today.

        Measured 2026-07-28 — all 44 registered protocols export a valid
        ``PermissionHints``. If this ever goes red, a connector lost its export
        and would previously have shipped an empty manifest in silence.

        **``isinstance(..., PermissionHints)`` alone is NOT the assertion.** A
        connector that loses its ``permission_hints.py`` entirely takes the
        ``ModuleNotFoundError`` branch and returns ``_DEFAULT`` — which *is* a
        ``PermissionHints`` — so that check stays green while the connector
        ships an empty manifest. It cannot fail for the reason it is cited for.
        The real assertion is that each protocol resolves to hints that are not
        the empty sentinel.
        """
        from almanak.connectors._strategy_base.registry import (
            ConnectorRegistry,
            _import_all_connectors,
        )
        from almanak.framework.permissions.hints import _DEFAULT, _PROTOCOL_CONNECTOR_MAP

        _import_all_connectors()
        protocols = sorted({m.name for m in ConnectorRegistry.all()} | set(_PROTOCOL_CONNECTOR_MAP))
        assert protocols, "connector registry is empty — the sweep would be vacuous"
        for protocol in protocols:
            hints = get_permission_hints(protocol)
            assert isinstance(hints, PermissionHints), protocol
            assert hints is not _DEFAULT, (
                f"{protocol} resolved to the empty default — its permission_hints module is "
                "missing or unexported, and it would ship a manifest with zero core grants"
            )

    def test_no_connector_silently_becomes_value_empty(self) -> None:
        """Identity (``is not _DEFAULT``) is NOT enough — value-emptiness is the real shape.

        ``is not _DEFAULT`` only catches a connector whose module was *deleted*.
        A connector that is *gutted* — ``PERMISSION_HINTS = PermissionHints()``
        after a bad merge — returns a distinct object that is value-equal to the
        default, so the identity check stays green while the manifest collapses
        to MultiSend-only with ``warnings: []``. That was demonstrated against
        ``uniswap_v3``, the most-used LP/SWAP connector, with every test in this
        file passing.

        So the assertion is value-based, with the connectors that are
        *deliberately* empty carried in an explicit list. Those 19 are the
        documented Class-B break set (``manifest-coverage-baseline.json``,
        tracked by VIB-6018 / VIB-6057): they authorise nothing today and that
        is known, recorded, and gated elsewhere. **This list must SHRINK.** A
        connector added to it is a connector whose Safe-path calls revert; a
        connector removed from it has been fixed.

        The point of the list is that any connector NOT on it — the other 25,
        including every V3-family LP connector — cannot silently become empty.
        """
        from almanak.connectors._strategy_base.registry import (
            ConnectorRegistry,
            _import_all_connectors,
        )
        from almanak.framework.permissions.hints import _PROTOCOL_CONNECTOR_MAP

        # Measured 2026-07-28. Every entry is a connector that ships a literal
        # ``PermissionHints()`` and is recorded in the coverage baseline or is
        # non-Safe-reachable (Solana / monad / zerog / off-chain venues).
        deliberately_empty = {
            "balancer_v2",
            "benqi",
            "curvance",
            "drift",
            "enso",
            "ethena",
            "euler_v2",
            "gimo",
            "jupiter",
            "kamino",
            "kraken",
            "lagoon",
            "lido",
            "lifi",
            "meteora",
            "orca",
            "raydium",
            "silo_v2",
            "stargate",
        }

        _import_all_connectors()
        protocols = sorted({m.name for m in ConnectorRegistry.all()} | set(_PROTOCOL_CONNECTOR_MAP))
        empty_now = {p for p in protocols if get_permission_hints(p) == PermissionHints()}

        newly_empty = sorted(empty_now - deliberately_empty)
        assert not newly_empty, (
            f"{newly_empty} now export empty PermissionHints(). Their Zodiac manifest collapses to "
            "infrastructure only, so every Safe-path call for them reverts unauthorized — silently, "
            "with warnings: []. Restore the declaration; do not add them to the allowlist."
        )

        no_longer_empty = sorted(deliberately_empty - empty_now)
        assert not no_longer_empty, (
            f"{no_longer_empty} now declare real hints — remove them from 'deliberately_empty' (and "
            "from manifest-coverage-baseline.json). This list must shrink, never silently rot."
        )


class TestMissingAlongPathDiscriminator:
    """The absent-vs-broken discriminator must compare dotted COMPONENTS.

    A raw ``module_path.startswith(exc.name)`` matches partial component names,
    so a broken nested import is misread as "connector has no hints module" and
    silently degrades to empty hints — the exact fail-open this module closes.
    """

    TARGET = "almanak.connectors.uniswap_v3.permission_hints"

    @pytest.mark.parametrize(
        "missing",
        [
            "almanak.connectors.uniswap_v3.permission_hints",  # the module itself
            "almanak.connectors.uniswap_v3",  # its package
            "almanak.connectors",
            "almanak",
        ],
    )
    def test_genuinely_absent_along_path_is_recognised(self, missing: str) -> None:
        assert hints_mod._is_missing_along_path(self.TARGET, missing) is True

    @pytest.mark.parametrize(
        "missing",
        [
            # Realistic: a refactor drops the version suffix. A raw prefix test
            # matches this and swallows the broken import.
            "almanak.connectors.uniswap",
            # Realistic: `from .permission import X` inside the hints module.
            "almanak.connectors.uniswap_v3.permission",
            "almanak.connectors.uniswap_v3.permission_hint",
            "some_third_party_dep",
            "",
            None,
        ],
    )
    def test_partial_component_or_foreign_module_is_not_absent(self, missing: str | None) -> None:
        assert hints_mod._is_missing_along_path(self.TARGET, missing) is False

    def test_nested_partial_component_import_error_reaches_the_caller(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """End-to-end: the swallow must not happen through ``get_permission_hints``."""

        def fake_import(name: str, *args: object, **kwargs: object) -> object:
            if name == "almanak.connectors.uniswap_v3.permission_hints":
                raise ModuleNotFoundError(
                    "No module named 'almanak.connectors.uniswap'",
                    name="almanak.connectors.uniswap",
                )
            return _REAL_IMPORT(name, *args, **kwargs)

        monkeypatch.setattr(hints_mod.importlib, "import_module", fake_import)
        with pytest.raises(ModuleNotFoundError):
            get_permission_hints("uniswap_v3")
