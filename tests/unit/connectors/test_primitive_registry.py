"""Tests for the connector-owned ``PrimitiveRegistry``.

Pins the contract that each connector declares its own ``Primitive`` + the
position-type alias strings it answers to, and that the registry resolves an
alias label to the owning connector's primitive. This registry is consumed by
the accounting position-state materializer, so its correctness is
accounting-critical.
"""

from __future__ import annotations

import importlib

import pytest

from almanak.connectors._strategy_base.primitive_registry import (
    PrimitiveDeclaration,
    PrimitiveRegistry,
    primitive_for_position_label,
)
from almanak.framework.primitives.types import Primitive


class TestDeclarationCompleteness:
    """Every loader entry resolves to a valid, non-empty declaration."""

    @pytest.mark.parametrize(
        "connector, module_path",
        sorted(PrimitiveRegistry._BUILTIN_LOADERS.items()),
    )
    def test_loader_module_exports_valid_declaration(self, connector: str, module_path: str) -> None:
        module = importlib.import_module(module_path)
        decl = getattr(module, "PRIMITIVE", None)
        assert isinstance(decl, PrimitiveDeclaration), f"{module_path} must export a PRIMITIVE: PrimitiveDeclaration"
        assert isinstance(decl.primitive, Primitive)
        assert decl.position_type_aliases, f"{module_path} declares no position_type_aliases"
        for alias in decl.position_type_aliases:
            assert isinstance(alias, str) and alias, f"{module_path} declares an invalid alias {alias!r}"

    def test_loader_module_path_matches_connector_folder(self) -> None:
        for connector, module_path in PrimitiveRegistry._BUILTIN_LOADERS.items():
            assert module_path == f"almanak.connectors.{connector}.primitive", (
                f"{connector!r} loader points at {module_path!r}; expected almanak.connectors.{connector}.primitive"
            )


class TestLabelResolution:
    """``primitive_for_label`` resolves protocol-alias labels correctly."""

    @pytest.mark.parametrize(
        "label, expected",
        [
            ("UNI_V3", Primitive.LP),
            ("UNISWAP_V3", Primitive.LP),
            ("AERODROME", Primitive.LP),
            ("AERODROME_LP", Primitive.LP),
            ("TRADERJOE_LP", Primitive.LP),
            ("UNI_V4", Primitive.LP_V4),
            ("UNISWAP_V4", Primitive.LP_V4),
            ("AAVE_V3", Primitive.LENDING),
            ("AAVE", Primitive.LENDING),
            ("MORPHO", Primitive.LENDING),
            ("MORPHO_BLUE", Primitive.LENDING),
            ("COMPOUND_V3", Primitive.LENDING),
            ("COMPOUND", Primitive.LENDING),
            ("GMX", Primitive.PERP),
            ("GMX_V2", Primitive.PERP),
            ("DRIFT", Primitive.PERP),
            ("HYPERLIQUID", Primitive.PERP),
            ("POLYMARKET", Primitive.PREDICTION),
        ],
    )
    def test_protocol_alias_resolves(self, label: str, expected: Primitive) -> None:
        assert PrimitiveRegistry.primitive_for_label(label) is expected
        assert primitive_for_position_label(label) is expected

    def test_case_and_whitespace_insensitive(self) -> None:
        assert PrimitiveRegistry.primitive_for_label("  aave_v3  ") is Primitive.LENDING
        assert PrimitiveRegistry.primitive_for_label("Gmx_V2") is Primitive.PERP

    def test_generic_labels_not_owned_by_registry(self) -> None:
        """Generic taxonomy labels (LP / LENDING / PERP / …) are NOT in the
        connector registry — they live in the taxonomy's generic table."""
        for generic in ("LP", "LENDING", "SUPPLY", "BORROW", "PERP", "VAULT", "CEX"):
            assert PrimitiveRegistry.primitive_for_label(generic) is None

    def test_unknown_label_returns_none(self) -> None:
        assert PrimitiveRegistry.primitive_for_label("DEFINITELY_NOT_A_PROTOCOL") is None


class TestNoCollisions:
    """No two connectors may claim the same alias label."""

    def test_label_map_builds_without_collision(self) -> None:
        # Building the map raises on a collision; a clean build proves none.
        label_map = PrimitiveRegistry.label_map()
        assert label_map  # non-empty
        # Spot-check a few entries survived the build.
        assert label_map["AAVE_V3"] is Primitive.LENDING
        assert label_map["UNI_V4"] is Primitive.LP_V4


class TestDeclarationFrozensetEnforcement:
    """``PrimitiveDeclaration`` coerces/validates ``position_type_aliases``.

    Without this, a connector author passing a bare string would silently
    register each character as an alias — an accounting-critical routing bug.
    """

    def test_bare_str_rejected(self) -> None:
        # A bare string is iterable; iterating it would register "D","R",...
        with pytest.raises(TypeError, match="bare"):
            PrimitiveDeclaration(
                primitive=Primitive.PERP,
                position_type_aliases="DRIFT",  # type: ignore[arg-type]
            )

    def test_bytes_rejected(self) -> None:
        with pytest.raises(TypeError):
            PrimitiveDeclaration(
                primitive=Primitive.PERP,
                position_type_aliases=b"DRIFT",  # type: ignore[arg-type]
            )

    def test_set_coerced_to_frozenset(self) -> None:
        decl = PrimitiveDeclaration(
            primitive=Primitive.PERP,
            position_type_aliases={"DRIFT", "DRIFT_V2"},  # type: ignore[arg-type]
        )
        assert isinstance(decl.position_type_aliases, frozenset)
        assert decl.position_type_aliases == frozenset({"DRIFT", "DRIFT_V2"})

    def test_list_coerced_to_frozenset(self) -> None:
        decl = PrimitiveDeclaration(
            primitive=Primitive.PERP,
            position_type_aliases=["DRIFT", "DRIFT"],  # type: ignore[arg-type]
        )
        assert isinstance(decl.position_type_aliases, frozenset)
        assert decl.position_type_aliases == frozenset({"DRIFT"})

    def test_frozenset_preserved(self) -> None:
        aliases = frozenset({"AAVE_V3", "AAVE"})
        decl = PrimitiveDeclaration(
            primitive=Primitive.LENDING,
            position_type_aliases=aliases,
        )
        assert decl.position_type_aliases == aliases

    def test_non_iterable_rejected(self) -> None:
        with pytest.raises(TypeError):
            PrimitiveDeclaration(
                primitive=Primitive.PERP,
                position_type_aliases=123,  # type: ignore[arg-type]
            )

    def test_non_string_member_rejected(self) -> None:
        with pytest.raises(TypeError):
            PrimitiveDeclaration(
                primitive=Primitive.PERP,
                position_type_aliases=frozenset({"DRIFT", 1}),  # type: ignore[arg-type]
            )

    def test_empty_string_member_rejected(self) -> None:
        with pytest.raises(TypeError):
            PrimitiveDeclaration(
                primitive=Primitive.PERP,
                position_type_aliases=frozenset({"DRIFT", ""}),  # type: ignore[arg-type]
            )


class TestBrokenConnectorIsolation:
    """A broken/missing connector is skipped; healthy ones still resolve.

    The module's broken-connector-isolation contract: building the aggregated
    map imports every connector per-connector in isolation, so one failed
    import (or invalid declaration) cannot poison lookups for unrelated
    connectors. Its own labels simply resolve to ``None``.
    """

    def test_broken_connector_skipped_others_resolve(self) -> None:
        # Inject a loader pointing at a non-importable module alongside the
        # real built-ins. The build must skip it (warning) and still index
        # every healthy connector's labels.
        original = dict(PrimitiveRegistry._BUILTIN_LOADERS)
        try:
            PrimitiveRegistry._BUILTIN_LOADERS = {
                **original,
                "definitely_broken": "almanak.connectors.definitely_broken.primitive",
            }
            PrimitiveRegistry.reset_cache()
            # Healthy connectors still resolve.
            assert PrimitiveRegistry.primitive_for_label("AAVE_V3") is Primitive.LENDING
            assert PrimitiveRegistry.primitive_for_label("UNI_V4") is Primitive.LP_V4
            assert PrimitiveRegistry.primitive_for_label("GMX_V2") is Primitive.PERP
        finally:
            PrimitiveRegistry._BUILTIN_LOADERS = original
            PrimitiveRegistry.reset_cache()

    def test_connector_with_invalid_declaration_skipped(self, tmp_path) -> None:
        # A loader whose module exists but exports a malformed PRIMITIVE
        # (caught by _load_declaration) is likewise skipped, not fatal.
        original = dict(PrimitiveRegistry._BUILTIN_LOADERS)
        try:
            # Point at a real module that has no PRIMITIVE attribute at all —
            # _load_declaration raises TypeError, which must be isolated.
            PrimitiveRegistry._BUILTIN_LOADERS = {
                **original,
                "no_primitive": "almanak.connectors._strategy_base.capabilities_registry",
            }
            PrimitiveRegistry.reset_cache()
            assert PrimitiveRegistry.primitive_for_label("AAVE_V3") is Primitive.LENDING
            # The broken loader contributed nothing.
            assert PrimitiveRegistry.primitive_for_label("CAPABILITIES_REGISTRY") is None
        finally:
            PrimitiveRegistry._BUILTIN_LOADERS = original
            PrimitiveRegistry.reset_cache()
