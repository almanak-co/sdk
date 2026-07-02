"""Guard test: every connector position-read declaration is registry-complete.

VIB-5126 / VIB-5420 (D2) promoted the framework LP/vault valuer's two hardcoded
protocol-name dispatch sets — ``FungibleLpPositionReader._BOOTSTRAP`` and
``CurveLpPositionReader._SUPPORTED_PROTOCOLS`` — onto connector manifests, where
each capability-gated repricer is declared as
``position_read=PositionReadDecl(kind=..., builder=...)`` and dispatched through
``PositionReadRegistry``. Connector self-containment only holds if every such
declaration resolves cleanly AND the framework readers consult the registry
rather than naming a protocol. This test turns "declared a kind the reader
doesn't dispatch" or "re-introduced a hardcoded protocol set" into a CI failure.

It is the position-read sibling of
``test_flash_loan_registry_completeness.py`` / ``test_perps_read_registry``.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from almanak.connectors._connector import CONNECTOR_REGISTRY
from almanak.connectors._strategy_base.position_read_base import (
    BUILDER_REQUIRED_KINDS,
    CURVE_LP,
    FUNGIBLE_LP,
    POSITION_READ_KINDS,
)
from almanak.connectors._strategy_base.position_read_registry import PositionReadRegistry
from almanak.framework.valuation.curve_lp_position_reader import CurveLpPositionReader
from almanak.framework.valuation.fungible_lp_position_reader import FungibleLpPositionReader

_VALUATION_DIR = Path(__file__).resolve().parents[3] / "almanak" / "framework" / "valuation"

# Which framework reader owns each kind's ``.supports()`` dispatch. Used to
# prove the manifest declaration routes the position to the right reader (and
# ONLY that reader), byte-for-byte the behaviour the old hardcoded sets gave.
_READER_FOR_KIND = {
    FUNGIBLE_LP: FungibleLpPositionReader,
    CURVE_LP: CurveLpPositionReader,
}


def _connectors_with_position_read():
    return CONNECTOR_REGISTRY.with_position_read()


def test_discovery_finds_the_known_declarations() -> None:
    # Sanity-check discovery so a silently-empty registry can't make the
    # completeness assertions vacuously pass.
    names = {c.name for c in _connectors_with_position_read()}
    assert "curve" in names
    assert "fluid_dex_lp" in names


def test_reader_kind_map_is_closed() -> None:
    # Every recognised kind must have an owning framework reader (so a kind
    # constant cannot rot unwired), and every owning reader's kind is recognised.
    assert set(_READER_FOR_KIND) == set(POSITION_READ_KINDS)


def test_every_kind_is_declared_by_a_connector() -> None:
    # Every kind the framework supports is declared by at least one connector —
    # otherwise the reader is dead code with no position to mark.
    declared_kinds = {c.position_read.kind for c in _connectors_with_position_read()}
    assert declared_kinds == set(POSITION_READ_KINDS), (
        f"kinds declared on manifests {declared_kinds} != supported kinds {set(POSITION_READ_KINDS)}"
    )


@pytest.mark.parametrize(
    "name",
    sorted(c.name for c in CONNECTOR_REGISTRY.with_position_read()),
)
def test_position_read_decl_resolves_and_dispatches(name: str) -> None:
    connector = CONNECTOR_REGISTRY.get(name)
    assert connector is not None and connector.position_read is not None
    decl = connector.position_read

    # Kind is recognised and the registry agrees on dispatch.
    assert decl.kind in POSITION_READ_KINDS
    assert PositionReadRegistry.has(name)
    assert PositionReadRegistry.kind(name) == decl.kind
    assert PositionReadRegistry.canonical(name) == name

    # Builder presence matches the kind's contract, and resolves to a callable.
    if decl.kind in BUILDER_REQUIRED_KINDS:
        assert decl.builder is not None, f"{name}: kind={decl.kind} requires a builder"
        builder = PositionReadRegistry.builder(name)
        assert callable(builder), f"{name}: builder did not resolve to a callable"
    else:
        assert decl.builder is None, f"{name}: framework-valued kind={decl.kind} must not declare a builder"
        assert PositionReadRegistry.builder(name) is None

    # The declaration routes the position to its owning framework reader's
    # ``.supports()`` and to NO other reader — proving the manifest decl gives
    # exactly the dispatch the old hardcoded set did.
    for kind, reader_cls in _READER_FOR_KIND.items():
        supported = reader_cls().supports(name)
        assert supported is (kind == decl.kind), (
            f"{name}: {reader_cls.__name__}.supports() = {supported}, expected {kind == decl.kind}"
        )


def test_framework_readers_consult_the_registry_not_a_hardcoded_set() -> None:
    # Anti-bypass: the capability-gated readers must dispatch through the
    # registry, not re-introduce a hardcoded protocol-name set. (Sibling of
    # ``test_flash_loan_boot_file_has_no_concrete_connector_imports``.)
    import almanak.framework.valuation.curve_lp_position_reader as curve_mod
    import almanak.framework.valuation.fungible_lp_position_reader as fungible_mod

    # Source still references the registry (the dispatch seam), not a literal set.
    assert "PositionReadRegistry" in (_VALUATION_DIR / "fungible_lp_position_reader.py").read_text()
    assert "PositionReadRegistry" in (_VALUATION_DIR / "curve_lp_position_reader.py").read_text()

    # The retired hardcoded dispatch surfaces must be gone for good (runtime
    # check — immune to docstring mentions of the historical names).
    assert not hasattr(fungible_mod, "_BOOTSTRAP")
    assert not hasattr(fungible_mod, "_BUILDERS")
    assert not hasattr(fungible_mod, "register_fungible_lp_reader")
    assert not hasattr(CurveLpPositionReader, "_SUPPORTED_PROTOCOLS")
    assert not hasattr(curve_mod, "_SUPPORTED_PROTOCOLS")
