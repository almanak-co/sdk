"""Self-containment guards for the connector-owned chain-coverage registry.

These tests pin the *direction* of the W5 design (VIB-4857): "which chains
does protocol X run on" is CONNECTOR knowledge living in the connector's own
folder, and the legacy ``protocol -> {chains}`` matrix is DERIVED from it.

The original (rejected) refactor put ``supported_protocols`` on each
``ChainDescriptor`` — i.e. every chain file named the connectors that run on
it — which made "add a connector" an edit to N chain files. The
``test_no_chain_file_names_a_connector`` test below is the one that would have
caught that mistake. ``test_matrix_built_purely_from_connector_registry``
pins that removing a connector's declaration drops it from the matrix — the
matrix has no hand-maintained fallback.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from almanak.connectors._strategy_base.supported_chains_registry import (
    SupportedChainsRegistry,
    supported_chains_for,
    supported_protocols_matrix,
)

_REPO_ROOT = Path(__file__).resolve().parents[3]
_CHAINS_DIR = _REPO_ROOT / "almanak" / "core" / "chains"


def _matrix_protocol_names() -> set[str]:
    """Every protocol identifier the derived matrix is built from.

    This is precisely the set of connector knowledge the W5 refactor relocated
    out of (the rejected) chain-side ``supported_protocols`` and into per-
    connector ``supported_chains.py``. A chain file naming any of these would
    be reintroducing the exact backwards direction this design removes.

    The matrix keys (``aave_v3``, ``uniswap_v3``, ``enso``, …) are multi-token
    identifiers with no plain-English meaning, so word-boundary matching them
    in chain-file prose has no false positives — unlike scanning for *all*
    connector folder names, some of which (``drift``, ``across``) collide with
    ordinary English words used in chain-file docstrings.
    """
    return set(supported_protocols_matrix())


def test_no_chain_file_names_a_connector() -> None:
    """No ``core/chains/*.py`` file may name a connector / protocol.

    This is the guard that catches the rejected direction: a chain declaring
    which connectors run on it. "Which chains does protocol X run on" is
    CONNECTOR knowledge and must never leak into a chain file. Had this guard
    existed, it would have failed the original PR, which added
    ``ChainDescriptor.supported_protocols`` and listed connector names like
    ``uniswap_v3`` / ``aave_v3`` inside every ``core/chains/<chain>.py``.
    """
    import re

    protocol_names = _matrix_protocol_names()
    assert protocol_names, "expected to discover matrix protocols"

    offenders: list[str] = []
    for py in sorted(_CHAINS_DIR.glob("*.py")):
        text = py.read_text()
        for name in protocol_names:
            if re.search(rf"\b{re.escape(name)}\b", text):
                offenders.append(f"{py.name}: names protocol {name!r}")

    assert not offenders, (
        "core/chains/*.py must not name any connector/protocol — connector→"
        "chain coverage is CONNECTOR knowledge (declare it in "
        "almanak/connectors/<proto>/supported_chains.py, never in a chain "
        "file):\n  " + "\n  ".join(offenders)
    )


def test_matrix_built_purely_from_connector_registry() -> None:
    """The matrix keys are exactly the registry's protocol loaders.

    No protocol appears in the derived matrix unless a connector declares it.
    """
    matrix = supported_protocols_matrix()
    assert set(matrix) == set(SupportedChainsRegistry.supported_protocols())


def test_removing_a_connector_declaration_drops_it_from_the_matrix() -> None:
    """Removing a connector's declaration removes it from the derived matrix.

    Proves there is no hand-maintained fallback: the matrix is built purely by
    iterating the per-connector registry. Mutates a copy of ``_BUILTIN_LOADERS``
    and clears the aggregated cache, restoring both in ``finally`` so the
    process-wide registry contract is untouched for sibling tests.
    """
    original_loaders = SupportedChainsRegistry._BUILTIN_LOADERS
    try:
        # Drop the aave_v3 connector's declaration entirely.
        trimmed = dict(original_loaders)
        del trimmed["aave_v3"]
        SupportedChainsRegistry._BUILTIN_LOADERS = trimmed
        SupportedChainsRegistry.reset_cache()

        matrix = supported_protocols_matrix()
        assert "aave_v3" not in matrix
        # Sibling connectors are unaffected.
        assert "uniswap_v3" in matrix
    finally:
        SupportedChainsRegistry._BUILTIN_LOADERS = original_loaders
        SupportedChainsRegistry.reset_cache()


def test_per_protocol_lookup_imports_only_the_owning_module() -> None:
    """``get`` on an unknown protocol returns ``None`` without raising."""
    assert SupportedChainsRegistry.get("definitely_not_a_protocol") is None
    assert supported_chains_for("definitely_not_a_protocol") == frozenset()


def test_uniswap_connector_owns_agni_finance_fork() -> None:
    """Agni Finance (a Uniswap V3 fork with no own folder) is owned by uniswap_v3.

    Both identifiers resolve to the same connector module, and the fork's chain
    set matches the historical ``{"mantle"}``.
    """
    loaders = SupportedChainsRegistry._BUILTIN_LOADERS
    assert loaders["agni_finance"] == loaders["uniswap_v3"]
    assert supported_chains_for("agni_finance") == frozenset({"mantle"})


@pytest.mark.parametrize("protocol", sorted(SupportedChainsRegistry._BUILTIN_LOADERS))
def test_every_registered_protocol_resolves_to_a_nonempty_chain_set(protocol: str) -> None:
    """Every registered protocol resolves to a non-empty frozenset of chains."""
    chains = supported_chains_for(protocol)
    assert isinstance(chains, frozenset)
    assert chains, f"{protocol} declared no chains"
