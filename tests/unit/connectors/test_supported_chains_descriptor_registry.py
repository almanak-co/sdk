"""Invariants for descriptor-registry connector support queries."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from almanak.connectors._connector import CONNECTOR_REGISTRY, SupportedChainsSpec
from almanak.core.chains.base import DESCRIPTOR as BASE
from almanak.core.chains.ethereum import DESCRIPTOR as ETHEREUM
from almanak.core.chains.polygon import DESCRIPTOR as POLYGON
from almanak.core.intent_types import IntentType

_REPO_ROOT = Path(__file__).resolve().parents[3]
_CHAINS_DIR = _REPO_ROOT / "almanak" / "core" / "chains"


def test_no_chain_file_names_a_connector() -> None:
    """Chain descriptors must not own the connector-to-chain relationship."""
    offenders: list[str] = []
    for path in sorted(_CHAINS_DIR.glob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            target_name = None
            if isinstance(node, ast.Assign):
                targets = node.targets
            elif isinstance(node, ast.AnnAssign):
                targets = [node.target]
            else:
                continue
            for target in targets:
                if isinstance(target, ast.Name):
                    target_name = target.id
                elif isinstance(target, ast.Attribute):
                    target_name = target.attr
                if target_name == "supported_protocols":
                    offenders.append(f"{path.name}:{node.lineno}: assigns supported_protocols")
    assert not offenders, (
        "core/chains/*.py must not own supported_protocols; put support inline "
        "on almanak/connectors/<protocol>/connector.py:\n  " + "\n  ".join(offenders)
    )


def test_compatibility_matrix_is_derived_from_descriptors() -> None:
    """Every matrix key traces to a descriptor, and every descriptor reaches the matrix.

    Comparing the matrix against a second call to the same builder would only
    prove the call is idempotent — it holds for any implementation, including
    one reading a hand-maintained table. These assert the two directions the
    name actually claims.
    """
    matrix = CONNECTOR_REGISTRY.supported_protocols_matrix()
    assert matrix

    onchain = [
        connector
        for connector in CONNECTOR_REGISTRY.with_strategy_support()
        if connector.supported_chains is not None and not connector.supported_chains.is_offchain
    ]
    assert onchain

    declared = {key for connector in onchain for key in connector.protocol_keys}
    unexplained = sorted(set(matrix) - declared)
    assert not unexplained, f"matrix keys with no owning descriptor: {unexplained}"

    missing = sorted(declared - set(matrix))
    assert not missing, f"on-chain descriptor keys absent from the matrix: {missing}"

    # Each row is that key's own coverage, not the connector-wide union — the
    # narrowing an alias override exists to express.
    for connector in onchain:
        for key in connector.protocol_keys:
            assert matrix[key] == set(connector.supported_chains_for_protocol(key)), key


def test_every_strategy_connector_has_exactly_one_inline_declaration() -> None:
    connectors = CONNECTOR_REGISTRY.with_strategy_support()
    assert connectors
    for connector in connectors:
        assert connector.supported_chains is not None, connector.name


def test_offchain_venues_are_excluded_from_runtime_protocol_map() -> None:
    matrix = CONNECTOR_REGISTRY.supported_protocols_matrix()
    assert "kraken" not in matrix
    assert CONNECTOR_REGISTRY.supported_chains_for("kraken") == ()


def test_unknown_protocol_query_is_empty() -> None:
    assert CONNECTOR_REGISTRY.get("definitely_not_a_protocol") is None
    assert CONNECTOR_REGISTRY.supported_chains_for("definitely_not_a_protocol") == ()


def test_fluid_intent_coverage_is_not_cross_product_widened() -> None:
    assert set(CONNECTOR_REGISTRY.supported_chains_for("fluid", intent="SWAP")) == {
        "arbitrum",
        "base",
        "ethereum",
        "polygon",
    }
    for intent in ("SUPPLY", "WITHDRAW"):
        assert set(CONNECTOR_REGISTRY.supported_chains_for("fluid", intent=intent)) == {
            "arbitrum",
            "base",
        }


def test_agni_override_does_not_widen_canonical_uniswap() -> None:
    """A protocol override REPLACES the alias's chain set; it never unions into canonical.

    Probed on a synthetic spec rather than on the production uniswap_v3 row: Agni
    and Uniswap V3 both have real Mantle deployments (different factory, position
    manager, quoter and router), so "mantle absent from uniswap_v3" would assert a
    coverage claim that is false, not the leak this test exists to catch.
    """
    spec = SupportedChainsSpec(chains=(ETHEREUM, BASE), protocol_overrides={"forky": (POLYGON,)})
    assert spec.chains_for_protocol("forky") == ("polygon",)
    # The override's chain must NOT appear in the canonical read...
    assert spec.chains_for_protocol("canonical_reader") == ("ethereum", "base")
    assert "polygon" not in spec.default_chains_union()
    # ...even though the deliberate cross-alias union still sees it.
    assert "polygon" in spec.all_chains()

    # Production: the alias resolves to exactly its own chain, strictly narrower
    # than the connector it shares a compiler with.
    agni = CONNECTOR_REGISTRY.supported_chains_for("agni_finance")
    uniswap = CONNECTOR_REGISTRY.supported_chains_for("uniswap_v3")
    assert agni == ("mantle",)
    assert set(agni) < set(uniswap)
    # linea has no intent suite for any uniswap_v3 verb, so it stays unpublished.
    assert "linea" not in uniswap


@pytest.mark.parametrize(
    "protocol",
    sorted(CONNECTOR_REGISTRY.supported_protocols_matrix()),
)
def test_every_onchain_protocol_and_alias_resolves(protocol: str) -> None:
    chains = CONNECTOR_REGISTRY.supported_chains_for(protocol)
    assert chains, protocol


def test_protocol_override_keys_are_stripped_before_normalisation() -> None:
    """A padded protocol override key reaches its clean spelling.

    Validation only tested ``raw_key.strip()`` for emptiness while storing the
    unstripped key, so ``" forky"`` was accepted and then never matched by
    ``protocol.lower()``. Intent override keys are canonical ``IntentType``
    members and therefore cannot carry whitespace.
    """
    spec = SupportedChainsSpec(
        chains=(ETHEREUM, BASE),
        intent_overrides={IntentType.LP_OPEN: (BASE,)},
        protocol_overrides={"  forky\t": (POLYGON,)},
    )
    assert spec.chains_for_intent("LP_OPEN") == ("base",)
    assert spec.chains_for_protocol("forky") == ("polygon",)
    assert spec.supports(chain="ethereum", intent="LP_OPEN") is False
    assert spec.supports(chain="base", intent="LP_OPEN") is True

    # Stripping happens BEFORE the duplicate check, so padded and clean
    # spellings of one key collide instead of both being stored.
    with pytest.raises(ValueError, match="duplicate key"):
        SupportedChainsSpec(
            chains=(ETHEREUM,),
            protocol_overrides={"forky": (ETHEREUM,), " forky": (ETHEREUM,)},
        )
