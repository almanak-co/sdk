from __future__ import annotations

from dataclasses import replace

import pytest

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import Connector, SupportedChainsSpec
from almanak.connectors._lifecycle_declaration_bundle import (
    LifecycleClaimCell,
    LifecycleDeclarationBundle,
)
from almanak.core.capability_obligations import (
    EvidenceKind,
    EvidenceRef,
    ObligationDeclaration,
    ObligationId,
    Satisfied,
)
from almanak.core.chains.arbitrum import DESCRIPTOR as ARBITRUM
from almanak.core.chains.ethereum import DESCRIPTOR as ETHEREUM
from almanak.core.intent_types import IntentType


def _satisfied(obligation: ObligationId) -> ObligationDeclaration:
    return ObligationDeclaration(
        obligation,
        Satisfied(
            provider_ref="almanak.example:Provider",
            contract_version="v1",
            test_evidence=(EvidenceRef(EvidenceKind.CONTRACT_TEST, "tests/example.py"),),
        ),
    )


def _bundle() -> LifecycleDeclarationBundle:
    return LifecycleDeclarationBundle(
        bundle_id="test-lending.ethereum-arbitrum",
        cells=(
            LifecycleClaimCell(protocol="test_lending", chain=ARBITRUM, intent=IntentType.BORROW),
            LifecycleClaimCell(protocol="test_lending", chain=ETHEREUM, intent=IntentType.SUPPLY),
        ),
        declarations=(
            _satisfied(ObligationId.ASSET_RESOLUTION),
            _satisfied(ObligationId.COMPILER),
        ),
        source_ref="Connector.lifecycle_declarations[test-lending.ethereum-arbitrum]",
        source_detail="Exact reviewed lending evidence.",
    )


def test_bundle_expands_to_exact_deterministic_flat_declarations() -> None:
    expanded = _bundle().expand()
    assert len(expanded) == 4
    assert [
        (item.protocol, item.chain, item.intent, item.declaration.obligation, item.rule_id) for item in expanded
    ] == [
        (
            "test_lending",
            "arbitrum",
            IntentType.BORROW,
            ObligationId.ASSET_RESOLUTION,
            "test-lending.ethereum-arbitrum.asset_resolution",
        ),
        (
            "test_lending",
            "arbitrum",
            IntentType.BORROW,
            ObligationId.COMPILER,
            "test-lending.ethereum-arbitrum.compiler",
        ),
        (
            "test_lending",
            "ethereum",
            IntentType.SUPPLY,
            ObligationId.ASSET_RESOLUTION,
            "test-lending.ethereum-arbitrum.asset_resolution",
        ),
        (
            "test_lending",
            "ethereum",
            IntentType.SUPPLY,
            ObligationId.COMPILER,
            "test-lending.ethereum-arbitrum.compiler",
        ),
    ]


def test_bundle_expansion_remains_subject_to_connector_scope_validation() -> None:
    connector = Connector(
        name="test_lending",
        kind=ProtocolKind.LENDING,
        strategy_intents=(IntentType.SUPPLY, IntentType.BORROW),
        supported_chains=SupportedChainsSpec(chains=(ETHEREUM, ARBITRUM)),
        lifecycle_declarations=_bundle().expand(),
    )
    assert len(connector.lifecycle_declarations) == 4

    with pytest.raises(ValueError, match="outside connector support"):
        replace(connector, supported_chains=SupportedChainsSpec(chains=(ETHEREUM,)))


@pytest.mark.parametrize(
    ("field", "value", "error"),
    [
        ("cells", (), "non-empty tuple"),
        ("declarations", (), "non-empty tuple"),
        ("cells", (_bundle().cells[0], _bundle().cells[0]), "duplicates"),
        (
            "declarations",
            (
                _satisfied(ObligationId.COMPILER),
                _satisfied(ObligationId.COMPILER),
            ),
            "duplicate obligations",
        ),
    ],
)
def test_bundle_rejects_empty_or_duplicate_axes(field: str, value: object, error: str) -> None:
    with pytest.raises(ValueError, match=error):
        replace(_bundle(), **{field: value})


def test_bundle_rejects_noncanonical_cell_or_obligation_order() -> None:
    bundle = _bundle()
    with pytest.raises(ValueError, match="cells must use canonical order"):
        replace(bundle, cells=tuple(reversed(bundle.cells)))
    with pytest.raises(ValueError, match="declarations must use canonical obligation order"):
        replace(bundle, declarations=tuple(reversed(bundle.declarations)))
