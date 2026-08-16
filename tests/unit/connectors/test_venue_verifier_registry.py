from __future__ import annotations

from typing import Any

import pytest

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import Connector, ImportRef, VenueVerifierDecl
from almanak.connectors._strategy_base.venue_verifier_registry import (
    VenueVerifierRegistry,
    VenueVerifierRegistryError,
)
from almanak.core.asset_identity import AssetIdentity, AssetNamespace
from almanak.core.chains.ethereum import DESCRIPTOR as ETHEREUM
from almanak.framework.primitives.types import Primitive
from almanak.framework.venues import (
    BaseVenueVerifier,
    VenueBindingComponent,
    VenueBindingFailure,
    VenueBindingFailureReason,
    VenueBindingFailureState,
    VenueObservedFact,
    VenueReferenceNamespace,
    VenueTargetRef,
    VenueTargetRole,
    VenueVerificationEvidence,
    VenueVerificationRequest,
    build_verified_venue_binding,
)

VERIFIER_REF = ImportRef("tests.unit.connectors.test_venue_verifier_registry", "ExampleVenueVerifier")
POOL = VenueTargetRef(
    VenueTargetRole.POOL,
    VenueReferenceNamespace.EVM_ADDRESS,
    "0x1111111111111111111111111111111111111111",
)
ROUTER = VenueTargetRef(
    VenueTargetRole.ROUTER,
    VenueReferenceNamespace.EVM_ADDRESS,
    "0x2222222222222222222222222222222222222222",
)
ASSETS = (
    AssetIdentity("ethereum", AssetNamespace.ERC20, "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
    AssetIdentity("ethereum", AssetNamespace.ERC20, "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
)
COMPONENTS = (VenueBindingComponent("fee_tier", "500"),)


class ExampleVenueVerifier(BaseVenueVerifier):
    def verify_venue(
        self,
        request: VenueVerificationRequest,
        gateway: Any,
        *,
        block_number: int | None = None,
    ) -> VenueBindingFailure:
        return VenueBindingFailure(
            VenueBindingFailureState.UNAVAILABLE,
            VenueBindingFailureReason.GATEWAY_UNAVAILABLE,
            "fixture does not perform I/O",
        )


def _declaration(
    protocol: str = "example_v3",
    verifier: ImportRef = VERIFIER_REF,
    *,
    contract_version: str = "example.exact_venue.v1",
    component_names: tuple[str, ...] = ("fee_tier",),
) -> VenueVerifierDecl:
    return VenueVerifierDecl(
        protocol=protocol,
        verifier=verifier,
        contract_version=contract_version,
        binding_policy_version=1,
        chains=(ETHEREUM,),
        primitives=(Primitive.LP,),
        component_names=component_names,
    )


def _connector(
    name: str = "example_v3",
    *,
    aliases: tuple[str, ...] = (),
    declaration: VenueVerifierDecl | None = None,
) -> Connector:
    declarations = () if declaration is None else (declaration,)
    return Connector(
        name=name,
        kind=ProtocolKind.LP,
        aliases=aliases,
        venue_verifiers=declarations,
    )


def _request() -> VenueVerificationRequest:
    return VenueVerificationRequest(
        chain="ethereum",
        protocol="example_v3",
        primitive=Primitive.LP,
        requested_refs=(POOL,),
        ordered_assets=ASSETS,
        binding_components=COMPONENTS,
        binding_policy_version=1,
    )


def _evidence() -> VenueVerificationEvidence:
    return VenueVerificationEvidence(
        chain="ethereum",
        verifier_ref="tests.unit.connectors.test_venue_verifier_registry:ExampleVenueVerifier",
        verifier_contract_version="example.exact_venue.v1",
        block_number=100,
        block_hash="0x" + "44" * 32,
        observed_facts=(VenueObservedFact("router", ROUTER.reference, ROUTER),),
    )


def _success():
    return build_verified_venue_binding(
        chain="ethereum",
        protocol="example_v3",
        primitive=Primitive.LP,
        identity_refs=(POOL,),
        binding_components=COMPONENTS,
        ordered_assets=ASSETS,
        binding_policy_version=1,
        operational_refs=(ROUTER,),
        evidence=_evidence(),
    )


def test_registry_is_lazy_exact_and_subset_scoped(monkeypatch: pytest.MonkeyPatch) -> None:
    connector = _connector(declaration=_declaration())
    monkeypatch.setattr(ImportRef, "load", lambda _self: pytest.fail("registry construction imported verifier"))
    registry = VenueVerifierRegistry((connector,))
    assert registry.supported_protocols() == ("example_v3",)
    assert registry.has("Example-V3")
    assert registry.declaration("example_v3") == connector.venue_verifiers[0]
    assert VenueVerifierRegistry(()).supported_protocols() == ()


def test_default_registry_matches_manifest_declarations_without_loading_providers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from almanak.connectors._connector import CONNECTOR_REGISTRY

    expected = tuple(
        sorted(
            declaration.protocol
            for connector in CONNECTOR_REGISTRY.with_venue_verifiers()
            for declaration in connector.venue_verifiers
        )
    )
    monkeypatch.setattr(ImportRef, "load", lambda _self: pytest.fail("registry construction imported verifier"))
    assert VenueVerifierRegistry().supported_protocols() == expected


def test_registry_loads_only_a_declared_base_verifier_subclass() -> None:
    registry = VenueVerifierRegistry((_connector(declaration=_declaration()),))
    assert registry.load_class("example_v3") is ExampleVenueVerifier

    bad = _connector(declaration=_declaration(verifier=ImportRef("builtins", "str")))
    with pytest.raises(VenueVerifierRegistryError, match="not a BaseVenueVerifier"):
        VenueVerifierRegistry((bad,)).load_class("example_v3")


def test_connector_rejects_duplicate_unordered_and_unowned_declarations() -> None:
    with pytest.raises(ValueError, match="duplicate protocol"):
        Connector(
            name="example_v3",
            kind=ProtocolKind.LP,
            venue_verifiers=(_declaration(), _declaration()),
        )
    other = _declaration("alias")
    with pytest.raises(ValueError, match="canonically ordered"):
        Connector(
            name="example_v3",
            kind=ProtocolKind.LP,
            aliases=("alias",),
            venue_verifiers=(_declaration(), other),
        )
    with pytest.raises(ValueError, match="not owned"):
        _connector(declaration=_declaration("unowned"))
    with pytest.raises(ValueError, match="canonical lowercase snake_case"):
        _declaration(component_names=("x" * 65,))


@pytest.mark.parametrize(
    "contract_version",
    (
        "",
        " example.v1",
        "example.v1 ",
        "Example.v1",
        ".example.v1",
        "example/v1",
        "exámple.v1",
    ),
)
def test_venue_verifier_declaration_rejects_noncanonical_contract_versions(contract_version: str) -> None:
    with pytest.raises(ValueError, match="contract_version"):
        _declaration(contract_version=contract_version)


def test_registry_rejects_duplicate_protocol_owners_across_injected_connectors() -> None:
    first = _connector("first", aliases=("shared",), declaration=_declaration("shared"))
    second = _connector("second", aliases=("shared",), declaration=_declaration("shared"))
    with pytest.raises(VenueVerifierRegistryError, match="declared by both"):
        VenueVerifierRegistry((first, second))


def test_registry_rejects_unknown_protocol_and_malformed_connector_inputs() -> None:
    registry = VenueVerifierRegistry((_connector(declaration=_declaration()),))
    with pytest.raises(VenueVerifierRegistryError, match="no exact venue verifier"):
        registry.declaration("unknown")
    with pytest.raises(TypeError, match="exact Connector"):
        VenueVerifierRegistry((object(),))  # type: ignore[arg-type]


def test_result_validation_binds_success_to_request_and_manifest_contract() -> None:
    registry = VenueVerifierRegistry((_connector(declaration=_declaration()),))
    assert registry.validate_result(_request(), _success()) == _success()

    wrong_request = VenueVerificationRequest(
        chain="ethereum",
        protocol="example_v3",
        primitive=Primitive.LP,
        requested_refs=(POOL,),
        ordered_assets=ASSETS,
        binding_components=(VenueBindingComponent("tick_spacing", "10"),),
        binding_policy_version=1,
    )
    with pytest.raises(VenueVerifierRegistryError, match="component schema"):
        registry.validate_result(wrong_request, _success())


def test_result_validation_rejects_provider_and_result_substitution() -> None:
    registry = VenueVerifierRegistry((_connector(declaration=_declaration()),))
    wrong_evidence = VenueVerificationEvidence(
        chain="ethereum",
        verifier_ref="almanak.connectors.other:Verifier",
        verifier_contract_version="example.exact_venue.v1",
        block_number=100,
        block_hash="0x" + "44" * 32,
        observed_facts=(VenueObservedFact("router", ROUTER.reference, ROUTER),),
    )
    wrong_success = build_verified_venue_binding(
        chain="ethereum",
        protocol="example_v3",
        primitive=Primitive.LP,
        identity_refs=(POOL,),
        binding_components=COMPONENTS,
        ordered_assets=ASSETS,
        binding_policy_version=1,
        operational_refs=(ROUTER,),
        evidence=wrong_evidence,
    )
    with pytest.raises(VenueVerifierRegistryError, match="wrong verifier"):
        registry.validate_result(_request(), wrong_success)


def test_descriptor_registry_exposes_only_connectors_with_venue_verifiers() -> None:
    from almanak.connectors._connector_descriptor import ConnectorRegistry

    with_verifier = _connector(declaration=_declaration())
    without = _connector("without")
    registry = ConnectorRegistry()
    registry._connectors = (without, with_verifier)
    assert registry.with_venue_verifiers() == (with_verifier,)
