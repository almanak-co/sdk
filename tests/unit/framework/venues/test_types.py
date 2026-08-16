from __future__ import annotations

import hashlib

import pytest

from almanak.core.asset_identity import AssetIdentity, AssetNamespace
from almanak.framework.primitives.types import Primitive
from almanak.framework.venues import (
    ExactVenueBinding,
    VenueBindingComponent,
    VenueBindingFailure,
    VenueBindingFailureReason,
    VenueBindingFailureState,
    VenueObservedFact,
    VenueReferenceNamespace,
    VenueTargetRef,
    VenueTargetRole,
    VenueVerificationEvidence,
    VerifiedVenueBinding,
    build_verified_venue_binding,
)
from almanak.framework.venues.verifier import VenueVerificationRequest

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
FACTORY = VenueTargetRef(
    VenueTargetRole.FACTORY,
    VenueReferenceNamespace.EVM_ADDRESS,
    "0x3333333333333333333333333333333333333333",
)
USDC = AssetIdentity("ethereum", AssetNamespace.ERC20, "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
WETH = AssetIdentity("ethereum", AssetNamespace.ERC20, "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
COMPONENTS = (VenueBindingComponent("fee_tier", "500"),)


def _evidence(*, block_number: int = 20, router: VenueTargetRef = ROUTER) -> VenueVerificationEvidence:
    return VenueVerificationEvidence(
        chain="ethereum",
        verifier_ref="almanak.connectors.example.venue_verifier:ExampleVenueVerifier",
        verifier_contract_version="example.exact_venue.v1",
        block_number=block_number,
        block_hash="0x" + "44" * 32,
        observed_facts=(VenueObservedFact("router", router.reference, router),),
    )


def _verified(
    *,
    identity_refs: tuple[VenueTargetRef, ...] = (POOL,),
    components: tuple[VenueBindingComponent, ...] = COMPONENTS,
    assets: tuple[AssetIdentity, ...] = (USDC, WETH),
    operational_refs: tuple[VenueTargetRef, ...] = (ROUTER,),
    evidence: VenueVerificationEvidence | None = None,
) -> VerifiedVenueBinding:
    return build_verified_venue_binding(
        chain="ethereum",
        protocol="example_v3",
        primitive=Primitive.LP,
        identity_refs=identity_refs,
        binding_components=components,
        ordered_assets=assets,
        binding_policy_version=1,
        operational_refs=operational_refs,
        evidence=evidence or _evidence(),
    )


def test_exact_binding_has_a_golden_canonical_preimage_and_hash() -> None:
    result = _verified()
    expected = (
        b'{"bindingComponents":[{"name":"fee_tier","value":"500"}],'
        b'"bindingPolicyVersion":1,"chain":"ethereum",'
        b'"identityRefs":[{"reference":"0x1111111111111111111111111111111111111111",'
        b'"referenceNamespace":"evm_address","role":"pool"}],'
        b'"orderedAssets":[{"assetNamespace":"erc20",'
        b'"assetReference":"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",'
        b'"chain":"ethereum","schemaVersion":1},{"assetNamespace":"erc20",'
        b'"assetReference":"0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",'
        b'"chain":"ethereum","schemaVersion":1}],"primitive":"lp",'
        b'"protocol":"example_v3","schemaVersion":1}'
    )
    assert result.binding.canonical_preimage_bytes() == expected
    assert result.binding.binding_hash == hashlib.sha256(expected).hexdigest()


def test_operational_endpoints_and_observation_block_do_not_change_binding_hash() -> None:
    first = _verified()
    second_router = VenueTargetRef(
        VenueTargetRole.ROUTER,
        VenueReferenceNamespace.EVM_ADDRESS,
        "0x5555555555555555555555555555555555555555",
    )
    second = _verified(
        operational_refs=(second_router,),
        evidence=_evidence(block_number=21, router=second_router),
    )
    assert first.binding.binding_hash == second.binding.binding_hash


def test_canonical_preimage_uses_ascii_json_escaping_without_escaping_solidus() -> None:
    result = _verified(components=(VenueBindingComponent("hook", 'route/"exact"'),))
    preimage = result.binding.canonical_preimage_bytes()
    assert b'"value":"route/\\"exact\\""' in preimage
    assert b"\\/" not in preimage


@pytest.mark.parametrize(
    ("mutation", "value"),
    [
        ("components", (VenueBindingComponent("fee_tier", "3000"),)),
        ("assets", (WETH, USDC)),
        (
            "identity_refs",
            (
                VenueTargetRef(
                    VenueTargetRole.POOL,
                    VenueReferenceNamespace.EVM_ADDRESS,
                    "0x6666666666666666666666666666666666666666",
                ),
            ),
        ),
    ],
)
def test_identity_mutations_change_binding_hash(mutation: str, value: tuple[object, ...]) -> None:
    baseline = _verified()
    changed = _verified(**{mutation: value})
    assert changed.binding.binding_hash != baseline.binding.binding_hash


def test_exact_binding_cannot_be_constructed_without_verification() -> None:
    with pytest.raises(TypeError, match="created only"):
        ExactVenueBinding()


def test_binding_rejects_unordered_duplicate_and_operational_identity_inputs() -> None:
    market = VenueTargetRef(
        VenueTargetRole.MARKET,
        VenueReferenceNamespace.EVM_ADDRESS,
        "0x7777777777777777777777777777777777777777",
    )
    with pytest.raises(ValueError, match="canonically ordered"):
        _verified(identity_refs=(POOL, market))
    with pytest.raises(ValueError, match="duplicate"):
        _verified(assets=(USDC, USDC))
    with pytest.raises(ValueError, match="physical venue roles"):
        _verified(identity_refs=(ROUTER,))


def test_binding_rejects_mutable_containers_cross_chain_and_raw_primitive() -> None:
    polygon_asset = AssetIdentity(
        "polygon",
        AssetNamespace.ERC20,
        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    )
    with pytest.raises(TypeError, match="must be a tuple"):
        _verified(identity_refs=[POOL])  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="cross-chain"):
        _verified(assets=(USDC, polygon_asset))
    solana_program = VenueTargetRef(
        VenueTargetRole.PROGRAM,
        VenueReferenceNamespace.SOLANA_ADDRESS,
        "11111111111111111111111111111111",
    )
    with pytest.raises(ValueError, match="invalid on 'ethereum'"):
        _verified(identity_refs=(solana_program,))
    with pytest.raises(TypeError, match="Primitive"):
        build_verified_venue_binding(
            chain="ethereum",
            protocol="example_v3",
            primitive="lp",  # type: ignore[arg-type]
            identity_refs=(POOL,),
            binding_components=COMPONENTS,
            ordered_assets=(USDC, WETH),
            binding_policy_version=1,
            operational_refs=(ROUTER,),
            evidence=_evidence(),
        )


def test_evidence_and_success_are_block_anchored_and_exact() -> None:
    with pytest.raises(ValueError, match="block_number"):
        _evidence(block_number=0)
    with pytest.raises(ValueError, match="block_hash"):
        VenueVerificationEvidence(
            chain="ethereum",
            verifier_ref="example.verifier:Verifier",
            verifier_contract_version="v1",
            block_number=1,
            block_hash="0xBAD",
            observed_facts=(VenueObservedFact("router", ROUTER.reference, ROUTER),),
        )
    with pytest.raises(ValueError, match="lack observed facts"):
        _verified(operational_refs=(FACTORY,))


def test_failure_states_accept_only_their_closed_reason_set() -> None:
    failure = VenueBindingFailure(
        state=VenueBindingFailureState.UNAVAILABLE,
        reason_code=VenueBindingFailureReason.GATEWAY_UNAVAILABLE,
        detail="gateway read failed",
    )
    assert failure.evidence is None
    with pytest.raises(ValueError, match="invalid"):
        VenueBindingFailure(
            state=VenueBindingFailureState.UNSUPPORTED,
            reason_code=VenueBindingFailureReason.TARGET_MISMATCH,
            detail="wrong class",
        )


def test_request_rejects_noncanonical_and_malformed_scope() -> None:
    request = VenueVerificationRequest(
        chain="ethereum",
        protocol="example_v3",
        primitive=Primitive.LP,
        requested_refs=(POOL,),
        ordered_assets=(USDC, WETH),
        binding_components=COMPONENTS,
        binding_policy_version=1,
    )
    assert request.protocol == "example_v3"
    with pytest.raises(ValueError, match="canonical chain"):
        VenueVerificationRequest(
            chain="ETHEREUM",
            protocol="example_v3",
            primitive=Primitive.LP,
            requested_refs=(POOL,),
            ordered_assets=(USDC, WETH),
            binding_components=COMPONENTS,
            binding_policy_version=1,
        )
    with pytest.raises(ValueError, match="duplicate assets"):
        VenueVerificationRequest(
            chain="ethereum",
            protocol="example_v3",
            primitive=Primitive.LP,
            requested_refs=(POOL,),
            ordered_assets=(USDC, USDC),
            binding_components=COMPONENTS,
            binding_policy_version=1,
        )
    with pytest.raises(ValueError, match="physical venue roles"):
        VenueVerificationRequest(
            chain="ethereum",
            protocol="example_v3",
            primitive=Primitive.LP,
            requested_refs=(ROUTER,),
            ordered_assets=(USDC, WETH),
            binding_components=COMPONENTS,
            binding_policy_version=1,
        )
