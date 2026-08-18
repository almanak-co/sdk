from __future__ import annotations

from datetime import UTC, datetime
from typing import cast

from almanak.core.asset_identity import AssetIdentity, AssetNamespace
from almanak.framework.primitives.types import Primitive
from almanak.framework.venues import (
    ExactVenueDataConsumer,
    ExactVenueDataRegistryAdapter,
    ExactVenueDataResult,
    ExactVenueFeatureRequest,
    ExactVenueObservation,
    QuoteParameters,
    VenueBindingComponent,
    VenueDataFailureReason,
    VenueDataFailureState,
    VenueDataProvenance,
    VenueObservationAnchor,
    VenueObservedFact,
    VenueReferenceNamespace,
    VenueTargetRef,
    VenueTargetRole,
    VenueVerificationEvidence,
    build_verified_venue_binding,
)


def _request() -> ExactVenueFeatureRequest:
    pool = VenueTargetRef(VenueTargetRole.POOL, VenueReferenceNamespace.EVM_ADDRESS, "0x" + "11" * 20)
    router = VenueTargetRef(VenueTargetRole.ROUTER, VenueReferenceNamespace.EVM_ADDRESS, "0x" + "22" * 20)
    asset0 = AssetIdentity("base", AssetNamespace.ERC20, "0x" + "aa" * 20)
    asset1 = AssetIdentity("base", AssetNamespace.ERC20, "0x" + "bb" * 20)
    verified = build_verified_venue_binding(
        chain="base",
        protocol="uniswap_v3",
        primitive=Primitive.LP,
        identity_refs=(pool,),
        binding_components=(VenueBindingComponent("fee_tier", "500"),),
        ordered_assets=(asset0, asset1),
        binding_policy_version=1,
        operational_refs=(router,),
        evidence=VenueVerificationEvidence(
            chain="base",
            verifier_ref="tests.unit.framework.venues.test_consumer:provider",
            verifier_contract_version="test.v1",
            block_number=20,
            block_hash="0x" + "44" * 32,
            observed_facts=(VenueObservedFact("router", router.reference, router),),
        ),
    )
    return ExactVenueFeatureRequest(
        verified_binding=verified,
        parameters=QuoteParameters(0, 1, 1_000_000, 20),
        feature_contract_version="exact_quote.v1",
    )


def _observation(request: ExactVenueFeatureRequest) -> ExactVenueObservation[int]:
    return ExactVenueObservation.from_request(
        request=request,
        value=123,
        anchor=VenueObservationAnchor(
            observed_at=datetime(2026, 8, 18, 12, tzinfo=UTC),
            block_number=20,
            block_hash="0x" + "44" * 32,
        ),
        provenance=VenueDataProvenance(
            provider_ref="tests.unit.framework.venues.test_consumer:provider",
            provider_contract_version="test.v1",
            source="gateway_rpc",
            source_observation_ref="base:20",
        ),
    )


class _Provider:
    def __init__(self, result: object) -> None:
        self.result = result
        self.requests: list[ExactVenueFeatureRequest] = []

    def read_exact(self, request: ExactVenueFeatureRequest) -> ExactVenueDataResult:
        self.requests.append(request)
        return cast(ExactVenueDataResult, self.result)


def test_exact_consumer_returns_provider_observation_and_preserves_identity() -> None:
    request = _request()
    provider = _Provider(_observation(request))

    result = ExactVenueDataConsumer(provider).read(request)

    assert result is provider.result
    assert result.binding_hash == request.binding_hash
    assert result.feature_identity == request.feature_identity
    assert provider.requests == [request]


def test_exact_consumer_does_not_accept_legacy_or_untyped_provider_value() -> None:
    provider = _Provider({"amount_out": 123, "pool": "0x" + "99" * 20})

    result = ExactVenueDataConsumer(provider).read(_request())

    assert result.state is VenueDataFailureState.UNAVAILABLE
    assert result.reason_code is VenueDataFailureReason.INCOMPLETE_PROVENANCE


def test_exact_consumer_converts_provider_transport_failure_to_unavailable() -> None:
    class BrokenProvider:
        def read_exact(self, request: ExactVenueFeatureRequest) -> ExactVenueDataResult:
            raise RuntimeError("gateway offline")

    result = ExactVenueDataConsumer(BrokenProvider()).read(_request())

    assert result.state is VenueDataFailureState.UNAVAILABLE
    assert result.reason_code is VenueDataFailureReason.PROVIDER_UNAVAILABLE


def test_exact_consumer_rejects_observation_for_another_request() -> None:
    request = _request()
    other_request = ExactVenueFeatureRequest(
        verified_binding=request.verified_binding,
        parameters=QuoteParameters(0, 1, 2_000_000, 20),
        feature_contract_version="exact_quote.v1",
    )
    provider = _Provider(_observation(other_request))

    result = ExactVenueDataConsumer(provider).read(request)

    assert result.state is VenueDataFailureState.MISMATCHED
    assert result.reason_code is VenueDataFailureReason.RESPONSE_IDENTITY_MISMATCH


def test_exact_consumer_adapts_gateway_aware_registry_provider() -> None:
    request = _request()
    gateway = object()
    calls: list[tuple[ExactVenueFeatureRequest, object]] = []

    def observe(current: ExactVenueFeatureRequest, current_gateway: object) -> ExactVenueDataResult:
        calls.append((current, current_gateway))
        return _observation(current)

    result = ExactVenueDataConsumer(ExactVenueDataRegistryAdapter(observe, gateway)).read(request)

    assert result is not None
    assert calls == [(request, gateway)]
