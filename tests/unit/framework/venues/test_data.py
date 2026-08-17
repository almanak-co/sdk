from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, cast

import pytest

from almanak.core.asset_identity import AssetIdentity, AssetNamespace
from almanak.core.capability_obligations import ExactTargetFeature
from almanak.framework.data.timeframes import OHLCVTimeframe
from almanak.framework.primitives.types import Primitive
from almanak.framework.venues import (
    DepthParameters,
    ExactVenueFeatureRequest,
    ExactVenueObservation,
    OhlcvParameters,
    QuoteParameters,
    ReferencePriceMethod,
    ReferencePriceParameters,
    TwapParameters,
    VenueBindingComponent,
    VenueDataAnchorKind,
    VenueDataFailure,
    VenueDataFailureReason,
    VenueDataFailureState,
    VenueDataProvenance,
    VenueObservationAnchor,
    VenueObservedFact,
    VenueReferenceNamespace,
    VenueTargetRef,
    VenueTargetRole,
    VenueVerificationEvidence,
    VerifiedVenueBinding,
    build_verified_venue_binding,
)

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
USDC = AssetIdentity("base", AssetNamespace.ERC20, "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
WETH = AssetIdentity("base", AssetNamespace.ERC20, "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")


def _verified() -> VerifiedVenueBinding:
    return build_verified_venue_binding(
        chain="base",
        protocol="uniswap_v3",
        primitive=Primitive.LP,
        identity_refs=(POOL,),
        binding_components=(VenueBindingComponent("fee_tier", "500"),),
        ordered_assets=(USDC, WETH),
        binding_policy_version=1,
        operational_refs=(ROUTER,),
        evidence=VenueVerificationEvidence(
            chain="base",
            verifier_ref="almanak.connectors.uniswap_v3.venue_verifier:UniswapV3VenueVerifier",
            verifier_contract_version="uniswap_v3.exact_venue.v1",
            block_number=20,
            block_hash="0x" + "44" * 32,
            observed_facts=(VenueObservedFact("router", ROUTER.reference, ROUTER),),
        ),
    )


_FEATURE_VERSIONS = {
    ExactTargetFeature.QUOTE: "exact_quote.v1",
    ExactTargetFeature.TWAP: "exact_twap.v1",
    ExactTargetFeature.OHLCV: "exact_ohlcv.v1",
    ExactTargetFeature.DEPTH: "exact_depth.v1",
    ExactTargetFeature.REFERENCE_PRICE: "exact_reference_price.v1",
}


def _request(parameters: object | None = None) -> ExactVenueFeatureRequest:
    exact_parameters = cast(Any, parameters or QuoteParameters(0, 1, 1_000_000, 20))
    return ExactVenueFeatureRequest(
        verified_binding=_verified(),
        parameters=exact_parameters,
        feature_contract_version=_FEATURE_VERSIONS[exact_parameters.feature],
    )


def _anchor() -> VenueObservationAnchor:
    return VenueObservationAnchor(
        observed_at=datetime(2026, 8, 16, 12, tzinfo=UTC),
        block_number=20,
        block_hash="0x" + "44" * 32,
    )


def _provenance() -> VenueDataProvenance:
    return VenueDataProvenance(
        provider_ref="almanak.connectors.uniswap_v3.quote_provider:UniswapV3QuoteProvider",
        provider_contract_version="uniswap_v3.quote.v1",
        source="gateway_rpc",
        source_observation_ref="base:20:0x" + "44" * 32,
    )


def test_request_has_a_golden_canonical_feature_identity() -> None:
    request = _request()
    expected = (
        b'{"bindingHash":"' + request.binding_hash.encode() + b'",'
        b'"feature":"quote","featureContractVersion":"exact_quote.v1",'
        b'"parameters":{"amountInBaseUnits":1000000,"asOfBlock":20,"baseAssetIndex":0,'
        b'"quoteAssetIndex":1},"schemaVersion":1}'
    )
    assert request.canonical_preimage_bytes() == expected
    assert request.feature_identity == hashlib.sha256(expected).hexdigest()
    assert request.feature is ExactTargetFeature.QUOTE


@pytest.mark.parametrize(
    ("parameters", "feature"),
    [
        (TwapParameters(0, 1, 300, 20), ExactTargetFeature.TWAP),
        (
            OhlcvParameters(
                0,
                1,
                OHLCVTimeframe.FIFTEEN_MINUTES,
                datetime(2026, 8, 16, 12, tzinfo=UTC),
                datetime(2026, 8, 16, 13, tzinfo=UTC),
            ),
            ExactTargetFeature.OHLCV,
        ),
        (DepthParameters(0, 1, 200, 20), ExactTargetFeature.DEPTH),
        (
            ReferencePriceParameters(0, 1, ReferencePriceMethod.POOL_SLOT0, 20),
            ExactTargetFeature.REFERENCE_PRICE,
        ),
    ],
)
def test_each_feature_uses_a_closed_parameter_contract(parameters: object, feature: ExactTargetFeature) -> None:
    request = _request(parameters)
    assert request.feature is feature
    expected_anchor = (
        VenueDataAnchorKind.OFFCHAIN_SOURCE
        if feature is ExactTargetFeature.OHLCV
        else VenueDataAnchorKind.CHAIN_BLOCK
    )
    assert request.anchor_kind is expected_anchor
    assert request.parameters.to_wire()


@pytest.mark.parametrize(
    ("parameters", "expected"),
    [
        (
            TwapParameters(0, 1, 300, 20),
            b'{"bindingHash":"{binding}","feature":"twap","featureContractVersion":"exact_twap.v1",'
            b'"parameters":{"asOfBlock":20,"baseAssetIndex":0,"quoteAssetIndex":1,"windowSeconds":300},'
            b'"schemaVersion":1}',
        ),
        (
            OhlcvParameters(
                0,
                1,
                OHLCVTimeframe.FIFTEEN_MINUTES,
                datetime(2026, 8, 16, 12, tzinfo=UTC),
                datetime(2026, 8, 16, 13, tzinfo=UTC),
            ),
            b'{"bindingHash":"{binding}","feature":"ohlcv","featureContractVersion":"exact_ohlcv.v1",'
            b'"parameters":{"baseAssetIndex":0,"endAt":"2026-08-16T13:00:00Z","quoteAssetIndex":1,'
            b'"startAt":"2026-08-16T12:00:00Z","timeframe":"15m"},"schemaVersion":1}',
        ),
        (
            DepthParameters(0, 1, 200, 20),
            b'{"bindingHash":"{binding}","feature":"depth","featureContractVersion":"exact_depth.v1",'
            b'"parameters":{"asOfBlock":20,"baseAssetIndex":0,"quoteAssetIndex":1,"rangeBps":200},'
            b'"schemaVersion":1}',
        ),
        (
            ReferencePriceParameters(0, 1, ReferencePriceMethod.POOL_SLOT0, 20),
            b'{"bindingHash":"{binding}","feature":"reference_price",'
            b'"featureContractVersion":"exact_reference_price.v1","parameters":{"asOfBlock":20,'
            b'"baseAssetIndex":0,"method":"pool_slot0","quoteAssetIndex":1},"schemaVersion":1}',
        ),
    ],
)
def test_each_non_quote_feature_has_a_golden_canonical_identity(parameters: object, expected: bytes) -> None:
    request = _request(parameters)
    expected_bytes = expected.replace(b"{binding}", request.binding_hash.encode())
    assert request.canonical_preimage_bytes() == expected_bytes
    assert request.feature_identity == hashlib.sha256(expected_bytes).hexdigest()


def test_request_requires_verified_binding_exact_parameter_type_and_valid_asset_indexes() -> None:
    with pytest.raises(TypeError, match="VerifiedVenueBinding"):
        ExactVenueFeatureRequest(
            verified_binding=cast(Any, _verified().binding),
            parameters=QuoteParameters(0, 1, 1, 20),
            feature_contract_version="v1",
        )
    with pytest.raises(TypeError, match="parameter type"):
        ExactVenueFeatureRequest(
            verified_binding=_verified(),
            parameters=cast(Any, {"base_asset_index": 0}),
            feature_contract_version="v1",
        )
    with pytest.raises(ValueError, match="must differ"):
        _request(QuoteParameters(0, 0, 1, 20))
    with pytest.raises(ValueError, match="must index"):
        _request(QuoteParameters(0, 2, 1, 20))
    with pytest.raises(ValueError, match="positive"):
        _request(QuoteParameters(0, 1, 0, 20))
    with pytest.raises(ValueError, match="evidence block"):
        _request(QuoteParameters(0, 1, 1, 21))
    with pytest.raises(TypeError, match="ReferencePriceMethod"):
        _request(ReferencePriceParameters(0, 1, cast(Any, "pool_slot0"), 20))


def test_ohlcv_requires_resolved_utc_whole_timeframe_interval() -> None:
    start = datetime(2026, 8, 16, 12, tzinfo=UTC)
    with pytest.raises(ValueError, match="align"):
        _request(
            OhlcvParameters(
                0,
                1,
                OHLCVTimeframe.FIFTEEN_MINUTES,
                start + timedelta(seconds=60),
                start + timedelta(seconds=960),
            )
        )
    with pytest.raises(ValueError, match="UTC"):
        _request(
            OhlcvParameters(
                0,
                1,
                OHLCVTimeframe.FIFTEEN_MINUTES,
                start.astimezone(timezone(timedelta(hours=1))),
                (start + timedelta(hours=1)).astimezone(timezone(timedelta(hours=1))),
            )
        )


def test_observation_is_constructed_from_request_and_preserves_measured_zero() -> None:
    request = _request()
    observation = ExactVenueObservation.from_request(
        request=request,
        value=0,
        anchor=_anchor(),
        provenance=_provenance(),
    )
    assert observation.value == 0
    assert observation.binding_hash == request.binding_hash
    assert observation.feature_identity == request.feature_identity
    assert observation.feature is request.feature
    with pytest.raises(TypeError, match="from_request"):
        ExactVenueObservation()
    with pytest.raises(ValueError, match="empty or unmeasured"):
        ExactVenueObservation.from_request(
            request=request,
            value=None,
            anchor=_anchor(),
            provenance=_provenance(),
        )
    for non_finite in (float("nan"), float("inf"), Decimal("NaN"), Decimal("Infinity")):
        with pytest.raises(ValueError, match="finite"):
            ExactVenueObservation.from_request(
                request=request,
                value=non_finite,
                anchor=_anchor(),
                provenance=_provenance(),
            )
    with pytest.raises(ValueError, match="empty or unmeasured"):
        ExactVenueObservation.from_request(
            request=request,
            value=(),
            anchor=_anchor(),
            provenance=_provenance(),
        )


def test_observation_rejects_a_different_block_or_false_block_on_offchain_candles() -> None:
    request = _request()
    with pytest.raises(ValueError, match="block identity"):
        ExactVenueObservation.from_request(
            request=request,
            value=1,
            anchor=VenueObservationAnchor(
                datetime(2026, 8, 16, 12, tzinfo=UTC),
                20,
                "0x" + "55" * 32,
            ),
            provenance=_provenance(),
        )

    candle_request = _request(
        OhlcvParameters(
            0,
            1,
            OHLCVTimeframe.FIFTEEN_MINUTES,
            datetime(2026, 8, 16, 12, tzinfo=UTC),
            datetime(2026, 8, 16, 13, tzinfo=UTC),
        )
    )
    with pytest.raises(ValueError, match="unrelated chain block"):
        ExactVenueObservation.from_request(
            request=candle_request,
            value=(1,),
            anchor=_anchor(),
            provenance=_provenance(),
        )


def test_anchor_requires_source_time_and_coherent_block_identity() -> None:
    with pytest.raises(ValueError, match="both"):
        VenueObservationAnchor(datetime(2026, 8, 16, 12, tzinfo=UTC), 20, None)
    with pytest.raises(ValueError, match="whole-second"):
        VenueObservationAnchor(datetime(2026, 8, 16, 12, 0, 0, 1, tzinfo=UTC), None, None)
    with pytest.raises(ValueError, match="block_hash"):
        VenueObservationAnchor(datetime(2026, 8, 16, 12, tzinfo=UTC), 20, "0xBAD")


def test_provenance_is_typed_and_canonical() -> None:
    with pytest.raises(ValueError, match="absolute.module"):
        VenueDataProvenance("todo", "v1", "gateway")
    with pytest.raises(ValueError, match="not canonical"):
        VenueDataProvenance("example.provider:Provider", "Version 1", "gateway")


def test_failures_are_closed_and_tied_to_the_exact_request() -> None:
    request = _request()
    failure = VenueDataFailure(
        request=request,
        state=VenueDataFailureState.UNAVAILABLE,
        reason_code=VenueDataFailureReason.TRANSPORT_UNAVAILABLE,
        detail="gateway read failed",
    )
    assert failure.binding_hash == request.binding_hash
    assert failure.feature_identity == request.feature_identity
    with pytest.raises(ValueError, match="invalid"):
        VenueDataFailure(
            request=request,
            state=VenueDataFailureState.UNSUPPORTED,
            reason_code=VenueDataFailureReason.BINDING_MISMATCH,
            detail="wrong class",
        )
