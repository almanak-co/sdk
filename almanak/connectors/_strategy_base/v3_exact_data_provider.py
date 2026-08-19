"""Exact, no-fallback TWAP observations for verified canonical V3 pools."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation, localcontext
from typing import Any

from almanak.connectors._strategy_base.v3_pool_abi import V3_FEE_SELECTOR, V3_TOKEN0_SELECTOR, V3_TOKEN1_SELECTOR
from almanak.core.asset_identity import AssetNamespace
from almanak.core.capability_obligations import ExactTargetFeature
from almanak.framework.data.interfaces import OHLCVCandle
from almanak.framework.venues import (
    BaseExactVenueDataProvider,
    ExactVenueDataGateway,
    ExactVenueFeatureRequest,
    ExactVenueObservation,
    OhlcvParameters,
    TwapParameters,
    VenueDataFailure,
    VenueDataFailureReason,
    VenueDataFailureState,
    VenueDataProvenance,
    VenueObservationAnchor,
    VenueTargetRef,
    VenueTargetRole,
)

PROVIDER_CONTRACT_VERSION = "v3_exact_data.v1"
PROVIDER_REF = "almanak.connectors._strategy_base.v3_exact_data_provider:V3ExactVenueDataProvider"
TWAP_FEATURE_CONTRACT_VERSION = "exact_twap.v1"
OHLCV_FEATURE_CONTRACT_VERSION = "exact_ohlcv.v1"
_SUPPORTED_PROTOCOLS = frozenset({"pancakeswap_v3", "uniswap_v3"})
_EXACT_OHLCV_SOURCE = "coingecko_onchain.exact_pool"
_OBSERVE_SELECTOR = bytes.fromhex("883bdbfd")
_DECIMALS_SELECTOR = bytes.fromhex("313ce567")
_WORD_BYTES = 32
_MAX_V3_TICK = 887_272


def _safe_exception_detail(exc: BaseException) -> str:
    """Collapse arbitrary transport text into the typed failure's ASCII wire contract."""
    detail = " ".join(str(exc).split()).encode("ascii", "backslashreplace").decode("ascii")
    return detail[:512].strip() or type(exc).__name__


def _failure(
    request: ExactVenueFeatureRequest,
    state: VenueDataFailureState,
    reason: VenueDataFailureReason,
    detail: str,
) -> VenueDataFailure:
    return VenueDataFailure(request=request, state=state, reason_code=reason, detail=detail)


def _word(value: int) -> bytes:
    return value.to_bytes(_WORD_BYTES, "big")


def _encode_observe(window_seconds: int) -> bytes:
    if window_seconds > 2**32 - 1:
        raise ValueError("V3 observe window exceeds uint32")
    return _OBSERVE_SELECTOR + _word(32) + _word(2) + _word(window_seconds) + _word(0)


def _decode_address(raw: bytes, field_name: str) -> str:
    if type(raw) is not bytes or len(raw) != _WORD_BYTES or raw[:12] != bytes(12):
        raise ValueError(f"{field_name} must be one canonically padded ABI address word")
    return "0x" + raw[12:].hex()


def _decode_uint(raw: bytes, field_name: str, *, bits: int) -> int:
    if type(raw) is not bytes or len(raw) != _WORD_BYTES:
        raise ValueError(f"{field_name} must be one ABI word")
    value = int.from_bytes(raw, "big")
    if value >= 2**bits:
        raise ValueError(f"{field_name} exceeds uint{bits}")
    return value


def _decode_observe(raw: bytes) -> tuple[int, int]:
    """Decode the canonical two-sample V3 observe response, rejecting drift."""
    if type(raw) is not bytes or len(raw) != 256:
        raise ValueError("observe response must be the canonical 256-byte two-array encoding")
    words = tuple(raw[index : index + 32] for index in range(0, len(raw), 32))
    if int.from_bytes(words[0], "big") != 64 or int.from_bytes(words[1], "big") != 160:
        raise ValueError("observe response has noncanonical array offsets")
    if int.from_bytes(words[2], "big") != 2 or int.from_bytes(words[5], "big") != 2:
        raise ValueError("observe response must contain exactly two samples per array")
    ticks = tuple(int.from_bytes(word, "big", signed=True) for word in words[3:5])
    if any(tick < -(2**55) or tick >= 2**55 for tick in ticks):
        raise ValueError("observe tick cumulative exceeds int56")
    liquidities = tuple(int.from_bytes(word, "big") for word in words[6:8])
    if any(value >= 2**160 for value in liquidities):
        raise ValueError("observe seconds-per-liquidity cumulative exceeds uint160")
    return ticks[0], ticks[1]


def _price_from_ticks(*, tick_start: int, tick_end: int, window_seconds: int, decimals: tuple[int, int]) -> Decimal:
    average_tick = (tick_end - tick_start) // window_seconds
    if abs(average_tick) > _MAX_V3_TICK:
        raise ValueError("observed average tick exceeds the V3 domain")
    with localcontext() as context:
        context.prec = 80
        price = Decimal("1.0001") ** average_tick
        price *= Decimal(10) ** (decimals[0] - decimals[1])
    if not price.is_finite() or price <= 0:
        raise InvalidOperation("observed V3 TWAP is not finite and positive")
    return price


def _pool_ref(request: ExactVenueFeatureRequest) -> VenueTargetRef:
    refs = request.verified_binding.binding.identity_refs
    pool_refs = tuple(reference for reference in refs if reference.role is VenueTargetRole.POOL)
    if len(refs) != 1 or len(pool_refs) != 1:
        raise ValueError("exact V3 TWAP requires one pool-only venue identity")
    return pool_refs[0]


def _asset_addresses(request: ExactVenueFeatureRequest) -> tuple[str, str]:
    assets = request.verified_binding.binding.ordered_assets
    if len(assets) != 2 or any(asset.asset_namespace is not AssetNamespace.ERC20 for asset in assets):
        raise ValueError("exact V3 TWAP requires exactly two ERC-20 assets")
    return assets[0].asset_reference, assets[1].asset_reference


def _expected_fee(request: ExactVenueFeatureRequest) -> int:
    components = request.verified_binding.binding.binding_components
    fee_values = tuple(component.value for component in components if component.name == "fee")
    if len(fee_values) != 1 or not fee_values[0].isdigit():
        raise ValueError("exact V3 binding must contain one canonical fee component")
    return int(fee_values[0])


class V3ExactVenueDataProvider(BaseExactVenueDataProvider):
    """Measure a verified V3 pool through generic block-pinned gateway reads."""

    def observe(
        self,
        request: ExactVenueFeatureRequest,
        gateway: ExactVenueDataGateway,
    ) -> ExactVenueObservation[Any] | VenueDataFailure:
        binding = request.verified_binding.binding
        if binding.protocol not in _SUPPORTED_PROTOCOLS:
            return _failure(
                request,
                VenueDataFailureState.UNSUPPORTED,
                VenueDataFailureReason.UNSUPPORTED_PROTOCOL,
                f"exact V3 data does not support protocol {binding.protocol!r}",
            )
        if request.feature is ExactTargetFeature.OHLCV and type(request.parameters) is OhlcvParameters:
            return self._observe_ohlcv(request, gateway)
        if request.feature is not ExactTargetFeature.TWAP or type(request.parameters) is not TwapParameters:
            return _failure(
                request,
                VenueDataFailureState.UNSUPPORTED,
                VenueDataFailureReason.UNSUPPORTED_FEATURE,
                f"{binding.protocol} exact data does not support feature {request.feature.value!r}",
            )
        if request.feature_contract_version != TWAP_FEATURE_CONTRACT_VERSION:
            return _failure(
                request,
                VenueDataFailureState.UNSUPPORTED,
                VenueDataFailureReason.UNSUPPORTED_FEATURE,
                f"exact V3 TWAP does not support feature contract {request.feature_contract_version!r}",
            )
        parameters = request.parameters
        try:
            pool = _pool_ref(request)
            assets = _asset_addresses(request)
            expected_fee = _expected_fee(request)
        except (TypeError, ValueError) as exc:
            return _failure(
                request,
                VenueDataFailureState.MISMATCHED,
                VenueDataFailureReason.BINDING_MISMATCH,
                f"exact V3 binding is invalid for TWAP: {_safe_exception_detail(exc)}",
            )
        try:
            observe_payload = _encode_observe(parameters.window_seconds)
        except ValueError as exc:
            return _failure(
                request,
                VenueDataFailureState.UNSUPPORTED,
                VenueDataFailureReason.UNSUPPORTED_FEATURE,
                f"exact V3 TWAP parameters are unsupported: {_safe_exception_detail(exc)}",
            )
        try:
            opening_block = gateway.block_identity(chain=binding.chain, block_number=parameters.as_of_block)
        except Exception as exc:
            return _failure(
                request,
                VenueDataFailureState.UNAVAILABLE,
                VenueDataFailureReason.TRANSPORT_UNAVAILABLE,
                f"cannot read exact V3 observation block: {_safe_exception_detail(exc)}",
            )
        if opening_block.block_hash != request.verified_binding.evidence.block_hash:
            return _failure(
                request,
                VenueDataFailureState.UNAVAILABLE,
                VenueDataFailureReason.REORGED_OBSERVATION,
                f"block {parameters.as_of_block} no longer matches verified venue evidence",
            )
        try:
            token0 = _decode_address(
                gateway.read(
                    chain=binding.chain,
                    target_address=pool.reference,
                    payload=bytes.fromhex(V3_TOKEN0_SELECTOR[2:]),
                    block_number=parameters.as_of_block,
                ),
                "token0",
            )
            token1 = _decode_address(
                gateway.read(
                    chain=binding.chain,
                    target_address=pool.reference,
                    payload=bytes.fromhex(V3_TOKEN1_SELECTOR[2:]),
                    block_number=parameters.as_of_block,
                ),
                "token1",
            )
            observed_fee = _decode_uint(
                gateway.read(
                    chain=binding.chain,
                    target_address=pool.reference,
                    payload=bytes.fromhex(V3_FEE_SELECTOR[2:]),
                    block_number=parameters.as_of_block,
                ),
                "fee",
                bits=24,
            )
            decimals = tuple(
                _decode_uint(
                    gateway.read(
                        chain=binding.chain,
                        target_address=asset,
                        payload=_DECIMALS_SELECTOR,
                        block_number=parameters.as_of_block,
                    ),
                    "decimals",
                    bits=8,
                )
                for asset in assets
            )
            tick_start, tick_end = _decode_observe(
                gateway.read(
                    chain=binding.chain,
                    target_address=pool.reference,
                    payload=observe_payload,
                    block_number=parameters.as_of_block,
                )
            )
            closing_block = gateway.block_identity(chain=binding.chain, block_number=parameters.as_of_block)
        except Exception as exc:
            return _failure(
                request,
                VenueDataFailureState.UNAVAILABLE,
                VenueDataFailureReason.TRANSPORT_UNAVAILABLE,
                f"gateway exact V3 TWAP read failed: {_safe_exception_detail(exc)}",
            )
        if (token0, token1, observed_fee) != (*assets, expected_fee):
            return _failure(
                request,
                VenueDataFailureState.MISMATCHED,
                VenueDataFailureReason.RESPONSE_IDENTITY_MISMATCH,
                "pool token order or fee no longer matches the verified binding",
            )
        if closing_block != opening_block:
            return _failure(
                request,
                VenueDataFailureState.UNAVAILABLE,
                VenueDataFailureReason.REORGED_OBSERVATION,
                f"block {parameters.as_of_block} changed during the exact V3 TWAP read",
            )
        try:
            price = _price_from_ticks(
                tick_start=tick_start,
                tick_end=tick_end,
                window_seconds=parameters.window_seconds,
                decimals=(decimals[0], decimals[1]),
            )
            if parameters.base_asset_index == 1 and parameters.quote_asset_index == 0:
                price = Decimal(1) / price
        except (ArithmeticError, ValueError) as exc:
            return _failure(
                request,
                VenueDataFailureState.UNAVAILABLE,
                VenueDataFailureReason.INCOMPLETE_PROVENANCE,
                f"exact V3 TWAP measurement is invalid: {_safe_exception_detail(exc)}",
            )
        try:
            return ExactVenueObservation.from_request(
                request=request,
                value=price,
                anchor=VenueObservationAnchor(
                    observed_at=datetime.fromtimestamp(opening_block.timestamp, tz=UTC),
                    block_number=opening_block.number,
                    block_hash=opening_block.block_hash,
                ),
                provenance=VenueDataProvenance(
                    provider_ref=PROVIDER_REF,
                    provider_contract_version=PROVIDER_CONTRACT_VERSION,
                    source="gateway_rpc.eth_call.v3_observe",
                    source_observation_ref=(
                        f"{binding.protocol}:{binding.chain}:{pool.reference}:"
                        f"{opening_block.number}:{parameters.window_seconds}"
                    ),
                ),
            )
        except (OSError, OverflowError, TypeError, ValueError) as exc:
            return _failure(
                request,
                VenueDataFailureState.UNAVAILABLE,
                VenueDataFailureReason.INCOMPLETE_PROVENANCE,
                f"exact V3 TWAP observation provenance is invalid: {_safe_exception_detail(exc)}",
            )

    def _observe_ohlcv(
        self,
        request: ExactVenueFeatureRequest,
        gateway: ExactVenueDataGateway,
    ) -> ExactVenueObservation[tuple[OHLCVCandle, ...]] | VenueDataFailure:
        binding = request.verified_binding.binding
        if request.feature_contract_version != OHLCV_FEATURE_CONTRACT_VERSION:
            return _failure(
                request,
                VenueDataFailureState.UNSUPPORTED,
                VenueDataFailureReason.UNSUPPORTED_FEATURE,
                f"exact V3 OHLCV does not support feature contract {request.feature_contract_version!r}",
            )
        parameters = request.parameters
        assert type(parameters) is OhlcvParameters
        interval_seconds = int((parameters.end_at - parameters.start_at).total_seconds())
        candle_count = interval_seconds // parameters.timeframe.seconds
        if candle_count > 1000 or interval_seconds > 180 * 24 * 60 * 60:
            return _failure(
                request,
                VenueDataFailureState.UNSUPPORTED,
                VenueDataFailureReason.UNSUPPORTED_FEATURE,
                "exact V3 OHLCV interval exceeds the provider's 1000-candle or 180-day request bound",
            )
        try:
            pool = _pool_ref(request)
            assets = _asset_addresses(request)
            _expected_fee(request)
        except (TypeError, ValueError) as exc:
            return _failure(
                request,
                VenueDataFailureState.MISMATCHED,
                VenueDataFailureReason.BINDING_MISMATCH,
                f"exact V3 binding is invalid for OHLCV: {_safe_exception_detail(exc)}",
            )
        base_address = assets[parameters.base_asset_index]
        quote_address = assets[parameters.quote_asset_index]
        try:
            response = gateway.exact_pool_ohlcv(
                chain=binding.chain,
                pool_address=pool.reference,
                base_token_address=base_address,
                quote_token_address=quote_address,
                timeframe=parameters.timeframe,
                start_ts=int(parameters.start_at.timestamp()),
                end_ts=int(parameters.end_at.timestamp()),
                binding_hash=request.binding_hash,
                feature_identity=request.feature_identity,
            )
        except (AttributeError, TypeError, ValueError) as exc:
            return _failure(
                request,
                VenueDataFailureState.UNAVAILABLE,
                VenueDataFailureReason.INCOMPLETE_PROVENANCE,
                "gateway exact V3 OHLCV response is unavailable or lacks identity echoes: "
                f"{_safe_exception_detail(exc)}",
            )
        except Exception as exc:
            return _failure(
                request,
                VenueDataFailureState.UNAVAILABLE,
                VenueDataFailureReason.TRANSPORT_UNAVAILABLE,
                f"gateway exact V3 OHLCV read failed: {_safe_exception_detail(exc)}",
            )

        expected_identity = (
            binding.chain,
            pool.reference,
            parameters.timeframe,
            int(parameters.start_at.timestamp()),
            int(parameters.end_at.timestamp()),
            request.binding_hash,
            request.feature_identity,
            base_address,
            quote_address,
            _EXACT_OHLCV_SOURCE,
        )
        observed_identity = (
            response.chain,
            response.pool_address,
            response.timeframe,
            response.start_ts,
            response.end_ts,
            response.binding_hash,
            response.feature_identity,
            response.base_token_address,
            response.quote_token_address,
            response.source,
        )
        if observed_identity != expected_identity:
            return _failure(
                request,
                VenueDataFailureState.MISMATCHED,
                VenueDataFailureReason.RESPONSE_IDENTITY_MISMATCH,
                "gateway exact V3 OHLCV identity echo does not match the verified request",
            )
        expected_timestamps = tuple(
            range(
                int(parameters.start_at.timestamp()),
                int(parameters.end_at.timestamp()),
                parameters.timeframe.seconds,
            )
        )
        observed_timestamps = tuple(int(candle.timestamp.timestamp()) for candle in response.candles)
        if observed_timestamps != expected_timestamps:
            return _failure(
                request,
                VenueDataFailureState.UNAVAILABLE,
                VenueDataFailureReason.STALE_OBSERVATION,
                "gateway exact V3 OHLCV page does not cover the complete requested interval",
            )
        for candle in response.candles:
            values = (candle.open, candle.high, candle.low, candle.close, candle.volume)
            if (
                candle.volume is None
                or any(type(value) is not Decimal or not value.is_finite() for value in values)
                or min(candle.open, candle.high, candle.low, candle.close) <= 0
                or candle.volume < 0
                or not candle.low <= min(candle.open, candle.close) <= max(candle.open, candle.close) <= candle.high
            ):
                return _failure(
                    request,
                    VenueDataFailureState.UNAVAILABLE,
                    VenueDataFailureReason.INCOMPLETE_PROVENANCE,
                    "gateway exact V3 OHLCV page contains empty or invalid measured values",
                )
        try:
            return ExactVenueObservation.from_request(
                request=request,
                value=response.candles,
                anchor=VenueObservationAnchor(
                    observed_at=response.observed_at,
                    block_number=None,
                    block_hash=None,
                ),
                provenance=VenueDataProvenance(
                    provider_ref=PROVIDER_REF,
                    provider_contract_version=PROVIDER_CONTRACT_VERSION,
                    source=response.source,
                    source_observation_ref=(
                        f"{binding.protocol}:{binding.chain}:{pool.reference}:"
                        f"{expected_timestamps[0]}:{int(parameters.end_at.timestamp())}:"
                        f"{parameters.timeframe.value}:{base_address}:{quote_address}"
                    ),
                ),
            )
        except (OSError, OverflowError, TypeError, ValueError) as exc:
            return _failure(
                request,
                VenueDataFailureState.UNAVAILABLE,
                VenueDataFailureReason.INCOMPLETE_PROVENANCE,
                f"exact V3 OHLCV observation provenance is invalid: {_safe_exception_detail(exc)}",
            )


__all__ = [
    "PROVIDER_CONTRACT_VERSION",
    "PROVIDER_REF",
    "OHLCV_FEATURE_CONTRACT_VERSION",
    "TWAP_FEATURE_CONTRACT_VERSION",
    "V3ExactVenueDataProvider",
]
