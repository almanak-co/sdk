"""Validation of the additive reference-price identity and composition wire contract."""

import re
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation, localcontext

from almanak.framework.market.models import ReferencePriceBasis, ReferencePriceCompositionData
from almanak.gateway.proto import gateway_pb2
from almanak.integrations.bstocks.catalog import TokenReferenceProfile


def _time(value: int) -> datetime:
    if value <= 0:
        raise ValueError("reference_composition_timestamp_missing")
    return datetime.fromtimestamp(value, UTC)


def _positive(value: str) -> Decimal:
    try:
        number = Decimal(value)
    except InvalidOperation as exc:
        raise ValueError("reference_composition_number_invalid") from exc
    if not number.is_finite() or number <= 0:
        raise ValueError("reference_composition_number_invalid")
    return number


def decode_reference_composition(
    response: gateway_pb2.ReferencePriceResponse,
    profile: TokenReferenceProfile | None,
    *,
    instrument: str,
    chain: str,
    quote: str,
) -> tuple[ReferencePriceBasis, ReferencePriceCompositionData | None]:
    """An old gateway's AVAILABLE response alone is not evidence of composition."""
    if profile is None:
        if (
            response.basis == gateway_pb2.REFERENCE_PRICE_BASIS_RAW_TOKEN
            or response.token_address
            or response.HasField("composition")
        ):
            raise ValueError("unexpected_token_reference_composition")
        return (
            ReferencePriceBasis.UNDERLYING_SHARE
            if response.basis == gateway_pb2.REFERENCE_PRICE_BASIS_UNDERLYING_SHARE
            else ReferencePriceBasis.UNSPECIFIED,
            None,
        )
    raw = _validate_reference_composition_identity(response, profile, instrument=instrument, chain=chain, quote=quote)
    return ReferencePriceBasis.RAW_TOKEN, _decode_validated_reference_composition(response, raw)


def _validate_reference_composition_identity(
    response: gateway_pb2.ReferencePriceResponse,
    profile: TokenReferenceProfile,
    *,
    instrument: str,
    chain: str,
    quote: str,
) -> gateway_pb2.ReferencePriceComposition:
    if (
        response.basis != gateway_pb2.REFERENCE_PRICE_BASIS_RAW_TOKEN
        or response.token_address.lower() != profile.address
        or response.instrument != instrument
        or response.chain != chain
        or response.quote != quote
        or not response.HasField("composition")
    ):
        raise ValueError("reference_composition_identity_missing_or_mismatched")
    raw = response.composition
    if (
        raw.underlying_instrument != profile.underlying
        or not raw.underlying_source
        or raw.underlying_observed_at != response.observed_at
        or raw.multiplier_block_number <= 0
        or not re.fullmatch(r"0x[0-9a-fA-F]{64}", raw.multiplier_block_hash)
        or raw.beacon_address.lower() != profile.beacon
        or raw.implementation_address.lower() != profile.implementation
    ):
        raise ValueError("reference_composition_provenance_invalid")
    return raw


def _decode_validated_reference_composition(
    response: gateway_pb2.ReferencePriceResponse,
    raw: gateway_pb2.ReferencePriceComposition,
) -> ReferencePriceCompositionData:
    underlying, multiplier = _positive(raw.underlying_price), _positive(raw.multiplier)
    with localcontext() as ctx:
        ctx.prec = 160
        if _positive(response.price) != underlying * multiplier:
            raise ValueError("reference_composition_price_mismatch")
    scheduled = _positive(raw.scheduled_multiplier) if raw.scheduled_multiplier else None
    if scheduled is not None or raw.multiplier_effective_at > raw.multiplier_block_timestamp:
        raise ValueError("reference_composition_adjustment_pending")
    composition = ReferencePriceCompositionData(
        underlying_instrument=raw.underlying_instrument,
        underlying_price=underlying,
        underlying_source=raw.underlying_source,
        underlying_observed_at=_time(raw.underlying_observed_at),
        multiplier=multiplier,
        multiplier_block_number=raw.multiplier_block_number,
        multiplier_block_hash=raw.multiplier_block_hash,
        multiplier_block_timestamp=_time(raw.multiplier_block_timestamp),
        multiplier_read_at=_time(raw.multiplier_read_at),
        scheduled_multiplier=scheduled,
        multiplier_effective_at=_time(raw.multiplier_effective_at) if raw.multiplier_effective_at else None,
        beacon_address=raw.beacon_address,
        implementation_address=raw.implementation_address,
        composed_at=_time(raw.composed_at),
    )
    if (
        composition.multiplier_effective_at
        and composition.underlying_observed_at <= composition.multiplier_effective_at
    ):
        raise ValueError("reference_composition_adjustment_not_aligned")
    if composition.composed_at < composition.multiplier_read_at:
        raise ValueError("reference_composition_clock_incoherent")
    return composition
