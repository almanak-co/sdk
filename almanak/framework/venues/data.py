"""Typed, provider-neutral observations for data bound to an exact venue.

This module performs no discovery or I/O.  Connector-owned providers consume
requests created from a :class:`VerifiedVenueBinding` and return either an
observation tied to that request or a closed failure.  Legacy address-, pair-,
and symbol-scoped data remains outside this contract.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any, ClassVar

from almanak.core.capability_obligations import ExactTargetFeature
from almanak.framework.data.timeframes import OHLCVTimeframe

from .types import ExactVenueBinding, VerifiedVenueBinding

EXACT_VENUE_DATA_SCHEMA_VERSION = 1

_ASCII_RE = re.compile(r"^[\x20-\x7e]+$")
_BLOCK_HASH_RE = re.compile(r"^0x[0-9a-f]{64}$")
_IMPORT_REF_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*:[A-Za-z_][A-Za-z0-9_]*$")
_VERSION_RE = re.compile(r"^[a-z0-9][a-z0-9._-]*$")


def _require_exact_str(value: object, field_name: str) -> str:
    if type(value) is not str:
        raise TypeError(f"{field_name} must be a string, got {type(value).__name__}")
    if not value or value != value.strip() or not _ASCII_RE.fullmatch(value):
        raise ValueError(f"{field_name} must be non-empty, trimmed printable ASCII")
    return value


def _require_positive_int(value: object, field_name: str) -> int:
    if type(value) is not int or value <= 0:
        raise ValueError(f"{field_name} must be a positive integer")
    return value


def _require_asset_pair(binding: ExactVenueBinding, base_index: object, quote_index: object) -> None:
    for value, name in ((base_index, "base_asset_index"), (quote_index, "quote_asset_index")):
        if type(value) is not int or value < 0 or value >= len(binding.ordered_assets):
            raise ValueError(f"{name} must index ExactVenueBinding.ordered_assets")
    if base_index == quote_index:
        raise ValueError("base_asset_index and quote_asset_index must differ")


def _require_utc_second(value: object, field_name: str) -> datetime:
    if type(value) is not datetime:
        raise TypeError(f"{field_name} must be a datetime")
    if value.tzinfo is not UTC or value.microsecond != 0:
        raise ValueError(f"{field_name} must be UTC with whole-second precision")
    return value


def _wire_timestamp(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


class VenueDataAnchorKind(StrEnum):
    """Closed observation-anchor semantics for an exact feature."""

    CHAIN_BLOCK = "chain_block"
    OFFCHAIN_SOURCE = "offchain_source"


class ReferencePriceMethod(StrEnum):
    """Reviewed methods that can produce an exact-venue reference price."""

    POOL_SLOT0 = "pool_slot0"


@dataclass(frozen=True, slots=True)
class QuoteParameters:
    """Exact quote identity in venue-defined asset indexes and base units."""

    base_asset_index: int
    quote_asset_index: int
    amount_in_base_units: int
    as_of_block: int

    feature: ClassVar[ExactTargetFeature] = ExactTargetFeature.QUOTE
    anchor_kind: ClassVar[VenueDataAnchorKind] = VenueDataAnchorKind.CHAIN_BLOCK

    def validate(self, binding: ExactVenueBinding) -> None:
        _require_asset_pair(binding, self.base_asset_index, self.quote_asset_index)
        _require_positive_int(self.amount_in_base_units, "QuoteParameters.amount_in_base_units")
        _require_positive_int(self.as_of_block, "QuoteParameters.as_of_block")

    def chain_anchor_block(self) -> int:
        return self.as_of_block

    def to_wire(self) -> dict[str, object]:
        return {
            "amountInBaseUnits": self.amount_in_base_units,
            "asOfBlock": self.as_of_block,
            "baseAssetIndex": self.base_asset_index,
            "quoteAssetIndex": self.quote_asset_index,
        }


@dataclass(frozen=True, slots=True)
class TwapParameters:
    """Exact TWAP identity at one requested block and fixed lookback window."""

    base_asset_index: int
    quote_asset_index: int
    window_seconds: int
    as_of_block: int

    feature: ClassVar[ExactTargetFeature] = ExactTargetFeature.TWAP
    anchor_kind: ClassVar[VenueDataAnchorKind] = VenueDataAnchorKind.CHAIN_BLOCK

    def validate(self, binding: ExactVenueBinding) -> None:
        _require_asset_pair(binding, self.base_asset_index, self.quote_asset_index)
        _require_positive_int(self.window_seconds, "TwapParameters.window_seconds")
        _require_positive_int(self.as_of_block, "TwapParameters.as_of_block")

    def chain_anchor_block(self) -> int:
        return self.as_of_block

    def to_wire(self) -> dict[str, object]:
        return {
            "asOfBlock": self.as_of_block,
            "baseAssetIndex": self.base_asset_index,
            "quoteAssetIndex": self.quote_asset_index,
            "windowSeconds": self.window_seconds,
        }


@dataclass(frozen=True, slots=True)
class OhlcvParameters:
    """Exact candle identity over a resolved half-open UTC interval."""

    base_asset_index: int
    quote_asset_index: int
    timeframe: OHLCVTimeframe
    start_at: datetime
    end_at: datetime

    feature: ClassVar[ExactTargetFeature] = ExactTargetFeature.OHLCV
    anchor_kind: ClassVar[VenueDataAnchorKind] = VenueDataAnchorKind.OFFCHAIN_SOURCE

    def validate(self, binding: ExactVenueBinding) -> None:
        _require_asset_pair(binding, self.base_asset_index, self.quote_asset_index)
        if type(self.timeframe) is not OHLCVTimeframe:
            raise TypeError("OhlcvParameters.timeframe must be an OHLCVTimeframe")
        start = _require_utc_second(self.start_at, "OhlcvParameters.start_at")
        end = _require_utc_second(self.end_at, "OhlcvParameters.end_at")
        if start >= end:
            raise ValueError("OhlcvParameters.start_at must precede end_at")
        if int(start.timestamp()) % self.timeframe.seconds or int(end.timestamp()) % self.timeframe.seconds:
            raise ValueError("OHLCV interval boundaries must align to the timeframe")

    def chain_anchor_block(self) -> None:
        return None

    def to_wire(self) -> dict[str, object]:
        return {
            "baseAssetIndex": self.base_asset_index,
            "endAt": _wire_timestamp(self.end_at),
            "quoteAssetIndex": self.quote_asset_index,
            "startAt": _wire_timestamp(self.start_at),
            "timeframe": self.timeframe.value,
        }


@dataclass(frozen=True, slots=True)
class DepthParameters:
    """Exact liquidity-depth identity at one block and symmetric range."""

    base_asset_index: int
    quote_asset_index: int
    range_bps: int
    as_of_block: int

    feature: ClassVar[ExactTargetFeature] = ExactTargetFeature.DEPTH
    anchor_kind: ClassVar[VenueDataAnchorKind] = VenueDataAnchorKind.CHAIN_BLOCK

    def validate(self, binding: ExactVenueBinding) -> None:
        _require_asset_pair(binding, self.base_asset_index, self.quote_asset_index)
        _require_positive_int(self.range_bps, "DepthParameters.range_bps")
        if self.range_bps > 10_000:
            raise ValueError("DepthParameters.range_bps must not exceed 10000")
        _require_positive_int(self.as_of_block, "DepthParameters.as_of_block")

    def chain_anchor_block(self) -> int:
        return self.as_of_block

    def to_wire(self) -> dict[str, object]:
        return {
            "asOfBlock": self.as_of_block,
            "baseAssetIndex": self.base_asset_index,
            "quoteAssetIndex": self.quote_asset_index,
            "rangeBps": self.range_bps,
        }


@dataclass(frozen=True, slots=True)
class ReferencePriceParameters:
    """Exact venue price identity using one closed provider method."""

    base_asset_index: int
    quote_asset_index: int
    method: ReferencePriceMethod
    as_of_block: int

    feature: ClassVar[ExactTargetFeature] = ExactTargetFeature.REFERENCE_PRICE
    anchor_kind: ClassVar[VenueDataAnchorKind] = VenueDataAnchorKind.CHAIN_BLOCK

    def validate(self, binding: ExactVenueBinding) -> None:
        _require_asset_pair(binding, self.base_asset_index, self.quote_asset_index)
        if type(self.method) is not ReferencePriceMethod:
            raise TypeError("ReferencePriceParameters.method must be a ReferencePriceMethod")
        _require_positive_int(self.as_of_block, "ReferencePriceParameters.as_of_block")

    def chain_anchor_block(self) -> int:
        return self.as_of_block

    def to_wire(self) -> dict[str, object]:
        return {
            "asOfBlock": self.as_of_block,
            "baseAssetIndex": self.base_asset_index,
            "method": self.method.value,
            "quoteAssetIndex": self.quote_asset_index,
        }


ExactVenueFeatureParameters = (
    QuoteParameters | TwapParameters | OhlcvParameters | DepthParameters | ReferencePriceParameters
)
_PARAMETER_TYPES = (QuoteParameters, TwapParameters, OhlcvParameters, DepthParameters, ReferencePriceParameters)


@dataclass(frozen=True, slots=True)
class ExactVenueFeatureRequest:
    """One exact-target data request grounded in verified venue evidence."""

    verified_binding: VerifiedVenueBinding
    parameters: ExactVenueFeatureParameters
    feature_contract_version: str
    feature: ExactTargetFeature = field(init=False)
    anchor_kind: VenueDataAnchorKind = field(init=False)
    feature_identity: str = field(init=False)

    def __post_init__(self) -> None:
        if type(self.verified_binding) is not VerifiedVenueBinding:
            raise TypeError("verified_binding must be a VerifiedVenueBinding")
        if type(self.parameters) not in _PARAMETER_TYPES:
            raise TypeError("parameters must be one exact venue feature parameter type")
        version = _require_exact_str(self.feature_contract_version, "feature_contract_version")
        if not _VERSION_RE.fullmatch(version):
            raise ValueError("feature_contract_version is not canonical")
        self.parameters.validate(self.verified_binding.binding)
        requested_block = self.parameters.chain_anchor_block()
        if self.parameters.anchor_kind is VenueDataAnchorKind.OFFCHAIN_SOURCE and requested_block is not None:
            raise ValueError("off-chain exact-data parameters cannot declare a chain anchor block")
        if self.parameters.anchor_kind is VenueDataAnchorKind.CHAIN_BLOCK and requested_block is None:
            raise ValueError("on-chain exact-data parameters must declare a chain anchor block")
        if (
            self.parameters.anchor_kind is VenueDataAnchorKind.CHAIN_BLOCK
            and requested_block != self.verified_binding.evidence.block_number
        ):
            raise ValueError("on-chain exact-data requests must use the verified binding evidence block")
        object.__setattr__(self, "feature", self.parameters.feature)
        object.__setattr__(self, "anchor_kind", self.parameters.anchor_kind)
        object.__setattr__(self, "feature_identity", hashlib.sha256(self.canonical_preimage_bytes()).hexdigest())

    @property
    def binding_hash(self) -> str:
        return self.verified_binding.binding.binding_hash

    def to_preimage_wire(self) -> dict[str, object]:
        return {
            "schemaVersion": EXACT_VENUE_DATA_SCHEMA_VERSION,
            "bindingHash": self.binding_hash,
            "feature": self.feature.value,
            "featureContractVersion": self.feature_contract_version,
            "parameters": self.parameters.to_wire(),
        }

    def canonical_preimage_bytes(self) -> bytes:
        return json.dumps(
            self.to_preimage_wire(),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("utf-8")


@dataclass(frozen=True, slots=True)
class VenueObservationAnchor:
    """Source observation time and optional coherent chain block."""

    observed_at: datetime
    block_number: int | None
    block_hash: str | None

    def __post_init__(self) -> None:
        _require_utc_second(self.observed_at, "VenueObservationAnchor.observed_at")
        if (self.block_number is None) != (self.block_hash is None):
            raise ValueError("block_number and block_hash must both be present or both be absent")
        if self.block_number is not None:
            _require_positive_int(self.block_number, "VenueObservationAnchor.block_number")
            if type(self.block_hash) is not str or not _BLOCK_HASH_RE.fullmatch(self.block_hash):
                raise ValueError("VenueObservationAnchor.block_hash must be canonical lowercase bytes32")


@dataclass(frozen=True, slots=True)
class VenueDataProvenance:
    """Provider identity and upstream observation reference."""

    provider_ref: str
    provider_contract_version: str
    source: str
    source_observation_ref: str | None = None

    def __post_init__(self) -> None:
        provider_ref = _require_exact_str(self.provider_ref, "VenueDataProvenance.provider_ref")
        if not _IMPORT_REF_RE.fullmatch(provider_ref):
            raise ValueError("VenueDataProvenance.provider_ref must be 'absolute.module:attribute'")
        version = _require_exact_str(
            self.provider_contract_version,
            "VenueDataProvenance.provider_contract_version",
        )
        if not _VERSION_RE.fullmatch(version):
            raise ValueError("VenueDataProvenance.provider_contract_version is not canonical")
        _require_exact_str(self.source, "VenueDataProvenance.source")
        if self.source_observation_ref is not None:
            _require_exact_str(self.source_observation_ref, "VenueDataProvenance.source_observation_ref")


@dataclass(frozen=True, slots=True, init=False)
class ExactVenueObservation[T]:
    """A measured value whose identity exactly echoes one verified request."""

    value: T
    feature: ExactTargetFeature
    binding_hash: str
    feature_identity: str
    anchor: VenueObservationAnchor
    provenance: VenueDataProvenance

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        raise TypeError("ExactVenueObservation is created only by from_request()")

    @classmethod
    def from_request(
        cls,
        *,
        request: ExactVenueFeatureRequest,
        value: T,
        anchor: VenueObservationAnchor,
        provenance: VenueDataProvenance,
    ) -> ExactVenueObservation[T]:
        if type(request) is not ExactVenueFeatureRequest:
            raise TypeError("request must be an ExactVenueFeatureRequest")
        if value is None or (type(value) in (str, bytes, tuple, list, dict, set, frozenset) and not value):
            raise ValueError("ExactVenueObservation.value cannot be an empty or unmeasured value")
        if (type(value) is float and not math.isfinite(value)) or (type(value) is Decimal and not value.is_finite()):
            raise ValueError("ExactVenueObservation.value must be finite when numeric")
        if type(anchor) is not VenueObservationAnchor:
            raise TypeError("anchor must be a VenueObservationAnchor")
        if type(provenance) is not VenueDataProvenance:
            raise TypeError("provenance must be a VenueDataProvenance")
        if request.anchor_kind is VenueDataAnchorKind.OFFCHAIN_SOURCE:
            if anchor.block_number is not None:
                raise ValueError("off-chain exact-data observations cannot claim an unrelated chain block")
        elif (
            anchor.block_number != request.verified_binding.evidence.block_number
            or anchor.block_hash != request.verified_binding.evidence.block_hash
        ):
            raise ValueError("observation block identity must match the verified binding evidence")
        instance = object.__new__(cls)
        object.__setattr__(instance, "value", value)
        object.__setattr__(instance, "feature", request.feature)
        object.__setattr__(instance, "binding_hash", request.binding_hash)
        object.__setattr__(instance, "feature_identity", request.feature_identity)
        object.__setattr__(instance, "anchor", anchor)
        object.__setattr__(instance, "provenance", provenance)
        return instance


class VenueDataFailureState(StrEnum):
    """Closed exact-data failure classes."""

    MISMATCHED = "mismatched"
    UNAVAILABLE = "unavailable"
    UNSUPPORTED = "unsupported"


class VenueDataFailureReason(StrEnum):
    """Stable machine-readable exact-data failure reasons."""

    RESPONSE_IDENTITY_MISMATCH = "response_identity_mismatch"
    BINDING_MISMATCH = "binding_mismatch"
    FEATURE_MISMATCH = "feature_mismatch"
    TRANSPORT_UNAVAILABLE = "transport_unavailable"
    PROVIDER_UNAVAILABLE = "provider_unavailable"
    BLOCK_UNAVAILABLE = "block_unavailable"
    STALE_OBSERVATION = "stale_observation"
    REORGED_OBSERVATION = "reorged_observation"
    INCOMPLETE_PROVENANCE = "incomplete_provenance"
    UNSUPPORTED_FEATURE = "unsupported_feature"
    UNSUPPORTED_CHAIN = "unsupported_chain"
    UNSUPPORTED_PROTOCOL = "unsupported_protocol"


_FAILURE_REASONS_BY_STATE = {
    VenueDataFailureState.MISMATCHED: frozenset(
        {
            VenueDataFailureReason.RESPONSE_IDENTITY_MISMATCH,
            VenueDataFailureReason.BINDING_MISMATCH,
            VenueDataFailureReason.FEATURE_MISMATCH,
        }
    ),
    VenueDataFailureState.UNAVAILABLE: frozenset(
        {
            VenueDataFailureReason.TRANSPORT_UNAVAILABLE,
            VenueDataFailureReason.PROVIDER_UNAVAILABLE,
            VenueDataFailureReason.BLOCK_UNAVAILABLE,
            VenueDataFailureReason.STALE_OBSERVATION,
            VenueDataFailureReason.REORGED_OBSERVATION,
            VenueDataFailureReason.INCOMPLETE_PROVENANCE,
        }
    ),
    VenueDataFailureState.UNSUPPORTED: frozenset(
        {
            VenueDataFailureReason.UNSUPPORTED_FEATURE,
            VenueDataFailureReason.UNSUPPORTED_CHAIN,
            VenueDataFailureReason.UNSUPPORTED_PROTOCOL,
        }
    ),
}


@dataclass(frozen=True, slots=True)
class VenueDataFailure:
    """Fail-safe result tied to the exact request that could not be measured."""

    request: ExactVenueFeatureRequest
    state: VenueDataFailureState
    reason_code: VenueDataFailureReason
    detail: str

    def __post_init__(self) -> None:
        if type(self.request) is not ExactVenueFeatureRequest:
            raise TypeError("VenueDataFailure.request must be an ExactVenueFeatureRequest")
        if type(self.state) is not VenueDataFailureState:
            raise TypeError("VenueDataFailure.state must be a VenueDataFailureState")
        if type(self.reason_code) is not VenueDataFailureReason:
            raise TypeError("VenueDataFailure.reason_code must be a VenueDataFailureReason")
        if self.reason_code not in _FAILURE_REASONS_BY_STATE[self.state]:
            raise ValueError(f"reason {self.reason_code.value!r} is invalid for {self.state.value!r}")
        _require_exact_str(self.detail, "VenueDataFailure.detail")

    @property
    def binding_hash(self) -> str:
        return self.request.binding_hash

    @property
    def feature_identity(self) -> str:
        return self.request.feature_identity


ExactVenueDataResult = ExactVenueObservation[Any] | VenueDataFailure


__all__ = [
    "EXACT_VENUE_DATA_SCHEMA_VERSION",
    "DepthParameters",
    "ExactVenueDataResult",
    "ExactVenueFeatureParameters",
    "ExactVenueFeatureRequest",
    "ExactVenueObservation",
    "OhlcvParameters",
    "QuoteParameters",
    "ReferencePriceMethod",
    "ReferencePriceParameters",
    "TwapParameters",
    "VenueDataFailure",
    "VenueDataAnchorKind",
    "VenueDataFailureReason",
    "VenueDataFailureState",
    "VenueDataProvenance",
    "VenueObservationAnchor",
]
