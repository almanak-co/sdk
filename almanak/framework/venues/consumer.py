"""Fail-closed dispatch for exact-venue data providers.

This is the boundary between the typed exact-data contract and connector-owned
readers.  A provider used here must already be venue-aware and return an
``ExactVenueObservation`` created from the request (or a ``VenueDataFailure``).
Legacy pair-, symbol-, and address-scoped readers cannot be adapted implicitly;
they therefore cannot accidentally satisfy an exact-data request.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Protocol

from .data import (
    ExactVenueDataResult,
    ExactVenueFeatureRequest,
    ExactVenueObservation,
    VenueDataFailure,
    VenueDataFailureReason,
    VenueDataFailureState,
)
from .provider import ExactVenueDataGateway

logger = logging.getLogger(__name__)


class ExactVenueDataProvider(Protocol):
    """Connector-owned exact reader.

    Implementations must not discover or substitute another venue.  They may
    return a typed failure when the requested feature is unavailable.
    """

    def read_exact(self, request: ExactVenueFeatureRequest) -> ExactVenueDataResult: ...


class ExactVenueDataRegistryAdapter:
    """Adapt the connector registry's gateway-aware provider contract."""

    def __init__(
        self,
        observe: Callable[[ExactVenueFeatureRequest, ExactVenueDataGateway], ExactVenueDataResult],
        gateway: ExactVenueDataGateway,
    ) -> None:
        self._observe = observe
        self._gateway = gateway

    def read_exact(self, request: ExactVenueFeatureRequest) -> ExactVenueDataResult:
        try:
            return self._observe(request, self._gateway)
        except Exception:
            logger.error("Exact venue data provider is unavailable", exc_info=True)
            raise


class ExactVenueDataConsumer:
    """Consume one exact request without permitting legacy fallback."""

    def __init__(self, provider: ExactVenueDataProvider) -> None:
        self._provider = provider

    def read(self, request: ExactVenueFeatureRequest) -> ExactVenueDataResult:
        """Read the requested feature, converting provider-contract violations to failure."""
        if type(request) is not ExactVenueFeatureRequest:
            raise TypeError("request must be an ExactVenueFeatureRequest")

        try:
            result = self._provider.read_exact(request)
        except Exception as exc:  # provider transport/implementation failures are unmeasured
            return VenueDataFailure(
                request=request,
                state=VenueDataFailureState.UNAVAILABLE,
                reason_code=VenueDataFailureReason.PROVIDER_UNAVAILABLE,
                detail=f"exact venue data provider failed: {type(exc).__name__}: {exc}",
            )

        if type(result) is VenueDataFailure:
            if result.request is not request:
                return VenueDataFailure(
                    request=request,
                    state=VenueDataFailureState.MISMATCHED,
                    reason_code=VenueDataFailureReason.RESPONSE_IDENTITY_MISMATCH,
                    detail="provider failure is associated with a different exact request",
                )
            return result

        if type(result) is not ExactVenueObservation:
            return VenueDataFailure(
                request=request,
                state=VenueDataFailureState.UNAVAILABLE,
                reason_code=VenueDataFailureReason.INCOMPLETE_PROVENANCE,
                detail="provider did not return an exact venue observation or typed failure",
            )

        if (
            result.feature is not request.feature
            or result.binding_hash != request.binding_hash
            or result.feature_identity != request.feature_identity
        ):
            return VenueDataFailure(
                request=request,
                state=VenueDataFailureState.MISMATCHED,
                reason_code=VenueDataFailureReason.RESPONSE_IDENTITY_MISMATCH,
                detail="provider observation does not echo the exact request identity",
            )
        return result


__all__ = ["ExactVenueDataConsumer", "ExactVenueDataProvider", "ExactVenueDataRegistryAdapter"]
