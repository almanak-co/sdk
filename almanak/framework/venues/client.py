"""Strategy-side facade for exact venue data observations."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from .consumer import ExactVenueDataConsumer, ExactVenueDataRegistryAdapter
from .data import (
    ExactVenueDataResult,
    ExactVenueFeatureRequest,
    VenueDataFailure,
    VenueDataFailureReason,
    VenueDataFailureState,
)
from .gateway import GatewayClientExactVenueDataGateway

if TYPE_CHECKING:
    from almanak.framework.gateway_client import GatewayClient


logger = logging.getLogger(__name__)


def observe_exact_venue_data(
    request: ExactVenueFeatureRequest,
    gateway_client: GatewayClient,
) -> ExactVenueDataResult:
    """Execute one connector-declared exact request through the SDK gateway.

    Provider discovery is delayed until invocation so importing the neutral
    venue contract does not eagerly discover or import concrete connectors.
    """
    if type(request) is not ExactVenueFeatureRequest:
        raise TypeError("request must be an exact ExactVenueFeatureRequest")
    try:
        gateway = GatewayClientExactVenueDataGateway(gateway_client)
    except (AttributeError, TypeError, ValueError) as exc:
        return VenueDataFailure(
            request=request,
            state=VenueDataFailureState.UNAVAILABLE,
            reason_code=VenueDataFailureReason.PROVIDER_UNAVAILABLE,
            detail=f"exact venue data gateway is unavailable: {exc}",
        )
    try:
        from almanak.connectors._strategy_base.exact_venue_data_registry import ExactVenueDataProviderRegistry

        registry = ExactVenueDataProviderRegistry()
        consumer = ExactVenueDataConsumer(ExactVenueDataRegistryAdapter(registry.observe, gateway))
        return consumer.read(request)
    except Exception as exc:
        # This is the public fail-closed boundary. Lazy connector discovery,
        # ImportRef loading, provider construction, and provider contract
        # validation must never leak a raw exception to an SDK caller or
        # accidentally invite a legacy fallback.
        logger.error("Exact venue data provider is unavailable", exc_info=True)
        return VenueDataFailure(
            request=request,
            state=VenueDataFailureState.UNAVAILABLE,
            reason_code=VenueDataFailureReason.PROVIDER_UNAVAILABLE,
            detail=f"exact venue data provider is unavailable: {type(exc).__name__}: {exc}",
        )


__all__ = ["observe_exact_venue_data"]
