"""Lazy exact-protocol registry for no-fallback venue data providers."""

from __future__ import annotations

from collections.abc import Iterable

from almanak.connectors._connector import CONNECTOR_REGISTRY, Connector, ExactVenueDataProviderDecl
from almanak.framework.venues import (
    BaseExactVenueDataProvider,
    ExactVenueDataGateway,
    ExactVenueFeatureRequest,
    ExactVenueObservation,
    VenueDataFailure,
    VenueDataFailureReason,
    VenueDataFailureState,
)


class ExactVenueDataRegistryError(ValueError):
    """A provider declaration or result violates the exact-data contract."""


class ExactVenueDataProviderRegistry:
    """Dispatch exact requests by verified protocol, with no fallback lane."""

    def __init__(self, connectors: Iterable[Connector] | None = None) -> None:
        selected = CONNECTOR_REGISTRY.with_exact_venue_data_providers() if connectors is None else tuple(connectors)
        if any(type(connector) is not Connector for connector in selected):
            raise TypeError("connectors must contain exact Connector values")
        self._declarations = self._index(tuple(selected))

    @staticmethod
    def _index(connectors: tuple[Connector, ...]) -> dict[str, ExactVenueDataProviderDecl]:
        declarations: dict[str, ExactVenueDataProviderDecl] = {}
        owners: dict[str, str] = {}
        for connector in connectors:
            for declaration in connector.exact_venue_data_providers:
                previous = owners.get(declaration.protocol)
                if previous is not None:
                    raise ExactVenueDataRegistryError(
                        f"exact-data protocol {declaration.protocol!r} is declared by both "
                        f"{previous!r} and {connector.name!r}"
                    )
                owners[declaration.protocol] = connector.name
                declarations[declaration.protocol] = declaration
        return declarations

    def supported_protocols(self) -> tuple[str, ...]:
        return tuple(sorted(self._declarations))

    def observe(
        self,
        request: ExactVenueFeatureRequest,
        gateway: ExactVenueDataGateway,
    ) -> ExactVenueObservation[object] | VenueDataFailure:
        if type(request) is not ExactVenueFeatureRequest:
            raise TypeError("request must be an exact ExactVenueFeatureRequest")
        binding = request.verified_binding.binding
        declaration = self._declarations.get(binding.protocol)
        if declaration is None:
            return VenueDataFailure(
                request=request,
                state=VenueDataFailureState.UNSUPPORTED,
                reason_code=VenueDataFailureReason.UNSUPPORTED_PROTOCOL,
                detail=f"no exact-data provider is declared for protocol {binding.protocol!r}",
            )
        if binding.chain not in declaration.chains:
            return VenueDataFailure(
                request=request,
                state=VenueDataFailureState.UNSUPPORTED,
                reason_code=VenueDataFailureReason.UNSUPPORTED_CHAIN,
                detail=f"exact-data provider {binding.protocol!r} does not declare chain {binding.chain!r}",
            )
        if request.feature not in declaration.features:
            return VenueDataFailure(
                request=request,
                state=VenueDataFailureState.UNSUPPORTED,
                reason_code=VenueDataFailureReason.UNSUPPORTED_FEATURE,
                detail=f"exact-data provider {binding.protocol!r} does not declare feature {request.feature.value!r}",
            )
        provider_class = declaration.provider.load()
        if not isinstance(provider_class, type) or not issubclass(provider_class, BaseExactVenueDataProvider):
            raise ExactVenueDataRegistryError(
                f"{declaration.provider_ref} is not a BaseExactVenueDataProvider subclass"
            )
        result = provider_class().observe(request, gateway)
        if type(result) is VenueDataFailure:
            if result.request is not request:
                raise ExactVenueDataRegistryError("exact-data failure does not retain its exact request")
            return result
        if type(result) is not ExactVenueObservation:
            raise ExactVenueDataRegistryError("provider returned neither an exact observation nor a typed failure")
        if (
            result.binding_hash != request.binding_hash
            or result.feature_identity != request.feature_identity
            or result.feature is not request.feature
        ):
            raise ExactVenueDataRegistryError("exact-data observation does not match its request identity")
        if (
            result.provenance.provider_ref != declaration.provider_ref
            or result.provenance.provider_contract_version != declaration.contract_version
        ):
            raise ExactVenueDataRegistryError("exact-data observation does not match its provider declaration")
        return result


__all__ = ["ExactVenueDataProviderRegistry", "ExactVenueDataRegistryError"]
