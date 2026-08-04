"""Discovery and validation for integration manifests."""

from __future__ import annotations

import importlib
import pkgutil
import threading
from collections.abc import Iterable
from typing import Any

from almanak.core.chains._helpers import external_chain_id_map

from .capabilities import (
    GatewayApiClientFactory,
    GatewayOracleReaderFactory,
    GatewayPortfolioProviderFactory,
    GatewayPriceSourceFactory,
)
from .descriptor import Integration


class IntegrationRegistry:
    """Registry of self-contained provider integration manifests."""

    def __init__(self) -> None:
        self._integrations: dict[str, Integration] = {}
        self._discovered = False
        self._discovery_lock = threading.Lock()

    def register(self, integration: Integration) -> None:
        key = integration.name.strip().lower()
        existing = self._integrations.get(key)
        if existing is not None and existing != integration:
            raise ValueError(f"Duplicate integration manifest for {integration.name!r}")
        self._integrations[key] = integration

    def discover(self) -> None:
        """Import only ``<provider>.integration`` manifest modules."""
        if self._discovered:
            return
        with self._discovery_lock:
            if self._discovered:
                return
            package = importlib.import_module("almanak.integrations")
            discovered: dict[str, Integration] = {}
            for module in sorted(pkgutil.iter_modules(package.__path__), key=lambda item: item.name):
                # Top-level support modules (for example ``chains.py``) are
                # registry infrastructure, not provider packages. Every
                # concrete package must carry a manifest and fails closed.
                if module.name.startswith("_") or not module.ispkg:
                    continue
                manifest_module = importlib.import_module(f"almanak.integrations.{module.name}.integration")
                manifest = getattr(manifest_module, "INTEGRATION", None)
                if not isinstance(manifest, Integration):
                    raise TypeError(
                        f"almanak.integrations.{module.name}.integration must export INTEGRATION: Integration"
                    )
                if manifest.name != module.name:
                    raise ValueError(f"Integration package {module.name!r} exported mismatched name {manifest.name!r}")
                existing = discovered.get(manifest.name) or self._integrations.get(manifest.name)
                if existing is not None and existing != manifest:
                    raise ValueError(f"Duplicate integration manifest for {manifest.name!r}")
                discovered[manifest.name] = manifest

            # Publish atomically only after every provider package imports and
            # validates. Missing integration.py is a packaging error, not an
            # optional provider: fail loudly and leave discovery retryable.
            self._integrations.update(discovered)
            self._discovered = True

    def all(self) -> tuple[Integration, ...]:
        self.discover()
        return tuple(self._integrations[name] for name in sorted(self._integrations))

    def get(self, name: str) -> Integration:
        self.discover()
        return self._integrations[name.strip().lower()]

    def chain_id_map(self, name: str) -> dict[str, str]:
        """Return the chain-owned identifier projection for one integration."""
        integration = self.get(name)
        return external_chain_id_map(integration.name)

    def asset_id_map(self, name: str) -> dict[str, str]:
        """Return a copy of one provider's symbol-to-asset-id catalogue."""
        integration = self.get(name)
        return dict(integration.asset_ids or {})

    def market_symbol_map(self, name: str) -> dict[tuple[str, str], str]:
        """Return a copy of one venue's canonical pair-to-market catalogue."""
        return dict(self.get(name).market_symbols or {})

    def gateway_price_source_factories(self) -> tuple[GatewayPriceSourceFactory, ...]:
        """Instantiate and validate all manifest-published price factories."""
        factories: list[GatewayPriceSourceFactory] = []
        names: set[str] = set()
        refs: Iterable[tuple[Integration, Any]] = (
            (integration, integration.gateway_price_source)
            for integration in self.all()
            if integration.gateway_price_source is not None
        )
        for integration, ref in sorted(refs, key=lambda item: (item[1].order, item[1].module, item[1].attribute)):
            factory = ref.instantiate()
            if not isinstance(factory, GatewayPriceSourceFactory):
                raise TypeError(f"{ref.module}.{ref.attribute} does not implement GatewayPriceSourceFactory")
            if factory.name != integration.name:
                raise ValueError(f"Integration {integration.name!r} published mismatched price source {factory.name!r}")
            if factory.order != ref.order:
                raise ValueError(
                    f"Integration {integration.name!r} price source order mismatch: "
                    f"manifest={ref.order}, factory={factory.order}"
                )
            if factory.name in names:
                raise ValueError(f"Duplicate gateway price source factory name {factory.name!r}")
            names.add(factory.name)
            factories.append(factory)
        return tuple(sorted(factories, key=lambda factory: (factory.order, factory.name)))

    def price_source_policy(self, name: str) -> tuple[str | None, frozenset[str]]:
        """Return manifest-owned exclusivity policy for a price source."""
        integration = self.get(name)
        return integration.price_source_exclusive_group, integration.price_source_blocked_by_groups

    def gateway_oracle_reader_factory(self, name: str) -> GatewayOracleReaderFactory:
        """Load one integration's provider-exact oracle reader factory."""
        integration = self.get(name)
        ref = integration.gateway_oracle_reader
        if ref is None:
            raise KeyError(f"Integration {integration.name!r} has no gateway oracle reader")
        factory = ref.instantiate()
        if not isinstance(factory, GatewayOracleReaderFactory):
            raise TypeError(f"{ref.module}.{ref.attribute} does not implement GatewayOracleReaderFactory")
        if factory.name != integration.name:
            raise ValueError(f"Integration {integration.name!r} published mismatched oracle reader {factory.name!r}")
        return factory

    def gateway_api_client_factory(self, name: str) -> GatewayApiClientFactory:
        """Load one integration's gateway API-client factory."""
        integration = self.get(name)
        ref = integration.gateway_api_client
        if ref is None:
            raise KeyError(f"Integration {integration.name!r} has no gateway API client")
        factory = ref.instantiate()
        if not isinstance(factory, GatewayApiClientFactory) or factory.name != integration.name:
            raise TypeError(f"Invalid gateway API-client factory for integration {integration.name!r}")
        return factory

    def gateway_portfolio_provider_factory(self, name: str) -> GatewayPortfolioProviderFactory:
        """Load one integration's normalized portfolio-provider factory."""
        integration = self.get(name)
        ref = integration.gateway_portfolio_provider
        if ref is None:
            raise KeyError(f"Integration {integration.name!r} has no gateway portfolio provider")
        factory = ref.instantiate()
        if not isinstance(factory, GatewayPortfolioProviderFactory) or factory.name != integration.name:
            raise TypeError(f"Invalid gateway portfolio-provider factory for integration {integration.name!r}")
        return factory


INTEGRATION_REGISTRY = IntegrationRegistry()
