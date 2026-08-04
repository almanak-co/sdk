"""Focused factory contracts for integration-manifest gateway capabilities."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from almanak.integrations import INTEGRATION_REGISTRY
from almanak.integrations._base import PriceSourceScope
from almanak.integrations.binance.gateway.factory import BinancePriceSourceFactory
from almanak.integrations.chainlink.gateway.factory import ChainlinkPriceSourceFactory
from almanak.integrations.coingecko.gateway.client import CoinGeckoIntegration
from almanak.integrations.coingecko.gateway.factory import CoinGeckoClientFactory, CoinGeckoPriceSourceFactory
from almanak.integrations.coingecko.gateway.price_source import CoinGeckoPriceSource
from almanak.integrations.dexscreener.gateway.factory import DexScreenerPriceSourceFactory
from almanak.integrations.okx.gateway.client import OkxIntegration
from almanak.integrations.thegraph.gateway.client import TheGraphIntegration
from almanak.integrations.thegraph.gateway.factory import TheGraphClientFactory


def test_binance_price_source_factory_selection() -> None:
    factory = BinancePriceSourceFactory()

    assert factory.name == "binance"
    assert factory.scope is PriceSourceScope.SHARED
    assert factory.order == 20
    assert factory.supports(None) is False
    assert factory.supports("solana") is False
    # Cross-provider exclusion is manifest policy, not connector discovery
    # hidden inside this provider's supports() method.
    assert factory.supports("hyperevm") is True
    assert factory.supports("arbitrum") is True
    assert INTEGRATION_REGISTRY.price_source_policy("binance") == (
        None,
        frozenset({"venue_oracle"}),
    )


@pytest.mark.parametrize("factory", [ChainlinkPriceSourceFactory(), DexScreenerPriceSourceFactory()])
def test_chain_scoped_price_factories_reject_missing_chain(factory) -> None:
    with pytest.raises(ValueError, match="requires a chain"):
        factory.build(chain=None, settings=SimpleNamespace())


def test_hypercore_factory_name_and_exclusivity_policy_are_manifest_owned() -> None:
    factory = next(
        factory for factory in INTEGRATION_REGISTRY.gateway_price_source_factories() if factory.name == "hypercore"
    )
    assert factory.name == "hypercore"
    assert INTEGRATION_REGISTRY.price_source_policy("hypercore") == ("venue_oracle", frozenset())


def test_coingecko_price_source_factory_contract() -> None:
    settings = SimpleNamespace(coingecko_api_key="cg-key")
    factory = CoinGeckoPriceSourceFactory()

    source = factory.build(chain="arbitrum", settings=settings)

    assert factory.name == "coingecko"
    assert factory.scope is PriceSourceScope.SHARED
    assert factory.order == 40
    assert factory.supports(None) is True
    assert factory.supports("solana") is True
    assert isinstance(source, CoinGeckoPriceSource)
    assert source._api_key == "cg-key"
    assert source._cache_ttl == 30


def test_coingecko_client_factory_contract() -> None:
    settings = SimpleNamespace(coingecko_api_key="cg-key")
    factory = CoinGeckoClientFactory()

    client = factory.build(settings=settings)

    assert factory.name == "coingecko"
    assert isinstance(client, CoinGeckoIntegration)
    assert client._api_key == "cg-key"


def test_thegraph_client_factory_propagates_configured_and_missing_keys() -> None:
    factory = TheGraphClientFactory()

    keyed = factory.build(settings=SimpleNamespace(thegraph_api_key="graph-key"))
    keyless = factory.build(settings=SimpleNamespace(thegraph_api_key=None))

    assert factory.name == "thegraph"
    assert isinstance(keyed, TheGraphIntegration)
    assert keyed._api_key == "graph-key"
    assert isinstance(keyless, TheGraphIntegration)
    assert keyless._api_key is None


def test_okx_portfolio_factory_resolves_from_registry_and_propagates_settings() -> None:
    factory = INTEGRATION_REGISTRY.gateway_portfolio_provider_factory("okx")

    provider = factory.build(api_key="okx-key", cache_ttl=17)

    assert factory.name == "okx"
    assert factory.requires_api_key is False
    assert isinstance(provider, OkxIntegration)
    assert provider._api_key == "okx-key"
    assert provider.default_cache_ttl == 17
