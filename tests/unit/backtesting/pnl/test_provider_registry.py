"""Tests for historical data provider registry selection."""

from collections.abc import Iterator
from copy import deepcopy

import pytest

from almanak.framework.backtesting.pnl.providers.registry import ProviderRegistry


class AlphaProvider:
    """Simple provider class used for registry tests."""


class BetaProvider:
    """Simple provider class used for registry tests."""


class GammaProvider:
    """Simple provider class used for registry tests."""


@pytest.fixture(autouse=True)
def isolated_registry() -> Iterator[None]:
    """Isolate the global provider registry from built-in registrations."""
    original = ProviderRegistry._providers.copy()
    ProviderRegistry.clear()
    try:
        yield
    finally:
        ProviderRegistry.clear()
        ProviderRegistry._providers.update(original)


def test_register_does_not_mutate_metadata() -> None:
    metadata = {
        "description": "Primary oracle",
        "supported_tokens": ["ETH"],
        "supported_chains": ["arbitrum"],
        "rate_limit": "strict",
    }
    original_metadata = deepcopy(metadata)

    ProviderRegistry.register(
        "alpha",
        AlphaProvider,
        priority=10,
        metadata=metadata,
    )

    assert metadata == original_metadata
    registered = ProviderRegistry.get_metadata("alpha")
    assert registered is not None
    assert registered.description == "Primary oracle"
    assert registered.supported_tokens == ["ETH"]
    assert registered.supported_chains == ["arbitrum"]
    assert registered.extra == {"rate_limit": "strict"}


def test_get_best_provider_matches_chain_and_token_case_insensitively() -> None:
    ProviderRegistry.register("fallback", AlphaProvider, priority=100)
    ProviderRegistry.register(
        "arbitrum_eth",
        BetaProvider,
        priority=10,
        metadata={"supported_tokens": ["ETH"], "supported_chains": ["arbitrum"]},
    )
    ProviderRegistry.register(
        "base_eth",
        GammaProvider,
        priority=1,
        metadata={"supported_tokens": ["ETH"], "supported_chains": ["base"]},
    )

    best = ProviderRegistry.get_best_provider(token="eth", chain="ARBITRUM")

    assert best is not None
    assert best.name == "arbitrum_eth"
    assert best.provider_class is BetaProvider


def test_get_best_provider_returns_none_without_wildcard_or_match() -> None:
    ProviderRegistry.register(
        "base_eth",
        AlphaProvider,
        priority=1,
        metadata={"supported_tokens": ["ETH"], "supported_chains": ["base"]},
    )
    ProviderRegistry.register(
        "arbitrum_usdc",
        BetaProvider,
        priority=2,
        metadata={"supported_tokens": ["USDC"], "supported_chains": ["arbitrum"]},
    )

    assert ProviderRegistry.get_best_provider(token="WETH", chain="optimism") is None


def test_chain_and_token_queries_keep_priority_order() -> None:
    ProviderRegistry.register("fallback", AlphaProvider, priority=50)
    ProviderRegistry.register(
        "arbitrum_eth",
        BetaProvider,
        priority=20,
        metadata={"supported_tokens": ["ETH"], "supported_chains": ["arbitrum"]},
    )
    ProviderRegistry.register(
        "arbitrum_usdc",
        GammaProvider,
        priority=10,
        metadata={"supported_tokens": ["USDC"], "supported_chains": ["arbitrum"]},
    )

    assert [meta.name for meta in ProviderRegistry.get_for_chain("ARBITRUM")] == [
        "arbitrum_usdc",
        "arbitrum_eth",
        "fallback",
    ]
    assert [meta.name for meta in ProviderRegistry.get_for_token("eth")] == [
        "arbitrum_eth",
        "fallback",
    ]
