"""Branch coverage for AcrossBridgeAdapter._build_routes.

_build_routes expands the static chain / token / completion-time constants
into the full BridgeRoute matrix. Covered: same-chain pair skipping, the
empty-token-list skip, the ETH-from-WETH synthesis (and its two negative
arms), and the completion-time lookup with its 180s default. Constants are
monkeypatched on the adapter module for the synthetic-token branches. No
network.
"""

from unittest.mock import MagicMock

import pytest

from almanak.connectors.across.adapter import (
    ACROSS_CHAIN_IDS,
    ACROSS_SUPPORTED_TOKENS,
    AcrossBridgeAdapter,
)


@pytest.fixture
def adapter():
    return AcrossBridgeAdapter(token_resolver=MagicMock())


class TestBuildRoutes:
    def test_builds_full_matrix_of_directed_chain_pairs(self, adapter):
        routes = adapter._build_routes()

        chains = list(ACROSS_CHAIN_IDS.keys())
        assert len(routes) == len(chains) * (len(chains) - 1)
        # Same-chain pairs are skipped.
        assert all(route.from_chain != route.to_chain for route in routes)
        # Directed: both orientations of every pair are present.
        pairs = {(route.from_chain, route.to_chain) for route in routes}
        assert ("arbitrum", "ethereum") in pairs
        assert ("ethereum", "arbitrum") in pairs
        assert all(route.is_active for route in routes)

    def test_eth_not_duplicated_when_already_supported(self, adapter):
        # Default constants already include ETH -> the synthesis arm is a no-op.
        assert "ETH" in ACROSS_SUPPORTED_TOKENS
        routes = adapter._build_routes()
        for route in routes:
            assert route.tokens.count("ETH") == 1
            assert route.tokens == ACROSS_SUPPORTED_TOKENS

    def test_completion_time_from_constants_and_default(self, adapter):
        routes = {(route.from_chain, route.to_chain): route for route in adapter._build_routes()}

        # Known pairs use the constants table.
        assert routes[("arbitrum", "ethereum")].estimated_time_seconds == 240
        assert routes[("arbitrum", "optimism")].estimated_time_seconds == 120
        # Pairs absent from the table (linea/zksync rows) fall back to 180s.
        assert routes[("linea", "zksync")].estimated_time_seconds == 180
        assert routes[("zksync", "ethereum")].estimated_time_seconds == 180

    def test_eth_synthesized_when_only_weth_supported(self, adapter, monkeypatch):
        monkeypatch.setattr(
            "almanak.connectors.across.adapter.ACROSS_SUPPORTED_TOKENS", ["WETH", "USDC"]
        )

        routes = adapter._build_routes()

        assert routes
        for route in routes:
            assert route.tokens == ["WETH", "USDC", "ETH"]

    def test_no_eth_synthesis_without_weth(self, adapter, monkeypatch):
        monkeypatch.setattr("almanak.connectors.across.adapter.ACROSS_SUPPORTED_TOKENS", ["USDC"])

        routes = adapter._build_routes()

        assert routes
        for route in routes:
            assert route.tokens == ["USDC"]

    def test_empty_token_list_yields_no_routes(self, adapter, monkeypatch):
        monkeypatch.setattr("almanak.connectors.across.adapter.ACROSS_SUPPORTED_TOKENS", [])

        assert adapter._build_routes() == []

    def test_token_lists_are_independent_copies(self, adapter):
        routes = adapter._build_routes()

        routes[0].tokens.append("FAKE")

        assert "FAKE" not in routes[1].tokens
        assert "FAKE" not in ACROSS_SUPPORTED_TOKENS
