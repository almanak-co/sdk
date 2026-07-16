"""Tests for manifest-driven provider routing in PerpBacktestAdapter."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

from almanak.framework.backtesting.adapters.perp_adapter import (
    PerpBacktestAdapter,
    PerpBacktestConfig,
)


def _make_adapter(
    *,
    funding_rate_source: str = "historical",
    injected_providers: dict[str, Any] | None = None,
) -> PerpBacktestAdapter:
    """Create a minimal adapter with historical funding enabled."""
    config = PerpBacktestConfig(
        strategy_type="perp",
        funding_rate_source=funding_rate_source,
        protocol="gmx",
        chain="arbitrum",
    )
    return PerpBacktestAdapter(config, injected_providers=injected_providers)


class TestGetProviderForProtocol:
    """_get_provider_for_protocol routes through FundingHistoryRegistry."""

    def test_alias_routes_to_cached_provider(self) -> None:
        mock_provider = MagicMock()
        adapter = _make_adapter(injected_providers={"gmx": mock_provider})

        assert adapter._get_provider_for_protocol("gmx") is mock_provider
        assert adapter._get_provider_for_protocol("gmx_v2") is mock_provider
        assert adapter._get_provider_for_protocol("GMX") is mock_provider

    def test_canonical_venue_routes_to_cached_provider(self) -> None:
        mock_provider = MagicMock()
        adapter = _make_adapter(injected_providers={"hyperliquid": mock_provider})

        assert adapter._get_provider_for_protocol("hyperliquid") is mock_provider
        assert adapter._get_provider_for_protocol("HYPERLIQUID") is mock_provider

    def test_unknown_protocol_returns_none(self) -> None:
        adapter = _make_adapter()

        assert adapter._get_provider_for_protocol("some_unknown_protocol") is None

    def test_unknown_protocol_does_not_poison_provider_cache(self) -> None:
        adapter = _make_adapter()

        assert adapter._get_provider_for_protocol("totally_unknown") is None
        assert adapter._provider_cache == {}
        assert adapter._provider_tried == set()


class TestProviderCaching:
    """Repeated calls return the same generic cache entry."""

    def test_provider_cached_across_aliases(self) -> None:
        adapter = _make_adapter()

        first = adapter._get_provider_for_protocol("gmx")
        second = adapter._get_provider_for_protocol("gmx_v2")

        assert first is not None
        assert first is second
        assert adapter._provider_tried == {("gmx_v2", "arbitrum")}
        assert adapter._provider_cache[("gmx_v2", "arbitrum")] is first

    def test_injected_provider_seeds_generic_cache(self) -> None:
        sentinel = MagicMock()
        adapter = _make_adapter(injected_providers={"hyperliquid": sentinel})

        assert adapter._provider_tried == {("hyperliquid", "*")}
        assert adapter._provider_cache[("hyperliquid", "*")] is sentinel

    def test_provider_cache_starts_empty_without_injections(self) -> None:
        adapter = _make_adapter()

        assert adapter._provider_tried == set()
        assert adapter._provider_cache == {}


class TestDeclaredChainCanonicalization:
    """Declared funding chains canonicalize before comparison (CodeRabbit
    review on #3270): a manifest declaring the registered alias "avax" must
    serve the canonical "avalanche" run chain — chain identity is never
    raw-string compared (the round-5 lesson, applied to the DECLARED side)."""

    def test_alias_declared_chain_serves_canonical_run_chain(self, monkeypatch) -> None:
        from almanak.connectors._strategy_base.funding_history_registry import (
            FundingHistoryRegistry,
        )

        monkeypatch.setattr(FundingHistoryRegistry, "declared_chains", classmethod(lambda cls, protocol: ("avax",)))

        mock_provider = MagicMock()
        mock_provider.chain = "avalanche"
        adapter = _make_adapter(injected_providers={"gmx_v2:avalanche": mock_provider})

        # Pre-fix: declared {"avax"} != canonical "avalanche" -> the injection
        # was REJECTED at seeding and the lookup fell back to None.
        assert adapter._get_provider_for_protocol("gmx_v2", "avalanche") is mock_provider

    def test_alias_declared_chain_still_rejects_undeclared(self, monkeypatch) -> None:
        from almanak.connectors._strategy_base.funding_history_registry import (
            FundingHistoryRegistry,
        )

        monkeypatch.setattr(FundingHistoryRegistry, "declared_chains", classmethod(lambda cls, protocol: ("avax",)))

        mock_provider = MagicMock()
        mock_provider.chain = "arbitrum"
        adapter = _make_adapter(injected_providers={"gmx_v2:arbitrum": mock_provider})

        # Canonicalization must not LOOSEN the contract: arbitrum stays
        # undeclared and both the seeding and the lookup reject it.
        assert adapter._get_provider_for_protocol("gmx_v2", "arbitrum") is None
