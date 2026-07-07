"""Tests for MarketService reinitialization when chain info arrives late.

Covers the case where the gateway starts without --chains (deployed environments)
and MarketService must upgrade from CoinGecko-only to the full 4-source pricing
stack when chain info arrives via RegisterChains or GetBalance.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.services.market_service import MarketServiceServicer


def _make_settings(**overrides) -> GatewaySettings:
    """Create a GatewaySettings with no chains (simulates deployed env)."""
    defaults = {
        "chains": [],
        "network": "mainnet",
        "coingecko_api_key": "",
    }
    defaults.update(overrides)
    return GatewaySettings(**defaults)


class TestMarketServiceReinitialize:
    """Tests for MarketService.reinitialize()."""

    @pytest.mark.asyncio
    async def test_reinit_upgrades_from_coingecko_only(self):
        """Reinitializing with a chain upgrades from 1 source (CoinGecko) to
        4 sources (Chainlink + Binance + DexScreener + CoinGecko). The
        manual_override safety valve is held separately on the servicer and
        consulted only as a last-resort fallback in GetPrice, so it is not
        counted in the aggregator's source list — keeping it out of the
        median vote prevents a low-confidence override from corrupting a
        live price (Bug 3 of the 0G DogFooding report, 2026-04-16)."""
        settings = _make_settings()
        servicer = MarketServiceServicer(settings)

        # Initial init: no chains -> CoinGecko only (override off by default)
        await servicer._ensure_initialized()
        assert servicer._initialized is True
        assert len(servicer._price_aggregator._sources) == 1
        assert servicer._manual_price_override is None

        # Reinitialize with a chain
        await servicer.reinitialize("arbitrum")

        assert servicer._initialized is True
        assert settings.chains[0] == "arbitrum"
        # Full EVM stack: Chainlink + Binance + DexScreener + CoinGecko
        assert len(servicer._price_aggregator._sources) == 4
        # Override still off (setting wasn't changed)
        assert servicer._manual_price_override is None

    @pytest.mark.asyncio
    async def test_reinit_closes_old_sources(self):
        """Reinitializing should close the old price sources' HTTP sessions.

        VIB-5651 lead decision 2: reinit dedup-closes the per-chain SOURCES via
        ``_close_price_sources`` (each distinct source once, by ``id()``), not the
        aggregator wrapper — shared sources are referenced by multiple
        sub-aggregators, so closing per-aggregator would double-close them. Assert
        the source's ``close`` is awaited and a fresh aggregator is built.
        """
        settings = _make_settings()
        servicer = MarketServiceServicer(settings)

        await servicer._ensure_initialized()
        old_aggregator = servicer._price_aggregator
        # The CoinGecko-only source held by the pre-reinit aggregator.
        old_source = old_aggregator.sources[0]
        old_source.close = AsyncMock()

        await servicer.reinitialize("base")

        old_source.close.assert_awaited_once()
        assert servicer._price_aggregator is not old_aggregator

    @pytest.mark.asyncio
    async def test_reinit_serves_existing_secondary_chain(self):
        """A chain already configured (but not primary) is served by its own
        chain-correct aggregator after reinit.

        VIB-5651 lead decision 4: pricing is keyed by chain, not list index, so
        reinit no longer reorders ``settings.chains`` to force the requested chain
        to index 0 (that reorder was only load-bearing when a single aggregator
        used ``chains[0]``). The observable contract the old reorder protected —
        the requested chain gets a full pricing stack — is preserved: arbitrum
        gets its own 4-source aggregator regardless of position.
        """
        settings = _make_settings(chains=["ethereum", "arbitrum"])
        servicer = MarketServiceServicer(settings)

        await servicer._ensure_initialized()

        await servicer.reinitialize("arbitrum")

        # Both chains still configured, no duplicates; order is no longer forced.
        assert set(settings.chains) == {"ethereum", "arbitrum"}
        assert len(settings.chains) == 2  # no duplicates
        # Arbitrum is served by its own chain-correct aggregator (full EVM stack).
        assert "arbitrum" in servicer._price_aggregators
        assert len(servicer._price_aggregators["arbitrum"].sources) == 4

    @pytest.mark.asyncio
    async def test_reinit_never_exposes_empty_aggregator_map(self):
        """Regression (audit-3195 Important #1): reinit must REBUILD the live
        ``_price_aggregators`` before closing the old sources, so a concurrent
        GetPrice that early-returns from ``_ensure_initialized`` (``_initialized``
        stays True) never observes an empty map → no KeyError in
        ``_aggregator_for``. Assert that at the moment old sources are closed the
        live map is already the fresh, non-empty one and ``_initialized`` held.
        """
        settings = _make_settings(chains=["arbitrum"])
        servicer = MarketServiceServicer(settings)
        await servicer._ensure_initialized()

        observed: dict = {}
        orig_close = servicer._close_aggregator_sources

        async def _spy_close(aggregators):
            # The close runs AFTER the rebuild in the fixed order, so the live
            # map must already be non-empty here (the empty window is gone).
            observed["live_map_size"] = len(servicer._price_aggregators)
            observed["initialized"] = servicer._initialized
            return await orig_close(aggregators)

        servicer._close_aggregator_sources = _spy_close  # type: ignore[method-assign]

        await servicer.reinitialize("base")

        assert observed["live_map_size"] >= 1  # never the empty map
        assert observed["initialized"] is True  # stayed initialized throughout
        # And the post-condition: base is now served.
        assert "base" in servicer._price_aggregators

    @pytest.mark.asyncio
    async def test_reinit_no_duplicate_if_already_primary(self):
        """Reinitializing with the current primary chain should not duplicate it."""
        settings = _make_settings(chains=["arbitrum"])
        servicer = MarketServiceServicer(settings)

        await servicer._ensure_initialized()
        await servicer.reinitialize("arbitrum")

        assert settings.chains == ["arbitrum"]

    @pytest.mark.asyncio
    async def test_reinit_sets_chain_when_empty(self):
        """Reinitializing with no chains configured should set the chain."""
        settings = _make_settings()
        servicer = MarketServiceServicer(settings)

        await servicer.reinitialize("base")

        assert settings.chains == ["base"]
        assert servicer._initialized is True


class TestGetBalanceAutoReinit:
    """Tests for auto-reinit triggered by GetBalance requests."""

    @pytest.mark.asyncio
    async def test_first_balance_triggers_reinit(self):
        """GetBalance with a chain should trigger reinit when no chains configured."""
        settings = _make_settings()
        servicer = MarketServiceServicer(settings)

        # Pre-initialize: CoinGecko only (manual_override held separately)
        await servicer._ensure_initialized()
        assert len(servicer._price_aggregator._sources) == 1

        # Mock the gRPC context and balance provider
        mock_context = MagicMock()
        mock_request = MagicMock()
        mock_request.chain = "arbitrum"
        mock_request.token = "USDC"
        mock_request.wallet_address = "0x" + "a" * 40

        with patch.object(servicer, "_get_balance_provider", new_callable=AsyncMock) as mock_provider:
            mock_balance = MagicMock()
            mock_balance.get_balance = AsyncMock(return_value={"balance": "100", "decimals": 6})
            mock_provider.return_value = mock_balance

            await servicer.GetBalance(mock_request, mock_context)

        # After GetBalance, should have upgraded to full EVM stack (override still separate)
        assert settings.chains[0] == "arbitrum"
        assert len(servicer._price_aggregator._sources) == 4

    @pytest.mark.asyncio
    async def test_no_reinit_when_chain_already_configured(self):
        """GetBalance should not reinit when chains are already configured."""
        settings = _make_settings(chains=["arbitrum"])
        servicer = MarketServiceServicer(settings)

        await servicer._ensure_initialized()

        with patch.object(servicer, "reinitialize", new_callable=AsyncMock) as mock_reinit:
            mock_context = MagicMock()
            mock_request = MagicMock()
            mock_request.chain = "arbitrum"
            mock_request.token = "USDC"
            mock_request.wallet_address = "0x" + "a" * 40

            with patch.object(servicer, "_get_balance_provider", new_callable=AsyncMock) as mock_provider:
                mock_balance = MagicMock()
                mock_balance.get_balance = AsyncMock(return_value={"balance": "100", "decimals": 6})
                mock_provider.return_value = mock_balance

                await servicer.GetBalance(mock_request, mock_context)

            mock_reinit.assert_not_awaited()
