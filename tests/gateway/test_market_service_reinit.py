"""Tests for MarketService reinitialization when chain info arrives late.

Covers the case where the gateway starts without --chains (deployed environments)
and MarketService must upgrade from CoinGecko-only to the full 4-source pricing
stack when chain info arrives via RegisterChains or GetBalance.
"""

import asyncio
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
        """Reinitializing with a chain should upgrade from 1 source to 4 sources."""
        settings = _make_settings()
        servicer = MarketServiceServicer(settings)

        # Initial init: no chains -> CoinGecko only
        await servicer._ensure_initialized()
        assert servicer._initialized is True
        assert len(servicer._price_aggregator._sources) == 1

        # Reinitialize with a chain
        await servicer.reinitialize("arbitrum")

        assert servicer._initialized is True
        assert settings.chains[0] == "arbitrum"
        # Full EVM stack: Chainlink + Binance + DexScreener + CoinGecko
        assert len(servicer._price_aggregator._sources) == 4

    @pytest.mark.asyncio
    async def test_reinit_closes_old_aggregator(self):
        """Reinitializing should close the old price aggregator's HTTP sessions."""
        settings = _make_settings()
        servicer = MarketServiceServicer(settings)

        await servicer._ensure_initialized()
        old_aggregator = servicer._price_aggregator
        old_aggregator.close = AsyncMock()

        await servicer.reinitialize("base")

        old_aggregator.close.assert_awaited_once()
        assert servicer._price_aggregator is not old_aggregator

    @pytest.mark.asyncio
    async def test_reinit_moves_existing_chain_to_front(self):
        """If the chain is already in settings but not at index 0, move it to front."""
        settings = _make_settings(chains=["ethereum", "arbitrum"])
        servicer = MarketServiceServicer(settings)

        await servicer._ensure_initialized()
        assert settings.chains[0] == "ethereum"

        await servicer.reinitialize("arbitrum")

        assert settings.chains[0] == "arbitrum"
        assert settings.chains[1] == "ethereum"
        assert len(settings.chains) == 2  # no duplicates

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

        # Pre-initialize with CoinGecko-only
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

        # After GetBalance, should have upgraded to 4-source
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
