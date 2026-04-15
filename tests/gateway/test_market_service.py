"""Tests for MarketService gateway implementation."""

import logging
import os
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.market_service import MarketServiceServicer


@pytest.fixture
def settings():
    """Create test settings."""
    return GatewaySettings()


@pytest.fixture
def market_service(settings):
    """Create MarketService instance."""
    return MarketServiceServicer(settings)


@pytest.fixture
def mock_context():
    """Create mock gRPC context."""
    context = MagicMock()
    context.set_code = MagicMock()
    context.set_details = MagicMock()
    return context


class TestMarketServiceGetPrice:
    """Tests for MarketService.GetPrice."""

    @pytest.mark.asyncio
    async def test_get_price_success(self, market_service, mock_context):
        """GetPrice returns price from aggregator."""
        from datetime import UTC, datetime

        from almanak.framework.data.interfaces import PriceResult

        # Mock the price aggregator
        mock_result = PriceResult(
            price=Decimal("2500.50"),
            source="coingecko",
            timestamp=datetime.now(UTC),
            confidence=0.95,
            stale=False,
        )

        with patch.object(market_service, "_price_aggregator") as mock_aggregator:
            mock_aggregator.get_aggregated_price = AsyncMock(return_value=mock_result)
            market_service._initialized = True

            request = gateway_pb2.PriceRequest(token="ETH", quote="USD")
            response = await market_service.GetPrice(request, mock_context)

            assert response.price == "2500.50"
            assert response.source == "coingecko"
            assert response.confidence == 0.95
            assert response.stale is False

    @pytest.mark.asyncio
    async def test_get_price_default_quote(self, market_service, mock_context):
        """GetPrice defaults to USD when quote not specified."""
        from datetime import UTC, datetime

        from almanak.framework.data.interfaces import PriceResult

        mock_result = PriceResult(
            price=Decimal("100.00"),
            source="test",
            timestamp=datetime.now(UTC),
            confidence=1.0,
            stale=False,
        )

        with patch.object(market_service, "_price_aggregator") as mock_aggregator:
            mock_aggregator.get_aggregated_price = AsyncMock(return_value=mock_result)
            market_service._initialized = True

            request = gateway_pb2.PriceRequest(token="WBTC")  # No quote specified
            await market_service.GetPrice(request, mock_context)

            # Verify USD was used as default
            mock_aggregator.get_aggregated_price.assert_called_once_with("WBTC", "USD")

    @pytest.mark.asyncio
    async def test_get_price_error_handling(self, market_service, mock_context):
        """GetPrice handles errors gracefully."""
        with patch.object(market_service, "_price_aggregator") as mock_aggregator:
            mock_aggregator.get_aggregated_price = AsyncMock(side_effect=Exception("API error"))
            market_service._initialized = True

            request = gateway_pb2.PriceRequest(token="INVALID", quote="USD")
            response = await market_service.GetPrice(request, mock_context)

            # Should return empty response and set error code
            assert response.price == ""
            mock_context.set_code.assert_called()


class TestMarketServiceGetBalance:
    """Tests for MarketService.GetBalance."""

    @pytest.mark.asyncio
    async def test_get_balance_requires_wallet(self, market_service, mock_context):
        """GetBalance requires wallet_address."""
        request = gateway_pb2.BalanceRequest(token="WETH", chain="arbitrum")
        await market_service.GetBalance(request, mock_context)

        mock_context.set_code.assert_called()
        mock_context.set_details.assert_called_with("wallet_address: required")

    @pytest.mark.asyncio
    async def test_get_balance_success(self, market_service, mock_context):
        """GetBalance returns balance from provider."""
        from datetime import UTC, datetime

        from almanak.framework.data.interfaces import BalanceResult

        # Use valid Ethereum address format (0x + 40 hex chars)
        valid_address = "0x1234567890123456789012345678901234567890"

        mock_result = BalanceResult(
            balance=Decimal("10.5"),
            token="WETH",
            address=valid_address,
            decimals=18,
            raw_balance=10500000000000000000,
            timestamp=datetime.now(UTC),
            stale=False,
        )

        # Mock balance provider
        mock_provider = MagicMock()
        mock_provider.get_balance = AsyncMock(return_value=mock_result)

        with patch.object(market_service, "_get_balance_provider", return_value=mock_provider):
            market_service._initialized = True

            # Also mock price aggregator for USD conversion
            with patch.object(market_service, "_price_aggregator") as mock_aggregator:
                mock_aggregator.get_aggregated_price = AsyncMock(side_effect=Exception("Skip USD"))

                request = gateway_pb2.BalanceRequest(
                    token="WETH",
                    chain="arbitrum",
                    wallet_address=valid_address,
                )
                response = await market_service.GetBalance(request, mock_context)

                assert response.balance == "10.5"
                assert response.decimals == 18

    @pytest.mark.asyncio
    async def test_get_balance_unknown_address_dynamic_resolution(
        self, market_service, mock_context
    ):
        """GetBalance resolves an unknown ERC20 address on-chain and returns the balance.

        Direct regression for the OPENAGENTS staging bug: a strategy passes a raw
        ERC20 address that isn't in DEFAULT_TOKENS, and we expect the gateway to
        fetch decimals on-chain via OnChainLookup rather than raising
        "Use add_token()".
        """
        from almanak.framework.data.tokens.exceptions import (
            TokenNotFoundError as FrameworkTokenNotFoundError,
        )
        from almanak.gateway.data.balance.web3_provider import Web3BalanceProvider
        from almanak.gateway.services.onchain_lookup import (
            TokenMetadata as OnChainTokenMetadata,
        )

        unknown_address = "0xcb5ff7331193c45f61f05b035ddabe08f13f6ba3"
        wallet_address = "0x1234567890123456789012345678901234567890"

        # Isolate the test from the global TokenResolver singleton (which has a
        # persistent disk cache) so prior runs can't pollute state. We replace it
        # with a mock that always misses statically and swallows register() calls.
        isolated_resolver = MagicMock()
        isolated_resolver.resolve.side_effect = FrameworkTokenNotFoundError(
            token=unknown_address, chain="base"
        )

        # OnChainLookup returns valid ERC20 metadata for the unknown address.
        fake_lookup = MagicMock()
        fake_lookup.lookup = AsyncMock(
            return_value=OnChainTokenMetadata(
                address=unknown_address,
                symbol="OPENAGENTS",
                decimals=18,
                name="OpenAgents",
                is_native=False,
            )
        )

        # 3.14 OPENAGENTS in raw base units (18 decimals).
        raw_balance = 3_140_000_000_000_000_000

        # Patch at the class level so the real Web3BalanceProvider (built by
        # MarketServiceServicer._get_balance_provider) picks them up.
        with patch(
            "almanak.framework.data.tokens.resolver.get_token_resolver",
            return_value=isolated_resolver,
        ), patch.object(
            Web3BalanceProvider, "_get_onchain_lookup", return_value=fake_lookup
        ), patch.object(
            Web3BalanceProvider,
            "_get_erc20_balance_with_retry",
            new=AsyncMock(return_value=raw_balance),
        ):
            market_service._initialized = True
            # Skip the USD-conversion pricing branch.
            with patch.object(market_service, "_price_aggregator") as mock_aggregator:
                mock_aggregator.get_aggregated_price = AsyncMock(
                    side_effect=Exception("Skip USD")
                )

                request = gateway_pb2.BalanceRequest(
                    token=unknown_address,
                    chain="base",
                    wallet_address=wallet_address,
                )
                response = await market_service.GetBalance(request, mock_context)

            # Response success: status code was never overridden to an error.
            mock_context.set_code.assert_not_called()

            assert response.balance == "3.14"
            assert response.decimals == 18
            assert response.address == unknown_address
            assert response.raw_balance == str(raw_balance)

        fake_lookup.lookup.assert_awaited_once_with("base", unknown_address)
        # SECURITY: register() must NOT be called -- the contract-reported symbol
        # is untrusted, and persisting (chain, SYMBOL) -> address into the shared
        # TokenResolver cache would create a symbol-squatting surface across
        # providers. The balance provider's own BalanceCacheEntry TTL handles
        # repeat-call efficiency; after TTL expiry we pay one more ~150ms
        # OnChainLookup call, which is acceptable.
        isolated_resolver.register.assert_not_called()


class TestMarketServiceBatchGetBalances:
    """Tests for MarketService.BatchGetBalances."""

    @pytest.mark.asyncio
    async def test_batch_get_balances_partial_failure_logs_debug(self, market_service, mock_context, caplog):
        """BatchGetBalances logs per-token failures at DEBUG, not WARNING."""
        valid_address = "0x1234567890123456789012345678901234567890"

        mock_provider = MagicMock()
        mock_provider.get_balance = AsyncMock(side_effect=Exception("token not found on chain"))

        with patch.object(market_service, "_get_balance_provider", return_value=mock_provider):
            market_service._initialized = True

            request = gateway_pb2.BatchBalanceRequest(
                requests=[
                    gateway_pb2.BalanceRequest(
                        token="USDT",
                        chain="base",
                        wallet_address=valid_address,
                    )
                ]
            )

            with caplog.at_level(logging.DEBUG, logger="almanak.gateway.services.market_service"):
                response = await market_service.BatchGetBalances(request, mock_context)

        # Partial failure is returned in response, not raised
        assert len(response.responses) == 1
        assert response.responses[0].error != ""

        # Failure is logged at DEBUG, not WARNING
        debug_msgs = [r for r in caplog.records if r.levelno == logging.DEBUG and "BatchGetBalances" in r.message]
        warning_msgs = [r for r in caplog.records if r.levelno == logging.WARNING and "BatchGetBalances" in r.message]
        assert len(debug_msgs) >= 1
        assert len(warning_msgs) == 0


class TestMarketServiceInitialization:
    """Tests for MarketService price source initialization."""

    @pytest.mark.asyncio
    async def test_evm_chain_has_four_sources(self):
        """EVM chain gets 4-source pricing: Chainlink + Binance + DexScreener + CoinGecko."""
        env = os.environ.copy()
        env.pop("COINGECKO_API_KEY", None)
        env.pop("ALMANAK_GATEWAY_COINGECKO_API_KEY", None)
        # Must also mock load_dotenv to prevent .env file from re-populating
        # COINGECKO_API_KEY into os.environ during GatewaySettings model validation.
        with patch.dict(os.environ, env, clear=True), patch("dotenv.load_dotenv"):
            settings = GatewaySettings(coingecko_api_key=None, chains=["arbitrum"])
        service = MarketServiceServicer(settings)

        try:
            with patch("almanak.gateway.data.price.onchain.get_rpc_url", return_value="http://localhost:8545"):
                await service._ensure_initialized()

            assert service._price_aggregator is not None
            sources = service._price_aggregator.sources
            assert len(sources) == 4
            source_names = [s.source_name for s in sources]
            assert source_names == ["onchain", "binance", "dexscreener", "coingecko"]

            coingecko_sources = [source for source in sources if source.source_name == "coingecko"]
            assert len(coingecko_sources) == 1
            assert coingecko_sources[0]._api_key == ""
        finally:
            await service.close()

    @pytest.mark.asyncio
    async def test_evm_chain_with_cg_key_has_four_sources(self):
        """EVM chain with CG key still gets 4-source pricing."""
        settings = GatewaySettings(coingecko_api_key="test-key-123", chains=["arbitrum"])
        service = MarketServiceServicer(settings)

        try:
            with patch("almanak.gateway.data.price.onchain.get_rpc_url", return_value="http://localhost:8545"):
                await service._ensure_initialized()

            sources = service._price_aggregator.sources
            assert len(sources) == 4
            source_names = [s.source_name for s in sources]
            assert source_names == ["onchain", "binance", "dexscreener", "coingecko"]
            cg = [s for s in sources if s.source_name == "coingecko"][0]
            assert cg._api_key == "test-key-123"
        finally:
            await service.close()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("chain", ["arbitrum", "mantle"])
    @pytest.mark.parametrize("cg_key", [None, "key-123"])
    async def test_all_evm_chains_get_four_sources(self, chain, cg_key):
        """Aggregator has 4 sources for any EVM chain, regardless of CG key."""
        settings = GatewaySettings(coingecko_api_key=cg_key, chains=[chain])
        service = MarketServiceServicer(settings)

        try:
            with patch("almanak.gateway.data.price.onchain.get_rpc_url", return_value="http://localhost:8545"):
                await service._ensure_initialized()

            assert len(service._price_aggregator.sources) == 4
        finally:
            await service.close()

    @pytest.mark.asyncio
    async def test_uses_first_configured_chain(self):
        """On-chain source uses first chain from settings."""
        settings = GatewaySettings(chains=["base", "arbitrum"])
        service = MarketServiceServicer(settings)

        try:
            with patch("almanak.gateway.data.price.onchain.get_rpc_url", return_value="http://localhost:8545"):
                await service._ensure_initialized()

            # Find the on-chain source
            onchain_sources = [s for s in service._price_aggregator.sources if s.source_name == "onchain"]
            assert len(onchain_sources) == 1
            assert onchain_sources[0]._chain == "base"
        finally:
            await service.close()

    @pytest.mark.asyncio
    async def test_no_chains_disables_onchain_pricing(self):
        """Without chains configured, on-chain pricing is disabled (CoinGecko only)."""
        settings = GatewaySettings(chains=[])
        service = MarketServiceServicer(settings)

        try:
            await service._ensure_initialized()

            # Only CoinGecko source when no chain is configured
            assert len(service._price_aggregator.sources) == 1
            assert service._price_aggregator.sources[0].source_name == "coingecko"
        finally:
            await service.close()


class TestMarketServicePriceAlias:
    """Tests for native->wrapped price alias fallback."""

    @pytest.mark.asyncio
    async def test_mnt_falls_back_to_wmnt(self, market_service, mock_context):
        """GetPrice for MNT falls back to WMNT when MNT lookup fails."""
        from datetime import UTC, datetime

        from almanak.framework.data.interfaces import AllDataSourcesFailed, PriceResult

        wmnt_result = PriceResult(
            price=Decimal("0.85"),
            source="binance",
            timestamp=datetime.now(UTC),
            confidence=0.90,
            stale=False,
        )

        call_count = 0

        async def mock_get_price(token, quote, **kwargs):
            nonlocal call_count
            call_count += 1
            if token == "MNT":
                raise AllDataSourcesFailed(errors={"all": "no sources"})
            return wmnt_result

        market_service._price_aggregator = MagicMock()
        market_service._price_aggregator.get_aggregated_price = AsyncMock(side_effect=mock_get_price)
        market_service._price_aggregator.get_last_details = MagicMock(return_value=None)
        market_service._initialized = True

        request = gateway_pb2.PriceRequest(token="MNT", quote="USD")
        response = await market_service.GetPrice(request, mock_context)

        assert response.price == "0.85"
        assert response.source == "binance"
        assert call_count == 2  # MNT failed, then WMNT succeeded

    @pytest.mark.asyncio
    async def test_no_alias_for_known_token(self, market_service, mock_context):
        """GetPrice for ETH succeeds directly without alias fallback."""
        from datetime import UTC, datetime

        from almanak.framework.data.interfaces import PriceResult

        eth_result = PriceResult(
            price=Decimal("3000.00"),
            source="binance",
            timestamp=datetime.now(UTC),
            confidence=0.95,
            stale=False,
        )

        market_service._price_aggregator = MagicMock()
        market_service._price_aggregator.get_aggregated_price = AsyncMock(return_value=eth_result)
        market_service._price_aggregator.get_last_details = MagicMock(return_value=None)
        market_service._initialized = True

        request = gateway_pb2.PriceRequest(token="ETH", quote="USD")
        response = await market_service.GetPrice(request, mock_context)

        assert response.price == "3000.00"
        # Should only call once - no fallback needed
        market_service._price_aggregator.get_aggregated_price.assert_called_once_with("ETH", "USD")


class TestMarketServiceGetIndicator:
    """Tests for MarketService.GetIndicator."""

    @pytest.mark.asyncio
    async def test_get_indicator_unsupported_type(self, market_service, mock_context):
        """GetIndicator rejects unsupported indicator types."""
        request = gateway_pb2.IndicatorRequest(
            indicator_type="INVALID",
            token="ETH",
        )
        await market_service.GetIndicator(request, mock_context)

        mock_context.set_code.assert_called()
        assert "not supported" in str(mock_context.set_details.call_args)
