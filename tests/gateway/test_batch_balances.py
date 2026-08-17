"""Tests for BatchGetBalances RPC in MarketService.

Tests concurrent balance queries across multiple tokens/chains with
partial success handling.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.market_service import MarketServiceServicer


@pytest.fixture
def settings():
    """Create mock gateway settings."""
    from almanak.gateway.core.settings import GatewaySettings

    return GatewaySettings(
        grpc_host="localhost",
        grpc_port=50051,
        network="mainnet",
    )


@pytest.fixture
def market_service(settings):
    """Create MarketServiceServicer."""
    return MarketServiceServicer(settings)


@pytest.fixture
def mock_context():
    """Create mock gRPC context."""
    ctx = AsyncMock()
    ctx.set_code = MagicMock()
    ctx.set_details = MagicMock()
    return ctx


class TestBatchGetBalances:
    """Test BatchGetBalances RPC."""

    @pytest.mark.asyncio
    async def test_empty_batch(self, market_service, mock_context):
        """Empty request returns empty response."""
        request = gateway_pb2.BatchBalanceRequest(requests=[])
        response = await market_service.BatchGetBalances(request, mock_context)
        assert len(response.responses) == 0

    @pytest.mark.asyncio
    async def test_single_request(self, market_service, mock_context):
        """Single request in batch works correctly."""
        # Mock the balance provider
        mock_provider = AsyncMock()
        mock_result = MagicMock()
        mock_result.balance = 1000.5
        mock_result.address = "0xUsdc"
        mock_result.decimals = 6
        mock_result.raw_balance = 1000500000
        mock_result.timestamp = MagicMock()
        mock_result.timestamp.timestamp.return_value = 1234567890
        mock_result.stale = False
        mock_provider.get_balance = AsyncMock(return_value=mock_result)

        # Mock price aggregator
        mock_price = MagicMock()
        mock_price.price = 1.0
        mock_aggregator = AsyncMock()
        mock_aggregator.get_aggregated_price = AsyncMock(return_value=mock_price)

        market_service._initialized = True
        market_service._price_aggregator = mock_aggregator

        with patch.object(market_service, "_get_balance_provider", return_value=mock_provider):
            request = gateway_pb2.BatchBalanceRequest(
                requests=[
                    gateway_pb2.BalanceRequest(
                        token="USDC",
                        chain="arbitrum",
                        wallet_address="0x1234567890abcdef1234567890abcdef12345678",
                    )
                ]
            )
            response = await market_service.BatchGetBalances(request, mock_context)

        assert len(response.responses) == 1
        assert response.responses[0].balance == "1000.5"

    @pytest.mark.asyncio
    async def test_batch_prices_when_identity_resolution_fails(self, market_service, mock_context):
        """Each batch item keeps symbol pricing when identity metadata fails."""
        mock_provider = AsyncMock()
        mock_result = MagicMock()
        mock_result.balance = 2
        mock_result.address = "0x4200000000000000000000000000000000000006"
        mock_result.decimals = 18
        mock_result.raw_balance = 2 * 10**18
        mock_result.timestamp.timestamp.return_value = 1234567890
        mock_result.stale = False
        mock_provider.get_balance = AsyncMock(return_value=mock_result)

        mock_price = MagicMock()
        mock_price.price = 2500
        mock_aggregator = AsyncMock()
        mock_aggregator.get_aggregated_price = AsyncMock(return_value=mock_price)
        market_service._initialized = True

        with (
            patch.object(market_service, "_get_balance_provider", return_value=mock_provider),
            patch.object(market_service, "_aggregator_for", return_value=mock_aggregator),
            patch.object(
                market_service,
                "_resolve_token_for_pricing",
                new=AsyncMock(side_effect=RuntimeError("resolver unavailable")),
            ),
        ):
            response = await market_service.BatchGetBalances(
                gateway_pb2.BatchBalanceRequest(
                    requests=[
                        gateway_pb2.BalanceRequest(
                            token="WETH",
                            chain="base",
                            wallet_address="0x1234567890abcdef1234567890abcdef12345678",
                        )
                    ]
                ),
                mock_context,
            )

        assert response.responses[0].balance_usd == "5000"
        mock_aggregator.get_aggregated_price.assert_awaited_once_with("WETH", "USD", resolved_token=None)

    @pytest.mark.asyncio
    async def test_get_balance_forwards_resolved_token_to_price_aggregator(self, market_service, mock_context):
        """Unary balance pricing preserves a successfully resolved token identity."""
        market_service.settings.chains = ["base"]
        market_service._initialized = True

        resolved_token = MagicMock(name="resolved_token")
        provider = AsyncMock()
        result = MagicMock()
        result.balance = 2
        result.address = "0x4200000000000000000000000000000000000006"
        result.decimals = 18
        result.raw_balance = 2 * 10**18
        result.timestamp.timestamp.return_value = 1234567890
        result.stale = False
        provider.get_balance = AsyncMock(return_value=result)

        price = MagicMock()
        price.price = 2500
        aggregator = AsyncMock()
        aggregator.get_aggregated_price = AsyncMock(return_value=price)

        with (
            patch.object(market_service, "_get_balance_provider", return_value=provider),
            patch.object(market_service, "_aggregator_for", return_value=aggregator),
            patch.object(
                market_service,
                "_resolve_token_for_pricing",
                new=AsyncMock(return_value=resolved_token),
            ) as resolver,
        ):
            response = await market_service.GetBalance(
                gateway_pb2.BalanceRequest(
                    token="WETH",
                    chain="base",
                    wallet_address="0x1234567890abcdef1234567890abcdef12345678",
                ),
                mock_context,
            )

        assert response.balance_usd == "5000"
        resolver.assert_awaited_once_with("WETH", "base")
        aggregator.get_aggregated_price.assert_awaited_once_with(
            "WETH",
            "USD",
            resolved_token=resolved_token,
        )

    @pytest.mark.asyncio
    async def test_batch_forwards_resolved_token_to_price_aggregator(self, market_service, mock_context):
        """Batch balance pricing preserves each successfully resolved token identity."""
        market_service.settings.chains = ["base"]
        market_service._initialized = True

        resolved_token = MagicMock(name="resolved_token")
        provider = AsyncMock()
        result = MagicMock()
        result.balance = 2
        result.address = "0x4200000000000000000000000000000000000006"
        result.decimals = 18
        result.raw_balance = 2 * 10**18
        result.timestamp.timestamp.return_value = 1234567890
        result.stale = False
        provider.get_balance = AsyncMock(return_value=result)

        price = MagicMock()
        price.price = 2500
        aggregator = AsyncMock()
        aggregator.get_aggregated_price = AsyncMock(return_value=price)

        with (
            patch.object(market_service, "_get_balance_provider", return_value=provider),
            patch.object(market_service, "_aggregator_for", return_value=aggregator),
            patch.object(
                market_service,
                "_resolve_token_for_pricing",
                new=AsyncMock(return_value=resolved_token),
            ) as resolver,
        ):
            response = await market_service.BatchGetBalances(
                gateway_pb2.BatchBalanceRequest(
                    requests=[
                        gateway_pb2.BalanceRequest(
                            token="WETH",
                            chain="base",
                            wallet_address="0x1234567890abcdef1234567890abcdef12345678",
                        )
                    ]
                ),
                mock_context,
            )

        assert response.responses[0].balance_usd == "5000"
        resolver.assert_awaited_once_with("WETH", "base")
        aggregator.get_aggregated_price.assert_awaited_once_with(
            "WETH",
            "USD",
            resolved_token=resolved_token,
        )

    @pytest.mark.asyncio
    async def test_get_balance_rejects_unconfigured_chain_before_provider(self, market_service, mock_context):
        """Unary balance requests cannot escape the configured chain boundary."""
        import grpc

        market_service.settings.chains = ["arbitrum"]
        market_service._initialized = True

        with (
            patch.object(market_service, "_get_balance_provider", new=AsyncMock()) as provider,
            patch.object(market_service, "_aggregator_for") as aggregator_for,
        ):
            response = await market_service.GetBalance(
                gateway_pb2.BalanceRequest(
                    token="WETH",
                    chain="base",
                    wallet_address="0x1234567890abcdef1234567890abcdef12345678",
                ),
                mock_context,
            )

        assert response.balance == ""
        mock_context.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)
        assert "not configured" in mock_context.set_details.call_args.args[0]
        provider.assert_not_awaited()
        aggregator_for.assert_not_called()

    @pytest.mark.asyncio
    async def test_batch_rejects_unconfigured_chain_before_provider(self, market_service, mock_context):
        """Batch items report an error without querying an unconfigured chain."""
        market_service.settings.chains = ["arbitrum"]
        market_service._initialized = True

        with (
            patch.object(market_service, "_get_balance_provider", new=AsyncMock()) as provider,
            patch.object(market_service, "_aggregator_for") as aggregator_for,
        ):
            response = await market_service.BatchGetBalances(
                gateway_pb2.BatchBalanceRequest(
                    requests=[
                        gateway_pb2.BalanceRequest(
                            token="WETH",
                            chain="base",
                            wallet_address="0x1234567890abcdef1234567890abcdef12345678",
                        )
                    ]
                ),
                mock_context,
            )

        assert "not configured" in response.responses[0].error
        provider.assert_not_awaited()
        aggregator_for.assert_not_called()

    @pytest.mark.asyncio
    async def test_batch_honors_block_tag_and_echoes_block_number(self, market_service, mock_context):
        """VIB-3350 (audit M3): a batched pinned read threads block_tag to the
        provider and echoes block_number — it must not silently return 'latest'."""
        mock_provider = AsyncMock()
        mock_result = MagicMock()
        mock_result.balance = 5.0
        mock_result.address = "0xUsdc"
        mock_result.decimals = 6
        mock_result.raw_balance = 5000000
        mock_result.timestamp = MagicMock()
        mock_result.timestamp.timestamp.return_value = 1234567890
        mock_result.stale = False
        mock_provider.get_balance = AsyncMock(return_value=mock_result)

        mock_aggregator = AsyncMock()
        mock_aggregator.get_aggregated_price = AsyncMock(side_effect=Exception("no price"))
        market_service._initialized = True
        market_service._price_aggregator = mock_aggregator

        with patch.object(market_service, "_get_balance_provider", return_value=mock_provider):
            request = gateway_pb2.BatchBalanceRequest(
                requests=[
                    gateway_pb2.BalanceRequest(
                        token="USDC",
                        chain="arbitrum",
                        wallet_address="0x1234567890abcdef1234567890abcdef12345678",
                        block_tag=21_000_000,
                    )
                ]
            )
            response = await market_service.BatchGetBalances(request, mock_context)

        # provider was called pinned to the requested block
        mock_provider.get_balance.assert_awaited_once_with("USDC", block=21_000_000)
        assert response.responses[0].block_number == 21_000_000

    @pytest.mark.asyncio
    async def test_invalid_chain_returns_per_response_error(self, market_service, mock_context):
        """Invalid chain returns error in the individual response, not overall failure."""
        market_service._initialized = True

        request = gateway_pb2.BatchBalanceRequest(
            requests=[
                gateway_pb2.BalanceRequest(
                    token="USDC",
                    chain="invalid_chain",
                    wallet_address="0x1234567890abcdef1234567890abcdef12345678",
                )
            ]
        )
        response = await market_service.BatchGetBalances(request, mock_context)

        assert len(response.responses) == 1
        assert response.responses[0].error != ""

    @pytest.mark.asyncio
    async def test_invalid_address_returns_per_response_error(self, market_service, mock_context):
        """Invalid wallet address returns error in the individual response."""
        market_service._initialized = True

        request = gateway_pb2.BatchBalanceRequest(
            requests=[
                gateway_pb2.BalanceRequest(
                    token="USDC",
                    chain="arbitrum",
                    wallet_address="not-an-address",
                )
            ]
        )
        response = await market_service.BatchGetBalances(request, mock_context)

        assert len(response.responses) == 1
        assert response.responses[0].error != ""


# Valid base58 Solana wallet (USDC mint address — passes address validation).
_SOLANA_WALLET = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"


class TestBlockPinRejectedForNonEvm:
    """VIB-3350 (CodeRabbit Critical): a block-pinned read (block_tag > 0) on a
    non-EVM chain whose provider has no ``block`` kwarg must be rejected loudly
    with INVALID_ARGUMENT, not let through to raise TypeError -> opaque INTERNAL.
    The Solana provider's get_balance/get_native_balance accept no ``block``."""

    @pytest.mark.asyncio
    async def test_unary_get_balance_rejects_pinned_solana_read(self, market_service, mock_context):
        import grpc

        market_service._initialized = True
        # Provider whose pinned calls would TypeError if ever reached.
        mock_provider = AsyncMock()
        mock_provider.get_balance = AsyncMock(side_effect=AssertionError("provider must not be called"))
        mock_provider.get_native_balance = AsyncMock(side_effect=AssertionError("provider must not be called"))

        with patch.object(market_service, "_get_balance_provider", return_value=mock_provider):
            request = gateway_pb2.BalanceRequest(
                token="USDC",
                chain="solana",
                wallet_address=_SOLANA_WALLET,
                block_tag=21_000_000,
            )
            response = await market_service.GetBalance(request, mock_context)

        mock_context.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)
        assert "block-pinned" in mock_context.set_details.call_args.args[0]
        assert response.balance == ""  # empty response, no balance returned
        mock_provider.get_balance.assert_not_awaited()
        mock_provider.get_native_balance.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_batch_get_balances_rejects_pinned_solana_read(self, market_service, mock_context):
        market_service._initialized = True
        mock_provider = AsyncMock()
        mock_provider.get_balance = AsyncMock(side_effect=AssertionError("provider must not be called"))
        mock_provider.get_native_balance = AsyncMock(side_effect=AssertionError("provider must not be called"))

        with patch.object(market_service, "_get_balance_provider", return_value=mock_provider):
            request = gateway_pb2.BatchBalanceRequest(
                requests=[
                    gateway_pb2.BalanceRequest(
                        token="USDC",
                        chain="solana",
                        wallet_address=_SOLANA_WALLET,
                        block_tag=21_000_000,
                    )
                ]
            )
            response = await market_service.BatchGetBalances(request, mock_context)

        assert len(response.responses) == 1
        assert "block-pinned" in response.responses[0].error
        assert response.responses[0].balance == ""
        mock_provider.get_balance.assert_not_awaited()
        mock_provider.get_native_balance.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_unpinned_solana_read_still_allowed(self, market_service, mock_context):
        """Sanity: an UNPINNED Solana balance read (no block_tag) must still work —
        the guard only rejects block_tag > 0, never ordinary 'latest' reads."""
        mock_provider = AsyncMock()
        mock_result = MagicMock()
        mock_result.balance = 42.0
        mock_result.address = _SOLANA_WALLET
        mock_result.decimals = 6
        mock_result.raw_balance = 42_000_000
        mock_result.timestamp = MagicMock()
        mock_result.timestamp.timestamp.return_value = 1234567890
        mock_result.stale = False
        mock_provider.get_balance = AsyncMock(return_value=mock_result)

        mock_aggregator = AsyncMock()
        mock_aggregator.get_aggregated_price = AsyncMock(side_effect=Exception("no price"))
        market_service._initialized = True
        market_service._price_aggregator = mock_aggregator

        with patch.object(market_service, "_get_balance_provider", return_value=mock_provider):
            request = gateway_pb2.BalanceRequest(
                token="USDC",
                chain="solana",
                wallet_address=_SOLANA_WALLET,
            )
            response = await market_service.GetBalance(request, mock_context)

        mock_provider.get_balance.assert_awaited_once_with("USDC")  # no block= kwarg
        assert response.balance == "42.0"
        assert response.block_number == 0  # unpinned -> 0


class TestMNTNativeBalance:
    """Verify that MNT (Mantle native token) routes to get_native_balance(), not get_balance()."""

    def _make_mock_result(self):
        mock_result = MagicMock()
        mock_result.balance = 500.0
        mock_result.address = "0x0000000000000000000000000000000000000000"
        mock_result.decimals = 18
        mock_result.raw_balance = 500 * 10**18
        mock_result.timestamp = MagicMock()
        mock_result.timestamp.timestamp.return_value = 1234567890
        mock_result.stale = False
        return mock_result

    @pytest.mark.asyncio
    async def test_get_balance_mnt_calls_get_native_balance(self, market_service, mock_context):
        """GetBalance for MNT must invoke get_native_balance(), not get_balance()."""
        mock_provider = AsyncMock()
        mock_result = self._make_mock_result()
        mock_provider.get_native_balance = AsyncMock(return_value=mock_result)
        mock_provider.get_balance = AsyncMock(side_effect=AssertionError("get_balance must not be called for MNT"))

        mock_price = MagicMock()
        mock_price.price = 1.0
        mock_aggregator = AsyncMock()
        mock_aggregator.get_aggregated_price = AsyncMock(return_value=mock_price)

        market_service._initialized = True
        market_service._price_aggregator = mock_aggregator

        with patch.object(market_service, "_get_balance_provider", return_value=mock_provider):
            request = gateway_pb2.BalanceRequest(
                token="MNT",
                chain="mantle",
                wallet_address="0x1234567890abcdef1234567890abcdef12345678",
            )
            response = await market_service.GetBalance(request, mock_context)

        mock_provider.get_native_balance.assert_awaited_once()
        assert response.balance == "500.0"

    @pytest.mark.asyncio
    async def test_batch_get_balances_mnt_calls_get_native_balance(self, market_service, mock_context):
        """BatchGetBalances with MNT token must invoke get_native_balance()."""
        mock_provider = AsyncMock()
        mock_result = self._make_mock_result()
        mock_provider.get_native_balance = AsyncMock(return_value=mock_result)
        mock_provider.get_balance = AsyncMock(side_effect=AssertionError("get_balance must not be called for MNT"))

        mock_aggregator = AsyncMock()
        mock_aggregator.get_aggregated_price = AsyncMock(side_effect=Exception("no price"))

        market_service._initialized = True
        market_service._price_aggregator = mock_aggregator

        with patch.object(market_service, "_get_balance_provider", return_value=mock_provider):
            request = gateway_pb2.BatchBalanceRequest(
                requests=[
                    gateway_pb2.BalanceRequest(
                        token="MNT",
                        chain="mantle",
                        wallet_address="0x1234567890abcdef1234567890abcdef12345678",
                    )
                ]
            )
            response = await market_service.BatchGetBalances(request, mock_context)

        assert len(response.responses) == 1
        mock_provider.get_native_balance.assert_awaited_once()
        assert response.responses[0].balance == "500.0"
