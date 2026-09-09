"""Tests for TokenService gateway implementation."""

import asyncio
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import grpc
import pytest

from almanak.framework.data.tokens import (
    InvalidTokenAddressError,
    ResolvedToken,
    TokenNotFoundError,
)
from almanak.framework.data.tokens.models import BridgeType
from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.onchain_lookup import TokenMetadata
from almanak.gateway.services.token_service import (
    DEFAULT_ONCHAIN_TIMEOUT,
    DEFAULT_RATE_LIMIT,
    TokenRateLimiter,
    TokenServiceServicer,
)


@pytest.fixture
def settings():
    """Create test settings."""
    return GatewaySettings()


@pytest.fixture
def token_service(settings):
    """Create TokenService instance."""
    return TokenServiceServicer(settings)


@pytest.fixture
def mock_context():
    """Create mock gRPC context."""
    context = MagicMock()
    context.set_code = MagicMock()
    context.set_details = MagicMock()
    return context


@pytest.mark.asyncio
@pytest.mark.parametrize("with_channel", [False, True])
@pytest.mark.parametrize("endpoint", ["ResolveToken", "GetTokenDecimals"])
async def test_unknown_evm_address_uses_owned_metadata_lookup(
    token_service, mock_context, tmp_path, with_channel, endpoint
):
    from almanak.framework.data.tokens.resolver import TokenResolver

    address = "0x1234567890123456789012345678901234567890"
    channel = MagicMock() if with_channel else None
    token_service._resolver = TokenResolver(cache_file=str(tmp_path / "tokens.json"), gateway_channel=channel)
    lookup = MagicMock()
    lookup.lookup = AsyncMock(
        return_value=TokenMetadata(
            address=address,
            symbol="UNKNOWNTEST",
            name="Unknown Test Token",
            decimals=18,
        )
    )
    with patch.object(token_service, "_get_onchain_lookup", new_callable=AsyncMock, return_value=lookup):
        response = await getattr(token_service, endpoint)(
            getattr(gateway_pb2, endpoint + "Request")(token=address, chain="bsc"),
            mock_context,
        )
    assert response.success
    if endpoint == "ResolveToken":
        assert response.address.lower() == address
    assert response.decimals == 18
    lookup.lookup.assert_awaited_once_with("bsc", address)
    mock_context.set_code.assert_not_called()
    if channel is not None:
        channel.unary_unary.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("error", "status"),
    [
        (TimeoutError("RPC timeout"), grpc.StatusCode.DEADLINE_EXCEEDED),
        (RuntimeError("RPC unavailable"), grpc.StatusCode.INTERNAL),
    ],
)
@pytest.mark.parametrize("endpoint", ["ResolveToken", "GetTokenDecimals"])
async def test_unknown_evm_address_owned_lookup_errors(token_service, mock_context, tmp_path, error, status, endpoint):
    from almanak.framework.data.tokens.resolver import TokenResolver

    address = "0x1234567890123456789012345678901234567890"
    channel = MagicMock()
    token_service._resolver = TokenResolver(cache_file=str(tmp_path / "tokens.json"), gateway_channel=channel)
    lookup = MagicMock()
    lookup.lookup = AsyncMock(side_effect=error)
    with patch.object(token_service, "_get_onchain_lookup", new_callable=AsyncMock, return_value=lookup):
        response = await getattr(token_service, endpoint)(
            getattr(gateway_pb2, endpoint + "Request")(token=address, chain="bsc"), mock_context
        )
    assert not response.success
    assert response.error
    mock_context.set_code.assert_called_once_with(status)
    lookup.lookup.assert_awaited_once_with("bsc", address)
    channel.unary_unary.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("chain", ["arbitrum", "solana"])
async def test_decimal_symbol_miss_never_selects_a_dynamic_contract(token_service, mock_context, tmp_path, chain):
    from almanak.framework.data.tokens.resolver import TokenResolver

    channel = MagicMock()
    token_service._resolver = TokenResolver(cache_file=str(tmp_path / "tokens.json"), gateway_channel=channel)
    candidate = gateway_pb2.TokenMetadataResponse(success=True, symbol="UNKNOWNTEST", decimals=9)
    with (
        patch.object(token_service, "_try_evm_symbol_lookup", new_callable=AsyncMock, return_value=candidate) as evm,
        patch.object(
            token_service, "_try_solana_symbol_lookup", new_callable=AsyncMock, return_value=candidate
        ) as solana,
        patch.object(token_service, "GetTokenMetadata", new_callable=AsyncMock) as metadata,
    ):
        response = await token_service.GetTokenDecimals(
            gateway_pb2.GetTokenDecimalsRequest(token="UNKNOWNTEST", chain=chain), mock_context
        )
    assert not response.success and response.error
    mock_context.set_code.assert_called_once_with(grpc.StatusCode.NOT_FOUND)
    evm.assert_not_awaited()
    solana.assert_not_awaited()
    metadata.assert_not_awaited()
    channel.unary_unary.assert_not_called()


@pytest.mark.asyncio
async def test_decimal_exact_solana_mint_keeps_owned_discovery(token_service, mock_context, tmp_path):
    from almanak.framework.data.tokens.resolver import TokenResolver

    mint = "9" * 44
    channel = MagicMock()
    token_service._resolver = TokenResolver(cache_file=str(tmp_path / "tokens.json"), gateway_channel=channel)
    with patch.object(
        token_service,
        "_try_solana_mint_lookup",
        new_callable=AsyncMock,
        return_value=gateway_pb2.TokenMetadataResponse(success=True, address=mint, decimals=6),
    ) as lookup:
        response = await token_service.GetTokenDecimals(
            gateway_pb2.GetTokenDecimalsRequest(token=mint, chain="solana"), mock_context
        )
    assert response.success and response.decimals == 6
    lookup.assert_awaited_once_with(mint)
    channel.unary_unary.assert_not_called()


@pytest.fixture
def sample_resolved_token():
    """Create sample ResolvedToken for testing."""
    return ResolvedToken(
        symbol="USDC",
        address="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
        decimals=6,
        chain="arbitrum",
        chain_id=42161,
        name="USD Coin",
        coingecko_id="usd-coin",
        is_stablecoin=True,
        is_native=False,
        is_wrapped_native=False,
        canonical_symbol="USDC",
        bridge_type=BridgeType.NATIVE,
        source="static",
        is_verified=True,
        resolved_at=datetime.now(),
    )


# =============================================================================
# Rate Limiter Tests
# =============================================================================


class TestTokenRateLimiter:
    """Tests for TokenRateLimiter."""

    @pytest.mark.asyncio
    async def test_acquire_success(self):
        """Rate limiter allows requests within limit."""
        limiter = TokenRateLimiter(max_rate=10)

        for _ in range(5):
            assert await limiter.acquire() is True

    @pytest.mark.asyncio
    async def test_acquire_rate_limited(self):
        """Rate limiter blocks requests exceeding limit."""
        limiter = TokenRateLimiter(max_rate=2)

        # First two should succeed
        assert await limiter.acquire() is True
        assert await limiter.acquire() is True

        # Third should fail
        assert await limiter.acquire() is False

    @pytest.mark.asyncio
    async def test_rate_limit_resets_after_window(self):
        """Rate limiter resets after time window passes."""
        limiter = TokenRateLimiter(max_rate=1)

        # First should succeed
        assert await limiter.acquire() is True

        # Second should fail
        assert await limiter.acquire() is False

        # Wait for window to pass
        await asyncio.sleep(1.1)

        # Should succeed again
        assert await limiter.acquire() is True

    @pytest.mark.asyncio
    async def test_wait_and_acquire_success(self):
        """wait_and_acquire waits for slot to be available."""
        limiter = TokenRateLimiter(max_rate=1)

        # First immediate
        assert await limiter.acquire() is True

        # Second waits
        result = await limiter.wait_and_acquire(timeout=2.0)
        assert result is True

    @pytest.mark.asyncio
    async def test_wait_and_acquire_timeout(self):
        """wait_and_acquire times out when limit exceeded."""
        limiter = TokenRateLimiter(max_rate=1)

        # Fill up the limit
        assert await limiter.acquire() is True

        # Wait should timeout (very short window)
        result = await limiter.wait_and_acquire(timeout=0.1)
        assert result is False


# =============================================================================
# TokenService Initialization Tests
# =============================================================================


class TestTokenServiceInit:
    """Tests for TokenService initialization."""

    def test_init_with_defaults(self, settings):
        """TokenService initializes with default values."""
        service = TokenServiceServicer(settings)

        assert service.settings == settings
        assert service._onchain_timeout == DEFAULT_ONCHAIN_TIMEOUT
        assert service._onchain_lookups == {}
        assert service._resolver is not None

    def test_init_with_custom_values(self, settings):
        """TokenService accepts custom configuration."""
        service = TokenServiceServicer(
            settings,
            onchain_timeout=5.0,
            rate_limit=5,
        )

        assert service._onchain_timeout == 5.0
        assert service._rate_limiter._max_rate == 5


# =============================================================================
# ResolveToken Tests
# =============================================================================


class TestResolveToken:
    """Tests for TokenService.ResolveToken."""

    @pytest.mark.asyncio
    async def test_resolve_by_symbol_success(self, token_service, mock_context, sample_resolved_token):
        """ResolveToken returns token by symbol."""
        with patch.object(token_service._resolver, "resolve", return_value=sample_resolved_token):
            request = gateway_pb2.ResolveTokenRequest(token="USDC", chain="arbitrum")
            response = await token_service.ResolveToken(request, mock_context)

            assert response.success is True
            assert response.symbol == "USDC"
            assert response.address == "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
            assert response.decimals == 6
            assert response.is_verified is True
            assert response.source == "static"

    @pytest.mark.asyncio
    async def test_resolve_by_address_success(self, token_service, mock_context, sample_resolved_token):
        """ResolveToken returns token by address."""
        with patch.object(token_service._resolver, "resolve", return_value=sample_resolved_token):
            request = gateway_pb2.ResolveTokenRequest(
                token="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
                chain="arbitrum",
            )
            response = await token_service.ResolveToken(request, mock_context)

            assert response.success is True
            assert response.symbol == "USDC"
            assert response.decimals == 6

    @pytest.mark.asyncio
    async def test_resolve_evm_address_miss_hits_morpho_listed_index(self, token_service, mock_context):
        """ResolveToken consults the Morpho address index when the resolver misses an EVM address."""
        vault = "0xbeef0e0834849acc03f0089f01f4f1eeb06873c9"
        morpho_hit = gateway_pb2.TokenMetadataResponse(
            success=True,
            symbol="steakUSDC",
            address=vault,
            decimals=18,
            name="Steakhouse USDC",
            source="morpho_vault",
        )
        with (
            patch.object(
                token_service._resolver,
                "resolve",
                side_effect=TokenNotFoundError(token=vault, chain="base", reason="Not found"),
            ),
            patch.object(token_service, "_try_morpho_address_lookup", new=AsyncMock(return_value=morpho_hit)) as lookup,
        ):
            response = await token_service.ResolveToken(
                gateway_pb2.ResolveTokenRequest(token=vault, chain="base"),
                mock_context,
            )

        assert response is morpho_hit
        lookup.assert_awaited_once_with(vault, "base")
        mock_context.set_code.assert_not_called()

    @pytest.mark.asyncio
    async def test_resolve_invalid_chain(self, token_service, mock_context):
        """ResolveToken returns error for invalid chain."""
        request = gateway_pb2.ResolveTokenRequest(token="USDC", chain="invalid_chain")
        response = await token_service.ResolveToken(request, mock_context)

        assert response.success is False
        assert "invalid_chain" in response.error.lower() or "chain" in response.error.lower()
        mock_context.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)

    @pytest.mark.asyncio
    async def test_resolve_empty_token(self, token_service, mock_context):
        """ResolveToken returns error for empty token."""
        request = gateway_pb2.ResolveTokenRequest(token="", chain="arbitrum")
        response = await token_service.ResolveToken(request, mock_context)

        assert response.success is False
        assert "required" in response.error.lower()
        mock_context.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)

    @pytest.mark.asyncio
    async def test_resolve_token_not_found(self, token_service, mock_context):
        """ResolveToken returns error when token not found."""
        with patch.object(
            token_service._resolver,
            "resolve",
            side_effect=TokenNotFoundError(
                token="UNKNOWN",
                chain="arbitrum",
                reason="Not found",
            ),
        ):
            request = gateway_pb2.ResolveTokenRequest(token="UNKNOWN", chain="arbitrum")
            response = await token_service.ResolveToken(request, mock_context)

            assert response.success is False
            assert "UNKNOWN" in response.error
            mock_context.set_code.assert_called_with(grpc.StatusCode.NOT_FOUND)

    @pytest.mark.asyncio
    async def test_resolve_invalid_address(self, token_service, mock_context):
        """ResolveToken returns error for invalid address format."""
        with patch.object(
            token_service._resolver,
            "resolve",
            side_effect=InvalidTokenAddressError(
                token="0xINVALID",
                chain="arbitrum",
                reason="Invalid hex characters",
            ),
        ):
            request = gateway_pb2.ResolveTokenRequest(token="0xINVALID", chain="arbitrum")
            response = await token_service.ResolveToken(request, mock_context)

            assert response.success is False
            mock_context.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)

    @pytest.mark.asyncio
    async def test_resolve_defaults_to_arbitrum(self, token_service, mock_context, sample_resolved_token):
        """ResolveToken defaults to arbitrum when chain not specified."""
        with patch.object(token_service._resolver, "resolve", return_value=sample_resolved_token) as mock_resolve:
            request = gateway_pb2.ResolveTokenRequest(token="USDC", chain="")
            await token_service.ResolveToken(request, mock_context)

            # Verify arbitrum was used
            mock_resolve.assert_called_once_with("USDC", "arbitrum", skip_gateway=True)


# =============================================================================
# GetTokenMetadata Tests
# =============================================================================


class TestGetTokenMetadata:
    """Tests for TokenService.GetTokenMetadata."""

    @pytest.mark.asyncio
    async def test_get_metadata_from_static(self, token_service, mock_context, sample_resolved_token):
        """GetTokenMetadata returns static token when available."""
        with patch.object(token_service._resolver, "resolve", return_value=sample_resolved_token):
            request = gateway_pb2.GetTokenMetadataRequest(
                address="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
                chain="arbitrum",
            )
            response = await token_service.GetTokenMetadata(request, mock_context)

            assert response.success is True
            assert response.symbol == "USDC"
            assert response.is_verified is True

    @pytest.mark.asyncio
    async def test_get_metadata_on_chain_lookup(self, token_service, mock_context):
        """GetTokenMetadata performs on-chain lookup for unknown token."""
        # Token not in static registry
        with patch.object(
            token_service._resolver,
            "resolve",
            side_effect=TokenNotFoundError(
                token="0x1234567890123456789012345678901234567890",
                chain="arbitrum",
                reason="Not found",
            ),
        ):
            # Mock on-chain lookup
            mock_lookup = AsyncMock()
            mock_metadata = TokenMetadata(
                symbol="NEWTOKEN",
                name="New Token",
                decimals=18,
                address="0x1234567890123456789012345678901234567890",
                is_native=False,
            )
            mock_lookup.lookup = AsyncMock(return_value=mock_metadata)

            with (
                patch.object(token_service, "_get_onchain_lookup", return_value=mock_lookup),
                patch.object(token_service._resolver, "register"),  # Mock register to avoid side effects
            ):
                request = gateway_pb2.GetTokenMetadataRequest(
                    address="0x1234567890123456789012345678901234567890",
                    chain="arbitrum",
                )
                response = await token_service.GetTokenMetadata(request, mock_context)

                assert response.success is True
                assert response.symbol == "NEWTOKEN"
                assert response.decimals == 18
                assert response.is_verified is False
                assert response.source == "on_chain"

    @pytest.mark.asyncio
    async def test_get_metadata_invalid_address(self, token_service, mock_context):
        """GetTokenMetadata returns error for invalid address."""
        request = gateway_pb2.GetTokenMetadataRequest(
            address="not_an_address",
            chain="arbitrum",
        )
        response = await token_service.GetTokenMetadata(request, mock_context)

        assert response.success is False
        mock_context.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)

    @pytest.mark.asyncio
    async def test_get_metadata_rate_limited(self, token_service, mock_context):
        """GetTokenMetadata returns error when rate limited."""
        # Set up a rate limiter that always fails to acquire
        mock_rate_limiter = AsyncMock()
        mock_rate_limiter.wait_and_acquire = AsyncMock(return_value=False)
        token_service._rate_limiter = mock_rate_limiter

        # Token not in static registry
        with patch.object(
            token_service._resolver,
            "resolve",
            side_effect=TokenNotFoundError(
                token="0x1234567890123456789012345678901234567890",
                chain="arbitrum",
                reason="Not found",
            ),
        ):
            request = gateway_pb2.GetTokenMetadataRequest(
                address="0x1234567890123456789012345678901234567890",
                chain="arbitrum",
            )
            response = await token_service.GetTokenMetadata(request, mock_context)

            assert response.success is False
            assert "rate limit" in response.error.lower()
            mock_context.set_code.assert_called_with(grpc.StatusCode.RESOURCE_EXHAUSTED)

    @pytest.mark.asyncio
    async def test_get_metadata_on_chain_timeout(self, token_service, mock_context):
        """GetTokenMetadata returns error on on-chain lookup timeout."""
        # Token not in static registry
        with patch.object(
            token_service._resolver,
            "resolve",
            side_effect=TokenNotFoundError(
                token="0x1234567890123456789012345678901234567890",
                chain="arbitrum",
                reason="Not found",
            ),
        ):
            # Mock on-chain lookup that times out
            async def slow_lookup(*args):
                await asyncio.sleep(100)  # Very slow
                return None

            mock_lookup = MagicMock()
            mock_lookup.lookup = slow_lookup

            # Set very short timeout
            token_service._onchain_timeout = 0.01

            with patch.object(token_service, "_get_onchain_lookup", return_value=mock_lookup):
                request = gateway_pb2.GetTokenMetadataRequest(
                    address="0x1234567890123456789012345678901234567890",
                    chain="arbitrum",
                )
                response = await token_service.GetTokenMetadata(request, mock_context)

                assert response.success is False
                assert "timed out" in response.error.lower()
                mock_context.set_code.assert_called_with(grpc.StatusCode.DEADLINE_EXCEEDED)

    @pytest.mark.asyncio
    async def test_get_metadata_on_chain_not_found(self, token_service, mock_context):
        """GetTokenMetadata returns error when on-chain lookup fails."""
        # Token not in static registry
        with patch.object(
            token_service._resolver,
            "resolve",
            side_effect=TokenNotFoundError(
                token="0x1234567890123456789012345678901234567890",
                chain="arbitrum",
                reason="Not found",
            ),
        ):
            # Mock on-chain lookup that returns None
            mock_lookup = AsyncMock()
            mock_lookup.lookup = AsyncMock(return_value=None)

            with patch.object(token_service, "_get_onchain_lookup", return_value=mock_lookup):
                request = gateway_pb2.GetTokenMetadataRequest(
                    address="0x1234567890123456789012345678901234567890",
                    chain="arbitrum",
                )
                response = await token_service.GetTokenMetadata(request, mock_context)

                assert response.success is False
                mock_context.set_code.assert_called_with(grpc.StatusCode.NOT_FOUND)


# =============================================================================
# GetTokenDecimals Tests
# =============================================================================


class TestGetTokenDecimals:
    """Tests for TokenService.GetTokenDecimals."""

    @pytest.mark.asyncio
    async def test_get_decimals_success(self, token_service, mock_context):
        """GetTokenDecimals returns decimals for known token."""
        with patch.object(
            token_service._resolver,
            "resolve",
            return_value=ResolvedToken(
                symbol="USDC",
                address="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
                decimals=6,
                chain="arbitrum",
                chain_id=42161,
            ),
        ):
            request = gateway_pb2.GetTokenDecimalsRequest(token="USDC", chain="arbitrum")
            response = await token_service.GetTokenDecimals(request, mock_context)

            assert response.success is True
            assert response.decimals == 6
            assert response.error == ""

    @pytest.mark.asyncio
    async def test_get_decimals_by_address(self, token_service, mock_context):
        """GetTokenDecimals works with address."""
        with patch.object(
            token_service._resolver,
            "resolve",
            return_value=ResolvedToken(
                symbol="WETH",
                address="0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
                decimals=18,
                chain="arbitrum",
                chain_id=42161,
            ),
        ):
            request = gateway_pb2.GetTokenDecimalsRequest(
                token="0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
                chain="arbitrum",
            )
            response = await token_service.GetTokenDecimals(request, mock_context)

            assert response.success is True
            assert response.decimals == 18

    @pytest.mark.asyncio
    async def test_get_decimals_not_found(self, token_service, mock_context):
        """GetTokenDecimals returns error for unknown token."""
        with patch.object(token_service, "_try_evm_symbol_lookup", new_callable=AsyncMock, return_value=None):
            request = gateway_pb2.GetTokenDecimalsRequest(token="UNKNOWN", chain="arbitrum")
            response = await token_service.GetTokenDecimals(request, mock_context)

            assert response.success is False
            assert response.decimals == 0
            assert "UNKNOWN" in response.error
            mock_context.set_code.assert_called_with(grpc.StatusCode.NOT_FOUND)

    @pytest.mark.asyncio
    async def test_get_decimals_empty_token(self, token_service, mock_context):
        """GetTokenDecimals returns error for empty token."""
        request = gateway_pb2.GetTokenDecimalsRequest(token="", chain="arbitrum")
        response = await token_service.GetTokenDecimals(request, mock_context)

        assert response.success is False
        assert "required" in response.error.lower()


# =============================================================================
# BatchResolveTokens Tests
# =============================================================================


class TestBatchResolveTokens:
    """Tests for TokenService.BatchResolveTokens."""

    @pytest.mark.asyncio
    async def test_batch_resolve_success(self, token_service, mock_context):
        """BatchResolveTokens returns all tokens successfully."""
        usdc = ResolvedToken(
            symbol="USDC",
            address="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            decimals=6,
            chain="arbitrum",
            chain_id=42161,
            name="USD Coin",
            coingecko_id="usd-coin",
            is_stablecoin=True,
            is_native=False,
            is_wrapped_native=False,
            canonical_symbol="USDC",
            bridge_type=BridgeType.NATIVE,
            source="static",
            is_verified=True,
            resolved_at=datetime.now(),
        )
        weth = ResolvedToken(
            symbol="WETH",
            address="0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
            decimals=18,
            chain="arbitrum",
            chain_id=42161,
            name="Wrapped Ether",
            coingecko_id="weth",
            is_stablecoin=False,
            is_native=False,
            is_wrapped_native=True,
            canonical_symbol="WETH",
            bridge_type=BridgeType.NATIVE,
            source="static",
            is_verified=True,
            resolved_at=datetime.now(),
        )

        def mock_resolve(token, chain, *, log_errors=True, skip_gateway=False):
            assert log_errors is False
            assert skip_gateway is True
            if token == "USDC":
                return usdc
            return weth

        with patch.object(token_service._resolver, "resolve", side_effect=mock_resolve):
            request = gateway_pb2.BatchResolveTokensRequest(
                tokens=["USDC", "WETH"],
                chain="arbitrum",
            )
            response = await token_service.BatchResolveTokens(request, mock_context)

            assert response.success is True
            assert len(response.tokens) == 2
            assert response.tokens[0].symbol == "USDC"
            assert response.tokens[0].decimals == 6
            assert response.tokens[1].symbol == "WETH"
            assert response.tokens[1].decimals == 18

    @pytest.mark.asyncio
    async def test_batch_resolve_partial_failure(self, token_service, mock_context, sample_resolved_token):
        """BatchResolveTokens returns partial success with errors."""

        def mock_resolve(token, chain, *, log_errors=True, skip_gateway=False):
            assert log_errors is False
            assert skip_gateway is True
            if token == "USDC":
                return sample_resolved_token
            raise TokenNotFoundError(token=token, chain=chain, reason="Not found")

        with patch.object(token_service._resolver, "resolve", side_effect=mock_resolve):
            request = gateway_pb2.BatchResolveTokensRequest(
                tokens=["USDC", "UNKNOWN"],
                chain="arbitrum",
            )
            response = await token_service.BatchResolveTokens(request, mock_context)

            assert response.success is False  # Not all succeeded
            assert len(response.tokens) == 2
            assert response.tokens[0].success is True
            assert response.tokens[0].symbol == "USDC"
            assert response.tokens[1].success is False
            assert "UNKNOWN" in response.tokens[1].error

    @pytest.mark.asyncio
    async def test_batch_resolve_empty_list(self, token_service, mock_context):
        """BatchResolveTokens returns error for empty token list."""
        request = gateway_pb2.BatchResolveTokensRequest(tokens=[], chain="arbitrum")
        response = await token_service.BatchResolveTokens(request, mock_context)

        assert response.success is False
        assert "required" in response.error.lower()
        mock_context.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)

    @pytest.mark.asyncio
    async def test_batch_resolve_invalid_chain(self, token_service, mock_context):
        """BatchResolveTokens returns error for invalid chain."""
        request = gateway_pb2.BatchResolveTokensRequest(
            tokens=["USDC", "WETH"],
            chain="invalid_chain",
        )
        response = await token_service.BatchResolveTokens(request, mock_context)

        assert response.success is False
        mock_context.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)


# =============================================================================
# Service Lifecycle Tests
# =============================================================================


class TestServiceLifecycle:
    """Tests for service lifecycle management."""

    @pytest.mark.asyncio
    async def test_close_cleans_up_lookups(self, token_service):
        """close() cleans up OnChainLookup instances."""
        # Add some mock lookups
        mock_lookup1 = AsyncMock()
        mock_lookup2 = AsyncMock()
        token_service._onchain_lookups = {
            "arbitrum": mock_lookup1,
            "base": mock_lookup2,
        }

        await token_service.close()

        mock_lookup1.close.assert_called_once()
        mock_lookup2.close.assert_called_once()
        assert token_service._onchain_lookups == {}


# =============================================================================
# Constants Tests
# =============================================================================


class TestConstants:
    """Tests for module constants."""

    def test_default_timeout(self):
        """Default timeout is 10 seconds."""
        assert DEFAULT_ONCHAIN_TIMEOUT == 10.0

    def test_default_rate_limit(self):
        """Default rate limit is 10 lookups per second."""
        assert DEFAULT_RATE_LIMIT == 10
