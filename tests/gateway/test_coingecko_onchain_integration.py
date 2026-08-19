"""Tests for the CoinGeckoOnchainGetOHLCV gRPC handler in IntegrationServiceServicer."""

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import grpc
import pytest

from almanak.core.finality import DataFinality
from almanak.framework.data.interfaces import OHLCVCandle
from almanak.framework.data.timeframes import OHLCVTimeframe
from almanak.gateway.data.ohlcv.coingecko_onchain_provider import (
    CoinGeckoOnchainOHLCVProvider,
    ExactPoolOHLCVResult,
)
from almanak.gateway.proto import gateway_pb2

# Real pool addresses, used only for shape: the provider is always mocked.
EVM_POOL = "0xd0b53D9277642d899DF5C87A3966A349A798F224"  # Uniswap v3 WETH/USDC, Base
SOLANA_POOL = "HJPjoWUrhoZzkNfRpHuieeFk9WcZWjwy6PBjZ81ngndJ"  # Orca whirlpool SOL/USDC


def _make_context() -> MagicMock:
    """Create a mock gRPC ServicerContext."""
    ctx = MagicMock(spec=grpc.aio.ServicerContext)
    ctx.set_code = MagicMock()
    ctx.set_details = MagicMock()
    return ctx


def _make_ohlcv_candle(ts_offset: int = 0) -> OHLCVCandle:
    return OHLCVCandle(
        timestamp=datetime(2026, 1, 1, hour=ts_offset, tzinfo=UTC),
        open=Decimal("1800.0"),
        high=Decimal("1820.0"),
        low=Decimal("1790.0"),
        close=Decimal("1810.0"),
        volume=Decimal("50000.0"),
    )


def test_provider_fetch_success_stamps_off_chain_finality() -> None:
    """The gateway-owned provider preserves its historical finality wire value."""
    provider = CoinGeckoOnchainOHLCVProvider(api_key="test-key")
    provider.get_ohlcv = AsyncMock(return_value=[_make_ohlcv_candle()])

    with asyncio.Runner() as runner:
        with patch("asyncio.get_event_loop", return_value=runner.get_loop()):
            envelope = provider.fetch(token="ALMANAK", chain="base")

    assert envelope.meta.finality is DataFinality.OFF_CHAIN
    assert envelope.meta.finality.value == "off_chain"


def test_exact_identity_fields_extend_the_legacy_proto_without_renumbering() -> None:
    request_fields = gateway_pb2.CoinGeckoOnchainOHLCVRequest.DESCRIPTOR.fields_by_name
    response_fields = gateway_pb2.CoinGeckoOnchainOHLCVResponse.DESCRIPTOR.fields_by_name

    assert request_fields["include_empty_intervals"].number == 7
    assert {
        name: request_fields[name].number
        for name in (
            "start_ts",
            "end_ts",
            "binding_hash",
            "feature_identity",
            "base_token_address",
            "quote_token_address",
        )
    } == {
        "start_ts": 8,
        "end_ts": 9,
        "binding_hash": 10,
        "feature_identity": 11,
        "base_token_address": 12,
        "quote_token_address": 13,
    }
    assert response_fields["candles"].number == 1
    assert response_fields["success"].number == 13
    assert response_fields["error"].number == 14


class TestCoinGeckoOnchainGetOHLCV:
    """Tests for IntegrationServiceServicer.CoinGeckoOnchainGetOHLCV."""

    @pytest.fixture
    def service(self):
        """Create an IntegrationServiceServicer with mocked dependencies."""
        from almanak.gateway.core.settings import GatewaySettings
        from almanak.gateway.services.integration_service import IntegrationServiceServicer

        svc = IntegrationServiceServicer.__new__(IntegrationServiceServicer)
        svc._initialized = True
        svc.settings = GatewaySettings(coingecko_api_key="test-key")
        svc._binance = None
        svc._coingecko = None
        svc._thegraph = None
        return svc

    @pytest.mark.asyncio
    async def test_empty_token_returns_invalid_argument(self, service):
        """Empty token triggers INVALID_ARGUMENT."""
        ctx = _make_context()
        request = gateway_pb2.CoinGeckoOnchainOHLCVRequest(token="", chain="base")

        await service.CoinGeckoOnchainGetOHLCV(request, ctx)

        ctx.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)
        ctx.set_details.assert_called_with("token is required and cannot be empty")

    @pytest.mark.asyncio
    async def test_empty_chain_returns_invalid_argument(self, service):
        """Empty chain triggers INVALID_ARGUMENT."""
        ctx = _make_context()
        request = gateway_pb2.CoinGeckoOnchainOHLCVRequest(token="ALMANAK", chain="")

        await service.CoinGeckoOnchainGetOHLCV(request, ctx)

        ctx.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)
        ctx.set_details.assert_called_with("chain is required and cannot be empty")

    @pytest.mark.asyncio
    async def test_invalid_timeframe_returns_invalid_argument(self, service):
        """Unsupported timeframe triggers INVALID_ARGUMENT."""
        ctx = _make_context()
        request = gateway_pb2.CoinGeckoOnchainOHLCVRequest(
            token="ALMANAK",
            chain="base",
            timeframe="2h",
        )

        await service.CoinGeckoOnchainGetOHLCV(request, ctx)

        ctx.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)
        details = ctx.set_details.call_args.args[0]
        assert "CoinGeckoOnchainOHLCVRequest.timeframe" in details
        assert "1m, 5m, 15m, 1h, 4h, 1d" in details

    @pytest.mark.asyncio
    async def test_limit_out_of_range_returns_invalid_argument(self, service):
        """Limit outside 1-1000 triggers INVALID_ARGUMENT."""
        ctx = _make_context()
        request = gateway_pb2.CoinGeckoOnchainOHLCVRequest(
            token="ALMANAK",
            chain="base",
            timeframe="1h",
            limit=1001,
        )

        await service.CoinGeckoOnchainGetOHLCV(request, ctx)

        ctx.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)
        assert "limit must be between" in ctx.set_details.call_args[0][0]

    @pytest.mark.asyncio
    async def test_success_returns_candles(self, service):
        """A serialized legacy client request still uses the unbound lane."""
        ctx = _make_context()
        legacy_wire = gateway_pb2.CoinGeckoOnchainOHLCVRequest(
            token="ALMANAK",
            chain="base",
            timeframe="1h",
            limit=2,
        ).SerializeToString()
        request = gateway_pb2.CoinGeckoOnchainOHLCVRequest.FromString(legacy_wire)

        candles = [_make_ohlcv_candle(0), _make_ohlcv_candle(1)]

        mock_provider = AsyncMock()
        mock_provider.get_ohlcv.return_value = candles
        mock_provider.__aenter__ = AsyncMock(return_value=mock_provider)
        mock_provider.__aexit__ = AsyncMock(return_value=False)

        with patch(
            "almanak.gateway.data.ohlcv.coingecko_onchain_provider.CoinGeckoOnchainOHLCVProvider",
            return_value=mock_provider,
        ):
            response = await service.CoinGeckoOnchainGetOHLCV(request, ctx)

        assert len(response.candles) == 2
        assert response.candles[0].close == "1810.0"
        assert response.candles[0].volume == "50000.0"
        assert mock_provider.get_ohlcv.call_args.kwargs["timeframe"] is OHLCVTimeframe.ONE_HOUR
        mock_provider.get_exact_pool_ohlcv.assert_not_called()
        # Should NOT have set an error code
        ctx.set_code.assert_not_called()

    @pytest.mark.asyncio
    async def test_exact_mode_returns_complete_identity_acknowledgement(self, service):
        ctx = _make_context()
        pool = EVM_POOL.lower()
        token0 = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        token1 = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        start = 1_766_001_600
        end = start + 3600
        request = gateway_pb2.CoinGeckoOnchainOHLCVRequest(
            token="legacy-token-is-not-exact-base",
            quote=token1,
            chain="base",
            timeframe="1h",
            limit=1,
            pool_address=pool,
            include_empty_intervals=True,
            start_ts=start,
            end_ts=end,
            binding_hash="11" * 32,
            feature_identity="22" * 32,
            base_token_address=token0,
            quote_token_address=token1,
        )
        exact_result = ExactPoolOHLCVResult(
            candles=(_make_ohlcv_candle(),),
            chain="base",
            pool_address=pool,
            base_token_address=token0,
            quote_token_address=token1,
            timeframe=OHLCVTimeframe.ONE_HOUR,
            start_ts=start,
            end_ts=end,
            binding_hash="11" * 32,
            feature_identity="22" * 32,
            observed_at=datetime.fromtimestamp(end, tz=UTC),
        )
        mock_provider = AsyncMock()
        mock_provider.get_exact_pool_ohlcv.return_value = exact_result
        mock_provider.__aenter__ = AsyncMock(return_value=mock_provider)
        mock_provider.__aexit__ = AsyncMock(return_value=False)

        with patch(
            "almanak.gateway.data.ohlcv.coingecko_onchain_provider.CoinGeckoOnchainOHLCVProvider",
            return_value=mock_provider,
        ):
            response = await service.CoinGeckoOnchainGetOHLCV(request, ctx)

        assert response.success is True
        assert response.chain == "base"
        assert response.pool_address == pool
        assert response.timeframe == "1h"
        assert response.start_ts == start
        assert response.end_ts == end
        assert response.binding_hash == "11" * 32
        assert response.feature_identity == "22" * 32
        assert response.base_token_address == token0
        assert response.quote_token_address == token1
        assert response.source == "coingecko_onchain.exact_pool"
        assert response.observed_at == end
        mock_provider.get_ohlcv.assert_not_called()
        mock_provider.get_exact_pool_ohlcv.assert_awaited_once()
        assert mock_provider.get_exact_pool_ohlcv.call_args.kwargs == {
            "chain": "base",
            "pool_address": pool,
            "base_token_address": token0,
            "quote_token_address": token1,
            "timeframe": OHLCVTimeframe.ONE_HOUR,
            "start_ts": start,
            "end_ts": end,
            "binding_hash": "11" * 32,
            "feature_identity": "22" * 32,
        }
        ctx.set_code.assert_not_called()

    @pytest.mark.asyncio
    async def test_exact_mode_preserves_mixed_case_solana_token_addresses(self, service):
        ctx = _make_context()
        token0 = "So11111111111111111111111111111111111111112"
        token1 = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
        start = 1_766_001_600
        end = start + 3600
        request = gateway_pb2.CoinGeckoOnchainOHLCVRequest(
            token="legacy-token-is-not-exact-base",
            chain="solana",
            timeframe="1h",
            limit=1,
            pool_address=SOLANA_POOL,
            include_empty_intervals=True,
            start_ts=start,
            end_ts=end,
            binding_hash="11" * 32,
            feature_identity="22" * 32,
            base_token_address=token0,
            quote_token_address=token1,
        )
        exact_result = ExactPoolOHLCVResult(
            candles=(_make_ohlcv_candle(),),
            chain="solana",
            pool_address=SOLANA_POOL,
            base_token_address=token0,
            quote_token_address=token1,
            timeframe=OHLCVTimeframe.ONE_HOUR,
            start_ts=start,
            end_ts=end,
            binding_hash="11" * 32,
            feature_identity="22" * 32,
            observed_at=datetime.fromtimestamp(end, tz=UTC),
        )
        mock_provider = AsyncMock()
        mock_provider.get_exact_pool_ohlcv.return_value = exact_result
        mock_provider.__aenter__ = AsyncMock(return_value=mock_provider)
        mock_provider.__aexit__ = AsyncMock(return_value=False)

        with patch(
            "almanak.gateway.data.ohlcv.coingecko_onchain_provider.CoinGeckoOnchainOHLCVProvider",
            return_value=mock_provider,
        ):
            response = await service.CoinGeckoOnchainGetOHLCV(request, ctx)

        assert response.success is True
        assert response.chain == "solana"
        assert response.pool_address == SOLANA_POOL
        assert response.timeframe == "1h"
        assert response.start_ts == start
        assert response.end_ts == end
        assert response.binding_hash == "11" * 32
        assert response.feature_identity == "22" * 32
        assert response.base_token_address == token0
        assert response.quote_token_address == token1
        assert response.source == "coingecko_onchain.exact_pool"
        assert response.observed_at == end
        forwarded = mock_provider.get_exact_pool_ohlcv.call_args.kwargs
        assert forwarded["pool_address"] == SOLANA_POOL
        assert forwarded["base_token_address"] == token0
        assert forwarded["quote_token_address"] == token1
        ctx.set_code.assert_not_called()

    @pytest.mark.asyncio
    async def test_partial_exact_mode_is_rejected_before_provider(self, service):
        ctx = _make_context()
        request = gateway_pb2.CoinGeckoOnchainOHLCVRequest(
            token="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            chain="base",
            timeframe="1h",
            limit=1,
            pool_address=EVM_POOL,
            binding_hash="11" * 32,
        )
        provider_cls = MagicMock()

        with patch(
            "almanak.gateway.data.ohlcv.coingecko_onchain_provider.CoinGeckoOnchainOHLCVProvider",
            provider_cls,
        ):
            response = await service.CoinGeckoOnchainGetOHLCV(request, ctx)

        assert response.success is False
        ctx.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)
        provider_cls.assert_not_called()

    @pytest.mark.asyncio
    async def test_include_empty_intervals_forwarded_to_provider(self, service):
        """VIB-4875: the proto flag is threaded through to the provider call."""
        ctx = _make_context()
        request = gateway_pb2.CoinGeckoOnchainOHLCVRequest(
            token="NVDAON",
            chain="ethereum",
            timeframe="1h",
            limit=2,
            include_empty_intervals=True,
        )

        mock_provider = AsyncMock()
        mock_provider.get_ohlcv.return_value = [_make_ohlcv_candle(0)]
        mock_provider.__aenter__ = AsyncMock(return_value=mock_provider)
        mock_provider.__aexit__ = AsyncMock(return_value=False)

        with patch(
            "almanak.gateway.data.ohlcv.coingecko_onchain_provider.CoinGeckoOnchainOHLCVProvider",
            return_value=mock_provider,
        ):
            await service.CoinGeckoOnchainGetOHLCV(request, ctx)

        assert mock_provider.get_ohlcv.call_args.kwargs["include_empty_intervals"] is True
        ctx.set_code.assert_not_called()

    @pytest.mark.asyncio
    async def test_provider_error_returns_sanitized_internal(self, service):
        """Provider exceptions yield INTERNAL with sanitized message."""
        ctx = _make_context()
        request = gateway_pb2.CoinGeckoOnchainOHLCVRequest(
            token="ALMANAK",
            chain="base",
            timeframe="1h",
            limit=10,
        )

        mock_provider = AsyncMock()
        mock_provider.get_ohlcv.side_effect = RuntimeError("upstream API 500")
        mock_provider.__aenter__ = AsyncMock(return_value=mock_provider)
        mock_provider.__aexit__ = AsyncMock(return_value=False)

        with patch(
            "almanak.gateway.data.ohlcv.coingecko_onchain_provider.CoinGeckoOnchainOHLCVProvider",
            return_value=mock_provider,
        ):
            await service.CoinGeckoOnchainGetOHLCV(request, ctx)

        ctx.set_code.assert_called_with(grpc.StatusCode.INTERNAL)
        # Must NOT leak raw error text — VIB-3800 sanitization replaces the
        # raw exception string with a fixed opaque message.
        details = ctx.set_details.call_args[0][0]
        assert "upstream API 500" not in details
        assert details == "Internal gateway error"

    @pytest.mark.asyncio
    async def test_value_error_returns_invalid_argument(self, service):
        """ValueError from provider yields INVALID_ARGUMENT."""
        ctx = _make_context()
        request = gateway_pb2.CoinGeckoOnchainOHLCVRequest(
            token="ALMANAK",
            chain="base",
            timeframe="1h",
            limit=10,
        )

        mock_provider = AsyncMock()
        mock_provider.get_ohlcv.side_effect = ValueError("unsupported timeframe")
        mock_provider.__aenter__ = AsyncMock(return_value=mock_provider)
        mock_provider.__aexit__ = AsyncMock(return_value=False)

        with patch(
            "almanak.gateway.data.ohlcv.coingecko_onchain_provider.CoinGeckoOnchainOHLCVProvider",
            return_value=mock_provider,
        ):
            await service.CoinGeckoOnchainGetOHLCV(request, ctx)

        ctx.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)


class TestCoinGeckoOnchainPoolAddressValidation:
    """pool_address is caller-supplied and lands in an outbound URL path segment.

    The provider builds ``/networks/{network}/pools/{pool_address}/ohlcv/{tf}``,
    so an unvalidated value carrying ``/`` or ``..`` can reshape the request path
    and reach other upstream endpoints. The handler must reject malformed values
    at the gRPC boundary, before the provider is constructed.
    """

    @pytest.fixture
    def service(self):
        """Create an IntegrationServiceServicer with mocked dependencies."""
        from almanak.gateway.core.settings import GatewaySettings
        from almanak.gateway.services.integration_service import IntegrationServiceServicer

        svc = IntegrationServiceServicer.__new__(IntegrationServiceServicer)
        svc._initialized = True
        svc.settings = GatewaySettings(coingecko_api_key="test-key")
        svc._binance = None
        svc._coingecko = None
        svc._thegraph = None
        return svc

    @staticmethod
    def _mock_provider() -> AsyncMock:
        mock_provider = AsyncMock()
        mock_provider.get_ohlcv.return_value = [_make_ohlcv_candle(0)]
        mock_provider.__aenter__ = AsyncMock(return_value=mock_provider)
        mock_provider.__aexit__ = AsyncMock(return_value=False)
        return mock_provider

    @pytest.mark.parametrize(
        ("chain", "pool_address"),
        [
            # Path traversal / injection: these are the values that could reach
            # a different CoinGecko Onchain endpoint if passed through.
            ("base", f"{EVM_POOL}/../../networks/eth/tokens/{EVM_POOL}"),
            ("base", "../../pools"),
            ("base", f"{EVM_POOL}/ohlcv/day"),
            ("solana", f"{SOLANA_POOL}/../.."),
            # Plain malformed addresses.
            ("base", "not-an-address"),
            ("base", "0x1234"),
            # Right shape, wrong chain family (base58 pool on an EVM chain and
            # vice versa) -- these would 404 upstream at best.
            ("base", SOLANA_POOL),
            ("solana", EVM_POOL),
        ],
    )
    @pytest.mark.asyncio
    async def test_malformed_pool_address_rejected_before_egress(self, service, chain, pool_address):
        """Malformed pool_address yields INVALID_ARGUMENT and never constructs the provider."""
        ctx = _make_context()
        request = gateway_pb2.CoinGeckoOnchainOHLCVRequest(
            token="ALMANAK",
            chain=chain,
            timeframe="1h",
            limit=10,
            pool_address=pool_address,
        )

        provider_cls = MagicMock()
        with patch(
            "almanak.gateway.data.ohlcv.coingecko_onchain_provider.CoinGeckoOnchainOHLCVProvider",
            provider_cls,
        ):
            response = await service.CoinGeckoOnchainGetOHLCV(request, ctx)

        ctx.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)
        assert ctx.set_details.call_args[0][0].startswith("pool_address:")
        assert not response.candles
        # No provider construction == no egress.
        provider_cls.assert_not_called()

    @pytest.mark.parametrize(
        ("chain", "pool_address"),
        [
            ("base", EVM_POOL),
            ("ethereum", EVM_POOL),
            ("solana", SOLANA_POOL),
            # Chain aliases resolve to their canonical name before the family
            # check, so a Solana pool on "sol" is not mistaken for an EVM chain.
            ("sol", SOLANA_POOL),
        ],
    )
    @pytest.mark.asyncio
    async def test_well_formed_pool_address_forwarded(self, service, chain, pool_address):
        """A well-formed pool address reaches the provider verbatim on EVM and Solana."""
        ctx = _make_context()
        request = gateway_pb2.CoinGeckoOnchainOHLCVRequest(
            token="ALMANAK",
            chain=chain,
            timeframe="1h",
            limit=10,
            pool_address=pool_address,
        )

        mock_provider = self._mock_provider()
        with patch(
            "almanak.gateway.data.ohlcv.coingecko_onchain_provider.CoinGeckoOnchainOHLCVProvider",
            return_value=mock_provider,
        ):
            response = await service.CoinGeckoOnchainGetOHLCV(request, ctx)

        ctx.set_code.assert_not_called()
        assert len(response.candles) == 1
        assert mock_provider.get_ohlcv.call_args.kwargs["pool_address"] == pool_address
        # The chain is forwarded unmodified -- alias resolution is used only to
        # pick the address family, never to rewrite the provider's input.
        assert mock_provider.get_ohlcv.call_args.kwargs["chain"] == chain

    @pytest.mark.parametrize("pool_address", ["", "   "])
    @pytest.mark.asyncio
    async def test_blank_pool_address_means_resolve_by_symbol(self, service, pool_address):
        """Empty/whitespace pool_address stays optional and normalizes to None."""
        ctx = _make_context()
        request = gateway_pb2.CoinGeckoOnchainOHLCVRequest(
            token="ALMANAK",
            chain="base",
            timeframe="1h",
            limit=10,
            pool_address=pool_address,
        )

        mock_provider = self._mock_provider()
        with patch(
            "almanak.gateway.data.ohlcv.coingecko_onchain_provider.CoinGeckoOnchainOHLCVProvider",
            return_value=mock_provider,
        ):
            await service.CoinGeckoOnchainGetOHLCV(request, ctx)

        ctx.set_code.assert_not_called()
        assert mock_provider.get_ohlcv.call_args.kwargs["pool_address"] is None

    @pytest.mark.parametrize("pool_address", [EVM_POOL, SOLANA_POOL, "../../pools"])
    @pytest.mark.asyncio
    async def test_unregistered_chain_skips_shape_check(self, service, pool_address):
        """An unregistered chain must not be converted into a hard INVALID_ARGUMENT.

        The OHLCV router fails over on the provider's retryable "Unsupported
        chain" error, so the handler forwards these untouched -- including a
        base58 pool, which the EVM branch would otherwise reject purely because
        the chain is unknown. Safe because the provider resolves its network map
        before building any URL; see the no-egress test below for that proof.
        """
        ctx = _make_context()
        request = gateway_pb2.CoinGeckoOnchainOHLCVRequest(
            token="ALMANAK",
            chain="definitely-not-a-chain",
            timeframe="1h",
            limit=10,
            pool_address=pool_address,
        )

        mock_provider = self._mock_provider()
        with patch(
            "almanak.gateway.data.ohlcv.coingecko_onchain_provider.CoinGeckoOnchainOHLCVProvider",
            return_value=mock_provider,
        ):
            await service.CoinGeckoOnchainGetOHLCV(request, ctx)

        ctx.set_code.assert_not_called()
        assert mock_provider.get_ohlcv.call_args.kwargs["pool_address"] == pool_address

    @pytest.mark.parametrize("pool_address", [f"{EVM_POOL}/../../networks/eth/tokens", "../../pools"])
    @pytest.mark.asyncio
    async def test_unregistered_chain_never_egresses(self, pool_address):
        """Skipping the shape check on an unregistered chain cannot leak.

        This is the invariant that makes the skip above safe: the real provider
        resolves its vendor network map -- derived from the same ChainRegistry
        that backs validate_chain, so never wider than it -- before it builds a
        URL or opens a session. An unregistered chain therefore dies on the
        retryable "Unsupported chain" error with the pool address never reaching
        a request path.
        """
        from almanak.framework.data.interfaces import DataSourceUnavailable
        from almanak.gateway.data.ohlcv.coingecko_onchain_provider import CoinGeckoOnchainOHLCVProvider

        provider = CoinGeckoOnchainOHLCVProvider(api_key="test-key")

        async def _explode() -> None:
            raise AssertionError("provider opened a session for an unregistered chain")

        with patch.object(provider, "_get_session", _explode):
            with pytest.raises(DataSourceUnavailable) as exc_info:
                await provider.get_ohlcv(
                    token="ALMANAK",
                    quote="USD",
                    timeframe="1h",
                    limit=10,
                    chain="definitely-not-a-chain",
                    pool_address=pool_address,
                )

        assert "Unsupported chain" in str(exc_info.value)
