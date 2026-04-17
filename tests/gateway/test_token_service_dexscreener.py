"""Integration tests for DexScreener fallback in TokenService (VIB-2983).

Covers the wiring from ``_try_evm_symbol_lookup`` through DexScreener +
on-chain confirmation, ``ResolveToken``'s AmbiguousTokenError handling,
and that ``_cache_discovered_token`` tags the result with the right
``source`` string for observability.

DexScreener HTTP is mocked; the on-chain lookup is patched to return
a known TokenMetadata; these are unit-ish gateway tests.
"""

from __future__ import annotations

from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import grpc
import pytest

from almanak.core.enums import Chain
from almanak.framework.data.tokens import ResolvedToken, TokenNotFoundError
from almanak.framework.data.tokens.exceptions import AmbiguousTokenError
from almanak.framework.data.tokens.models import BridgeType
from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.services import dexscreener_lookup
from almanak.gateway.services.dexscreener_lookup import DexScreenerResult
from almanak.gateway.services.onchain_lookup import TokenMetadata
from almanak.gateway.services.token_service import TokenServiceServicer


@pytest.fixture
def settings() -> GatewaySettings:
    return GatewaySettings()


@pytest.fixture
def service(settings: GatewaySettings) -> TokenServiceServicer:
    return TokenServiceServicer(settings)


@pytest.fixture
def mock_context() -> MagicMock:
    ctx = MagicMock()
    ctx.set_code = MagicMock()
    ctx.set_details = MagicMock()
    return ctx


@pytest.fixture(autouse=True)
def _reset_metrics() -> None:
    dexscreener_lookup._reset_for_tests()
    yield
    dexscreener_lookup._reset_for_tests()


@pytest.fixture(autouse=True)
def _isolate_singleton(service: TokenServiceServicer, monkeypatch: pytest.MonkeyPatch) -> None:
    """Prevent per-test assignments to ``service._resolver`` from leaking into
    the global ``TokenResolver`` singleton. We swap ``service._resolver`` to a
    MagicMock for the duration of the test; pytest's monkeypatch restores
    the original attribute when the test exits.
    """
    mock_resolver = MagicMock()
    # Default: unknown tokens raise TokenNotFoundError. Tests override as needed.
    mock_resolver.resolve.side_effect = TokenNotFoundError(
        token="_default_",
        chain="_default_",
        reason="Not in static registry",
    )
    mock_resolver.register = MagicMock()
    monkeypatch.setattr(service, "_resolver", mock_resolver)


def _token_not_found(symbol: str, chain: str) -> TokenNotFoundError:
    return TokenNotFoundError(token=symbol, chain=chain, reason="Not in static registry")


def _token_metadata(symbol: str, address: str, decimals: int = 18) -> TokenMetadata:
    return TokenMetadata(
        address=address,
        symbol=symbol,
        name=symbol,
        decimals=decimals,
        is_native=False,
    )


class TestDexScreenerSuccessPath:
    """DexScreener fallback resolves a new-launch token on Linea (CoinGecko gap)."""

    @pytest.mark.asyncio
    async def test_linea_new_launch_resolved_via_dexscreener(
        self,
        service: TokenServiceServicer,
        mock_context: MagicMock,
    ) -> None:
        symbol = "NEWLAUNCH"
        chain = "linea"
        address = "0x1234567890123456789012345678901234567890"

        service._resolver.resolve = MagicMock(side_effect=_token_not_found(symbol, chain))  # type: ignore[method-assign]

        ds_result = DexScreenerResult(
            address=address,
            chain=chain,
            symbol=symbol,
            liquidity_usd=50_000.0,
            volume_24h_usd=5_000.0,
            pair_url="https://dexscreener.com/linea/x",
        )

        with (
            patch(
                "almanak.gateway.services.token_service.dexscreener_find_token_address",
                new=AsyncMock(return_value=ds_result),
            ),
            patch.object(
                service,
                "_confirm_address_on_chain",
                new=AsyncMock(return_value=_token_metadata(symbol, address)),
            ),
            patch.object(service, "_cache_discovered_token") as mock_cache,
        ):
            from almanak.gateway.proto import gateway_pb2

            response = await service.ResolveToken(
                gateway_pb2.ResolveTokenRequest(token=symbol, chain=chain),
                mock_context,
            )

        assert response.symbol == symbol
        assert response.address == address
        assert response.decimals == 18
        # Response carries the correct dynamic-source tag so the client-side
        # resolver persists it (VIB-2983 audit: Codex P2).
        assert response.source == "dexscreener_dynamic"

        # Verify provenance tag: cache called with source="dexscreener_dynamic"
        mock_cache.assert_called_once()
        _meta_arg, chain_arg = mock_cache.call_args.args
        assert chain_arg == chain
        assert mock_cache.call_args.kwargs.get("source") == "dexscreener_dynamic"


class TestDexScreenerAmbiguousPath:
    """When DexScreener returns multiple non-dominant candidates, caller gets NOT_FOUND + candidate list."""

    @pytest.mark.asyncio
    async def test_ambiguous_symbol_returns_not_found_with_candidates(
        self,
        service: TokenServiceServicer,
        mock_context: MagicMock,
    ) -> None:
        symbol = "DUPE"
        chain = "base"
        service._resolver.resolve = MagicMock(side_effect=_token_not_found(symbol, chain))  # type: ignore[method-assign]

        ambig = AmbiguousTokenError(
            token=symbol,
            chain=chain,
            reason="Multiple liquid DUPE contracts on base",
            matching_addresses=["0xAAA" + "0" * 37, "0xBBB" + "0" * 37],
            suggestions=["Candidate 0xAAA...: liq=$50k, vol=$5k", "Candidate 0xBBB...: liq=$40k, vol=$4k"],
        )

        with (
            patch(
                "almanak.gateway.services.token_service.dexscreener_find_token_address",
                new=AsyncMock(side_effect=ambig),
            ),
            # Stub CoinGecko so it can't short-circuit the DexScreener branch
            # with a real-network hit for "DUPE" on base.
            patch.object(
                service,
                "_coingecko_find_address",
                new=AsyncMock(return_value=None),
            ),
        ):
            from almanak.gateway.proto import gateway_pb2

            response = await service.ResolveToken(
                gateway_pb2.ResolveTokenRequest(token=symbol, chain=chain),
                mock_context,
            )

        mock_context.set_code.assert_called_once_with(grpc.StatusCode.NOT_FOUND)
        details_arg = mock_context.set_details.call_args.args[0]
        assert "DUPE" in details_arg
        assert "base" in details_arg
        assert response.symbol == ""  # error response carries no metadata


class TestDexScreenerOnChainConfirmFails:
    """DexScreener returns an address but RPC confirmation fails → NOT_FOUND."""

    @pytest.mark.asyncio
    async def test_onchain_confirm_failure_returns_not_found(
        self,
        service: TokenServiceServicer,
        mock_context: MagicMock,
    ) -> None:
        symbol = "BAD"
        chain = "arbitrum"
        service._resolver.resolve = MagicMock(side_effect=_token_not_found(symbol, chain))  # type: ignore[method-assign]

        ds_result = DexScreenerResult(
            address="0x" + "42" * 20,
            chain=chain,
            symbol=symbol,
            liquidity_usd=50_000.0,
            volume_24h_usd=5_000.0,
        )

        with (
            patch(
                "almanak.gateway.services.token_service.dexscreener_find_token_address",
                new=AsyncMock(return_value=ds_result),
            ),
            patch.object(
                service,
                "_coingecko_find_address",
                new=AsyncMock(return_value=None),
            ),
            patch.object(
                service,
                "_confirm_address_on_chain",
                new=AsyncMock(return_value=None),
            ),
        ):
            from almanak.gateway.proto import gateway_pb2

            await service.ResolveToken(
                gateway_pb2.ResolveTokenRequest(token=symbol, chain=chain),
                mock_context,
            )

        mock_context.set_code.assert_called_once_with(grpc.StatusCode.NOT_FOUND)


class TestCoinGeckoThenDexScreenerOrder:
    """CoinGecko hit wins; DexScreener is not called. Source tag = coingecko_dynamic."""

    @pytest.mark.asyncio
    async def test_coingecko_hit_shortcircuits_dexscreener(
        self,
        service: TokenServiceServicer,
        mock_context: MagicMock,
    ) -> None:
        symbol = "CGHIT"
        chain = "arbitrum"
        address = "0x" + "cc" * 20
        service._resolver.resolve = MagicMock(side_effect=_token_not_found(symbol, chain))  # type: ignore[method-assign]

        ds_mock = AsyncMock()
        with (
            patch.object(
                service,
                "_coingecko_find_address",
                new=AsyncMock(return_value=address),
            ),
            patch.object(
                service,
                "_confirm_address_on_chain",
                new=AsyncMock(return_value=_token_metadata(symbol, address, decimals=8)),
            ),
            patch(
                "almanak.gateway.services.token_service.dexscreener_find_token_address",
                new=ds_mock,
            ),
            patch.object(service, "_cache_discovered_token") as mock_cache,
        ):
            from almanak.gateway.proto import gateway_pb2

            response = await service.ResolveToken(
                gateway_pb2.ResolveTokenRequest(token=symbol, chain=chain),
                mock_context,
            )

        assert response.address == address
        assert response.decimals == 8
        assert response.source == "coingecko_dynamic"  # audit Codex P2
        ds_mock.assert_not_called()
        assert mock_cache.call_args.kwargs.get("source") == "coingecko_dynamic"


class TestSymbolIdentityCheck:
    """Blocker 1 regression: on-chain symbol must match requested symbol.

    Without this, a contract that reports symbol()="USDC" but was indexed by
    DexScreener under a different symbol could be silently resolved — and a
    strategy doing swap("USDC", ...) would route funds to the attacker.
    """

    @pytest.mark.asyncio
    async def test_mismatched_onchain_symbol_is_rejected(
        self,
        service: TokenServiceServicer,
        mock_context: MagicMock,
    ) -> None:
        chain = "arbitrum"
        address = "0x" + "42" * 20

        # Simulate the real OnChainLookup returning a contract whose on-chain
        # symbol is NOT the one the caller asked for. Must be rejected.
        class _StubLookup:
            async def lookup(self, _chain: str, _addr: str) -> TokenMetadata:
                return _token_metadata("WRONG", address, decimals=18)

        service._get_onchain_lookup = AsyncMock(return_value=_StubLookup())  # type: ignore[method-assign]
        service._rate_limiter = MagicMock()
        service._rate_limiter.wait_and_acquire = AsyncMock(return_value=True)

        result = await service._confirm_address_on_chain(
            address,
            chain,
            expected_symbol="USDC",
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_matching_onchain_symbol_is_accepted_case_insensitive(
        self,
        service: TokenServiceServicer,
        mock_context: MagicMock,
    ) -> None:
        chain = "arbitrum"
        address = "0x" + "42" * 20

        class _StubLookup:
            async def lookup(self, _chain: str, _addr: str) -> TokenMetadata:
                return _token_metadata("USDC", address, decimals=6)

        service._get_onchain_lookup = AsyncMock(return_value=_StubLookup())  # type: ignore[method-assign]
        service._rate_limiter = MagicMock()
        service._rate_limiter.wait_and_acquire = AsyncMock(return_value=True)

        result = await service._confirm_address_on_chain(
            address,
            chain,
            expected_symbol="usdc",
        )
        assert result is not None
        assert result.decimals == 6

    @pytest.mark.asyncio
    async def test_rate_limiter_exhausted_returns_none(
        self,
        service: TokenServiceServicer,
        mock_context: MagicMock,
    ) -> None:
        # Important 4: dynamic-path RPC must go through the rate limiter.
        # If the bucket is exhausted, _confirm_address_on_chain returns None
        # rather than hammering the RPC.
        service._rate_limiter = MagicMock()
        service._rate_limiter.wait_and_acquire = AsyncMock(return_value=False)
        service._get_onchain_lookup = AsyncMock()  # type: ignore[method-assign]

        result = await service._confirm_address_on_chain(
            "0x" + "42" * 20,
            "arbitrum",
            expected_symbol="USDC",
        )
        assert result is None
        service._get_onchain_lookup.assert_not_called()


class TestCacheOverwriteGuard:
    """Blocker 2 regression: dexscreener_dynamic cannot overwrite higher-ranked entries."""

    @pytest.mark.asyncio
    async def test_dexscreener_does_not_overwrite_static_entry(
        self,
        service: TokenServiceServicer,
    ) -> None:
        address = "0x" + "cc" * 20
        symbol = "USDC"
        chain = "arbitrum"

        # Simulate an existing cached entry with source="static" (higher rank)
        existing = ResolvedToken(
            symbol=symbol,
            address=address,
            decimals=6,
            chain=Chain.ARBITRUM,
            chain_id=42161,
            name="USD Coin",
            source="static",
            is_verified=True,
            resolved_at=datetime.now(),
            bridge_type=BridgeType.NATIVE,
        )
        service._resolver.resolve.side_effect = None
        service._resolver.resolve.return_value = existing

        # Attempt to cache a dexscreener_dynamic entry for the same symbol
        service._cache_discovered_token(
            _token_metadata(symbol, address, decimals=6),
            chain,
            source="dexscreener_dynamic",
        )

        service._resolver.register.assert_not_called()

    @pytest.mark.asyncio
    async def test_dexscreener_writes_when_no_existing_entry(
        self,
        service: TokenServiceServicer,
    ) -> None:
        address = "0x" + "dd" * 20
        symbol = "NEWLAUNCH"
        chain = "linea"

        service._resolver.resolve = MagicMock(side_effect=_token_not_found(symbol, chain))  # type: ignore[method-assign]

        service._cache_discovered_token(
            _token_metadata(symbol, address, decimals=18),
            chain,
            source="dexscreener_dynamic",
        )

        service._resolver.register.assert_called_once()
        registered = service._resolver.register.call_args.args[0]
        assert registered.source == "dexscreener_dynamic"

    @pytest.mark.asyncio
    async def test_equal_rank_overwrite_is_blocked(
        self,
        service: TokenServiceServicer,
    ) -> None:
        """CR-fix: two ``dexscreener_dynamic`` results must not clobber each other.

        First-write-wins applies to same-rank sources too — a later
        dexscreener hit for the same symbol that returns a different
        address/decimals gets logged and dropped.
        """
        address = "0x" + "ee" * 20
        symbol = "MEME"
        chain = "base"

        existing = ResolvedToken(
            symbol=symbol,
            address=address,
            decimals=18,
            chain=Chain.BASE,
            chain_id=8453,
            name=None,
            source="dexscreener_dynamic",
            is_verified=False,
            resolved_at=datetime.now(),
            bridge_type=BridgeType.NATIVE,
        )
        service._resolver.resolve.side_effect = None
        service._resolver.resolve.return_value = existing

        service._cache_discovered_token(
            _token_metadata(symbol, address, decimals=18),
            chain,
            source="dexscreener_dynamic",
        )

        service._resolver.register.assert_not_called()


class TestAmbiguousMarker:
    """Blocker 3 regression: AmbiguousTokenError surfaces via gRPC with candidate addresses."""

    @pytest.mark.asyncio
    async def test_ambiguous_error_details_include_marker_and_addresses(
        self,
        service: TokenServiceServicer,
        mock_context: MagicMock,
    ) -> None:
        symbol = "DUPE"
        chain = "base"
        candidates = ["0x" + "aa" * 20, "0x" + "bb" * 20]
        service._resolver.resolve = MagicMock(side_effect=_token_not_found(symbol, chain))  # type: ignore[method-assign]

        ambig = AmbiguousTokenError(
            token=symbol,
            chain=chain,
            reason="Multiple liquid DUPE contracts on base",
            matching_addresses=candidates,
        )

        with (
            patch(
                "almanak.gateway.services.token_service.dexscreener_find_token_address",
                new=AsyncMock(side_effect=ambig),
            ),
            patch.object(
                service,
                "_coingecko_find_address",
                new=AsyncMock(return_value=None),
            ),
        ):
            from almanak.gateway.proto import gateway_pb2

            await service.ResolveToken(
                gateway_pb2.ResolveTokenRequest(token=symbol, chain=chain),
                mock_context,
            )

        details = mock_context.set_details.call_args.args[0]
        assert "AMBIGUOUS_SYMBOL" in details
        for candidate in candidates:
            assert candidate in details
