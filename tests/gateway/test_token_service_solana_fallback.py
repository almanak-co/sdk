"""Tests for the Solana token resolution fallback chain in TokenService.

The chain is: static registry -> Jupiter token list -> direct SPL mint RPC.
These tests lock in the ordering and ensure each stage is skipped cleanly once
an earlier stage returns a hit.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import grpc
import pytest

from almanak.framework.data.tokens import TokenNotFoundError
from almanak.framework.execution.solana.rpc import SolanaRpcError
from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.spl_mint_lookup import SPL_TOKEN_PROGRAM, SplMintInfo
from almanak.gateway.services.token_service import TokenServiceServicer


MINT_ADDRESS = "GWrbDx2K7vngKTcwipwEh99ia11DymNgERDAE7nCjNjc"


@pytest.fixture
def token_service() -> TokenServiceServicer:
    return TokenServiceServicer(GatewaySettings())


@pytest.fixture
def mock_context() -> MagicMock:
    ctx = MagicMock()
    ctx.set_code = MagicMock()
    ctx.set_details = MagicMock()
    return ctx


def _jupiter_meta(mint: str, symbol: str = "MSBR", decimals: int = 6) -> MagicMock:
    """Minimal stand-in for JupiterTokenMetadata."""
    meta = MagicMock()
    meta.address = mint
    meta.symbol = symbol
    meta.name = symbol
    meta.decimals = decimals
    return meta


class TestSolanaMintFallbackChain:
    @pytest.mark.asyncio
    async def test_jupiter_hit_skips_spl_lookup(self, token_service: TokenServiceServicer) -> None:
        """When Jupiter resolves the mint, SPL RPC must not be called.

        Jupiter provides the human-readable symbol we want; hitting RPC
        afterwards would waste a round-trip and risk shadowing Jupiter's data.
        """
        jupiter = MagicMock()
        jupiter.lookup_by_mint = MagicMock(return_value=_jupiter_meta(MINT_ADDRESS))
        spl_lookup = MagicMock()
        spl_lookup.lookup = AsyncMock()

        with (
            patch.object(token_service, "_get_jupiter", AsyncMock(return_value=jupiter)),
            patch.object(token_service, "_get_spl_lookup", AsyncMock(return_value=spl_lookup)),
        ):
            result = await token_service._try_solana_mint_lookup(MINT_ADDRESS)

        assert result is not None
        assert result.success is True
        assert result.decimals == 6
        assert result.symbol == "MSBR"
        spl_lookup.lookup.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_jupiter_miss_triggers_spl_fallback(self, token_service: TokenServiceServicer) -> None:
        """Jupiter's curated list excludes long-tail mints; SPL RPC must fill the gap."""
        jupiter = MagicMock()
        jupiter.lookup_by_mint = MagicMock(return_value=None)  # not in Jupiter
        spl_lookup = MagicMock()
        spl_lookup.lookup = AsyncMock(
            return_value=SplMintInfo(
                address=MINT_ADDRESS,
                decimals=9,
                owner_program=SPL_TOKEN_PROGRAM,
            )
        )

        with (
            patch.object(token_service, "_get_jupiter", AsyncMock(return_value=jupiter)),
            patch.object(token_service, "_get_spl_lookup", AsyncMock(return_value=spl_lookup)),
        ):
            result = await token_service._try_solana_mint_lookup(MINT_ADDRESS)

        assert result is not None
        assert result.success is True
        assert result.decimals == 9
        # We use the mint as the symbol when we have no off-chain name —
        # strategy code already holds the mint from Edge's sdkSpec.tokens[].
        assert result.symbol == MINT_ADDRESS
        assert result.source == "spl_onchain"
        spl_lookup.lookup.assert_awaited_once_with(MINT_ADDRESS)

    @pytest.mark.asyncio
    async def test_jupiter_raises_still_falls_back_to_spl(self, token_service: TokenServiceServicer) -> None:
        """A Jupiter exception (e.g. HTTP failure) must not block the RPC fallback."""
        jupiter = MagicMock()
        jupiter.lookup_by_mint = MagicMock(side_effect=RuntimeError("jupiter boom"))
        spl_lookup = MagicMock()
        spl_lookup.lookup = AsyncMock(
            return_value=SplMintInfo(
                address=MINT_ADDRESS,
                decimals=6,
                owner_program=SPL_TOKEN_PROGRAM,
            )
        )

        with (
            patch.object(token_service, "_get_jupiter", AsyncMock(return_value=jupiter)),
            patch.object(token_service, "_get_spl_lookup", AsyncMock(return_value=spl_lookup)),
        ):
            result = await token_service._try_solana_mint_lookup(MINT_ADDRESS)

        assert result is not None
        assert result.success is True
        assert result.decimals == 6
        spl_lookup.lookup.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_both_miss_returns_none(self, token_service: TokenServiceServicer) -> None:
        """True unresolvable mints must surface as None so the caller emits NOT_FOUND."""
        jupiter = MagicMock()
        jupiter.lookup_by_mint = MagicMock(return_value=None)
        spl_lookup = MagicMock()
        spl_lookup.lookup = AsyncMock(return_value=None)

        with (
            patch.object(token_service, "_get_jupiter", AsyncMock(return_value=jupiter)),
            patch.object(token_service, "_get_spl_lookup", AsyncMock(return_value=spl_lookup)),
        ):
            result = await token_service._try_solana_mint_lookup(MINT_ADDRESS)

        assert result is None


class TestGetTokenMetadataSolanaIntegration:
    """End-to-end tests through the gRPC method entry point."""

    @pytest.mark.asyncio
    async def test_get_token_metadata_spl_fallback_success(
        self, token_service: TokenServiceServicer, mock_context: MagicMock
    ) -> None:
        """GetTokenMetadata(solana, mint) returns success via SPL RPC when Jupiter misses."""
        jupiter = MagicMock()
        jupiter.lookup_by_mint = MagicMock(return_value=None)
        spl_lookup = MagicMock()
        spl_lookup.lookup = AsyncMock(
            return_value=SplMintInfo(
                address=MINT_ADDRESS,
                decimals=6,
                owner_program=SPL_TOKEN_PROGRAM,
            )
        )

        # Force the static-resolve fast path to miss so we reach the Solana branch.
        with (
            patch.object(token_service, "_get_jupiter", AsyncMock(return_value=jupiter)),
            patch.object(token_service, "_get_spl_lookup", AsyncMock(return_value=spl_lookup)),
            patch.object(
                token_service._resolver,
                "resolve",
                side_effect=TokenNotFoundError(token=MINT_ADDRESS, chain="solana", reason="miss"),
            ),
        ):
            request = gateway_pb2.GetTokenMetadataRequest(address=MINT_ADDRESS, chain="solana")
            response = await token_service.GetTokenMetadata(request, mock_context)

        assert response.success is True
        assert response.address == MINT_ADDRESS
        assert response.decimals == 6
        assert response.source == "spl_onchain"
        mock_context.set_code.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_token_metadata_solana_both_miss_returns_not_found(
        self, token_service: TokenServiceServicer, mock_context: MagicMock
    ) -> None:
        jupiter = MagicMock()
        jupiter.lookup_by_mint = MagicMock(return_value=None)
        spl_lookup = MagicMock()
        spl_lookup.lookup = AsyncMock(return_value=None)

        with (
            patch.object(token_service, "_get_jupiter", AsyncMock(return_value=jupiter)),
            patch.object(token_service, "_get_spl_lookup", AsyncMock(return_value=spl_lookup)),
            patch.object(
                token_service._resolver,
                "resolve",
                side_effect=TokenNotFoundError(token=MINT_ADDRESS, chain="solana", reason="miss"),
            ),
        ):
            request = gateway_pb2.GetTokenMetadataRequest(address=MINT_ADDRESS, chain="solana")
            response = await token_service.GetTokenMetadata(request, mock_context)

        assert response.success is False
        mock_context.set_code.assert_called_once_with(grpc.StatusCode.NOT_FOUND)

    @pytest.mark.asyncio
    async def test_resolver_cache_fast_path_short_circuits_solana(
        self, token_service: TokenServiceServicer, mock_context: MagicMock
    ) -> None:
        """A cached/registered Solana mint is served from the resolver without
        touching Jupiter or SPL RPC. Without this fast path a previously-resolved
        mint would be re-fetched (and, in the Jupiter-miss case, re-written with
        the mint address as symbol, silently degrading metadata)."""
        from almanak.core.enums import Chain
        from almanak.framework.data.tokens import ResolvedToken
        from almanak.framework.data.tokens.models import BridgeType

        cached = ResolvedToken(
            symbol="MSBR",
            address=MINT_ADDRESS,
            decimals=6,
            chain=Chain.SOLANA,
            chain_id=0,
            canonical_symbol="MSBR",
            bridge_type=BridgeType.NATIVE,
            source="jupiter",
            is_verified=False,
        )
        jupiter = MagicMock()
        jupiter.lookup_by_mint = MagicMock(return_value=None)
        spl_lookup = MagicMock()
        spl_lookup.lookup = AsyncMock(return_value=None)

        with (
            patch.object(token_service, "_get_jupiter", AsyncMock(return_value=jupiter)),
            patch.object(token_service, "_get_spl_lookup", AsyncMock(return_value=spl_lookup)),
            patch.object(token_service._resolver, "resolve", return_value=cached),
        ):
            request = gateway_pb2.GetTokenMetadataRequest(address=MINT_ADDRESS, chain="solana")
            response = await token_service.GetTokenMetadata(request, mock_context)

        assert response.success is True
        assert response.symbol == "MSBR"
        assert response.source == "jupiter"
        jupiter.lookup_by_mint.assert_not_called()
        spl_lookup.lookup.assert_not_awaited()
        mock_context.set_code.assert_not_called()

    @pytest.mark.asyncio
    async def test_solana_rpc_timeout_returns_deadline_exceeded(
        self, token_service: TokenServiceServicer, mock_context: MagicMock
    ) -> None:
        """A transient SPL RPC timeout must NOT be surfaced as NOT_FOUND. The
        resolver negative-caches NOT_FOUND responses, so a single timeout
        would poison resolution of a perfectly valid mint until TTL expires.
        DEADLINE_EXCEEDED signals "retry later" instead."""
        jupiter = MagicMock()
        jupiter.lookup_by_mint = MagicMock(return_value=None)
        spl_lookup = MagicMock()
        spl_lookup.lookup = AsyncMock(side_effect=TimeoutError())

        with (
            patch.object(token_service, "_get_jupiter", AsyncMock(return_value=jupiter)),
            patch.object(token_service, "_get_spl_lookup", AsyncMock(return_value=spl_lookup)),
            patch.object(
                token_service._resolver,
                "resolve",
                side_effect=TokenNotFoundError(token=MINT_ADDRESS, chain="solana", reason="miss"),
            ),
        ):
            request = gateway_pb2.GetTokenMetadataRequest(address=MINT_ADDRESS, chain="solana")
            response = await token_service.GetTokenMetadata(request, mock_context)

        assert response.success is False
        mock_context.set_code.assert_called_once_with(grpc.StatusCode.DEADLINE_EXCEEDED)

    @pytest.mark.asyncio
    async def test_solana_rpc_error_returns_unavailable(
        self, token_service: TokenServiceServicer, mock_context: MagicMock
    ) -> None:
        """Same reasoning as the timeout case: a SolanaRpcError is a transient
        failure, not a definitive miss, and must not be negative-cached."""
        jupiter = MagicMock()
        jupiter.lookup_by_mint = MagicMock(return_value=None)
        spl_lookup = MagicMock()
        spl_lookup.lookup = AsyncMock(
            side_effect=SolanaRpcError("getAccountInfo", "rpc down", code=-32000)
        )

        with (
            patch.object(token_service, "_get_jupiter", AsyncMock(return_value=jupiter)),
            patch.object(token_service, "_get_spl_lookup", AsyncMock(return_value=spl_lookup)),
            patch.object(
                token_service._resolver,
                "resolve",
                side_effect=TokenNotFoundError(token=MINT_ADDRESS, chain="solana", reason="miss"),
            ),
        ):
            request = gateway_pb2.GetTokenMetadataRequest(address=MINT_ADDRESS, chain="solana")
            response = await token_service.GetTokenMetadata(request, mock_context)

        assert response.success is False
        mock_context.set_code.assert_called_once_with(grpc.StatusCode.UNAVAILABLE)


class TestResolveTokenSolanaGuards:
    """ResolveToken must mirror GetTokenMetadata's Solana guardrails. Both
    endpoints end up calling _try_solana_mint_lookup, so any rate-limiter /
    transient-error defence applied on one side must apply on the other —
    otherwise ResolveToken becomes an unthrottled backdoor to the Solana
    RPC."""

    @pytest.mark.asyncio
    async def test_resolve_token_rate_limits_solana_mint(
        self, token_service: TokenServiceServicer, mock_context: MagicMock
    ) -> None:
        """A saturated bucket must short-circuit with RESOURCE_EXHAUSTED
        before any Solana RPC call is issued."""
        # Drain the bucket to saturate the limiter.
        token_service._rate_limiter.acquire = AsyncMock(return_value=False)
        token_service._rate_limiter.wait_and_acquire = AsyncMock(return_value=False)

        from almanak.framework.data.tokens import TokenNotFoundError

        with patch.object(
            token_service._resolver,
            "resolve",
            side_effect=TokenNotFoundError(token=MINT_ADDRESS, chain="solana", reason="miss"),
        ):
            request = gateway_pb2.ResolveTokenRequest(token=MINT_ADDRESS, chain="solana")
            response = await token_service.ResolveToken(request, mock_context)

        assert response.success is False
        mock_context.set_code.assert_called_once_with(grpc.StatusCode.RESOURCE_EXHAUSTED)

    @pytest.mark.asyncio
    async def test_resolve_token_solana_rpc_timeout_is_deadline_exceeded(
        self, token_service: TokenServiceServicer, mock_context: MagicMock
    ) -> None:
        """ResolveToken must map SPL RPC timeouts to DEADLINE_EXCEEDED — the
        same contract GetTokenMetadata uses. Letting TimeoutError escape
        here would show up to the client as an uncategorized INTERNAL and
        get negative-cached."""
        jupiter = MagicMock()
        jupiter.lookup_by_mint = MagicMock(return_value=None)
        spl_lookup = MagicMock()
        spl_lookup.lookup = AsyncMock(side_effect=TimeoutError())

        from almanak.framework.data.tokens import TokenNotFoundError

        with (
            patch.object(token_service, "_get_jupiter", AsyncMock(return_value=jupiter)),
            patch.object(token_service, "_get_spl_lookup", AsyncMock(return_value=spl_lookup)),
            patch.object(
                token_service._resolver,
                "resolve",
                side_effect=TokenNotFoundError(token=MINT_ADDRESS, chain="solana", reason="miss"),
            ),
        ):
            request = gateway_pb2.ResolveTokenRequest(token=MINT_ADDRESS, chain="solana")
            response = await token_service.ResolveToken(request, mock_context)

        assert response.success is False
        mock_context.set_code.assert_called_once_with(grpc.StatusCode.DEADLINE_EXCEEDED)

    @pytest.mark.asyncio
    async def test_resolve_token_solana_rpc_error_is_unavailable(
        self, token_service: TokenServiceServicer, mock_context: MagicMock
    ) -> None:
        jupiter = MagicMock()
        jupiter.lookup_by_mint = MagicMock(return_value=None)
        spl_lookup = MagicMock()
        spl_lookup.lookup = AsyncMock(
            side_effect=SolanaRpcError("getAccountInfo", "rpc down", code=-32000)
        )

        from almanak.framework.data.tokens import TokenNotFoundError

        with (
            patch.object(token_service, "_get_jupiter", AsyncMock(return_value=jupiter)),
            patch.object(token_service, "_get_spl_lookup", AsyncMock(return_value=spl_lookup)),
            patch.object(
                token_service._resolver,
                "resolve",
                side_effect=TokenNotFoundError(token=MINT_ADDRESS, chain="solana", reason="miss"),
            ),
        ):
            request = gateway_pb2.ResolveTokenRequest(token=MINT_ADDRESS, chain="solana")
            response = await token_service.ResolveToken(request, mock_context)

        assert response.success is False
        mock_context.set_code.assert_called_once_with(grpc.StatusCode.UNAVAILABLE)


class TestGetTokenMetadataFastPathErrors:
    """The fast-path in GetTokenMetadata must not silently swallow non-
    NotFound resolver errors. Letting, e.g., AmbiguousTokenError or an
    InvalidTokenAddressError from the resolver cascade into the dynamic
    RPC path would waste a rate-limited slot and mask a local failure."""

    @pytest.mark.asyncio
    async def test_resolver_resolution_error_surfaces_as_internal(
        self, token_service: TokenServiceServicer, mock_context: MagicMock
    ) -> None:
        from almanak.framework.data.tokens import TokenResolutionError

        with patch.object(
            token_service._resolver,
            "resolve",
            side_effect=TokenResolutionError(token=MINT_ADDRESS, chain="solana", reason="resolver boom"),
        ):
            request = gateway_pb2.GetTokenMetadataRequest(address=MINT_ADDRESS, chain="solana")
            response = await token_service.GetTokenMetadata(request, mock_context)

        assert response.success is False
        mock_context.set_code.assert_called_once_with(grpc.StatusCode.INTERNAL)


class TestSourceRankGuard:
    """The ``_SOURCE_RANK`` first-write-wins guard must apply to SPL fallbacks.
    Without it, a Jupiter-miss followed by a successful SPL lookup would
    silently replace a richer Jupiter cache entry with ``symbol=<mint>``."""

    @pytest.mark.asyncio
    async def test_spl_onchain_does_not_clobber_jupiter(
        self, token_service: TokenServiceServicer
    ) -> None:
        """Given a cached jupiter entry (rank 30), an incoming spl_onchain
        (rank 20) must be blocked from overwriting by _cache_discovered_token.
        We verify by asserting register() is NOT called."""
        spl_lookup = MagicMock()
        spl_lookup.lookup = AsyncMock(
            return_value=SplMintInfo(
                address=MINT_ADDRESS,
                decimals=6,
                owner_program=SPL_TOKEN_PROGRAM,
            )
        )

        # Simulate an existing jupiter cache entry.
        from almanak.core.enums import Chain
        from almanak.framework.data.tokens import ResolvedToken
        from almanak.framework.data.tokens.models import BridgeType

        jupiter_cached = ResolvedToken(
            symbol="MSBR",
            address=MINT_ADDRESS,
            decimals=6,
            chain=Chain.SOLANA,
            chain_id=0,
            canonical_symbol="MSBR",
            bridge_type=BridgeType.NATIVE,
            source="jupiter",
            is_verified=False,
        )

        with (
            patch.object(token_service, "_get_spl_lookup", AsyncMock(return_value=spl_lookup)),
            # _cache_discovered_token calls resolver.resolve(..., skip_gateway=True)
            # to check for an existing entry under address or symbol.
            patch.object(token_service._resolver, "resolve", return_value=jupiter_cached),
            patch.object(token_service._resolver, "register") as mock_register,
        ):
            await token_service._try_spl_mint_rpc_lookup(MINT_ADDRESS)

        # The rank guard should block the SPL write.
        mock_register.assert_not_called()

    @pytest.mark.asyncio
    async def test_spl_onchain_writes_when_no_prior_entry(
        self, token_service: TokenServiceServicer
    ) -> None:
        """With no prior cache entry, the SPL fallback should populate the
        cache. This is the happy-path that the bug fix must preserve."""
        spl_lookup = MagicMock()
        spl_lookup.lookup = AsyncMock(
            return_value=SplMintInfo(
                address=MINT_ADDRESS,
                decimals=6,
                owner_program=SPL_TOKEN_PROGRAM,
            )
        )

        with (
            patch.object(token_service, "_get_spl_lookup", AsyncMock(return_value=spl_lookup)),
            patch.object(
                token_service._resolver,
                "resolve",
                side_effect=TokenNotFoundError(token=MINT_ADDRESS, chain="solana", reason="miss"),
            ),
            patch.object(token_service._resolver, "register") as mock_register,
        ):
            result = await token_service._try_spl_mint_rpc_lookup(MINT_ADDRESS)

        assert result is not None
        assert result.source == "spl_onchain"
        mock_register.assert_called_once()
        registered = mock_register.call_args.args[0]
        assert registered.address == MINT_ADDRESS
        assert registered.decimals == 6
        assert registered.source == "spl_onchain"
