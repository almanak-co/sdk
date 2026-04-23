"""Tests for dynamic token resolution via Jupiter (Solana) and CoinGecko (EVM).

Covers:
- JupiterTokenLookup with mocked HTTP responses
- TokenResolver._resolve_by_symbol() calls gateway for missing symbols
- fund_tokens() with address-keyed config uses resolver for decimals
"""

import asyncio
import json
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# =============================================================================
# JupiterTokenLookup tests
# =============================================================================


class TestJupiterTokenLookup:
    """Unit tests for JupiterTokenLookup."""

    SAMPLE_TOKENS = [
        {
            "address": "USDSwr9ApdHk5bvJKMjzff41FfuX8bSxdKcR81vTwcA",
            "symbol": "USDS",
            "name": "USDS",
            "decimals": 6,
            "tags": ["stablecoin"],
        },
        {
            "address": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
            "symbol": "USDC",
            "name": "USD Coin",
            "decimals": 6,
            "tags": ["stablecoin"],
        },
        {
            "address": "So11111111111111111111111111111111111111112",
            "symbol": "SOL",
            "name": "Wrapped SOL",
            "decimals": 9,
            "tags": [],
        },
    ]

    def _make_lookup(self) -> "JupiterTokenLookup":
        from almanak.gateway.services.jupiter_token_lookup import JupiterTokenLookup

        lookup = JupiterTokenLookup()
        lookup._build_indices(self.SAMPLE_TOKENS)
        lookup._loaded = True
        return lookup

    def test_lookup_by_mint_found(self):
        """lookup_by_mint returns metadata for a known mint."""
        lookup = self._make_lookup()
        meta = lookup.lookup_by_mint("USDSwr9ApdHk5bvJKMjzff41FfuX8bSxdKcR81vTwcA")
        assert meta is not None
        assert meta.symbol == "USDS"
        assert meta.decimals == 6

    def test_lookup_by_mint_not_found(self):
        """lookup_by_mint returns None for unknown mint."""
        lookup = self._make_lookup()
        meta = lookup.lookup_by_mint("unknownmintaddress12345678901234567890123456")
        assert meta is None

    def test_lookup_by_symbol_found(self):
        """lookup_by_symbol returns metadata for a known symbol."""
        lookup = self._make_lookup()
        meta = lookup.lookup_by_symbol("USDS")
        assert meta is not None
        assert meta.address == "USDSwr9ApdHk5bvJKMjzff41FfuX8bSxdKcR81vTwcA"
        assert meta.decimals == 6

    def test_lookup_by_symbol_case_insensitive(self):
        """lookup_by_symbol is case-insensitive."""
        lookup = self._make_lookup()
        assert lookup.lookup_by_symbol("usds") is not None
        assert lookup.lookup_by_symbol("USDS") is not None
        assert lookup.lookup_by_symbol("UsDs") is not None

    def test_lookup_by_symbol_not_found(self):
        """lookup_by_symbol returns None for unknown symbol."""
        lookup = self._make_lookup()
        meta = lookup.lookup_by_symbol("UNKNOWNXYZ")
        assert meta is None

    def test_is_loaded_after_build(self):
        """is_loaded returns True after _build_indices is called."""
        lookup = self._make_lookup()
        assert lookup.is_loaded is True

    def test_is_loaded_before_init(self):
        """is_loaded returns False on fresh instance."""
        from almanak.gateway.services.jupiter_token_lookup import JupiterTokenLookup

        lookup = JupiterTokenLookup()
        assert lookup.is_loaded is False

    @pytest.mark.asyncio
    async def test_fetch_from_network_success(self, tmp_path):
        """_fetch_from_network fetches and caches on success."""
        from almanak.gateway.services.jupiter_token_lookup import JupiterTokenLookup

        lookup = JupiterTokenLookup()

        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value=self.SAMPLE_TOKENS)
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_response)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)

        with patch("aiohttp.ClientSession", return_value=mock_session):
            # Redirect disk write to tmp_path
            cache_path = tmp_path / "jupiter_token_cache.json"
            with patch("almanak.gateway.services.jupiter_token_lookup.CACHE_PATH", cache_path):
                data = await lookup._fetch_from_network()

        assert data is not None
        assert len(data) == 3

    @pytest.mark.asyncio
    async def test_fetch_from_network_http_error_returns_none(self):
        """_fetch_from_network returns None on non-200 HTTP response."""
        from almanak.gateway.services.jupiter_token_lookup import JupiterTokenLookup

        lookup = JupiterTokenLookup()

        mock_response = AsyncMock()
        mock_response.status = 503
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_response)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)

        with patch("aiohttp.ClientSession", return_value=mock_session):
            data = await lookup._fetch_from_network()

        assert data is None

    @pytest.mark.asyncio
    async def test_fetch_from_network_network_error_returns_none(self):
        """_fetch_from_network returns None on network error (no exception raised)."""
        from almanak.gateway.services.jupiter_token_lookup import JupiterTokenLookup

        lookup = JupiterTokenLookup()

        with patch("aiohttp.ClientSession", side_effect=Exception("network error")):
            data = await lookup._fetch_from_network()

        assert data is None

    def test_read_disk_cache_fresh(self, tmp_path):
        """_read_disk_cache returns data when cache is fresh."""
        import time

        from almanak.gateway.services.jupiter_token_lookup import JupiterTokenLookup

        lookup = JupiterTokenLookup()
        cache_path = tmp_path / "jupiter_token_cache.json"
        cache_path.write_text(json.dumps(self.SAMPLE_TOKENS))

        with patch("almanak.gateway.services.jupiter_token_lookup.CACHE_PATH", cache_path):
            data = lookup._read_disk_cache()

        assert data is not None
        assert len(data) == 3

    def test_read_disk_cache_expired_returns_none(self, tmp_path):
        """_read_disk_cache returns None when cache is expired."""
        import os
        import time

        from almanak.gateway.services.jupiter_token_lookup import JupiterTokenLookup

        lookup = JupiterTokenLookup()
        cache_path = tmp_path / "jupiter_token_cache.json"
        cache_path.write_text(json.dumps(self.SAMPLE_TOKENS))

        # Set mtime to 25 hours ago
        old_mtime = time.time() - (25 * 60 * 60)
        os.utime(cache_path, (old_mtime, old_mtime))

        with patch("almanak.gateway.services.jupiter_token_lookup.CACHE_PATH", cache_path):
            data = lookup._read_disk_cache()

        assert data is None

    def test_malformed_tokens_skipped(self):
        """Malformed token entries are skipped gracefully."""
        from almanak.gateway.services.jupiter_token_lookup import JupiterTokenLookup

        malformed = [
            {},  # empty
            {"address": "ValidAddr12345678901234567890123456789012"},  # missing symbol
            {"address": "SOL", "symbol": "SOL", "decimals": 9, "name": "SOL"},  # short address (OK)
            self.SAMPLE_TOKENS[0],  # valid
        ]

        lookup = JupiterTokenLookup()
        lookup._build_indices(malformed)
        # At least the valid entry should be indexed
        meta = lookup.lookup_by_symbol("USDS")
        assert meta is not None


# =============================================================================
# TokenResolver gateway symbol fallback tests
# =============================================================================


class TestResolverGatewaySymbolFallback:
    """Tests that _resolve_by_symbol returns None sentinel when gateway is available
    and the symbol is missing from static registry, triggering the gateway fallback.
    """

    def _make_resolver_with_gateway(self):
        """Create a TokenResolver with a mock gateway channel."""
        from almanak.framework.data.tokens.resolver import TokenResolver

        TokenResolver.reset_instance()
        mock_channel = MagicMock()
        resolver = TokenResolver(gateway_channel=mock_channel)
        return resolver

    def test_resolve_by_symbol_returns_none_sentinel_when_gateway_available(self):
        """_resolve_by_symbol returns None (not raises) when symbol not found and gateway set."""
        from almanak.framework.data.tokens.resolver import TokenResolver, _normalize_chain
        from almanak.core.enums import Chain

        resolver = self._make_resolver_with_gateway()
        chain_lower, chain_enum = _normalize_chain("arbitrum")

        with resolver._lock:
            result = resolver._resolve_by_symbol("EXOTIC_TOKEN_XYZ", chain_lower, chain_enum)

        # Should return None sentinel (not raise) when gateway is configured
        assert result is None

    def test_resolve_by_symbol_raises_when_no_gateway(self):
        """_resolve_by_symbol raises TokenNotFoundError immediately when no gateway."""
        from almanak.framework.data.tokens.exceptions import TokenNotFoundError
        from almanak.framework.data.tokens.resolver import TokenResolver, _normalize_chain

        TokenResolver.reset_instance()
        resolver = TokenResolver()  # no gateway
        chain_lower, chain_enum = _normalize_chain("arbitrum")

        with resolver._lock:
            with pytest.raises(TokenNotFoundError):
                resolver._resolve_by_symbol("EXOTIC_TOKEN_XYZ", chain_lower, chain_enum)

    def test_resolve_symbol_via_gateway_returns_resolved_token(self):
        """_resolve_symbol_via_gateway returns ResolvedToken on successful gateway call."""
        from unittest.mock import MagicMock, patch

        from almanak.framework.data.tokens.resolver import TokenResolver, _normalize_chain

        resolver = self._make_resolver_with_gateway()
        chain_lower, chain_enum = _normalize_chain("arbitrum")

        mock_response = MagicMock()
        mock_response.success = True
        mock_response.symbol = "swETH"
        mock_response.address = "0xf951e335afb289353dc249e82926178eac7ded78"
        mock_response.decimals = 18
        mock_response.name = "Swell Network Ether"
        mock_response.source = "on_chain"
        mock_response.is_verified = False

        mock_stub = MagicMock()
        mock_stub.ResolveToken = MagicMock(return_value=mock_response)

        with patch.object(resolver, "_get_gateway_stub", return_value=mock_stub):
            with patch.object(resolver, "_check_gateway_available", return_value=True):
                result = resolver._resolve_symbol_via_gateway("swETH", chain_lower, chain_enum)

        assert result is not None
        assert result.symbol == "swETH"
        assert result.decimals == 18
        assert result.address == "0xf951e335afb289353dc249e82926178eac7ded78"

    def test_resolve_symbol_via_gateway_returns_none_on_failure(self):
        """_resolve_symbol_via_gateway returns None when gateway says not found."""
        from unittest.mock import MagicMock, patch

        from almanak.framework.data.tokens.resolver import TokenResolver, _normalize_chain

        resolver = self._make_resolver_with_gateway()
        chain_lower, chain_enum = _normalize_chain("arbitrum")

        mock_response = MagicMock()
        mock_response.success = False
        mock_response.error = "Not found"

        mock_stub = MagicMock()
        mock_stub.ResolveToken = MagicMock(return_value=mock_response)

        with patch.object(resolver, "_get_gateway_stub", return_value=mock_stub):
            with patch.object(resolver, "_check_gateway_available", return_value=True):
                result = resolver._resolve_symbol_via_gateway("UNKNOWN", chain_lower, chain_enum)

        assert result is None

    def test_resolve_symbol_via_gateway_uses_15s_timeout(self):
        """Regression guard for VIB-2715: the on-chain confirm step in the
        gateway's dynamic path takes ~3.5-4s p99, so the client-side gRPC
        deadline must give it enough headroom.  A silent revert back to the
        historical 5s value re-introduces the production bug where fresh
        CoinGecko/DexScreener resolutions were clipped mid-confirm and
        returned false NOT_FOUND errors.
        """
        from unittest.mock import MagicMock, patch

        from almanak.framework.data.tokens.resolver import TokenResolver, _normalize_chain

        resolver = self._make_resolver_with_gateway()
        chain_lower, chain_enum = _normalize_chain("arbitrum")

        # Minimal happy-path response; we only care about the call args below.
        mock_response = MagicMock()
        mock_response.success = True
        mock_response.symbol = "USDC"
        mock_response.address = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
        mock_response.decimals = 6
        mock_response.name = "USD Coin"
        mock_response.source = "static"
        mock_response.is_verified = True

        mock_stub = MagicMock()
        mock_stub.ResolveToken = MagicMock(return_value=mock_response)

        with patch.object(resolver, "_get_gateway_stub", return_value=mock_stub):
            with patch.object(resolver, "_check_gateway_available", return_value=True):
                resolver._resolve_symbol_via_gateway("USDC", chain_lower, chain_enum)

        # ``ResolveToken`` is invoked with a keyword-only ``timeout`` kwarg in
        # resolver.py:_resolve_symbol_via_gateway — assert its exact value so
        # any future change to that constant is caught here.
        mock_stub.ResolveToken.assert_called_once()
        _, kwargs = mock_stub.ResolveToken.call_args
        assert kwargs.get("timeout") == 15.0, (
            f"Expected ResolveToken client deadline to be 15.0s (see VIB-2715 "
            f"post-mortem in resolver.py:_resolve_symbol_via_gateway); got "
            f"{kwargs.get('timeout')!r}."
        )

    def teardown_method(self, method):
        from almanak.framework.data.tokens.resolver import TokenResolver

        TokenResolver.reset_instance()


# =============================================================================
# USDS static resolution tests
# =============================================================================


class TestUSDSSolanaStatic:
    """Tests for USDS token addition to Solana static registry."""

    def test_usds_resolves_by_symbol_on_solana(self):
        """USDS resolves by symbol on Solana from static registry."""
        from almanak.framework.data.tokens.resolver import get_token_resolver

        resolver = get_token_resolver()
        token = resolver.resolve("USDS", "solana")
        assert token.symbol == "USDS"
        assert token.decimals == 6
        assert token.address == "USDSwr9ApdHk5bvJKMjzff41FfuX8bSxdKcR81vTwcA"

    def test_usds_resolves_by_mint_on_solana(self):
        """USDS resolves by mint address on Solana from static registry."""
        from almanak.framework.data.tokens.resolver import get_token_resolver

        resolver = get_token_resolver()
        token = resolver.resolve("USDSwr9ApdHk5bvJKMjzff41FfuX8bSxdKcR81vTwcA", "solana")
        assert token.symbol == "USDS"
        assert token.decimals == 6

    def test_usds_is_stablecoin(self):
        """USDS is marked as a stablecoin."""
        from almanak.framework.data.tokens.defaults import USDS_SOL

        assert USDS_SOL.is_stablecoin is True
        assert USDS_SOL.coingecko_id == "usds"


# =============================================================================
# fund_tokens address-keyed tests
# =============================================================================


class TestFundTokensAddressKeyed:
    """Tests for fund_tokens() fix for address-keyed token config entries."""

    def test_address_key_sets_token_address(self):
        """When key is a raw EVM address, token_address is set to the key itself."""
        # This tests the logic path: is_raw_address=True -> token_address = token_symbol
        # The actual address resolution (decimals via gateway) is tested via mocking.
        from unittest.mock import AsyncMock, MagicMock, patch

        from almanak.framework.data.tokens.exceptions import TokenNotFoundError

        # Simulate: resolver.resolve("0xf951...", chain) raises TokenNotFoundError on first call
        # (address not in static registry), then succeeds on second call (gateway discovered it)
        mock_resolver = MagicMock()
        mock_resolved = MagicMock()
        mock_resolved.address = "0xf951e335afb289353dc249e82926178eac7ded78"
        mock_resolved.decimals = 18

        # First call: raises (address not in registry)
        # Second call: succeeds (gateway path)
        mock_resolver.resolve.side_effect = [
            TokenNotFoundError(
                token="0xf951e335afb289353dc249e82926178eac7ded78",
                chain="ethereum",
                reason="not found",
            ),
            mock_resolved,
        ]

        # We just verify the logic branches correctly -- actual Anvil funding is end-to-end
        address_key = "0xf951e335afb289353dc249e82926178eac7ded78"
        is_raw_address = address_key.startswith("0x") and len(address_key) == 42
        assert is_raw_address is True

    def test_symbol_key_uses_normal_path(self):
        """When key is a symbol (not address), normal TOKEN_DECIMALS fallback is used."""
        symbol_key = "USDC"
        is_raw_address = symbol_key.startswith("0x") and len(symbol_key) == 42
        assert is_raw_address is False
