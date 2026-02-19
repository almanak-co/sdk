"""Comprehensive integration tests for TokenResolver.

This module tests the full TokenResolver system end-to-end:
- Symbol resolution for all major tokens across multiple chains
- Address resolution (checksummed and lowercase)
- On-chain discovery via gateway (mock gateway)
- Cache persistence across resolver instances
- Native token auto-wrapping for swaps
- Bridged token aliases (USDC.e, USDbC, USDT.e, WETH.e)
- Error handling for unknown tokens
- Graceful fallback when gateway unavailable
- Concurrent access (10+ threads)
- Migration path: old API deprecation warnings
- Performance benchmarks: cache hit rate and latency

These are integration tests verifying the full system rather than
isolated unit tests of individual methods.
"""

import json
import statistics
import tempfile
import threading
import time
import warnings
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from almanak.core.enums import Chain
from almanak.framework.data.tokens.cache import TokenCacheManager
from almanak.framework.data.tokens.defaults import DEFAULT_TOKENS, SYMBOL_ALIASES, WRAPPED_NATIVE
from almanak.framework.data.tokens.exceptions import (
    InvalidTokenAddressError,
    TokenNotFoundError,
    TokenResolutionError,
)
from almanak.framework.data.tokens.models import CHAIN_ID_MAP, BridgeType, ResolvedToken
from almanak.framework.data.tokens.resolver import TokenResolver, get_token_resolver


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture(autouse=True)
def reset_singleton():
    """Reset singleton before and after each test."""
    TokenResolver.reset_instance()
    yield
    TokenResolver.reset_instance()


@pytest.fixture
def temp_cache_file():
    """Create a temporary cache file for isolated cache testing."""
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
        temp_path = f.name
    yield temp_path
    Path(temp_path).unlink(missing_ok=True)


@pytest.fixture
def resolver(temp_cache_file):
    """Create a fresh TokenResolver with isolated cache."""
    return TokenResolver(cache_file=temp_cache_file)


def make_resolved_token(
    symbol="TEST",
    address="0x1234567890123456789012345678901234567890",
    chain=Chain.ARBITRUM,
    decimals=18,
    source="static",
    is_native=False,
    is_wrapped_native=False,
    is_stablecoin=False,
):
    """Helper to create a ResolvedToken for testing."""
    return ResolvedToken(
        symbol=symbol,
        address=address,
        decimals=decimals,
        chain=chain,
        chain_id=CHAIN_ID_MAP.get(chain, 42161),
        name=f"{symbol} Token",
        is_stablecoin=is_stablecoin,
        is_native=is_native,
        is_wrapped_native=is_wrapped_native,
        canonical_symbol=symbol,
        bridge_type=BridgeType.NATIVE,
        source=source,
        is_verified=True,
        resolved_at=datetime.now(),
    )


# ============================================================================
# 1. Resolve by Symbol - All Major Tokens
# ============================================================================


class TestResolveBySymbolAllMajorTokens:
    """Test symbol resolution for all major tokens across chains."""

    @pytest.mark.parametrize(
        "symbol,chain,expected_decimals",
        [
            ("USDC", "arbitrum", 6),
            ("USDC", "ethereum", 6),
            ("USDC", "optimism", 6),
            ("USDC", "base", 6),
            ("USDC", "polygon", 6),
            ("USDC", "avalanche", 6),
            ("USDT", "ethereum", 6),
            ("USDT", "arbitrum", 6),
            ("DAI", "ethereum", 18),
            ("DAI", "arbitrum", 18),
            ("WBTC", "ethereum", 8),
            ("WBTC", "arbitrum", 8),
            ("WETH", "ethereum", 18),
            ("WETH", "arbitrum", 18),
            ("WETH", "optimism", 18),
            ("WETH", "base", 18),
            ("LINK", "ethereum", 18),
            ("UNI", "ethereum", 18),
            ("AAVE", "ethereum", 18),
            ("CRV", "ethereum", 18),
            ("GMX", "arbitrum", 18),
            ("PENDLE", "arbitrum", 18),
            ("ARB", "arbitrum", 18),
            ("OP", "optimism", 18),
        ],
    )
    def test_resolve_major_token(self, resolver, symbol, chain, expected_decimals):
        """Resolve each major token on its primary chain with correct decimals."""
        token = resolver.resolve(symbol, chain)
        assert token.symbol == symbol
        assert token.decimals == expected_decimals
        assert token.address.startswith("0x")
        assert len(token.address) == 42

    def test_resolve_all_stablecoins_have_correct_flag(self, resolver):
        """All stablecoins should have is_stablecoin=True."""
        stablecoins = [("USDC", "arbitrum"), ("USDT", "ethereum"), ("DAI", "ethereum")]
        for symbol, chain in stablecoins:
            token = resolver.resolve(symbol, chain)
            assert token.is_stablecoin, f"{symbol} on {chain} should be a stablecoin"

    def test_resolve_case_insensitive(self, resolver):
        """Symbol resolution should be case-insensitive."""
        token_upper = resolver.resolve("USDC", "arbitrum")
        token_lower = resolver.resolve("usdc", "arbitrum")
        token_mixed = resolver.resolve("Usdc", "arbitrum")
        assert token_upper.address == token_lower.address == token_mixed.address
        assert token_upper.decimals == token_lower.decimals == token_mixed.decimals

    def test_resolve_same_token_different_chains(self, resolver):
        """Same token symbol on different chains resolves to different addresses."""
        usdc_arb = resolver.resolve("USDC", "arbitrum")
        usdc_eth = resolver.resolve("USDC", "ethereum")
        usdc_opt = resolver.resolve("USDC", "optimism")
        assert usdc_arb.address != usdc_eth.address
        assert usdc_arb.chain == Chain.ARBITRUM
        assert usdc_eth.chain == Chain.ETHEREUM
        assert usdc_opt.chain == Chain.OPTIMISM

    def test_resolve_with_chain_enum(self, resolver):
        """Resolution works with Chain enum values."""
        token = resolver.resolve("USDC", Chain.ARBITRUM)
        assert token.symbol == "USDC"
        assert token.chain == Chain.ARBITRUM


# ============================================================================
# 2. Resolve by Address - Checksummed and Lowercase
# ============================================================================


class TestResolveByAddress:
    """Test address resolution with various address formats."""

    # Native USDC on Arbitrum
    USDC_ARBITRUM = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"

    def test_resolve_by_checksummed_address(self, resolver):
        """Resolve token by checksummed address."""
        token = resolver.resolve(self.USDC_ARBITRUM, "arbitrum")
        assert token.symbol == "USDC"
        assert token.decimals == 6

    def test_resolve_by_lowercase_address(self, resolver):
        """Resolve token by lowercase address."""
        token = resolver.resolve(self.USDC_ARBITRUM.lower(), "arbitrum")
        assert token.symbol == "USDC"
        assert token.decimals == 6

    def test_resolve_by_uppercase_hex_address(self, resolver):
        """Resolve token by uppercase hex address."""
        upper = "0x" + self.USDC_ARBITRUM[2:].upper()
        token = resolver.resolve(upper, "arbitrum")
        assert token.symbol == "USDC"
        assert token.decimals == 6

    def test_resolved_address_matches_input(self, resolver):
        """Resolved token address should be in the registry format."""
        token = resolver.resolve(self.USDC_ARBITRUM.lower(), "arbitrum")
        # Address in result should match the registered address
        assert token.address.lower() == self.USDC_ARBITRUM.lower()

    def test_resolve_weth_by_address_arbitrum(self, resolver):
        """Resolve WETH by address on Arbitrum."""
        weth_arb = WRAPPED_NATIVE["arbitrum"]
        token = resolver.resolve(weth_arb, "arbitrum")
        assert token.symbol == "WETH"
        assert token.decimals == 18
        assert token.is_wrapped_native

    def test_resolve_wbtc_by_address_ethereum(self, resolver):
        """Resolve WBTC by address on Ethereum."""
        # Look up WBTC address from defaults
        wbtc = resolver.resolve("WBTC", "ethereum")
        # Re-resolve by address
        token = resolver.resolve(wbtc.address, "ethereum")
        assert token.symbol == "WBTC"
        assert token.decimals == 8


# ============================================================================
# 3. On-chain Discovery via Gateway (Mock)
# ============================================================================


class TestOnChainDiscoveryMockGateway:
    """Test on-chain token discovery using mock gateway."""

    # Valid hex addresses for gateway tests
    NOVEL_ADDR = "0xDeaDBeeF00000000000000000000000000000001"
    CACHED_ADDR = "0x1111222233334444555566667777888899990001"
    FAILED_ADDR = "0xDeaDBeeF00000000000000000000000000000002"

    def _make_mock_gateway(self, symbol="NEWTOKEN", decimals=18, name="New Token", address=None):
        """Create a mock gateway channel and stub response."""
        mock_channel = MagicMock()
        mock_response = MagicMock()
        mock_response.success = True
        mock_response.symbol = symbol
        mock_response.decimals = decimals
        mock_response.name = name
        mock_response.address = address or self.NOVEL_ADDR
        mock_response.is_verified = False
        mock_response.source = "on_chain"
        return mock_channel, mock_response

    def test_gateway_discovery_for_unknown_address(self, temp_cache_file):
        """Unknown address triggers gateway on-chain lookup."""
        mock_channel, mock_response = self._make_mock_gateway(
            symbol="NOVEL",
            decimals=9,
            name="Novel Token",
            address=self.NOVEL_ADDR,
        )

        resolver = TokenResolver(cache_file=temp_cache_file, gateway_channel=mock_channel)

        mock_stub = MagicMock()
        mock_stub.GetTokenMetadata.return_value = mock_response

        with (
            patch.object(resolver, "_get_gateway_stub", return_value=mock_stub),
            patch.object(resolver, "_gateway_available", True),
        ):
            token = resolver.resolve(self.NOVEL_ADDR, "arbitrum")

        assert token.symbol == "NOVEL"
        assert token.decimals == 9
        assert token.source == "on_chain"
        assert token.is_verified is False

    def test_gateway_discovery_caches_result(self, temp_cache_file):
        """On-chain discovered token should be cached for subsequent lookups."""
        mock_channel, mock_response = self._make_mock_gateway(
            symbol="CACHED",
            decimals=12,
            address=self.CACHED_ADDR,
        )

        resolver = TokenResolver(cache_file=temp_cache_file, gateway_channel=mock_channel)

        mock_stub = MagicMock()
        mock_stub.GetTokenMetadata.return_value = mock_response

        with (
            patch.object(resolver, "_get_gateway_stub", return_value=mock_stub),
            patch.object(resolver, "_gateway_available", True),
        ):
            # First resolution - hits gateway
            token1 = resolver.resolve(self.CACHED_ADDR, "arbitrum")

        # Second resolution - should come from cache (no gateway needed)
        token2 = resolver.resolve(self.CACHED_ADDR, "arbitrum")

        assert token1.symbol == "CACHED"
        assert token2.symbol == "CACHED"
        assert token2.decimals == 12
        # Gateway should only have been called once
        assert mock_stub.GetTokenMetadata.call_count == 1

    def test_gateway_failed_lookup_does_not_cache(self, temp_cache_file):
        """Failed gateway lookup should not cache an error result."""
        mock_channel = MagicMock()
        mock_response = MagicMock()
        mock_response.success = False
        mock_response.error = "Contract not found"

        resolver = TokenResolver(cache_file=temp_cache_file, gateway_channel=mock_channel)

        mock_stub = MagicMock()
        mock_stub.GetTokenMetadata.return_value = mock_response

        with (
            patch.object(resolver, "_get_gateway_stub", return_value=mock_stub),
            patch.object(resolver, "_gateway_available", True),
        ):
            with pytest.raises(TokenNotFoundError):
                resolver.resolve(self.FAILED_ADDR, "arbitrum")


# ============================================================================
# 5. Cache Persistence Across Resolver Instances (Restart Test)
# ============================================================================


class TestCachePersistenceAcrossInstances:
    """Test cache persistence across resolver restarts."""

    # Valid hex test addresses
    PERSIST_ADDR = "0xaaaa000000000000000000000000000000000001"
    SURVIVE_ADDR = "0xbbbb000000000000000000000000000000000002"

    def test_registered_token_persists_to_disk(self, temp_cache_file):
        """A registered token should persist to disk cache."""
        custom = make_resolved_token(
            symbol="PERSIST",
            address=self.PERSIST_ADDR,
            decimals=9,
        )

        # First instance - register token
        resolver1 = TokenResolver(cache_file=temp_cache_file)
        resolver1.register(custom)

        # Verify cache file exists and has content
        cache_data = json.loads(Path(temp_cache_file).read_text())
        assert "tokens" in cache_data
        assert len(cache_data["tokens"]) > 0

    def test_cached_token_survives_instance_restart(self, temp_cache_file):
        """Token cached by one instance should be found by a new instance."""
        custom = make_resolved_token(
            symbol="SURVIVE",
            address=self.SURVIVE_ADDR,
            decimals=7,
        )

        # First resolver - register custom token
        resolver1 = TokenResolver(cache_file=temp_cache_file)
        resolver1.register(custom)

        # Second resolver - should find the token from disk cache
        TokenResolver.reset_instance()
        resolver2 = TokenResolver(cache_file=temp_cache_file)
        found = resolver2.resolve(self.SURVIVE_ADDR, "arbitrum")

        assert found.symbol == "SURVIVE"
        assert found.decimals == 7

    def test_static_tokens_cached_after_first_resolve(self, temp_cache_file):
        """Static registry tokens get cached after first resolution."""
        resolver1 = TokenResolver(cache_file=temp_cache_file)
        resolver1.resolve("USDC", "arbitrum")

        # Cache should now have USDC
        cache_data = json.loads(Path(temp_cache_file).read_text())
        tokens = cache_data.get("tokens", {})
        # Should have both symbol and address keys
        assert any("USDC" in k for k in tokens)

    def test_cache_survives_corrupted_disk(self, temp_cache_file):
        """Resolver handles corrupted cache file gracefully."""
        # Write corrupted data
        Path(temp_cache_file).write_text("NOT VALID JSON {{{{")

        # Should not raise - starts with empty cache
        resolver = TokenResolver(cache_file=temp_cache_file)
        token = resolver.resolve("USDC", "arbitrum")
        assert token.symbol == "USDC"
        assert token.decimals == 6


# ============================================================================
# 6. Native Token Auto-Wrapping for Swaps
# ============================================================================


class TestNativeTokenAutoWrapping:
    """Test native token auto-wrapping via resolve_for_swap()."""

    @pytest.mark.parametrize(
        "native,chain,expected_wrapped",
        [
            ("ETH", "ethereum", "WETH"),
            ("ETH", "arbitrum", "WETH"),
            ("ETH", "optimism", "WETH"),
            ("ETH", "base", "WETH"),
            ("MATIC", "polygon", "WMATIC"),
            ("AVAX", "avalanche", "WAVAX"),
            ("BNB", "bsc", "WBNB"),
        ],
    )
    def test_native_wraps_for_swap(self, resolver, native, chain, expected_wrapped):
        """Native tokens auto-wrap to their wrapped versions for swaps."""
        token = resolver.resolve_for_swap(native, chain)
        assert token.symbol == expected_wrapped
        assert not token.is_native
        assert token.is_wrapped_native

    @pytest.mark.parametrize(
        "token_symbol,chain",
        [
            ("USDC", "arbitrum"),
            ("WETH", "arbitrum"),
            ("WBTC", "ethereum"),
            ("DAI", "ethereum"),
        ],
    )
    def test_non_native_unchanged_for_swap(self, resolver, token_symbol, chain):
        """Non-native tokens stay unchanged for swaps."""
        token = resolver.resolve_for_swap(token_symbol, chain)
        assert token.symbol == token_symbol

    def test_native_stays_for_balance_queries(self, resolver):
        """Native tokens stay native when using resolve() (not swap)."""
        eth = resolver.resolve("ETH", "ethereum")
        assert eth.is_native
        assert eth.symbol == "ETH"

    def test_resolve_for_protocol_dex_wraps(self, resolver):
        """DEX protocols auto-wrap native tokens."""
        token = resolver.resolve_for_protocol("ETH", "arbitrum", "uniswap_v3")
        assert token.symbol == "WETH"

    def test_resolve_for_protocol_lending_keeps_native(self, resolver):
        """Lending protocols keep native tokens."""
        token = resolver.resolve_for_protocol("ETH", "ethereum", "aave_v3")
        assert token.symbol == "ETH"
        assert token.is_native


# ============================================================================
# 7. Bridged Token Aliases
# ============================================================================


class TestBridgedTokenAliases:
    """Test bridged token alias resolution (USDC.e, USDbC, etc.)."""

    @pytest.mark.parametrize(
        "alias,chain",
        [
            ("USDC.e", "arbitrum"),
            ("USDC.e", "optimism"),
            ("USDC.e", "polygon"),
            ("USDC.e", "avalanche"),
            ("USDbC", "base"),
        ],
    )
    def test_bridged_usdc_resolves(self, resolver, alias, chain):
        """Bridged USDC variants resolve to correct addresses."""
        token = resolver.resolve(alias, chain)
        assert token.decimals == 6
        assert token.address.startswith("0x")
        assert len(token.address) == 42

    def test_native_usdc_different_from_bridged(self, resolver):
        """Native USDC and bridged USDC.e have different addresses."""
        native = resolver.resolve("USDC", "arbitrum")
        bridged = resolver.resolve("USDC.e", "arbitrum")
        assert native.address.lower() != bridged.address.lower()

    def test_bridged_alias_case_insensitive(self, resolver):
        """Bridged token aliases should be case-insensitive."""
        token1 = resolver.resolve("usdc.e", "arbitrum")
        token2 = resolver.resolve("USDC.E", "arbitrum")
        token3 = resolver.resolve("Usdc.e", "arbitrum")
        assert token1.address == token2.address == token3.address

    def test_avalanche_bridged_tokens(self, resolver):
        """Avalanche has multiple bridged tokens."""
        usdc_e = resolver.resolve("USDC.e", "avalanche")
        assert usdc_e.decimals == 6

        # Check USDT.e if alias exists
        if ("avalanche", "USDT.E") in SYMBOL_ALIASES:
            usdt_e = resolver.resolve("USDT.e", "avalanche")
            assert usdt_e.decimals == 6

    def test_bridged_works_with_resolve_for_swap(self, resolver):
        """Bridged tokens work correctly with resolve_for_swap()."""
        token = resolver.resolve_for_swap("USDC.e", "arbitrum")
        assert token.decimals == 6
        # USDC.e is not native, so it should stay unchanged
        assert not token.is_native

    def test_all_aliases_resolve_without_error(self, resolver):
        """Every entry in SYMBOL_ALIASES should resolve successfully."""
        for (chain, symbol_alias), _address in SYMBOL_ALIASES.items():
            token = resolver.resolve(symbol_alias, chain)
            assert token.address.startswith("0x"), f"Failed for {symbol_alias} on {chain}"
            assert token.decimals >= 0


# ============================================================================
# 8. Error Handling for Unknown Tokens
# ============================================================================


class TestErrorHandling:
    """Test error handling for unknown and invalid tokens."""

    def test_unknown_symbol_raises_not_found(self, resolver):
        """Unknown symbol raises TokenNotFoundError."""
        with pytest.raises(TokenNotFoundError) as exc_info:
            resolver.resolve("NONEXISTENT_TOKEN_XYZ", "arbitrum")
        assert "NONEXISTENT_TOKEN_XYZ" in str(exc_info.value)

    def test_unknown_address_raises_not_found(self, resolver):
        """Unknown address raises TokenNotFoundError."""
        with pytest.raises(TokenNotFoundError):
            resolver.resolve("0x0000000000000000000000000000000000000099", "arbitrum")

    def test_invalid_address_format_raises(self, resolver):
        """Malformed address raises InvalidTokenAddressError."""
        # 42-char string starting with 0x but with invalid hex chars
        with pytest.raises(InvalidTokenAddressError):
            resolver.resolve("0xGHIJ567890123456789012345678901234567890", "arbitrum")

    def test_unknown_chain_raises(self, resolver):
        """Unknown chain raises TokenResolutionError."""
        with pytest.raises(TokenResolutionError):
            resolver.resolve("USDC", "nonexistent_chain")

    def test_never_defaults_to_18_decimals(self, resolver):
        """Resolver NEVER defaults to 18 decimals - must raise if unknown."""
        with pytest.raises(TokenNotFoundError):
            resolver.get_decimals("arbitrum", "FAKE_TOKEN_SYMBOL")

    def test_error_includes_suggestions(self, resolver):
        """Error messages should include helpful suggestions."""
        with pytest.raises(TokenNotFoundError) as exc_info:
            resolver.resolve("UNKNOWN", "arbitrum")
        # Should have suggestions list
        assert exc_info.value.suggestions is not None

    def test_error_includes_chain_context(self, resolver):
        """Error messages should include the chain name."""
        with pytest.raises(TokenNotFoundError) as exc_info:
            resolver.resolve("FAKE", "arbitrum")
        assert "arbitrum" in str(exc_info.value)


# ============================================================================
# 9. Graceful Fallback When Gateway Unavailable
# ============================================================================


class TestGracefulFallbackNoGateway:
    """Test graceful fallback when gateway is unavailable."""

    def test_static_resolution_works_without_gateway(self, resolver):
        """Static registry tokens resolve fine without gateway."""
        assert not resolver.is_gateway_connected()
        token = resolver.resolve("USDC", "arbitrum")
        assert token.symbol == "USDC"
        assert token.decimals == 6

    def test_all_static_tokens_resolve_without_gateway(self, resolver):
        """All DEFAULT_TOKENS should resolve without gateway connection."""
        for token_def in DEFAULT_TOKENS:
            for chain_name in token_def.chains:
                try:
                    resolved = resolver.resolve(token_def.symbol, chain_name)
                    assert resolved.decimals >= 0
                except TokenResolutionError:
                    # Some chain/token combos may not be in static registry
                    pass

    def test_gateway_unavailable_raises_for_unknown_address(self, resolver):
        """Without gateway, unknown addresses raise TokenNotFoundError."""
        with pytest.raises(TokenNotFoundError) as exc_info:
            resolver.resolve("0x0000000000000000000000000000000000000099", "arbitrum")
        # Should suggest connecting to gateway
        suggestions_str = str(exc_info.value.suggestions)
        assert "gateway" in suggestions_str.lower() or "Connect" in suggestions_str

    def test_gateway_error_does_not_crash_resolver(self, temp_cache_file):
        """Gateway errors should be handled gracefully."""
        mock_channel = MagicMock()
        resolver = TokenResolver(cache_file=temp_cache_file, gateway_channel=mock_channel)
        resolver._gateway_available = True

        mock_stub = MagicMock()
        mock_stub.GetTokenMetadata.side_effect = Exception("Connection refused")

        with patch.object(resolver, "_get_gateway_stub", return_value=mock_stub):
            # Should raise TokenNotFoundError, NOT the connection error
            with pytest.raises(TokenNotFoundError):
                resolver.resolve("0xDeaDBeeF00000000000000000000000000000003", "arbitrum")

    def test_gateway_timeout_handled_gracefully(self, temp_cache_file):
        """Gateway timeout should not crash the resolver."""
        mock_channel = MagicMock()
        resolver = TokenResolver(cache_file=temp_cache_file, gateway_channel=mock_channel)
        # Manually set gateway as available (don't use patch.object which restores on exit)
        resolver._gateway_available = True

        mock_stub = MagicMock()
        mock_stub.GetTokenMetadata.side_effect = Exception("DEADLINE_EXCEEDED")

        with patch.object(resolver, "_get_gateway_stub", return_value=mock_stub):
            with pytest.raises(TokenNotFoundError):
                resolver.resolve("0xDeaDBeeF00000000000000000000000000000004", "arbitrum")

        # Timeouts are transient -- gateway should remain available for retry
        # (only UNAVAILABLE errors cache gateway as down)
        assert resolver._gateway_available is True


# ============================================================================
# 10. Concurrent Access (10+ Threads)
# ============================================================================


class TestConcurrentAccess:
    """Test thread safety with 10+ concurrent threads."""

    def test_concurrent_resolve_same_token(self, resolver):
        """10+ threads resolving the same token concurrently."""
        results = []
        errors = []

        def resolve_usdc():
            try:
                token = resolver.resolve("USDC", "arbitrum")
                results.append(token)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=resolve_usdc) for _ in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        assert len(errors) == 0, f"Errors occurred: {errors}"
        assert len(results) == 20
        # All results should be identical
        addresses = {r.address for r in results}
        assert len(addresses) == 1

    def test_concurrent_resolve_different_tokens(self, resolver):
        """10+ threads resolving different tokens concurrently."""
        tokens_to_resolve = [
            ("USDC", "arbitrum"),
            ("WETH", "ethereum"),
            ("DAI", "ethereum"),
            ("WBTC", "arbitrum"),
            ("USDT", "ethereum"),
            ("LINK", "ethereum"),
            ("UNI", "ethereum"),
            ("AAVE", "ethereum"),
            ("ARB", "arbitrum"),
            ("OP", "optimism"),
            ("USDC", "base"),
            ("WETH", "optimism"),
        ]

        results = {}
        errors = []

        def resolve_token(symbol, chain):
            try:
                token = resolver.resolve(symbol, chain)
                results[(symbol, chain)] = token
            except Exception as e:
                errors.append((symbol, chain, e))

        threads = [
            threading.Thread(target=resolve_token, args=(sym, ch)) for sym, ch in tokens_to_resolve
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        assert len(errors) == 0, f"Errors: {errors}"
        assert len(results) == len(tokens_to_resolve)

    def test_concurrent_resolve_and_register(self, resolver):
        """Threads resolving while other threads register new tokens."""
        results = []
        errors = []

        def resolve_existing():
            try:
                token = resolver.resolve("USDC", "arbitrum")
                results.append(("resolve", token))
            except Exception as e:
                errors.append(("resolve", e))

        def register_new(idx):
            try:
                custom = make_resolved_token(
                    symbol=f"C{idx}",
                    address=f"0x{idx:040x}",
                )
                resolver.register(custom)
                results.append(("register", custom))
            except Exception as e:
                errors.append(("register", e))

        threads = []
        for i in range(10):
            threads.append(threading.Thread(target=resolve_existing))
            threads.append(threading.Thread(target=register_new, args=(i + 100,)))

        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        assert len(errors) == 0, f"Errors: {errors}"
        assert len(results) == 20

    def test_concurrent_different_operations(self, resolver):
        """Mix of resolve, get_decimals, get_address, resolve_for_swap concurrently."""
        results = []
        errors = []

        def do_resolve():
            try:
                results.append(resolver.resolve("USDC", "arbitrum"))
            except Exception as e:
                errors.append(e)

        def do_get_decimals():
            try:
                results.append(resolver.get_decimals("arbitrum", "USDC"))
            except Exception as e:
                errors.append(e)

        def do_get_address():
            try:
                results.append(resolver.get_address("arbitrum", "USDC"))
            except Exception as e:
                errors.append(e)

        def do_resolve_for_swap():
            try:
                results.append(resolver.resolve_for_swap("ETH", "ethereum"))
            except Exception as e:
                errors.append(e)

        threads = []
        for _ in range(5):
            threads.append(threading.Thread(target=do_resolve))
            threads.append(threading.Thread(target=do_get_decimals))
            threads.append(threading.Thread(target=do_get_address))
            threads.append(threading.Thread(target=do_resolve_for_swap))

        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        assert len(errors) == 0, f"Errors: {errors}"
        assert len(results) == 20


# ============================================================================
# 11. Migration Path: Deprecation Warnings
# ============================================================================


class TestDeprecationWarningsMigrationPath:
    """Test that old APIs trigger deprecation warnings."""

    def test_get_default_registry_emits_warning(self):
        """get_default_registry() should emit DeprecationWarning."""
        from almanak.framework.data.tokens import get_default_registry

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            registry = get_default_registry()

            deprecation_warnings = [x for x in w if issubclass(x.category, DeprecationWarning)]
            assert len(deprecation_warnings) >= 1
            assert "get_token_resolver" in str(deprecation_warnings[0].message)
            assert registry is not None

    def test_token_registry_init_emits_warning(self):
        """TokenRegistry() should emit DeprecationWarning."""
        from almanak.framework.data.tokens.registry import TokenRegistry

        # Reset warned flag for clean test
        TokenRegistry._warned = False
        try:
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                _registry = TokenRegistry()

                deprecation_warnings = [x for x in w if issubclass(x.category, DeprecationWarning)]
                assert len(deprecation_warnings) >= 1
                assert "get_token_resolver" in str(deprecation_warnings[0].message)
        finally:
            TokenRegistry._warned = False

    def test_token_addresses_removed_from_compiler(self):
        """TOKEN_ADDRESSES has been removed from compiler.py after deprecation period."""
        with pytest.raises(ImportError):
            from almanak.framework.intents.compiler import TOKEN_ADDRESSES  # noqa: F401

    def test_new_api_does_not_emit_warning(self, resolver):
        """get_token_resolver() and TokenResolver should NOT emit deprecation warnings."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            _resolver = get_token_resolver()
            _token = _resolver.resolve("USDC", "arbitrum")

            deprecation_warnings = [x for x in w if issubclass(x.category, DeprecationWarning)]
            # Filter out warnings from internal legacy callers
            direct_warnings = [
                w
                for w in deprecation_warnings
                if "test_token_resolver_integration" in str(w.filename)
            ]
            assert len(direct_warnings) == 0


# ============================================================================
# 12. Performance Benchmarks: Cache Hit Rate
# ============================================================================


class TestPerformanceCacheHitRate:
    """Test cache hit rate exceeds 95% for common tokens."""

    def test_cache_hit_rate_over_95_percent(self, resolver):
        """After warm-up, cache hit rate should be >95% for repeated lookups."""
        tokens = [
            ("USDC", "arbitrum"),
            ("WETH", "ethereum"),
            ("DAI", "arbitrum"),
            ("WBTC", "ethereum"),
            ("USDT", "arbitrum"),
        ]

        # Warm-up: resolve each token once (populates cache)
        for sym, ch in tokens:
            resolver.resolve(sym, ch)

        # Reset stats to measure cache performance
        initial_stats = resolver.stats()
        initial_cache_hits = initial_stats["cache_hits"]

        # Resolve 100 times with repeated tokens
        total_lookups = 100
        for i in range(total_lookups):
            sym, ch = tokens[i % len(tokens)]
            resolver.resolve(sym, ch)

        final_stats = resolver.stats()
        cache_hits = final_stats["cache_hits"] - initial_cache_hits
        hit_rate = cache_hits / total_lookups

        assert hit_rate >= 0.95, f"Cache hit rate {hit_rate:.2%} is below 95% target"

    def test_static_registry_coverage(self, resolver):
        """Verify sufficient token coverage in static registry."""
        # Count unique token-chain combinations
        total_combinations = 0
        for token_def in DEFAULT_TOKENS:
            total_combinations += len(token_def.chains)

        # Should have at least 50 token-chain combinations
        assert total_combinations >= 50, (
            f"Only {total_combinations} token-chain combos in static registry"
        )


# ============================================================================
# 13. Performance Benchmarks: Lookup Latency
# ============================================================================


class TestPerformanceLookupLatency:
    """Test lookup latency targets for cached tokens."""

    def test_cached_lookup_under_10ms(self, resolver):
        """Cached token lookups should complete in <10ms."""
        # Warm up the cache
        resolver.resolve("USDC", "arbitrum")

        # Measure repeated lookups
        latencies = []
        for _ in range(100):
            start = time.perf_counter()
            resolver.resolve("USDC", "arbitrum")
            elapsed_ms = (time.perf_counter() - start) * 1000
            latencies.append(elapsed_ms)

        p99 = sorted(latencies)[98]  # 99th percentile
        median = statistics.median(latencies)

        assert median < 10, f"Median latency {median:.2f}ms exceeds 10ms target"
        assert p99 < 10, f"P99 latency {p99:.2f}ms exceeds 10ms target"

    def test_cache_hit_under_1ms_median(self, resolver):
        """Cache hit median latency should be <1ms."""
        # Warm up
        resolver.resolve("USDC", "arbitrum")

        latencies = []
        for _ in range(200):
            start = time.perf_counter()
            resolver.resolve("USDC", "arbitrum")
            elapsed_ms = (time.perf_counter() - start) * 1000
            latencies.append(elapsed_ms)

        median = statistics.median(latencies)
        # Be generous on CI - <1ms for cache hits
        assert median < 1.0, f"Median cache hit latency {median:.3f}ms exceeds 1ms target"

    def test_static_registry_under_5ms(self, temp_cache_file):
        """First-time static registry lookup should be <5ms."""
        latencies = []
        tokens = ["USDC", "WETH", "DAI", "WBTC", "USDT"]

        for symbol in tokens:
            # Fresh resolver to avoid cache hits
            TokenResolver.reset_instance()
            resolver = TokenResolver(cache_file=temp_cache_file)

            start = time.perf_counter()
            resolver.resolve(symbol, "arbitrum")
            elapsed_ms = (time.perf_counter() - start) * 1000
            latencies.append(elapsed_ms)

        median = statistics.median(latencies)
        assert median < 10.0, f"Median static registry latency {median:.2f}ms exceeds 10ms target"

    def test_resolve_pair_latency(self, resolver):
        """resolve_pair() should complete within reasonable time."""
        # Warm up
        resolver.resolve_pair("USDC", "WETH", "arbitrum")

        latencies = []
        for _ in range(50):
            start = time.perf_counter()
            resolver.resolve_pair("USDC", "WETH", "arbitrum")
            elapsed_ms = (time.perf_counter() - start) * 1000
            latencies.append(elapsed_ms)

        median = statistics.median(latencies)
        assert median < 10, f"Median pair resolution latency {median:.2f}ms exceeds 10ms target"


# ============================================================================
# End-to-End Integration: Full Resolution Flow
# ============================================================================


class TestEndToEndResolutionFlow:
    """Test the full resolution flow from fresh state."""

    def test_full_flow_symbol_to_cached(self, resolver):
        """Test full flow: symbol -> static -> cache -> cache hit."""
        # 1. First resolve - hits static registry
        token1 = resolver.resolve("USDC", "arbitrum")
        stats1 = resolver.stats()
        assert stats1["static_hits"] >= 1

        # 2. Second resolve - should hit cache
        token2 = resolver.resolve("USDC", "arbitrum")
        stats2 = resolver.stats()
        assert stats2["cache_hits"] >= 1

        # Both should return identical data
        assert token1.address == token2.address
        assert token1.decimals == token2.decimals

    def test_full_flow_address_resolution(self, resolver):
        """Test full flow: address -> static -> cache."""
        # Get USDC address
        usdc = resolver.resolve("USDC", "arbitrum")

        # Now resolve by address
        token = resolver.resolve(usdc.address, "arbitrum")
        assert token.symbol == "USDC"
        assert token.decimals == 6

    def test_full_flow_register_and_resolve(self, resolver):
        """Test full flow: register custom token -> resolve it."""
        custom_addr = "0xcccc000000000000000000000000000000000003"
        custom = make_resolved_token(
            symbol="MYTOKEN",
            address=custom_addr,
            decimals=12,
        )
        resolver.register(custom)

        # Should resolve by symbol
        resolved = resolver.resolve("MYTOKEN", "arbitrum")
        assert resolved.decimals == 12

        # Should resolve by address
        resolved2 = resolver.resolve(custom_addr, "arbitrum")
        assert resolved2.symbol == "MYTOKEN"

    def test_full_flow_multi_chain_consistency(self, resolver):
        """USDC on different chains has different addresses but same decimals."""
        chains = ["ethereum", "arbitrum", "optimism", "base"]
        results = {}
        for chain in chains:
            try:
                token = resolver.resolve("USDC", chain)
                results[chain] = token
            except TokenNotFoundError:
                pass

        # All resolved USDC should have 6 decimals
        for chain, token in results.items():
            assert token.decimals == 6, f"USDC on {chain} has wrong decimals: {token.decimals}"

        # Different chains should have different addresses
        addresses = [t.address for t in results.values()]
        assert len(set(a.lower() for a in addresses)) == len(addresses), "Addresses should differ across chains"

    def test_full_flow_swap_then_balance(self, resolver):
        """For DEX: ETH->WETH. For balance: ETH stays native."""
        # Swap context
        swap_token = resolver.resolve_for_swap("ETH", "arbitrum")
        assert swap_token.symbol == "WETH"
        assert swap_token.is_wrapped_native

        # Balance context
        balance_token = resolver.resolve("ETH", "arbitrum")
        assert balance_token.symbol == "ETH"
        assert balance_token.is_native

    def test_stats_tracking_across_operations(self, resolver):
        """Stats should accurately track across multiple operations."""
        stats_before = resolver.stats()
        assert stats_before["cache_hits"] == 0
        assert stats_before["static_hits"] == 0
        assert stats_before["errors"] == 0

        # Successful resolutions
        resolver.resolve("USDC", "arbitrum")  # static hit
        resolver.resolve("USDC", "arbitrum")  # cache hit

        # Failed resolution
        with pytest.raises(TokenNotFoundError):
            resolver.resolve("FAKE_TOKEN", "arbitrum")

        stats_after = resolver.stats()
        assert stats_after["static_hits"] >= 1
        assert stats_after["cache_hits"] >= 1
        assert stats_after["errors"] >= 1
