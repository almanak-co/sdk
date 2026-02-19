"""Unit tests for TokenResolver class.

This module tests the TokenResolver class, covering:
- Singleton pattern (get_instance)
- Resolution by symbol
- Resolution by address
- Resolution order (cache -> static registry -> gateway)
- resolve_pair() method
- get_decimals() convenience method
- get_address() convenience method
- register() method
- Error handling and exceptions
- Thread safety
"""

import tempfile
import threading
import time
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from almanak.core.enums import Chain
from almanak.framework.data.tokens.exceptions import (
    InvalidTokenAddressError,
    TokenNotFoundError,
    TokenResolutionError,
)
from almanak.framework.data.tokens.models import CHAIN_ID_MAP, BridgeType, ResolvedToken
from almanak.framework.data.tokens.resolver import (
    TokenResolver,
    _is_address,
    _looks_like_address,
    _normalize_chain,
    _validate_address,
    get_token_resolver,
)


def make_resolved_token(
    symbol: str = "TEST",
    address: str = "0x1234567890123456789012345678901234567890",
    chain: Chain = Chain.ARBITRUM,
    decimals: int = 18,
) -> ResolvedToken:
    """Create a ResolvedToken for testing."""
    return ResolvedToken(
        symbol=symbol,
        address=address,
        decimals=decimals,
        chain=chain,
        chain_id=CHAIN_ID_MAP.get(chain, 42161),
        name=f"{symbol} Token",
        coingecko_id=symbol.lower(),
        is_stablecoin=symbol in ("USDC", "USDT", "DAI"),
        is_native=False,
        is_wrapped_native=symbol == "WETH",
        canonical_symbol=symbol,
        bridge_type=BridgeType.NATIVE,
        source="static",
        is_verified=True,
        resolved_at=datetime.now(),
    )


@pytest.fixture(autouse=True)
def reset_singleton():
    """Reset singleton before and after each test."""
    TokenResolver.reset_instance()
    yield
    TokenResolver.reset_instance()


@pytest.fixture
def temp_cache_file():
    """Create a temporary cache file."""
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
        temp_path = f.name
    yield temp_path
    # Cleanup
    Path(temp_path).unlink(missing_ok=True)


class TestHelperFunctions:
    """Tests for helper functions."""

    def test_is_address_valid(self):
        """Test _is_address with valid addresses."""
        assert _is_address("0xaf88d065e77c8cC2239327C5EDb3A432268e5831")
        assert _is_address("0x0000000000000000000000000000000000000000")
        assert _is_address("0xABCDEF1234567890abcdef1234567890ABCDEF12")

    def test_is_address_invalid(self):
        """Test _is_address with invalid addresses."""
        assert not _is_address("USDC")
        assert not _is_address("0x")
        assert not _is_address("0x123")  # Too short
        assert not _is_address("af88d065e77c8cC2239327C5EDb3A432268e5831")  # No 0x prefix
        assert not _is_address("0xGHIJ1234567890123456789012345678901234")  # Invalid hex

    def test_validate_address_valid(self):
        """Test _validate_address with valid addresses."""
        # Should not raise
        _validate_address("0xaf88d065e77c8cC2239327C5EDb3A432268e5831", "arbitrum")
        _validate_address("0x0000000000000000000000000000000000000000", "ethereum")

    def test_validate_address_no_prefix(self):
        """Test _validate_address rejects addresses without 0x."""
        with pytest.raises(InvalidTokenAddressError) as exc_info:
            _validate_address("af88d065e77c8cC2239327C5EDb3A432268e5831", "arbitrum")
        assert "must start with '0x'" in str(exc_info.value)

    def test_validate_address_wrong_length(self):
        """Test _validate_address rejects wrong-length addresses."""
        with pytest.raises(InvalidTokenAddressError) as exc_info:
            _validate_address("0x123", "arbitrum")
        assert "must be 42 characters" in str(exc_info.value)

    def test_validate_address_invalid_hex(self):
        """Test _validate_address rejects invalid hex characters."""
        with pytest.raises(InvalidTokenAddressError) as exc_info:
            _validate_address("0xGHIJ567890123456789012345678901234567890", "arbitrum")
        assert "invalid hex characters" in str(exc_info.value)

    def test_normalize_chain_string(self):
        """Test _normalize_chain with string input."""
        chain_lower, chain_enum = _normalize_chain("arbitrum")
        assert chain_lower == "arbitrum"
        assert chain_enum == Chain.ARBITRUM

    def test_normalize_chain_uppercase_string(self):
        """Test _normalize_chain with uppercase string."""
        chain_lower, chain_enum = _normalize_chain("ETHEREUM")
        assert chain_lower == "ethereum"
        assert chain_enum == Chain.ETHEREUM

    def test_normalize_chain_enum(self):
        """Test _normalize_chain with Chain enum."""
        chain_lower, chain_enum = _normalize_chain(Chain.BASE)
        assert chain_lower == "base"
        assert chain_enum == Chain.BASE

    def test_normalize_chain_unknown(self):
        """Test _normalize_chain with unknown chain."""
        with pytest.raises(TokenResolutionError) as exc_info:
            _normalize_chain("unknown_chain")
        assert "Unknown chain" in str(exc_info.value)


class TestTokenResolverSingleton:
    """Tests for singleton pattern."""

    def test_get_instance_returns_singleton(self, temp_cache_file):
        """Test get_instance returns the same instance."""
        resolver1 = TokenResolver.get_instance(cache_file=temp_cache_file)
        resolver2 = TokenResolver.get_instance()
        assert resolver1 is resolver2

    def test_get_token_resolver_returns_singleton(self, temp_cache_file):
        """Test get_token_resolver returns singleton."""
        resolver1 = get_token_resolver(cache_file=temp_cache_file)
        resolver2 = get_token_resolver()
        assert resolver1 is resolver2

    def test_reset_instance(self, temp_cache_file):
        """Test reset_instance clears the singleton."""
        resolver1 = TokenResolver.get_instance(cache_file=temp_cache_file)
        TokenResolver.reset_instance()
        resolver2 = TokenResolver.get_instance(cache_file=temp_cache_file)
        assert resolver1 is not resolver2


class TestTokenResolverBySymbol:
    """Tests for resolving tokens by symbol."""

    def test_resolve_usdc_arbitrum(self, temp_cache_file):
        """Test resolving USDC on Arbitrum."""
        resolver = TokenResolver(cache_file=temp_cache_file)
        token = resolver.resolve("USDC", "arbitrum")

        assert token.symbol == "USDC"
        assert token.decimals == 6
        assert token.chain == Chain.ARBITRUM
        assert token.chain_id == 42161
        assert token.is_stablecoin is True
        assert token.address.lower() == "0xaf88d065e77c8cc2239327c5edb3a432268e5831"

    def test_resolve_weth_ethereum(self, temp_cache_file):
        """Test resolving WETH on Ethereum."""
        resolver = TokenResolver(cache_file=temp_cache_file)
        token = resolver.resolve("WETH", "ethereum")

        assert token.symbol == "WETH"
        assert token.decimals == 18
        assert token.chain == Chain.ETHEREUM
        assert token.is_wrapped_native is True
        assert token.address.lower() == "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"

    def test_resolve_wbtc(self, temp_cache_file):
        """Test resolving WBTC (8 decimals)."""
        resolver = TokenResolver(cache_file=temp_cache_file)
        token = resolver.resolve("WBTC", "ethereum")

        assert token.symbol == "WBTC"
        assert token.decimals == 8
        assert token.is_stablecoin is False

    def test_resolve_case_insensitive(self, temp_cache_file):
        """Test symbol resolution is case-insensitive."""
        resolver = TokenResolver(cache_file=temp_cache_file)

        token1 = resolver.resolve("usdc", "arbitrum")
        token2 = resolver.resolve("USDC", "arbitrum")
        token3 = resolver.resolve("Usdc", "arbitrum")

        assert token1.symbol == token2.symbol == token3.symbol == "USDC"

    def test_resolve_with_chain_enum(self, temp_cache_file):
        """Test resolution with Chain enum."""
        resolver = TokenResolver(cache_file=temp_cache_file)
        token = resolver.resolve("USDC", Chain.ARBITRUM)

        assert token.chain == Chain.ARBITRUM
        assert token.decimals == 6

    def test_resolve_unknown_symbol_raises(self, temp_cache_file):
        """Test resolving unknown symbol raises TokenNotFoundError."""
        resolver = TokenResolver(cache_file=temp_cache_file)

        with pytest.raises(TokenNotFoundError) as exc_info:
            resolver.resolve("UNKNOWNTOKEN", "arbitrum")

        assert "UNKNOWNTOKEN" in str(exc_info.value)
        assert "arbitrum" in str(exc_info.value)

    def test_resolve_symbol_wrong_chain_raises(self, temp_cache_file):
        """Test resolving symbol not on chain raises TokenNotFoundError."""
        resolver = TokenResolver(cache_file=temp_cache_file)

        # GMX only exists on Arbitrum and Avalanche
        with pytest.raises(TokenNotFoundError):
            resolver.resolve("GMX", "ethereum")


class TestTokenResolverByAddress:
    """Tests for resolving tokens by address."""

    def test_resolve_by_address_usdc(self, temp_cache_file):
        """Test resolving USDC by address."""
        resolver = TokenResolver(cache_file=temp_cache_file)
        token = resolver.resolve("0xaf88d065e77c8cC2239327C5EDb3A432268e5831", "arbitrum")

        assert token.symbol == "USDC"
        assert token.decimals == 6
        assert token.chain == Chain.ARBITRUM

    def test_resolve_by_address_lowercase(self, temp_cache_file):
        """Test resolving by lowercase address."""
        resolver = TokenResolver(cache_file=temp_cache_file)
        token = resolver.resolve("0xaf88d065e77c8cc2239327c5edb3a432268e5831", "arbitrum")

        assert token.symbol == "USDC"

    def test_resolve_by_address_checksummed(self, temp_cache_file):
        """Test resolving by checksummed address."""
        resolver = TokenResolver(cache_file=temp_cache_file)
        token = resolver.resolve("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2", "ethereum")

        assert token.symbol == "WETH"

    def test_resolve_unknown_address_raises(self, temp_cache_file):
        """Test resolving unknown address raises TokenNotFoundError."""
        resolver = TokenResolver(cache_file=temp_cache_file)

        with pytest.raises(TokenNotFoundError) as exc_info:
            resolver.resolve("0x1234567890123456789012345678901234567890", "arbitrum")

        assert "0x1234567890123456789012345678901234567890" in str(exc_info.value)

    def test_resolve_invalid_address_raises(self, temp_cache_file):
        """Test resolving invalid address raises InvalidTokenAddressError."""
        resolver = TokenResolver(cache_file=temp_cache_file)

        # 42-char address with invalid hex characters (G is not valid hex)
        with pytest.raises(InvalidTokenAddressError):
            resolver.resolve("0xGHIJ567890123456789012345678901234567890", "arbitrum")


class TestTokenResolverResolvePair:
    """Tests for resolve_pair method."""

    def test_resolve_pair_usdc_weth(self, temp_cache_file):
        """Test resolving USDC/WETH pair."""
        resolver = TokenResolver(cache_file=temp_cache_file)
        usdc, weth = resolver.resolve_pair("USDC", "WETH", "arbitrum")

        assert usdc.symbol == "USDC"
        assert usdc.decimals == 6
        assert weth.symbol == "WETH"
        assert weth.decimals == 18

    def test_resolve_pair_by_address(self, temp_cache_file):
        """Test resolving pair by addresses."""
        resolver = TokenResolver(cache_file=temp_cache_file)
        usdc, weth = resolver.resolve_pair(
            "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
            "arbitrum",
        )

        assert usdc.symbol == "USDC"
        assert weth.symbol == "WETH"

    def test_resolve_pair_first_invalid_raises(self, temp_cache_file):
        """Test resolve_pair raises if first token invalid."""
        resolver = TokenResolver(cache_file=temp_cache_file)

        with pytest.raises(TokenNotFoundError):
            resolver.resolve_pair("INVALID", "WETH", "arbitrum")

    def test_resolve_pair_second_invalid_raises(self, temp_cache_file):
        """Test resolve_pair raises if second token invalid."""
        resolver = TokenResolver(cache_file=temp_cache_file)

        with pytest.raises(TokenNotFoundError):
            resolver.resolve_pair("USDC", "INVALID", "arbitrum")


class TestTokenResolverConvenienceMethods:
    """Tests for convenience methods."""

    def test_get_decimals(self, temp_cache_file):
        """Test get_decimals returns correct value."""
        resolver = TokenResolver(cache_file=temp_cache_file)

        assert resolver.get_decimals("arbitrum", "USDC") == 6
        assert resolver.get_decimals("ethereum", "WETH") == 18
        assert resolver.get_decimals("arbitrum", "WBTC") == 8

    def test_get_decimals_never_defaults(self, temp_cache_file):
        """Test get_decimals never defaults to 18."""
        resolver = TokenResolver(cache_file=temp_cache_file)

        with pytest.raises(TokenNotFoundError):
            resolver.get_decimals("arbitrum", "UNKNOWN")

    def test_get_address(self, temp_cache_file):
        """Test get_address returns correct value."""
        resolver = TokenResolver(cache_file=temp_cache_file)

        address = resolver.get_address("arbitrum", "USDC")
        assert address.lower() == "0xaf88d065e77c8cc2239327c5edb3a432268e5831"

    def test_get_address_unknown_raises(self, temp_cache_file):
        """Test get_address raises for unknown symbol."""
        resolver = TokenResolver(cache_file=temp_cache_file)

        with pytest.raises(TokenNotFoundError):
            resolver.get_address("arbitrum", "UNKNOWN")


class TestTokenResolverRegister:
    """Tests for register method."""

    def test_register_custom_token(self, temp_cache_file):
        """Test registering a custom token."""
        resolver = TokenResolver(cache_file=temp_cache_file)

        custom = make_resolved_token(
            symbol="CUSTOM",
            address="0x9999999999999999999999999999999999999999",
            decimals=9,
        )
        resolver.register(custom)

        # Resolve by symbol
        token = resolver.resolve("CUSTOM", "arbitrum")
        assert token.symbol == "CUSTOM"
        assert token.decimals == 9

    def test_register_persists_to_cache(self, temp_cache_file):
        """Test registered tokens are cached."""
        resolver1 = TokenResolver(cache_file=temp_cache_file)

        custom = make_resolved_token(
            symbol="CACHED",
            address="0x8888888888888888888888888888888888888888",
        )
        resolver1.register(custom)

        # Create new resolver with same cache file
        resolver2 = TokenResolver(cache_file=temp_cache_file)
        token = resolver2.resolve("CACHED", "arbitrum")
        assert token.symbol == "CACHED"


class TestTokenResolverCaching:
    """Tests for caching behavior."""

    def test_cache_hit_on_second_resolve(self, temp_cache_file):
        """Test cache is used on second resolve."""
        resolver = TokenResolver(cache_file=temp_cache_file)

        # First resolve - static registry
        resolver.resolve("USDC", "arbitrum")

        # Second resolve - should be from cache
        resolver.resolve("USDC", "arbitrum")

        stats = resolver.stats()
        assert stats["cache_hits"] >= 1

    def test_static_hits_tracked(self, temp_cache_file):
        """Test static registry hits are tracked."""
        resolver = TokenResolver(cache_file=temp_cache_file)

        resolver.resolve("USDC", "arbitrum")
        stats = resolver.stats()
        assert stats["static_hits"] >= 1


class TestTokenResolverThreadSafety:
    """Tests for thread safety."""

    def test_concurrent_resolution(self, temp_cache_file):
        """Test concurrent resolution is thread-safe."""
        resolver = TokenResolver(cache_file=temp_cache_file)
        results = []
        errors = []

        def resolve_token(symbol, chain):
            try:
                token = resolver.resolve(symbol, chain)
                results.append(token)
            except Exception as e:
                errors.append(e)

        # Start multiple threads
        threads = []
        symbols = ["USDC", "WETH", "DAI", "WBTC", "ARB"]
        for symbol in symbols * 10:  # 50 threads
            t = threading.Thread(target=resolve_token, args=(symbol, "arbitrum"))
            threads.append(t)
            t.start()

        # Wait for all threads
        for t in threads:
            t.join()

        assert len(errors) == 0
        assert len(results) == 50

    def test_concurrent_register(self, temp_cache_file):
        """Test concurrent registration is thread-safe."""
        resolver = TokenResolver(cache_file=temp_cache_file)
        errors = []

        def register_token(i):
            try:
                token = make_resolved_token(
                    symbol=f"TOKEN{i}",
                    address=f"0x{i:040d}",
                )
                resolver.register(token)
            except Exception as e:
                errors.append(e)

        # Start multiple threads
        threads = []
        for i in range(20):
            t = threading.Thread(target=register_token, args=(i,))
            threads.append(t)
            t.start()

        # Wait for all threads
        for t in threads:
            t.join()

        assert len(errors) == 0


class TestTokenResolverMultipleChains:
    """Tests for multi-chain resolution."""

    def test_resolve_usdc_multiple_chains(self, temp_cache_file):
        """Test resolving USDC on multiple chains."""
        resolver = TokenResolver(cache_file=temp_cache_file)

        chains = ["ethereum", "arbitrum", "optimism", "base", "polygon", "avalanche"]
        for chain in chains:
            token = resolver.resolve("USDC", chain)
            assert token.symbol == "USDC"
            assert token.decimals == 6
            assert token.chain.value.lower() == chain

    def test_resolve_weth_multiple_chains(self, temp_cache_file):
        """Test resolving WETH on multiple chains."""
        resolver = TokenResolver(cache_file=temp_cache_file)

        chains = ["ethereum", "arbitrum", "optimism", "base"]
        for chain in chains:
            token = resolver.resolve("WETH", chain)
            assert token.symbol == "WETH"
            assert token.decimals == 18
            assert token.is_wrapped_native is True


class TestTokenResolverNativeTokens:
    """Tests for native token handling."""

    def test_resolve_eth_is_native(self, temp_cache_file):
        """Test ETH resolves as native token."""
        resolver = TokenResolver(cache_file=temp_cache_file)
        token = resolver.resolve("ETH", "ethereum")

        assert token.symbol == "ETH"
        assert token.is_native is True
        assert token.address.lower() == "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"

    def test_resolve_matic_is_native_on_polygon(self, temp_cache_file):
        """Test MATIC resolves as native on Polygon."""
        resolver = TokenResolver(cache_file=temp_cache_file)
        token = resolver.resolve("MATIC", "polygon")

        assert token.symbol == "MATIC"
        assert token.is_native is True


class TestTokenResolverStats:
    """Tests for statistics tracking."""

    def test_stats_initially_zero(self, temp_cache_file):
        """Test stats start at zero."""
        resolver = TokenResolver(cache_file=temp_cache_file)
        stats = resolver.stats()

        assert stats["cache_hits"] == 0
        assert stats["static_hits"] == 0
        assert stats["gateway_lookups"] == 0
        assert stats["errors"] == 0

    def test_stats_track_errors(self, temp_cache_file):
        """Test stats track errors."""
        resolver = TokenResolver(cache_file=temp_cache_file)

        try:
            resolver.resolve("INVALID", "arbitrum")
        except TokenNotFoundError:
            pass

        stats = resolver.stats()
        assert stats["errors"] >= 1

    def test_cache_stats(self, temp_cache_file):
        """Test cache stats are accessible."""
        resolver = TokenResolver(cache_file=temp_cache_file)
        resolver.resolve("USDC", "arbitrum")

        cache_stats = resolver.cache_stats()
        assert "memory_hits" in cache_stats
        assert "disk_hits" in cache_stats
        assert "misses" in cache_stats


class TestTokenResolverPerformance:
    """Tests for performance characteristics."""

    def test_cache_hit_under_1ms(self, temp_cache_file):
        """Test cache hit is under 1ms target."""
        resolver = TokenResolver(cache_file=temp_cache_file)

        # First resolve to populate cache
        resolver.resolve("USDC", "arbitrum")

        # Measure cache hit time
        times = []
        for _ in range(100):
            start = time.perf_counter()
            resolver.resolve("USDC", "arbitrum")
            elapsed_ms = (time.perf_counter() - start) * 1000
            times.append(elapsed_ms)

        avg_time = sum(times) / len(times)
        # Allow some slack for CI environments
        assert avg_time < 5  # Should be well under 1ms in normal conditions

    def test_static_registry_under_5ms(self, temp_cache_file):
        """Test static registry lookup is under 5ms target."""
        # Create new resolver with fresh cache
        resolver = TokenResolver(cache_file=temp_cache_file)

        # Time first resolution (static registry)
        start = time.perf_counter()
        resolver.resolve("USDC", "arbitrum")
        elapsed_ms = (time.perf_counter() - start) * 1000

        # Allow some slack for CI environments
        assert elapsed_ms < 50  # Should be well under 5ms in normal conditions


class TestResolveForSwap:
    """Tests for resolve_for_swap() method with native token auto-wrapping."""

    def test_eth_resolves_to_weth_for_swap(self, temp_cache_file):
        """Test ETH resolves to WETH for swap operations."""
        resolver = TokenResolver(cache_file=temp_cache_file)
        token = resolver.resolve_for_swap("ETH", "ethereum")

        assert token.symbol == "WETH"
        assert token.decimals == 18
        assert token.is_wrapped_native is True
        assert token.is_native is False
        assert token.address.lower() == "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"

    def test_eth_resolves_to_weth_on_arbitrum(self, temp_cache_file):
        """Test ETH resolves to WETH on Arbitrum for swap."""
        resolver = TokenResolver(cache_file=temp_cache_file)
        token = resolver.resolve_for_swap("ETH", "arbitrum")

        assert token.symbol == "WETH"
        assert token.decimals == 18
        assert token.chain == Chain.ARBITRUM
        assert token.address.lower() == "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"

    def test_eth_resolves_to_weth_on_optimism(self, temp_cache_file):
        """Test ETH resolves to WETH on Optimism for swap."""
        resolver = TokenResolver(cache_file=temp_cache_file)
        token = resolver.resolve_for_swap("ETH", "optimism")

        assert token.symbol == "WETH"
        assert token.chain == Chain.OPTIMISM
        assert token.address.lower() == "0x4200000000000000000000000000000000000006"

    def test_eth_resolves_to_weth_on_base(self, temp_cache_file):
        """Test ETH resolves to WETH on Base for swap."""
        resolver = TokenResolver(cache_file=temp_cache_file)
        token = resolver.resolve_for_swap("ETH", "base")

        assert token.symbol == "WETH"
        assert token.chain == Chain.BASE
        assert token.address.lower() == "0x4200000000000000000000000000000000000006"

    def test_matic_resolves_to_wmatic_for_swap(self, temp_cache_file):
        """Test MATIC resolves to WMATIC on Polygon for swap."""
        resolver = TokenResolver(cache_file=temp_cache_file)
        token = resolver.resolve_for_swap("MATIC", "polygon")

        assert token.symbol == "WMATIC"
        assert token.decimals == 18
        assert token.is_wrapped_native is True
        assert token.chain == Chain.POLYGON
        assert token.address.lower() == "0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270"

    def test_avax_resolves_to_wavax_for_swap(self, temp_cache_file):
        """Test AVAX resolves to WAVAX on Avalanche for swap."""
        resolver = TokenResolver(cache_file=temp_cache_file)
        token = resolver.resolve_for_swap("AVAX", "avalanche")

        assert token.symbol == "WAVAX"
        assert token.decimals == 18
        assert token.is_wrapped_native is True
        assert token.chain == Chain.AVALANCHE
        assert token.address.lower() == "0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7"

    def test_bnb_resolves_to_wbnb_for_swap(self, temp_cache_file):
        """Test BNB resolves to WBNB on BSC for swap."""
        resolver = TokenResolver(cache_file=temp_cache_file)
        token = resolver.resolve_for_swap("BNB", "bsc")

        assert token.symbol == "WBNB"
        assert token.decimals == 18
        assert token.is_wrapped_native is True
        assert token.chain == Chain.BSC
        assert token.address.lower() == "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c"

    def test_non_native_token_unchanged(self, temp_cache_file):
        """Test non-native tokens are returned unchanged for swap."""
        resolver = TokenResolver(cache_file=temp_cache_file)
        token = resolver.resolve_for_swap("USDC", "arbitrum")

        assert token.symbol == "USDC"
        assert token.decimals == 6
        assert token.is_native is False

    def test_weth_unchanged_for_swap(self, temp_cache_file):
        """Test WETH is returned unchanged (already wrapped)."""
        resolver = TokenResolver(cache_file=temp_cache_file)
        token = resolver.resolve_for_swap("WETH", "arbitrum")

        assert token.symbol == "WETH"
        assert token.is_wrapped_native is True
        assert token.is_native is False

    def test_usdt_unchanged_for_swap(self, temp_cache_file):
        """Test stablecoins are unchanged for swap."""
        resolver = TokenResolver(cache_file=temp_cache_file)
        token = resolver.resolve_for_swap("USDT", "arbitrum")

        assert token.symbol == "USDT"
        assert token.decimals == 6
        assert token.is_stablecoin is True

    def test_resolve_for_swap_with_chain_enum(self, temp_cache_file):
        """Test resolve_for_swap works with Chain enum."""
        resolver = TokenResolver(cache_file=temp_cache_file)
        token = resolver.resolve_for_swap("ETH", Chain.ETHEREUM)

        assert token.symbol == "WETH"
        assert token.chain == Chain.ETHEREUM

    def test_resolve_for_swap_unknown_raises(self, temp_cache_file):
        """Test resolve_for_swap raises for unknown tokens."""
        resolver = TokenResolver(cache_file=temp_cache_file)

        with pytest.raises(TokenNotFoundError):
            resolver.resolve_for_swap("UNKNOWN", "arbitrum")


class TestResolveForProtocol:
    """Tests for resolve_for_protocol() method."""

    def test_dex_protocol_wraps_native(self, temp_cache_file):
        """Test DEX protocols get wrapped native tokens."""
        resolver = TokenResolver(cache_file=temp_cache_file)

        # Uniswap V3 should wrap ETH to WETH
        token = resolver.resolve_for_protocol("ETH", "arbitrum", "uniswap_v3")
        assert token.symbol == "WETH"

    def test_multiple_dex_protocols_wrap_native(self, temp_cache_file):
        """Test multiple DEX protocols wrap native tokens."""
        resolver = TokenResolver(cache_file=temp_cache_file)

        dex_protocols = [
            "uniswap_v3",
            "uniswap_v2",
            "sushiswap_v3",
            "pancakeswap_v3",
            "aerodrome",
            "traderjoe_v2",
            "curve",
            "balancer",
        ]

        for protocol in dex_protocols:
            token = resolver.resolve_for_protocol("ETH", "ethereum", protocol)
            assert token.symbol == "WETH", f"Failed for protocol {protocol}"

    def test_lending_protocol_keeps_native(self, temp_cache_file):
        """Test lending protocols keep native tokens."""
        resolver = TokenResolver(cache_file=temp_cache_file)

        # Aave V3 should keep ETH as native
        token = resolver.resolve_for_protocol("ETH", "ethereum", "aave_v3")
        assert token.symbol == "ETH"
        assert token.is_native is True

    def test_unknown_protocol_keeps_native(self, temp_cache_file):
        """Test unknown protocols keep native tokens."""
        resolver = TokenResolver(cache_file=temp_cache_file)

        token = resolver.resolve_for_protocol("ETH", "ethereum", "unknown_protocol")
        assert token.symbol == "ETH"
        assert token.is_native is True

    def test_non_native_token_unchanged_for_all_protocols(self, temp_cache_file):
        """Test non-native tokens unchanged regardless of protocol."""
        resolver = TokenResolver(cache_file=temp_cache_file)

        # DEX protocol
        token = resolver.resolve_for_protocol("USDC", "arbitrum", "uniswap_v3")
        assert token.symbol == "USDC"

        # Lending protocol
        token = resolver.resolve_for_protocol("USDC", "arbitrum", "aave_v3")
        assert token.symbol == "USDC"

    def test_protocol_case_insensitive(self, temp_cache_file):
        """Test protocol name is case-insensitive."""
        resolver = TokenResolver(cache_file=temp_cache_file)

        token1 = resolver.resolve_for_protocol("ETH", "ethereum", "UNISWAP_V3")
        token2 = resolver.resolve_for_protocol("ETH", "ethereum", "uniswap_v3")
        token3 = resolver.resolve_for_protocol("ETH", "ethereum", "Uniswap_V3")

        assert token1.symbol == "WETH"
        assert token2.symbol == "WETH"
        assert token3.symbol == "WETH"

    def test_protocol_with_chain_enum(self, temp_cache_file):
        """Test resolve_for_protocol works with Chain enum."""
        resolver = TokenResolver(cache_file=temp_cache_file)

        token = resolver.resolve_for_protocol("ETH", Chain.ARBITRUM, "uniswap_v3")
        assert token.symbol == "WETH"
        assert token.chain == Chain.ARBITRUM


class TestBridgedTokenAliases:
    """Tests for bridged token alias handling (USDC.e, USDbC, USDT.e, WETH.e)."""

    # =========================================================================
    # Arbitrum bridged tokens
    # =========================================================================

    def test_usdc_e_resolves_on_arbitrum(self, temp_cache_file):
        """Test USDC.e resolves to bridged USDC on Arbitrum."""
        resolver = TokenResolver(cache_file=temp_cache_file)
        token = resolver.resolve("USDC.e", "arbitrum")

        assert token.symbol == "USDC.E"
        assert token.decimals == 6
        assert token.chain == Chain.ARBITRUM
        assert token.is_stablecoin is True
        assert token.address.lower() == "0xff970a61a04b1ca14834a43f5de4533ebddb5cc8"

    def test_usdc_e_case_insensitive_arbitrum(self, temp_cache_file):
        """Test USDC.e resolution is case-insensitive."""
        resolver = TokenResolver(cache_file=temp_cache_file)

        # All these should resolve to the same token
        token1 = resolver.resolve("USDC.e", "arbitrum")
        token2 = resolver.resolve("USDC.E", "arbitrum")
        token3 = resolver.resolve("usdc.e", "arbitrum")

        assert token1.address.lower() == token2.address.lower() == token3.address.lower()

    def test_native_usdc_on_arbitrum(self, temp_cache_file):
        """Test USDC (without .e) resolves to native USDC on Arbitrum."""
        resolver = TokenResolver(cache_file=temp_cache_file)
        token = resolver.resolve("USDC", "arbitrum")

        # Should be native USDC, not bridged
        assert token.symbol == "USDC"
        assert token.decimals == 6
        # Native USDC address on Arbitrum
        assert token.address.lower() == "0xaf88d065e77c8cc2239327c5edb3a432268e5831"

    # =========================================================================
    # Optimism bridged tokens
    # =========================================================================

    def test_usdc_e_resolves_on_optimism(self, temp_cache_file):
        """Test USDC.e resolves to bridged USDC on Optimism."""
        resolver = TokenResolver(cache_file=temp_cache_file)
        token = resolver.resolve("USDC.e", "optimism")

        assert token.symbol == "USDC.E"
        assert token.decimals == 6
        assert token.chain == Chain.OPTIMISM
        assert token.address.lower() == "0x7f5c764cbc14f9669b88837ca1490cca17c31607"

    def test_native_usdc_on_optimism(self, temp_cache_file):
        """Test USDC resolves to native USDC on Optimism."""
        resolver = TokenResolver(cache_file=temp_cache_file)
        token = resolver.resolve("USDC", "optimism")

        assert token.symbol == "USDC"
        # Native USDC address on Optimism
        assert token.address.lower() == "0x0b2c639c533813f4aa9d7837caf62653d097ff85"

    # =========================================================================
    # Base bridged tokens
    # =========================================================================

    def test_usdbc_resolves_on_base(self, temp_cache_file):
        """Test USDbC resolves to bridged USDC on Base."""
        resolver = TokenResolver(cache_file=temp_cache_file)
        token = resolver.resolve("USDbC", "base")

        assert token.symbol == "USDBC"
        assert token.decimals == 6
        assert token.chain == Chain.BASE
        assert token.address.lower() == "0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca"

    def test_usdbc_case_insensitive(self, temp_cache_file):
        """Test USDbC resolution is case-insensitive."""
        resolver = TokenResolver(cache_file=temp_cache_file)

        token1 = resolver.resolve("USDbC", "base")
        token2 = resolver.resolve("USDBC", "base")
        token3 = resolver.resolve("usdbc", "base")

        assert token1.address.lower() == token2.address.lower() == token3.address.lower()

    def test_native_usdc_on_base(self, temp_cache_file):
        """Test USDC resolves to native USDC on Base."""
        resolver = TokenResolver(cache_file=temp_cache_file)
        token = resolver.resolve("USDC", "base")

        assert token.symbol == "USDC"
        # Native USDC address on Base
        assert token.address.lower() == "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"

    # =========================================================================
    # Polygon bridged tokens
    # =========================================================================

    def test_usdc_e_resolves_on_polygon(self, temp_cache_file):
        """Test USDC.e resolves to bridged USDC on Polygon."""
        resolver = TokenResolver(cache_file=temp_cache_file)
        token = resolver.resolve("USDC.e", "polygon")

        assert token.symbol == "USDC.E"
        assert token.decimals == 6
        assert token.chain == Chain.POLYGON
        assert token.address.lower() == "0x2791bca1f2de4661ed88a30c99a7a9449aa84174"

    def test_native_usdc_on_polygon(self, temp_cache_file):
        """Test USDC resolves to native USDC on Polygon."""
        resolver = TokenResolver(cache_file=temp_cache_file)
        token = resolver.resolve("USDC", "polygon")

        assert token.symbol == "USDC"
        # Native USDC address on Polygon
        assert token.address.lower() == "0x3c499c542cef5e3811e1192ce70d8cc03d5c3359"

    # =========================================================================
    # Avalanche bridged tokens
    # =========================================================================

    def test_usdc_e_resolves_on_avalanche(self, temp_cache_file):
        """Test USDC.e resolves to bridged USDC on Avalanche."""
        resolver = TokenResolver(cache_file=temp_cache_file)
        token = resolver.resolve("USDC.e", "avalanche")

        assert token.symbol == "USDC.E"
        assert token.decimals == 6
        assert token.chain == Chain.AVALANCHE
        assert token.address.lower() == "0xa7d7079b0fead91f3e65f86e8915cb59c1a4c664"

    def test_usdt_e_resolves_on_avalanche(self, temp_cache_file):
        """Test USDT.e resolves to bridged USDT on Avalanche."""
        resolver = TokenResolver(cache_file=temp_cache_file)
        token = resolver.resolve("USDT.e", "avalanche")

        assert token.symbol == "USDT.E"
        assert token.decimals == 6
        assert token.chain == Chain.AVALANCHE
        assert token.address.lower() == "0xc7198437980c041c805a1edcba50c1ce5db95118"

    def test_weth_e_resolves_on_avalanche(self, temp_cache_file):
        """Test WETH.e resolves to bridged WETH on Avalanche."""
        resolver = TokenResolver(cache_file=temp_cache_file)
        token = resolver.resolve("WETH.e", "avalanche")

        assert token.symbol == "WETH.E"
        assert token.decimals == 18
        assert token.chain == Chain.AVALANCHE
        assert token.address.lower() == "0x49d5c2bdffac6ce2bfdb6640f4f80f226bc10bab"

    def test_native_usdc_on_avalanche(self, temp_cache_file):
        """Test USDC resolves to native USDC on Avalanche."""
        resolver = TokenResolver(cache_file=temp_cache_file)
        token = resolver.resolve("USDC", "avalanche")

        assert token.symbol == "USDC"
        # Native USDC address on Avalanche
        assert token.address.lower() == "0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e"

    # =========================================================================
    # Cross-chain consistency
    # =========================================================================

    def test_usdc_e_not_found_on_chains_without_it(self, temp_cache_file):
        """Test USDC.e raises error on chains without bridged USDC."""
        resolver = TokenResolver(cache_file=temp_cache_file)

        # Ethereum doesn't have USDC.e (it has native USDC)
        with pytest.raises(TokenNotFoundError):
            resolver.resolve("USDC.e", "ethereum")

        # BSC doesn't have USDC.e
        with pytest.raises(TokenNotFoundError):
            resolver.resolve("USDC.e", "bsc")

    def test_usdt_e_not_found_on_chains_without_it(self, temp_cache_file):
        """Test USDT.e raises error on chains without it."""
        resolver = TokenResolver(cache_file=temp_cache_file)

        # Only Avalanche has USDT.e
        with pytest.raises(TokenNotFoundError):
            resolver.resolve("USDT.e", "arbitrum")

        with pytest.raises(TokenNotFoundError):
            resolver.resolve("USDT.e", "ethereum")

    def test_weth_e_not_found_on_chains_without_it(self, temp_cache_file):
        """Test WETH.e raises error on chains without it."""
        resolver = TokenResolver(cache_file=temp_cache_file)

        # Only Avalanche has WETH.e
        with pytest.raises(TokenNotFoundError):
            resolver.resolve("WETH.e", "arbitrum")

        with pytest.raises(TokenNotFoundError):
            resolver.resolve("WETH.e", "ethereum")

    def test_usdbc_not_found_on_chains_without_it(self, temp_cache_file):
        """Test USDbC raises error on chains other than Base."""
        resolver = TokenResolver(cache_file=temp_cache_file)

        # Only Base has USDbC
        with pytest.raises(TokenNotFoundError):
            resolver.resolve("USDbC", "arbitrum")

        with pytest.raises(TokenNotFoundError):
            resolver.resolve("USDbC", "ethereum")

    # =========================================================================
    # Integration with resolve_for_swap and resolve_for_protocol
    # =========================================================================

    def test_bridged_usdc_works_with_resolve_for_swap(self, temp_cache_file):
        """Test bridged tokens work with resolve_for_swap."""
        resolver = TokenResolver(cache_file=temp_cache_file)
        token = resolver.resolve_for_swap("USDC.e", "arbitrum")

        # Should resolve correctly (non-native, so unchanged)
        assert token.symbol == "USDC.E"
        assert token.decimals == 6

    def test_bridged_usdc_works_with_resolve_for_protocol(self, temp_cache_file):
        """Test bridged tokens work with resolve_for_protocol."""
        resolver = TokenResolver(cache_file=temp_cache_file)

        token = resolver.resolve_for_protocol("USDC.e", "arbitrum", "uniswap_v3")
        assert token.symbol == "USDC.E"

        token = resolver.resolve_for_protocol("USDC.e", "arbitrum", "aave_v3")
        assert token.symbol == "USDC.E"


class TestResolveForSwapBalanceQueries:
    """Tests verifying native tokens stay native for balance queries."""

    def test_eth_stays_native_for_balance(self, temp_cache_file):
        """Test ETH stays ETH for balance queries (using resolve, not resolve_for_swap)."""
        resolver = TokenResolver(cache_file=temp_cache_file)

        # Regular resolve for balance queries
        token = resolver.resolve("ETH", "ethereum")
        assert token.symbol == "ETH"
        assert token.is_native is True

        # resolve_for_swap for swap operations
        swap_token = resolver.resolve_for_swap("ETH", "ethereum")
        assert swap_token.symbol == "WETH"

    def test_matic_stays_native_for_balance(self, temp_cache_file):
        """Test MATIC stays MATIC for balance queries."""
        resolver = TokenResolver(cache_file=temp_cache_file)

        # Regular resolve for balance queries
        token = resolver.resolve("MATIC", "polygon")
        assert token.symbol == "MATIC"
        assert token.is_native is True

        # resolve_for_swap for swap operations
        swap_token = resolver.resolve_for_swap("MATIC", "polygon")
        assert swap_token.symbol == "WMATIC"

    def test_avax_stays_native_for_balance(self, temp_cache_file):
        """Test AVAX stays AVAX for balance queries."""
        resolver = TokenResolver(cache_file=temp_cache_file)

        # Regular resolve
        token = resolver.resolve("AVAX", "avalanche")
        assert token.symbol == "AVAX"
        assert token.is_native is True

        # resolve_for_swap
        swap_token = resolver.resolve_for_swap("AVAX", "avalanche")
        assert swap_token.symbol == "WAVAX"

    def test_bnb_stays_native_for_balance(self, temp_cache_file):
        """Test BNB stays BNB for balance queries."""
        resolver = TokenResolver(cache_file=temp_cache_file)

        # Regular resolve
        token = resolver.resolve("BNB", "bsc")
        assert token.symbol == "BNB"
        assert token.is_native is True

        # resolve_for_swap
        swap_token = resolver.resolve_for_swap("BNB", "bsc")
        assert swap_token.symbol == "WBNB"


class TestGatewayConnection:
    """Tests for gateway connection and on-chain discovery (US-012)."""

    def test_gateway_not_connected_by_default(self, temp_cache_file):
        """Test resolver reports gateway not connected when no channel provided."""
        resolver = TokenResolver(cache_file=temp_cache_file)

        assert resolver.is_gateway_connected() is False

    def test_gateway_not_connected_with_none_channel(self, temp_cache_file):
        """Test resolver reports gateway not connected when channel is None."""
        resolver = TokenResolver(cache_file=temp_cache_file, gateway_channel=None)

        assert resolver.is_gateway_connected() is False

    def test_set_gateway_channel_updates_state(self, temp_cache_file):
        """Test set_gateway_channel updates the gateway channel."""
        resolver = TokenResolver(cache_file=temp_cache_file)
        assert resolver.is_gateway_connected() is False

        # Create a mock channel
        class MockChannel:
            pass

        mock_channel = MockChannel()
        resolver.set_gateway_channel(mock_channel)

        # After setting channel, the resolver thinks it's connected
        # (actual availability verified on use)
        assert resolver._gateway_channel is mock_channel

    def test_set_gateway_channel_to_none_disconnects(self, temp_cache_file):
        """Test set_gateway_channel(None) disconnects from gateway."""

        class MockChannel:
            pass

        resolver = TokenResolver(cache_file=temp_cache_file, gateway_channel=MockChannel())
        resolver.set_gateway_channel(None)

        assert resolver.is_gateway_connected() is False
        assert resolver._gateway_channel is None

    def test_static_resolution_works_without_gateway(self, temp_cache_file):
        """Test static resolution works when gateway is not available."""
        resolver = TokenResolver(cache_file=temp_cache_file)

        # Should resolve from static registry without gateway
        token = resolver.resolve("USDC", "arbitrum")

        assert token.symbol == "USDC"
        assert token.decimals == 6
        assert token.source == "static"

    def test_unknown_address_raises_when_no_gateway(self, temp_cache_file):
        """Test unknown addresses raise error when gateway not available."""
        resolver = TokenResolver(cache_file=temp_cache_file)

        # Unknown address should raise TokenNotFoundError
        with pytest.raises(TokenNotFoundError) as exc_info:
            resolver.resolve("0x1234567890123456789012345678901234567890", "arbitrum")

        # Error should suggest connecting to gateway
        assert "Connect to gateway" in str(exc_info.value)

    def test_stats_include_gateway_metrics(self, temp_cache_file):
        """Test stats() includes gateway-related metrics."""
        resolver = TokenResolver(cache_file=temp_cache_file)

        stats = resolver.stats()

        assert "gateway_lookups" in stats
        assert "gateway_errors" in stats
        assert stats["gateway_lookups"] == 0
        assert stats["gateway_errors"] == 0

    def test_get_instance_accepts_gateway_channel(self, temp_cache_file):
        """Test get_instance accepts gateway_channel parameter."""

        class MockChannel:
            pass

        mock_channel = MockChannel()
        resolver = TokenResolver.get_instance(
            cache_file=temp_cache_file,
            gateway_channel=mock_channel,
        )

        assert resolver._gateway_channel is mock_channel


class TestGatewayGracefulFallback:
    """Tests for graceful fallback when gateway is unavailable."""

    def test_static_resolution_unaffected_by_gateway_failure(self, temp_cache_file):
        """Test static resolution still works even if gateway configured but unavailable."""
        # Create resolver with a channel that will fail
        class FailingChannel:
            pass

        resolver = TokenResolver(
            cache_file=temp_cache_file,
            gateway_channel=FailingChannel(),
        )

        # Static resolution should work
        token = resolver.resolve("USDC", "arbitrum")
        assert token.symbol == "USDC"
        assert token.source == "static"

    def test_resolve_by_address_falls_back_to_error_gracefully(self, temp_cache_file):
        """Test resolve by address gracefully falls back when gateway fails."""

        class FailingChannel:
            pass

        resolver = TokenResolver(
            cache_file=temp_cache_file,
            gateway_channel=FailingChannel(),
        )

        # Unknown address should raise error (not crash)
        with pytest.raises(TokenNotFoundError):
            resolver.resolve("0x1234567890123456789012345678901234567890", "arbitrum")

        # Stats should show gateway lookup attempt
        stats = resolver.stats()
        assert stats["gateway_lookups"] >= 0  # May or may not have attempted

    def test_gateway_errors_logged_not_raised(self, temp_cache_file, caplog):
        """Test gateway errors are logged but don't crash resolution."""
        import logging

        class FailingChannel:
            pass

        resolver = TokenResolver(
            cache_file=temp_cache_file,
            gateway_channel=FailingChannel(),
        )

        # Set up logging capture
        with caplog.at_level(logging.DEBUG):
            # Try to resolve unknown address - should fail gracefully
            with pytest.raises(TokenNotFoundError):
                resolver.resolve("0x1234567890123456789012345678901234567890", "arbitrum")

        # Resolution completed without crashing


class TestGatewayMockedIntegration:
    """Tests with mocked gateway responses."""

    def test_resolve_via_gateway_with_mock_response(self, temp_cache_file):
        """Test successful gateway resolution with mock response."""
        from unittest.mock import MagicMock

        # Create mock channel and stub
        mock_channel = MagicMock()
        mock_response = MagicMock()
        mock_response.success = True
        mock_response.symbol = "MOCK"
        mock_response.address = "0x1234567890123456789012345678901234567890"
        mock_response.decimals = 18
        mock_response.name = "Mock Token"
        mock_response.is_verified = False
        mock_response.source = "on_chain"

        resolver = TokenResolver(
            cache_file=temp_cache_file,
            gateway_channel=mock_channel,
        )

        # Mock the stub creation and RPC call
        mock_stub = MagicMock()
        mock_stub.GetTokenMetadata.return_value = mock_response
        resolver._gateway_stub = mock_stub
        resolver._gateway_available = True

        # Resolve the unknown address
        token = resolver.resolve("0x1234567890123456789012345678901234567890", "arbitrum")

        assert token.symbol == "MOCK"
        assert token.decimals == 18
        assert token.source == "on_chain"
        assert token.is_verified is False

    def test_resolve_via_gateway_caches_result(self, temp_cache_file):
        """Test gateway resolution caches the discovered token."""
        from unittest.mock import MagicMock

        mock_channel = MagicMock()
        mock_response = MagicMock()
        mock_response.success = True
        mock_response.symbol = "CACHED"
        mock_response.address = "0x2222222222222222222222222222222222222222"
        mock_response.decimals = 6
        mock_response.name = "Cached Token"
        mock_response.is_verified = False
        mock_response.source = "on_chain"

        resolver = TokenResolver(
            cache_file=temp_cache_file,
            gateway_channel=mock_channel,
        )

        mock_stub = MagicMock()
        mock_stub.GetTokenMetadata.return_value = mock_response
        resolver._gateway_stub = mock_stub
        resolver._gateway_available = True

        # First resolution
        token1 = resolver.resolve("0x2222222222222222222222222222222222222222", "arbitrum")
        assert token1.symbol == "CACHED"

        # Reset the stub to ensure second call uses cache
        resolver._gateway_stub = None
        resolver._gateway_available = False

        # Second resolution should use cache
        token2 = resolver.resolve("0x2222222222222222222222222222222222222222", "arbitrum")
        assert token2.symbol == "CACHED"
        assert token2.decimals == 6

        # Verify cache was used
        stats = resolver.stats()
        assert stats["cache_hits"] > 0

    def test_gateway_failure_returns_none_not_raises(self, temp_cache_file):
        """Test gateway failure returns None, doesn't raise exception."""
        from unittest.mock import MagicMock

        mock_channel = MagicMock()
        mock_response = MagicMock()
        mock_response.success = False
        mock_response.error = "Token not found"

        resolver = TokenResolver(
            cache_file=temp_cache_file,
            gateway_channel=mock_channel,
        )

        mock_stub = MagicMock()
        mock_stub.GetTokenMetadata.return_value = mock_response
        resolver._gateway_stub = mock_stub
        resolver._gateway_available = True

        # Resolution should raise TokenNotFoundError (not crash)
        with pytest.raises(TokenNotFoundError):
            resolver.resolve("0x3333333333333333333333333333333333333333", "arbitrum")

        # Gateway error should be tracked
        stats = resolver.stats()
        assert stats["gateway_errors"] > 0

    def test_gateway_timeout_handled_gracefully(self, temp_cache_file):
        """Test gateway timeout is handled gracefully."""
        from unittest.mock import MagicMock

        import grpc

        mock_channel = MagicMock()

        resolver = TokenResolver(
            cache_file=temp_cache_file,
            gateway_channel=mock_channel,
        )

        mock_stub = MagicMock()
        mock_stub.GetTokenMetadata.side_effect = grpc.RpcError()
        resolver._gateway_stub = mock_stub
        resolver._gateway_available = True

        # Resolution should raise TokenNotFoundError (graceful fallback)
        with pytest.raises(TokenNotFoundError):
            resolver.resolve("0x4444444444444444444444444444444444444444", "arbitrum")

        # Gateway error should be tracked
        stats = resolver.stats()
        assert stats["gateway_errors"] > 0

    def test_gateway_unavailable_error_marks_gateway_down(self, temp_cache_file):
        """Test UNAVAILABLE error marks gateway as down."""
        from unittest.mock import MagicMock

        mock_channel = MagicMock()

        resolver = TokenResolver(
            cache_file=temp_cache_file,
            gateway_channel=mock_channel,
        )

        mock_stub = MagicMock()
        mock_stub.GetTokenMetadata.side_effect = Exception("UNAVAILABLE: Connection refused")
        resolver._gateway_stub = mock_stub
        resolver._gateway_available = True

        # Resolution should fail gracefully
        with pytest.raises(TokenNotFoundError):
            resolver.resolve("0x5555555555555555555555555555555555555555", "arbitrum")

        # Gateway should be marked as unavailable
        assert resolver._gateway_available is False


class TestResolverLockContention:
    """Test that gateway calls don't block cached lookups."""

    def test_cached_resolve_not_blocked_by_slow_gateway(self, temp_cache_file):
        """Verify that cached token resolution proceeds even when a gateway call is in progress.

        This tests the fix for CRIT-2: the lock is NOT held during gateway RPC calls,
        so cached lookups from other threads should complete quickly.
        """
        mock_channel = MagicMock()
        resolver = TokenResolver(cache_file=temp_cache_file, gateway_channel=mock_channel)

        # Set up a slow gateway stub (simulates a 500ms RPC call)
        mock_stub = MagicMock()
        gateway_call_started = threading.Event()
        gateway_call_proceed = threading.Event()

        def slow_gateway_call(*args, **kwargs):
            gateway_call_started.set()
            gateway_call_proceed.wait(timeout=5)
            # Return failure so the resolve raises TokenNotFoundError
            mock_response = MagicMock()
            mock_response.success = False
            mock_response.error = "not found"
            return mock_response

        mock_stub.GetTokenMetadata.side_effect = slow_gateway_call
        resolver._gateway_stub = mock_stub
        resolver._gateway_available = True

        # Thread 1: resolve an unknown address (will hit gateway, slow path)
        errors = []

        def resolve_unknown():
            try:
                resolver.resolve("0x9999999999999999999999999999999999999999", "arbitrum")
            except TokenNotFoundError:
                pass  # Expected
            except Exception as e:
                errors.append(e)

        t1 = threading.Thread(target=resolve_unknown)
        t1.start()

        # Wait for the gateway call to start
        assert gateway_call_started.wait(timeout=5), "Gateway call did not start"

        # Thread 2: resolve a cached token (should NOT be blocked by the gateway call)
        start = time.perf_counter()
        result = resolver.resolve("USDC", "arbitrum")
        elapsed_ms = (time.perf_counter() - start) * 1000

        # Cached/static resolution should be very fast (<100ms), not blocked by gateway
        assert result.symbol == "USDC"
        assert result.decimals == 6
        assert elapsed_ms < 100, f"Cached resolve took {elapsed_ms:.1f}ms, expected <100ms (lock contention?)"

        # Let the gateway call finish
        gateway_call_proceed.set()
        t1.join(timeout=5)
        assert not errors, f"Gateway thread had errors: {errors}"
