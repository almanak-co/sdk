"""Tests for UniswapV3PoolPriceReader and pool price utilities.

Tests cover:
- sqrtPriceX96 decoding to human-readable prices
- slot0 / uint / address response decoding
- read_pool_price with mocked RPC
- Cache behavior (hit/miss, TTL expiry)
- resolve_pool_address (static registry, factory call, fallback)
- Token decimal resolution (TokenResolver, on-chain fallback, error)
- Error handling for invalid RPC responses
"""

from __future__ import annotations

import time
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.framework.data.exceptions import DataUnavailableError
from almanak.framework.data.models import DataClassification, DataEnvelope
from almanak.framework.data.pools.reader import (
    FEE_SELECTOR,
    LIQUIDITY_SELECTOR,
    SLOT0_SELECTOR,
    TOKEN0_SELECTOR,
    TOKEN1_SELECTOR,
    PoolPrice,
    UniswapV3PoolPriceReader,
    decode_address,
    decode_slot0,
    decode_sqrt_price_x96,
    decode_uint,
)

# ---------------------------------------------------------------------------
# Helpers to build mock RPC responses
# ---------------------------------------------------------------------------


def _uint256_bytes(value: int) -> bytes:
    """Encode an unsigned int as a 32-byte big-endian word."""
    return value.to_bytes(32, byteorder="big")


def _int256_bytes(value: int) -> bytes:
    """Encode a signed int as a 32-byte big-endian signed word."""
    return value.to_bytes(32, byteorder="big", signed=True)


def _address_bytes(address: str) -> bytes:
    """Encode an address as a 32-byte left-padded word."""
    addr_bytes = bytes.fromhex(address.replace("0x", ""))
    return b"\x00" * 12 + addr_bytes


def _build_slot0_response(sqrt_price_x96: int, tick: int) -> bytes:
    """Build a mock slot0() response with 7 words."""
    return (
        _uint256_bytes(sqrt_price_x96)
        + _int256_bytes(tick)
        + _uint256_bytes(0)  # observationIndex
        + _uint256_bytes(0)  # observationCardinality
        + _uint256_bytes(0)  # observationCardinalityNext
        + _uint256_bytes(0)  # feeProtocol
        + _uint256_bytes(1)  # unlocked (true)
    )


# ---------------------------------------------------------------------------
# Test decode_sqrt_price_x96
# ---------------------------------------------------------------------------


class TestDecodeSqrtPriceX96:
    """Tests for the sqrtPriceX96 -> price conversion."""

    def test_equal_decimals_price_1(self):
        """When sqrtPriceX96 = 2^96, price should be 1.0 for equal decimals."""
        price = decode_sqrt_price_x96(2**96, token0_decimals=18, token1_decimals=18)
        assert price == Decimal("1")

    def test_usdc_weth_realistic(self):
        """Decode a realistic USDC/WETH sqrtPriceX96 value.

        For USDC/WETH pool (token0=USDC 6 dec, token1=WETH 18 dec):
        sqrtPriceX96 = 1353984806969506445907301085090816 gives ~ price of
        token0 (USDC) in terms of token1 (WETH). Since USDC is ~$1 and WETH ~$1800,
        the price of USDC in WETH is approximately 1/1800 ≈ 0.000556.

        But with decimal adjustment (10^(6-18) = 10^-12), the raw price
        needs to be multiplied by 10^-12.
        """
        # sqrtPriceX96 for ETH ~$1800 in a USDC/WETH pool
        # sqrt(1800 * 10^12) * 2^96 ≈ 1.342e12 * 7.923e28 ≈ 1.06e41
        # Let's use a known value: sqrtPriceX96 that gives price ≈ 0.000556
        # price_raw = (sqrtPriceX96 / 2^96)^2
        # price = price_raw * 10^(6-18) = price_raw * 10^-12
        # We want price ≈ 0.000556 (USDC priced in WETH)
        # price_raw = 0.000556 / 10^-12 = 0.000556 * 10^12 = 556000000
        # sqrtPriceX96 = sqrt(556000000) * 2^96 = 23580.44 * 7.923e28 ≈ 1.868e33

        sqrt_price = int(Decimal("23580.44") * Decimal(2**96))
        price = decode_sqrt_price_x96(sqrt_price, token0_decimals=6, token1_decimals=18)
        # Should be approximately 0.000556
        assert Decimal("0.0005") < price < Decimal("0.0006")

    def test_zero_sqrt_price(self):
        """Zero sqrtPriceX96 should give zero price."""
        price = decode_sqrt_price_x96(0, 18, 6)
        assert price == Decimal("0")

    def test_different_decimals(self):
        """Test price calculation with different decimal configurations."""
        # sqrtPriceX96 = 2^96 -> raw_price = 1.0
        # With token0=8 dec, token1=18 dec: price = 1.0 * 10^(8-18) = 10^-10
        price = decode_sqrt_price_x96(2**96, token0_decimals=8, token1_decimals=18)
        assert price == Decimal("1e-10")


# ---------------------------------------------------------------------------
# Test decode_slot0
# ---------------------------------------------------------------------------


class TestDecodeSlot0:
    """Tests for slot0() response decoding."""

    def test_basic_decode(self):
        """Decode a valid slot0 response."""
        data = _build_slot0_response(sqrt_price_x96=12345, tick=-200)
        sqrt_price, tick = decode_slot0(data)
        assert sqrt_price == 12345
        assert tick == -200

    def test_positive_tick(self):
        """Decode slot0 with a positive tick."""
        data = _build_slot0_response(sqrt_price_x96=2**96, tick=100)
        sqrt_price, tick = decode_slot0(data)
        assert sqrt_price == 2**96
        assert tick == 100

    def test_large_negative_tick(self):
        """Decode slot0 with a large negative tick."""
        data = _build_slot0_response(sqrt_price_x96=1, tick=-887272)
        _, tick = decode_slot0(data)
        assert tick == -887272

    def test_too_short_raises(self):
        """Slot0 response < 64 bytes raises DataUnavailableError."""
        with pytest.raises(DataUnavailableError, match="too short"):
            decode_slot0(b"\x00" * 63)

    def test_empty_raises(self):
        """Empty response raises DataUnavailableError."""
        with pytest.raises(DataUnavailableError, match="too short"):
            decode_slot0(b"")


# ---------------------------------------------------------------------------
# Test decode_uint / decode_address
# ---------------------------------------------------------------------------


class TestDecodeHelpers:
    """Tests for decode_uint and decode_address."""

    def test_decode_uint_basic(self):
        assert decode_uint(_uint256_bytes(42)) == 42

    def test_decode_uint_zero(self):
        assert decode_uint(_uint256_bytes(0)) == 0

    def test_decode_uint_large(self):
        val = 2**128 - 1
        assert decode_uint(_uint256_bytes(val)) == val

    def test_decode_uint_too_short(self):
        with pytest.raises(DataUnavailableError, match="too short"):
            decode_uint(b"\x00" * 31)

    def test_decode_address_basic(self):
        addr = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
        result = decode_address(_address_bytes(addr))
        assert result.lower() == addr.lower()

    def test_decode_address_too_short(self):
        with pytest.raises(DataUnavailableError, match="too short"):
            decode_address(b"\x00" * 31)


# ---------------------------------------------------------------------------
# Test PoolPrice dataclass
# ---------------------------------------------------------------------------


class TestPoolPrice:
    """Tests for the PoolPrice frozen dataclass."""

    def test_construction(self):
        pp = PoolPrice(
            price=Decimal("1800.50"),
            tick=-200000,
            liquidity=10**18,
            fee_tier=3000,
            block_number=19000000,
            timestamp=datetime.now(UTC),
        )
        assert pp.price == Decimal("1800.50")
        assert pp.tick == -200000
        assert pp.fee_tier == 3000

    def test_frozen(self):
        pp = PoolPrice(
            price=Decimal("1"),
            tick=0,
            liquidity=0,
            fee_tier=500,
            block_number=1,
            timestamp=datetime.now(UTC),
        )
        with pytest.raises(AttributeError):
            pp.price = Decimal("2")  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Test UniswapV3PoolPriceReader.read_pool_price
# ---------------------------------------------------------------------------


class TestReadPoolPrice:
    """Tests for read_pool_price with mocked RPC."""

    POOL_ADDR = "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640"
    TOKEN0_ADDR = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"  # USDC
    TOKEN1_ADDR = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"  # WETH

    def _make_rpc_call(self, sqrt_price_x96: int = 2**96, tick: int = 0, liquidity: int = 10**18, fee: int = 3000):
        """Create a mock rpc_call that returns appropriate data per selector."""

        def rpc_call(chain: str, to: str, calldata: str) -> bytes:
            selector = calldata[:10] if len(calldata) >= 10 else calldata
            if selector == SLOT0_SELECTOR:
                return _build_slot0_response(sqrt_price_x96, tick)
            elif selector == LIQUIDITY_SELECTOR:
                return _uint256_bytes(liquidity)
            elif selector == TOKEN0_SELECTOR:
                return _address_bytes(self.TOKEN0_ADDR)
            elif selector == TOKEN1_SELECTOR:
                return _address_bytes(self.TOKEN1_ADDR)
            elif selector == FEE_SELECTOR:
                return _uint256_bytes(fee)
            elif selector == "0x313ce567":
                # decimals() — return based on which token
                if to.lower() == self.TOKEN0_ADDR.lower():
                    return _uint256_bytes(6)  # USDC
                else:
                    return _uint256_bytes(18)  # WETH
            return b"\x00" * 32

        return rpc_call

    def test_basic_read(self):
        """Read a pool price and verify envelope structure."""
        reader = UniswapV3PoolPriceReader(
            rpc_call=self._make_rpc_call(),
            cache_ttl_seconds=0,  # disable cache for this test
        )
        envelope = reader.read_pool_price(self.POOL_ADDR, "ethereum")

        assert isinstance(envelope, DataEnvelope)
        assert isinstance(envelope.value, PoolPrice)
        assert envelope.meta.source == "alchemy_rpc"
        assert envelope.meta.cache_hit is False
        assert envelope.classification == DataClassification.EXECUTION_GRADE
        assert envelope.meta.finality == "latest"
        assert envelope.value.fee_tier == 3000

    def test_price_delegation(self):
        """DataEnvelope transparent delegation works for PoolPrice fields."""
        reader = UniswapV3PoolPriceReader(
            rpc_call=self._make_rpc_call(),
            cache_ttl_seconds=0,
        )
        envelope = reader.read_pool_price(self.POOL_ADDR, "ethereum")

        # These should delegate to PoolPrice
        assert envelope.price == envelope.value.price
        assert envelope.tick == envelope.value.tick
        assert envelope.liquidity == envelope.value.liquidity

    def test_custom_source_name(self):
        """Custom source name appears in metadata."""
        reader = UniswapV3PoolPriceReader(
            rpc_call=self._make_rpc_call(),
            source_name="custom_rpc",
            cache_ttl_seconds=0,
        )
        envelope = reader.read_pool_price(self.POOL_ADDR, "ethereum")
        assert envelope.meta.source == "custom_rpc"

    def test_custom_finality(self):
        """Finality parameter is passed through to metadata."""
        reader = UniswapV3PoolPriceReader(
            rpc_call=self._make_rpc_call(),
            cache_ttl_seconds=0,
        )
        envelope = reader.read_pool_price(self.POOL_ADDR, "ethereum", finality="finalized")
        assert envelope.meta.finality == "finalized"

    def test_block_number_in_meta(self):
        """Block number appears in metadata when provided."""
        reader = UniswapV3PoolPriceReader(
            rpc_call=self._make_rpc_call(),
            cache_ttl_seconds=0,
        )
        envelope = reader.read_pool_price(self.POOL_ADDR, "ethereum", block_number=19000000)
        assert envelope.meta.block_number == 19000000
        assert envelope.value.block_number == 19000000

    def test_rpc_error_raises_data_unavailable(self):
        """RPC failure wraps as DataUnavailableError."""

        def failing_rpc(chain, to, calldata):
            raise ConnectionError("RPC timeout")

        reader = UniswapV3PoolPriceReader(rpc_call=failing_rpc, cache_ttl_seconds=0)
        with pytest.raises(DataUnavailableError, match="RPC call failed"):
            reader.read_pool_price(self.POOL_ADDR, "ethereum")

    def test_token_resolver_used_for_decimals(self):
        """When TokenResolver is provided, it's used for decimals."""
        mock_resolver = MagicMock()
        mock_resolved = MagicMock()
        mock_resolved.decimals = 6
        mock_resolver.resolve.return_value = mock_resolved

        rpc_call = self._make_rpc_call()

        reader = UniswapV3PoolPriceReader(
            rpc_call=rpc_call,
            token_resolver=mock_resolver,
            cache_ttl_seconds=0,
        )
        reader.read_pool_price(self.POOL_ADDR, "ethereum")
        # TokenResolver should have been called for each token
        assert mock_resolver.resolve.call_count == 2

    def test_token_resolver_fallback_to_onchain(self):
        """If TokenResolver fails, falls back to on-chain decimals() call."""
        mock_resolver = MagicMock()
        mock_resolver.resolve.side_effect = Exception("not found")

        # The fallback to on-chain should still work
        reader = UniswapV3PoolPriceReader(
            rpc_call=self._make_rpc_call(),
            token_resolver=mock_resolver,
            cache_ttl_seconds=0,
        )
        envelope = reader.read_pool_price(self.POOL_ADDR, "ethereum")
        assert isinstance(envelope.value, PoolPrice)


# ---------------------------------------------------------------------------
# Test cache behavior
# ---------------------------------------------------------------------------


class TestCacheBehavior:
    """Tests for the TTL-based price cache."""

    POOL_ADDR = "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640"
    TOKEN0_ADDR = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
    TOKEN1_ADDR = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"

    def _make_rpc_call(self):
        """Create a counting mock RPC call."""
        call_count = {"n": 0}

        def rpc_call(chain, to, calldata):
            selector = calldata[:10] if len(calldata) >= 10 else calldata
            call_count["n"] += 1
            if selector == SLOT0_SELECTOR:
                return _build_slot0_response(2**96, 0)
            elif selector == LIQUIDITY_SELECTOR:
                return _uint256_bytes(10**18)
            elif selector == TOKEN0_SELECTOR:
                return _address_bytes(self.TOKEN0_ADDR)
            elif selector == TOKEN1_SELECTOR:
                return _address_bytes(self.TOKEN1_ADDR)
            elif selector == FEE_SELECTOR:
                return _uint256_bytes(3000)
            elif selector == "0x313ce567":
                if to.lower() == self.TOKEN0_ADDR.lower():
                    return _uint256_bytes(6)
                return _uint256_bytes(18)
            return b"\x00" * 32

        return rpc_call, call_count

    def test_cache_hit(self):
        """Second read within TTL returns cached result."""
        rpc_call, call_count = self._make_rpc_call()
        reader = UniswapV3PoolPriceReader(rpc_call=rpc_call, cache_ttl_seconds=60)

        env1 = reader.read_pool_price(self.POOL_ADDR, "ethereum")
        count_after_first = call_count["n"]

        env2 = reader.read_pool_price(self.POOL_ADDR, "ethereum")
        # No additional RPC calls should have been made
        assert call_count["n"] == count_after_first
        assert env2.meta.cache_hit is True
        assert env1.meta.cache_hit is False

    def test_cache_miss_after_ttl(self, monkeypatch):
        """After TTL expires, a fresh RPC call is made."""
        rpc_call, call_count = self._make_rpc_call()
        reader = UniswapV3PoolPriceReader(rpc_call=rpc_call, cache_ttl_seconds=1)

        reader.read_pool_price(self.POOL_ADDR, "ethereum")
        count_after_first = call_count["n"]

        # Advance monotonic time past TTL
        original_monotonic = time.monotonic
        offset = [0.0]

        def patched_monotonic():
            return original_monotonic() + offset[0]

        monkeypatch.setattr(time, "monotonic", patched_monotonic)
        offset[0] = 2.0  # 2 seconds past TTL of 1s

        env2 = reader.read_pool_price(self.POOL_ADDR, "ethereum")
        # Should have made new RPC calls
        assert call_count["n"] > count_after_first
        assert env2.meta.cache_hit is False

    def test_different_pools_cached_separately(self):
        """Different pool addresses have separate cache entries."""
        rpc_call, call_count = self._make_rpc_call()
        reader = UniswapV3PoolPriceReader(rpc_call=rpc_call, cache_ttl_seconds=60)

        pool_a = "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640"
        pool_b = "0x8ad599c3A0ff1De082011EFDDc58f1908eb6e6D8"

        reader.read_pool_price(pool_a, "ethereum")
        count_after_a = call_count["n"]

        reader.read_pool_price(pool_b, "ethereum")
        # Should have made new calls for pool_b
        assert call_count["n"] > count_after_a

    def test_clear_cache(self):
        """clear_cache() forces fresh reads."""
        rpc_call, call_count = self._make_rpc_call()
        reader = UniswapV3PoolPriceReader(rpc_call=rpc_call, cache_ttl_seconds=60)

        reader.read_pool_price(self.POOL_ADDR, "ethereum")
        count_after_first = call_count["n"]

        reader.clear_cache()
        reader.read_pool_price(self.POOL_ADDR, "ethereum")
        assert call_count["n"] > count_after_first

    def test_cache_disabled_with_zero_ttl(self):
        """TTL=0 means every read goes to RPC."""
        rpc_call, call_count = self._make_rpc_call()
        reader = UniswapV3PoolPriceReader(rpc_call=rpc_call, cache_ttl_seconds=0)

        reader.read_pool_price(self.POOL_ADDR, "ethereum")
        count_after_first = call_count["n"]

        reader.read_pool_price(self.POOL_ADDR, "ethereum")
        assert call_count["n"] > count_after_first


# ---------------------------------------------------------------------------
# Test resolve_pool_address
# ---------------------------------------------------------------------------


class TestResolvePoolAddress:
    """Tests for resolve_pool_address (static registry and factory)."""

    def _noop_rpc(self, chain, to, calldata):
        return b"\x00" * 32

    def test_known_pool_from_static_registry(self):
        """Known pools are resolved from the static registry without RPC."""
        reader = UniswapV3PoolPriceReader(rpc_call=self._noop_rpc, cache_ttl_seconds=0)

        usdc = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
        weth = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"

        addr = reader.resolve_pool_address(usdc, weth, "ethereum", fee_tier=500)
        assert addr is not None
        assert addr == "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640"

    def test_known_pool_reverse_order(self):
        """Token order doesn't matter — addresses are sorted internally."""
        reader = UniswapV3PoolPriceReader(rpc_call=self._noop_rpc, cache_ttl_seconds=0)

        usdc = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
        weth = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"

        addr = reader.resolve_pool_address(weth, usdc, "ethereum", fee_tier=500)
        assert addr is not None
        assert addr == "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640"

    def test_factory_fallback(self):
        """When pool not in registry, factory getPool() is called."""
        expected_pool = "0x1234567890abcdef1234567890abcdef12345678"

        def factory_rpc(chain, to, calldata):
            # Return a non-zero pool address from factory
            return _address_bytes(expected_pool)

        reader = UniswapV3PoolPriceReader(rpc_call=factory_rpc, cache_ttl_seconds=0)
        # Use addresses not in the static registry
        addr = reader.resolve_pool_address(
            "0x1111111111111111111111111111111111111111",
            "0x2222222222222222222222222222222222222222",
            "ethereum",
            fee_tier=500,
        )
        assert addr is not None
        assert addr.lower() == expected_pool.lower()

    def test_factory_returns_zero_address(self):
        """Factory returning zero address means pool doesn't exist."""
        reader = UniswapV3PoolPriceReader(rpc_call=self._noop_rpc, cache_ttl_seconds=0)
        addr = reader.resolve_pool_address(
            "0x1111111111111111111111111111111111111111",
            "0x2222222222222222222222222222222222222222",
            "ethereum",
            fee_tier=500,
        )
        assert addr is None

    def test_unsupported_chain_factory(self):
        """Chain without factory returns None for unknown pools."""
        reader = UniswapV3PoolPriceReader(rpc_call=self._noop_rpc, cache_ttl_seconds=0)
        addr = reader.resolve_pool_address(
            "0x1111111111111111111111111111111111111111",
            "0x2222222222222222222222222222222222222222",
            "unsupported_chain",
            fee_tier=500,
        )
        assert addr is None

    def test_symbol_resolution_with_token_resolver(self):
        """Symbols are resolved to addresses via TokenResolver."""
        mock_resolver = MagicMock()

        def mock_resolve(token, chain):
            result = MagicMock()
            if token == "USDC":
                result.address = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
            elif token == "WETH":
                result.address = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
            return result

        mock_resolver.resolve = mock_resolve

        reader = UniswapV3PoolPriceReader(
            rpc_call=self._noop_rpc,
            token_resolver=mock_resolver,
            cache_ttl_seconds=0,
        )
        addr = reader.resolve_pool_address("USDC", "WETH", "ethereum", fee_tier=500)
        assert addr is not None

    def test_symbol_without_resolver_returns_none(self):
        """Symbols without a TokenResolver cannot be resolved."""
        reader = UniswapV3PoolPriceReader(rpc_call=self._noop_rpc, cache_ttl_seconds=0)
        addr = reader.resolve_pool_address("USDC", "WETH", "ethereum", fee_tier=500)
        assert addr is None


# ---------------------------------------------------------------------------
# Test edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    """Edge case and error handling tests."""

    def test_chain_case_insensitive(self):
        """Chain names are normalized to lowercase."""
        TOKEN0_ADDR = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
        TOKEN1_ADDR = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"

        def rpc_call(chain, to, calldata):
            selector = calldata[:10] if len(calldata) >= 10 else calldata
            if selector == SLOT0_SELECTOR:
                return _build_slot0_response(2**96, 0)
            elif selector == LIQUIDITY_SELECTOR:
                return _uint256_bytes(10**18)
            elif selector == TOKEN0_SELECTOR:
                return _address_bytes(TOKEN0_ADDR)
            elif selector == TOKEN1_SELECTOR:
                return _address_bytes(TOKEN1_ADDR)
            elif selector == FEE_SELECTOR:
                return _uint256_bytes(500)
            elif selector == "0x313ce567":
                return _uint256_bytes(18)
            return b"\x00" * 32

        reader = UniswapV3PoolPriceReader(rpc_call=rpc_call, cache_ttl_seconds=60)
        reader.read_pool_price("0xPool", "ETHEREUM")
        env2 = reader.read_pool_price("0xPool", "ethereum")
        # Should hit cache since chain is normalized
        assert env2.meta.cache_hit is True

    def test_latency_tracked(self):
        """Latency is tracked in metadata for non-cached reads."""
        TOKEN0_ADDR = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
        TOKEN1_ADDR = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"

        def rpc_call(chain, to, calldata):
            selector = calldata[:10] if len(calldata) >= 10 else calldata
            if selector == SLOT0_SELECTOR:
                return _build_slot0_response(2**96, 0)
            elif selector == LIQUIDITY_SELECTOR:
                return _uint256_bytes(10**18)
            elif selector == TOKEN0_SELECTOR:
                return _address_bytes(TOKEN0_ADDR)
            elif selector == TOKEN1_SELECTOR:
                return _address_bytes(TOKEN1_ADDR)
            elif selector == FEE_SELECTOR:
                return _uint256_bytes(500)
            elif selector == "0x313ce567":
                return _uint256_bytes(18)
            return b"\x00" * 32

        reader = UniswapV3PoolPriceReader(rpc_call=rpc_call, cache_ttl_seconds=0)
        env = reader.read_pool_price("0xPool", "ethereum")
        # Latency should be >= 0 (may be 0 for fast mock calls)
        assert env.meta.latency_ms >= 0

    def test_envelope_is_execution_grade(self):
        """Pool price envelopes are always EXECUTION_GRADE."""
        TOKEN0_ADDR = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
        TOKEN1_ADDR = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"

        def rpc_call(chain, to, calldata):
            selector = calldata[:10] if len(calldata) >= 10 else calldata
            if selector == SLOT0_SELECTOR:
                return _build_slot0_response(2**96, 0)
            elif selector == LIQUIDITY_SELECTOR:
                return _uint256_bytes(10**18)
            elif selector == TOKEN0_SELECTOR:
                return _address_bytes(TOKEN0_ADDR)
            elif selector == TOKEN1_SELECTOR:
                return _address_bytes(TOKEN1_ADDR)
            elif selector == FEE_SELECTOR:
                return _uint256_bytes(500)
            elif selector == "0x313ce567":
                return _uint256_bytes(18)
            return b"\x00" * 32

        reader = UniswapV3PoolPriceReader(rpc_call=rpc_call, cache_ttl_seconds=0)
        env = reader.read_pool_price("0xPool", "ethereum")
        assert env.is_execution_grade is True
        assert env.classification == DataClassification.EXECUTION_GRADE
