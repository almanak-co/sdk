"""Tests for AerodromePoolReader, PancakeSwapV3PoolReader, and PoolReaderRegistry.

Tests cover:
- AerodromePoolReader: slot0 reads, known pool resolution, factory fallback, cache
- PancakeSwapV3PoolReader: slot0 reads, known pool resolution, factory fallback, cache
- PoolReaderRegistry: dynamic dispatch, lazy instantiation, protocol listing,
  custom registration, error handling
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from almanak.framework.data.exceptions import DataUnavailableError
from almanak.framework.data.models import DataClassification, DataEnvelope
from almanak.framework.data.pools.reader import (
    _AERODROME_KNOWN_POOLS,
    _KNOWN_POOLS,
    _PANCAKESWAP_KNOWN_POOLS,
    AERODROME_CL_FACTORY,
    FEE_SELECTOR,
    LIQUIDITY_SELECTOR,
    PANCAKESWAP_V3_FACTORY,
    SLOT0_SELECTOR,
    TOKEN0_SELECTOR,
    TOKEN1_SELECTOR,
    UNISWAP_V3_FACTORY,
    AerodromePoolReader,
    PancakeSwapV3PoolReader,
    PoolPrice,
    PoolReaderRegistry,
    UniswapV3PoolPriceReader,
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
        + _uint256_bytes(0)
        + _uint256_bytes(0)
        + _uint256_bytes(0)
        + _uint256_bytes(0)
        + _uint256_bytes(1)
    )


# Common token addresses for tests
USDC_BASE = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
WETH_BASE = "0x4200000000000000000000000000000000000006"
USDC_ARB = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
WETH_ARB = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"
USDC_ETH = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
WETH_ETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"


def _make_rpc_call(
    token0_addr: str = USDC_BASE,
    token1_addr: str = WETH_BASE,
    sqrt_price_x96: int = 2**96,
    tick: int = 0,
    liquidity: int = 10**18,
    fee: int = 3000,
    token0_decimals: int = 6,
    token1_decimals: int = 18,
):
    """Create a mock rpc_call that returns appropriate data per selector."""

    def rpc_call(chain: str, to: str, calldata: str) -> bytes:
        selector = calldata[:10] if len(calldata) >= 10 else calldata
        if selector == SLOT0_SELECTOR:
            return _build_slot0_response(sqrt_price_x96, tick)
        elif selector == LIQUIDITY_SELECTOR:
            return _uint256_bytes(liquidity)
        elif selector == TOKEN0_SELECTOR:
            return _address_bytes(token0_addr)
        elif selector == TOKEN1_SELECTOR:
            return _address_bytes(token1_addr)
        elif selector == FEE_SELECTOR:
            return _uint256_bytes(fee)
        elif selector == "0x313ce567":
            if to.lower() == token0_addr.lower():
                return _uint256_bytes(token0_decimals)
            else:
                return _uint256_bytes(token1_decimals)
        return b"\x00" * 32

    return rpc_call


# ---------------------------------------------------------------------------
# Test AerodromePoolReader
# ---------------------------------------------------------------------------


class TestAerodromePoolReader:
    """Tests for the Aerodrome CL pool reader."""

    POOL_ADDR = "0xb2cc224c1c9feE385f8ad6a55b4d94E92359DC59"

    def test_inherits_from_uniswap(self):
        """AerodromePoolReader is a subclass of UniswapV3PoolPriceReader."""
        assert issubclass(AerodromePoolReader, UniswapV3PoolPriceReader)

    def test_protocol_name(self):
        """Protocol name is 'aerodrome'."""
        assert AerodromePoolReader.protocol_name == "aerodrome"

    def test_factory_addresses(self):
        """Uses Aerodrome CL factory addresses."""
        assert AerodromePoolReader._factory_addresses is AERODROME_CL_FACTORY
        assert "base" in AerodromePoolReader._factory_addresses

    def test_known_pools(self):
        """Uses Aerodrome known pools registry."""
        assert AerodromePoolReader._known_pools is _AERODROME_KNOWN_POOLS
        assert "base" in AerodromePoolReader._known_pools

    def test_read_pool_price(self):
        """Read a pool price from an Aerodrome CL pool."""
        rpc_call = _make_rpc_call(
            token0_addr=WETH_BASE,
            token1_addr=USDC_BASE,
            token0_decimals=18,
            token1_decimals=6,
            fee=100,
        )
        reader = AerodromePoolReader(rpc_call=rpc_call, cache_ttl_seconds=0)
        envelope = reader.read_pool_price(self.POOL_ADDR, "base")

        assert isinstance(envelope, DataEnvelope)
        assert isinstance(envelope.value, PoolPrice)
        assert envelope.meta.source == "alchemy_rpc"
        assert envelope.meta.cache_hit is False
        assert envelope.classification == DataClassification.EXECUTION_GRADE

    def test_cache_works(self):
        """Cache works the same as in UniswapV3PoolPriceReader."""
        rpc_call = _make_rpc_call(token0_addr=WETH_BASE, token1_addr=USDC_BASE)
        reader = AerodromePoolReader(rpc_call=rpc_call, cache_ttl_seconds=60)

        env1 = reader.read_pool_price(self.POOL_ADDR, "base")
        env2 = reader.read_pool_price(self.POOL_ADDR, "base")

        assert env1.meta.cache_hit is False
        assert env2.meta.cache_hit is True

    def test_resolve_known_pool_on_base(self):
        """Resolve a known Aerodrome pool from the static registry."""
        rpc_call = _make_rpc_call()
        reader = AerodromePoolReader(rpc_call=rpc_call, cache_ttl_seconds=0)

        addr = reader.resolve_pool_address(WETH_BASE, USDC_BASE, "base", fee_tier=100)
        assert addr is not None
        assert addr == "0xb2cc224c1c9feE385f8ad6a55b4d94E92359DC59"

    def test_resolve_unknown_pool_uses_factory(self):
        """Unknown pools fall back to factory getPool() call."""
        expected_pool = "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"

        def factory_rpc(chain, to, calldata):
            return _address_bytes(expected_pool)

        reader = AerodromePoolReader(rpc_call=factory_rpc, cache_ttl_seconds=0)
        addr = reader.resolve_pool_address(
            "0x1111111111111111111111111111111111111111",
            "0x2222222222222222222222222222222222222222",
            "base",
            fee_tier=500,
        )
        assert addr is not None
        assert addr.lower() == expected_pool.lower()

    def test_resolve_no_factory_for_chain(self):
        """Chain without factory returns None for unknown pools."""

        def noop(chain, to, calldata):
            return b"\x00" * 32

        reader = AerodromePoolReader(rpc_call=noop, cache_ttl_seconds=0)
        addr = reader.resolve_pool_address(
            "0x1111111111111111111111111111111111111111",
            "0x2222222222222222222222222222222222222222",
            "ethereum",  # Aerodrome only on Base
            fee_tier=500,
        )
        assert addr is None

    def test_rpc_error_raises_data_unavailable(self):
        """RPC failure wraps as DataUnavailableError."""

        def failing_rpc(chain, to, calldata):
            raise ConnectionError("RPC timeout")

        reader = AerodromePoolReader(rpc_call=failing_rpc, cache_ttl_seconds=0)
        with pytest.raises(DataUnavailableError, match="RPC call failed"):
            reader.read_pool_price(self.POOL_ADDR, "base")

    def test_custom_source_name(self):
        """Custom source name is passed through."""
        rpc_call = _make_rpc_call(token0_addr=WETH_BASE, token1_addr=USDC_BASE)
        reader = AerodromePoolReader(rpc_call=rpc_call, source_name="infura_rpc", cache_ttl_seconds=0)
        envelope = reader.read_pool_price(self.POOL_ADDR, "base")
        assert envelope.meta.source == "infura_rpc"


# ---------------------------------------------------------------------------
# Test PancakeSwapV3PoolReader
# ---------------------------------------------------------------------------


class TestPancakeSwapV3PoolReader:
    """Tests for the PancakeSwap V3 pool reader."""

    POOL_ADDR = "0xd9e2a1a61B6E61b275cEc326465d417e52C1a621"

    def test_inherits_from_uniswap(self):
        """PancakeSwapV3PoolReader is a subclass of UniswapV3PoolPriceReader."""
        assert issubclass(PancakeSwapV3PoolReader, UniswapV3PoolPriceReader)

    def test_protocol_name(self):
        """Protocol name is 'pancakeswap_v3'."""
        assert PancakeSwapV3PoolReader.protocol_name == "pancakeswap_v3"

    def test_factory_addresses(self):
        """Uses PancakeSwap V3 factory addresses."""
        assert PancakeSwapV3PoolReader._factory_addresses is PANCAKESWAP_V3_FACTORY
        assert "arbitrum" in PancakeSwapV3PoolReader._factory_addresses
        assert "ethereum" in PancakeSwapV3PoolReader._factory_addresses
        assert "base" in PancakeSwapV3PoolReader._factory_addresses
        assert "bsc" in PancakeSwapV3PoolReader._factory_addresses

    def test_known_pools(self):
        """Uses PancakeSwap known pools registry."""
        assert PancakeSwapV3PoolReader._known_pools is _PANCAKESWAP_KNOWN_POOLS
        assert "arbitrum" in PancakeSwapV3PoolReader._known_pools

    def test_read_pool_price(self):
        """Read a pool price from a PancakeSwap V3 pool."""
        rpc_call = _make_rpc_call(
            token0_addr=WETH_ARB,
            token1_addr=USDC_ARB,
            token0_decimals=18,
            token1_decimals=6,
            fee=500,
        )
        reader = PancakeSwapV3PoolReader(rpc_call=rpc_call, cache_ttl_seconds=0)
        envelope = reader.read_pool_price(self.POOL_ADDR, "arbitrum")

        assert isinstance(envelope, DataEnvelope)
        assert isinstance(envelope.value, PoolPrice)
        assert envelope.meta.source == "alchemy_rpc"
        assert envelope.meta.cache_hit is False
        assert envelope.classification == DataClassification.EXECUTION_GRADE

    def test_cache_works(self):
        """Cache works the same as in UniswapV3PoolPriceReader."""
        rpc_call = _make_rpc_call(token0_addr=WETH_ARB, token1_addr=USDC_ARB)
        reader = PancakeSwapV3PoolReader(rpc_call=rpc_call, cache_ttl_seconds=60)

        env1 = reader.read_pool_price(self.POOL_ADDR, "arbitrum")
        env2 = reader.read_pool_price(self.POOL_ADDR, "arbitrum")

        assert env1.meta.cache_hit is False
        assert env2.meta.cache_hit is True

    def test_resolve_known_pool_on_arbitrum(self):
        """Resolve a known PancakeSwap V3 pool from the static registry."""
        rpc_call = _make_rpc_call()
        reader = PancakeSwapV3PoolReader(rpc_call=rpc_call, cache_ttl_seconds=0)

        addr = reader.resolve_pool_address(WETH_ARB, USDC_ARB, "arbitrum", fee_tier=500)
        assert addr is not None
        assert addr == "0xd9e2a1a61B6E61b275cEc326465d417e52C1a621"

    def test_resolve_known_pool_on_ethereum(self):
        """Resolve a known PancakeSwap V3 pool on Ethereum."""
        rpc_call = _make_rpc_call()
        reader = PancakeSwapV3PoolReader(rpc_call=rpc_call, cache_ttl_seconds=0)

        addr = reader.resolve_pool_address(USDC_ETH, WETH_ETH, "ethereum", fee_tier=500)
        assert addr is not None
        assert addr == "0x6CA298D2983aB03Aa1da7679389D955A4eFEE15C"

    def test_resolve_unknown_pool_uses_factory(self):
        """Unknown pools fall back to factory getPool() call."""
        expected_pool = "0xabcdef1234567890abcdef1234567890abcdef12"

        def factory_rpc(chain, to, calldata):
            return _address_bytes(expected_pool)

        reader = PancakeSwapV3PoolReader(rpc_call=factory_rpc, cache_ttl_seconds=0)
        addr = reader.resolve_pool_address(
            "0x1111111111111111111111111111111111111111",
            "0x2222222222222222222222222222222222222222",
            "arbitrum",
            fee_tier=500,
        )
        assert addr is not None
        assert addr.lower() == expected_pool.lower()

    def test_rpc_error_raises_data_unavailable(self):
        """RPC failure wraps as DataUnavailableError."""

        def failing_rpc(chain, to, calldata):
            raise ConnectionError("RPC timeout")

        reader = PancakeSwapV3PoolReader(rpc_call=failing_rpc, cache_ttl_seconds=0)
        with pytest.raises(DataUnavailableError, match="RPC call failed"):
            reader.read_pool_price(self.POOL_ADDR, "arbitrum")

    def test_token_resolver_used_for_decimals(self):
        """When TokenResolver is provided, it's used for decimals."""
        mock_resolver = MagicMock()
        mock_resolved = MagicMock()
        mock_resolved.decimals = 18
        mock_resolver.resolve.return_value = mock_resolved

        rpc_call = _make_rpc_call(token0_addr=WETH_ARB, token1_addr=USDC_ARB)
        reader = PancakeSwapV3PoolReader(
            rpc_call=rpc_call,
            token_resolver=mock_resolver,
            cache_ttl_seconds=0,
        )
        reader.read_pool_price(self.POOL_ADDR, "arbitrum")
        assert mock_resolver.resolve.call_count == 2


# ---------------------------------------------------------------------------
# Test PoolReaderRegistry
# ---------------------------------------------------------------------------


class TestPoolReaderRegistry:
    """Tests for PoolReaderRegistry dynamic dispatch."""

    def _noop_rpc(self, chain, to, calldata):
        return b"\x00" * 32

    def test_get_reader_uniswap(self):
        """Get a UniswapV3PoolPriceReader from the registry."""
        registry = PoolReaderRegistry(rpc_call=self._noop_rpc)
        reader = registry.get_reader("ethereum", "uniswap_v3")
        assert isinstance(reader, UniswapV3PoolPriceReader)
        assert not isinstance(reader, AerodromePoolReader)
        assert not isinstance(reader, PancakeSwapV3PoolReader)

    def test_get_reader_aerodrome(self):
        """Get an AerodromePoolReader from the registry."""
        registry = PoolReaderRegistry(rpc_call=self._noop_rpc)
        reader = registry.get_reader("base", "aerodrome")
        assert isinstance(reader, AerodromePoolReader)

    def test_get_reader_pancakeswap(self):
        """Get a PancakeSwapV3PoolReader from the registry."""
        registry = PoolReaderRegistry(rpc_call=self._noop_rpc)
        reader = registry.get_reader("arbitrum", "pancakeswap_v3")
        assert isinstance(reader, PancakeSwapV3PoolReader)

    def test_case_insensitive(self):
        """Protocol names are case-insensitive."""
        registry = PoolReaderRegistry(rpc_call=self._noop_rpc)
        reader = registry.get_reader("base", "Aerodrome")
        assert isinstance(reader, AerodromePoolReader)

    def test_lazy_instantiation(self):
        """Readers are lazily created on first access."""
        registry = PoolReaderRegistry(rpc_call=self._noop_rpc)
        assert len(registry._readers) == 0

        registry.get_reader("base", "aerodrome")
        assert len(registry._readers) == 1

        registry.get_reader("base", "aerodrome")
        assert len(registry._readers) == 1  # same instance reused

    def test_same_instance_returned(self):
        """Same reader instance is returned for same protocol."""
        registry = PoolReaderRegistry(rpc_call=self._noop_rpc)
        r1 = registry.get_reader("base", "aerodrome")
        r2 = registry.get_reader("ethereum", "aerodrome")
        assert r1 is r2

    def test_unknown_protocol_raises(self):
        """Unknown protocol raises ValueError."""
        registry = PoolReaderRegistry(rpc_call=self._noop_rpc)
        with pytest.raises(ValueError, match="Unknown protocol 'unknown'"):
            registry.get_reader("ethereum", "unknown")

    def test_supported_protocols(self):
        """Lists all supported protocols."""
        registry = PoolReaderRegistry(rpc_call=self._noop_rpc)
        protos = registry.supported_protocols
        assert "aerodrome" in protos
        assert "pancakeswap_v3" in protos
        assert "uniswap_v3" in protos

    def test_protocols_for_chain_base(self):
        """Base chain supports all three protocols."""
        registry = PoolReaderRegistry(rpc_call=self._noop_rpc)
        protos = registry.protocols_for_chain("base")
        assert "aerodrome" in protos
        assert "pancakeswap_v3" in protos
        assert "uniswap_v3" in protos

    def test_protocols_for_chain_arbitrum(self):
        """Arbitrum chain supports Uniswap V3 and PancakeSwap V3."""
        registry = PoolReaderRegistry(rpc_call=self._noop_rpc)
        protos = registry.protocols_for_chain("arbitrum")
        assert "uniswap_v3" in protos
        assert "pancakeswap_v3" in protos
        assert "aerodrome" not in protos

    def test_protocols_for_chain_bsc(self):
        """BSC supports PancakeSwap V3 only."""
        registry = PoolReaderRegistry(rpc_call=self._noop_rpc)
        protos = registry.protocols_for_chain("bsc")
        assert "pancakeswap_v3" in protos
        assert "uniswap_v3" not in protos
        assert "aerodrome" not in protos

    def test_protocols_for_unknown_chain(self):
        """Unknown chain returns empty list."""
        registry = PoolReaderRegistry(rpc_call=self._noop_rpc)
        protos = registry.protocols_for_chain("unknown_chain")
        assert protos == []

    def test_register_custom_protocol(self):
        """Register a custom protocol reader."""

        class CustomReader(UniswapV3PoolPriceReader):
            protocol_name: str = "custom"
            _factory_addresses: dict[str, str] = {"ethereum": "0x0000000000000000000000000000000000000001"}
            _known_pools: dict[str, dict[tuple[str, str, int], str]] = {}

        registry = PoolReaderRegistry(rpc_call=self._noop_rpc)
        registry.register_protocol("custom", CustomReader)

        assert "custom" in registry.supported_protocols
        reader = registry.get_reader("ethereum", "custom")
        assert isinstance(reader, CustomReader)

    def test_register_overrides_existing(self):
        """Registering an existing protocol name overrides it."""

        class NewUniswap(UniswapV3PoolPriceReader):
            protocol_name: str = "uniswap_v3_new"
            _factory_addresses: dict[str, str] = {}
            _known_pools: dict[str, dict[tuple[str, str, int], str]] = {}

        registry = PoolReaderRegistry(rpc_call=self._noop_rpc)
        # Pre-warm the cache
        old = registry.get_reader("ethereum", "uniswap_v3")

        registry.register_protocol("uniswap_v3", NewUniswap)
        new = registry.get_reader("ethereum", "uniswap_v3")
        assert isinstance(new, NewUniswap)
        assert old is not new

    def test_constructor_params_forwarded(self):
        """Constructor params (token_resolver, TTL, source) are forwarded to readers."""
        mock_resolver = MagicMock()
        registry = PoolReaderRegistry(
            rpc_call=self._noop_rpc,
            token_resolver=mock_resolver,
            cache_ttl_seconds=5.0,
            source_name="custom_rpc",
        )
        reader = registry.get_reader("base", "aerodrome")
        assert reader._token_resolver is mock_resolver
        assert reader._cache_ttl == 5.0
        assert reader._source_name == "custom_rpc"

    def test_registry_read_pool_price_through_reader(self):
        """End-to-end: use registry to get a reader and read a pool price."""
        rpc_call = _make_rpc_call(
            token0_addr=WETH_BASE,
            token1_addr=USDC_BASE,
            token0_decimals=18,
            token1_decimals=6,
            fee=100,
        )
        registry = PoolReaderRegistry(rpc_call=rpc_call, cache_ttl_seconds=0)
        reader = registry.get_reader("base", "aerodrome")
        envelope = reader.read_pool_price("0xb2cc224c1c9feE385f8ad6a55b4d94E92359DC59", "base")

        assert isinstance(envelope, DataEnvelope)
        assert isinstance(envelope.value, PoolPrice)
        assert envelope.meta.cache_hit is False


# ---------------------------------------------------------------------------
# Test protocol-specific pool registries are distinct
# ---------------------------------------------------------------------------


class TestProtocolIsolation:
    """Verify each protocol reader uses its own pool registry."""

    def _noop_rpc(self, chain, to, calldata):
        return b"\x00" * 32

    def test_uniswap_uses_own_known_pools(self):
        """UniswapV3 reader uses _KNOWN_POOLS."""
        assert UniswapV3PoolPriceReader._known_pools is _KNOWN_POOLS

    def test_aerodrome_uses_own_known_pools(self):
        """Aerodrome reader uses _AERODROME_KNOWN_POOLS."""
        assert AerodromePoolReader._known_pools is _AERODROME_KNOWN_POOLS

    def test_pancakeswap_uses_own_known_pools(self):
        """PancakeSwap reader uses _PANCAKESWAP_KNOWN_POOLS."""
        assert PancakeSwapV3PoolReader._known_pools is _PANCAKESWAP_KNOWN_POOLS

    def test_uniswap_uses_own_factory(self):
        """UniswapV3 reader uses UNISWAP_V3_FACTORY."""
        assert UniswapV3PoolPriceReader._factory_addresses is UNISWAP_V3_FACTORY

    def test_aerodrome_uses_own_factory(self):
        """Aerodrome reader uses AERODROME_CL_FACTORY."""
        assert AerodromePoolReader._factory_addresses is AERODROME_CL_FACTORY

    def test_pancakeswap_uses_own_factory(self):
        """PancakeSwap reader uses PANCAKESWAP_V3_FACTORY."""
        assert PancakeSwapV3PoolReader._factory_addresses is PANCAKESWAP_V3_FACTORY

    def test_aerodrome_does_not_resolve_uniswap_pools(self):
        """Aerodrome reader doesn't find Uniswap pools in its registry."""
        reader = AerodromePoolReader(rpc_call=self._noop_rpc, cache_ttl_seconds=0)
        # This USDC/WETH pool is only in _KNOWN_POOLS (Uniswap), not _AERODROME_KNOWN_POOLS
        addr = reader.resolve_pool_address(USDC_ETH, WETH_ETH, "ethereum", fee_tier=500)
        assert addr is None

    def test_uniswap_does_not_resolve_aerodrome_pools(self):
        """Uniswap reader doesn't find Aerodrome pools in its registry."""
        reader = UniswapV3PoolPriceReader(rpc_call=self._noop_rpc, cache_ttl_seconds=0)
        # Aerodrome pool key
        addr = reader.resolve_pool_address(WETH_BASE, USDC_BASE, "base", fee_tier=100)
        # Not in Uniswap known pools
        assert addr is None
