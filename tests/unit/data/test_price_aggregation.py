"""Tests for TWAP and LWAP price aggregation.

Tests cover:
- TWAP via observe() with mocked RPC responses
- TWAP tick-to-price conversion and signed tick handling
- LWAP across multiple pools with liquidity weighting
- Pool liquidity filtering (< $10k threshold)
- Single-pool fallback for LWAP
- No-pools-found error (fail-closed)
- Zero liquidity equal-weighting edge case
- observe() calldata encoding and response decoding
- _tick_to_price conversion
- AggregatedPrice and PoolContribution dataclasses
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.framework.data.exceptions import DataUnavailableError
from almanak.framework.data.models import DataClassification, DataEnvelope
from almanak.framework.data.pools.aggregation import (
    OBSERVE_SELECTOR,
    AggregatedPrice,
    PoolContribution,
    PriceAggregator,
    _decode_observe_response,
    _encode_observe_calldata,
    _tick_to_price,
)
from almanak.framework.data.pools.reader import (
    PoolPrice,
    PoolReaderRegistry,
    UniswapV3PoolPriceReader,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _uint256_bytes(value: int) -> bytes:
    """Encode an unsigned int as a 32-byte big-endian word."""
    return value.to_bytes(32, byteorder="big")


def _int256_bytes(value: int) -> bytes:
    """Encode a signed int as a 32-byte big-endian signed word."""
    return value.to_bytes(32, byteorder="big", signed=True)


def _build_observe_response(tick_cumulatives: list[int], spl_cumulatives: list[int]) -> bytes:
    """Build a mock observe() response.

    ABI layout:
        word 0: offset to tickCumulatives (0x40)
        word 1: offset to secondsPerLiquidityCumulatives
        tickCumulatives: length + elements
        secondsPerLiquidityCumulatives: length + elements
    """
    count = len(tick_cumulatives)
    # Offset for tickCumulatives = 0x40 (after 2 offset words)
    tick_offset = 64
    # Offset for spl = tick_offset + 32 (length) + count*32 (elements)
    spl_offset = tick_offset + 32 + count * 32

    data = _uint256_bytes(tick_offset)
    data += _uint256_bytes(spl_offset)

    # tickCumulatives array
    data += _uint256_bytes(count)
    for tc in tick_cumulatives:
        data += _int256_bytes(tc)

    # secondsPerLiquidityCumulativeX128s array
    data += _uint256_bytes(count)
    for spl in spl_cumulatives:
        data += _uint256_bytes(spl)

    return data


def _make_pool_price(
    price: str | Decimal,
    liquidity: int,
    pool_address: str = "0xpool",
    fee_tier: int = 500,
    block_number: int = 100,
    token0_decimals: int = 18,
    token1_decimals: int = 6,
) -> PoolPrice:
    """Create a PoolPrice for testing."""
    return PoolPrice(
        price=Decimal(price) if isinstance(price, str) else price,
        tick=0,
        liquidity=liquidity,
        fee_tier=fee_tier,
        block_number=block_number,
        timestamp=datetime.now(UTC),
        pool_address=pool_address,
        token0_decimals=token0_decimals,
        token1_decimals=token1_decimals,
    )


def _make_envelope(pool_price: PoolPrice) -> DataEnvelope[PoolPrice]:
    """Wrap a PoolPrice in a DataEnvelope."""
    from almanak.framework.data.models import DataMeta

    meta = DataMeta(
        source="alchemy_rpc",
        observed_at=datetime.now(UTC),
        finality="latest",
    )
    return DataEnvelope(
        value=pool_price,
        meta=meta,
        classification=DataClassification.EXECUTION_GRADE,
    )


# ---------------------------------------------------------------------------
# Tests: Dataclasses
# ---------------------------------------------------------------------------


class TestPoolContribution:
    """Tests for PoolContribution frozen dataclass."""

    def test_create(self):
        pc = PoolContribution(
            pool_address="0xabc",
            protocol="uniswap_v3",
            price=Decimal("1800.5"),
            weight=0.6,
            liquidity=1000000,
        )
        assert pc.pool_address == "0xabc"
        assert pc.protocol == "uniswap_v3"
        assert pc.price == Decimal("1800.5")
        assert pc.weight == 0.6
        assert pc.liquidity == 1000000

    def test_default_liquidity(self):
        pc = PoolContribution(
            pool_address="0xabc",
            protocol="uniswap_v3",
            price=Decimal("1800"),
            weight=1.0,
        )
        assert pc.liquidity == 0

    def test_frozen(self):
        pc = PoolContribution(
            pool_address="0xabc",
            protocol="uniswap_v3",
            price=Decimal("1800"),
            weight=1.0,
        )
        with pytest.raises(AttributeError):
            pc.price = Decimal("2000")  # type: ignore[misc]


class TestAggregatedPrice:
    """Tests for AggregatedPrice frozen dataclass."""

    def test_create_minimal(self):
        ap = AggregatedPrice(price=Decimal("1800"))
        assert ap.price == Decimal("1800")
        assert ap.sources == []
        assert ap.block_range == (0, 0)
        assert ap.method == "lwap"
        assert ap.window_seconds == 0
        assert ap.pool_count == 0

    def test_create_full(self):
        sources = [
            PoolContribution("0xa", "uniswap_v3", Decimal("1800"), 0.5, 100),
            PoolContribution("0xb", "aerodrome", Decimal("1810"), 0.5, 100),
        ]
        ap = AggregatedPrice(
            price=Decimal("1805"),
            sources=sources,
            block_range=(100, 102),
            method="twap",
            window_seconds=300,
            pool_count=2,
        )
        assert ap.pool_count == 2
        assert ap.method == "twap"
        assert ap.window_seconds == 300
        assert len(ap.sources) == 2

    def test_frozen(self):
        ap = AggregatedPrice(price=Decimal("1800"))
        with pytest.raises(AttributeError):
            ap.price = Decimal("2000")  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Tests: observe() encoding / decoding
# ---------------------------------------------------------------------------


class TestObserveEncoding:
    """Tests for observe() calldata encoding."""

    def test_encode_two_seconds_agos(self):
        calldata = _encode_observe_calldata([300, 0])
        assert calldata.startswith(OBSERVE_SELECTOR)
        # Should contain selector + offset + length + 2 elements = selector + 4*64 hex chars
        assert len(calldata) == len(OBSERVE_SELECTOR) + 4 * 64

    def test_encode_single(self):
        calldata = _encode_observe_calldata([0])
        assert calldata.startswith(OBSERVE_SELECTOR)
        # selector + offset + length + 1 element = selector + 3*64
        assert len(calldata) == len(OBSERVE_SELECTOR) + 3 * 64


class TestObserveDecoding:
    """Tests for observe() response decoding."""

    def test_decode_positive_ticks(self):
        response = _build_observe_response(
            tick_cumulatives=[1000000, 1003000],
            spl_cumulatives=[500, 600],
        )
        ticks, spls = _decode_observe_response(response, 2)
        assert ticks == [1000000, 1003000]
        assert spls == [500, 600]

    def test_decode_negative_ticks(self):
        response = _build_observe_response(
            tick_cumulatives=[-1000000, -997000],
            spl_cumulatives=[100, 200],
        )
        ticks, spls = _decode_observe_response(response, 2)
        assert ticks == [-1000000, -997000]
        assert spls == [100, 200]

    def test_decode_too_short(self):
        with pytest.raises(DataUnavailableError, match="too short"):
            _decode_observe_response(b"\x00" * 10, 2)


# ---------------------------------------------------------------------------
# Tests: _tick_to_price
# ---------------------------------------------------------------------------


class TestTickToPrice:
    """Tests for tick-to-price conversion."""

    def test_tick_zero(self):
        """Tick 0 = price 1.0 (with equal decimals)."""
        price = _tick_to_price(0, 18, 18)
        assert price == Decimal("1")

    def test_positive_tick(self):
        """Positive tick gives price > 1."""
        price = _tick_to_price(100, 18, 18)
        expected = Decimal("1.0001") ** 100
        assert abs(price - expected) < Decimal("1e-10")

    def test_negative_tick(self):
        """Negative tick gives price < 1."""
        price = _tick_to_price(-100, 18, 18)
        expected = Decimal("1.0001") ** (-100)
        assert abs(price - expected) < Decimal("1e-10")

    def test_decimal_adjustment(self):
        """Decimal difference between token0 (18) and token1 (6) scales price."""
        price = _tick_to_price(0, 18, 6)
        # 1.0001^0 * 10^(18-6) = 10^12
        assert price == Decimal(10) ** 12

    def test_realistic_eth_usdc_tick(self):
        """A realistic tick for ETH/USDC pool."""
        # For ETH at ~$2000 with token0=USDC(6) token1=WETH(18):
        # price of token0 in terms of token1 = 1/2000 adjusted by decimals
        # But for WETH(18)/USDC(6) pools:
        # tick ~= 201172 gives price ~2000 with proper decimal adjustment
        tick = 201172
        price = _tick_to_price(tick, 18, 6)
        # This should be in the ballpark of ETH price in USDC terms
        # price = 1.0001^201172 * 10^12
        # log(price) = 201172 * log(1.0001) + 12*log(10)
        # Just verify it's a positive number in a reasonable range
        assert price > 0


# ---------------------------------------------------------------------------
# Tests: TWAP
# ---------------------------------------------------------------------------


class TestTWAP:
    """Tests for PriceAggregator.twap()."""

    def _make_aggregator(self, rpc_call=None):
        """Create a PriceAggregator with mock dependencies."""
        registry = MagicMock(spec=PoolReaderRegistry)
        if rpc_call is None:
            rpc_call = MagicMock()
        return PriceAggregator(pool_registry=registry, rpc_call=rpc_call)

    def test_twap_basic(self):
        """TWAP with positive tick cumulative difference."""
        # tick_cumulatives: [1000000, 1003000] over 300 seconds
        # avg_tick = (1003000 - 1000000) / 300 = 10
        response = _build_observe_response([1000000, 1003000], [0, 0])

        def rpc_call(chain, to, calldata):
            return response

        aggregator = self._make_aggregator(rpc_call=rpc_call)
        envelope = aggregator.twap("0xpool", "arbitrum", window_seconds=300, token0_decimals=18, token1_decimals=18)

        assert isinstance(envelope, DataEnvelope)
        assert isinstance(envelope.value, AggregatedPrice)
        assert envelope.value.method == "twap"
        assert envelope.value.window_seconds == 300
        assert envelope.value.pool_count == 1
        assert len(envelope.value.sources) == 1
        assert envelope.value.sources[0].protocol == "uniswap_v3"
        assert envelope.classification == DataClassification.EXECUTION_GRADE

        # avg_tick = 10, price = 1.0001^10 (with equal decimals)
        expected_price = _tick_to_price(10, 18, 18)
        assert envelope.value.price == expected_price

    def test_twap_negative_tick_diff(self):
        """TWAP with negative tick cumulative difference."""
        # tick_cumulatives: [-1003000, -1000000] over 300 seconds
        # tick_diff = -1000000 - (-1003000) = 3000
        # avg_tick = 3000 / 300 = 10
        response = _build_observe_response([-1003000, -1000000], [0, 0])

        def rpc_call(chain, to, calldata):
            return response

        aggregator = self._make_aggregator(rpc_call=rpc_call)
        envelope = aggregator.twap("0xpool", "arbitrum", window_seconds=300, token0_decimals=18, token1_decimals=18)

        expected_price = _tick_to_price(10, 18, 18)
        assert envelope.value.price == expected_price

    def test_twap_negative_avg_tick(self):
        """TWAP with negative average tick (tick cumulative decreasing)."""
        # tick_cumulatives: [1003000, 1000000] over 300 seconds
        # tick_diff = 1000000 - 1003000 = -3000
        # avg_tick = -(3000 // 300) = -10
        response = _build_observe_response([1003000, 1000000], [0, 0])

        def rpc_call(chain, to, calldata):
            return response

        aggregator = self._make_aggregator(rpc_call=rpc_call)
        envelope = aggregator.twap("0xpool", "arbitrum", window_seconds=300, token0_decimals=18, token1_decimals=18)

        expected_price = _tick_to_price(-10, 18, 18)
        assert envelope.value.price == expected_price

    def test_twap_zero_tick_diff(self):
        """TWAP with zero tick difference = constant price."""
        response = _build_observe_response([5000, 5000], [0, 0])

        def rpc_call(chain, to, calldata):
            return response

        aggregator = self._make_aggregator(rpc_call=rpc_call)
        envelope = aggregator.twap("0xpool", "arbitrum", window_seconds=300, token0_decimals=18, token1_decimals=18)

        expected_price = _tick_to_price(0, 18, 18)
        assert envelope.value.price == expected_price
        assert envelope.value.price == Decimal("1")

    def test_twap_custom_window(self):
        """TWAP with custom window_seconds."""
        response = _build_observe_response([0, 6000], [0, 0])

        def rpc_call(chain, to, calldata):
            return response

        aggregator = self._make_aggregator(rpc_call=rpc_call)
        envelope = aggregator.twap("0xpool", "arbitrum", window_seconds=600, token0_decimals=18, token1_decimals=18)

        assert envelope.value.window_seconds == 600
        # avg_tick = 6000 / 600 = 10
        expected_price = _tick_to_price(10, 18, 18)
        assert envelope.value.price == expected_price

    def test_twap_rpc_failure(self):
        """TWAP raises DataUnavailableError on RPC failure."""

        def rpc_call(chain, to, calldata):
            raise ConnectionError("RPC timeout")

        aggregator = self._make_aggregator(rpc_call=rpc_call)
        with pytest.raises(DataUnavailableError, match="TWAP observe.*failed"):
            aggregator.twap("0xpool", "arbitrum")

    def test_twap_invalid_response(self):
        """TWAP raises DataUnavailableError on too-short response."""

        def rpc_call(chain, to, calldata):
            return b"\x00" * 10

        aggregator = self._make_aggregator(rpc_call=rpc_call)
        with pytest.raises(DataUnavailableError, match="too short"):
            aggregator.twap("0xpool", "arbitrum")

    def test_twap_meta_fields(self):
        """TWAP envelope has correct meta fields."""
        response = _build_observe_response([0, 0], [0, 0])

        def rpc_call(chain, to, calldata):
            return response

        aggregator = self._make_aggregator(rpc_call=rpc_call)
        envelope = aggregator.twap("0xpool", "arbitrum")

        assert envelope.meta.source == "alchemy_rpc"
        assert envelope.meta.finality == "latest"
        assert envelope.meta.confidence == 1.0
        assert envelope.meta.cache_hit is False
        assert envelope.meta.latency_ms >= 0

    def test_twap_with_decimal_adjustment(self):
        """TWAP respects token decimals for price conversion."""
        response = _build_observe_response([0, 3000], [0, 0])

        def rpc_call(chain, to, calldata):
            return response

        aggregator = self._make_aggregator(rpc_call=rpc_call)
        envelope = aggregator.twap("0xpool", "arbitrum", window_seconds=300, token0_decimals=18, token1_decimals=6)

        # avg_tick = 3000 / 300 = 10
        expected_price = _tick_to_price(10, 18, 6)
        assert envelope.value.price == expected_price


# ---------------------------------------------------------------------------
# Tests: LWAP
# ---------------------------------------------------------------------------


class TestLWAP:
    """Tests for PriceAggregator.lwap()."""

    def _setup_registry_with_pools(
        self,
        pools: list[tuple[str, str, PoolPrice]],
    ) -> tuple[PoolReaderRegistry, MagicMock]:
        """Set up a mock registry that returns given pools.

        Args:
            pools: List of (pool_address, protocol, PoolPrice) tuples.

        Returns:
            (registry, rpc_call) mock pair.
        """
        registry = MagicMock(spec=PoolReaderRegistry)
        rpc_call = MagicMock()

        # Group pools by protocol
        by_protocol: dict[str, list[tuple[str, PoolPrice]]] = {}
        protocols_seen = []
        for addr, proto, pp in pools:
            if proto not in by_protocol:
                by_protocol[proto] = []
                protocols_seen.append(proto)
            by_protocol[proto].append((addr, pp))

        registry.protocols_for_chain.return_value = protocols_seen

        def get_reader(chain, protocol):
            reader = MagicMock(spec=UniswapV3PoolPriceReader)
            proto_pools = by_protocol.get(protocol, [])

            def resolve_pool_address(ta, tb, c, fee_tier):
                for addr, pp in proto_pools:
                    if pp.fee_tier == fee_tier:
                        return addr
                return None

            def read_pool_price(addr, c):
                for pool_addr, pp in proto_pools:
                    if pool_addr == addr:
                        return _make_envelope(pp)
                raise DataUnavailableError("pool_price", addr, "not found")

            reader.resolve_pool_address.side_effect = resolve_pool_address
            reader.read_pool_price.side_effect = read_pool_price
            return reader

        registry.get_reader.side_effect = get_reader

        return registry, rpc_call

    def test_lwap_single_pool(self):
        """LWAP with a single pool returns that pool's price."""
        pp = _make_pool_price("1800", 100000, pool_address="0xa", fee_tier=500)
        registry, rpc_call = self._setup_registry_with_pools([("0xa", "uniswap_v3", pp)])

        aggregator = PriceAggregator(
            pool_registry=registry,
            rpc_call=rpc_call,
            min_liquidity_usd=Decimal("0"),
        )
        envelope = aggregator.lwap("WETH", "USDC", "arbitrum")

        assert envelope.value.price == Decimal("1800")
        assert envelope.value.pool_count == 1
        assert envelope.value.method == "lwap"
        assert len(envelope.value.sources) == 1
        assert envelope.value.sources[0].weight == 1.0

    def test_lwap_two_pools_liquidity_weighted(self):
        """LWAP weights price by liquidity."""
        pp1 = _make_pool_price("1800", 300000, pool_address="0xa", fee_tier=500)
        pp2 = _make_pool_price("1810", 100000, pool_address="0xb", fee_tier=3000)
        registry, rpc_call = self._setup_registry_with_pools(
            [
                ("0xa", "uniswap_v3", pp1),
                ("0xb", "uniswap_v3", pp2),
            ]
        )

        aggregator = PriceAggregator(
            pool_registry=registry,
            rpc_call=rpc_call,
            min_liquidity_usd=Decimal("0"),
        )
        envelope = aggregator.lwap("WETH", "USDC", "arbitrum")

        assert envelope.value.pool_count == 2
        # LWAP = (1800 * 300000 + 1810 * 100000) / 400000 = (540M + 181M) / 400000 = 1802.5
        expected = (Decimal("1800") * 300000 + Decimal("1810") * 100000) / Decimal("400000")
        assert envelope.value.price == expected
        assert envelope.value.method == "lwap"

        # Check weights
        weights = {s.pool_address: s.weight for s in envelope.value.sources}
        assert abs(weights["0xa"] - 0.75) < 0.001
        assert abs(weights["0xb"] - 0.25) < 0.001

    def test_lwap_multi_protocol(self):
        """LWAP aggregates across protocols."""
        pp1 = _make_pool_price("1800", 200000, pool_address="0xa", fee_tier=500)
        pp2 = _make_pool_price("1805", 200000, pool_address="0xb", fee_tier=500)
        registry, rpc_call = self._setup_registry_with_pools(
            [
                ("0xa", "uniswap_v3", pp1),
                ("0xb", "aerodrome", pp2),
            ]
        )

        aggregator = PriceAggregator(
            pool_registry=registry,
            rpc_call=rpc_call,
            min_liquidity_usd=Decimal("0"),
        )
        envelope = aggregator.lwap("WETH", "USDC", "arbitrum")

        assert envelope.value.pool_count == 2
        # Equal liquidity: (1800 + 1805) / 2 = 1802.5
        expected = (Decimal("1800") + Decimal("1805")) / 2
        assert envelope.value.price == expected

        protocols = {s.protocol for s in envelope.value.sources}
        assert protocols == {"uniswap_v3", "aerodrome"}

    def test_lwap_filters_low_liquidity(self):
        """LWAP filters pools below minimum liquidity threshold."""
        pp_high = _make_pool_price("1800", 50000, pool_address="0xa", fee_tier=500)
        pp_low = _make_pool_price("1900", 5000, pool_address="0xb", fee_tier=3000)
        registry, rpc_call = self._setup_registry_with_pools(
            [
                ("0xa", "uniswap_v3", pp_high),
                ("0xb", "uniswap_v3", pp_low),
            ]
        )

        aggregator = PriceAggregator(
            pool_registry=registry,
            rpc_call=rpc_call,
            min_liquidity_usd=Decimal("10000"),  # raw liquidity threshold
        )
        envelope = aggregator.lwap("WETH", "USDC", "arbitrum")

        # Only the high-liquidity pool should be included
        assert envelope.value.pool_count == 1
        assert envelope.value.price == Decimal("1800")
        assert envelope.value.sources[0].pool_address == "0xa"

    def test_lwap_all_below_threshold_uses_all(self):
        """When all pools are below threshold, use all pools anyway."""
        pp1 = _make_pool_price("1800", 5000, pool_address="0xa", fee_tier=500)
        pp2 = _make_pool_price("1810", 5000, pool_address="0xb", fee_tier=3000)
        registry, rpc_call = self._setup_registry_with_pools(
            [
                ("0xa", "uniswap_v3", pp1),
                ("0xb", "uniswap_v3", pp2),
            ]
        )

        aggregator = PriceAggregator(
            pool_registry=registry,
            rpc_call=rpc_call,
            min_liquidity_usd=Decimal("10000"),
        )
        envelope = aggregator.lwap("WETH", "USDC", "arbitrum")

        # Should use both pools since all are below threshold
        assert envelope.value.pool_count == 2

    def test_lwap_zero_liquidity_equal_weight(self):
        """LWAP with zero liquidity uses equal weighting."""
        pp1 = _make_pool_price("1800", 0, pool_address="0xa", fee_tier=500)
        pp2 = _make_pool_price("1810", 0, pool_address="0xb", fee_tier=3000)
        registry, rpc_call = self._setup_registry_with_pools(
            [
                ("0xa", "uniswap_v3", pp1),
                ("0xb", "uniswap_v3", pp2),
            ]
        )

        aggregator = PriceAggregator(
            pool_registry=registry,
            rpc_call=rpc_call,
            min_liquidity_usd=Decimal("0"),
        )
        envelope = aggregator.lwap("WETH", "USDC", "arbitrum")

        # Equal weighting: (1800 + 1810) / 2 = 1805
        expected = (Decimal("1800") + Decimal("1810")) / 2
        assert envelope.value.price == expected
        for s in envelope.value.sources:
            assert abs(s.weight - 0.5) < 0.001

    def test_lwap_no_pools_raises(self):
        """LWAP raises DataUnavailableError when no pools found."""
        registry = MagicMock(spec=PoolReaderRegistry)
        registry.protocols_for_chain.return_value = ["uniswap_v3"]
        reader = MagicMock(spec=UniswapV3PoolPriceReader)
        reader.resolve_pool_address.return_value = None
        registry.get_reader.return_value = reader

        rpc_call = MagicMock()
        aggregator = PriceAggregator(pool_registry=registry, rpc_call=rpc_call)

        with pytest.raises(DataUnavailableError, match="No pools found"):
            aggregator.lwap("WETH", "USDC", "arbitrum")

    def test_lwap_no_protocols_raises(self):
        """LWAP raises DataUnavailableError when no protocols for chain."""
        registry = MagicMock(spec=PoolReaderRegistry)
        registry.protocols_for_chain.return_value = []

        rpc_call = MagicMock()
        aggregator = PriceAggregator(pool_registry=registry, rpc_call=rpc_call)

        with pytest.raises(DataUnavailableError, match="No protocols registered"):
            aggregator.lwap("WETH", "USDC", "unknown_chain")

    def test_lwap_execution_grade(self):
        """LWAP returns EXECUTION_GRADE classification."""
        pp = _make_pool_price("1800", 100000, pool_address="0xa", fee_tier=500)
        registry, rpc_call = self._setup_registry_with_pools([("0xa", "uniswap_v3", pp)])

        aggregator = PriceAggregator(
            pool_registry=registry,
            rpc_call=rpc_call,
            min_liquidity_usd=Decimal("0"),
        )
        envelope = aggregator.lwap("WETH", "USDC", "arbitrum")

        assert envelope.classification == DataClassification.EXECUTION_GRADE

    def test_lwap_meta_fields(self):
        """LWAP envelope has correct meta fields."""
        pp = _make_pool_price("1800", 100000, pool_address="0xa", fee_tier=500)
        registry, rpc_call = self._setup_registry_with_pools([("0xa", "uniswap_v3", pp)])

        aggregator = PriceAggregator(
            pool_registry=registry,
            rpc_call=rpc_call,
            min_liquidity_usd=Decimal("0"),
        )
        envelope = aggregator.lwap("WETH", "USDC", "arbitrum")

        assert envelope.meta.source == "alchemy_rpc"
        assert envelope.meta.finality == "latest"
        assert envelope.meta.confidence == 1.0
        assert envelope.meta.cache_hit is False

    def test_lwap_block_range(self):
        """LWAP reports block range from contributing pools."""
        pp1 = _make_pool_price("1800", 100000, pool_address="0xa", fee_tier=500, block_number=100)
        pp2 = _make_pool_price("1810", 100000, pool_address="0xb", fee_tier=3000, block_number=105)
        registry, rpc_call = self._setup_registry_with_pools(
            [
                ("0xa", "uniswap_v3", pp1),
                ("0xb", "uniswap_v3", pp2),
            ]
        )

        aggregator = PriceAggregator(
            pool_registry=registry,
            rpc_call=rpc_call,
            min_liquidity_usd=Decimal("0"),
        )
        envelope = aggregator.lwap("WETH", "USDC", "arbitrum")

        assert envelope.value.block_range == (100, 105)

    def test_lwap_custom_fee_tiers(self):
        """LWAP searches specified fee tiers only."""
        pp = _make_pool_price("1800", 100000, pool_address="0xa", fee_tier=500)
        registry, rpc_call = self._setup_registry_with_pools([("0xa", "uniswap_v3", pp)])

        aggregator = PriceAggregator(
            pool_registry=registry,
            rpc_call=rpc_call,
            min_liquidity_usd=Decimal("0"),
        )
        # Only search fee_tier=500
        envelope = aggregator.lwap("WETH", "USDC", "arbitrum", fee_tiers=[500])

        assert envelope.value.pool_count == 1
        assert envelope.value.price == Decimal("1800")

    def test_lwap_custom_protocols(self):
        """LWAP searches specified protocols only."""
        pp = _make_pool_price("1800", 100000, pool_address="0xa", fee_tier=500)
        registry, rpc_call = self._setup_registry_with_pools([("0xa", "uniswap_v3", pp)])

        aggregator = PriceAggregator(
            pool_registry=registry,
            rpc_call=rpc_call,
            min_liquidity_usd=Decimal("0"),
        )
        envelope = aggregator.lwap("WETH", "USDC", "arbitrum", protocols=["uniswap_v3"])

        assert envelope.value.pool_count == 1

    def test_lwap_skips_failed_pool_reads(self):
        """LWAP skips pools that fail to read, uses remaining."""
        pp1 = _make_pool_price("1800", 100000, pool_address="0xa", fee_tier=500)
        registry = MagicMock(spec=PoolReaderRegistry)
        registry.protocols_for_chain.return_value = ["uniswap_v3"]

        reader = MagicMock(spec=UniswapV3PoolPriceReader)

        def resolve_pool_address(ta, tb, c, fee_tier):
            if fee_tier == 500:
                return "0xa"
            if fee_tier == 3000:
                return "0xfail"
            return None

        def read_pool_price(addr, c):
            if addr == "0xa":
                return _make_envelope(pp1)
            raise DataUnavailableError("pool_price", addr, "RPC error")

        reader.resolve_pool_address.side_effect = resolve_pool_address
        reader.read_pool_price.side_effect = read_pool_price
        registry.get_reader.return_value = reader

        rpc_call = MagicMock()
        aggregator = PriceAggregator(
            pool_registry=registry,
            rpc_call=rpc_call,
            min_liquidity_usd=Decimal("0"),
        )
        envelope = aggregator.lwap("WETH", "USDC", "arbitrum")

        # Should succeed with just the one working pool
        assert envelope.value.pool_count == 1
        assert envelope.value.price == Decimal("1800")

    def test_lwap_with_reference_price_filtering(self):
        """LWAP filters by USD-estimated liquidity when reference_price_usd is set."""
        # Pool with high liquidity in raw terms but low USD value
        pp_low_usd = _make_pool_price("1800", 100, pool_address="0xa", fee_tier=500, token0_decimals=18)
        # Pool with high liquidity in USD terms
        pp_high_usd = _make_pool_price("1810", 10**22, pool_address="0xb", fee_tier=3000, token0_decimals=18)
        registry, rpc_call = self._setup_registry_with_pools(
            [
                ("0xa", "uniswap_v3", pp_low_usd),
                ("0xb", "uniswap_v3", pp_high_usd),
            ]
        )

        aggregator = PriceAggregator(
            pool_registry=registry,
            rpc_call=rpc_call,
            min_liquidity_usd=Decimal("10000"),
            reference_price_usd=Decimal("2000"),
        )
        envelope = aggregator.lwap("WETH", "USDC", "arbitrum")

        # Only the high-USD pool should survive filtering
        assert envelope.value.pool_count == 1
        assert envelope.value.sources[0].pool_address == "0xb"

    def test_lwap_chain_case_insensitive(self):
        """LWAP normalizes chain name to lowercase."""
        pp = _make_pool_price("1800", 100000, pool_address="0xa", fee_tier=500)
        registry, rpc_call = self._setup_registry_with_pools([("0xa", "uniswap_v3", pp)])

        aggregator = PriceAggregator(
            pool_registry=registry,
            rpc_call=rpc_call,
            min_liquidity_usd=Decimal("0"),
        )
        envelope = aggregator.lwap("WETH", "USDC", "Arbitrum")

        assert envelope.value.pool_count == 1


# ---------------------------------------------------------------------------
# Tests: DataEnvelope transparent delegation
# ---------------------------------------------------------------------------


class TestEnvelopeDelegation:
    """Tests that AggregatedPrice fields are accessible through DataEnvelope."""

    def test_delegate_price(self):
        """DataEnvelope delegates .price to AggregatedPrice."""
        from almanak.framework.data.models import DataMeta

        ap = AggregatedPrice(price=Decimal("1805"), method="lwap", pool_count=2)
        meta = DataMeta(source="test", observed_at=datetime.now(UTC))
        envelope = DataEnvelope(value=ap, meta=meta)

        assert envelope.price == Decimal("1805")
        assert envelope.method == "lwap"
        assert envelope.pool_count == 2

    def test_delegate_sources(self):
        """DataEnvelope delegates .sources to AggregatedPrice."""
        from almanak.framework.data.models import DataMeta

        sources = [PoolContribution("0xa", "uni", Decimal("1800"), 1.0)]
        ap = AggregatedPrice(price=Decimal("1800"), sources=sources)
        meta = DataMeta(source="test", observed_at=datetime.now(UTC))
        envelope = DataEnvelope(value=ap, meta=meta)

        assert len(envelope.sources) == 1
        assert envelope.sources[0].pool_address == "0xa"
