"""Tests for LiquidityDepthReader, SlippageEstimator, and related utilities.

Tests cover:
- TickData, LiquidityDepth, SlippageEstimate dataclass construction
- _tick_to_price conversion
- _encode_int24 / _encode_int16 encoding
- _decode_tick_data decoding
- LiquidityDepthReader: read_liquidity_depth with mocked RPC
- LiquidityDepthReader: tick bitmap scanning and tick reads
- LiquidityDepthReader: tick spacing detection (fee tier, on-chain, default)
- SlippageEstimator: V3 swap simulation
- SlippageEstimator: V2 constant-product slippage (x*y=k)
- SlippageEstimator: pool resolution and error handling
- MarketSnapshot integration: liquidity_depth() and estimate_slippage()
- High slippage warnings
- EXECUTION_GRADE classification
"""

from __future__ import annotations

import math
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.data.exceptions import DataUnavailableError
from almanak.framework.data.market_snapshot import (
    LiquidityDepthUnavailableError,
    MarketSnapshot,
    SlippageEstimateUnavailableError,
)
from almanak.framework.data.models import DataClassification, DataEnvelope
from almanak.framework.data.pools.liquidity import (
    FEE_TO_TICK_SPACING,
    LiquidityDepth,
    LiquidityDepthReader,
    SlippageEstimate,
    SlippageEstimator,
    TickData,
    _decode_tick_data,
    _encode_int16,
    _encode_int24,
    _tick_to_price,
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


def _int128_bytes(value: int) -> bytes:
    """Encode a signed int128 as a 32-byte big-endian signed word."""
    return value.to_bytes(32, byteorder="big", signed=True)


def _address_bytes(address: str) -> bytes:
    """Encode an address as a 32-byte left-padded word."""
    addr_bytes = bytes.fromhex(address.replace("0x", ""))
    return b"\x00" * 12 + addr_bytes


def _build_slot0_response(sqrt_price_x96: int, tick: int) -> bytes:
    """Build a mock slot0() response."""
    return _uint256_bytes(sqrt_price_x96) + _int256_bytes(tick) + b"\x00" * (5 * 32)


def _build_tick_response(liquidity_gross: int, liquidity_net: int) -> bytes:
    """Build a mock ticks(int24) response."""
    return _uint256_bytes(liquidity_gross) + _int128_bytes(liquidity_net)


def _build_bitmap_response(bitmap: int) -> bytes:
    """Build a mock tickBitmap(int16) response."""
    return _uint256_bytes(bitmap)


# ---------------------------------------------------------------------------
# Test dataclass construction
# ---------------------------------------------------------------------------


class TestTickData:
    """Tests for TickData frozen dataclass."""

    def test_construction(self):
        td = TickData(tick_index=100, liquidity_net=5000, price_at_tick=Decimal("1800.5"))
        assert td.tick_index == 100
        assert td.liquidity_net == 5000
        assert td.price_at_tick == Decimal("1800.5")

    def test_frozen(self):
        td = TickData(tick_index=100, liquidity_net=5000, price_at_tick=Decimal("1800"))
        with pytest.raises(AttributeError):
            td.tick_index = 200

    def test_negative_tick(self):
        td = TickData(tick_index=-500, liquidity_net=-3000, price_at_tick=Decimal("0.001"))
        assert td.tick_index == -500
        assert td.liquidity_net == -3000


class TestLiquidityDepth:
    """Tests for LiquidityDepth frozen dataclass."""

    def test_construction(self):
        ticks = [
            TickData(tick_index=-100, liquidity_net=1000, price_at_tick=Decimal("1790")),
            TickData(tick_index=100, liquidity_net=-1000, price_at_tick=Decimal("1810")),
        ]
        depth = LiquidityDepth(
            ticks=ticks,
            total_liquidity=50000,
            current_tick=0,
            current_price=Decimal("1800"),
            pool_address="0xabc",
            token0_decimals=18,
            token1_decimals=6,
            tick_spacing=60,
        )
        assert len(depth.ticks) == 2
        assert depth.total_liquidity == 50000
        assert depth.current_tick == 0
        assert depth.tick_spacing == 60


class TestSlippageEstimate:
    """Tests for SlippageEstimate frozen dataclass."""

    def test_construction(self):
        est = SlippageEstimate(
            expected_price=Decimal("1795.50"),
            price_impact_bps=25,
            effective_slippage_bps=30,
            recommended_max_size=Decimal("100000"),
        )
        assert est.expected_price == Decimal("1795.50")
        assert est.price_impact_bps == 25
        assert est.effective_slippage_bps == 30
        assert est.recommended_max_size == Decimal("100000")


# ---------------------------------------------------------------------------
# Test encoding helpers
# ---------------------------------------------------------------------------


class TestEncoding:
    """Tests for ABI encoding helpers."""

    def test_encode_int24_positive(self):
        result = _encode_int24(100)
        assert len(result) == 64  # 32 bytes hex
        assert int(result, 16) == 100

    def test_encode_int24_zero(self):
        result = _encode_int24(0)
        assert result == "0" * 64

    def test_encode_int24_negative(self):
        result = _encode_int24(-100)
        # Twos complement: should decode back to negative
        val = int(result, 16)
        assert val == (1 << 256) - 100

    def test_encode_int16_positive(self):
        result = _encode_int16(5)
        assert len(result) == 64
        assert int(result, 16) == 5

    def test_encode_int16_negative(self):
        result = _encode_int16(-3)
        val = int(result, 16)
        assert val == (1 << 256) - 3


# ---------------------------------------------------------------------------
# Test decoding helpers
# ---------------------------------------------------------------------------


class TestDecoding:
    """Tests for tick data decoding."""

    def test_decode_tick_data_positive(self):
        data = _build_tick_response(1000, 500)
        gross, net = _decode_tick_data(data)
        assert gross == 1000
        assert net == 500

    def test_decode_tick_data_negative_net(self):
        data = _build_tick_response(1000, -500)
        gross, net = _decode_tick_data(data)
        assert gross == 1000
        assert net == -500

    def test_decode_tick_data_short_response(self):
        gross, net = _decode_tick_data(b"\x00" * 30)
        assert gross == 0
        assert net == 0


# ---------------------------------------------------------------------------
# Test _tick_to_price
# ---------------------------------------------------------------------------


class TestTickToPrice:
    """Tests for tick -> price conversion."""

    def test_tick_zero(self):
        price = _tick_to_price(0, 18, 18)
        assert abs(price - Decimal("1")) < Decimal("0.001")

    def test_positive_tick(self):
        # 1.0001^100 ~ 1.01005
        price = _tick_to_price(100, 18, 18)
        expected = Decimal(str(math.pow(1.0001, 100)))
        assert abs(price - expected) < Decimal("0.001")

    def test_negative_tick(self):
        # 1.0001^(-100) ~ 0.99005
        price = _tick_to_price(-100, 18, 18)
        expected = Decimal(str(math.pow(1.0001, -100)))
        assert abs(price - expected) < Decimal("0.001")

    def test_decimal_adjustment(self):
        # With different decimals: multiply by 10^(token0_dec - token1_dec)
        price = _tick_to_price(0, 18, 6)
        # 1.0001^0 * 10^(18-6) = 1 * 10^12
        assert abs(price - Decimal("1000000000000")) < Decimal("1")

    def test_usdc_weth_typical_tick(self):
        # Typical USDC/WETH tick around -201000 with (6, 18) decimals
        # This gives a small number since USDC is token0 (6 dec), WETH is token1 (18 dec)
        price = _tick_to_price(-201000, 6, 18)
        assert price > Decimal("0")
        assert price < Decimal("1")  # fraction since decimal adjustment is 10^(-12)


# ---------------------------------------------------------------------------
# Test FEE_TO_TICK_SPACING
# ---------------------------------------------------------------------------


class TestFeeToTickSpacing:
    """Tests for fee-to-tick-spacing mapping."""

    def test_standard_fees(self):
        assert FEE_TO_TICK_SPACING[100] == 1
        assert FEE_TO_TICK_SPACING[500] == 10
        assert FEE_TO_TICK_SPACING[3000] == 60
        assert FEE_TO_TICK_SPACING[10000] == 200


# ---------------------------------------------------------------------------
# Test LiquidityDepthReader
# ---------------------------------------------------------------------------


class TestLiquidityDepthReader:
    """Tests for LiquidityDepthReader with mocked RPC."""

    def _make_rpc_mock(
        self,
        slot0_sqrt_price: int = 79228162514264337593543950336,  # 2^96 = price 1.0
        slot0_tick: int = 0,
        liquidity: int = 100000,
        tick_spacing: int = 60,
        bitmap_words: dict[int, int] | None = None,
        tick_data: dict[int, tuple[int, int]] | None = None,
    ):
        """Create a mock RPC call that dispatches on selector."""
        from almanak.framework.data.pools.liquidity import TICK_BITMAP_SELECTOR, TICK_SPACING_SELECTOR, TICKS_SELECTOR
        from almanak.framework.data.pools.reader import LIQUIDITY_SELECTOR, SLOT0_SELECTOR

        if bitmap_words is None:
            bitmap_words = {}
        if tick_data is None:
            tick_data = {}

        def mock_rpc(chain, to_addr, calldata):
            selector = calldata[:10] if isinstance(calldata, str) else calldata
            if selector == SLOT0_SELECTOR:
                return _build_slot0_response(slot0_sqrt_price, slot0_tick)
            if selector == LIQUIDITY_SELECTOR:
                return _uint256_bytes(liquidity)
            if selector == TICK_SPACING_SELECTOR:
                return _int256_bytes(tick_spacing)
            if isinstance(selector, str) and selector.startswith(TICK_BITMAP_SELECTOR):
                # Parse word position from calldata
                word_hex = calldata[10:]
                word_val = int(word_hex, 16)
                if word_val >= (1 << 255):
                    word_val -= 1 << 256
                return _build_bitmap_response(bitmap_words.get(word_val, 0))
            if isinstance(selector, str) and selector.startswith(TICKS_SELECTOR):
                # Parse tick index from calldata
                tick_hex = calldata[10:]
                tick_val = int(tick_hex, 16)
                if tick_val >= (1 << 255):
                    tick_val -= 1 << 256
                gross, net = tick_data.get(tick_val, (0, 0))
                return _build_tick_response(gross, net)
            return b"\x00" * 32

        return mock_rpc

    def test_basic_read(self):
        """Test basic liquidity depth read with pre-supplied state."""
        rpc = self._make_rpc_mock()
        reader = LiquidityDepthReader(rpc_call=rpc, tick_range_multiplier=10)

        envelope = reader.read_liquidity_depth(
            pool_address="0xabc",
            chain="arbitrum",
            current_tick=0,
            current_liquidity=50000,
            current_price=Decimal("1800"),
            token0_decimals=18,
            token1_decimals=6,
            tick_spacing=60,
        )

        assert isinstance(envelope, DataEnvelope)
        assert envelope.classification == DataClassification.EXECUTION_GRADE
        assert envelope.value.current_tick == 0
        assert envelope.value.total_liquidity == 50000
        assert envelope.value.current_price == Decimal("1800")
        assert envelope.meta.source == "alchemy_rpc"
        assert envelope.meta.cache_hit is False

    def test_read_with_rpc_fallback(self):
        """Test that slot0/liquidity are read from RPC when not provided."""
        rpc = self._make_rpc_mock(
            slot0_sqrt_price=79228162514264337593543950336,
            slot0_tick=100,
            liquidity=75000,
            tick_spacing=60,
        )
        reader = LiquidityDepthReader(rpc_call=rpc, tick_range_multiplier=5)

        envelope = reader.read_liquidity_depth(
            pool_address="0xdef",
            chain="ethereum",
            token0_decimals=18,
            token1_decimals=18,
        )

        assert envelope.value.current_tick == 100
        assert envelope.value.total_liquidity == 75000
        assert envelope.value.tick_spacing == 60

    def test_tick_scanning_finds_initialized_ticks(self):
        """Test that the bitmap scanner finds initialized ticks."""
        # Set up bitmap: word 0, bit 1 set -> compressed tick 1 -> actual tick 60
        # Word 0, bit 2 set -> compressed tick 2 -> actual tick 120
        bitmap_words = {0: 0b110}  # bits 1 and 2 set
        tick_data = {
            60: (1000, 500),  # liquidityGross=1000, liquidityNet=500
            120: (2000, -500),  # liquidityGross=2000, liquidityNet=-500
        }
        rpc = self._make_rpc_mock(
            slot0_tick=80,
            liquidity=5000,
            tick_spacing=60,
            bitmap_words=bitmap_words,
            tick_data=tick_data,
        )
        reader = LiquidityDepthReader(rpc_call=rpc, tick_range_multiplier=5)

        envelope = reader.read_liquidity_depth(
            pool_address="0x111",
            chain="arbitrum",
            current_tick=80,
            current_liquidity=5000,
            current_price=Decimal("1800"),
            token0_decimals=18,
            token1_decimals=6,
            tick_spacing=60,
        )

        ticks = envelope.value.ticks
        assert len(ticks) == 2
        assert ticks[0].tick_index == 60
        assert ticks[0].liquidity_net == 500
        assert ticks[1].tick_index == 120
        assert ticks[1].liquidity_net == -500

    def test_tick_spacing_from_fee_tier(self):
        """Test tick spacing inference from fee tier."""
        rpc = self._make_rpc_mock()
        reader = LiquidityDepthReader(rpc_call=rpc, tick_range_multiplier=5)

        envelope = reader.read_liquidity_depth(
            pool_address="0xabc",
            chain="arbitrum",
            current_tick=0,
            current_liquidity=100,
            current_price=Decimal("1"),
            token0_decimals=18,
            token1_decimals=18,
            fee_tier=500,  # Should infer tick_spacing=10
        )

        assert envelope.value.tick_spacing == 10

    def test_tick_spacing_from_rpc(self):
        """Test tick spacing read from contract when fee tier not available."""
        rpc = self._make_rpc_mock(tick_spacing=200)
        reader = LiquidityDepthReader(rpc_call=rpc, tick_range_multiplier=5)

        envelope = reader.read_liquidity_depth(
            pool_address="0xabc",
            chain="arbitrum",
            current_tick=0,
            current_liquidity=100,
            current_price=Decimal("1"),
            token0_decimals=18,
            token1_decimals=18,
        )

        assert envelope.value.tick_spacing == 200

    def test_rpc_error_raises_data_unavailable(self):
        """Test that RPC errors are wrapped in DataUnavailableError."""

        def failing_rpc(chain, to, calldata):
            raise ConnectionError("RPC down")

        reader = LiquidityDepthReader(rpc_call=failing_rpc)

        with pytest.raises(DataUnavailableError, match="Failed to read liquidity depth"):
            reader.read_liquidity_depth(
                pool_address="0xfail",
                chain="ethereum",
            )

    def test_transparent_delegation(self):
        """Test DataEnvelope transparent delegation to LiquidityDepth."""
        rpc = self._make_rpc_mock()
        reader = LiquidityDepthReader(rpc_call=rpc, tick_range_multiplier=5)

        envelope = reader.read_liquidity_depth(
            pool_address="0xtest",
            chain="base",
            current_tick=42,
            current_liquidity=999,
            current_price=Decimal("1500"),
            token0_decimals=18,
            token1_decimals=6,
            tick_spacing=60,
        )

        # Transparent delegation
        assert envelope.total_liquidity == 999
        assert envelope.current_tick == 42


# ---------------------------------------------------------------------------
# Test SlippageEstimator - V2 constant product
# ---------------------------------------------------------------------------


class TestSlippageEstimatorV2:
    """Tests for V2-style constant-product slippage estimation."""

    def _make_estimator(self):
        reader = LiquidityDepthReader(rpc_call=lambda *a: b"\x00" * 32)
        return SlippageEstimator(liquidity_reader=reader)

    def test_small_trade_low_slippage(self):
        """Small trade relative to pool should have low slippage."""
        est = self._make_estimator()
        result = est.estimate_slippage_v2(
            amount_in=Decimal("1"),
            reserve_in=Decimal("1000000"),
            reserve_out=Decimal("1000000"),
            fee_bps=30,
        )
        assert result.price_impact_bps < 5  # < 0.05%
        assert result.effective_slippage_bps < 40  # < 0.4% (including 0.3% fee)

    def test_large_trade_high_slippage(self):
        """Large trade relative to pool should have high slippage."""
        est = self._make_estimator()
        result = est.estimate_slippage_v2(
            amount_in=Decimal("100000"),
            reserve_in=Decimal("1000000"),
            reserve_out=Decimal("1000000"),
            fee_bps=30,
        )
        assert result.price_impact_bps > 800  # > 8%
        assert result.expected_price < Decimal("1")  # Worse than mid-market

    def test_zero_amount(self):
        """Zero amount should return mid-market price."""
        est = self._make_estimator()
        result = est.estimate_slippage_v2(
            amount_in=Decimal("0"),
            reserve_in=Decimal("1000000"),
            reserve_out=Decimal("1000000"),
            fee_bps=30,
        )
        assert result.expected_price == Decimal("1")
        assert result.price_impact_bps == 0

    def test_zero_reserves_raises(self):
        """Zero reserves should raise DataUnavailableError."""
        est = self._make_estimator()
        with pytest.raises(DataUnavailableError, match="reserves must be positive"):
            est.estimate_slippage_v2(
                amount_in=Decimal("100"),
                reserve_in=Decimal("0"),
                reserve_out=Decimal("1000000"),
            )

    def test_fee_impact(self):
        """Higher fees should increase effective slippage."""
        est = self._make_estimator()

        # 0.3% fee
        result_low = est.estimate_slippage_v2(
            amount_in=Decimal("1000"),
            reserve_in=Decimal("1000000"),
            reserve_out=Decimal("1000000"),
            fee_bps=30,
        )
        # 1% fee
        result_high = est.estimate_slippage_v2(
            amount_in=Decimal("1000"),
            reserve_in=Decimal("1000000"),
            reserve_out=Decimal("1000000"),
            fee_bps=100,
        )

        assert result_high.effective_slippage_bps > result_low.effective_slippage_bps

    def test_recommended_max_size(self):
        """Recommended max size should be approximately 1% of reserves."""
        est = self._make_estimator()
        result = est.estimate_slippage_v2(
            amount_in=Decimal("1000"),
            reserve_in=Decimal("1000000"),
            reserve_out=Decimal("1000000"),
        )
        # recommended_max = reserve_in * 0.01 = 10000
        assert result.recommended_max_size == Decimal("10000")

    def test_known_calculation(self):
        """Verify against manually computed slippage for known inputs."""
        est = self._make_estimator()
        # reserve_in=1000, reserve_out=2000, amount_in=100, fee=0
        # mid_price = 2000/1000 = 2.0
        # amount_out = 2000*100 / (1000+100) = 181.818...
        # exec_price = 181.818.../100 = 1.818...
        # price_impact = |1 - 1.818.../2.0| * 10000 ~ 909 bps
        result = est.estimate_slippage_v2(
            amount_in=Decimal("100"),
            reserve_in=Decimal("1000"),
            reserve_out=Decimal("2000"),
            fee_bps=0,
        )
        assert abs(result.price_impact_bps - 909) <= 1
        assert abs(result.expected_price - Decimal("1.818181818181818181818181818")) < Decimal("0.01")


# ---------------------------------------------------------------------------
# Test SlippageEstimator - V3 simulation
# ---------------------------------------------------------------------------


class TestSlippageEstimatorV3:
    """Tests for V3 swap simulation through tick ranges."""

    def _make_pool_reader_registry(self, pool_price=None, pool_address=None):
        """Create a mock PoolReaderRegistry."""
        registry = MagicMock()
        reader = MagicMock()

        if pool_price is not None:
            from datetime import UTC, datetime

            from almanak.framework.data.models import DataClassification, DataEnvelope, DataMeta

            meta = DataMeta(
                source="test",
                observed_at=datetime.now(UTC),
                block_number=100,
                finality="latest",
                staleness_ms=0,
                latency_ms=10,
                confidence=1.0,
                cache_hit=False,
            )
            envelope = DataEnvelope(value=pool_price, meta=meta, classification=DataClassification.EXECUTION_GRADE)
            reader.read_pool_price.return_value = envelope

        if pool_address is not None:
            reader.resolve_pool_address.return_value = pool_address
        else:
            reader.resolve_pool_address.return_value = None

        registry.protocols_for_chain.return_value = ["uniswap_v3"]
        registry.get_reader.return_value = reader

        return registry

    def test_estimate_slippage_with_pool_address(self):
        """Test slippage estimation when pool address is provided."""
        from datetime import UTC, datetime

        from almanak.framework.data.pools.reader import PoolPrice

        pool_price = PoolPrice(
            price=Decimal("1800"),
            tick=0,
            liquidity=10**18,
            fee_tier=3000,
            block_number=100,
            timestamp=datetime.now(UTC),
            pool_address="0xpool",
            token0_decimals=18,
            token1_decimals=6,
        )

        registry = self._make_pool_reader_registry(pool_price=pool_price, pool_address="0xpool")

        # Create liquidity reader that returns some ticks
        def mock_rpc(chain, to, calldata):
            return b"\x00" * 32

        liq_reader = LiquidityDepthReader(rpc_call=mock_rpc, tick_range_multiplier=5)

        # Patch read_liquidity_depth to return known data
        depth = LiquidityDepth(
            ticks=[
                TickData(tick_index=-120, liquidity_net=10**15, price_at_tick=Decimal("1750")),
                TickData(tick_index=-60, liquidity_net=10**15, price_at_tick=Decimal("1790")),
                TickData(tick_index=60, liquidity_net=-(10**15), price_at_tick=Decimal("1810")),
                TickData(tick_index=120, liquidity_net=-(10**15), price_at_tick=Decimal("1850")),
            ],
            total_liquidity=10**18,
            current_tick=0,
            current_price=Decimal("1800"),
            pool_address="0xpool",
            token0_decimals=18,
            token1_decimals=6,
            tick_spacing=60,
        )
        from almanak.framework.data.models import DataMeta

        depth_envelope = DataEnvelope(
            value=depth,
            meta=DataMeta(
                source="test",
                observed_at=datetime.now(UTC),
                finality="latest",
            ),
            classification=DataClassification.EXECUTION_GRADE,
        )

        with patch.object(liq_reader, "read_liquidity_depth", return_value=depth_envelope):
            estimator = SlippageEstimator(
                liquidity_reader=liq_reader,
                pool_reader_registry=registry,
            )

            envelope = estimator.estimate_slippage(
                token_in="WETH",
                token_out="USDC",
                amount=Decimal("1"),
                chain="arbitrum",
                pool_address="0xpool",
            )

        assert isinstance(envelope, DataEnvelope)
        assert envelope.classification == DataClassification.EXECUTION_GRADE
        assert isinstance(envelope.value, SlippageEstimate)
        assert envelope.value.expected_price > Decimal("0")

    def test_no_pool_found_raises(self):
        """Test that missing pool raises DataUnavailableError."""
        registry = self._make_pool_reader_registry(pool_address=None)
        liq_reader = LiquidityDepthReader(rpc_call=lambda *a: b"\x00" * 32)
        estimator = SlippageEstimator(
            liquidity_reader=liq_reader,
            pool_reader_registry=registry,
        )

        with pytest.raises(DataUnavailableError, match="No pool found"):
            estimator.estimate_slippage(
                token_in="RARE_TOKEN",
                token_out="USDC",
                amount=Decimal("100"),
                chain="arbitrum",
            )

    def test_no_registry_raises(self):
        """Test that missing registry raises DataUnavailableError when resolving."""
        liq_reader = LiquidityDepthReader(rpc_call=lambda *a: b"\x00" * 32)
        estimator = SlippageEstimator(liquidity_reader=liq_reader, pool_reader_registry=None)

        with pytest.raises(DataUnavailableError):
            estimator.estimate_slippage(
                token_in="WETH",
                token_out="USDC",
                amount=Decimal("1"),
                chain="arbitrum",
            )

    def test_high_slippage_warning(self, caplog):
        """Test that high slippage triggers a warning log."""
        from datetime import UTC, datetime

        from almanak.framework.data.pools.reader import PoolPrice

        pool_price = PoolPrice(
            price=Decimal("1800"),
            tick=0,
            liquidity=100,  # Very low liquidity
            fee_tier=3000,
            block_number=100,
            timestamp=datetime.now(UTC),
            pool_address="0xpool",
            token0_decimals=18,
            token1_decimals=6,
        )

        registry = self._make_pool_reader_registry(pool_price=pool_price, pool_address="0xpool")
        liq_reader = LiquidityDepthReader(rpc_call=lambda *a: b"\x00" * 32)

        # Return empty liquidity -> will give max slippage
        depth = LiquidityDepth(
            ticks=[],
            total_liquidity=0,
            current_tick=0,
            current_price=Decimal("1800"),
            pool_address="0xpool",
            token0_decimals=18,
            token1_decimals=6,
            tick_spacing=60,
        )
        from almanak.framework.data.models import DataMeta

        depth_envelope = DataEnvelope(
            value=depth,
            meta=DataMeta(source="test", observed_at=datetime.now(UTC), finality="latest"),
            classification=DataClassification.EXECUTION_GRADE,
        )

        with patch.object(liq_reader, "read_liquidity_depth", return_value=depth_envelope):
            estimator = SlippageEstimator(
                liquidity_reader=liq_reader,
                pool_reader_registry=registry,
                high_slippage_threshold_bps=50,
            )

            with caplog.at_level("WARNING", logger="almanak.framework.data.pools.liquidity"):
                envelope = estimator.estimate_slippage(
                    token_in="WETH",
                    token_out="USDC",
                    amount=Decimal("1000"),
                    chain="arbitrum",
                    pool_address="0xpool",
                )

            # Should have logged a warning about high slippage
            assert envelope.value.effective_slippage_bps > 0 or envelope.value.price_impact_bps >= 10000
            assert any("slippage" in msg.lower() for msg in caplog.messages), "Expected high slippage warning log"


# ---------------------------------------------------------------------------
# Test MarketSnapshot integration
# ---------------------------------------------------------------------------


class TestMarketSnapshotLiquidity:
    """Tests for liquidity_depth() and estimate_slippage() on MarketSnapshot."""

    def test_liquidity_depth_no_reader_raises(self):
        """Test that calling liquidity_depth without reader raises ValueError."""
        snapshot = MarketSnapshot(chain="arbitrum", wallet_address="0x1")

        with pytest.raises(ValueError, match="No liquidity depth reader"):
            snapshot.liquidity_depth("0xpool")

    def test_estimate_slippage_no_estimator_raises(self):
        """Test that calling estimate_slippage without estimator raises ValueError."""
        snapshot = MarketSnapshot(chain="arbitrum", wallet_address="0x1")

        with pytest.raises(ValueError, match="No slippage estimator"):
            snapshot.estimate_slippage("WETH", "USDC", Decimal("1"))

    def test_liquidity_depth_delegates(self):
        """Test that liquidity_depth delegates to reader correctly."""
        mock_reader = MagicMock()
        from datetime import UTC, datetime

        from almanak.framework.data.models import DataMeta

        depth = LiquidityDepth(
            ticks=[],
            total_liquidity=100,
            current_tick=0,
            current_price=Decimal("1800"),
            tick_spacing=60,
        )
        envelope = DataEnvelope(
            value=depth,
            meta=DataMeta(source="test", observed_at=datetime.now(UTC), finality="latest"),
            classification=DataClassification.EXECUTION_GRADE,
        )
        mock_reader.read_liquidity_depth.return_value = envelope

        snapshot = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0x1",
            liquidity_depth_reader=mock_reader,
        )

        result = snapshot.liquidity_depth("0xpool")
        assert result.value.total_liquidity == 100
        mock_reader.read_liquidity_depth.assert_called_once_with(
            pool_address="0xpool",
            chain="arbitrum",
        )

    def test_liquidity_depth_chain_override(self):
        """Test that chain parameter overrides strategy chain."""
        mock_reader = MagicMock()
        from datetime import UTC, datetime

        from almanak.framework.data.models import DataMeta

        depth = LiquidityDepth(
            ticks=[],
            total_liquidity=100,
            current_tick=0,
            current_price=Decimal("1800"),
            tick_spacing=60,
        )
        envelope = DataEnvelope(
            value=depth,
            meta=DataMeta(source="test", observed_at=datetime.now(UTC), finality="latest"),
            classification=DataClassification.EXECUTION_GRADE,
        )
        mock_reader.read_liquidity_depth.return_value = envelope

        snapshot = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0x1",
            liquidity_depth_reader=mock_reader,
        )

        snapshot.liquidity_depth("0xpool", chain="base")
        mock_reader.read_liquidity_depth.assert_called_once_with(
            pool_address="0xpool",
            chain="base",
        )

    def test_liquidity_depth_error_wrapping(self):
        """Test that reader errors are wrapped in LiquidityDepthUnavailableError."""
        mock_reader = MagicMock()
        mock_reader.read_liquidity_depth.side_effect = RuntimeError("RPC failure")

        snapshot = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0x1",
            liquidity_depth_reader=mock_reader,
        )

        with pytest.raises(LiquidityDepthUnavailableError, match="RPC failure"):
            snapshot.liquidity_depth("0xpool")

    def test_estimate_slippage_delegates(self):
        """Test that estimate_slippage delegates to estimator correctly."""
        mock_estimator = MagicMock()
        from datetime import UTC, datetime

        from almanak.framework.data.models import DataMeta

        est = SlippageEstimate(
            expected_price=Decimal("1795"),
            price_impact_bps=25,
            effective_slippage_bps=30,
            recommended_max_size=Decimal("50000"),
        )
        envelope = DataEnvelope(
            value=est,
            meta=DataMeta(source="test", observed_at=datetime.now(UTC), finality="latest"),
            classification=DataClassification.EXECUTION_GRADE,
        )
        mock_estimator.estimate_slippage.return_value = envelope

        snapshot = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0x1",
            slippage_estimator=mock_estimator,
        )

        result = snapshot.estimate_slippage("WETH", "USDC", Decimal("10"))
        assert result.value.price_impact_bps == 25
        mock_estimator.estimate_slippage.assert_called_once_with(
            token_in="WETH",
            token_out="USDC",
            amount=Decimal("10"),
            chain="arbitrum",
            protocol=None,
        )

    def test_estimate_slippage_error_wrapping(self):
        """Test that estimator errors are wrapped in SlippageEstimateUnavailableError."""
        mock_estimator = MagicMock()
        mock_estimator.estimate_slippage.side_effect = RuntimeError("No pool")

        snapshot = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0x1",
            slippage_estimator=mock_estimator,
        )

        with pytest.raises(SlippageEstimateUnavailableError, match="No pool"):
            snapshot.estimate_slippage("WETH", "USDC", Decimal("1"))

    def test_estimate_slippage_with_protocol(self):
        """Test that protocol parameter is forwarded."""
        mock_estimator = MagicMock()
        from datetime import UTC, datetime

        from almanak.framework.data.models import DataMeta

        est = SlippageEstimate(
            expected_price=Decimal("1795"),
            price_impact_bps=25,
            effective_slippage_bps=30,
            recommended_max_size=Decimal("50000"),
        )
        envelope = DataEnvelope(
            value=est,
            meta=DataMeta(source="test", observed_at=datetime.now(UTC), finality="latest"),
            classification=DataClassification.EXECUTION_GRADE,
        )
        mock_estimator.estimate_slippage.return_value = envelope

        snapshot = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0x1",
            slippage_estimator=mock_estimator,
        )

        snapshot.estimate_slippage("WETH", "USDC", Decimal("10"), protocol="pancakeswap_v3")
        mock_estimator.estimate_slippage.assert_called_once_with(
            token_in="WETH",
            token_out="USDC",
            amount=Decimal("10"),
            chain="arbitrum",
            protocol="pancakeswap_v3",
        )
