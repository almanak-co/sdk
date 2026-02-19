"""Tests for HexDecoder utilities.

This module provides comprehensive tests for all HexDecoder methods including:
- Address decoding from topics
- Unsigned integer decoding (uint256, uint160, uint128)
- Signed integer decoding (int256, int128, int24)
- Dynamic array decoding
- Edge cases and error handling
"""

from almanak.framework.connectors.base.hex_utils import HexDecoder


class TestHexDecoderNormalization:
    """Tests for hex string normalization."""

    def test_normalize_hex_with_prefix(self):
        """Test normalizing hex string with 0x prefix."""
        result = HexDecoder.normalize_hex("0xabcdef")
        assert result == "abcdef"

    def test_normalize_hex_without_prefix(self):
        """Test normalizing hex string without 0x prefix."""
        result = HexDecoder.normalize_hex("abcdef")
        assert result == "abcdef"

    def test_normalize_hex_bytes(self):
        """Test normalizing bytes input."""
        result = HexDecoder.normalize_hex(b"\xab\xcd\xef")
        assert result == "abcdef"

    def test_normalize_hex_empty(self):
        """Test normalizing empty values."""
        assert HexDecoder.normalize_hex("") == ""
        assert HexDecoder.normalize_hex("0x") == ""


class TestTopicToAddress:
    """Tests for converting topics to addresses."""

    def test_topic_to_address_string(self):
        """Test converting topic string to address."""
        topic = "0x000000000000000000000000a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
        result = HexDecoder.topic_to_address(topic)
        assert result == "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"

    def test_topic_to_address_bytes(self):
        """Test converting topic bytes to address."""
        # 32 bytes: 12 zero bytes + 20 address bytes
        topic = b"\x00" * 12 + bytes.fromhex("a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48")
        result = HexDecoder.topic_to_address(topic)
        assert result == "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"

    def test_topic_to_address_without_prefix(self):
        """Test converting topic without 0x prefix."""
        topic = "000000000000000000000000a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
        result = HexDecoder.topic_to_address(topic)
        assert result == "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"

    def test_topic_to_address_empty(self):
        """Test converting empty topic."""
        assert HexDecoder.topic_to_address("") == ""
        assert HexDecoder.topic_to_address(b"") == ""


class TestTopicToBytes32:
    """Tests for converting topics to bytes32."""

    def test_topic_to_bytes32_string(self):
        """Test converting topic string to bytes32."""
        topic = "0x0000000000000000000000000000000000000000000000000000000000000001"
        result = HexDecoder.topic_to_bytes32(topic)
        assert result == "0x0000000000000000000000000000000000000000000000000000000000000001"

    def test_topic_to_bytes32_bytes(self):
        """Test converting topic bytes to bytes32."""
        topic = b"\x00" * 31 + b"\x01"
        result = HexDecoder.topic_to_bytes32(topic)
        assert result == "0x0000000000000000000000000000000000000000000000000000000000000001"

    def test_topic_to_bytes32_short(self):
        """Test converting short hex to bytes32 (should pad)."""
        topic = "0x01"
        result = HexDecoder.topic_to_bytes32(topic)
        assert result == "0x0000000000000000000000000000000000000000000000000000000000000001"


class TestDecodeUint256:
    """Tests for decoding unsigned 256-bit integers."""

    def test_decode_uint256_zero(self):
        """Test decoding zero value."""
        hex_str = "0x" + "00" * 32
        result = HexDecoder.decode_uint256(hex_str)
        assert result == 0

    def test_decode_uint256_small(self):
        """Test decoding small positive value."""
        hex_str = "0x" + "00" * 31 + "0a"  # 10
        result = HexDecoder.decode_uint256(hex_str)
        assert result == 10

    def test_decode_uint256_large(self):
        """Test decoding large positive value."""
        hex_str = "0x" + "00" * 28 + "000003e8"  # 1000
        result = HexDecoder.decode_uint256(hex_str)
        assert result == 1000

    def test_decode_uint256_max(self):
        """Test decoding maximum uint256 value."""
        hex_str = "0x" + "ff" * 32
        result = HexDecoder.decode_uint256(hex_str)
        assert result == 2**256 - 1

    def test_decode_uint256_with_offset(self):
        """Test decoding with byte offset."""
        # Two values: 100 then 200
        hex_str = "0x" + "00" * 31 + "64" + "00" * 31 + "c8"
        result1 = HexDecoder.decode_uint256(hex_str, offset=0)
        result2 = HexDecoder.decode_uint256(hex_str, offset=32)
        assert result1 == 100
        assert result2 == 200


class TestDecodeInt256:
    """Tests for decoding signed 256-bit integers."""

    def test_decode_int256_zero(self):
        """Test decoding zero value."""
        hex_str = "0x" + "00" * 32
        result = HexDecoder.decode_int256(hex_str)
        assert result == 0

    def test_decode_int256_positive(self):
        """Test decoding positive value."""
        hex_str = "0x" + "00" * 28 + "000003e8"  # 1000
        result = HexDecoder.decode_int256(hex_str)
        assert result == 1000

    def test_decode_int256_negative_small(self):
        """Test decoding small negative value (two's complement)."""
        # -10 in two's complement
        hex_str = "0x" + "ff" * 31 + "f6"
        result = HexDecoder.decode_int256(hex_str)
        assert result == -10

    def test_decode_int256_negative_large(self):
        """Test decoding large negative value."""
        # -1000 in two's complement
        hex_str = "0x" + "ff" * 28 + "fffffc18"
        result = HexDecoder.decode_int256(hex_str)
        assert result == -1000

    def test_decode_int256_max_positive(self):
        """Test decoding maximum positive int256."""
        hex_str = "0x" + "7" + "f" * 63
        result = HexDecoder.decode_int256(hex_str)
        assert result == 2**255 - 1

    def test_decode_int256_max_negative(self):
        """Test decoding maximum negative int256."""
        hex_str = "0x" + "8" + "0" * 63
        result = HexDecoder.decode_int256(hex_str)
        assert result == -(2**255)

    def test_decode_int256_with_offset(self):
        """Test decoding with byte offset."""
        # Two values: 1000 then -1000
        hex_str = "0x" + "00" * 28 + "000003e8" + "ff" * 28 + "fffffc18"
        result1 = HexDecoder.decode_int256(hex_str, offset=0)
        result2 = HexDecoder.decode_int256(hex_str, offset=32)
        assert result1 == 1000
        assert result2 == -1000


class TestDecodeInt128:
    """Tests for decoding signed 128-bit integers."""

    def test_decode_int128_positive(self):
        """Test decoding positive int128."""
        hex_str = "0x" + "00" * 28 + "000003e8"  # 1000
        result = HexDecoder.decode_int128(hex_str)
        assert result == 1000

    def test_decode_int128_negative(self):
        """Test decoding negative int128 (stored in 256-bit slot)."""
        # -1000 in two's complement (stored as int256)
        hex_str = "0x" + "ff" * 28 + "fffffc18"
        result = HexDecoder.decode_int128(hex_str)
        assert result == -1000


class TestDecodeInt24:
    """Tests for decoding signed 24-bit integers (Uniswap V3 ticks)."""

    def test_decode_int24_zero(self):
        """Test decoding zero tick."""
        hex_str = "0x" + "00" * 32
        result = HexDecoder.decode_int24(hex_str)
        assert result == 0

    def test_decode_int24_positive(self):
        """Test decoding positive tick."""
        hex_str = "0x" + "00" * 31 + "64"  # 100
        result = HexDecoder.decode_int24(hex_str)
        assert result == 100

    def test_decode_int24_negative_small(self):
        """Test decoding small negative tick."""
        # -100 in int256 two's complement
        hex_str = "0x" + "ff" * 31 + "9c"
        result = HexDecoder.decode_int24(hex_str)
        assert result == -100

    def test_decode_int24_max_positive(self):
        """Test decoding max positive int24 (2^23 - 1 = 8388607)."""
        hex_str = "0x" + "00" * 29 + "7fffff"
        result = HexDecoder.decode_int24(hex_str)
        assert result == 8388607

    def test_decode_int24_max_negative(self):
        """Test decoding max negative int24 (-2^23 = -8388608)."""
        # Stored as int256 two's complement
        hex_str = "0x" + "ff" * 29 + "800000"
        result = HexDecoder.decode_int24(hex_str)
        assert result == -8388608


class TestDecodeUint160:
    """Tests for decoding unsigned 160-bit integers (Uniswap sqrtPrice)."""

    def test_decode_uint160_zero(self):
        """Test decoding zero value."""
        hex_str = "0x" + "00" * 32
        result = HexDecoder.decode_uint160(hex_str)
        assert result == 0

    def test_decode_uint160_value(self):
        """Test decoding uint160 value."""
        # sqrtPriceX96 example
        hex_str = "0x" + "00" * 12 + "000001000000000000000000"
        result = HexDecoder.decode_uint160(hex_str)
        assert result > 0


class TestDecodeUint128:
    """Tests for decoding unsigned 128-bit integers (Uniswap liquidity)."""

    def test_decode_uint128_zero(self):
        """Test decoding zero liquidity."""
        hex_str = "0x" + "00" * 32
        result = HexDecoder.decode_uint128(hex_str)
        assert result == 0

    def test_decode_uint128_value(self):
        """Test decoding uint128 value."""
        hex_str = "0x" + "00" * 16 + "0000000000001000"
        result = HexDecoder.decode_uint128(hex_str)
        assert result == 4096


class TestDecodeDynamicArray:
    """Tests for decoding dynamic arrays."""

    def test_decode_dynamic_array_empty(self):
        """Test decoding empty array."""
        # Offset pointing to length=0
        hex_str = "0x" + "00" * 31 + "20"  # Offset to byte 32
        hex_str += "00" * 32  # Length = 0
        result = HexDecoder.decode_dynamic_array(hex_str, offset=0)
        assert result == []

    def test_decode_dynamic_array_single(self):
        """Test decoding single element array."""
        # Offset to byte 32, length=1, value=42
        hex_str = "0x" + "00" * 31 + "20"  # Offset
        hex_str += "00" * 31 + "01"  # Length = 1
        hex_str += "00" * 31 + "2a"  # Value = 42
        result = HexDecoder.decode_dynamic_array(hex_str, offset=0)
        assert result == [42]

    def test_decode_dynamic_array_multiple(self):
        """Test decoding multiple element array."""
        # Offset to byte 32, length=3, values=[1,2,3]
        hex_str = "0x" + "00" * 31 + "20"  # Offset
        hex_str += "00" * 31 + "03"  # Length = 3
        hex_str += "00" * 31 + "01"  # Value[0] = 1
        hex_str += "00" * 31 + "02"  # Value[1] = 2
        hex_str += "00" * 31 + "03"  # Value[2] = 3
        result = HexDecoder.decode_dynamic_array(hex_str, offset=0)
        assert result == [1, 2, 3]

    def test_decode_dynamic_array_invalid(self):
        """Test decoding with insufficient data."""
        hex_str = "0x" + "00" * 10  # Too short
        result = HexDecoder.decode_dynamic_array(hex_str, offset=0)
        assert result == []


class TestDecodeAddressFromData:
    """Tests for decoding addresses from event data."""

    def test_decode_address_from_data(self):
        """Test decoding address from data section."""
        # 32-byte slot with address in last 20 bytes
        hex_str = "0x000000000000000000000000a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
        result = HexDecoder.decode_address_from_data(hex_str)
        assert result == "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"

    def test_decode_address_from_data_with_offset(self):
        """Test decoding address with offset."""
        # Two addresses
        hex_str = "0x"
        hex_str += "000000000000000000000000a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
        hex_str += "000000000000000000000000dac17f958d2ee523a2206206994597c13d831ec7"
        result1 = HexDecoder.decode_address_from_data(hex_str, offset=0)
        result2 = HexDecoder.decode_address_from_data(hex_str, offset=32)
        assert result1 == "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
        assert result2 == "0xdac17f958d2ee523a2206206994597c13d831ec7"


class TestSplitIntoChunks:
    """Tests for splitting hex strings into chunks."""

    def test_split_into_chunks_default(self):
        """Test splitting with default 64-char chunks."""
        hex_str = "0x" + "a" * 64 + "b" * 64
        result = HexDecoder.split_into_chunks(hex_str)
        assert len(result) == 2
        assert result[0] == "a" * 64
        assert result[1] == "b" * 64

    def test_split_into_chunks_custom_size(self):
        """Test splitting with custom chunk size."""
        hex_str = "0x" + "a" * 32 + "b" * 32
        result = HexDecoder.split_into_chunks(hex_str, chunk_size=32)
        assert len(result) == 2
        assert result[0] == "a" * 32
        assert result[1] == "b" * 32

    def test_split_into_chunks_uneven(self):
        """Test splitting with uneven length."""
        hex_str = "0x" + "a" * 50
        result = HexDecoder.split_into_chunks(hex_str, chunk_size=32)
        assert len(result) == 2
        assert result[0] == "a" * 32
        assert result[1] == "a" * 18


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    def test_empty_string(self):
        """Test handling empty strings."""
        assert HexDecoder.decode_uint256("") == 0
        assert HexDecoder.decode_int256("") == 0
        assert HexDecoder.topic_to_address("") == ""

    def test_invalid_hex(self):
        """Test handling invalid hex (should not crash)."""
        # Python's int() will raise ValueError for invalid hex
        # but our methods should handle empty chunks gracefully
        assert HexDecoder.decode_uint256("0x", offset=100) == 0

    def test_short_data(self):
        """Test handling data shorter than expected."""
        hex_str = "0x00"  # Only 1 byte
        result = HexDecoder.decode_uint256(hex_str)
        # Should not crash, returns what it can decode
        assert isinstance(result, int)


# Property-based tests would go here if using hypothesis library
# Example:
# from hypothesis import given, strategies as st
#
# @given(st.integers(min_value=0, max_value=2**256-1))
# def test_uint256_round_trip(value):
#     hex_str = hex(value)[2:].zfill(64)
#     decoded = HexDecoder.decode_uint256(hex_str, 0)
#     assert decoded == value
