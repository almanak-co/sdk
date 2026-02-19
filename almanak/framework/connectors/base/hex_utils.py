"""Hex decoding utilities for receipt parsers.

This module provides static utility methods for decoding hex-encoded event data
from transaction logs. Supports all common EVM integer types including signed
and unsigned integers of various sizes.

All methods handle both bytes and string inputs, with or without '0x' prefix.
"""

import logging
from typing import Any

logger = logging.getLogger(__name__)


class HexDecoder:
    """Static utilities for decoding hex-encoded event data.

    This class provides methods for decoding common EVM types from hex strings,
    including:
    - Addresses (20 bytes)
    - Unsigned integers: uint256, uint160, uint128
    - Signed integers: int256, int128, int24
    - Dynamic arrays (for batch events)
    - Raw bytes32 values

    All methods are static and handle both bytes and string inputs.
    """

    @staticmethod
    def normalize_hex(value: Any) -> str:
        """Normalize a hex value to a string without '0x' prefix.

        Args:
            value: Bytes or string value to normalize

        Returns:
            Hex string without '0x' prefix
        """
        if isinstance(value, bytes):
            return value.hex()
        elif isinstance(value, str):
            return value[2:] if value.startswith("0x") else value
        return str(value)

    @staticmethod
    def topic_to_address(topic: Any) -> str:
        """Convert a log topic to an Ethereum address.

        Topics are 32 bytes, but addresses are only 20 bytes. This extracts
        the last 20 bytes as the address.

        Args:
            topic: Topic value (bytes or hex string)

        Returns:
            Lowercase address with '0x' prefix, or empty string if topic is empty

        Example:
            >>> HexDecoder.topic_to_address(
            ...     "0x000000000000000000000000a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
            ... )
            '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
        """
        if isinstance(topic, bytes):
            if not topic:
                return ""
            return "0x" + topic[-20:].hex().lower()
        elif isinstance(topic, str):
            hex_str = HexDecoder.normalize_hex(topic)
            if not hex_str:
                return ""
            return "0x" + hex_str[-40:].lower()
        return ""

    @staticmethod
    def topic_to_bytes32(topic: Any) -> str:
        """Convert a log topic to a bytes32 hex string.

        Args:
            topic: Topic value (bytes or hex string)

        Returns:
            Full 32-byte hex string with '0x' prefix

        Example:
            >>> HexDecoder.topic_to_bytes32(b'\\x00' * 31 + b'\\x01')
            '0x0000000000000000000000000000000000000000000000000000000000000001'
        """
        if isinstance(topic, bytes):
            return "0x" + topic.hex()
        elif isinstance(topic, str):
            hex_str = HexDecoder.normalize_hex(topic)
            return "0x" + hex_str.zfill(64)
        return "0x" + "00" * 32

    @staticmethod
    def decode_uint256(hex_str: str, offset: int = 0) -> int:
        """Decode an unsigned 256-bit integer from hex string.

        Args:
            hex_str: Hex string to decode (with or without '0x')
            offset: Byte offset to start reading from

        Returns:
            Decoded unsigned integer value

        Example:
            >>> HexDecoder.decode_uint256("0x00000000000000000000000000000000000000000000000000000000000003e8")
            1000
        """
        hex_str = HexDecoder.normalize_hex(hex_str)
        chunk = hex_str[offset * 2 : offset * 2 + 64]
        return int(chunk, 16) if chunk else 0

    @staticmethod
    def decode_uint160(hex_str: str, offset: int = 0) -> int:
        """Decode an unsigned 160-bit integer from hex string.

        Used for Uniswap V3 sqrtPriceX96 which is uint160.

        Args:
            hex_str: Hex string to decode (with or without '0x')
            offset: Byte offset to start reading from

        Returns:
            Decoded unsigned integer value
        """
        hex_str = HexDecoder.normalize_hex(hex_str)
        chunk = hex_str[offset * 2 : offset * 2 + 64]
        return int(chunk, 16) if chunk else 0

    @staticmethod
    def decode_uint128(hex_str: str, offset: int = 0) -> int:
        """Decode an unsigned 128-bit integer from hex string.

        Used for Uniswap V3 liquidity which is uint128.

        Args:
            hex_str: Hex string to decode (with or without '0x')
            offset: Byte offset to start reading from

        Returns:
            Decoded unsigned integer value
        """
        hex_str = HexDecoder.normalize_hex(hex_str)
        chunk = hex_str[offset * 2 : offset * 2 + 64]
        return int(chunk, 16) if chunk else 0

    @staticmethod
    def decode_int256(hex_str: str, offset: int = 0) -> int:
        """Decode a signed 256-bit integer from hex string.

        Handles two's complement representation for negative numbers.

        Args:
            hex_str: Hex string to decode (with or without '0x')
            offset: Byte offset to start reading from

        Returns:
            Decoded signed integer value (can be negative)

        Example:
            >>> # Positive value
            >>> HexDecoder.decode_int256("0x00000000000000000000000000000000000000000000000000000000000003e8")
            1000
            >>> # Negative value (two's complement)
            >>> HexDecoder.decode_int256("0xfffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffc18")
            -1000
        """
        hex_str = HexDecoder.normalize_hex(hex_str)
        chunk = hex_str[offset * 2 : offset * 2 + 64]
        if not chunk:
            return 0

        value = int(chunk, 16)
        # Check if negative (most significant bit set)
        if value >= 2**255:
            value -= 2**256
        return value

    @staticmethod
    def decode_int128(hex_str: str, offset: int = 0) -> int:
        """Decode a signed 128-bit integer from hex string.

        Used by Curve for token amounts. Handles two's complement.

        Args:
            hex_str: Hex string to decode (with or without '0x')
            offset: Byte offset to start reading from

        Returns:
            Decoded signed integer value (can be negative)
        """
        hex_str = HexDecoder.normalize_hex(hex_str)
        chunk = hex_str[offset * 2 : offset * 2 + 64]
        if not chunk:
            return 0

        value = int(chunk, 16)
        # int128 is stored in 256-bit slot, check sign bit for int256
        if value >= 2**255:
            value -= 2**256
        return value

    @staticmethod
    def decode_int24(hex_str: str, offset: int = 0) -> int:
        """Decode a signed 24-bit integer from hex string.

        Used for Uniswap V3 ticks. Value is stored in 256-bit slot but
        only uses 24 bits. Handles two's complement.

        Args:
            hex_str: Hex string to decode (with or without '0x')
            offset: Byte offset to start reading from

        Returns:
            Decoded signed integer value (can be negative)

        Example:
            >>> # Positive tick
            >>> HexDecoder.decode_int24("0x0000000000000000000000000000000000000000000000000000000000000064")
            100
            >>> # Negative tick
            >>> HexDecoder.decode_int24("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff9c")
            -100
        """
        hex_str = HexDecoder.normalize_hex(hex_str)
        chunk = hex_str[offset * 2 : offset * 2 + 64]
        if not chunk:
            return 0

        value = int(chunk, 16)
        # Check if stored as negative int256
        if value >= 2**255:
            value -= 2**256
        # Clamp to int24 range [-2^23, 2^23-1]
        if value > 8388607:  # 2^23 - 1
            value = value % (2**24)
            if value >= 2**23:
                value -= 2**24
        elif value < -8388608:  # -2^23
            value = value % (2**24)
            if value >= 2**23:
                value -= 2**24
        return value

    @staticmethod
    def decode_dynamic_array(hex_str: str, offset: int = 0) -> list[int]:
        """Decode a dynamic array from hex string.

        Dynamic arrays in EVM events are encoded as:
        - Offset to array data (32 bytes)
        - Array length (32 bytes)
        - Array elements (32 bytes each)

        Used by TraderJoe V2 for bin IDs and Polymarket for batch transfers.

        Args:
            hex_str: Hex string to decode (with or without '0x')
            offset: Byte offset to start reading array offset

        Returns:
            List of decoded uint256 values

        Example:
            >>> # Array [1, 2, 3] encoded in event data
            >>> data = "0x" + "0" * 64 + "0" * 62 + "03" + "0" * 62 + "01" + "0" * 62 + "02" + "0" * 62 + "03"
            >>> HexDecoder.decode_dynamic_array(data, 0)
            [1, 2, 3]
        """
        hex_str = HexDecoder.normalize_hex(hex_str)

        # Read offset to array data (in bytes)
        array_offset_hex = hex_str[offset * 2 : offset * 2 + 64]
        if not array_offset_hex:
            return []
        array_offset = int(array_offset_hex, 16)

        # Read array length at the offset
        length_hex = hex_str[array_offset * 2 : array_offset * 2 + 64]
        if not length_hex:
            return []
        length = int(length_hex, 16)

        # Read array elements
        result = []
        for i in range(length):
            elem_offset = array_offset + 32 + (i * 32)
            elem_hex = hex_str[elem_offset * 2 : elem_offset * 2 + 64]
            if elem_hex:
                result.append(int(elem_hex, 16))

        return result

    @staticmethod
    def decode_address_from_data(hex_str: str, offset: int = 0) -> str:
        """Decode an address from event data (not indexed topic).

        Unlike indexed addresses which are in topics, non-indexed addresses
        appear in the data section as 32-byte values with leading zeros.

        Args:
            hex_str: Hex string to decode (with or without '0x')
            offset: Byte offset to start reading from

        Returns:
            Lowercase address with '0x' prefix

        Example:
            >>> HexDecoder.decode_address_from_data(
            ...     "0x000000000000000000000000a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
            ... )
            '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
        """
        hex_str = HexDecoder.normalize_hex(hex_str)
        chunk = hex_str[offset * 2 : offset * 2 + 64]
        if not chunk:
            return "0x" + "00" * 20

        # Extract last 20 bytes (40 hex chars)
        address_hex = chunk[-40:]
        return "0x" + address_hex.lower()

    @staticmethod
    def split_into_chunks(hex_str: str, chunk_size: int = 64) -> list[str]:
        """Split hex string into chunks of specified size.

        Useful for parsing event data with multiple parameters.

        Args:
            hex_str: Hex string to split (with or without '0x')
            chunk_size: Size of each chunk in hex characters (default 64 = 32 bytes)

        Returns:
            List of hex chunks

        Example:
            >>> data = "0x" + "0" * 64 + "1" * 64
            >>> chunks = HexDecoder.split_into_chunks(data)
            >>> len(chunks)
            2
        """
        hex_str = HexDecoder.normalize_hex(hex_str)
        return [hex_str[i : i + chunk_size] for i in range(0, len(hex_str), chunk_size)]


__all__ = ["HexDecoder"]
