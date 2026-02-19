"""Unit tests for the pool validation module.

Tests the validation functions without requiring Anvil or live RPC.
Verifies correct behavior for:
- No RPC URL -> returns exists=None with warning
- Unknown chain/protocol -> returns exists=None with warning
- Correct calldata encoding
- Zero address detection
"""

from unittest.mock import patch

import pytest

from almanak.framework.intents.pool_validation import (
    ZERO_ADDRESS,
    PoolValidationResult,
    _decode_address,
    _encode_get_pool_aerodrome,
    _encode_get_pool_v3,
    validate_aerodrome_pool,
    validate_traderjoe_pool,
    validate_v3_pool,
)


class TestPoolValidationResult:
    """Test PoolValidationResult dataclass."""

    def test_exists_true(self):
        result = PoolValidationResult(exists=True, pool_address="0xabc")
        assert result.exists is True
        assert result.pool_address == "0xabc"
        assert result.error is None
        assert result.warning is None

    def test_exists_false(self):
        result = PoolValidationResult(exists=False, error="Pool not found")
        assert result.exists is False
        assert result.error == "Pool not found"

    def test_exists_none(self):
        result = PoolValidationResult(exists=None, warning="No RPC")
        assert result.exists is None
        assert result.warning == "No RPC"


class TestV3PoolValidation:
    """Test validate_v3_pool function."""

    def test_no_rpc_url_returns_none(self):
        result = validate_v3_pool("arbitrum", "uniswap_v3", "0xabc", "0xdef", 3000, None)
        assert result.exists is None
        assert result.warning is not None
        assert "No RPC URL" in result.warning

    def test_unknown_protocol_returns_none(self):
        result = validate_v3_pool("arbitrum", "unknown_protocol", "0xabc", "0xdef", 3000, "http://localhost:8545")
        assert result.exists is None
        assert result.warning is not None
        assert "Unknown protocol" in result.warning

    def test_unknown_chain_returns_none(self):
        result = validate_v3_pool("unknown_chain", "uniswap_v3", "0xabc", "0xdef", 3000, "http://localhost:8545")
        assert result.exists is None
        assert result.warning is not None
        assert "No uniswap_v3 factory" in result.warning

    @patch("almanak.framework.intents.pool_validation._eth_call")
    def test_zero_address_returns_false(self, mock_eth_call):
        """When factory returns zero address, pool doesn't exist."""
        mock_eth_call.return_value = bytes(32)  # 32 zero bytes = zero address
        result = validate_v3_pool(
            "arbitrum", "uniswap_v3",
            "0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
            "0xaf88d065e77c8cc2239327c5edb3a432268e5831",
            3000, "http://localhost:8545"
        )
        assert result.exists is False
        assert result.error is not None
        assert "No uniswap_v3 pool found" in result.error

    @patch("almanak.framework.intents.pool_validation._eth_call")
    def test_valid_address_returns_true(self, mock_eth_call):
        """When factory returns a valid address, pool exists."""
        # Encode a valid address in 32 bytes (12 zero bytes + 20 address bytes)
        pool_addr = bytes(12) + bytes.fromhex("C31E54c7a869B9FcBEcc14363CF510d1c41fa443")
        mock_eth_call.return_value = pool_addr
        result = validate_v3_pool(
            "arbitrum", "uniswap_v3",
            "0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
            "0xaf88d065e77c8cc2239327c5edb3a432268e5831",
            500, "http://localhost:8545"
        )
        assert result.exists is True
        assert result.pool_address is not None
        assert "c31e54c7" in result.pool_address.lower()

    @patch("almanak.framework.intents.pool_validation._eth_call")
    def test_rpc_failure_returns_none(self, mock_eth_call):
        """When RPC call fails, return unknown."""
        mock_eth_call.return_value = None
        result = validate_v3_pool(
            "arbitrum", "uniswap_v3",
            "0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
            "0xaf88d065e77c8cc2239327c5edb3a432268e5831",
            3000, "http://localhost:8545"
        )
        assert result.exists is None
        assert result.warning is not None
        assert "RPC call" in result.warning


class TestAerodromePoolValidation:
    """Test validate_aerodrome_pool function."""

    def test_no_rpc_url_returns_none(self):
        result = validate_aerodrome_pool("base", "0xabc", "0xdef", False, None)
        assert result.exists is None
        assert "No RPC URL" in result.warning

    def test_unsupported_chain_returns_none(self):
        result = validate_aerodrome_pool("arbitrum", "0xabc", "0xdef", False, "http://localhost:8545")
        assert result.exists is None
        assert "No Aerodrome factory" in result.warning

    @patch("almanak.framework.intents.pool_validation._eth_call")
    def test_zero_address_returns_false(self, mock_eth_call):
        mock_eth_call.return_value = bytes(32)
        result = validate_aerodrome_pool(
            "base",
            "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
            "0x4200000000000000000000000000000000000006",
            False,
            "http://localhost:8545",
        )
        assert result.exists is False
        assert "volatile" in result.error

    @patch("almanak.framework.intents.pool_validation._eth_call")
    def test_valid_address_returns_true(self, mock_eth_call):
        pool_addr = bytes(12) + bytes.fromhex("abcdef1234567890abcdef1234567890abcdef12")
        mock_eth_call.return_value = pool_addr
        result = validate_aerodrome_pool(
            "base",
            "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
            "0x4200000000000000000000000000000000000006",
            False,
            "http://localhost:8545",
        )
        assert result.exists is True
        assert result.pool_address is not None

        # Ensure selector is for getPool(address,address,bool) (not the V3 uint24 overload)
        _, _, calldata = mock_eth_call.call_args.args
        assert calldata.startswith("0x79bc57d5")


class TestTraderJoePoolValidation:
    """Test validate_traderjoe_pool function."""

    def test_no_rpc_url_returns_none(self):
        result = validate_traderjoe_pool("avalanche", "0xabc", "0xdef", 20, None)
        assert result.exists is None
        assert "No RPC URL" in result.warning

    def test_unsupported_chain_returns_none(self):
        result = validate_traderjoe_pool("ethereum", "0xabc", "0xdef", 20, "http://localhost:8545")
        assert result.exists is None
        assert "No TraderJoe V2 factory" in result.warning

    @patch("almanak.framework.intents.pool_validation._eth_call")
    def test_zero_address_returns_false(self, mock_eth_call):
        # getLBPairInformation returns 4 words: binStep, LBPair, createdByOwner, ignoredForRouting
        # Return 128 bytes with zero address in second word
        mock_eth_call.return_value = bytes(128)
        result = validate_traderjoe_pool(
            "avalanche",
            "0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7",
            "0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E",
            20,
            "http://localhost:8545",
        )
        assert result.exists is False
        assert "No TraderJoe V2 pool found" in result.error

        # Ensure selector matches getLBPairInformation(address,address,uint256)
        _, _, calldata = mock_eth_call.call_args.args
        assert calldata.startswith("0x704037bd")

    @patch("almanak.framework.intents.pool_validation._eth_call")
    def test_valid_address_returns_true(self, mock_eth_call):
        # First word: binStep (20), second word: valid LBPair address
        first_word = (20).to_bytes(32, byteorder="big")
        pool_addr_word = bytes(12) + bytes.fromhex("abcdef1234567890abcdef1234567890abcdef12")
        third_word = bytes(32)  # createdByOwner
        fourth_word = bytes(32)  # ignoredForRouting
        mock_eth_call.return_value = first_word + pool_addr_word + third_word + fourth_word
        result = validate_traderjoe_pool(
            "avalanche",
            "0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7",
            "0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E",
            20,
            "http://localhost:8545",
        )
        assert result.exists is True
        assert result.pool_address is not None

        # Ensure selector matches getLBPairInformation(address,address,uint256)
        _, _, calldata = mock_eth_call.call_args.args
        assert calldata.startswith("0x704037bd")


class TestEncodingHelpers:
    """Test calldata encoding helpers."""

    def test_encode_get_pool_v3(self):
        calldata = _encode_get_pool_v3(
            "0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
            "0xaf88d065e77c8cc2239327c5edb3a432268e5831",
            3000,
        )
        # Should start with getPool selector
        assert calldata.startswith("0x1698ee82")
        # Total length: selector (10) + 3 * 64 = 202 chars
        assert len(calldata) == 202

    def test_encode_get_pool_aerodrome_volatile(self):
        calldata = _encode_get_pool_aerodrome("0xabc", "0xdef", False)
        assert calldata.startswith("0x79bc57d5")
        # stable=False -> last 64 chars should be all zeros
        assert calldata.endswith("0" * 64)

    def test_encode_get_pool_aerodrome_stable(self):
        calldata = _encode_get_pool_aerodrome("0xabc", "0xdef", True)
        assert calldata.startswith("0x79bc57d5")
        # stable=True -> last 64 chars should end with 1
        assert calldata[-1] == "1"
        assert calldata[-64:-1] == "0" * 63

    def test_decode_address_valid(self):
        # 12 zero bytes + 20 address bytes
        data = bytes(12) + bytes.fromhex("1234567890abcdef1234567890abcdef12345678")
        addr = _decode_address(data)
        assert addr == "0x1234567890abcdef1234567890abcdef12345678"

    def test_decode_address_zero(self):
        data = bytes(32)
        addr = _decode_address(data)
        assert addr == ZERO_ADDRESS

    def test_decode_address_too_short(self):
        data = bytes(16)
        addr = _decode_address(data)
        assert addr == ZERO_ADDRESS
