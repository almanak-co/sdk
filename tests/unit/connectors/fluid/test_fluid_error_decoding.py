"""Tests for Fluid DEX error decoding and min-amount handling.

Validates VIB-1798: raw on-chain revert errors are decoded into
human-readable messages for better diagnostics.
"""

import pytest

from almanak.framework.connectors.fluid.sdk import (
    FLUID_ERROR_SELECTORS,
    FluidMinAmountError,
    FluidSDKError,
    _extract_revert_hex,
    decode_fluid_revert,
)


class TestFluidErrorSelectors:
    """Verify known error selectors are registered."""

    def test_swap_too_small_registered(self):
        assert "dee51a8a" in FLUID_ERROR_SELECTORS
        assert FLUID_ERROR_SELECTORS["dee51a8a"] == "FluidDexSwapTooSmall"

    def test_liquidity_limit_registered(self):
        """VIB-1702: 0x2fee3e0e error from wstETH/ETH and weETH/ETH pools."""
        assert "2fee3e0e" in FLUID_ERROR_SELECTORS
        assert FLUID_ERROR_SELECTORS["2fee3e0e"] == "FluidDexLiquidityLimit"

    def test_panic_registered(self):
        assert "4e487b71" in FLUID_ERROR_SELECTORS


class TestDecodeFluidRevert:
    """Test the revert decoder."""

    def test_decode_swap_too_small_with_param(self):
        # Real revert from nightly: 0xdee51a8a + uint256(0x11559 = 71001)
        raw = "0xdee51a8a0000000000000000000000000000000000000000000000000000000000011559"
        result = decode_fluid_revert(raw)
        assert "FluidDexSwapTooSmall" in result
        assert "71001" in result  # 0x11559 = 71001
        assert "insufficient liquidity" in result.lower()

    def test_decode_swap_too_small_without_param(self):
        raw = "0xdee51a8a"
        result = decode_fluid_revert(raw)
        assert "insufficient liquidity" in result.lower()

    def test_decode_swap_too_small_suggests_smaller_size(self):
        """VIB-1702: error should suggest reducing trade size or using another protocol."""
        raw = "0xdee51a8a0000000000000000000000000000000000000000000000000000000000011559"
        result = decode_fluid_revert(raw)
        assert "smaller trade size" in result.lower() or "different protocol" in result.lower()

    def test_decode_swap_too_small_no_contradictory_claim(self):
        """VIB-1969: error must NOT claim amount is below minimum when it isn't."""
        raw = "0xdee51a8a0000000000000000000000000000000000000000000000000000000000011559"
        result = decode_fluid_revert(raw)
        # Must not say "minimum input" — the parameter may be the output threshold
        assert "minimum input" not in result.lower()
        # Must include the raw threshold for debugging
        assert "71001" in result

    def test_decode_liquidity_limit_with_param(self):
        """VIB-1702: 0x2fee3e0e error from wstETH/ETH pools."""
        raw = "0x2fee3e0e000000000000000000000000000000000000000000000000000000000000c769"
        result = decode_fluid_revert(raw)
        assert "FluidDexLiquidityLimit" in result
        assert "51049" in result  # 0xc769 = 51049
        assert "capacity exceeded" in result.lower()

    def test_decode_liquidity_limit_without_param(self):
        raw = "0x2fee3e0e"
        result = decode_fluid_revert(raw)
        assert "capacity exceeded" in result.lower()

    def test_decode_liquidity_limit_suggests_smaller_size(self):
        """VIB-1702: liquidity limit error should suggest alternatives."""
        raw = "0x2fee3e0e000000000000000000000000000000000000000000000000000000000000c769"
        result = decode_fluid_revert(raw)
        assert "smaller trade size" in result.lower() or "different protocol" in result.lower()

    def test_decode_panic(self):
        raw = "0x4e487b710000000000000000000000000000000000000000000000000000000000000011"
        result = decode_fluid_revert(raw)
        assert "Panic" in result
        assert "0x11" in result

    def test_decode_standard_error_string(self):
        """Standard Solidity Error(string) reverts are decoded to the human-readable message."""
        # Error(string) selector = 0x08c379a0, message = "hello"
        raw = (
            "0x08c379a0"
            "0000000000000000000000000000000000000000000000000000000000000020"  # offset=32
            "0000000000000000000000000000000000000000000000000000000000000005"  # length=5
            "68656c6c6f000000000000000000000000000000000000000000000000000000"  # "hello"
        )
        result = decode_fluid_revert(raw)
        assert result == "hello"

    def test_decode_unknown_selector(self):
        raw = "0xaabbccdd0000000000000000000000000000000000000000000000000000000000000001"
        result = decode_fluid_revert(raw)
        assert "Unknown revert" in result
        assert "aabbccdd" in result

    def test_decode_too_short(self):
        raw = "0xaabb"
        result = decode_fluid_revert(raw)
        assert "Unknown revert" in result

    def test_decode_no_prefix(self):
        raw = "dee51a8a0000000000000000000000000000000000000000000000000000000000011559"
        result = decode_fluid_revert(raw)
        assert "FluidDexSwapTooSmall" in result


class TestExtractRevertHex:
    """Test hex extraction from error strings."""

    def test_extract_from_web3_error(self):
        error = "execution reverted: 0xdee51a8a0000000000000000000000000000000000000000000000000000000000011559"
        result = _extract_revert_hex(error)
        assert result is not None
        assert result.startswith("0x")
        assert "dee51a8a" in result

    def test_extract_bare_hex(self):
        error = "0xdee51a8a00000000"
        result = _extract_revert_hex(error)
        assert result == "0xdee51a8a00000000"

    def test_no_hex_returns_none(self):
        error = "some other error without hex data"
        result = _extract_revert_hex(error)
        assert result is None

    def test_short_hex_ignored(self):
        # Less than 8 hex chars after 0x — not a selector
        error = "error 0xaabb"
        result = _extract_revert_hex(error)
        assert result is None


class TestFluidMinAmountError:
    """Test the error class hierarchy."""

    def test_is_fluid_sdk_error(self):
        err = FluidMinAmountError("too small")
        assert isinstance(err, FluidSDKError)
        assert isinstance(err, Exception)

    def test_message(self):
        err = FluidMinAmountError("FluidDexSwapTooSmall: the pool rejected this swap as too small")
        assert "too small" in str(err)
