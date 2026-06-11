"""Tests for Fluid error decoding (module-selector + errorId model).

Selector truth table established on-chain at Phase 0 (VIB-5028, report
``docs/internal/qa/fluid-protocol-validation-2026-06-10.md`` §V1.6):
Fluid uses one generic custom error per module wrapping a uint256 errorId.
The previous map labelled ``0xdee51a8a`` "FluidDexSwapTooSmall" — it is
actually ``FluidSafeTransferError``, and that mislabel produced the
VIB-2822 "all pools reject swaps" misdiagnosis.
"""

import pytest

from almanak.connectors.fluid.sdk import (
    DEX_T1_ERROR_IDS,
    FLUID_MODULE_ERROR_SELECTORS,
    FLUID_RESULT_CARRIER_SELECTORS,
    FluidMinAmountError,
    FluidSDKError,
    _extract_revert_hex,
    decode_fluid_revert,
    fluid_error_id,
)


class TestFluidSelectorTables:
    """Verify the selector tables match the on-chain truth table."""

    def test_safe_transfer_error_selector(self):
        # keccak("FluidSafeTransferError(uint256)")[:4] — previously
        # mislabelled "FluidDexSwapTooSmall" (the VIB-2822 root cause).
        assert FLUID_MODULE_ERROR_SELECTORS["dee51a8a"] == "FluidSafeTransferError"

    def test_dex_error_selector_is_generic(self):
        # keccak("FluidDexError(uint256)")[:4] — generic wrapper; the
        # errorId distinguishes the actual failure.
        assert FLUID_MODULE_ERROR_SELECTORS["2fee3e0e"] == "FluidDexError"

    def test_vault_error_selector(self):
        assert FLUID_MODULE_ERROR_SELECTORS["60121cca"] == "FluidVaultError"

    def test_result_carriers_registered(self):
        assert FLUID_RESULT_CARRIER_SELECTORS["b3bfda99"] == "FluidDexSwapResult"
        assert FLUID_RESULT_CARRIER_SELECTORS["1458577f"] == "FluidDexPerfectLiquidityOutput"

    def test_dex_t1_error_ids_cover_observed_cases(self):
        # Observed live at Phase 0:
        assert DEX_T1_ERROR_IDS[51049] == "DexT1__LimitingAmountsSwapAndNonPerfectActions"
        assert DEX_T1_ERROR_IDS[51003] == "DexT1__SmartColNotEnabled"
        assert DEX_T1_ERROR_IDS[51013] == "DexT1__UserSupplyInNotOn"


class TestDecodeFluidRevert:
    """Test the revert decoder."""

    def test_decode_dex_error_with_known_id(self):
        # Observed live: dust swap on the arbitrum USDC/USDT pool.
        raw = "0x2fee3e0e000000000000000000000000000000000000000000000000000000000000c769"
        result = decode_fluid_revert(raw)
        assert "FluidDexError" in result
        assert "51049" in result  # 0xc769
        assert "DexT1__LimitingAmountsSwapAndNonPerfectActions" in result

    def test_decode_dex_error_smart_col_not_enabled(self):
        raw = "0x2fee3e0e000000000000000000000000000000000000000000000000000000000000c73b"
        result = decode_fluid_revert(raw)
        assert "51003" in result
        assert "DexT1__SmartColNotEnabled" in result

    def test_decode_safe_transfer_error(self):
        # The exact revert the old quote shim produced (errorId 71001 was
        # previously misreported as a "pool threshold ... wei").
        raw = "0xdee51a8a0000000000000000000000000000000000000000000000000000000000011559"
        result = decode_fluid_revert(raw)
        assert "FluidSafeTransferError" in result
        assert "71001" in result
        # The old mislabel must be gone:
        assert "TooSmall" not in result
        assert "threshold" not in result.lower()

    def test_decode_vault_error_with_unknown_id(self):
        # Vault errorIds are not in the DexT1 table — render numerically.
        raw = "0x60121cca0000000000000000000000000000000000000000000000000000000000007927"
        result = decode_fluid_revert(raw)
        assert "FluidVaultError" in result
        assert "31015" in result  # 0x7927

    def test_decode_result_carrier_not_a_failure(self):
        raw = "0xb3bfda990000000000000000000000000000000000000000000000000000000002fb9d35"
        result = decode_fluid_revert(raw)
        assert "FluidDexSwapResult" in result
        assert "not a failure" in result

    def test_decode_panic(self):
        raw = "0x4e487b710000000000000000000000000000000000000000000000000000000000000011"
        result = decode_fluid_revert(raw)
        assert "Panic" in result
        assert "0x11" in result

    def test_decode_standard_error_string(self):
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
        assert "FluidSafeTransferError" in result


class TestFluidErrorId:
    """Test errorId extraction."""

    def test_extracts_id_from_module_error(self):
        raw = "0x2fee3e0e000000000000000000000000000000000000000000000000000000000000c769"
        assert fluid_error_id(raw) == 51049

    def test_none_for_panic(self):
        raw = "0x4e487b710000000000000000000000000000000000000000000000000000000000000011"
        assert fluid_error_id(raw) is None

    def test_none_for_carrier(self):
        raw = "0xb3bfda990000000000000000000000000000000000000000000000000000000002fb9d35"
        assert fluid_error_id(raw) is None

    def test_none_for_short_data(self):
        assert fluid_error_id("0x2fee3e0e") is None


class TestExtractRevertHex:
    """Test hex extraction from Web3 errors."""

    def test_extract_from_error_message_text(self):
        error = Exception(
            "execution reverted: 0xdee51a8a0000000000000000000000000000000000000000000000000000000000011559"
        )
        result = _extract_revert_hex(error)
        assert result is not None
        assert result.startswith("0x")
        assert "dee51a8a" in result

    def test_extract_from_data_attribute(self):
        error = Exception("execution reverted")
        error.data = "0x2fee3e0e000000000000000000000000000000000000000000000000000000000000c769"
        assert _extract_revert_hex(error) == error.data

    def test_extract_from_args_dict(self):
        error = Exception({"message": "execution reverted", "data": "0x4e487b71" + "00" * 31 + "11"})
        result = _extract_revert_hex(error)
        assert result is not None
        assert result.startswith("0x4e487b71")

    def test_no_hex_returns_none(self):
        assert _extract_revert_hex(Exception("some other error without hex data")) is None


class TestFluidMinAmountError:
    """Test the error class hierarchy."""

    def test_is_fluid_sdk_error(self):
        err = FluidMinAmountError("limit-gated")
        assert isinstance(err, FluidSDKError)
        assert isinstance(err, Exception)
