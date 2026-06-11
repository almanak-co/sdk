"""Tests for FluidSDK — addresses, chain validation, pool discovery, quoting."""

from unittest.mock import MagicMock

import pytest
from web3 import Web3

from almanak.connectors.fluid.sdk import (
    DEFAULT_GAS_ESTIMATES,
    FLUID_ADDRESSES,
    FLUID_NATIVE_TOKEN,
    DexPoolData,
    FluidMinAmountError,
    FluidSDK,
    FluidSDKError,
)

POOL = "0x3C0441B42195F4aD6aa9a0978E06096ea616CDa7"
USDC = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
USDT = "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9"
WALLET = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"


def _sdk() -> FluidSDK:
    """SDK against a dead RPC — only offline/mocked paths are exercised."""
    return FluidSDK(chain="arbitrum", rpc_url="http://127.0.0.1:1")


class TestFluidAddresses:
    def test_all_phase1_chains_present(self):
        # Phase-1 chain set (VIB-5029); addresses identical per chain
        # (Fluid deterministic deploys, verified Phase 0/1 on-chain).
        for chain in ("arbitrum", "base", "ethereum", "polygon"):
            entry = FLUID_ADDRESSES[chain]
            assert entry["dex_factory"] == "0x91716C4EDA1Fb55e84Bf8b4c7085f84285c19085"
            assert entry["dex_reserves_resolver"] == "0x05Bd8269A20C472b148246De20E6852091BF16Ff"

    def test_unsupported_chain_rejected(self):
        with pytest.raises(FluidSDKError, match="not supported"):
            FluidSDK(chain="avalanche", rpc_url="https://fake")

    def test_requires_transport(self):
        with pytest.raises(FluidSDKError, match="rpc_url"):
            FluidSDK(chain="arbitrum")


class TestDexPoolData:
    def test_construction(self):
        data = DexPoolData(
            dex_address="0x" + "1" * 40,
            token0="0x" + "a" * 40,
            token1="0x" + "b" * 40,
            fee_raw=100,
        )
        assert data.fee_raw == 100
        assert not data.is_smart_debt


class TestGasEstimates:
    def test_all_present(self):
        for key in ("approve", "swap"):
            assert key in DEFAULT_GAS_ESTIMATES
            assert DEFAULT_GAS_ESTIMATES[key] > 20_000


class TestFindPoolForPair:
    def _sdk_with_pools(self, pools: list[DexPoolData]) -> FluidSDK:
        sdk = _sdk()
        sdk.get_all_pools = MagicMock(return_value=pools)
        return sdk

    def test_forward_direction(self):
        sdk = self._sdk_with_pools([DexPoolData(dex_address=POOL, token0=USDC, token1=USDT)])
        assert sdk.find_pool_for_pair(USDC, USDT) == (POOL, True)

    def test_reverse_direction(self):
        sdk = self._sdk_with_pools([DexPoolData(dex_address=POOL, token0=USDC, token1=USDT)])
        assert sdk.find_pool_for_pair(USDT, USDC) == (POOL, False)

    def test_case_insensitive(self):
        sdk = self._sdk_with_pools([DexPoolData(dex_address=POOL, token0=USDC, token1=USDT)])
        assert sdk.find_pool_for_pair(USDC.lower(), USDT.upper().replace("0X", "0x")) == (POOL, True)

    def test_native_leg(self):
        sdk = self._sdk_with_pools([DexPoolData(dex_address=POOL, token0=USDC, token1=FLUID_NATIVE_TOKEN)])
        assert sdk.find_pool_for_pair(FLUID_NATIVE_TOKEN, USDC) == (POOL, False)

    def test_no_pool_returns_none(self):
        sdk = self._sdk_with_pools([DexPoolData(dex_address=POOL, token0=USDC, token1=USDT)])
        assert sdk.find_pool_for_pair(USDC, "0x" + "9" * 40) is None

    def test_find_dex_by_tokens_back_compat(self):
        sdk = self._sdk_with_pools([DexPoolData(dex_address=POOL, token0=USDC, token1=USDT)])
        assert sdk.find_dex_by_tokens(USDT, USDC) == POOL


class TestGetSwapQuote:
    def _sdk_with_estimate(self, side_effect=None, return_value=None) -> FluidSDK:
        sdk = _sdk()
        fn = MagicMock()
        if side_effect is not None:
            fn.call.side_effect = side_effect
        else:
            fn.call.return_value = return_value
        sdk._reserves_resolver = MagicMock()
        sdk._reserves_resolver.functions.estimateSwapIn.return_value = fn
        return sdk

    def test_returns_resolver_quote(self):
        sdk = self._sdk_with_estimate(return_value=50_037_813)
        assert sdk.get_swap_quote(POOL, True, 50_000_000) == 50_037_813

    def test_zero_quote_is_limit_gated(self):
        # The resolver returns 0 (not a revert) beyond current limits —
        # observed live at Phase 0 for a $50M quote.
        sdk = self._sdk_with_estimate(return_value=0)
        with pytest.raises(FluidMinAmountError, match="zero quote"):
            sdk.get_swap_quote(POOL, True, 50_000_000_000_000)

    def test_limit_gated_error_id_maps_to_min_amount_error(self):
        err = Exception(
            "execution reverted: custom error "
            "0x2fee3e0e000000000000000000000000000000000000000000000000000000000000c769"
        )
        sdk = self._sdk_with_estimate(side_effect=err)
        with pytest.raises(FluidMinAmountError, match="retry later"):
            sdk.get_swap_quote(POOL, True, 1)

    def test_limit_id_from_other_module_is_not_retryable(self):
        # errorId 51049 is limit-gated ONLY as a DexT1 (FluidDexError) id;
        # modules number errorIds independently, so the same number under
        # FluidVaultError (0x60121cca) must stay a hard FluidSDKError.
        err = Exception(
            "execution reverted: custom error "
            "0x60121cca000000000000000000000000000000000000000000000000000000000000c769"
        )
        sdk = self._sdk_with_estimate(side_effect=err)
        with pytest.raises(FluidSDKError) as exc_info:
            sdk.get_swap_quote(POOL, True, 1)
        assert not isinstance(exc_info.value, FluidMinAmountError)

    def test_other_revert_is_sdk_error(self):
        err = Exception(
            "execution reverted: custom error "
            "0x2fee3e0e000000000000000000000000000000000000000000000000000000000000c73b"
        )
        sdk = self._sdk_with_estimate(side_effect=err)
        with pytest.raises(FluidSDKError, match="SmartColNotEnabled"):
            sdk.get_swap_quote(POOL, True, 1)

    def test_non_revert_failure_is_sdk_error(self):
        sdk = self._sdk_with_estimate(side_effect=ConnectionError("rpc down"))
        with pytest.raises(FluidSDKError, match="Failed to get swap quote"):
            sdk.get_swap_quote(POOL, True, 1)


class TestSwapCalldata:
    def test_encode_swap_in_calldata_selector_and_shape(self):
        sdk = _sdk()
        data = sdk.encode_swap_in_calldata(True, 50_000_000, 49_500_000, WALLET)
        # keccak("swapIn(bool,uint256,uint256,address)")[:4]
        assert data.startswith("0x2668dfaa")
        # 4 static words after the selector
        assert len(data) == 2 + 8 + 4 * 64

    def test_encode_decodes_back(self):
        sdk = _sdk()
        data = sdk.encode_swap_in_calldata(False, 123, 45, WALLET)
        body = data[10:]
        words = [body[i : i + 64] for i in range(0, len(body), 64)]
        assert int(words[0], 16) == 0  # swap0to1=False
        assert int(words[1], 16) == 123
        assert int(words[2], 16) == 45
        assert Web3.to_checksum_address("0x" + words[3][24:]) == WALLET

    def test_build_swap_tx_shape(self):
        sdk = _sdk()
        tx = sdk.build_swap_tx(
            dex_address=POOL,
            swap0to1=True,
            amount_in=50_000_000,
            amount_out_min=49_500_000,
            to=WALLET,
        )
        assert tx["to"] == POOL
        assert tx["value"] == 0
        assert tx["gas"] == DEFAULT_GAS_ESTIMATES["swap"]
        assert tx["data"].startswith("0x2668dfaa")

    def test_build_swap_tx_native_value(self):
        sdk = _sdk()
        tx = sdk.build_swap_tx(
            dex_address=POOL,
            swap0to1=False,
            amount_in=10**18,
            amount_out_min=1,
            to=WALLET,
            value=10**18,
        )
        assert tx["value"] == 10**18
