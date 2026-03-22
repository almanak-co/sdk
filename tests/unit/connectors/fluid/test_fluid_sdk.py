"""Tests for FluidSDK — addresses, chain validation, debt guard."""

import pytest

from almanak.framework.connectors.fluid.sdk import DEFAULT_GAS_ESTIMATES, FLUID_ADDRESSES, DexPoolData, FluidSDKError


class TestFluidAddresses:
    def test_arbitrum_exists(self):
        assert "arbitrum" in FLUID_ADDRESSES
        arb = FLUID_ADDRESSES["arbitrum"]
        for key in ("dex_factory", "dex_resolver", "dex_reserves_resolver", "liquidity_resolver", "vault_resolver"):
            assert key in arb

    def test_no_ethereum_yet(self):
        assert "ethereum" not in FLUID_ADDRESSES


class TestDexPoolData:
    def test_construction(self):
        data = DexPoolData(dex_address="0x" + "1" * 40, token0="0x" + "a" * 40, token1="0x" + "b" * 40,
                           fee_bps=100, is_smart_collateral=False, is_smart_debt=False)
        assert data.fee_bps == 100
        assert not data.is_smart_debt


class TestGasEstimates:
    def test_all_present(self):
        for key in ("approve", "operate_open", "operate_close"):
            assert key in DEFAULT_GAS_ESTIMATES
            assert DEFAULT_GAS_ESTIMATES[key] > 20_000


class TestChainValidation:
    def test_unsupported_chain(self):
        from unittest.mock import patch
        with patch("almanak.framework.connectors.fluid.sdk.Web3"):
            with pytest.raises(FluidSDKError, match="not supported"):
                from almanak.framework.connectors.fluid.sdk import FluidSDK
                FluidSDK(chain="polygon", rpc_url="https://fake")


class TestDebtGuard:
    def test_nonzero_debt_raises(self):
        from unittest.mock import MagicMock, patch
        with patch("almanak.framework.connectors.fluid.sdk.Web3") as mock_web3_cls:
            mock_w3 = MagicMock()
            mock_web3_cls.return_value = mock_w3
            mock_web3_cls.HTTPProvider = MagicMock()
            mock_web3_cls.to_checksum_address = lambda x: x
            from almanak.framework.connectors.fluid.sdk import FluidSDK
            with patch.dict(FLUID_ADDRESSES, {"testchain": FLUID_ADDRESSES["arbitrum"]}):
                sdk = FluidSDK(chain="testchain", rpc_url="https://fake")
                with pytest.raises(FluidSDKError, match="smart-debt"):
                    sdk.build_operate_tx(dex_address="0x" + "1" * 40, nft_id=0, new_col=1000, new_debt=500, to="0x" + "a" * 40)
