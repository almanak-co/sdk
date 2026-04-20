"""Tests for FluidAdapter — chain restriction, encumbrance guard, FluidPositionDetails."""

from dataclasses import asdict
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.connectors.fluid.adapter import FluidAdapter, FluidConfig, FluidPositionDetails, TransactionData
from almanak.framework.connectors.fluid.sdk import FluidSDKError


class TestFluidPositionDetails:
    def test_fields(self):
        d = FluidPositionDetails(fluid_nft_id="42", dex_address="0xpool", token0="0xt0", token1="0xt1",
                                 swap_fee_apr=5.2, lending_yield_apr=3.1, combined_apr=8.3)
        assert d.fluid_nft_id == "42"
        assert isinstance(d.fluid_nft_id, str)

    def test_asdict(self):
        d = asdict(FluidPositionDetails(fluid_nft_id="42", dex_address="0xpool", token0="0xt0", token1="0xt1"))
        assert d["fluid_nft_id"] == "42"
        assert d["is_smart_debt"] is False


class TestChainRestriction:
    def test_rejects_non_arbitrum(self):
        with pytest.raises(FluidSDKError, match="Arbitrum only"):
            FluidAdapter(FluidConfig(chain="ethereum", wallet_address="0x" + "a" * 40, rpc_url="https://fake"))

    def test_rejects_base(self):
        with pytest.raises(FluidSDKError, match="Arbitrum only"):
            FluidAdapter(FluidConfig(chain="base", wallet_address="0x" + "a" * 40, rpc_url="https://fake"))


class TestEncumbranceGuard:
    def _make_adapter(self):
        config = FluidConfig(chain="arbitrum", wallet_address="0x" + "a" * 40, rpc_url="https://fake")
        with patch("almanak.framework.connectors.fluid.adapter.FluidSDK") as mock_cls:
            mock_sdk = MagicMock()
            mock_cls.return_value = mock_sdk
            adapter = FluidAdapter(config)
            return adapter, mock_sdk

    def test_blocks_open_phase1(self):
        """LP deposit always raises in phase 1 (Liquidity-layer not yet supported)."""
        adapter, mock_sdk = self._make_adapter()
        from decimal import Decimal
        with pytest.raises(FluidSDKError, match="not yet supported"):
            adapter.build_add_liquidity_transaction(dex_address="0x" + "1" * 40,
                                                    amount0=Decimal("1"), amount1=Decimal("1000"),
                                                    token0_decimals=18, token1_decimals=6)

    def test_allows_close(self):
        """LP close builds operate() transaction."""
        adapter, mock_sdk = self._make_adapter()
        mock_sdk.is_position_encumbered.return_value = False
        mock_sdk.build_operate_tx.return_value = {"to": "0x" + "1" * 40, "data": "0xd5bcb964" + "00" * 128, "value": 0, "gas": 250_000}
        tx = adapter.build_remove_liquidity_transaction(dex_address="0x" + "1" * 40, nft_id=42)
        assert isinstance(tx, TransactionData)
        assert tx.tx_type == "fluid_operate_close"


class TestTransactionData:
    def test_to_dict(self):
        tx = TransactionData(to="0xpool", data="0xabcdef", value=100, gas=350_000, tx_type="fluid_operate_open")
        d = tx.to_dict()
        assert d["gas_estimate"] == 350_000
        assert d["tx_type"] == "fluid_operate_open"
