"""Tests for FluidAdapter — chain validation, pool discovery, swap building."""

from unittest.mock import MagicMock, patch

import pytest

from almanak.connectors.fluid.adapter import FluidAdapter, FluidConfig, TransactionData
from almanak.connectors.fluid.sdk import FLUID_NATIVE_TOKEN, FluidSDKError

POOL = "0x3C0441B42195F4aD6aa9a0978E06096ea616CDa7"
USDC = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
USDT = "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9"
WALLET = "0x" + "a" * 40


def _make_adapter(chain: str = "arbitrum"):
    config = FluidConfig(chain=chain, wallet_address=WALLET, rpc_url="https://fake")
    with patch("almanak.connectors.fluid.adapter.FluidSDK") as mock_cls:
        mock_sdk = MagicMock()
        mock_cls.return_value = mock_sdk
        adapter = FluidAdapter(config, token_resolver=MagicMock())
        return adapter, mock_sdk


class TestChainValidation:
    def test_accepts_all_phase1_chains(self):
        for chain in ("arbitrum", "base", "ethereum", "polygon"):
            adapter, _ = _make_adapter(chain)
            assert adapter.chain == chain

    def test_rejects_unsupported_chain(self):
        with pytest.raises(FluidSDKError, match="not supported"):
            _make_adapter("avalanche")

    def test_config_requires_transport(self):
        with pytest.raises(FluidSDKError, match="rpc_url"):
            FluidConfig(chain="arbitrum", wallet_address=WALLET)


class TestPoolDiscovery:
    def test_find_pool_for_pair_resolves_and_delegates(self):
        adapter, mock_sdk = _make_adapter()
        mock_sdk.find_pool_for_pair.return_value = (POOL, True)
        assert adapter.find_pool_for_pair(USDC, USDT) == (POOL, True)
        mock_sdk.find_pool_for_pair.assert_called_once_with(USDC, USDT)

    def test_get_swap_quote_no_pool_raises(self):
        adapter, mock_sdk = _make_adapter()
        mock_sdk.find_pool_for_pair.return_value = None
        with pytest.raises(FluidSDKError, match="No Fluid pool"):
            adapter.get_swap_quote(USDC, USDT, 1_000_000)

    def test_get_swap_quote_delegates_direction(self):
        adapter, mock_sdk = _make_adapter()
        mock_sdk.find_pool_for_pair.return_value = (POOL, False)
        mock_sdk.get_swap_quote.return_value = 999
        assert adapter.get_swap_quote(USDT, USDC, 1_000_000) == 999
        mock_sdk.get_swap_quote.assert_called_once_with(POOL, False, 1_000_000)


class TestSwapBuilding:
    def test_build_swap_transaction_targets_pool(self):
        adapter, mock_sdk = _make_adapter()
        mock_sdk.find_pool_for_pair.return_value = (POOL, True)
        mock_sdk.build_swap_tx.return_value = {
            "to": POOL,
            "data": "0x2668dfaa" + "00" * 128,
            "value": 0,
            "gas": 250_000,
        }
        tx = adapter.build_swap_transaction(USDC, USDT, 50_000_000, 49_500_000)
        assert isinstance(tx, TransactionData)
        assert tx.to == POOL
        assert tx.tx_type == "swap"
        kwargs = mock_sdk.build_swap_tx.call_args.kwargs
        assert kwargs["to"] == WALLET
        assert kwargs["amount_out_min"] == 49_500_000
        # ERC-20 input: msg.value defaults to 0
        assert kwargs["value"] == 0

    def test_native_input_defaults_value_to_amount_in(self):
        # Fluid pools require msg.value == amountIn for native inputs; an
        # omitted value must default to amount_in, not silently build a
        # reverting value=0 transaction.
        adapter, mock_sdk = _make_adapter()
        mock_sdk.find_pool_for_pair.return_value = (POOL, False)
        mock_sdk.build_swap_tx.return_value = {
            "to": POOL,
            "data": "0x2668dfaa" + "00" * 128,
            "value": 10**16,
            "gas": 250_000,
        }
        adapter.build_swap_transaction(FLUID_NATIVE_TOKEN, USDC, 10**16, 1)
        assert mock_sdk.build_swap_tx.call_args.kwargs["value"] == 10**16

    def test_native_input_value_mismatch_raises(self):
        adapter, mock_sdk = _make_adapter()
        mock_sdk.find_pool_for_pair.return_value = (POOL, False)
        with pytest.raises(FluidSDKError, match="msg.value"):
            adapter.build_swap_transaction(FLUID_NATIVE_TOKEN, USDC, 10**16, 1, value=1)

    def test_erc20_input_nonzero_value_raises(self):
        adapter, mock_sdk = _make_adapter()
        mock_sdk.find_pool_for_pair.return_value = (POOL, True)
        with pytest.raises(FluidSDKError, match="msg.value"):
            adapter.build_swap_transaction(USDC, USDT, 50_000_000, 1, value=10**16)


class TestApprove:
    def test_build_approve_tx_shape(self):
        adapter, _ = _make_adapter()
        tx = adapter.build_approve_tx(USDC, POOL, 50_000_000)
        assert tx.to == USDC
        assert tx.tx_type == "approve"
        assert tx.data.startswith("0x095ea7b3")


class TestTransactionData:
    def test_to_dict(self):
        tx = TransactionData(to="0xpool", data="0xabcdef", value=100, gas=350_000, tx_type="swap")
        d = tx.to_dict()
        assert d["gas_estimate"] == 350_000
        assert d["tx_type"] == "swap"
