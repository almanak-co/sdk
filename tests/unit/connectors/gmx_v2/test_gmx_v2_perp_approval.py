"""Tests for GMX V2 PERP_OPEN ERC-20 approval prepend (VIB-131).

Verifies that the compiler prepends an ERC-20 approval TX for the collateral
token when compiling PERP_OPEN intents. The Router contract (not ExchangeRouter)
calls transferFrom() via pluginTransfer(), so the wallet must approve Router.

Native tokens (WETH/ETH/WAVAX/AVAX) are sent as msg.value via sendWnt() and
do NOT require approval.
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.intents.compiler import IntentCompiler


def _make_mock_compiler(chain: str = "arbitrum") -> IntentCompiler:
    """Create a compiler with minimal mocking for PERP_OPEN testing."""
    compiler = IntentCompiler.__new__(IntentCompiler)
    compiler.chain = chain
    compiler.wallet_address = "0x" + "1" * 40
    compiler.rpc_url = "http://localhost:8545"
    compiler._approve_cache = {}
    return compiler


def _make_perp_open_intent(
    collateral_token: str = "USDC",
    collateral_amount: Decimal = Decimal("100"),
    market: str = "ETH/USD",
    size_usd: Decimal = Decimal("1000"),
    is_long: bool = False,
):
    """Create a mock PerpOpenIntent."""
    from almanak.framework.intents.vocabulary import PerpOpenIntent

    return PerpOpenIntent(
        market=market,
        collateral_token=collateral_token,
        collateral_amount=collateral_amount,
        size_usd=size_usd,
        is_long=is_long,
        leverage=Decimal("10"),
        protocol="gmx_v2",
    )


class TestPerpOpenApproval:
    """Tests that PERP_OPEN correctly prepends approval for ERC-20 collateral."""

    @patch("almanak.framework.connectors.gmx_v2.sdk.Web3")
    def test_usdc_collateral_gets_approval(self, mock_web3_cls):
        """SHORT with USDC collateral should prepend an approve TX."""
        # Setup mocks
        mock_web3 = MagicMock()
        mock_web3_cls.return_value = mock_web3
        mock_web3.eth.gas_price = 100_000_000  # 0.1 gwei

        compiler = _make_mock_compiler()

        # Mock _build_approve_tx to track calls
        approve_calls = []

        def mock_build_approve(token_address, spender, amount):
            approve_calls.append({
                "token_address": token_address,
                "spender": spender,
                "amount": amount,
            })
            # Return a mock approve TX
            from almanak.framework.intents.compiler import TransactionData

            return [TransactionData(
                to=token_address,
                value=0,
                data="0x095ea7b3" + "0" * 128,  # approve selector
                gas_estimate=46000,
                description=f"Approve {token_address}",
                tx_type="approve",
            )]

        compiler._build_approve_tx = mock_build_approve

        # Mock _get_chain_rpc_url
        compiler._get_chain_rpc_url = lambda: "http://localhost:8545"

        # Create intent
        intent = _make_perp_open_intent(
            collateral_token="USDC",
            collateral_amount=Decimal("100"),
            is_long=False,
        )

        # Mock adapter
        mock_adapter_result = MagicMock()
        mock_adapter_result.success = True
        mock_adapter_result.order_key = "0xabc123"

        # Mock SDK
        mock_sdk = MagicMock()
        mock_sdk.EXCHANGE_ROUTER_ADDRESS = "0x1C3fa76e6E1088bCE750f23a5BFcffa1efEF6A41"
        mock_sdk.ROUTER_ADDRESS = "0x7452c558d45f8afC8c83dAe62C3f8A5BE19c71f6"
        mock_sdk.get_execution_fee.return_value = 100000000000000  # 0.0001 ETH
        mock_tx_data = MagicMock()
        mock_tx_data.to = mock_sdk.EXCHANGE_ROUTER_ADDRESS
        mock_tx_data.value = 100000000000000
        mock_tx_data.data = "0xmulticall"
        mock_tx_data.gas_estimate = 500000
        mock_sdk.build_increase_order_multicall.return_value = mock_tx_data

        with (
            patch("almanak.framework.connectors.GMXv2Adapter") as mock_adapter_cls,
            patch("almanak.framework.connectors.GMXv2Config"),
            patch("almanak.framework.connectors.gmx_v2.GMXV2SDK", return_value=mock_sdk),
            patch("almanak.framework.connectors.gmx_v2.GMX_V2_MARKETS", {
                "arbitrum": {"ETH/USD": "0xmarket"},
            }),
            patch("almanak.framework.connectors.gmx_v2.GMX_V2_TOKENS", {
                "arbitrum": {"USDC": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"},
            }),
        ):
            mock_adapter_cls.return_value.open_position.return_value = mock_adapter_result

            result = compiler._compile_perp_open(intent)

        # Should succeed
        assert result.status.value == "SUCCESS", f"Compilation failed: {result.error}"

        # Should have called _build_approve_tx for USDC targeting the Router
        # (Router calls transferFrom via pluginTransfer, not ExchangeRouter)
        assert len(approve_calls) == 1, "Should prepend one approval TX for USDC"
        assert approve_calls[0]["spender"] == mock_sdk.ROUTER_ADDRESS
        assert approve_calls[0]["token_address"] == "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
        # 100 USDC = 100 * 10^6 = 100_000_000 wei
        assert approve_calls[0]["amount"] == 100_000_000

        # Should have 2 TXs: approve + multicall
        assert len(result.transactions) == 2
        assert result.transactions[0].tx_type == "approve"
        assert result.transactions[1].tx_type == "perp_open"

    @patch("almanak.framework.connectors.gmx_v2.sdk.Web3")
    def test_weth_collateral_skips_approval(self, mock_web3_cls):
        """LONG with WETH collateral should NOT prepend an approve TX."""
        mock_web3 = MagicMock()
        mock_web3_cls.return_value = mock_web3
        mock_web3.eth.gas_price = 100_000_000

        compiler = _make_mock_compiler()

        approve_calls = []

        def mock_build_approve(token_address, spender, amount):
            approve_calls.append(True)
            return []

        compiler._build_approve_tx = mock_build_approve
        compiler._get_chain_rpc_url = lambda: "http://localhost:8545"

        intent = _make_perp_open_intent(
            collateral_token="WETH",
            collateral_amount=Decimal("0.5"),
            is_long=True,
        )

        mock_adapter_result = MagicMock()
        mock_adapter_result.success = True
        mock_adapter_result.order_key = "0xabc123"

        mock_sdk = MagicMock()
        mock_sdk.EXCHANGE_ROUTER_ADDRESS = "0x1C3fa76e6E1088bCE750f23a5BFcffa1efEF6A41"
        mock_sdk.get_execution_fee.return_value = 100000000000000
        mock_tx_data = MagicMock()
        mock_tx_data.to = mock_sdk.EXCHANGE_ROUTER_ADDRESS
        mock_tx_data.value = 600000000000000000  # 0.5 ETH + exec fee
        mock_tx_data.data = "0xmulticall"
        mock_tx_data.gas_estimate = 500000
        mock_sdk.build_increase_order_multicall.return_value = mock_tx_data

        with (
            patch("almanak.framework.connectors.GMXv2Adapter") as mock_adapter_cls,
            patch("almanak.framework.connectors.GMXv2Config"),
            patch("almanak.framework.connectors.gmx_v2.GMXV2SDK", return_value=mock_sdk),
            patch("almanak.framework.connectors.gmx_v2.GMX_V2_MARKETS", {
                "arbitrum": {"ETH/USD": "0xmarket"},
            }),
            patch("almanak.framework.connectors.gmx_v2.GMX_V2_TOKENS", {
                "arbitrum": {"WETH": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"},
            }),
        ):
            mock_adapter_cls.return_value.open_position.return_value = mock_adapter_result

            result = compiler._compile_perp_open(intent)

        assert result.status.value == "SUCCESS", f"Compilation failed: {result.error}"

        # Should NOT have called _build_approve_tx
        assert len(approve_calls) == 0, "WETH collateral should NOT need approval"

        # Should have 1 TX: just the multicall (no approve)
        assert len(result.transactions) == 1
        assert result.transactions[0].tx_type == "perp_open"
