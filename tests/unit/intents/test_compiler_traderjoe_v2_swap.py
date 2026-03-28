"""Unit tests for TraderJoe V2 swap intent compilation (VIB-1928).

Tests verify that IntentCompiler correctly compiles SwapIntent for the
traderjoe_v2 protocol using the dedicated _compile_swap_traderjoe_v2() path
instead of the DefaultSwapAdapter.
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.intents import SwapIntent
from almanak.framework.intents.compiler import (
    CompilationStatus,
    IntentCompiler,
    IntentCompilerConfig,
)

# Patch targets
TJ_ADAPTER_MODULE = "almanak.framework.connectors.traderjoe_v2"
TJ_ADAPTER_CLS = f"{TJ_ADAPTER_MODULE}.TraderJoeV2Adapter"
TJ_CONFIG_CLS = f"{TJ_ADAPTER_MODULE}.TraderJoeV2Config"
TJ_SDK_MODULE = "almanak.framework.connectors.traderjoe_v2.sdk"
TJ_ADDRESSES_MODULE = "almanak.core.contracts"

TEST_WALLET = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"


def _make_compiler(chain: str = "avalanche") -> IntentCompiler:
    return IntentCompiler(
        chain=chain,
        wallet_address=TEST_WALLET,
        config=IntentCompilerConfig(allow_placeholder_prices=True),
    )


class TestTraderJoeV2SwapRouting:
    """Verify SwapIntent(protocol='traderjoe_v2') routes to dedicated path."""

    def test_traderjoe_v2_swap_no_longer_blocked(self):
        """SwapIntent with protocol='traderjoe_v2' must NOT return VIB-1406 error."""
        compiler = _make_compiler()
        intent = SwapIntent(
            from_token="WAVAX",
            to_token="USDC",
            amount=Decimal("1.0"),
            max_slippage=Decimal("0.01"),
            protocol="traderjoe_v2",
            chain="avalanche",
        )

        result = compiler.compile(intent)

        # VIB-1928: must NOT fail with VIB-1406 guard error
        if result.status == CompilationStatus.FAILED:
            assert "VIB-1406" not in (result.error or ""), (
                "TraderJoe V2 swap still blocked by VIB-1406 guard!"
            )
            assert "not yet supported" not in (result.error or ""), (
                "TraderJoe V2 swap still returns 'not yet supported' error!"
            )
        # If compilation succeeds (with placeholder prices + local RPC), verify bundle
        if result.status == CompilationStatus.SUCCESS:
            assert result.action_bundle is not None
            assert result.action_bundle.metadata["protocol"] == "traderjoe_v2"

    def test_unsupported_chain_fails_with_message(self):
        """SwapIntent on a chain without TJ V2 must fail with helpful error."""
        compiler = _make_compiler(chain="optimism")
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount=Decimal("100"),
            max_slippage=Decimal("0.01"),
            protocol="traderjoe_v2",
            chain="optimism",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert result.error is not None
        assert "not supported" in result.error.lower(), (
            f"Expected chain-not-supported error, got: {result.error}"
        )


class TestTraderJoeV2SwapCompilation:
    """Test the full compilation path with mocked adapter."""

    @patch(f"{TJ_SDK_MODULE}.PoolNotFoundError", new=Exception)
    @patch(f"almanak.framework.intents.compiler.IntentCompiler._get_chain_rpc_url")
    @patch(f"almanak.framework.intents.compiler.IntentCompiler._build_approve_tx")
    @patch("almanak.framework.intents.pool_validation.validate_traderjoe_pool")
    @patch(f"{TJ_ADAPTER_CLS}")
    @patch(f"{TJ_CONFIG_CLS}")
    def test_swap_compiles_with_mocked_adapter(
        self,
        mock_config_cls,
        mock_adapter_cls,
        mock_validate_pool,
        mock_build_approve,
        mock_get_rpc,
    ):
        """Full compilation with mocked adapter returns SUCCESS."""
        from almanak.framework.intents.compiler import TransactionData
        from almanak.framework.intents.pool_validation import PoolValidationResult

        # Setup mocks
        mock_get_rpc.return_value = "http://localhost:8545"
        mock_validate_pool.return_value = PoolValidationResult(exists=True, pool_address="0x1234")

        # Mock approve TX
        approve_tx = TransactionData(
            to="0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E",
            value=0,
            data="0xapprove",
            gas_estimate=50000,
            description="Approve USDC",
            tx_type="approve",
        )
        mock_build_approve.return_value = [approve_tx]

        # Mock adapter
        mock_adapter = MagicMock()
        mock_adapter_cls.return_value = mock_adapter

        # Mock build_swap_transaction to return adapter's TransactionData
        from almanak.framework.connectors.traderjoe_v2.adapter import TransactionData as TJTransactionData

        mock_swap_tx = TJTransactionData(
            to="0xb4315e873dBcf96Ffd0acd8EA43f689D8c20fB30",
            data="0xswapdata",
            value=0,
            gas=200000,
            chain_id=43114,
        )
        mock_adapter.build_swap_transaction.return_value = mock_swap_tx

        # Mock pool auto-detection
        mock_adapter.sdk.get_pool_address.return_value = "0xpool"

        compiler = _make_compiler()
        intent = SwapIntent(
            from_token="USDC",
            to_token="WAVAX",
            amount=Decimal("100"),
            max_slippage=Decimal("0.01"),
            protocol="traderjoe_v2",
            chain="avalanche",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS, (
            f"TraderJoe V2 swap compilation failed: {result.error}"
        )
        assert result.action_bundle is not None
        assert len(result.transactions) == 2  # approve + swap
        assert result.transactions[0].tx_type == "approve"
        assert result.transactions[1].tx_type == "traderjoe_v2_swap"

        # Verify adapter was called correctly
        mock_adapter.build_swap_transaction.assert_called_once()
        call_kwargs = mock_adapter.build_swap_transaction.call_args
        assert call_kwargs.kwargs.get("bin_step") == 20 or call_kwargs[1].get("bin_step") == 20

    @patch(f"{TJ_SDK_MODULE}.PoolNotFoundError", new=Exception)
    @patch(f"almanak.framework.intents.compiler.IntentCompiler._get_chain_rpc_url")
    @patch(f"{TJ_ADAPTER_CLS}")
    @patch(f"{TJ_CONFIG_CLS}")
    def test_swap_no_pool_found_fails_gracefully(
        self,
        mock_config_cls,
        mock_adapter_cls,
        mock_get_rpc,
    ):
        """When no pool exists for the pair, compilation fails with helpful error."""
        mock_get_rpc.return_value = "http://localhost:8545"

        # Mock adapter - all pool lookups fail
        mock_adapter = MagicMock()
        mock_adapter_cls.return_value = mock_adapter
        mock_adapter.sdk.get_pool_address.side_effect = Exception("Pool not found")

        compiler = _make_compiler()
        intent = SwapIntent(
            from_token="USDC",
            to_token="WAVAX",
            amount=Decimal("100"),
            max_slippage=Decimal("0.01"),
            protocol="traderjoe_v2",
            chain="avalanche",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "No TraderJoe V2 pool found" in (result.error or "")

    def test_amount_all_rejected(self):
        """amount='all' must be rejected before compilation."""
        compiler = _make_compiler()
        intent = SwapIntent(
            from_token="USDC",
            to_token="WAVAX",
            amount="all",
            max_slippage=Decimal("0.01"),
            protocol="traderjoe_v2",
            chain="avalanche",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "all" in (result.error or "").lower()


class TestTraderJoeV2SwapMetadata:
    """Test ActionBundle metadata for TJ V2 swaps."""

    @patch(f"{TJ_SDK_MODULE}.PoolNotFoundError", new=Exception)
    @patch(f"almanak.framework.intents.compiler.IntentCompiler._get_chain_rpc_url")
    @patch(f"almanak.framework.intents.compiler.IntentCompiler._build_approve_tx")
    @patch("almanak.framework.intents.pool_validation.validate_traderjoe_pool")
    @patch(f"{TJ_ADAPTER_CLS}")
    @patch(f"{TJ_CONFIG_CLS}")
    def test_metadata_contains_protocol_and_bin_step(
        self,
        mock_config_cls,
        mock_adapter_cls,
        mock_validate_pool,
        mock_build_approve,
        mock_get_rpc,
    ):
        """ActionBundle metadata must include protocol and bin_step."""
        from almanak.framework.connectors.traderjoe_v2.adapter import TransactionData as TJTransactionData
        from almanak.framework.intents.pool_validation import PoolValidationResult

        mock_get_rpc.return_value = "http://localhost:8545"
        mock_validate_pool.return_value = PoolValidationResult(exists=True, pool_address="0x1234")
        mock_build_approve.return_value = []

        mock_adapter = MagicMock()
        mock_adapter_cls.return_value = mock_adapter
        mock_adapter.sdk.get_pool_address.return_value = "0xpool"
        mock_adapter.build_swap_transaction.return_value = TJTransactionData(
            to="0xrouter", data="0xdata", value=0, gas=200000, chain_id=43114,
        )

        compiler = _make_compiler()
        intent = SwapIntent(
            from_token="WAVAX",
            to_token="USDC",
            amount=Decimal("1.0"),
            max_slippage=Decimal("0.01"),
            protocol="traderjoe_v2",
            chain="avalanche",
        )

        result = compiler.compile(intent)
        assert result.status == CompilationStatus.SUCCESS

        metadata = result.action_bundle.metadata
        assert metadata["protocol"] == "traderjoe_v2"
        assert metadata["bin_step"] == 20  # auto-detected default
        assert metadata["chain"] == "avalanche"
        assert "router" in metadata
