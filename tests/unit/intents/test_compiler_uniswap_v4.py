"""Unit tests for Uniswap V4 swap intent compilation.

Tests verify that IntentCompiler correctly compiles SwapIntent for the
uniswap_v4 protocol by delegating to UniswapV4Adapter.compile_swap_intent().
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.intents import SwapIntent
from almanak.framework.intents.compiler import (
    CompilationStatus,
    IntentCompiler,
)

TEST_WALLET = "0x1234567890123456789012345678901234567890"

# Adapter module path for patching lazy imports inside the compiler
ADAPTER_MODULE = "almanak.framework.connectors.uniswap_v4.adapter"


def _make_mock_action_bundle(
    num_txs: int = 3,
    gas_estimate: int = 750_000,
    error: str | None = None,
) -> MagicMock:
    """Create a mock ActionBundle matching adapter.compile_swap_intent() output."""
    bundle = MagicMock()
    bundle.intent_type = "SWAP"
    bundle.transactions = [MagicMock() for _ in range(num_txs)] if not error else []
    bundle.metadata = {
        "intent_id": "test-intent-id",
        "from_token": {"symbol": "USDC", "address": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831", "decimals": 6, "is_native": False},
        "to_token": {"symbol": "WETH", "address": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1", "decimals": 18, "is_native": False},
        "amount_in": "100000000",
        "amount_out_minimum": "99500000",
        "slippage_bps": 50,
        "chain": "arbitrum",
        "router": "0x66a9893cC07D91D95644AEDD05D03f95e1dBA8Af",
        "pool_manager": "0x000000000004444c5dc75cB358380D2e3dE08A90",
        "gas_estimate": gas_estimate,
        "protocol_version": "v4",
    }
    if error:
        bundle.metadata["error"] = error
    return bundle


def _make_compiler(chain: str = "arbitrum", prices: dict | None = None) -> IntentCompiler:
    """Create IntentCompiler for a given chain."""
    if prices is None:
        prices = {"USDC": Decimal("1.0"), "WETH": Decimal("1800.0"), "ETH": Decimal("1800.0")}

    compiler = IntentCompiler(
        chain=chain,
        wallet_address=TEST_WALLET,
        price_oracle=prices,
    )
    return compiler


class TestUniswapV4SwapCompilation:
    """Test V4 swap compilation through the compiler."""

    @patch(f"{ADAPTER_MODULE}.UniswapV4Adapter")
    def test_swap_compiles_successfully(self, mock_adapter_cls):
        """V4 swap intent compiles to SUCCESS with correct ActionBundle."""
        mock_adapter = MagicMock()
        mock_adapter.compile_swap_intent.return_value = _make_mock_action_bundle(num_txs=3)
        mock_adapter_cls.return_value = mock_adapter

        compiler = _make_compiler()
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount=Decimal("100"),
            max_slippage=Decimal("0.005"),
            protocol="uniswap_v4",
            chain="arbitrum",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS, f"Expected SUCCESS, got {result.status}: {result.error}"
        assert result.action_bundle is not None
        assert result.action_bundle.metadata["protocol"] == "uniswap_v4"
        assert result.action_bundle.metadata["protocol_version"] == "v4"
        assert len(result.action_bundle.transactions) == 3
        assert result.transactions is not None
        assert len(result.transactions) == 3

    @patch(f"{ADAPTER_MODULE}.UniswapV4Adapter")
    def test_swap_with_amount_usd(self, mock_adapter_cls):
        """V4 swap intent with amount_usd compiles successfully."""
        mock_adapter = MagicMock()
        mock_adapter.compile_swap_intent.return_value = _make_mock_action_bundle()
        mock_adapter_cls.return_value = mock_adapter

        compiler = _make_compiler()
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount_usd=Decimal("500"),
            max_slippage=Decimal("0.01"),
            protocol="uniswap_v4",
            chain="arbitrum",
        )

        result = compiler.compile(intent)
        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle is not None
        assert len(result.action_bundle.transactions) > 0
        assert result.transactions is not None

    @patch(f"{ADAPTER_MODULE}.UniswapV4Adapter")
    def test_swap_adapter_failure_propagates(self, mock_adapter_cls):
        """When V4 adapter returns error bundle, compilation fails."""
        mock_adapter = MagicMock()
        mock_adapter.compile_swap_intent.return_value = _make_mock_action_bundle(error="Swap failed")
        mock_adapter_cls.return_value = mock_adapter

        compiler = _make_compiler()
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount=Decimal("100"),
            max_slippage=Decimal("0.005"),
            protocol="uniswap_v4",
            chain="arbitrum",
        )

        result = compiler.compile(intent)
        assert result.status == CompilationStatus.FAILED
        assert "failed" in result.error.lower()

    def test_swap_unsupported_chain_fails(self):
        """V4 swap on unsupported chain fails with clear error."""
        compiler = _make_compiler(chain="sonic")
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount=Decimal("100"),
            max_slippage=Decimal("0.005"),
            protocol="uniswap_v4",
            chain="sonic",
        )

        result = compiler.compile(intent)
        assert result.status == CompilationStatus.FAILED
        assert "not supported" in result.error.lower()

    @patch(f"{ADAPTER_MODULE}.UniswapV4Adapter")
    def test_swap_amount_all_rejected(self, mock_adapter_cls):
        """amount='all' must be resolved before compilation."""
        mock_adapter = MagicMock()
        mock_adapter.compile_swap_intent.side_effect = ValueError(
            "amount='all' must be resolved before compilation. "
            "Use Intent.set_resolved_amount() to resolve chained amounts."
        )
        mock_adapter_cls.return_value = mock_adapter

        compiler = _make_compiler()
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount="all",
            max_slippage=Decimal("0.005"),
            protocol="uniswap_v4",
            chain="arbitrum",
        )

        result = compiler.compile(intent)
        assert result.status == CompilationStatus.FAILED
        assert "resolved" in result.error.lower()

    @patch(f"{ADAPTER_MODULE}.UniswapV4Adapter")
    def test_swap_metadata_includes_router_and_pool_manager(self, mock_adapter_cls):
        """Compiled metadata includes canonical V4 contract addresses."""
        mock_adapter = MagicMock()
        mock_adapter.compile_swap_intent.return_value = _make_mock_action_bundle()
        mock_adapter_cls.return_value = mock_adapter

        compiler = _make_compiler()
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount=Decimal("100"),
            max_slippage=Decimal("0.005"),
            protocol="uniswap_v4",
            chain="arbitrum",
        )

        result = compiler.compile(intent)
        assert result.status == CompilationStatus.SUCCESS

        metadata = result.action_bundle.metadata
        assert metadata["router"] == "0x66a9893cC07D91D95644AEDD05D03f95e1dBA8Af"
        assert metadata["pool_manager"] == "0x000000000004444c5dc75cB358380D2e3dE08A90"
        assert metadata["chain"] == "arbitrum"

    @patch(f"{ADAPTER_MODULE}.UniswapV4Adapter")
    def test_swap_gas_estimate_aggregated(self, mock_adapter_cls):
        """Total gas estimate is read from adapter bundle metadata."""
        mock_adapter = MagicMock()
        mock_adapter.compile_swap_intent.return_value = _make_mock_action_bundle(gas_estimate=750_000)
        mock_adapter_cls.return_value = mock_adapter

        compiler = _make_compiler()
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount=Decimal("100"),
            max_slippage=Decimal("0.005"),
            protocol="uniswap_v4",
            chain="arbitrum",
        )

        result = compiler.compile(intent)
        assert result.status == CompilationStatus.SUCCESS
        assert result.total_gas_estimate == 750_000

    @patch(f"{ADAPTER_MODULE}.UniswapV4Adapter")
    def test_swap_metadata_includes_is_native(self, mock_adapter_cls):
        """Token metadata includes is_native field from adapter."""
        mock_adapter = MagicMock()
        mock_adapter.compile_swap_intent.return_value = _make_mock_action_bundle()
        mock_adapter_cls.return_value = mock_adapter

        compiler = _make_compiler()
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount=Decimal("100"),
            max_slippage=Decimal("0.005"),
            protocol="uniswap_v4",
            chain="arbitrum",
        )

        result = compiler.compile(intent)
        assert result.status == CompilationStatus.SUCCESS
        assert "is_native" in result.action_bundle.metadata["from_token"]
        assert "is_native" in result.action_bundle.metadata["to_token"]

    @patch(f"{ADAPTER_MODULE}.UniswapV4Adapter")
    def test_swap_metadata_includes_intent_id(self, mock_adapter_cls):
        """Compiled metadata includes intent_id for tracking."""
        mock_adapter = MagicMock()
        mock_adapter.compile_swap_intent.return_value = _make_mock_action_bundle()
        mock_adapter_cls.return_value = mock_adapter

        compiler = _make_compiler()
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount=Decimal("100"),
            max_slippage=Decimal("0.005"),
            protocol="uniswap_v4",
            chain="arbitrum",
        )

        result = compiler.compile(intent)
        assert result.status == CompilationStatus.SUCCESS
        assert "intent_id" in result.action_bundle.metadata


class TestUniswapV4SwapCompilationMultichain:
    """Test V4 swap compilation across all supported chains."""

    @pytest.mark.parametrize(
        "chain",
        ["ethereum", "arbitrum", "base", "optimism", "polygon", "avalanche", "bsc"],
    )
    @patch(f"{ADAPTER_MODULE}.UniswapV4Adapter")
    def test_swap_compiles_on_all_v4_chains(self, mock_adapter_cls, chain):
        """V4 swap compilation works on all chains with canonical CREATE2 addresses."""
        mock_adapter = MagicMock()
        mock_adapter.compile_swap_intent.return_value = _make_mock_action_bundle()
        mock_adapter_cls.return_value = mock_adapter

        compiler = _make_compiler(chain=chain)
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount=Decimal("100"),
            max_slippage=Decimal("0.005"),
            protocol="uniswap_v4",
            chain=chain,
        )

        result = compiler.compile(intent)
        assert result.status == CompilationStatus.SUCCESS, (
            f"V4 swap compilation failed on {chain}: {result.error}"
        )
        assert result.action_bundle.metadata["pool_manager"] == "0x000000000004444c5dc75cB358380D2e3dE08A90"


class TestUniswapV4NotBlocked:
    """Verify V4 compilation is no longer quarantined."""

    @patch(f"{ADAPTER_MODULE}.UniswapV4Adapter")
    def test_v4_swap_not_blocked(self, mock_adapter_cls):
        """V4 swap compilation no longer returns FAILED with VIB-1965 block."""
        mock_adapter = MagicMock()
        mock_adapter.compile_swap_intent.return_value = _make_mock_action_bundle()
        mock_adapter_cls.return_value = mock_adapter

        compiler = _make_compiler()
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount=Decimal("100"),
            max_slippage=Decimal("0.005"),
            protocol="uniswap_v4",
            chain="arbitrum",
        )

        result = compiler.compile(intent)
        # Must NOT be blocked
        assert result.status != CompilationStatus.FAILED or "blocked" not in (result.error or "").lower()
        assert result.status == CompilationStatus.SUCCESS
