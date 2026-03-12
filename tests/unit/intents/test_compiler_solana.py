"""Tests for Solana routing in the IntentCompiler."""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.intents.compiler import (
    CompilationStatus,
    IntentCompiler,
    IntentCompilerConfig,
)
from almanak.framework.intents.vocabulary import SwapIntent

# Test config that allows placeholder prices (no real price oracle needed)
TEST_CONFIG = IntentCompilerConfig(allow_placeholder_prices=True)


# ---------------------------------------------------------------------------
# _is_solana_chain() tests
# ---------------------------------------------------------------------------


class TestIsSolanaChain:
    def test_solana_chain(self):
        compiler = IntentCompiler(
            chain="solana", wallet_address="TestWallet123", config=TEST_CONFIG
        )
        assert compiler._is_solana_chain() is True

    def test_solana_chain_uppercase(self):
        compiler = IntentCompiler(
            chain="SOLANA", wallet_address="TestWallet123", config=TEST_CONFIG
        )
        assert compiler._is_solana_chain() is True

    def test_evm_chain_arbitrum(self):
        compiler = IntentCompiler(chain="arbitrum", config=TEST_CONFIG)
        assert compiler._is_solana_chain() is False

    def test_evm_chain_ethereum(self):
        compiler = IntentCompiler(chain="ethereum", config=TEST_CONFIG)
        assert compiler._is_solana_chain() is False

    def test_unknown_chain(self):
        compiler = IntentCompiler(chain="unknown_chain", config=TEST_CONFIG)
        assert compiler._is_solana_chain() is False


# ---------------------------------------------------------------------------
# _compile_swap() routing tests
# ---------------------------------------------------------------------------


class TestCompileSwapSolanaRouting:
    @patch("almanak.framework.intents.compiler.IntentCompiler._compile_jupiter_swap")
    def test_solana_routes_to_jupiter(self, mock_jupiter):
        """Verify that Solana chains route to _compile_jupiter_swap()."""
        mock_jupiter.return_value = MagicMock(
            status=CompilationStatus.SUCCESS,
            action_bundle=MagicMock(),
        )

        compiler = IntentCompiler(
            chain="solana", wallet_address="TestWallet123", config=TEST_CONFIG
        )
        intent = SwapIntent(
            from_token="USDC",
            to_token="SOL",
            amount=Decimal("100"),
            max_slippage=Decimal("0.005"),
        )

        compiler._compile_swap(intent)
        mock_jupiter.assert_called_once_with(intent)

    @patch("almanak.framework.intents.compiler.IntentCompiler._compile_jupiter_swap")
    def test_evm_does_not_route_to_jupiter(self, mock_jupiter):
        """Verify that EVM chains do NOT route to Jupiter."""
        compiler = IntentCompiler(chain="arbitrum", config=TEST_CONFIG)
        intent = SwapIntent(
            from_token="USDC",
            to_token="ETH",
            amount=Decimal("100"),
            max_slippage=Decimal("0.005"),
        )

        # This will fail at token resolution (no RPC), but it should NOT call Jupiter
        compiler._compile_swap(intent)
        mock_jupiter.assert_not_called()


# ---------------------------------------------------------------------------
# _compile_jupiter_swap() integration tests
# ---------------------------------------------------------------------------


class TestCompileJupiterSwap:
    @patch("almanak.framework.connectors.jupiter.adapter.JupiterClient")
    def test_successful_compilation(self, mock_client_cls):
        """Test full Jupiter swap compilation path."""
        from almanak.framework.connectors.jupiter.models import (
            JupiterQuote,
            JupiterSwapTransaction,
        )

        mock_client = MagicMock()
        mock_quote = JupiterQuote(
            input_mint="EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
            output_mint="So11111111111111111111111111111111111111112",
            in_amount="100000000",
            out_amount="666666",
            price_impact_pct="0.05",
            raw_response={"inputMint": "A", "outputMint": "B", "routePlan": []},
        )
        mock_swap_tx = JupiterSwapTransaction(
            swap_transaction="base64_tx_data",
            last_valid_block_height=280000000,
            priority_fee_lamports=5000,
            quote=mock_quote,
        )
        mock_client.get_quote.return_value = mock_quote
        mock_client.get_swap_transaction.return_value = mock_swap_tx
        mock_client_cls.return_value = mock_client

        # Create a mock token resolver
        mock_resolver = MagicMock()
        mock_resolved_usdc = MagicMock(
            address="EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
            decimals=6,
            symbol="USDC",
        )
        mock_resolved_sol = MagicMock(
            address="So11111111111111111111111111111111111111112",
            decimals=9,
            symbol="SOL",
        )
        mock_resolver.resolve_for_swap.side_effect = lambda t, c: {
            "USDC": mock_resolved_usdc,
            "SOL": mock_resolved_sol,
        }[t]
        mock_resolver.resolve.side_effect = lambda t, c: {
            "USDC": mock_resolved_usdc,
            "SOL": mock_resolved_sol,
        }[t]

        compiler = IntentCompiler(
            chain="solana",
            wallet_address="TestWallet123",
            price_oracle={"USDC": Decimal("1"), "SOL": Decimal("150")},
            token_resolver=mock_resolver,
        )

        intent = SwapIntent(
            from_token="USDC",
            to_token="SOL",
            amount=Decimal("100"),
            max_slippage=Decimal("0.005"),
        )

        result = compiler._compile_jupiter_swap(intent)

        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle is not None
        assert result.action_bundle.metadata["protocol"] == "jupiter"
        assert result.action_bundle.metadata["chain_family"] == "SOLANA"
        assert len(result.action_bundle.transactions) == 1
        assert result.action_bundle.transactions[0]["serialized_transaction"] == "base64_tx_data"

    def test_compilation_failure_returns_failed_status(self):
        """Test that compilation errors produce FAILED status, not exceptions."""
        compiler = IntentCompiler(
            chain="solana",
            wallet_address="TestWallet123",
            config=TEST_CONFIG,
        )

        intent = SwapIntent(
            from_token="UNKNOWN_TOKEN",
            to_token="SOL",
            amount=Decimal("100"),
            max_slippage=Decimal("0.005"),
        )

        # This should not raise - it should return a FAILED CompilationResult
        result = compiler._compile_jupiter_swap(intent)
        assert result.status == CompilationStatus.FAILED
        assert result.error is not None
