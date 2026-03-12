"""Tests for Jupiter adapter."""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.connectors.jupiter.adapter import JupiterAdapter, SolanaTransactionData
from almanak.framework.connectors.jupiter.client import JupiterConfig
from almanak.framework.connectors.jupiter.models import JupiterQuote, JupiterSwapTransaction
from almanak.framework.data.tokens.exceptions import TokenResolutionError
from almanak.framework.intents.vocabulary import SwapIntent


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
WSOL_MINT = "So11111111111111111111111111111111111111112"


@pytest.fixture
def mock_token_resolver():
    resolver = MagicMock()

    def resolve_side_effect(token, chain):
        tokens = {
            "USDC": MagicMock(address=USDC_MINT, decimals=6, symbol="USDC"),
            "SOL": MagicMock(address=WSOL_MINT, decimals=9, symbol="SOL"),
            "WSOL": MagicMock(address=WSOL_MINT, decimals=9, symbol="WSOL"),
            USDC_MINT: MagicMock(address=USDC_MINT, decimals=6, symbol="USDC"),
            WSOL_MINT: MagicMock(address=WSOL_MINT, decimals=9, symbol="WSOL"),
        }
        if token in tokens:
            return tokens[token]
        raise TokenResolutionError(
            token=token,
            chain=chain,
            reason=f"Unknown token: {token}",
        )

    resolver.resolve.side_effect = resolve_side_effect
    resolver.resolve_for_swap.side_effect = resolve_side_effect
    return resolver


@pytest.fixture
def price_provider():
    return {
        "USDC": Decimal("1"),
        "SOL": Decimal("150"),
        "WSOL": Decimal("150"),
    }


@pytest.fixture
def jupiter_config():
    return JupiterConfig(wallet_address="TestWallet123456789abcdef")


@pytest.fixture
def mock_quote():
    return JupiterQuote(
        input_mint=USDC_MINT,
        output_mint=WSOL_MINT,
        in_amount="100000000",
        out_amount="666666",
        other_amount_threshold="663333",
        price_impact_pct="0.05",
        slippage_bps=50,
        raw_response={
            "inputMint": USDC_MINT,
            "outputMint": WSOL_MINT,
            "inAmount": "100000000",
            "outAmount": "666666",
            "otherAmountThreshold": "663333",
            "priceImpactPct": "0.05",
            "slippageBps": 50,
            "routePlan": [],
        },
    )


@pytest.fixture
def mock_swap_tx(mock_quote):
    return JupiterSwapTransaction(
        swap_transaction="base64_encoded_tx_data",
        last_valid_block_height=280000000,
        priority_fee_lamports=5000,
        quote=mock_quote,
    )


# ---------------------------------------------------------------------------
# SolanaTransactionData tests
# ---------------------------------------------------------------------------


class TestSolanaTransactionData:
    def test_to_dict(self):
        tx = SolanaTransactionData(
            serialized_transaction="base64data",
            tx_type="swap",
            description="Swap USDC -> SOL",
            last_valid_block_height=280000000,
            priority_fee_lamports=5000,
        )
        d = tx.to_dict()
        assert d["serialized_transaction"] == "base64data"
        assert d["chain_family"] == "SOLANA"
        assert d["tx_type"] == "swap"
        assert d["last_valid_block_height"] == 280000000
        assert d["priority_fee_lamports"] == 5000


# ---------------------------------------------------------------------------
# JupiterAdapter tests
# ---------------------------------------------------------------------------


class TestJupiterAdapterInit:
    def test_requires_price_provider(self, jupiter_config, mock_token_resolver):
        with pytest.raises(ValueError, match="requires price_provider"):
            JupiterAdapter(
                config=jupiter_config,
                token_resolver=mock_token_resolver,
            )

    def test_allow_placeholder_prices(self, jupiter_config, mock_token_resolver):
        adapter = JupiterAdapter(
            config=jupiter_config,
            allow_placeholder_prices=True,
            token_resolver=mock_token_resolver,
        )
        assert adapter._using_placeholders is True

    def test_with_price_provider(self, jupiter_config, mock_token_resolver, price_provider):
        adapter = JupiterAdapter(
            config=jupiter_config,
            price_provider=price_provider,
            token_resolver=mock_token_resolver,
        )
        assert adapter._using_placeholders is False


class TestJupiterAdapterResolveToken:
    def test_resolve_symbol(self, jupiter_config, mock_token_resolver, price_provider):
        adapter = JupiterAdapter(
            config=jupiter_config,
            price_provider=price_provider,
            token_resolver=mock_token_resolver,
        )
        assert adapter.resolve_token_address("USDC") == USDC_MINT

    def test_resolve_passthrough_base58(self, jupiter_config, mock_token_resolver, price_provider):
        """Base58 addresses longer than 32 chars are passed through."""
        adapter = JupiterAdapter(
            config=jupiter_config,
            price_provider=price_provider,
            token_resolver=mock_token_resolver,
        )
        long_address = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
        assert adapter.resolve_token_address(long_address) == long_address

    def test_resolve_unknown_token_raises(self, jupiter_config, mock_token_resolver, price_provider):
        adapter = JupiterAdapter(
            config=jupiter_config,
            price_provider=price_provider,
            token_resolver=mock_token_resolver,
        )
        with pytest.raises(TokenResolutionError):
            adapter.resolve_token_address("UNKNOWN")


class TestJupiterAdapterCompileSwap:
    @patch("almanak.framework.connectors.jupiter.adapter.JupiterClient")
    def test_compile_swap_with_amount(
        self,
        mock_client_cls,
        jupiter_config,
        mock_token_resolver,
        price_provider,
        mock_quote,
        mock_swap_tx,
    ):
        # Set up mock client
        mock_client = MagicMock()
        mock_client.get_quote.return_value = mock_quote
        mock_client.get_swap_transaction.return_value = mock_swap_tx
        mock_client_cls.return_value = mock_client

        adapter = JupiterAdapter(
            config=jupiter_config,
            price_provider=price_provider,
            token_resolver=mock_token_resolver,
        )

        intent = SwapIntent(
            from_token="USDC",
            to_token="SOL",
            amount=Decimal("100"),
            max_slippage=Decimal("0.005"),
        )

        bundle = adapter.compile_swap_intent(intent)

        # Verify ActionBundle structure
        assert bundle.intent_type == "SWAP"
        assert len(bundle.transactions) == 1  # No approval on Solana
        assert bundle.transactions[0]["serialized_transaction"] == "base64_encoded_tx_data"
        assert bundle.transactions[0]["chain_family"] == "SOLANA"
        assert bundle.transactions[0]["tx_type"] == "swap"
        assert bundle.metadata["protocol"] == "jupiter"
        assert bundle.metadata["chain"] == "solana"
        assert bundle.metadata["chain_family"] == "SOLANA"
        assert bundle.metadata["input_mint"] == USDC_MINT
        assert bundle.metadata["output_mint"] == WSOL_MINT
        assert bundle.metadata["deferred_swap"] is True

    @patch("almanak.framework.connectors.jupiter.adapter.JupiterClient")
    def test_compile_swap_with_amount_usd(
        self,
        mock_client_cls,
        jupiter_config,
        mock_token_resolver,
        price_provider,
        mock_quote,
        mock_swap_tx,
    ):
        mock_client = MagicMock()
        mock_client.get_quote.return_value = mock_quote
        mock_client.get_swap_transaction.return_value = mock_swap_tx
        mock_client_cls.return_value = mock_client

        adapter = JupiterAdapter(
            config=jupiter_config,
            price_provider=price_provider,
            token_resolver=mock_token_resolver,
        )

        intent = SwapIntent(
            from_token="USDC",
            to_token="SOL",
            amount_usd=Decimal("100"),
            max_slippage=Decimal("0.005"),
        )

        bundle = adapter.compile_swap_intent(intent)

        assert bundle.intent_type == "SWAP"
        assert "error" not in bundle.metadata
        # $100 USDC at $1/USDC = 100 USDC = 100_000_000 smallest units (6 decimals)
        mock_client.get_quote.assert_called_once()
        call_args = mock_client.get_quote.call_args
        assert call_args[1]["amount"] == 100_000_000

    def test_compile_swap_amount_all_zero_balance_returns_error(
        self,
        jupiter_config,
        mock_token_resolver,
        price_provider,
    ):
        adapter = JupiterAdapter(
            config=jupiter_config,
            price_provider=price_provider,
            token_resolver=mock_token_resolver,
        )

        # Mock the RPC call to return zero balance
        adapter._resolve_all_amount = lambda mint: (0, 6)

        intent = SwapIntent(
            from_token="USDC",
            to_token="SOL",
            amount="all",
            max_slippage=Decimal("0.005"),
        )

        bundle = adapter.compile_swap_intent(intent)
        assert "error" in bundle.metadata
        assert "amount='all'" in bundle.metadata["error"]

    def test_compile_swap_amount_all_rpc_failure_returns_error(
        self,
        jupiter_config,
        mock_token_resolver,
        price_provider,
    ):
        adapter = JupiterAdapter(
            config=jupiter_config,
            price_provider=price_provider,
            token_resolver=mock_token_resolver,
        )

        # Mock the RPC call to return None (failure)
        adapter._resolve_all_amount = lambda mint: None

        intent = SwapIntent(
            from_token="USDC",
            to_token="SOL",
            amount="all",
            max_slippage=Decimal("0.005"),
        )

        bundle = adapter.compile_swap_intent(intent)
        assert "error" in bundle.metadata
        assert "amount='all'" in bundle.metadata["error"]

    def test_compile_swap_missing_price_returns_error(
        self,
        jupiter_config,
        mock_token_resolver,
    ):
        adapter = JupiterAdapter(
            config=jupiter_config,
            price_provider={"SOL": Decimal("150")},  # No USDC price
            token_resolver=mock_token_resolver,
        )

        intent = SwapIntent(
            from_token="USDC",
            to_token="SOL",
            amount_usd=Decimal("100"),
            max_slippage=Decimal("0.005"),
        )

        bundle = adapter.compile_swap_intent(intent)
        assert "error" in bundle.metadata
        assert "Price unavailable" in bundle.metadata["error"]


class TestJupiterAdapterGetFreshSwapTransaction:
    @patch("almanak.framework.connectors.jupiter.adapter.JupiterClient")
    def test_get_fresh_swap_transaction(
        self,
        mock_client_cls,
        jupiter_config,
        mock_token_resolver,
        price_provider,
        mock_quote,
        mock_swap_tx,
    ):
        mock_client = MagicMock()
        mock_client.get_quote.return_value = mock_quote
        mock_client.get_swap_transaction.return_value = mock_swap_tx
        mock_client_cls.return_value = mock_client

        adapter = JupiterAdapter(
            config=jupiter_config,
            price_provider=price_provider,
            token_resolver=mock_token_resolver,
        )

        metadata = {
            "from_token": "USDC",
            "to_token": "SOL",
            "route_params": {
                "input_mint": USDC_MINT,
                "output_mint": WSOL_MINT,
                "amount": 100_000_000,
                "slippage_bps": 50,
            },
        }

        fresh = adapter.get_fresh_swap_transaction(metadata)

        assert fresh["serialized_transaction"] == "base64_encoded_tx_data"
        assert fresh["chain_family"] == "SOLANA"
        assert fresh["tx_type"] == "swap"

    def test_get_fresh_swap_transaction_missing_params(
        self,
        jupiter_config,
        mock_token_resolver,
        price_provider,
    ):
        adapter = JupiterAdapter(
            config=jupiter_config,
            price_provider=price_provider,
            token_resolver=mock_token_resolver,
        )

        with pytest.raises(ValueError, match="route_params"):
            adapter.get_fresh_swap_transaction({})

    @patch("almanak.framework.connectors.jupiter.adapter.JupiterClient")
    def test_get_fresh_swap_transaction_passes_priority_fee(
        self,
        mock_client_cls,
        jupiter_config,
        mock_token_resolver,
        price_provider,
        mock_quote,
        mock_swap_tx,
    ):
        """Priority fee params from route_params are forwarded to get_swap_transaction."""
        mock_client = MagicMock()
        mock_client.get_quote.return_value = mock_quote
        mock_client.get_swap_transaction.return_value = mock_swap_tx
        mock_client_cls.return_value = mock_client

        adapter = JupiterAdapter(
            config=jupiter_config,
            price_provider=price_provider,
            token_resolver=mock_token_resolver,
        )

        metadata = {
            "from_token": "USDC",
            "to_token": "SOL",
            "route_params": {
                "input_mint": USDC_MINT,
                "output_mint": WSOL_MINT,
                "amount": 100_000_000,
                "slippage_bps": 50,
                "priority_fee_level": "low",
                "priority_fee_max_lamports": 250_000,
            },
        }

        adapter.get_fresh_swap_transaction(metadata)

        call_kwargs = mock_client.get_swap_transaction.call_args[1]
        assert call_kwargs["priority_fee_level"] == "low"
        assert call_kwargs["priority_fee_max_lamports"] == 250_000


class TestJupiterAdapterPriorityFeeThreading:
    @patch("almanak.framework.connectors.jupiter.adapter.JupiterClient")
    def test_compile_swap_threads_priority_fee(
        self,
        mock_client_cls,
        jupiter_config,
        mock_token_resolver,
        price_provider,
        mock_quote,
        mock_swap_tx,
    ):
        """Priority fee fields from SwapIntent are threaded to get_swap_transaction."""
        mock_client = MagicMock()
        mock_client.get_quote.return_value = mock_quote
        mock_client.get_swap_transaction.return_value = mock_swap_tx
        mock_client_cls.return_value = mock_client

        adapter = JupiterAdapter(
            config=jupiter_config,
            price_provider=price_provider,
            token_resolver=mock_token_resolver,
        )

        intent = SwapIntent(
            from_token="USDC",
            to_token="SOL",
            amount=Decimal("100"),
            max_slippage=Decimal("0.005"),
            priority_fee_level="medium",
            priority_fee_max_lamports=500_000,
        )

        bundle = adapter.compile_swap_intent(intent)

        # Verify threading to client
        call_kwargs = mock_client.get_swap_transaction.call_args[1]
        assert call_kwargs["priority_fee_level"] == "medium"
        assert call_kwargs["priority_fee_max_lamports"] == 500_000

        # Verify stored in route_params for refresh
        route_params = bundle.metadata["route_params"]
        assert route_params["priority_fee_level"] == "medium"
        assert route_params["priority_fee_max_lamports"] == 500_000

    @patch("almanak.framework.connectors.jupiter.adapter.JupiterClient")
    def test_compile_swap_default_priority_fee(
        self,
        mock_client_cls,
        jupiter_config,
        mock_token_resolver,
        price_provider,
        mock_quote,
        mock_swap_tx,
    ):
        """Without priority fee fields, defaults (None) are passed through."""
        mock_client = MagicMock()
        mock_client.get_quote.return_value = mock_quote
        mock_client.get_swap_transaction.return_value = mock_swap_tx
        mock_client_cls.return_value = mock_client

        adapter = JupiterAdapter(
            config=jupiter_config,
            price_provider=price_provider,
            token_resolver=mock_token_resolver,
        )

        intent = SwapIntent(
            from_token="USDC",
            to_token="SOL",
            amount=Decimal("100"),
        )

        adapter.compile_swap_intent(intent)

        call_kwargs = mock_client.get_swap_transaction.call_args[1]
        assert call_kwargs["priority_fee_level"] is None
        assert call_kwargs["priority_fee_max_lamports"] is None
