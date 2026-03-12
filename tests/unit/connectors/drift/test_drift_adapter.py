"""Unit tests for DriftAdapter — intent compilation to ActionBundles.

All tests use mocked API/RPC responses. No network access required.
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.connectors.drift.adapter import DriftAdapter
from almanak.framework.connectors.drift.constants import PERP_MARKETS
from almanak.framework.connectors.drift.exceptions import DriftMarketError, DriftValidationError
from almanak.framework.connectors.drift.models import DriftConfig, DriftUserAccount
from almanak.framework.intents.vocabulary import PerpCloseIntent, PerpOpenIntent

TEST_WALLET = "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM"


class TestDriftAdapterInit:
    """Test DriftAdapter initialization."""

    def test_init_success(self):
        config = DriftConfig(wallet_address=TEST_WALLET)
        adapter = DriftAdapter(config)
        assert adapter.wallet_address == TEST_WALLET

    def test_init_with_token_resolver(self):
        config = DriftConfig(wallet_address=TEST_WALLET)
        mock_resolver = MagicMock()
        adapter = DriftAdapter(config, token_resolver=mock_resolver)
        assert adapter._token_resolver == mock_resolver


class TestMarketResolution:
    """Test market index resolution."""

    def setup_method(self):
        config = DriftConfig(wallet_address=TEST_WALLET)
        self.adapter = DriftAdapter(config)

    def test_resolve_sol_perp(self):
        assert self.adapter._resolve_market_index("SOL-PERP") == 0

    def test_resolve_btc_perp(self):
        assert self.adapter._resolve_market_index("BTC-PERP") == 1

    def test_resolve_eth_perp(self):
        assert self.adapter._resolve_market_index("ETH-PERP") == 2

    def test_resolve_slash_format(self):
        assert self.adapter._resolve_market_index("SOL/USD") == 0

    def test_resolve_base_asset_only(self):
        assert self.adapter._resolve_market_index("SOL") == 0
        assert self.adapter._resolve_market_index("BTC") == 1

    def test_resolve_numeric_index(self):
        assert self.adapter._resolve_market_index("0") == 0
        assert self.adapter._resolve_market_index("2") == 2

    def test_resolve_case_insensitive(self):
        assert self.adapter._resolve_market_index("sol-perp") == 0
        assert self.adapter._resolve_market_index("Sol/Usd") == 0

    def test_resolve_unknown_market_raises(self):
        with pytest.raises(DriftMarketError, match="Unknown Drift market"):
            self.adapter._resolve_market_index("INVALID-MARKET")


class TestBaseAmountCalculation:
    """Test base_asset_amount calculation."""

    def setup_method(self):
        config = DriftConfig(wallet_address=TEST_WALLET)
        self.adapter = DriftAdapter(config)

    def test_calculate_sol_amount(self):
        # $500 at $150/SOL = 3.33 SOL = 3_333_333_333 in base precision
        amount = self.adapter._calculate_base_amount(
            size_usd=Decimal("500"),
            oracle_price=Decimal("150"),
        )
        assert amount == 3_333_333_333

    def test_calculate_btc_amount(self):
        # $1000 at $50000/BTC = 0.02 BTC = 20_000_000 in base precision
        amount = self.adapter._calculate_base_amount(
            size_usd=Decimal("1000"),
            oracle_price=Decimal("50000"),
        )
        assert amount == 20_000_000

    def test_zero_price_raises(self):
        with pytest.raises(DriftValidationError, match="positive"):
            self.adapter._calculate_base_amount(
                size_usd=Decimal("500"),
                oracle_price=Decimal("0"),
            )


class TestCompilePerpOpen:
    """Test perp open intent compilation."""

    def setup_method(self):
        config = DriftConfig(wallet_address=TEST_WALLET)
        self.adapter = DriftAdapter(config)

    @patch.object(DriftAdapter, "_get_oracle_price", return_value=Decimal("150"))
    def test_compile_market_long(self, mock_price):
        intent = PerpOpenIntent(
            market="SOL-PERP",
            collateral_token="USDC",
            collateral_amount=Decimal("100"),
            size_usd=Decimal("500"),
            is_long=True,
            leverage=Decimal("5"),
            protocol="drift",
        )
        bundle = self.adapter.compile_perp_open_intent(intent)

        assert bundle.intent_type == "PERP_OPEN"
        assert len(bundle.transactions) == 1
        assert bundle.metadata["protocol"] == "drift"
        assert bundle.metadata["chain"] == "solana"
        assert bundle.metadata["chain_family"] == "SOLANA"
        assert bundle.metadata["direction"] == "long"
        assert bundle.metadata["market"] == "SOL-PERP"
        assert bundle.metadata["market_index"] == 0
        assert "error" not in bundle.metadata

    @patch.object(DriftAdapter, "_get_oracle_price", return_value=Decimal("150"))
    def test_compile_market_short(self, mock_price):
        intent = PerpOpenIntent(
            market="SOL-PERP",
            collateral_token="USDC",
            collateral_amount=Decimal("100"),
            size_usd=Decimal("500"),
            is_long=False,
            leverage=Decimal("5"),
            protocol="drift",
        )
        bundle = self.adapter.compile_perp_open_intent(intent)

        assert bundle.metadata["direction"] == "short"
        assert "error" not in bundle.metadata

    @patch.object(DriftAdapter, "_get_oracle_price", return_value=Decimal("50000"))
    def test_compile_btc_perp(self, mock_price):
        intent = PerpOpenIntent(
            market="BTC-PERP",
            collateral_token="USDC",
            collateral_amount=Decimal("200"),
            size_usd=Decimal("1000"),
            is_long=True,
            leverage=Decimal("5"),
            protocol="drift",
        )
        bundle = self.adapter.compile_perp_open_intent(intent)

        assert bundle.metadata["market"] == "BTC-PERP"
        assert bundle.metadata["market_index"] == 1

    def test_compile_unknown_market_returns_error(self):
        intent = PerpOpenIntent(
            market="INVALID",
            collateral_token="USDC",
            collateral_amount=Decimal("100"),
            size_usd=Decimal("500"),
            is_long=True,
            protocol="drift",
        )
        bundle = self.adapter.compile_perp_open_intent(intent)

        assert bundle.transactions == []
        assert "error" in bundle.metadata

    @patch.object(DriftAdapter, "_get_oracle_price", return_value=Decimal("150"))
    def test_transaction_has_solana_format(self, mock_price):
        intent = PerpOpenIntent(
            market="SOL-PERP",
            collateral_token="USDC",
            collateral_amount=Decimal("100"),
            size_usd=Decimal("500"),
            is_long=True,
            protocol="drift",
        )
        bundle = self.adapter.compile_perp_open_intent(intent)

        tx = bundle.transactions[0]
        assert tx["chain_family"] == "SOLANA"
        assert "serialized_transaction" in tx
        assert len(tx["serialized_transaction"]) > 0  # Base64 string


class TestCompilePerpClose:
    """Test perp close intent compilation."""

    def setup_method(self):
        config = DriftConfig(wallet_address=TEST_WALLET)
        self.adapter = DriftAdapter(config)

    @patch.object(DriftAdapter, "_get_oracle_price", return_value=Decimal("150"))
    def test_compile_close_partial(self, mock_price):
        intent = PerpCloseIntent(
            market="SOL-PERP",
            collateral_token="USDC",
            is_long=True,
            size_usd=Decimal("250"),
            protocol="drift",
        )
        bundle = self.adapter.compile_perp_close_intent(intent)

        assert bundle.intent_type == "PERP_CLOSE"
        assert bundle.metadata["protocol"] == "drift"
        assert bundle.metadata["reduce_only"] is True

    def test_compile_close_full_no_rpc_returns_error(self):
        """Closing full position without RPC fails (can't read on-chain position)."""
        intent = PerpCloseIntent(
            market="SOL-PERP",
            collateral_token="USDC",
            is_long=True,
            size_usd=None,  # Full close
            protocol="drift",
        )
        bundle = self.adapter.compile_perp_close_intent(intent)

        # Should error because we can't read position without RPC
        assert "error" in bundle.metadata


class TestErrorBundle:
    """Test error bundle creation."""

    def test_error_bundle_format(self):
        config = DriftConfig(wallet_address=TEST_WALLET)
        adapter = DriftAdapter(config)
        bundle = adapter._error_bundle(
            intent_type=MagicMock(value="PERP_OPEN"),
            intent_id="test-id",
            error="Something went wrong",
        )
        assert bundle.transactions == []
        assert bundle.metadata["error"] == "Something went wrong"
        assert bundle.metadata["protocol"] == "drift"
