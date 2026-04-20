"""Unit tests for Compound V3 intent compilation paths.

Tests verify that IntentCompiler correctly compiles SupplyIntent, WithdrawIntent,
BorrowIntent, and RepayIntent for the compound_v3 protocol.
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.intents import BorrowIntent, RepayIntent, SupplyIntent, WithdrawIntent
from almanak.framework.intents.compiler import (
    CompilationStatus,
    IntentCompiler,
    IntentCompilerConfig,
)

# Patch targets - these are lazy-imported inside compile methods
ADAPTER_MODULE = "almanak.framework.connectors.compound_v3.adapter"
ADAPTER_CLS = f"{ADAPTER_MODULE}.CompoundV3Adapter"
CONFIG_CLS = f"{ADAPTER_MODULE}.CompoundV3Config"
COMET_ADDRESSES = f"{ADAPTER_MODULE}.COMPOUND_V3_COMET_ADDRESSES"

TEST_WALLET = "0x1234567890123456789012345678901234567890"
TEST_COMET = "0xc3d688B66703497DAA19211EEdff47f25384cdc3"

MOCK_CHAIN_ADDRESSES = {
    "ethereum": {
        "usdc": {
            "comet_address": TEST_COMET,
            "name": "USDC",
            "base_token": "USDC",
            "base_token_address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            "collaterals": {},
        },
    },
    "arbitrum": {
        "usdc": {
            "comet_address": TEST_COMET,
            "name": "USDC",
            "base_token": "USDC",
            "base_token_address": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            "collaterals": {},
        },
    },
}


def _mock_tx_result(description: str, gas: int = 150_000) -> MagicMock:
    """Create a mock successful TransactionResult."""
    result = MagicMock()
    result.success = True
    result.error = None
    result.tx_data = {
        "to": TEST_COMET,
        "value": 0,
        "data": "0xabcdef",
    }
    result.gas_estimate = gas
    result.description = description
    return result


def _mock_failed_result(error: str) -> MagicMock:
    """Create a mock failed TransactionResult."""
    result = MagicMock()
    result.success = False
    result.error = error
    result.tx_data = None
    result.gas_estimate = 0
    result.description = None
    return result


@pytest.fixture
def compiler():
    """Create an IntentCompiler for Ethereum with placeholder prices."""
    config = IntentCompilerConfig(allow_placeholder_prices=True)
    return IntentCompiler(chain="ethereum", config=config)


@pytest.fixture
def arbitrum_compiler():
    """Create an IntentCompiler for Arbitrum with placeholder prices."""
    config = IntentCompilerConfig(allow_placeholder_prices=True)
    return IntentCompiler(chain="arbitrum", config=config)


# =============================================================================
# SUPPLY
# =============================================================================

class TestCompoundV3Supply:
    """Test _compile_supply for compound_v3 protocol."""

    @patch(COMET_ADDRESSES, MOCK_CHAIN_ADDRESSES)
    @patch(CONFIG_CLS)
    @patch(ADAPTER_CLS)
    def test_supply_success(self, mock_adapter_cls, mock_config_cls, compiler):
        mock_adapter = MagicMock()
        mock_adapter.comet_address = TEST_COMET
        mock_adapter.supply.return_value = _mock_tx_result("Supply 100 USDC to Compound V3")
        mock_adapter_cls.return_value = mock_adapter

        intent = SupplyIntent(
            token="USDC",
            amount=Decimal("100"),
            protocol="compound_v3",
            market_id="usdc",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle is not None
        assert result.action_bundle.metadata["protocol"] == "compound_v3"
        assert result.action_bundle.metadata["market"] == "usdc"
        assert len(result.transactions) >= 2  # approve + supply

    @patch(COMET_ADDRESSES, {"ethereum": {"usdc": {"comet_address": TEST_COMET}}})
    def test_supply_unsupported_chain(self):
        config = IntentCompilerConfig(allow_placeholder_prices=True)
        bsc_compiler = IntentCompiler(chain="bsc", config=config)

        intent = SupplyIntent(
            token="USDC",
            amount=Decimal("100"),
            protocol="compound_v3",
        )

        result = bsc_compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "not available on chain" in result.error

    @patch(COMET_ADDRESSES, {"ethereum": {"usdc": {"comet_address": TEST_COMET}}})
    def test_supply_unsupported_market(self, compiler):
        intent = SupplyIntent(
            token="USDC",
            amount=Decimal("100"),
            protocol="compound_v3",
            market_id="nonexistent",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "not available" in result.error
        assert "nonexistent" in result.error

    @patch(COMET_ADDRESSES, MOCK_CHAIN_ADDRESSES)
    @patch(CONFIG_CLS)
    @patch(ADAPTER_CLS)
    def test_supply_defaults_to_usdc_market(self, mock_adapter_cls, mock_config_cls, compiler):
        mock_adapter = MagicMock()
        mock_adapter.comet_address = TEST_COMET
        mock_adapter.supply.return_value = _mock_tx_result("Supply USDC")
        mock_adapter_cls.return_value = mock_adapter

        intent = SupplyIntent(
            token="USDC",
            amount=Decimal("100"),
            protocol="compound_v3",
            # No market_id -> defaults to "usdc"
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS
        mock_config_cls.assert_called_once_with(
            chain="ethereum",
            wallet_address=compiler.wallet_address,
            market="usdc",
        )

    @patch(COMET_ADDRESSES, MOCK_CHAIN_ADDRESSES)
    @patch(CONFIG_CLS)
    @patch(ADAPTER_CLS)
    def test_supply_adapter_failure(self, mock_adapter_cls, mock_config_cls, compiler):
        mock_adapter = MagicMock()
        mock_adapter.comet_address = TEST_COMET
        mock_adapter.supply.return_value = _mock_failed_result("Insufficient balance")
        mock_adapter_cls.return_value = mock_adapter

        intent = SupplyIntent(
            token="USDC",
            amount=Decimal("100"),
            protocol="compound_v3",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "Compound V3 supply failed" in result.error


# =============================================================================
# WITHDRAW
# =============================================================================

class TestCompoundV3Withdraw:
    """Test _compile_withdraw for compound_v3 protocol."""

    @patch(COMET_ADDRESSES, MOCK_CHAIN_ADDRESSES)
    @patch(CONFIG_CLS)
    @patch(ADAPTER_CLS)
    def test_withdraw_success(self, mock_adapter_cls, mock_config_cls, compiler):
        mock_adapter = MagicMock()
        mock_adapter.comet_address = TEST_COMET
        mock_adapter.withdraw.return_value = _mock_tx_result("Withdraw 50 USDC from Compound V3")
        mock_adapter_cls.return_value = mock_adapter

        intent = WithdrawIntent(
            token="USDC",
            amount=Decimal("50"),
            protocol="compound_v3",
            market_id="usdc",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle is not None
        assert result.action_bundle.metadata["protocol"] == "compound_v3"
        assert result.action_bundle.metadata["market"] == "usdc"
        assert len(result.transactions) >= 1

    @patch(COMET_ADDRESSES, MOCK_CHAIN_ADDRESSES)
    @patch(CONFIG_CLS)
    @patch(ADAPTER_CLS)
    def test_withdraw_adapter_failure(self, mock_adapter_cls, mock_config_cls, compiler):
        mock_adapter = MagicMock()
        mock_adapter.comet_address = TEST_COMET
        mock_adapter.withdraw.return_value = _mock_failed_result("No balance to withdraw")
        mock_adapter_cls.return_value = mock_adapter

        intent = WithdrawIntent(
            token="USDC",
            amount=Decimal("50"),
            protocol="compound_v3",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "Compound V3 withdraw failed" in result.error


# =============================================================================
# BORROW
# =============================================================================

class TestCompoundV3Borrow:
    """Test _compile_borrow for compound_v3 protocol."""

    @patch(COMET_ADDRESSES, MOCK_CHAIN_ADDRESSES)
    @patch(CONFIG_CLS)
    @patch(ADAPTER_CLS)
    def test_borrow_success(self, mock_adapter_cls, mock_config_cls, compiler):
        mock_adapter = MagicMock()
        mock_adapter.comet_address = TEST_COMET
        mock_adapter.supply_collateral.return_value = _mock_tx_result("Supply collateral WETH")
        mock_adapter.borrow.return_value = _mock_tx_result("Borrow 500 USDC from Compound V3")
        mock_adapter_cls.return_value = mock_adapter

        intent = BorrowIntent(
            collateral_token="WETH",
            collateral_amount=Decimal("1"),
            borrow_token="USDC",
            borrow_amount=Decimal("500"),
            protocol="compound_v3",
            market_id="usdc",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle is not None
        assert result.action_bundle.metadata["protocol"] == "compound_v3"
        assert result.action_bundle.metadata["market"] == "usdc"

    @patch(COMET_ADDRESSES, MOCK_CHAIN_ADDRESSES)
    @patch(CONFIG_CLS)
    @patch(ADAPTER_CLS)
    def test_borrow_with_collateral(self, mock_adapter_cls, mock_config_cls, compiler):
        """Test borrow with collateral_token triggers supply_collateral first."""
        mock_adapter = MagicMock()
        mock_adapter.comet_address = TEST_COMET
        mock_adapter.supply_collateral.return_value = _mock_tx_result("Supply collateral WETH")
        mock_adapter.borrow.return_value = _mock_tx_result("Borrow 500 USDC")
        mock_adapter_cls.return_value = mock_adapter

        intent = BorrowIntent(
            collateral_token="WETH",
            collateral_amount=Decimal("1"),
            borrow_token="USDC",
            borrow_amount=Decimal("500"),
            protocol="compound_v3",
            market_id="usdc",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS
        # Should have: approve collateral + supply_collateral + borrow = 3+ txs
        assert len(result.transactions) >= 2

    @patch(COMET_ADDRESSES, MOCK_CHAIN_ADDRESSES)
    @patch(CONFIG_CLS)
    @patch(ADAPTER_CLS)
    def test_borrow_adapter_failure(self, mock_adapter_cls, mock_config_cls, compiler):
        mock_adapter = MagicMock()
        mock_adapter.comet_address = TEST_COMET
        mock_adapter.supply_collateral.return_value = _mock_tx_result("Supply collateral WETH")
        mock_adapter.borrow.return_value = _mock_failed_result("Insufficient collateral")
        mock_adapter_cls.return_value = mock_adapter

        intent = BorrowIntent(
            collateral_token="WETH",
            collateral_amount=Decimal("1"),
            borrow_token="USDC",
            borrow_amount=Decimal("500"),
            protocol="compound_v3",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "Compound V3 borrow failed" in result.error

    @patch(COMET_ADDRESSES, {"ethereum": {"usdc": {"comet_address": TEST_COMET}}})
    def test_borrow_unsupported_chain(self):
        """Borrow on unsupported chain fails (token resolution or chain check)."""
        config = IntentCompilerConfig(allow_placeholder_prices=True)
        polygon_compiler = IntentCompiler(chain="polygon", config=config)

        intent = BorrowIntent(
            collateral_token="WETH",
            collateral_amount=Decimal("1"),
            borrow_token="USDC",
            borrow_amount=Decimal("500"),
            protocol="compound_v3",
        )

        result = polygon_compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "not available on chain" in result.error


# =============================================================================
# REPAY
# =============================================================================

class TestCompoundV3Repay:
    """Test _compile_repay for compound_v3 protocol."""

    @patch(COMET_ADDRESSES, MOCK_CHAIN_ADDRESSES)
    @patch(CONFIG_CLS)
    @patch(ADAPTER_CLS)
    def test_repay_success(self, mock_adapter_cls, mock_config_cls, compiler):
        mock_adapter = MagicMock()
        mock_adapter.comet_address = TEST_COMET
        mock_adapter.repay.return_value = _mock_tx_result("Repay 200 USDC to Compound V3")
        mock_adapter_cls.return_value = mock_adapter

        intent = RepayIntent(
            token="USDC",
            amount=Decimal("200"),
            protocol="compound_v3",
            market_id="usdc",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle is not None
        assert result.action_bundle.metadata["protocol"] == "compound_v3"
        assert result.action_bundle.metadata["market"] == "usdc"
        assert result.action_bundle.metadata["repay_full"] is False
        # approve + repay
        assert len(result.transactions) >= 2

    @patch(COMET_ADDRESSES, MOCK_CHAIN_ADDRESSES)
    @patch(CONFIG_CLS)
    @patch(ADAPTER_CLS)
    def test_repay_full_debt(self, mock_adapter_cls, mock_config_cls, compiler):
        mock_adapter = MagicMock()
        mock_adapter.comet_address = TEST_COMET
        mock_adapter.repay.return_value = _mock_tx_result("Repay full debt to Compound V3")
        mock_adapter_cls.return_value = mock_adapter

        intent = RepayIntent(
            token="USDC",
            amount=Decimal("0"),
            protocol="compound_v3",
            market_id="usdc",
            repay_full=True,
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle is not None
        assert result.action_bundle.metadata["repay_full"] is True
        # Verify adapter was called with repay_all=True
        mock_adapter.repay.assert_called_once()
        call_kwargs = mock_adapter.repay.call_args.kwargs
        assert call_kwargs.get("repay_all") is True

    @patch(COMET_ADDRESSES, MOCK_CHAIN_ADDRESSES)
    @patch(CONFIG_CLS)
    @patch(ADAPTER_CLS)
    def test_repay_adapter_failure(self, mock_adapter_cls, mock_config_cls, compiler):
        mock_adapter = MagicMock()
        mock_adapter.comet_address = TEST_COMET
        mock_adapter.repay.return_value = _mock_failed_result("No outstanding debt")
        mock_adapter_cls.return_value = mock_adapter

        intent = RepayIntent(
            token="USDC",
            amount=Decimal("200"),
            protocol="compound_v3",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "Compound V3 repay failed" in result.error

    @patch(COMET_ADDRESSES, {"ethereum": {"usdc": {"comet_address": TEST_COMET}}})
    def test_repay_unsupported_chain(self):
        config = IntentCompilerConfig(allow_placeholder_prices=True)
        bsc_compiler = IntentCompiler(chain="bsc", config=config)

        intent = RepayIntent(
            token="USDC",
            amount=Decimal("200"),
            protocol="compound_v3",
        )

        result = bsc_compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "not available on chain" in result.error

    @patch(COMET_ADDRESSES, {"ethereum": {"usdc": {"comet_address": TEST_COMET}}})
    def test_repay_unsupported_market(self, compiler):
        intent = RepayIntent(
            token="USDC",
            amount=Decimal("200"),
            protocol="compound_v3",
            market_id="nonexistent",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "not available" in result.error


# =============================================================================
# UNSUPPORTED PROTOCOL ERROR MESSAGE
# =============================================================================

class TestUnsupportedProtocolErrors:
    """Verify error messages include compound_v3 in supported list."""

    def test_supply_error_mentions_compound_v3(self, compiler):
        intent = SupplyIntent(
            token="USDC",
            amount=Decimal("100"),
            protocol="nonexistent_protocol",
        )
        result = compiler.compile(intent)
        assert result.status == CompilationStatus.FAILED
        assert "compound_v3" in result.error

    def test_withdraw_error_mentions_compound_v3(self, compiler):
        intent = WithdrawIntent(
            token="USDC",
            amount=Decimal("100"),
            protocol="nonexistent_protocol",
        )
        result = compiler.compile(intent)
        assert result.status == CompilationStatus.FAILED
        assert "compound_v3" in result.error

    def test_borrow_error_mentions_compound_v3(self, compiler):
        intent = BorrowIntent(
            collateral_token="WETH",
            collateral_amount=Decimal("1"),
            borrow_token="USDC",
            borrow_amount=Decimal("100"),
            protocol="nonexistent_protocol",
        )
        result = compiler.compile(intent)
        assert result.status == CompilationStatus.FAILED
        assert "compound_v3" in result.error

    def test_repay_error_mentions_compound_v3(self, compiler):
        intent = RepayIntent(
            token="USDC",
            amount=Decimal("100"),
            protocol="nonexistent_protocol",
        )
        result = compiler.compile(intent)
        assert result.status == CompilationStatus.FAILED
        assert "compound_v3" in result.error
