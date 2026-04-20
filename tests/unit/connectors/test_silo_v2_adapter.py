"""Unit tests for Silo V2 adapter — MAX_UINT256 fallback paths."""

from decimal import Decimal
from unittest.mock import patch

import pytest

from almanak.framework.connectors.silo_v2.adapter import (
    MAX_UINT256,
    SILO_V2_FUNCTION_SELECTORS,
    SiloV2Adapter,
    SiloV2Config,
)


@pytest.fixture
def adapter():
    """Create a Silo V2 adapter with default config."""
    config = SiloV2Config(
        chain="avalanche",
        wallet_address="0x1234567890123456789012345678901234567890",
    )
    return SiloV2Adapter(config)


class TestSiloV2WithdrawAll:
    """Test withdraw_all MAX_UINT256 fallback path."""

    @patch("almanak.framework.data.tokens.get_token_resolver")
    def test_withdraw_all_zero_amount_uses_redeem_max(self, mock_resolver, adapter):
        """withdraw_all=True with amount=0 uses redeem(MAX_UINT256)."""
        mock_resolver.return_value.get_decimals.return_value = 6
        result = adapter.withdraw(asset="USDC", amount=Decimal("0"), withdraw_all=True)
        assert result.success is True
        assert result.tx_data is not None
        # Must use redeem selector, not withdraw selector
        assert result.tx_data["data"].startswith(SILO_V2_FUNCTION_SELECTORS["redeem"])
        # MAX_UINT256 encoded as first parameter
        max_hex = f"{MAX_UINT256:064x}"
        assert max_hex in result.tx_data["data"]

    @patch("almanak.framework.data.tokens.get_token_resolver")
    def test_withdraw_all_with_amount_uses_withdraw(self, mock_resolver, adapter):
        """withdraw_all=True with amount > 0 uses withdraw() with the provided amount."""
        mock_resolver.return_value.get_decimals.return_value = 6
        result = adapter.withdraw(asset="USDC", amount=Decimal("1000"), withdraw_all=True)
        assert result.success is True
        assert result.tx_data is not None
        # Uses withdraw selector (not redeem) when explicit amount provided
        assert result.tx_data["data"].startswith(SILO_V2_FUNCTION_SELECTORS["withdraw"])


class TestSiloV2RepayAll:
    """Test repay_all MAX_UINT256 fallback path."""

    @patch("almanak.framework.data.tokens.get_token_resolver")
    def test_repay_all_zero_amount_uses_max_uint256(self, mock_resolver, adapter):
        """repay_all=True with amount=0 uses repay(MAX_UINT256)."""
        mock_resolver.return_value.get_decimals.return_value = 6
        result = adapter.repay(asset="USDC", amount=Decimal("0"), repay_all=True)
        assert result.success is True
        assert result.tx_data is not None
        assert result.tx_data["data"].startswith(SILO_V2_FUNCTION_SELECTORS["repay"])
        max_hex = f"{MAX_UINT256:064x}"
        assert max_hex in result.tx_data["data"]

    @patch("almanak.framework.data.tokens.get_token_resolver")
    def test_repay_all_with_amount_uses_provided_amount(self, mock_resolver, adapter):
        """repay_all=True with amount > 0 uses the provided amount."""
        mock_resolver.return_value.get_decimals.return_value = 6
        result = adapter.repay(asset="USDC", amount=Decimal("500"), repay_all=True)
        assert result.success is True
        assert result.tx_data is not None
        assert result.tx_data["data"].startswith(SILO_V2_FUNCTION_SELECTORS["repay"])
        # Should NOT contain MAX_UINT256
        max_hex = f"{MAX_UINT256:064x}"
        assert max_hex not in result.tx_data["data"]
