"""Unit tests for Silo V2 adapter — MAX_UINT256 fallback paths."""

from decimal import Decimal
from unittest.mock import patch

import pytest

from almanak.connectors.silo_v2.adapter import (
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
    """Test the full-exit withdraw encoder paths (VIB-5800)."""

    @patch("almanak.framework.data.tokens.get_token_resolver")
    def test_withdraw_all_zero_amount_refuses_max_uint_redeem(self, mock_resolver, adapter):
        """withdraw_all=True with amount=0 must FAIL, not encode redeem(MAX_UINT256).

        Silo V2's redeem() reverts NotEnoughLiquidity() (0x4323a555) on MAX_UINT256
        (proven on an Avalanche fork, VIB-5800). The pure encoder refuses to emit a
        redeem it knows reverts on-chain; the caller must resolve the redeemable
        share balance (maxRedeem / balanceOf) and use redeem_shares().
        """
        mock_resolver.return_value.get_decimals.return_value = 6
        result = adapter.withdraw(asset="USDC", amount=Decimal("0"), withdraw_all=True)
        assert result.success is False
        assert result.tx_data is None
        assert "redeem_shares" in result.error
        assert "MAX_UINT256" in result.error

    @patch("almanak.framework.data.tokens.get_token_resolver")
    def test_redeem_shares_encodes_explicit_share_count(self, mock_resolver, adapter):
        """redeem_shares(shares) encodes redeem() with the EXACT share count."""
        result = adapter.redeem_shares(
            shares=3_000_000_000_000,
            market_name="WAVAX/USDC",
            silo_address="0xfA5f7d5BcD70dC2F031eE906fc692a9e19584CB0",
            collateral_type=1,
        )
        assert result.success is True
        assert result.tx_data is not None
        # Must use redeem selector with the explicit share count (never MAX_UINT256).
        assert result.tx_data["data"].startswith(SILO_V2_FUNCTION_SELECTORS["redeem"])
        assert f"{3_000_000_000_000:064x}" in result.tx_data["data"]
        assert f"{MAX_UINT256:064x}" not in result.tx_data["data"]

    @patch("almanak.framework.data.tokens.get_token_resolver")
    def test_withdraw_all_with_amount_uses_withdraw(self, mock_resolver, adapter):
        """withdraw_all=True with amount > 0 uses withdraw() with the provided amount."""
        mock_resolver.return_value.get_decimals.return_value = 6
        result = adapter.withdraw(asset="USDC", amount=Decimal("1000"), withdraw_all=True)
        assert result.success is True
        assert result.tx_data is not None
        # Uses withdraw selector (not redeem) when explicit amount provided
        assert result.tx_data["data"].startswith(SILO_V2_FUNCTION_SELECTORS["withdraw"])

    @patch("almanak.framework.data.tokens.get_token_resolver")
    def test_withdraw_sub_base_unit_amount_refuses_zero_encode(self, mock_resolver, adapter):
        """A positive amount that truncates to 0 base units must FAIL, not encode withdraw(0)."""
        mock_resolver.return_value.get_decimals.return_value = 6
        # 0.0000001 USDC * 10**6 = 0.1 -> int() truncates to 0 wei.
        result = adapter.withdraw(asset="USDC", amount=Decimal("0.0000001"), withdraw_all=False)
        assert result.success is False
        assert result.tx_data is None
        assert "rounds to zero base units" in result.error


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
