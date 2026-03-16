"""Tests for Morpho Blue withdraw_all overflow fix (VIB-588).

Morpho Blue stores position values as uint128 internally. Using MAX_UINT256
for withdraw_all and withdraw_collateral_all causes:
- withdraw: overflow in Morpho's mulDivDown (shares * totalAssets)
- withdrawCollateral: revert on toUint128() cast

The fix queries actual on-chain position values instead of using MAX_UINT256.
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.connectors.morpho_blue.adapter import (
    MORPHO_BLUE_ADDRESSES,
    MORPHO_MARKETS,
    MORPHO_WITHDRAW_COLLATERAL_SELECTOR,
    MORPHO_WITHDRAW_SELECTOR,
    MorphoBlueAdapter,
    MorphoBlueConfig,
    MorphoBluePosition,
)

WSTETH_USDC_MARKET_ID = "0xb323495f7e4148be5643a4ea4a8221eef163e4bccfdedc2a6f4696baacbc86cc"


def _extract_withdraw_shares(calldata: str) -> int:
    """Extract the shares_wei value from encoded withdraw calldata."""
    # Layout: selector(10) + MarketParams(5*64) + assets(64) + shares(64) + ...
    payload = calldata[len(MORPHO_WITHDRAW_SELECTOR):]
    shares_offset = 64 * 6  # 5 MarketParams slots + 1 assets slot
    return int(payload[shares_offset: shares_offset + 64], 16)


def _extract_withdraw_assets(calldata: str) -> int:
    """Extract the assets_wei value from encoded withdraw calldata."""
    # Layout: selector(10) + MarketParams(5*64) + assets(64) + ...
    payload = calldata[len(MORPHO_WITHDRAW_SELECTOR):]
    assets_offset = 64 * 5  # 5 MarketParams slots
    return int(payload[assets_offset: assets_offset + 64], 16)


def _extract_collateral_amount(calldata: str) -> int:
    """Extract the amount from encoded withdrawCollateral calldata."""
    # Layout: selector(10) + MarketParams(5*64) + amount(64) + ...
    payload = calldata[len(MORPHO_WITHDRAW_COLLATERAL_SELECTOR):]
    amount_offset = 64 * 5  # 5 MarketParams slots
    return int(payload[amount_offset: amount_offset + 64], 16)


@pytest.fixture
def adapter() -> MorphoBlueAdapter:
    """Create Morpho Blue adapter for Ethereum."""
    config = MorphoBlueConfig(
        chain="ethereum",
        wallet_address="0x1234567890123456789012345678901234567890",
    )
    return MorphoBlueAdapter(config)


class TestWithdrawAllOverflow:
    """Tests that withdraw_all uses actual on-chain shares instead of MAX_UINT256."""

    def test_withdraw_all_uses_actual_supply_shares(self, adapter: MorphoBlueAdapter) -> None:
        """withdraw_all should query on-chain supply_shares, not use MAX_UINT256."""
        mock_position = MorphoBluePosition(
            market_id=WSTETH_USDC_MARKET_ID,
            supply_shares=Decimal("1000000000000000000000"),  # 1000 * 1e18
            borrow_shares=Decimal("0"),
            collateral=Decimal("0"),
        )
        with patch.object(adapter, "get_position_on_chain", return_value=mock_position) as mock_get:
            result = adapter.withdraw(
                market_id=WSTETH_USDC_MARKET_ID,
                amount=Decimal("0"),
                withdraw_all=True,
            )

        assert result.success is True
        assert result.tx_data is not None
        assert result.tx_data["data"].startswith(MORPHO_WITHDRAW_SELECTOR)

        # Verify assets=0 and shares=actual supply shares
        assert _extract_withdraw_assets(result.tx_data["data"]) == 0
        assert _extract_withdraw_shares(result.tx_data["data"]) == 1000000000000000000000

        mock_get.assert_called_once()

    def test_withdraw_all_no_position_returns_error(self, adapter: MorphoBlueAdapter) -> None:
        """withdraw_all with no supply position should return error."""
        mock_position = MorphoBluePosition(
            market_id=WSTETH_USDC_MARKET_ID,
            supply_shares=Decimal("0"),
            borrow_shares=Decimal("0"),
            collateral=Decimal("0"),
        )
        with patch.object(adapter, "get_position_on_chain", return_value=mock_position):
            result = adapter.withdraw(
                market_id=WSTETH_USDC_MARKET_ID,
                amount=Decimal("0"),
                withdraw_all=True,
            )

        assert result.success is False
        assert "No supply position" in result.error

    def test_withdraw_specific_amount_unchanged(self, adapter: MorphoBlueAdapter) -> None:
        """Regular withdraw (not all) should not query position."""
        with patch.object(adapter, "get_position_on_chain") as mock_get:
            result = adapter.withdraw(
                market_id=WSTETH_USDC_MARKET_ID,
                amount=Decimal("100"),
            )

        assert result.success is True
        mock_get.assert_not_called()


class TestWithdrawCollateralAllOverflow:
    """Tests that withdraw_collateral with withdraw_all uses actual on-chain amount."""

    def test_withdraw_collateral_all_uses_actual_amount(self, adapter: MorphoBlueAdapter) -> None:
        """withdraw_collateral(withdraw_all=True) should use actual collateral, not MAX_UINT256."""
        mock_position = MorphoBluePosition(
            market_id=WSTETH_USDC_MARKET_ID,
            supply_shares=Decimal("0"),
            borrow_shares=Decimal("0"),
            collateral=Decimal("5000000000"),  # 5000 USDC in wei (6 decimals)
        )
        with patch.object(adapter, "get_position_on_chain", return_value=mock_position) as mock_get:
            result = adapter.withdraw_collateral(
                market_id=WSTETH_USDC_MARKET_ID,
                amount=Decimal("0"),
                withdraw_all=True,
            )

        assert result.success is True
        assert result.tx_data is not None
        assert result.tx_data["data"].startswith(MORPHO_WITHDRAW_COLLATERAL_SELECTOR)

        # Verify amount = actual collateral, not MAX_UINT256
        assert _extract_collateral_amount(result.tx_data["data"]) == 5000000000

        mock_get.assert_called_once()

    def test_withdraw_collateral_all_no_position_returns_error(self, adapter: MorphoBlueAdapter) -> None:
        """withdraw_collateral(withdraw_all=True) with no collateral should return error."""
        mock_position = MorphoBluePosition(
            market_id=WSTETH_USDC_MARKET_ID,
            supply_shares=Decimal("0"),
            borrow_shares=Decimal("0"),
            collateral=Decimal("0"),
        )
        with patch.object(adapter, "get_position_on_chain", return_value=mock_position):
            result = adapter.withdraw_collateral(
                market_id=WSTETH_USDC_MARKET_ID,
                amount=Decimal("0"),
                withdraw_all=True,
            )

        assert result.success is False
        assert "No collateral position" in result.error

    def test_withdraw_collateral_specific_amount_unchanged(self, adapter: MorphoBlueAdapter) -> None:
        """Regular withdraw_collateral (not all) should not query position."""
        with patch.object(adapter, "get_position_on_chain") as mock_get:
            result = adapter.withdraw_collateral(
                market_id=WSTETH_USDC_MARKET_ID,
                amount=Decimal("100"),
            )

        assert result.success is True
        mock_get.assert_not_called()
