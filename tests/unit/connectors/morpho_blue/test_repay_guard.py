"""Tests for Morpho Blue repay over-repay guard (VIB-648).

Morpho Blue panics (0x11 underflow) if repay amount > actual on-chain debt.
The adapter should cap asset-based repay amounts at the actual debt.
"""

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.framework.connectors.morpho_blue.adapter import (
    MORPHO_BLUE_ADDRESSES,
    MORPHO_MARKETS,
    MORPHO_REPAY_SELECTOR,
    MorphoBlueAdapter,
    MorphoBlueConfig,
)

WSTETH_USDC_MARKET_ID = "0xb323495f7e4148be5643a4ea4a8221eef163e4bccfdedc2a6f4696baacbc86cc"


def _extract_repay_assets(calldata: str) -> int:
    """Extract the assets_wei value from encoded repay calldata."""
    # Layout: selector(10) + MarketParams(5*64) + assets(64) + ...
    payload = calldata[len(MORPHO_REPAY_SELECTOR) :]
    assets_offset = 64 * 5  # 5 MarketParams slots
    return int(payload[assets_offset : assets_offset + 64], 16)


@pytest.fixture
def adapter() -> MorphoBlueAdapter:
    """Create Morpho Blue adapter for Ethereum."""
    config = MorphoBlueConfig(
        chain="ethereum",
        wallet_address="0x1234567890123456789012345678901234567890",
    )
    return MorphoBlueAdapter(config)


class TestRepayOverRepayGuard:
    """Tests for the over-repay guard that prevents 0x11 underflow."""

    def test_repay_caps_at_actual_debt(self, adapter: MorphoBlueAdapter) -> None:
        """Repay amount exceeding actual debt should be capped."""
        adapter._sdk_enabled = True
        adapter._sdk = MagicMock()
        # Actual debt: 500 USDC (500 * 1e6 = 500_000_000 wei)
        adapter._sdk.get_borrow_assets.return_value = 500_000_000

        # Request 505 USDC (exceeds debt)
        result = adapter.repay(
            market_id=WSTETH_USDC_MARKET_ID,
            amount=Decimal("505"),
        )
        assert result.success is True
        assert result.tx_data is not None
        assert result.tx_data["data"].startswith(MORPHO_REPAY_SELECTOR)

        # Verify the repay amount was capped to actual debt (500 USDC = 500_000_000 wei)
        assert _extract_repay_assets(result.tx_data["data"]) == 500_000_000

        # Verify SDK was called to check debt
        adapter._sdk.get_borrow_assets.assert_called_once()

    def test_repay_no_cap_when_under_debt(self, adapter: MorphoBlueAdapter) -> None:
        """Repay amount under actual debt should pass through unchanged."""
        adapter._sdk_enabled = True
        adapter._sdk = MagicMock()
        adapter._sdk.get_borrow_assets.return_value = 500_000_000  # 500 USDC

        result = adapter.repay(
            market_id=WSTETH_USDC_MARKET_ID,
            amount=Decimal("400"),
        )
        assert result.success is True
        assert result.tx_data is not None

        # Verify the repay amount is the original requested amount (400 USDC = 400_000_000 wei)
        assert _extract_repay_assets(result.tx_data["data"]) == 400_000_000

    def test_repay_sdk_error_proceeds_with_requested_amount(self, adapter: MorphoBlueAdapter) -> None:
        """SDK error during debt query should not block repay."""
        adapter._sdk_enabled = True
        adapter._sdk = MagicMock()
        adapter._sdk.get_borrow_assets.side_effect = Exception("RPC error")

        result = adapter.repay(
            market_id=WSTETH_USDC_MARKET_ID,
            amount=Decimal("505"),
        )
        assert result.success is True
        assert result.tx_data is not None

        # Verify the original amount is used when SDK query fails (505 USDC = 505_000_000 wei)
        assert _extract_repay_assets(result.tx_data["data"]) == 505_000_000

    def test_repay_sdk_disabled_skips_guard(self, adapter: MorphoBlueAdapter) -> None:
        """When SDK is disabled, repay should proceed without guard."""
        adapter._sdk_enabled = False

        result = adapter.repay(
            market_id=WSTETH_USDC_MARKET_ID,
            amount=Decimal("505"),
        )
        assert result.success is True
        assert result.tx_data is not None

        # Verify the original amount is used when SDK is disabled (505 USDC = 505_000_000 wei)
        assert _extract_repay_assets(result.tx_data["data"]) == 505_000_000

    def test_repay_shares_mode_skips_guard(self, adapter: MorphoBlueAdapter) -> None:
        """Shares-mode repay should not trigger the guard."""
        adapter._sdk_enabled = True
        adapter._sdk = MagicMock()

        result = adapter.repay(
            market_id=WSTETH_USDC_MARKET_ID,
            amount=Decimal("1000000000000000000"),
            shares_mode=True,
        )
        assert result.success is True
        # SDK should NOT be called for shares-mode repay
        adapter._sdk.get_borrow_assets.assert_not_called()
