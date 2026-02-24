"""Tests for GMX V2 execution fee calculation (VIB-31).

Validates that get_execution_fee uses GMX's adjustedGasLimit formula:
  adjustedGasLimit = baseGasLimit + orderGasLimit * multiplierFactor
instead of the raw orderGasLimit.
"""

from unittest.mock import MagicMock, PropertyMock, patch

import pytest

from almanak.framework.connectors.gmx_v2.sdk import (
    DECREASE_ORDER_GAS_LIMIT,
    GMX_GAS_BASE_AMOUNT,
    GMX_GAS_MULTIPLIER,
    INCREASE_ORDER_GAS_LIMIT,
    MIN_EXECUTION_FEE_FALLBACK,
    GMXV2SDK,
)


class TestAdjustedGasLimitFormula:
    """Verify the adjusted gas limit constants produce the expected result."""

    def test_increase_order_adjusted_gas_limit(self):
        """3M * 1.56 + 2.89M = ~7.57M (not 3M)."""
        adjusted = int(GMX_GAS_BASE_AMOUNT + INCREASE_ORDER_GAS_LIMIT * GMX_GAS_MULTIPLIER)
        assert adjusted == 7_570_000
        # This is ~2.5x the raw 3M limit — the old code was ~70% too low
        assert adjusted > INCREASE_ORDER_GAS_LIMIT * 2

    def test_decrease_order_adjusted_gas_limit(self):
        adjusted = int(GMX_GAS_BASE_AMOUNT + DECREASE_ORDER_GAS_LIMIT * GMX_GAS_MULTIPLIER)
        assert adjusted == 7_570_000


class TestGetExecutionFee:
    """Test get_execution_fee uses the adjusted formula."""

    @patch("almanak.framework.connectors.gmx_v2.sdk.Web3")
    def test_execution_fee_uses_adjusted_gas_limit(self, mock_web3_cls):
        """Execution fee = adjustedGasLimit * gasPrice * multiplier."""
        mock_web3 = MagicMock()
        mock_web3_cls.return_value = mock_web3
        # Use 1 gwei so fee exceeds MIN_EXECUTION_FEE_FALLBACK
        mock_web3.eth.gas_price = 1_000_000_000  # 1 gwei

        sdk = GMXV2SDK.__new__(GMXV2SDK)
        sdk.web3 = mock_web3

        fee = sdk.get_execution_fee(order_type="increase", multiplier=1.0)

        # adjustedGasLimit = 2_890_000 + 3_000_000 * 1.56 = 7_570_000
        # fee = 7_570_000 * 1_000_000_000 * 1.0 = 7_570_000_000_000_000 = 0.00757 ETH
        expected = int(7_570_000 * 1_000_000_000 * 1.0)
        assert fee == expected
        # Verify this is much larger than what the old formula would produce
        old_formula_fee = int(3_000_000 * 1_000_000_000 * 1.0)
        assert fee > old_formula_fee * 2

    @patch("almanak.framework.connectors.gmx_v2.sdk.Web3")
    def test_execution_fee_with_safety_multiplier(self, mock_web3_cls):
        """Default 1.5x multiplier applied on top of adjusted gas limit."""
        mock_web3 = MagicMock()
        mock_web3_cls.return_value = mock_web3
        mock_web3.eth.gas_price = 1_000_000_000  # 1 gwei

        sdk = GMXV2SDK.__new__(GMXV2SDK)
        sdk.web3 = mock_web3

        fee = sdk.get_execution_fee(order_type="increase", multiplier=1.5)

        expected = int(7_570_000 * 1_000_000_000 * 1.5)
        assert fee == expected

    @patch("almanak.framework.connectors.gmx_v2.sdk.Web3")
    def test_execution_fee_respects_minimum(self, mock_web3_cls):
        """Fee should be at least MIN_EXECUTION_FEE_FALLBACK."""
        mock_web3 = MagicMock()
        mock_web3_cls.return_value = mock_web3
        mock_web3.eth.gas_price = 1  # extremely low gas price

        sdk = GMXV2SDK.__new__(GMXV2SDK)
        sdk.web3 = mock_web3

        fee = sdk.get_execution_fee(order_type="increase", multiplier=1.0)
        assert fee == MIN_EXECUTION_FEE_FALLBACK

    @patch("almanak.framework.connectors.gmx_v2.sdk.Web3")
    def test_execution_fee_fallback_on_rpc_error(self, mock_web3_cls):
        """On RPC failure, returns 2x minimum fallback."""
        mock_web3 = MagicMock()
        mock_web3_cls.return_value = mock_web3
        type(mock_web3.eth).gas_price = PropertyMock(side_effect=Exception("RPC down"))

        sdk = GMXV2SDK.__new__(GMXV2SDK)
        sdk.web3 = mock_web3

        fee = sdk.get_execution_fee(order_type="increase")
        assert fee == MIN_EXECUTION_FEE_FALLBACK * 2
