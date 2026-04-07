"""Unit tests for the amount_resolver module.

Tests the resolve_amount_all() function and ProtocolBalanceReader implementations
without requiring on-chain execution or gateway connectivity.
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.intents.amount_resolver import (
    AmountResolutionCategory,
    AaveV3BalanceReader,
    CompoundV3BalanceReader,
    MorphoBlueBalanceReader,
    _INTENT_TYPE_TO_CATEGORY,
    get_reader_for_protocol,
    resolve_amount_all,
)


# =============================================================================
# Semantic Category Mapping
# =============================================================================


class TestSemanticCategories:
    """Test that intent types map to correct resolution categories."""

    def test_withdraw_maps_to_protocol_supply(self):
        assert _INTENT_TYPE_TO_CATEGORY["WITHDRAW"] == AmountResolutionCategory.PROTOCOL_SUPPLY

    def test_repay_maps_to_protocol_debt(self):
        assert _INTENT_TYPE_TO_CATEGORY["REPAY"] == AmountResolutionCategory.PROTOCOL_DEBT

    def test_swap_maps_to_wallet_balance(self):
        assert _INTENT_TYPE_TO_CATEGORY["SWAP"] == AmountResolutionCategory.WALLET_BALANCE

    def test_supply_maps_to_wallet_balance(self):
        assert _INTENT_TYPE_TO_CATEGORY["SUPPLY"] == AmountResolutionCategory.WALLET_BALANCE

    def test_bridge_maps_to_wallet_balance(self):
        assert _INTENT_TYPE_TO_CATEGORY["BRIDGE"] == AmountResolutionCategory.WALLET_BALANCE


# =============================================================================
# Reader Registry
# =============================================================================


class TestReaderRegistry:
    """Test protocol-to-reader lookups."""

    def test_aave_v3_reader(self):
        reader = get_reader_for_protocol("aave_v3")
        assert isinstance(reader, AaveV3BalanceReader)

    def test_spark_reader(self):
        reader = get_reader_for_protocol("spark")
        assert isinstance(reader, AaveV3BalanceReader)

    def test_compound_v3_reader(self):
        reader = get_reader_for_protocol("compound_v3")
        assert isinstance(reader, CompoundV3BalanceReader)

    def test_morpho_reader(self):
        reader = get_reader_for_protocol("morpho_blue")
        assert isinstance(reader, MorphoBlueBalanceReader)

    def test_unknown_protocol_returns_none(self):
        assert get_reader_for_protocol("unknown_protocol") is None


# =============================================================================
# resolve_amount_all() — passthrough cases
# =============================================================================


class TestResolveAmountAllPassthrough:
    """Test that intents without amount='all' pass through unchanged."""

    def test_concrete_amount_passes_through(self):
        intent = MagicMock()
        intent.amount = Decimal("100")
        result = resolve_amount_all(intent, chain="arbitrum", wallet_address="0x1234")
        assert result is intent

    def test_withdraw_all_flag_passes_through(self):
        intent = MagicMock()
        intent.amount = "all"
        intent.withdraw_all = True
        result = resolve_amount_all(intent, chain="arbitrum", wallet_address="0x1234")
        assert result is intent

    def test_repay_full_flag_passes_through(self):
        intent = MagicMock()
        intent.amount = "all"
        intent.withdraw_all = False
        intent.repay_full = True
        result = resolve_amount_all(intent, chain="arbitrum", wallet_address="0x1234")
        assert result is intent

    def test_no_intent_type_passes_through(self):
        intent = MagicMock(spec=[])  # No attributes
        intent.amount = "all"
        result = resolve_amount_all(intent, chain="arbitrum", wallet_address="0x1234")
        assert result is intent


# =============================================================================
# resolve_amount_all() — withdraw resolution
# =============================================================================


class TestResolveAmountAllWithdraw:
    """Test withdraw amount='all' resolution paths."""

    def _make_withdraw_intent(self, protocol="aave_v3", token="USDC"):
        """Create a mock WithdrawIntent."""
        from almanak.framework.intents.lending_intents import WithdrawIntent

        return WithdrawIntent(
            protocol=protocol,
            token=token,
            amount="all",
            chain="arbitrum",
        )

    def test_withdraw_unknown_protocol_sets_withdraw_all(self):
        """Unknown protocol should fall back to withdraw_all=True."""
        intent = self._make_withdraw_intent(protocol="unknown_lending")
        result = resolve_amount_all(intent, chain="arbitrum", wallet_address="0x1234")
        assert result.withdraw_all is True
        assert result.amount == Decimal("0")

    def test_withdraw_aave_no_gateway_sets_withdraw_all(self):
        """Aave V3 without gateway client should fall back to withdraw_all=True."""
        intent = self._make_withdraw_intent(protocol="aave_v3")
        # No gateway_client -> LendingPositionReader returns None -> withdraw_all
        result = resolve_amount_all(intent, chain="arbitrum", wallet_address="0x1234")
        assert result.withdraw_all is True

    @patch("almanak.framework.intents.amount_resolver.AaveV3BalanceReader.get_supply_balance")
    @patch("almanak.framework.intents.amount_resolver._resolve_token_address")
    @patch("almanak.framework.intents.amount_resolver._get_token_decimals")
    def test_withdraw_aave_resolves_concrete_amount(
        self, mock_decimals, mock_resolve, mock_supply
    ):
        """Aave V3 with successful balance query resolves to concrete amount."""
        mock_resolve.return_value = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
        mock_supply.return_value = 100_000_000  # 100 USDC in wei (6 decimals)
        mock_decimals.return_value = 6

        intent = self._make_withdraw_intent(protocol="aave_v3")
        result = resolve_amount_all(
            intent, chain="arbitrum", wallet_address="0x1234", gateway_client=MagicMock()
        )
        assert result.amount == Decimal("100")
        assert result.withdraw_all is False

    @patch("almanak.framework.intents.amount_resolver.AaveV3BalanceReader.get_supply_balance")
    @patch("almanak.framework.intents.amount_resolver._resolve_token_address")
    def test_withdraw_zero_balance_sets_withdraw_all(self, mock_resolve, mock_supply):
        """Zero balance should fall back to withdraw_all=True (nothing to withdraw)."""
        mock_resolve.return_value = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
        mock_supply.return_value = 0

        intent = self._make_withdraw_intent(protocol="aave_v3")
        result = resolve_amount_all(
            intent, chain="arbitrum", wallet_address="0x1234", gateway_client=MagicMock()
        )
        assert result.withdraw_all is True


# =============================================================================
# resolve_amount_all() — repay resolution
# =============================================================================


class TestResolveAmountAllRepay:
    """Test repay amount='all' resolution paths."""

    def _make_repay_intent(self, protocol="aave_v3", token="USDC"):
        from almanak.framework.intents.lending_intents import RepayIntent

        return RepayIntent(
            protocol=protocol,
            token=token,
            amount="all",
            chain="arbitrum",
        )

    def test_repay_unknown_protocol_sets_repay_full(self):
        intent = self._make_repay_intent(protocol="unknown_lending")
        result = resolve_amount_all(intent, chain="arbitrum", wallet_address="0x1234")
        assert result.repay_full is True
        assert result.amount == Decimal("0")

    def test_repay_aave_no_gateway_sets_repay_full(self):
        intent = self._make_repay_intent(protocol="aave_v3")
        result = resolve_amount_all(intent, chain="arbitrum", wallet_address="0x1234")
        assert result.repay_full is True


# =============================================================================
# Wallet balance intents (swap, supply) — passthrough
# =============================================================================


class TestWalletBalancePassthrough:
    """Test that wallet-balance intents pass through (resolved by caller)."""

    def test_swap_amount_all_passes_through(self):
        """SwapIntent(amount='all') should pass through — resolved by compiler/runner."""
        from almanak.framework.intents.vocabulary import SwapIntent

        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount="all",
            max_slippage=Decimal("0.01"),
            protocol="uniswap_v3",
            chain="arbitrum",
        )
        result = resolve_amount_all(intent, chain="arbitrum", wallet_address="0x1234")
        assert result.amount == "all"  # Unchanged — wallet balance resolved by caller


# =============================================================================
# ProtocolBalanceReader implementations
# =============================================================================


class TestMorphoBlueBalanceReader:
    """Test Morpho Blue reader returns None (delegates to adapter)."""

    def test_supply_returns_none(self):
        reader = MorphoBlueBalanceReader()
        result = reader.get_supply_balance("ethereum", "0x1234", "0x5678")
        assert result is None

    def test_debt_returns_none(self):
        reader = MorphoBlueBalanceReader()
        result = reader.get_debt_balance("ethereum", "0x1234", "0x5678")
        assert result is None


class TestCompoundV3BalanceReader:
    """Test Compound V3 reader."""

    def test_no_gateway_returns_none(self):
        reader = CompoundV3BalanceReader()
        result = reader.get_supply_balance("arbitrum", "0x1234", "0x5678", gateway_client=None)
        assert result is None

    def test_unknown_chain_returns_none(self):
        reader = CompoundV3BalanceReader()
        result = reader.get_supply_balance(
            "unknown_chain", "0x1234", "0x5678", gateway_client=MagicMock()
        )
        assert result is None
