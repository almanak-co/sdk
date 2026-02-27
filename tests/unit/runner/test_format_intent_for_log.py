"""Tests for _format_intent_for_log() in strategy_runner."""

from decimal import Decimal

from almanak.framework.intents.vocabulary import BorrowIntent, RepayIntent, SupplyIntent, WithdrawIntent
from almanak.framework.runner.strategy_runner import _format_intent_for_log


class TestFormatBorrowIntentForLog:
    """Tests that BorrowIntent summary uses correct field names."""

    def test_borrow_intent_shows_amount_and_token(self):
        """BorrowIntent summary should show borrow_amount and borrow_token, not N/A."""
        intent = BorrowIntent(
            protocol="aave_v3",
            collateral_token="WETH",
            collateral_amount=Decimal("1.0"),
            borrow_token="USDC",
            borrow_amount=Decimal("1000"),
        )
        result = _format_intent_for_log(intent)
        assert "N/A" not in result
        assert "1000" in result
        assert "USDC" in result
        assert "aave_v3" in result

    def test_borrow_intent_shows_collateral_info(self):
        """BorrowIntent summary should include collateral details."""
        intent = BorrowIntent(
            protocol="compound_v3",
            collateral_token="WETH",
            collateral_amount=Decimal("2.5"),
            borrow_token="USDC",
            borrow_amount=Decimal("500"),
        )
        result = _format_intent_for_log(intent)
        assert "WETH" in result
        assert "2.5" in result
        assert "compound_v3" in result

    def test_borrow_intent_chained_collateral(self):
        """BorrowIntent with collateral_amount='all' should show ALL."""
        intent = BorrowIntent(
            protocol="aave_v3",
            collateral_token="WETH",
            collateral_amount="all",
            borrow_token="USDC",
            borrow_amount=Decimal("1000"),
        )
        result = _format_intent_for_log(intent)
        assert "ALL" in result
        assert "WETH" in result


class TestFormatOtherLendingIntentsForLog:
    """Verify other lending intents still format correctly after the fix."""

    def test_supply_intent_shows_amount(self):
        intent = SupplyIntent(
            protocol="aave_v3",
            token="USDC",
            amount=Decimal("5000"),
        )
        result = _format_intent_for_log(intent)
        assert "N/A" not in result
        assert "5000" in result
        assert "aave_v3" in result

    def test_repay_intent_shows_amount(self):
        intent = RepayIntent(
            protocol="aave_v3",
            token="USDC",
            amount=Decimal("500"),
        )
        result = _format_intent_for_log(intent)
        assert "N/A" not in result
        assert "500" in result

    def test_withdraw_intent_shows_all(self):
        intent = WithdrawIntent(
            protocol="aave_v3",
            token="USDC",
            amount="all",
        )
        result = _format_intent_for_log(intent)
        assert "ALL" in result
        assert "USDC" in result
