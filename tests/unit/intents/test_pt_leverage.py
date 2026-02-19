"""Unit tests for PT leverage loop/unwind factory functions."""

from decimal import Decimal

import pytest

from almanak.framework.intents.pt_leverage import (
    MAX_LEVERAGE,
    MAX_SLIPPAGE_BPS_WARNING,
    MIN_DAYS_TO_MATURITY,
    MIN_PROJECTED_HEALTH_FACTOR,
    LeverageValidation,
    _validate_leverage_params,
    build_pt_leverage_loop,
    build_pt_leverage_unwind,
)
from almanak.framework.intents.vocabulary import (
    BorrowIntent,
    FlashLoanIntent,
    RepayIntent,
    SupplyIntent,
    SwapIntent,
    WithdrawIntent,
)


# =========================================================================
# Validation Tests
# =========================================================================


class TestValidateLeverageParams:
    """Test _validate_leverage_params safety checks."""

    def test_valid_params(self):
        # 3x leverage with 91.5% LLTV: HF = 0.915 * 3 / 2 = 1.3725
        result = _validate_leverage_params(
            target_leverage=Decimal("3"),
            lltv=Decimal("0.915"),
            max_slippage_bps=50,
            days_to_maturity=90,
        )
        assert result.is_valid is True
        assert len(result.errors) == 0
        assert len(result.warnings) == 0

    def test_leverage_exceeds_max(self):
        result = _validate_leverage_params(
            target_leverage=Decimal("15"),
            lltv=Decimal("0.915"),
        )
        assert result.is_valid is False
        assert any("exceeds maximum" in e for e in result.errors)

    def test_leverage_below_one(self):
        result = _validate_leverage_params(
            target_leverage=Decimal("0.5"),
            lltv=Decimal("0.915"),
        )
        assert result.is_valid is False
        assert any("must be > 1.0" in e for e in result.errors)

    def test_leverage_exactly_one_invalid(self):
        result = _validate_leverage_params(
            target_leverage=Decimal("1"),
            lltv=Decimal("0.915"),
        )
        # At 1x leverage, flash amount would be zero -- rejected
        assert result.is_valid is False
        assert any("must be > 1.0" in e for e in result.errors)

    def test_leverage_exactly_max_valid(self):
        result = _validate_leverage_params(
            target_leverage=MAX_LEVERAGE,
            lltv=Decimal("0.915"),
        )
        # 10x with 91.5% LLTV: HF = 0.915 * 10 / 9 = 1.0167
        # This is below MIN_PROJECTED_HF (1.3), so should fail
        assert result.is_valid is False
        assert any("health factor" in e.lower() for e in result.errors)

    def test_projected_hf_too_low(self):
        # At 8x leverage with 86% LLTV: HF = 0.86 * 8 / 7 = 0.983
        result = _validate_leverage_params(
            target_leverage=Decimal("8"),
            lltv=Decimal("0.86"),
        )
        assert result.is_valid is False
        assert any("health factor" in e.lower() for e in result.errors)

    def test_projected_hf_safe(self):
        # At 3x leverage with 91.5% LLTV: HF = 0.915 * 3 / 2 = 1.3725
        result = _validate_leverage_params(
            target_leverage=Decimal("3"),
            lltv=Decimal("0.915"),
        )
        assert result.is_valid is True

    def test_pt_maturity_too_soon(self):
        result = _validate_leverage_params(
            target_leverage=Decimal("3"),
            lltv=Decimal("0.915"),
            days_to_maturity=3,
        )
        assert result.is_valid is False
        assert any("expires in" in e for e in result.errors)

    def test_pt_maturity_none_skips_check(self):
        result = _validate_leverage_params(
            target_leverage=Decimal("3"),
            lltv=Decimal("0.915"),
            days_to_maturity=None,
        )
        assert result.is_valid is True

    def test_high_slippage_warning(self):
        result = _validate_leverage_params(
            target_leverage=Decimal("3"),
            lltv=Decimal("0.915"),
            max_slippage_bps=300,
        )
        assert result.is_valid is True  # Warnings don't block
        assert len(result.warnings) == 1
        assert "Slippage tolerance" in result.warnings[0]

    def test_multiple_errors(self):
        result = _validate_leverage_params(
            target_leverage=Decimal("15"),
            lltv=Decimal("0.5"),
            days_to_maturity=2,
        )
        assert result.is_valid is False
        assert len(result.errors) >= 2  # At least leverage cap + HF or maturity


# =========================================================================
# PT Leverage Loop Tests
# =========================================================================


class TestBuildPtLeverageLoop:
    """Test build_pt_leverage_loop factory."""

    def test_produces_flash_loan_intent(self):
        intent = build_pt_leverage_loop(
            borrow_token="USDC",
            pt_token="PT-sUSDe",
            morpho_market_id="0xmarket123",
            initial_amount=Decimal("10000"),
            target_leverage=Decimal("3"),
            lltv=Decimal("0.915"),
        )
        assert isinstance(intent, FlashLoanIntent)
        assert intent.provider == "morpho"
        assert intent.token == "USDC"

    def test_flash_amount_calculation(self):
        # 2x leverage with 91.5% LLTV: HF = 0.915 * 2 / 1 = 1.83 (safe)
        intent = build_pt_leverage_loop(
            borrow_token="USDC",
            pt_token="PT-sUSDe",
            morpho_market_id="0xmarket",
            initial_amount=Decimal("10000"),
            target_leverage=Decimal("2"),
            lltv=Decimal("0.915"),
        )
        # Flash amount = (2 - 1) * 10000 = 10000
        assert intent.amount == Decimal("10000")

    def test_callback_sequence(self):
        intent = build_pt_leverage_loop(
            borrow_token="USDC",
            pt_token="PT-sUSDe",
            morpho_market_id="0xmarket",
            initial_amount=Decimal("10000"),
            target_leverage=Decimal("3"),
            lltv=Decimal("0.915"),
            chain="ethereum",
        )
        callbacks = intent.callback_intents
        assert len(callbacks) == 3

        # 1. Swap borrow_token -> PT
        swap = callbacks[0]
        assert isinstance(swap, SwapIntent)
        assert swap.from_token == "USDC"
        assert swap.to_token == "PT-sUSDe"
        assert swap.amount == Decimal("30000")  # 3x * 10000
        assert swap.protocol == "pendle"

        # 2. Supply PT as collateral
        supply = callbacks[1]
        assert isinstance(supply, SupplyIntent)
        assert supply.token == "PT-sUSDe"
        assert supply.amount == "all"
        assert supply.protocol == "morpho_blue"

        # 3. Borrow to repay flash loan
        borrow = callbacks[2]
        assert isinstance(borrow, BorrowIntent)
        assert borrow.borrow_token == "USDC"
        assert borrow.borrow_amount == Decimal("20000")  # (3-1) * 10000
        assert borrow.collateral_token == "PT-sUSDe"
        assert borrow.protocol == "morpho_blue"

    def test_chain_propagation(self):
        intent = build_pt_leverage_loop(
            borrow_token="USDC",
            pt_token="PT-sUSDe",
            morpho_market_id="0xmarket",
            initial_amount=Decimal("10000"),
            target_leverage=Decimal("3"),
            lltv=Decimal("0.915"),
            chain="ethereum",
        )
        assert intent.chain == "ethereum"
        for cb in intent.callback_intents:
            assert cb.chain == "ethereum"

    def test_slippage_propagation(self):
        intent = build_pt_leverage_loop(
            borrow_token="USDC",
            pt_token="PT-sUSDe",
            morpho_market_id="0xmarket",
            initial_amount=Decimal("10000"),
            target_leverage=Decimal("3"),
            lltv=Decimal("0.915"),
            max_slippage=Decimal("0.01"),
        )
        swap = intent.callback_intents[0]
        assert isinstance(swap, SwapIntent)
        assert swap.max_slippage == Decimal("0.01")

    def test_safety_check_blocks_bad_leverage(self):
        with pytest.raises(ValueError, match="safety check failed"):
            build_pt_leverage_loop(
                borrow_token="USDC",
                pt_token="PT-sUSDe",
                morpho_market_id="0xmarket",
                initial_amount=Decimal("10000"),
                target_leverage=Decimal("15"),
                lltv=Decimal("0.915"),
            )

    def test_safety_check_blocks_low_hf(self):
        with pytest.raises(ValueError, match="safety check failed"):
            build_pt_leverage_loop(
                borrow_token="USDC",
                pt_token="PT-sUSDe",
                morpho_market_id="0xmarket",
                initial_amount=Decimal("10000"),
                target_leverage=Decimal("8"),
                lltv=Decimal("0.86"),
            )

    def test_safety_check_blocks_near_maturity(self):
        with pytest.raises(ValueError, match="safety check failed"):
            build_pt_leverage_loop(
                borrow_token="USDC",
                pt_token="PT-sUSDe",
                morpho_market_id="0xmarket",
                initial_amount=Decimal("10000"),
                target_leverage=Decimal("3"),
                lltv=Decimal("0.915"),
                days_to_maturity=3,
            )

    def test_market_id_propagation(self):
        intent = build_pt_leverage_loop(
            borrow_token="USDC",
            pt_token="PT-sUSDe",
            morpho_market_id="0xbc55abc123",
            initial_amount=Decimal("10000"),
            target_leverage=Decimal("3"),
            lltv=Decimal("0.915"),
        )
        supply = intent.callback_intents[1]
        borrow = intent.callback_intents[2]
        assert isinstance(supply, SupplyIntent)
        assert isinstance(borrow, BorrowIntent)
        assert supply.market_id == "0xbc55abc123"
        assert borrow.market_id == "0xbc55abc123"


# =========================================================================
# PT Leverage Unwind Tests
# =========================================================================


class TestBuildPtLeverageUnwind:
    """Test build_pt_leverage_unwind factory."""

    def test_produces_flash_loan_intent(self):
        intent = build_pt_leverage_unwind(
            borrow_token="USDC",
            pt_token="PT-sUSDe",
            morpho_market_id="0xmarket",
            total_debt=Decimal("40000"),
        )
        assert isinstance(intent, FlashLoanIntent)
        assert intent.provider == "morpho"
        assert intent.token == "USDC"
        assert intent.amount == Decimal("40000")

    def test_callback_sequence(self):
        intent = build_pt_leverage_unwind(
            borrow_token="USDC",
            pt_token="PT-sUSDe",
            morpho_market_id="0xmarket",
            total_debt=Decimal("40000"),
            chain="ethereum",
        )
        callbacks = intent.callback_intents
        assert len(callbacks) == 3

        # 1. Repay all Morpho debt
        repay = callbacks[0]
        assert isinstance(repay, RepayIntent)
        assert repay.token == "USDC"
        assert repay.amount == Decimal("40000")
        assert repay.protocol == "morpho_blue"
        assert repay.repay_full is True

        # 2. Withdraw all PT collateral
        withdraw = callbacks[1]
        assert isinstance(withdraw, WithdrawIntent)
        assert withdraw.token == "PT-sUSDe"
        assert withdraw.withdraw_all is True
        assert withdraw.protocol == "morpho_blue"

        # 3. Swap PT -> borrow_token
        swap = callbacks[2]
        assert isinstance(swap, SwapIntent)
        assert swap.from_token == "PT-sUSDe"
        assert swap.to_token == "USDC"
        assert swap.amount == "all"
        assert swap.protocol == "pendle"

    def test_chain_propagation(self):
        intent = build_pt_leverage_unwind(
            borrow_token="USDC",
            pt_token="PT-sUSDe",
            morpho_market_id="0xmarket",
            total_debt=Decimal("40000"),
            chain="ethereum",
        )
        assert intent.chain == "ethereum"
        for cb in intent.callback_intents:
            assert cb.chain == "ethereum"

    def test_slippage_propagation(self):
        intent = build_pt_leverage_unwind(
            borrow_token="USDC",
            pt_token="PT-sUSDe",
            morpho_market_id="0xmarket",
            total_debt=Decimal("40000"),
            max_slippage=Decimal("0.01"),
        )
        swap = intent.callback_intents[2]
        assert isinstance(swap, SwapIntent)
        assert swap.max_slippage == Decimal("0.01")

    def test_market_id_propagation(self):
        intent = build_pt_leverage_unwind(
            borrow_token="USDC",
            pt_token="PT-sUSDe",
            morpho_market_id="0xbc55abc123",
            total_debt=Decimal("40000"),
        )
        repay = intent.callback_intents[0]
        withdraw = intent.callback_intents[1]
        assert isinstance(repay, RepayIntent)
        assert isinstance(withdraw, WithdrawIntent)
        assert repay.market_id == "0xbc55abc123"
        assert withdraw.market_id == "0xbc55abc123"
