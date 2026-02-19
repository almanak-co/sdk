"""PT Leverage Loop/Unwind Intent Factories.

These are NOT new intent types -- they are factory functions that produce
FlashLoanIntents with nested callback sequences for atomic PT leverage
operations on Morpho Blue.

PT Leverage Loop:
    1. Flash loan borrow token (e.g., USDC)
    2. Swap borrow token -> PT token (e.g., PT-sUSDe) via Pendle
    3. Supply PT as collateral on Morpho Blue
    4. Borrow from Morpho Blue to repay flash loan

PT Leverage Unwind:
    1. Flash loan borrow token to repay Morpho debt
    2. Repay Morpho Blue debt in full
    3. Withdraw PT collateral from Morpho Blue
    4. Swap PT -> borrow token via Pendle to repay flash loan

Safety Checks:
    - Maximum leverage cap (10x)
    - Minimum projected health factor (1.3)
    - PT maturity check (>7 days)
    - Slippage sanity check
"""

import logging
from dataclasses import dataclass
from decimal import Decimal

from .vocabulary import (
    BorrowIntent,
    FlashLoanCallbackIntent,
    FlashLoanIntent,
    RepayIntent,
    SupplyIntent,
    SwapIntent,
    WithdrawIntent,
)

logger = logging.getLogger(__name__)


# =============================================================================
# Safety Constants
# =============================================================================

MAX_LEVERAGE = Decimal("10")
MIN_PROJECTED_HEALTH_FACTOR = Decimal("1.3")
MIN_DAYS_TO_MATURITY = 7
MAX_SLIPPAGE_BPS_WARNING = 200


# =============================================================================
# Validation
# =============================================================================


@dataclass
class LeverageValidation:
    """Result of leverage parameter validation."""

    is_valid: bool
    errors: list[str]
    warnings: list[str]


def _validate_leverage_params(
    target_leverage: Decimal,
    lltv: Decimal,
    max_slippage_bps: int = 50,
    days_to_maturity: int | None = None,
) -> LeverageValidation:
    """Validate leverage parameters for safety.

    Args:
        target_leverage: Desired leverage multiplier (e.g., 5.0)
        lltv: Liquidation LTV of the Morpho market
        max_slippage_bps: Maximum slippage in basis points
        days_to_maturity: Days until PT maturity (None = skip check)

    Returns:
        LeverageValidation with errors/warnings
    """
    errors: list[str] = []
    warnings: list[str] = []

    # Check leverage cap
    if target_leverage > MAX_LEVERAGE:
        errors.append(f"Target leverage {target_leverage}x exceeds maximum {MAX_LEVERAGE}x")

    if target_leverage <= Decimal("1"):
        errors.append(f"Target leverage {target_leverage}x must be > 1.0 for a leverage loop")

    # Check projected health factor
    # For a loop: HF = LLTV / (1 - 1/leverage)
    # At leverage L, borrowed fraction = (L-1)/L, so HF = LLTV * L / (L-1)
    if target_leverage > Decimal("1"):
        projected_hf = lltv * target_leverage / (target_leverage - Decimal("1"))
        if projected_hf < MIN_PROJECTED_HEALTH_FACTOR:
            errors.append(
                f"Projected health factor {projected_hf:.2f} is below minimum {MIN_PROJECTED_HEALTH_FACTOR}. "
                f"Reduce leverage or choose a market with higher LLTV."
            )

    # Check PT maturity
    if days_to_maturity is not None and days_to_maturity < MIN_DAYS_TO_MATURITY:
        errors.append(
            f"PT expires in {days_to_maturity} days, minimum is {MIN_DAYS_TO_MATURITY} days. "
            f"Choose a PT with later maturity."
        )

    # Check slippage
    if max_slippage_bps > MAX_SLIPPAGE_BPS_WARNING:
        warnings.append(
            f"Slippage tolerance {max_slippage_bps} bps is high (>{MAX_SLIPPAGE_BPS_WARNING} bps). "
            f"This may result in significant losses."
        )

    return LeverageValidation(
        is_valid=len(errors) == 0,
        errors=errors,
        warnings=warnings,
    )


# =============================================================================
# Factory Functions
# =============================================================================


def build_pt_leverage_loop(
    borrow_token: str,
    pt_token: str,
    morpho_market_id: str,
    initial_amount: Decimal,
    target_leverage: Decimal,
    lltv: Decimal,
    max_slippage: Decimal = Decimal("0.005"),
    chain: str | None = None,
    days_to_maturity: int | None = None,
) -> FlashLoanIntent:
    """Build a PT leverage loop as a FlashLoanIntent.

    The loop atomically:
    1. Flash loans (leverage - 1) * initial_amount of borrow_token
    2. Swaps all borrow_token -> PT via Pendle
    3. Supplies PT as collateral on Morpho Blue
    4. Borrows borrow_token from Morpho to repay flash loan

    Args:
        borrow_token: Token to borrow (e.g., "USDC")
        pt_token: PT token to buy (e.g., "PT-sUSDe")
        morpho_market_id: Morpho Blue market ID for the PT collateral market
        initial_amount: Initial capital in borrow_token units
        target_leverage: Target leverage multiplier (e.g., 5.0 for 5x)
        lltv: Liquidation LTV of the Morpho market (e.g., 0.915)
        max_slippage: Maximum slippage tolerance (e.g., 0.005 = 0.5%)
        chain: Target chain (defaults to strategy's chain)
        days_to_maturity: Days until PT maturity (for safety check)

    Returns:
        FlashLoanIntent with nested swap -> supply -> borrow callbacks

    Raises:
        ValueError: If safety checks fail
    """
    if initial_amount <= 0:
        raise ValueError("initial_amount must be positive")

    max_slippage_bps = int(max_slippage * 10000)
    validation = _validate_leverage_params(
        target_leverage=target_leverage,
        lltv=lltv,
        max_slippage_bps=max_slippage_bps,
        days_to_maturity=days_to_maturity,
    )

    if not validation.is_valid:
        raise ValueError(f"PT leverage loop safety check failed: {'; '.join(validation.errors)}")

    for warning in validation.warnings:
        logger.warning(f"PT leverage loop warning: {warning}")

    # Calculate flash loan amount: (leverage - 1) * initial_amount
    flash_amount = initial_amount * (target_leverage - Decimal("1"))
    total_pt_buy = initial_amount * target_leverage

    # Build callback sequence
    callbacks: list[FlashLoanCallbackIntent] = [
        # 1. Swap all borrow_token -> PT via Pendle
        SwapIntent(
            from_token=borrow_token,
            to_token=pt_token,
            amount=total_pt_buy,
            max_slippage=max_slippage,
            protocol="pendle",
            chain=chain,
        ),
        # 2. Supply all PT as collateral on Morpho Blue
        SupplyIntent(
            token=pt_token,
            amount="all",
            protocol="morpho_blue",
            market_id=morpho_market_id,
            chain=chain,
        ),
        # 3. Borrow from Morpho to repay flash loan
        BorrowIntent(
            collateral_token=pt_token,
            collateral_amount="all",
            borrow_token=borrow_token,
            borrow_amount=flash_amount,
            protocol="morpho_blue",
            market_id=morpho_market_id,
            chain=chain,
        ),
    ]

    return FlashLoanIntent(
        provider="morpho",
        token=borrow_token,
        amount=flash_amount,
        callback_intents=callbacks,
        chain=chain,
    )


def build_pt_leverage_unwind(
    borrow_token: str,
    pt_token: str,
    morpho_market_id: str,
    total_debt: Decimal,
    max_slippage: Decimal = Decimal("0.005"),
    chain: str | None = None,
) -> FlashLoanIntent:
    """Build a PT leverage unwind as a FlashLoanIntent.

    The unwind atomically:
    1. Flash loans total_debt of borrow_token
    2. Repays all Morpho Blue debt
    3. Withdraws all PT collateral from Morpho
    4. Swaps PT -> borrow_token via Pendle to repay flash loan

    Args:
        borrow_token: Token to repay debt (e.g., "USDC")
        pt_token: PT token being used as collateral (e.g., "PT-sUSDe")
        morpho_market_id: Morpho Blue market ID
        total_debt: Total outstanding debt to repay
        max_slippage: Maximum slippage tolerance
        chain: Target chain

    Returns:
        FlashLoanIntent with nested repay -> withdraw -> swap callbacks
    """
    callbacks: list[FlashLoanCallbackIntent] = [
        # 1. Repay all Morpho debt
        RepayIntent(
            token=borrow_token,
            amount=total_debt,
            protocol="morpho_blue",
            market_id=morpho_market_id,
            repay_full=True,
            chain=chain,
        ),
        # 2. Withdraw all PT collateral
        WithdrawIntent(
            token=pt_token,
            amount=Decimal("0"),  # Ignored when withdraw_all=True
            withdraw_all=True,
            protocol="morpho_blue",
            market_id=morpho_market_id,
            chain=chain,
        ),
        # 3. Swap PT -> borrow_token via Pendle
        SwapIntent(
            from_token=pt_token,
            to_token=borrow_token,
            amount="all",
            max_slippage=max_slippage,
            protocol="pendle",
            chain=chain,
        ),
    ]

    return FlashLoanIntent(
        provider="morpho",
        token=borrow_token,
        amount=total_debt,
        callback_intents=callbacks,
        chain=chain,
    )


__all__ = [
    "LeverageValidation",
    "build_pt_leverage_loop",
    "build_pt_leverage_unwind",
]
