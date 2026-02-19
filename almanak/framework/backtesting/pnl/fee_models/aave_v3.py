"""Aave V3 specific fee model for PnL backtesting.

This module provides a fee model implementation tailored to Aave V3's
lending protocol characteristics:

- Borrow: Small origination fee (typically 0-0.01% depending on asset)
- Supply: No protocol fee
- Withdraw: No protocol fee
- Repay: No protocol fee

Note: Aave V3 does not charge slippage in the traditional sense since
lending operations are not market-based swaps. Interest rates are
handled separately via the portfolio mark-to-market calculations.

Key Components:
    - AaveV3FeeModel: Fee model for Aave V3 lending operations

Example:
    from almanak.framework.backtesting.pnl.fee_models.aave_v3 import AaveV3FeeModel

    fee_model = AaveV3FeeModel(borrow_origination_fee_pct=Decimal("0.0001"))

    fee = fee_model.calculate_fee(
        intent_type=IntentType.BORROW,
        amount_usd=Decimal("10000"),
        market_state=market_state,
    )
"""

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from almanak.framework.backtesting.models import IntentType
from almanak.framework.backtesting.pnl.fee_models.base import FeeModel


@dataclass
class AaveV3FeeModel(FeeModel):
    """Fee model for Aave V3 lending protocol.

    Aave V3 charges fees primarily on borrow operations through an
    origination fee. Supply, withdraw, and repay operations have no
    protocol fees.

    Fee Structure:
        - BORROW: Origination fee (configurable, default 0.01%)
        - SUPPLY: No fee
        - WITHDRAW: No fee
        - REPAY: No fee

    The origination fee is a one-time fee charged when taking out a loan.
    Different assets may have different origination fee rates, which can
    be configured via the `asset_fees` parameter.

    Attributes:
        borrow_origination_fee_pct: Default origination fee for borrows
            (default 0.01% = 0.0001)
        asset_fees: Optional mapping of asset symbols to their specific
            origination fee percentages (overrides default)
        flash_loan_fee_pct: Fee for flash loans (default 0.05% = 0.0005)
            Note: Flash loans are not currently simulated but included
            for completeness

    Example:
        # Simple uniform fee
        model = AaveV3FeeModel(borrow_origination_fee_pct=Decimal("0.0001"))

        # Per-asset fees
        model = AaveV3FeeModel(
            borrow_origination_fee_pct=Decimal("0.0001"),
            asset_fees={
                "USDC": Decimal("0"),  # No fee for USDC
                "WETH": Decimal("0.0001"),  # 0.01% for WETH
            },
        )
    """

    borrow_origination_fee_pct: Decimal = Decimal("0.0001")  # 0.01% default
    asset_fees: dict[str, Decimal] | None = None
    flash_loan_fee_pct: Decimal = Decimal("0.0005")  # 0.05% for flash loans

    # Intents with zero fees in Aave V3
    _zero_fee_intents: frozenset[IntentType] = frozenset(
        {
            IntentType.SUPPLY,
            IntentType.WITHDRAW,
            IntentType.REPAY,
            IntentType.HOLD,
        }
    )

    def calculate_fee(
        self,
        trade_amount: Decimal,
        **kwargs: Any,
    ) -> Decimal:
        """Calculate Aave V3 fee for a lending operation.

        Only BORROW intents incur fees. Supply, withdraw, and repay
        operations have no protocol fees in Aave V3.

        Args:
            trade_amount: Notional amount of the operation in USD
            **kwargs: Additional parameters:
                - intent_type: Type of intent being executed (default: BORROW)
                - market_state: Current market state at execution time
                - protocol: Protocol being used (ignored, always aave_v3)
                - asset: Asset symbol for asset-specific fee lookup
                - is_flash_loan: If True, uses flash loan fee instead

        Returns:
            Fee amount in USD
        """
        # Get intent type from kwargs, default to BORROW
        intent_type = kwargs.get("intent_type", IntentType.BORROW)

        # Zero-fee intents
        if intent_type in self._zero_fee_intents:
            return Decimal("0")

        # Only BORROW has fees in Aave V3
        if intent_type != IntentType.BORROW:
            return Decimal("0")

        # Check for flash loan
        if kwargs.get("is_flash_loan"):
            return trade_amount * self.flash_loan_fee_pct

        # Determine fee rate to use
        fee_rate = self._resolve_fee_rate(**kwargs)

        return trade_amount * fee_rate

    def _resolve_fee_rate(self, **kwargs: Any) -> Decimal:
        """Resolve the fee rate to use based on kwargs and defaults.

        Priority:
        1. Explicit fee_pct parameter
        2. Asset-specific fee in asset_fees
        3. Default borrow origination fee

        Args:
            **kwargs: May contain fee_pct, asset

        Returns:
            The resolved fee rate as a Decimal
        """
        # Check for explicit fee percentage
        if "fee_pct" in kwargs:
            return Decimal(str(kwargs["fee_pct"]))

        # Check for asset-specific fee
        asset = kwargs.get("asset")
        if asset and self.asset_fees:
            asset_upper = str(asset).upper()
            if asset_upper in self.asset_fees:
                return self.asset_fees[asset_upper]

        return self.borrow_origination_fee_pct

    @property
    def model_name(self) -> str:
        """Return the unique name of this fee model."""
        return "aave_v3"

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        asset_fees_dict: dict[str, str] = {}
        if self.asset_fees:
            for asset, fee in self.asset_fees.items():
                asset_fees_dict[asset] = str(fee)

        return {
            "model_name": self.model_name,
            "borrow_origination_fee_pct": str(self.borrow_origination_fee_pct),
            "asset_fees": asset_fees_dict,
            "flash_loan_fee_pct": str(self.flash_loan_fee_pct),
        }


__all__ = [
    "AaveV3FeeModel",
]
