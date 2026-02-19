"""Morpho protocol fee model for PnL backtesting.

This module provides a fee model implementation tailored to Morpho's
lending protocol characteristics:

Morpho Blue (latest version):
- Supply: No protocol fee
- Borrow: No origination fee (interest handled separately)
- Withdraw: No protocol fee
- Repay: No protocol fee
- Liquidation: Liquidation incentive factor (LIF) applied to seized collateral

Morpho Optimizer (legacy):
- Routes deposits through Aave/Compound for improved rates
- Fees inherit from underlying protocol
- Small performance fee on rate optimization gains

Note: Morpho is known for being fee-free on most operations.
The value proposition is better rates through P2P matching, not lower fees.
Interest rates are handled separately via portfolio mark-to-market.

Key Components:
    - MorphoFeeModel: Fee model for Morpho lending operations

Example:
    from almanak.framework.backtesting.pnl.fee_models.morpho import MorphoFeeModel

    fee_model = MorphoFeeModel()

    # Most operations are fee-free
    fee = fee_model.calculate_fee(
        intent_type=IntentType.BORROW,
        amount_usd=Decimal("10000"),
    )  # Returns 0

    # Liquidation has incentive (penalty)
    fee = fee_model.calculate_fee(
        intent_type=IntentType.LIQUIDATION,
        amount_usd=Decimal("10000"),
        liquidation_incentive_factor=Decimal("1.05"),
    )
"""

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from almanak.framework.backtesting.models import IntentType
from almanak.framework.backtesting.pnl.fee_models.base import FeeModel


@dataclass
class MorphoFeeModel(FeeModel):
    """Fee model for Morpho lending protocol.

    Morpho Blue is designed to be fee-free for most operations. The protocol
    earns revenue through the spread between supply and borrow rates, not
    through explicit transaction fees.

    Fee Structure:
        - SUPPLY: No fee
        - BORROW: No fee (interest handled separately)
        - WITHDRAW: No fee
        - REPAY: No fee
        - LIQUIDATION: Liquidation incentive factor (e.g., 5% penalty)

    For Morpho Optimizer (legacy), there may be a small performance fee
    on the rate improvement gained through P2P matching.

    Attributes:
        liquidation_incentive_factor: Factor for liquidation penalty
            (default 1.05 = 5% bonus to liquidators, 5% penalty to borrowers)
        performance_fee_pct: Fee on rate optimization gains (Optimizer only)
            (default 0 for Morpho Blue, could be ~15% for Optimizer)
        asset_liquidation_incentives: Per-asset liquidation incentive factors

    Example:
        # Standard Morpho Blue model
        model = MorphoFeeModel()

        # Custom liquidation incentives by asset
        model = MorphoFeeModel(
            liquidation_incentive_factor=Decimal("1.05"),
            asset_liquidation_incentives={
                "WETH": Decimal("1.04"),  # 4% for ETH
                "WBTC": Decimal("1.06"),  # 6% for BTC
            },
        )
    """

    liquidation_incentive_factor: Decimal = Decimal("1.05")  # 5% penalty
    performance_fee_pct: Decimal = Decimal("0")  # 0% for Morpho Blue
    asset_liquidation_incentives: dict[str, Decimal] = field(default_factory=dict)

    # Intents with zero fees in Morpho
    _zero_fee_intents: frozenset[IntentType] = frozenset(
        {
            IntentType.SUPPLY,
            IntentType.BORROW,
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
        """Calculate Morpho fee for a lending operation.

        Most operations in Morpho are fee-free. The only explicit fee is
        the liquidation incentive, which is a bonus paid to liquidators
        (and effectively a penalty to the borrower).

        Args:
            trade_amount: Notional amount of the operation in USD
            **kwargs: Additional parameters:
                - intent_type: Type of intent being executed (default: BORROW)
                - market_state: Current market state at execution time
                - protocol: Protocol being used (ignored, always morpho)
                - asset: Asset symbol for asset-specific liquidation incentive
                - liquidation_incentive_factor: Override liquidation factor
                - is_liquidation: If True, calculates liquidation penalty
                - rate_improvement_usd: For Optimizer, the rate improvement in USD

        Returns:
            Fee amount in USD
        """
        # Get intent type from kwargs
        intent_type = kwargs.get("intent_type", IntentType.BORROW)

        # Check for explicit liquidation flag
        is_liquidation = kwargs.get("is_liquidation", False)

        # Handle liquidation fee (penalty to borrower)
        if is_liquidation:
            return self._calculate_liquidation_fee(trade_amount, **kwargs)

        # Zero-fee intents (which is most of them in Morpho)
        if intent_type in self._zero_fee_intents:
            return Decimal("0")

        # Check for rate improvement fee (Morpho Optimizer only)
        rate_improvement_usd = kwargs.get("rate_improvement_usd")
        if rate_improvement_usd and self.performance_fee_pct > 0:
            return Decimal(str(rate_improvement_usd)) * self.performance_fee_pct

        # Default: no fee
        return Decimal("0")

    def _calculate_liquidation_fee(
        self,
        trade_amount: Decimal,
        **kwargs: Any,
    ) -> Decimal:
        """Calculate the liquidation fee (penalty).

        The liquidation fee represents the incentive paid to liquidators,
        which is effectively a penalty for the borrower being liquidated.

        For example, with a 5% liquidation incentive factor:
        - Borrower debt: $10,000
        - Collateral seized: $10,500 (debt + 5% incentive)
        - Liquidation fee/penalty: $500

        Args:
            trade_amount: Amount of debt being liquidated in USD
            **kwargs: May contain asset, liquidation_incentive_factor

        Returns:
            Liquidation penalty in USD
        """
        # Check for explicit liquidation factor override
        if "liquidation_incentive_factor" in kwargs:
            lif = Decimal(str(kwargs["liquidation_incentive_factor"]))
        else:
            # Check for asset-specific factor
            asset = kwargs.get("asset")
            if asset and self.asset_liquidation_incentives:
                asset_upper = str(asset).upper()
                lif = self.asset_liquidation_incentives.get(asset_upper, self.liquidation_incentive_factor)
            else:
                lif = self.liquidation_incentive_factor

        # Fee is the bonus portion: (factor - 1) * amount
        # e.g., 1.05 factor means 5% bonus, so fee = 0.05 * amount
        return trade_amount * (lif - Decimal("1"))

    @property
    def model_name(self) -> str:
        """Return the unique name of this fee model."""
        return "morpho"

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        asset_incentives_dict: dict[str, str] = {}
        if self.asset_liquidation_incentives:
            for asset, incentive in self.asset_liquidation_incentives.items():
                asset_incentives_dict[asset] = str(incentive)

        return {
            "model_name": self.model_name,
            "liquidation_incentive_factor": str(self.liquidation_incentive_factor),
            "performance_fee_pct": str(self.performance_fee_pct),
            "asset_liquidation_incentives": asset_incentives_dict,
        }


__all__ = [
    "MorphoFeeModel",
]
