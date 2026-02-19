"""Compound V3 (Comet) protocol fee model for PnL backtesting.

This module provides a fee model implementation tailored to Compound V3's
(also known as Comet) lending protocol characteristics:

Fee Structure:
- Supply: No protocol fee (earn interest)
- Borrow: No origination fee (pay interest)
- Withdraw: No protocol fee
- Repay: No protocol fee
- Liquidation: Liquidation penalty varies by asset (storeFrontPriceFactor)

Key Differences from Compound V2:
- Compound V3 is a simpler, single-asset model (one base asset per market)
- No cToken minting - direct accounting in the Comet contract
- Streamlined liquidation with asset-specific discount factors
- Collateral assets cannot be borrowed (supply-only for collateral)

Note: Interest rates are handled separately via portfolio mark-to-market.
This model focuses on explicit transaction fees, not ongoing interest.

Key Components:
    - CompoundV3FeeModel: Fee model for Compound V3 lending operations
    - CompoundV3Market: Enum of supported Compound V3 markets

Example:
    from almanak.framework.backtesting.pnl.fee_models.compound_v3 import (
        CompoundV3FeeModel,
        CompoundV3Market,
    )

    fee_model = CompoundV3FeeModel()

    # Most operations are fee-free
    fee = fee_model.calculate_fee(
        intent_type=IntentType.BORROW,
        amount_usd=Decimal("10000"),
    )  # Returns 0

    # Liquidation has discount (penalty to borrower)
    fee = fee_model.calculate_fee(
        intent_type=IntentType.BORROW,
        amount_usd=Decimal("10000"),
        is_liquidation=True,
        asset="WETH",
    )
"""

from dataclasses import dataclass, field
from decimal import Decimal
from enum import StrEnum
from typing import Any

from almanak.framework.backtesting.models import IntentType
from almanak.framework.backtesting.pnl.fee_models.base import FeeModel


class CompoundV3Market(StrEnum):
    """Compound V3 market identifiers.

    Compound V3 uses isolated markets, each with a single base asset
    that can be borrowed and multiple collateral assets.
    """

    # Ethereum mainnet markets
    USDC_MAINNET = "usdc_mainnet"
    WETH_MAINNET = "weth_mainnet"

    # Arbitrum markets
    USDC_ARBITRUM = "usdc_arbitrum"
    WETH_ARBITRUM = "weth_arbitrum"

    # Base markets
    USDC_BASE = "usdc_base"
    WETH_BASE = "weth_base"
    USDC_BASE_V2 = "usdc_base_v2"  # Newer USDC market on Base

    # Optimism markets
    USDC_OPTIMISM = "usdc_optimism"
    WETH_OPTIMISM = "weth_optimism"

    # Polygon markets
    USDC_POLYGON = "usdc_polygon"


# Compound V3 liquidation discounts (storeFrontPriceFactor)
# These represent the discount liquidators receive when buying collateral
# Higher discount = more penalty to borrower
# Values are based on Compound V3 governance parameters
DEFAULT_LIQUIDATION_DISCOUNTS: dict[str, Decimal] = {
    # Major assets - lower discount
    "WETH": Decimal("0.05"),  # 5% discount
    "WBTC": Decimal("0.05"),  # 5% discount
    "CBETH": Decimal("0.07"),  # 7% discount (LST)
    "WSTETH": Decimal("0.07"),  # 7% discount (LST)
    "RETH": Decimal("0.07"),  # 7% discount (LST)
    # Stablecoins (when used as collateral)
    "USDC": Decimal("0.03"),  # 3% discount
    "USDT": Decimal("0.03"),  # 3% discount
    "DAI": Decimal("0.03"),  # 3% discount
    # Other assets
    "LINK": Decimal("0.08"),  # 8% discount
    "UNI": Decimal("0.10"),  # 10% discount
    "COMP": Decimal("0.08"),  # 8% discount
    "ARB": Decimal("0.10"),  # 10% discount
    "OP": Decimal("0.10"),  # 10% discount
}


@dataclass
class CompoundV3FeeModel(FeeModel):
    """Fee model for Compound V3 (Comet) lending protocol.

    Compound V3 is designed with minimal fees for most operations.
    The primary "fee" is the liquidation discount that penalizes
    borrowers who get liquidated.

    Fee Structure:
        - SUPPLY: No fee
        - BORROW: No fee (interest handled separately)
        - WITHDRAW: No fee
        - REPAY: No fee
        - LIQUIDATION: Asset-specific discount (e.g., 5-10%)

    The liquidation discount is the difference between the collateral's
    market value and the price paid by the liquidator. This incentivizes
    liquidators and penalizes underwater borrowers.

    Attributes:
        default_liquidation_discount: Default discount for unlisted assets
            (default 0.08 = 8%)
        liquidation_discounts: Per-asset liquidation discount rates
        market: Optional market identifier for market-specific behavior
        absorb_fee_pct: Fee for protocol absorb() calls (governance param)
            (default 0 - typically 0 but can be set by governance)

    Example:
        # Standard model
        model = CompoundV3FeeModel()

        # Custom liquidation discounts
        model = CompoundV3FeeModel(
            default_liquidation_discount=Decimal("0.10"),
            liquidation_discounts={
                "WETH": Decimal("0.05"),
                "WBTC": Decimal("0.05"),
            },
        )
    """

    default_liquidation_discount: Decimal = Decimal("0.08")  # 8% default
    liquidation_discounts: dict[str, Decimal] = field(default_factory=lambda: DEFAULT_LIQUIDATION_DISCOUNTS.copy())
    market: CompoundV3Market | str | None = None
    absorb_fee_pct: Decimal = Decimal("0")  # Governance-controlled fee on absorb

    # Intents with zero fees in Compound V3
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
        """Calculate Compound V3 fee for a lending operation.

        Most operations in Compound V3 are fee-free. The only explicit fee
        is the liquidation discount, which is the penalty incurred by
        borrowers who are liquidated.

        Args:
            trade_amount: Notional amount of the operation in USD
            **kwargs: Additional parameters:
                - intent_type: Type of intent being executed (default: BORROW)
                - market_state: Current market state at execution time
                - protocol: Protocol being used (ignored, always compound_v3)
                - asset: Asset symbol for asset-specific liquidation discount
                - is_liquidation: If True, calculates liquidation penalty
                - liquidation_discount: Override the discount rate
                - is_absorb: If True, uses absorb fee (governance param)

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

        # Check for absorb operation (protocol buys collateral at discount)
        if kwargs.get("is_absorb") and self.absorb_fee_pct > 0:
            return trade_amount * self.absorb_fee_pct

        # Zero-fee intents (which is most of them in Compound V3)
        if intent_type in self._zero_fee_intents:
            return Decimal("0")

        # Default: no fee
        return Decimal("0")

    def _calculate_liquidation_fee(
        self,
        trade_amount: Decimal,
        **kwargs: Any,
    ) -> Decimal:
        """Calculate the liquidation fee (penalty).

        The liquidation fee represents the discount given to liquidators,
        which is effectively a penalty for the borrower being liquidated.

        For example, with a 5% liquidation discount:
        - Collateral value: $10,000
        - Liquidator pays: $9,500 (10,000 * (1 - 0.05))
        - Liquidation fee/penalty: $500

        Args:
            trade_amount: Amount of collateral being liquidated in USD
            **kwargs: May contain asset, liquidation_discount

        Returns:
            Liquidation penalty in USD
        """
        # Check for explicit discount override
        if "liquidation_discount" in kwargs:
            discount = Decimal(str(kwargs["liquidation_discount"]))
        else:
            # Check for asset-specific discount
            asset = kwargs.get("asset")
            if asset:
                asset_upper = str(asset).upper()
                discount = self.liquidation_discounts.get(asset_upper, self.default_liquidation_discount)
            else:
                discount = self.default_liquidation_discount

        # Fee is the discount amount
        return trade_amount * discount

    def get_liquidation_discount(self, asset: str) -> Decimal:
        """Get the liquidation discount for a specific asset.

        Args:
            asset: Asset symbol (e.g., "WETH", "WBTC")

        Returns:
            Liquidation discount rate as a Decimal (e.g., 0.05 for 5%)
        """
        asset_upper = asset.upper()
        return self.liquidation_discounts.get(asset_upper, self.default_liquidation_discount)

    @property
    def model_name(self) -> str:
        """Return the unique name of this fee model."""
        return "compound_v3"

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        discounts_dict: dict[str, str] = {
            asset: str(discount) for asset, discount in self.liquidation_discounts.items()
        }

        market_str: str | None = None
        if self.market:
            market_str = self.market.value if isinstance(self.market, CompoundV3Market) else str(self.market)

        return {
            "model_name": self.model_name,
            "default_liquidation_discount": str(self.default_liquidation_discount),
            "liquidation_discounts": discounts_dict,
            "market": market_str,
            "absorb_fee_pct": str(self.absorb_fee_pct),
        }


__all__ = [
    "CompoundV3FeeModel",
    "CompoundV3Market",
    "DEFAULT_LIQUIDATION_DISCOUNTS",
]
