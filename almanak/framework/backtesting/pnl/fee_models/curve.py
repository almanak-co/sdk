"""Curve Finance specific fee model for PnL backtesting.

This module provides a fee model implementation tailored to Curve's
DEX characteristics:

- Dynamic fees: Fees adjust based on pool imbalance (higher when imbalanced)
- Admin fees: A portion of swap fees goes to the protocol
- Pool-specific base fees: Different pools can have different base fee rates

Key Components:
    - CurvePoolType: Enum for different Curve pool types
    - CurveFeeModel: Fee model with dynamic fee calculation

Example:
    from almanak.framework.backtesting.pnl.fee_models.curve import (
        CurveFeeModel,
        CurvePoolType,
    )

    fee_model = CurveFeeModel()

    # Standard stable pool swap
    fee = fee_model.calculate_fee(
        Decimal("1000"),
        intent_type=IntentType.SWAP,
        pool_type=CurvePoolType.STABLE,
    )

    # Tricrypto pool swap with imbalance
    fee = fee_model.calculate_fee(
        Decimal("1000"),
        intent_type=IntentType.SWAP,
        pool_type=CurvePoolType.TRICRYPTO,
        pool_imbalance=Decimal("0.1"),  # 10% imbalance
    )
"""

from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from typing import Any

from almanak.framework.backtesting.models import IntentType
from almanak.framework.backtesting.pnl.fee_models.base import FeeModel


class CurvePoolType(StrEnum):
    """Curve pool types with different fee structures.

    Each pool type has different characteristics:
    - STABLE: Plain stablecoin pools (e.g., 3pool), lowest fees
    - METAPOOL: Pools paired against base pools (e.g., FRAX/3CRV)
    - TRICRYPTO: Pools with 3 volatile assets (e.g., USDT/WBTC/WETH)
    - FACTORY: User-deployed pools with configurable fees
    - CRVUSD: Pools with crvUSD stablecoin
    """

    STABLE = "stable"
    METAPOOL = "metapool"
    TRICRYPTO = "tricrypto"
    FACTORY = "factory"
    CRVUSD = "crvusd"


# Base fees for each pool type (before dynamic adjustment)
CURVE_BASE_FEES: dict[CurvePoolType, Decimal] = {
    CurvePoolType.STABLE: Decimal("0.0004"),  # 0.04% = 4 bps
    CurvePoolType.METAPOOL: Decimal("0.0004"),  # 0.04%
    CurvePoolType.TRICRYPTO: Decimal("0.0013"),  # 0.13% (dynamic, this is mid-fee)
    CurvePoolType.FACTORY: Decimal("0.0004"),  # 0.04% default
    CurvePoolType.CRVUSD: Decimal("0.0001"),  # 0.01% for crvUSD pools
}


# Admin fee percentage (portion of swap fees that go to Curve DAO)
CURVE_ADMIN_FEE_SHARE = Decimal("0.5")  # 50% of fees to admin


@dataclass
class CurveFeeModel(FeeModel):
    """Fee model for Curve Finance DEX.

    Curve uses a dynamic fee model where fees can increase when pools
    become imbalanced. This helps maintain pool equilibrium by charging
    more for trades that would further imbalance the pool.

    Fee Structure:
        - STABLE pools: 0.04% base fee
        - METAPOOL pools: 0.04% base fee
        - TRICRYPTO pools: Dynamic fee (0.02% to 0.45% based on imbalance)
        - FACTORY pools: Configurable, default 0.04%
        - CRVUSD pools: 0.01% base fee

    The fee increases with pool imbalance using the formula:
        actual_fee = base_fee * (1 + imbalance_multiplier * pool_imbalance)

    Attributes:
        base_fees: Mapping of pool types to base fee percentages
        admin_fee_share: Share of fees going to protocol (default 50%)
        max_fee_multiplier: Maximum fee multiplier for imbalanced pools (default 3x)
        imbalance_sensitivity: How much imbalance affects fees (default 2)

    Example:
        model = CurveFeeModel()

        # Basic stable swap
        fee = model.calculate_fee(
            Decimal("1000"),
            intent_type=IntentType.SWAP,
            pool_type=CurvePoolType.STABLE,
        )

        # Tricrypto swap with 20% pool imbalance
        fee = model.calculate_fee(
            Decimal("1000"),
            intent_type=IntentType.SWAP,
            pool_type=CurvePoolType.TRICRYPTO,
            pool_imbalance=Decimal("0.2"),
        )
    """

    base_fees: dict[CurvePoolType, Decimal] | None = None
    admin_fee_share: Decimal = Decimal("0.5")  # 50% to admin
    max_fee_multiplier: Decimal = Decimal("3")  # Max 3x base fee
    imbalance_sensitivity: Decimal = Decimal("2")  # How quickly fee increases

    def __post_init__(self) -> None:
        """Initialize base fees with defaults if not provided."""
        if self.base_fees is None:
            self.base_fees = dict(CURVE_BASE_FEES)

    def calculate_fee(
        self,
        trade_amount: Decimal,
        **kwargs: Any,
    ) -> Decimal:
        """Calculate Curve fee for an intent with dynamic fee adjustment.

        Only SWAP intents incur fees. LP_OPEN and LP_CLOSE have no direct
        protocol fees (though depositing/withdrawing may have imbalance penalties).

        Args:
            trade_amount: Notional amount of the trade in USD
            **kwargs: Additional parameters:
                - intent_type: Type of intent being executed (default: SWAP)
                - market_state: Current market state at execution time
                - protocol: Protocol being used (ignored, always curve)
                - pool_type: CurvePoolType for fee lookup
                - pool_imbalance: Current pool imbalance ratio (0-1, default 0)
                - fee_pct: Explicit fee percentage override
                - base_fee: Explicit base fee override

        Returns:
            Fee amount in USD
        """
        # Get intent type from kwargs, default to SWAP
        intent_type = kwargs.get("intent_type", IntentType.SWAP)

        # LP operations have no direct swap fee
        if intent_type in (IntentType.LP_OPEN, IntentType.LP_CLOSE):
            return Decimal("0")

        # Only SWAP has fees
        if intent_type != IntentType.SWAP:
            return Decimal("0")

        # Determine the fee rate to use
        fee_rate = self._calculate_dynamic_fee(**kwargs)

        # Calculate fee
        return trade_amount * fee_rate

    def _calculate_dynamic_fee(self, **kwargs: Any) -> Decimal:
        """Calculate the dynamic fee based on pool type and imbalance.

        The fee formula is:
            fee = base_fee * (1 + sensitivity * imbalance^2)

        Capped at max_fee_multiplier * base_fee.

        Args:
            **kwargs: May contain fee_pct, base_fee, pool_type, pool_imbalance

        Returns:
            The calculated fee rate as a Decimal
        """
        # Check for explicit fee percentage
        if "fee_pct" in kwargs:
            return Decimal(str(kwargs["fee_pct"]))

        # Determine base fee from pool type
        base_fee = self._resolve_base_fee(**kwargs)

        # Get pool imbalance (0-1 range, 0 = perfectly balanced)
        pool_imbalance = Decimal(str(kwargs.get("pool_imbalance", "0")))

        # Clamp imbalance to valid range
        pool_imbalance = max(Decimal("0"), min(Decimal("1"), pool_imbalance))

        # Calculate fee multiplier based on imbalance
        # Use quadratic scaling for smoother response
        imbalance_factor = self.imbalance_sensitivity * pool_imbalance * pool_imbalance
        fee_multiplier = Decimal("1") + imbalance_factor

        # Cap at max multiplier
        fee_multiplier = min(fee_multiplier, self.max_fee_multiplier)

        return base_fee * fee_multiplier

    def _resolve_base_fee(self, **kwargs: Any) -> Decimal:
        """Resolve the base fee from kwargs or pool type.

        Priority:
        1. Explicit base_fee parameter
        2. Pool type lookup
        3. Default stable pool fee

        Args:
            **kwargs: May contain base_fee, pool_type

        Returns:
            The base fee rate as a Decimal
        """
        # Check for explicit base fee
        if "base_fee" in kwargs:
            return Decimal(str(kwargs["base_fee"]))

        # Determine pool type
        pool_type = self._resolve_pool_type(**kwargs)

        # Look up base fee
        if self.base_fees and pool_type in self.base_fees:
            return self.base_fees[pool_type]

        # Fallback to default stable fee
        return CURVE_BASE_FEES[CurvePoolType.STABLE]

    def _resolve_pool_type(self, **kwargs: Any) -> CurvePoolType:
        """Resolve the pool type from kwargs.

        Args:
            **kwargs: May contain pool_type

        Returns:
            The resolved CurvePoolType (defaults to STABLE)
        """
        if "pool_type" in kwargs:
            pool_type = kwargs["pool_type"]
            if isinstance(pool_type, CurvePoolType):
                return pool_type
            # Handle string value
            if isinstance(pool_type, str):
                pool_type_lower = pool_type.lower()
                for pt in CurvePoolType:
                    if pt.value == pool_type_lower or pt.name.lower() == pool_type_lower:
                        return pt

        return CurvePoolType.STABLE

    def estimate_imbalance_fee_impact(
        self,
        trade_amount: Decimal,
        pool_type: CurvePoolType = CurvePoolType.STABLE,
    ) -> dict[str, Decimal]:
        """Estimate fee impact at various imbalance levels.

        Useful for understanding how fees change with pool imbalance.

        Args:
            trade_amount: Trade amount in USD
            pool_type: Pool type for base fee lookup

        Returns:
            Dictionary mapping imbalance levels to fees
        """
        imbalance_levels = [
            ("balanced", Decimal("0")),
            ("slight", Decimal("0.05")),
            ("moderate", Decimal("0.15")),
            ("high", Decimal("0.30")),
            ("extreme", Decimal("0.50")),
        ]

        result = {}
        for name, imbalance in imbalance_levels:
            fee = self.calculate_fee(
                trade_amount,
                intent_type=IntentType.SWAP,
                pool_type=pool_type,
                pool_imbalance=imbalance,
            )
            result[name] = fee

        return result

    @property
    def model_name(self) -> str:
        """Return the unique name of this fee model."""
        return "curve"

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        base_fees_dict: dict[str, str] = {}
        if self.base_fees:
            for pool_type, fee in self.base_fees.items():
                base_fees_dict[pool_type.value] = str(fee)

        return {
            "model_name": self.model_name,
            "base_fees": base_fees_dict,
            "admin_fee_share": str(self.admin_fee_share),
            "max_fee_multiplier": str(self.max_fee_multiplier),
            "imbalance_sensitivity": str(self.imbalance_sensitivity),
        }


__all__ = [
    "CurvePoolType",
    "CurveFeeModel",
    "CURVE_BASE_FEES",
    "CURVE_ADMIN_FEE_SHARE",
]
