"""GMX V2 specific fee model for PnL backtesting.

This module provides a fee model implementation tailored to GMX V2's
perpetuals protocol characteristics:

- Position Open/Close: Position fee (default 0.1%)
- Swap: Swap fee (default 0.05%)
- Leverage: Impact fee based on position size and market conditions

Note: GMX V2 uses a complex fee structure that includes:
- Position fees (opening/closing)
- Borrow fees (hourly, handled separately in funding)
- Price impact fees (based on position size relative to open interest)

Key Components:
    - GMXFeeModel: Fee model for GMX V2 perpetual operations

Example:
    from almanak.framework.backtesting.pnl.fee_models.gmx import GMXFeeModel

    fee_model = GMXFeeModel(position_fee_pct=Decimal("0.001"))

    fee = fee_model.calculate_fee(
        intent_type=IntentType.PERP_OPEN,
        amount_usd=Decimal("10000"),
        market_state=market_state,
        leverage=Decimal("10"),  # Optional leverage parameter
    )
"""

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from almanak.framework.backtesting.models import IntentType
from almanak.framework.backtesting.pnl.fee_models.base import FeeModel


@dataclass
class GMXFeeModel(FeeModel):
    """Fee model for GMX V2 perpetuals protocol.

    GMX V2 charges fees on perpetual position operations. The fee structure
    includes position fees for opening/closing and swap fees for spot swaps.

    Fee Structure:
        - PERP_OPEN: Position fee (configurable, default 0.1%)
        - PERP_CLOSE: Position fee (configurable, default 0.1%)
        - SWAP: Swap fee (configurable, default 0.05%)
        - Other intents: No fee

    Position fees are calculated on the notional position size (amount * leverage).
    For example, a $1,000 position with 10x leverage has a $10,000 notional size,
    resulting in a $10 fee at 0.1% fee rate.

    Attributes:
        position_fee_pct: Fee for opening/closing positions (default 0.1% = 0.001)
        swap_fee_pct: Fee for swap operations (default 0.05% = 0.0005)
        asset_fees: Optional mapping of asset symbols to their specific
            position fee percentages (overrides default)
        execution_fee_usd: Fixed execution fee in USD (default $0.50)
            Note: This is a simplified model; actual GMX uses gas-based fees

    Example:
        # Simple uniform fee
        model = GMXFeeModel(position_fee_pct=Decimal("0.001"))

        # Per-asset fees
        model = GMXFeeModel(
            position_fee_pct=Decimal("0.001"),
            asset_fees={
                "ETH": Decimal("0.0008"),  # Lower fee for ETH
                "BTC": Decimal("0.0008"),  # Lower fee for BTC
                "ARB": Decimal("0.0015"),  # Higher fee for smaller markets
            },
        )
    """

    position_fee_pct: Decimal = Decimal("0.001")  # 0.1% default position fee
    swap_fee_pct: Decimal = Decimal("0.0005")  # 0.05% default swap fee
    asset_fees: dict[str, Decimal] | None = None
    execution_fee_usd: Decimal = Decimal("0.50")  # Fixed execution fee

    # Intents with zero fees in GMX
    _zero_fee_intents: frozenset[IntentType] = frozenset(
        {
            IntentType.HOLD,
            IntentType.SUPPLY,
            IntentType.WITHDRAW,
            IntentType.REPAY,
            IntentType.BORROW,
            IntentType.LP_OPEN,
            IntentType.LP_CLOSE,
            IntentType.BRIDGE,
        }
    )

    def calculate_fee(
        self,
        trade_amount: Decimal,
        **kwargs: Any,
    ) -> Decimal:
        """Calculate GMX V2 fee for a perpetual operation.

        PERP_OPEN and PERP_CLOSE incur position fees. SWAP incurs swap fees.
        Other operations have no protocol fees.

        Args:
            trade_amount: Notional amount of the operation in USD
            **kwargs: Additional parameters:
                - intent_type: Type of intent being executed (default: PERP_OPEN)
                - market_state: Current market state at execution time
                - protocol: Protocol being used (ignored, always gmx)
                - asset: Asset symbol for asset-specific fee lookup
                - leverage: Leverage multiplier (position fee applies to leveraged size)
                - fee_pct: Explicit fee percentage override
                - include_execution_fee: If True, adds fixed execution fee (default True)

        Returns:
            Fee amount in USD
        """
        # Get intent type from kwargs, default to PERP_OPEN
        intent_type = kwargs.get("intent_type", IntentType.PERP_OPEN)

        # Zero-fee intents
        if intent_type in self._zero_fee_intents:
            return Decimal("0")

        # Handle SWAP
        if intent_type == IntentType.SWAP:
            return trade_amount * self.swap_fee_pct

        # Only PERP_OPEN and PERP_CLOSE have position fees
        if intent_type not in (IntentType.PERP_OPEN, IntentType.PERP_CLOSE):
            return Decimal("0")

        # Determine fee rate to use
        fee_rate = self._resolve_fee_rate(**kwargs)

        # Calculate notional size with leverage
        leverage = Decimal(str(kwargs.get("leverage", "1")))
        notional_size = trade_amount * leverage

        # Calculate position fee
        position_fee = notional_size * fee_rate

        # Add execution fee if requested (default True)
        include_execution_fee = kwargs.get("include_execution_fee", True)
        if include_execution_fee:
            position_fee += self.execution_fee_usd

        return position_fee

    def _resolve_fee_rate(self, **kwargs: Any) -> Decimal:
        """Resolve the fee rate to use based on kwargs and defaults.

        Priority:
        1. Explicit fee_pct parameter
        2. Asset-specific fee in asset_fees
        3. Default position fee

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

        return self.position_fee_pct

    @property
    def model_name(self) -> str:
        """Return the unique name of this fee model."""
        return "gmx"

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        asset_fees_dict: dict[str, str] = {}
        if self.asset_fees:
            for asset, fee in self.asset_fees.items():
                asset_fees_dict[asset] = str(fee)

        return {
            "model_name": self.model_name,
            "position_fee_pct": str(self.position_fee_pct),
            "swap_fee_pct": str(self.swap_fee_pct),
            "asset_fees": asset_fees_dict,
            "execution_fee_usd": str(self.execution_fee_usd),
        }


__all__ = [
    "GMXFeeModel",
]
