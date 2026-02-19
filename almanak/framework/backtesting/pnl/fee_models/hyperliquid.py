"""Hyperliquid specific fee model for PnL backtesting.

This module provides a fee model implementation tailored to Hyperliquid's
perpetuals protocol characteristics:

- Maker/Taker Fees: Different rates for maker and taker orders
- Volume-Based Tiers: Fee discounts based on 14-day rolling volume
- HYPE Staking Discounts: Additional fee reduction for staked HYPE holders
- HIP-3 Markets: Deployer perp markets with different fee structure

Note: Hyperliquid uses a weighted volume formula:
    weighted_volume = perps_volume + 2 * spot_volume

Fee tiers are determined by 14-day rolling weighted volume:
    - VIP 0: < $5M volume - 0.045% taker, 0.015% maker
    - VIP 1: >= $5M volume - 0.040% taker, 0.012% maker
    - VIP 2: >= $25M volume - 0.035% taker, 0.008% maker
    - VIP 3: >= $100M volume - 0.030% taker, 0.004% maker
    - VIP 4: >= $500M volume - 0.028% taker, 0.002% maker
    - VIP 5: >= $1B volume - 0.027% taker, 0.001% maker
    - VIP 6: >= $2B volume - 0.026% taker, 0% maker (rebate)

Key Components:
    - HyperliquidFeeTier: Enum for volume-based fee tiers
    - HyperliquidFeeModel: Fee model for Hyperliquid perpetual operations

Example:
    from almanak.framework.backtesting.pnl.fee_models.hyperliquid import (
        HyperliquidFeeModel,
        HyperliquidFeeTier,
    )

    # Basic usage with VIP 0 (default)
    fee_model = HyperliquidFeeModel()
    fee = fee_model.calculate_fee(
        trade_amount=Decimal("10000"),
        is_maker=False,  # Taker order
    )

    # With specific tier
    fee_model = HyperliquidFeeModel(fee_tier=HyperliquidFeeTier.VIP_3)
    fee = fee_model.calculate_fee(Decimal("10000"), is_maker=True)

    # With custom 14-day volume (auto-selects tier)
    fee_model = HyperliquidFeeModel(volume_14d=Decimal("150000000"))  # $150M
    fee = fee_model.calculate_fee(Decimal("10000"), is_maker=False)
"""

from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Any

from almanak.framework.backtesting.pnl.fee_models.base import FeeModel


class HyperliquidFeeTier(Enum):
    """Volume-based fee tiers for Hyperliquid.

    Tiers are determined by 14-day rolling weighted trading volume.
    Higher volumes unlock lower fees and better maker rebates.

    Attributes:
        VIP_0: Base tier, <$5M volume
        VIP_1: $5M+ volume
        VIP_2: $25M+ volume
        VIP_3: $100M+ volume
        VIP_4: $500M+ volume
        VIP_5: $1B+ volume
        VIP_6: $2B+ volume (highest tier, 0% maker fee)
    """

    VIP_0 = "vip_0"  # < $5M volume
    VIP_1 = "vip_1"  # >= $5M volume
    VIP_2 = "vip_2"  # >= $25M volume
    VIP_3 = "vip_3"  # >= $100M volume
    VIP_4 = "vip_4"  # >= $500M volume
    VIP_5 = "vip_5"  # >= $1B volume
    VIP_6 = "vip_6"  # >= $2B volume


# Fee rates by tier: (taker_fee, maker_fee) as Decimal percentages
FEE_RATES_BY_TIER: dict[HyperliquidFeeTier, tuple[Decimal, Decimal]] = {
    HyperliquidFeeTier.VIP_0: (Decimal("0.00045"), Decimal("0.00015")),  # 0.045%, 0.015%
    HyperliquidFeeTier.VIP_1: (Decimal("0.00040"), Decimal("0.00012")),  # 0.040%, 0.012%
    HyperliquidFeeTier.VIP_2: (Decimal("0.00035"), Decimal("0.00008")),  # 0.035%, 0.008%
    HyperliquidFeeTier.VIP_3: (Decimal("0.00030"), Decimal("0.00004")),  # 0.030%, 0.004%
    HyperliquidFeeTier.VIP_4: (Decimal("0.00028"), Decimal("0.00002")),  # 0.028%, 0.002%
    HyperliquidFeeTier.VIP_5: (Decimal("0.00027"), Decimal("0.00001")),  # 0.027%, 0.001%
    HyperliquidFeeTier.VIP_6: (Decimal("0.00026"), Decimal("0")),  # 0.026%, 0%
}

# Volume thresholds for tier selection (in USD)
VOLUME_THRESHOLDS: list[tuple[Decimal, HyperliquidFeeTier]] = [
    (Decimal("2000000000"), HyperliquidFeeTier.VIP_6),  # $2B
    (Decimal("1000000000"), HyperliquidFeeTier.VIP_5),  # $1B
    (Decimal("500000000"), HyperliquidFeeTier.VIP_4),  # $500M
    (Decimal("100000000"), HyperliquidFeeTier.VIP_3),  # $100M
    (Decimal("25000000"), HyperliquidFeeTier.VIP_2),  # $25M
    (Decimal("5000000"), HyperliquidFeeTier.VIP_1),  # $5M
    (Decimal("0"), HyperliquidFeeTier.VIP_0),  # < $5M
]

# HIP-3 market fee multiplier (deployed perp markets have 2x base fees)
HIP3_FEE_MULTIPLIER = Decimal("2")

# HYPE staking discount tiers: (staked_hype, discount_percentage)
STAKING_DISCOUNTS: list[tuple[Decimal, Decimal]] = [
    (Decimal("100000"), Decimal("0.10")),  # 100k HYPE = 10% discount
    (Decimal("50000"), Decimal("0.08")),  # 50k HYPE = 8% discount
    (Decimal("10000"), Decimal("0.05")),  # 10k HYPE = 5% discount
    (Decimal("1000"), Decimal("0.02")),  # 1k HYPE = 2% discount
]


def get_tier_from_volume(volume_14d: Decimal) -> HyperliquidFeeTier:
    """Determine fee tier based on 14-day rolling volume.

    Args:
        volume_14d: 14-day weighted trading volume in USD

    Returns:
        The appropriate fee tier for the volume
    """
    for threshold, tier in VOLUME_THRESHOLDS:
        if volume_14d >= threshold:
            return tier
    return HyperliquidFeeTier.VIP_0


def get_staking_discount(staked_hype: Decimal) -> Decimal:
    """Get fee discount percentage based on staked HYPE amount.

    Args:
        staked_hype: Amount of HYPE tokens staked

    Returns:
        Discount percentage as decimal (e.g., 0.10 for 10% discount)
    """
    for threshold, discount in STAKING_DISCOUNTS:
        if staked_hype >= threshold:
            return discount
    return Decimal("0")


@dataclass
class HyperliquidFeeModel(FeeModel):
    """Fee model for Hyperliquid perpetuals protocol.

    Hyperliquid uses a tiered fee structure based on 14-day rolling
    trading volume, with separate maker and taker rates. Additional
    discounts are available for HYPE token stakers.

    Fee Structure:
        - Taker fees: 0.026% - 0.045% depending on tier
        - Maker fees: 0% - 0.015% depending on tier
        - HIP-3 markets: 2x base fees (deployer perp markets)

    Attributes:
        fee_tier: Current fee tier based on volume (default: VIP_0)
        volume_14d: 14-day rolling weighted volume in USD (used to auto-select tier)
        staked_hype: Amount of HYPE staked for fee discount
        is_hip3_market: Whether trading on HIP-3 deployer market (2x fees)
        custom_taker_fee: Override taker fee rate (optional)
        custom_maker_fee: Override maker fee rate (optional)

    Example:
        # Default VIP 0 tier
        model = HyperliquidFeeModel()

        # With specific volume-based tier
        model = HyperliquidFeeModel(volume_14d=Decimal("50000000"))  # $50M -> VIP 2

        # With staking discount
        model = HyperliquidFeeModel(
            fee_tier=HyperliquidFeeTier.VIP_0,
            staked_hype=Decimal("10000"),  # 5% discount
        )

        # HIP-3 market (2x base fees)
        model = HyperliquidFeeModel(is_hip3_market=True)
    """

    fee_tier: HyperliquidFeeTier = HyperliquidFeeTier.VIP_0
    volume_14d: Decimal | None = None
    staked_hype: Decimal = Decimal("0")
    is_hip3_market: bool = False
    custom_taker_fee: Decimal | None = None
    custom_maker_fee: Decimal | None = None

    # Cache for computed fees
    _taker_fee: Decimal = field(init=False, repr=False, default=Decimal("0"))
    _maker_fee: Decimal = field(init=False, repr=False, default=Decimal("0"))

    def __post_init__(self) -> None:
        """Initialize fee rates based on tier and modifiers."""
        # Auto-select tier from volume if provided
        if self.volume_14d is not None:
            self.fee_tier = get_tier_from_volume(self.volume_14d)

        # Get base rates for tier
        base_taker, base_maker = FEE_RATES_BY_TIER[self.fee_tier]

        # Apply HIP-3 market multiplier if applicable
        if self.is_hip3_market:
            base_taker = base_taker * HIP3_FEE_MULTIPLIER
            base_maker = base_maker * HIP3_FEE_MULTIPLIER

        # Apply staking discount
        staking_discount = get_staking_discount(self.staked_hype)
        discount_multiplier = Decimal("1") - staking_discount

        # Store computed rates (or custom overrides)
        self._taker_fee = (
            self.custom_taker_fee if self.custom_taker_fee is not None else base_taker * discount_multiplier
        )
        self._maker_fee = (
            self.custom_maker_fee if self.custom_maker_fee is not None else base_maker * discount_multiplier
        )

    @property
    def taker_fee_rate(self) -> Decimal:
        """Get the current taker fee rate.

        Returns:
            Taker fee rate as decimal (e.g., 0.00045 for 0.045%)
        """
        return self._taker_fee

    @property
    def maker_fee_rate(self) -> Decimal:
        """Get the current maker fee rate.

        Returns:
            Maker fee rate as decimal (e.g., 0.00015 for 0.015%)
        """
        return self._maker_fee

    @property
    def taker_fee_bps(self) -> Decimal:
        """Get taker fee in basis points.

        Returns:
            Taker fee in bps (e.g., 4.5 for 0.045%)
        """
        return self._taker_fee * Decimal("10000")

    @property
    def maker_fee_bps(self) -> Decimal:
        """Get maker fee in basis points.

        Returns:
            Maker fee in bps (e.g., 1.5 for 0.015%)
        """
        return self._maker_fee * Decimal("10000")

    def calculate_fee(
        self,
        trade_amount: Decimal,
        **kwargs: Any,
    ) -> Decimal:
        """Calculate Hyperliquid fee for a perpetual operation.

        Args:
            trade_amount: Notional amount of the operation in USD
            **kwargs: Additional parameters:
                - is_maker: Whether the order is a maker order (default: False)
                - leverage: Leverage multiplier (fee applies to full notional)
                - intent_type: Type of intent being executed (optional)

        Returns:
            Fee amount in USD

        Example:
            # Taker order
            fee = model.calculate_fee(Decimal("10000"), is_maker=False)

            # Maker order
            fee = model.calculate_fee(Decimal("10000"), is_maker=True)

            # With leverage (fee applies to full notional)
            fee = model.calculate_fee(Decimal("1000"), leverage=10)  # $10k notional
        """
        # Determine fee rate based on order type
        is_maker = kwargs.get("is_maker", False)
        fee_rate = self._maker_fee if is_maker else self._taker_fee

        # Apply leverage if provided (fee applies to full notional)
        leverage = Decimal(str(kwargs.get("leverage", "1")))
        notional_size = trade_amount * leverage

        # Calculate fee
        return notional_size * fee_rate

    def calculate_maker_fee(self, trade_amount: Decimal, leverage: Decimal = Decimal("1")) -> Decimal:
        """Calculate maker fee for a trade.

        Convenience method for maker orders.

        Args:
            trade_amount: Trade amount in USD (collateral)
            leverage: Leverage multiplier (default: 1)

        Returns:
            Maker fee in USD
        """
        return self.calculate_fee(trade_amount, is_maker=True, leverage=leverage)

    def calculate_taker_fee(self, trade_amount: Decimal, leverage: Decimal = Decimal("1")) -> Decimal:
        """Calculate taker fee for a trade.

        Convenience method for taker orders.

        Args:
            trade_amount: Trade amount in USD (collateral)
            leverage: Leverage multiplier (default: 1)

        Returns:
            Taker fee in USD
        """
        return self.calculate_fee(trade_amount, is_maker=False, leverage=leverage)

    def get_fee_summary(self) -> dict[str, Any]:
        """Get a summary of current fee configuration.

        Returns:
            Dictionary with fee tier, rates, and any applicable discounts
        """
        return {
            "tier": self.fee_tier.value,
            "taker_fee_pct": f"{self._taker_fee * 100:.4f}%",
            "maker_fee_pct": f"{self._maker_fee * 100:.4f}%",
            "taker_fee_bps": float(self.taker_fee_bps),
            "maker_fee_bps": float(self.maker_fee_bps),
            "volume_14d": str(self.volume_14d) if self.volume_14d else None,
            "staked_hype": str(self.staked_hype),
            "staking_discount_pct": f"{get_staking_discount(self.staked_hype) * 100:.1f}%",
            "is_hip3_market": self.is_hip3_market,
        }

    @property
    def model_name(self) -> str:
        """Return the unique name of this fee model."""
        return "hyperliquid"

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "model_name": self.model_name,
            "fee_tier": self.fee_tier.value,
            "volume_14d": str(self.volume_14d) if self.volume_14d else None,
            "staked_hype": str(self.staked_hype),
            "is_hip3_market": self.is_hip3_market,
            "taker_fee": str(self._taker_fee),
            "maker_fee": str(self._maker_fee),
            "custom_taker_fee": str(self.custom_taker_fee) if self.custom_taker_fee else None,
            "custom_maker_fee": str(self.custom_maker_fee) if self.custom_maker_fee else None,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "HyperliquidFeeModel":
        """Create a HyperliquidFeeModel from a dictionary.

        Args:
            data: Dictionary with fee model configuration

        Returns:
            Instantiated HyperliquidFeeModel
        """
        fee_tier_str = data.get("fee_tier", "vip_0")
        fee_tier = HyperliquidFeeTier(fee_tier_str)

        return cls(
            fee_tier=fee_tier,
            volume_14d=Decimal(data["volume_14d"]) if data.get("volume_14d") else None,
            staked_hype=Decimal(data.get("staked_hype", "0")),
            is_hip3_market=data.get("is_hip3_market", False),
            custom_taker_fee=Decimal(data["custom_taker_fee"]) if data.get("custom_taker_fee") else None,
            custom_maker_fee=Decimal(data["custom_maker_fee"]) if data.get("custom_maker_fee") else None,
        )


__all__ = [
    "HyperliquidFeeModel",
    "HyperliquidFeeTier",
    "FEE_RATES_BY_TIER",
    "VOLUME_THRESHOLDS",
    "get_tier_from_volume",
    "get_staking_discount",
]
