"""PancakeSwap V3 specific fee model for PnL backtesting.

This module provides a fee model implementation tailored to PancakeSwap V3's
DEX characteristics:

- Fee tiers: 0.01%, 0.05%, 0.25%, 1% (similar to Uniswap V3 but with different tier)
- No protocol fee on LP operations (only swap fees to LPs)
- Concentrated liquidity affects slippage based on tick range

Key Components:
    - PancakeSwapV3FeeTier: Enum of supported fee tiers
    - PancakeSwapV3FeeModel: Fee model using pool fee tiers

Example:
    from almanak.framework.backtesting.pnl.fee_models.pancakeswap_v3 import (
        PancakeSwapV3FeeModel,
        PancakeSwapV3FeeTier,
    )

    fee_model = PancakeSwapV3FeeModel(default_fee_tier=PancakeSwapV3FeeTier.MEDIUM)

    fee = fee_model.calculate_fee(
        Decimal("1000"),
        intent_type=IntentType.SWAP,
        fee_tier=PancakeSwapV3FeeTier.LOW,  # Override default
    )
"""

from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from typing import Any

from almanak.framework.backtesting.models import IntentType
from almanak.framework.backtesting.pnl.fee_models.base import FeeModel


class PancakeSwapV3FeeTier(StrEnum):
    """PancakeSwap V3 fee tiers.

    Each tier corresponds to a different pool fee percentage:
    - LOWEST (0.01%): Designed for stablecoin pairs
    - LOW (0.05%): Best for stable pairs with low volatility
    - MEDIUM (0.25%): Common for most pairs (differs from Uniswap's 0.3%)
    - HIGH (1%): For exotic pairs with high volatility

    The fee is charged on every swap and distributed to liquidity providers.
    """

    LOWEST = "100"  # 0.01% = 100 bps (1 bp = 0.0001)
    LOW = "500"  # 0.05% = 500 bps
    MEDIUM = "2500"  # 0.25% = 2500 bps (PancakeSwap's default, differs from Uniswap's 3000)
    HIGH = "10000"  # 1% = 10000 bps

    @property
    def fee_pct(self) -> Decimal:
        """Get the fee percentage as a decimal."""
        return Decimal(self.value) / Decimal("1000000")

    @property
    def fee_bps(self) -> int:
        """Get the fee in basis points."""
        return int(self.value)

    def __str__(self) -> str:
        """Return human-readable fee tier description."""
        pct = float(self.fee_pct) * 100
        return f"{pct:.2f}%"


# Mapping of fee tier values to enum members for lookup
PANCAKESWAP_FEE_TIER_MAP: dict[int, PancakeSwapV3FeeTier] = {
    100: PancakeSwapV3FeeTier.LOWEST,
    500: PancakeSwapV3FeeTier.LOW,
    2500: PancakeSwapV3FeeTier.MEDIUM,
    10000: PancakeSwapV3FeeTier.HIGH,
}


@dataclass
class PancakeSwapV3FeeModel(FeeModel):
    """Fee model for PancakeSwap V3 using pool fee tiers.

    PancakeSwap V3 charges fees only on swaps. The fee is determined by the
    pool's fee tier and goes entirely to liquidity providers. There is
    no protocol fee on LP operations (entering/exiting positions).

    Fee tiers:
        - 0.01% (100 bps): Stablecoin pairs (USDC/USDT)
        - 0.05% (500 bps): Stable pairs (USDC/DAI)
        - 0.25% (2500 bps): Most pairs (BNB/USDC) - default tier
        - 1% (10000 bps): Exotic pairs (low liquidity)

    Note: PancakeSwap V3 uses 0.25% as its most common tier (vs Uniswap's 0.3%)

    Attributes:
        default_fee_tier: Default fee tier when not specified (default: MEDIUM)
        token_pair_tiers: Optional mapping of token pairs to their fee tiers

    Example:
        model = PancakeSwapV3FeeModel(default_fee_tier=PancakeSwapV3FeeTier.MEDIUM)

        # Using default tier
        fee = model.calculate_fee(Decimal("1000"), intent_type=IntentType.SWAP)

        # Specifying fee tier
        fee = model.calculate_fee(
            Decimal("1000"),
            intent_type=IntentType.SWAP,
            fee_tier=PancakeSwapV3FeeTier.LOW,
        )
    """

    default_fee_tier: PancakeSwapV3FeeTier = PancakeSwapV3FeeTier.MEDIUM
    token_pair_tiers: dict[tuple[str, str], PancakeSwapV3FeeTier] | None = None

    def calculate_fee(
        self,
        trade_amount: Decimal,
        **kwargs: Any,
    ) -> Decimal:
        """Calculate PancakeSwap V3 fee for an intent.

        Only SWAP intents incur fees. LP_OPEN and LP_CLOSE have no protocol
        fees as PancakeSwap V3 doesn't charge for providing/removing liquidity.

        Args:
            trade_amount: Notional amount of the trade in USD
            **kwargs: Additional parameters:
                - intent_type: Type of intent being executed (default: SWAP)
                - market_state: Current market state at execution time
                - protocol: Protocol being used (ignored, always pancakeswap_v3)
                - fee_tier: PancakeSwapV3FeeTier to use (overrides default)
                - fee_tier_bps: Fee tier as integer bps (100, 500, 2500, 10000)
                - token_in: Input token symbol for pair-specific tier lookup
                - token_out: Output token symbol for pair-specific tier lookup

        Returns:
            Fee amount in USD
        """
        # Get intent type from kwargs, default to SWAP
        intent_type = kwargs.get("intent_type", IntentType.SWAP)

        # LP operations have no protocol fee
        if intent_type in (IntentType.LP_OPEN, IntentType.LP_CLOSE):
            return Decimal("0")

        # Only SWAP has fees in PancakeSwap V3
        if intent_type != IntentType.SWAP:
            return Decimal("0")

        # Determine the fee tier to use
        fee_tier = self._resolve_fee_tier(**kwargs)

        # Calculate fee
        return trade_amount * fee_tier.fee_pct

    def _resolve_fee_tier(self, **kwargs: Any) -> PancakeSwapV3FeeTier:
        """Resolve the fee tier to use based on kwargs and defaults.

        Priority:
        1. Explicit fee_tier parameter
        2. fee_tier_bps parameter converted to tier
        3. Token pair lookup in token_pair_tiers
        4. Default fee tier

        Args:
            **kwargs: May contain fee_tier, fee_tier_bps, token_in, token_out

        Returns:
            The resolved PancakeSwapV3FeeTier
        """
        # Check for explicit fee tier
        if "fee_tier" in kwargs:
            tier = kwargs["fee_tier"]
            if isinstance(tier, PancakeSwapV3FeeTier):
                return tier
            # Handle string enum value
            if isinstance(tier, str):
                for t in PancakeSwapV3FeeTier:
                    if t.value == tier or t.name.lower() == tier.lower():
                        return t

        # Check for fee_tier_bps
        if "fee_tier_bps" in kwargs:
            bps = int(kwargs["fee_tier_bps"])
            if bps in PANCAKESWAP_FEE_TIER_MAP:
                return PANCAKESWAP_FEE_TIER_MAP[bps]

        # Check for token pair mapping
        token_in = kwargs.get("token_in")
        token_out = kwargs.get("token_out")
        if self.token_pair_tiers and token_in and token_out:
            # Try both orderings
            pair = (str(token_in).upper(), str(token_out).upper())
            pair_reverse = (str(token_out).upper(), str(token_in).upper())
            if pair in self.token_pair_tiers:
                return self.token_pair_tiers[pair]
            if pair_reverse in self.token_pair_tiers:
                return self.token_pair_tiers[pair_reverse]

        return self.default_fee_tier

    @property
    def model_name(self) -> str:
        """Return the unique name of this fee model."""
        return "pancakeswap_v3"

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        token_pair_tiers_dict: dict[str, str] = {}
        if self.token_pair_tiers:
            for pair, tier in self.token_pair_tiers.items():
                key = f"{pair[0]}/{pair[1]}"
                token_pair_tiers_dict[key] = tier.value

        return {
            "model_name": self.model_name,
            "default_fee_tier": self.default_fee_tier.value,
            "default_fee_pct": str(self.default_fee_tier.fee_pct),
            "token_pair_tiers": token_pair_tiers_dict,
        }


__all__ = [
    "PancakeSwapV3FeeTier",
    "PancakeSwapV3FeeModel",
    "PANCAKESWAP_FEE_TIER_MAP",
]
