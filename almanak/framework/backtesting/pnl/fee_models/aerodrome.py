"""Aerodrome specific fee model for PnL backtesting.

This module provides a fee model implementation tailored to Aerodrome's
DEX characteristics on Base:

- Stable pools: Lower fees (typically 0.01% or 0.05%) for correlated assets
- Volatile pools: Higher fees (typically 0.3%) for uncorrelated assets
- Protocol fee: A portion of swap fees goes to the protocol (configurable)

Key Components:
    - AerodromePoolType: Enum for stable vs volatile pool types
    - AerodromeFeeModel: Fee model with pool-type-based fees

Example:
    from almanak.framework.backtesting.pnl.fee_models.aerodrome import (
        AerodromeFeeModel,
        AerodromePoolType,
    )

    fee_model = AerodromeFeeModel()

    # Stable pool swap
    fee = fee_model.calculate_fee(
        Decimal("1000"),
        intent_type=IntentType.SWAP,
        pool_type=AerodromePoolType.STABLE,
    )

    # Volatile pool swap (default)
    fee = fee_model.calculate_fee(
        Decimal("1000"),
        intent_type=IntentType.SWAP,
    )
"""

from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from typing import Any

from almanak.framework.backtesting.models import IntentType
from almanak.framework.backtesting.pnl.fee_models.base import FeeModel


class AerodromePoolType(StrEnum):
    """Aerodrome pool types.

    Aerodrome uses two main pool types with different fee structures:
    - STABLE: For correlated assets (e.g., USDC/USDT), uses curve-style x^3*y + y^3*x = k
    - VOLATILE: For uncorrelated assets (e.g., ETH/USDC), uses standard x*y = k

    The pool type determines both the swap curve and the default fee rate.
    """

    STABLE = "stable"
    VOLATILE = "volatile"


# Default fee rates for each pool type (in decimal form)
AERODROME_DEFAULT_FEES: dict[AerodromePoolType, Decimal] = {
    AerodromePoolType.STABLE: Decimal("0.0001"),  # 0.01% for stable pools
    AerodromePoolType.VOLATILE: Decimal("0.003"),  # 0.3% for volatile pools
}


@dataclass
class AerodromeFeeModel(FeeModel):
    """Fee model for Aerodrome DEX on Base.

    Aerodrome is a ve(3,3) DEX on Base that uses different fee structures
    for stable and volatile pools. Stable pools have lower fees optimized
    for correlated assets, while volatile pools use standard AMM fees.

    Fee Structure:
        - STABLE pools: 0.01% default (customizable)
        - VOLATILE pools: 0.3% default (customizable)
        - LP operations: No direct protocol fee

    The protocol may take a portion of swap fees as a protocol fee,
    which is configurable via the protocol_fee_share parameter.

    Attributes:
        stable_fee_pct: Fee for stable pool swaps (default 0.01%)
        volatile_fee_pct: Fee for volatile pool swaps (default 0.3%)
        protocol_fee_share: Share of fees going to protocol (default 0, range 0-1)
        token_pair_pool_types: Optional mapping of token pairs to pool types

    Example:
        model = AerodromeFeeModel(
            stable_fee_pct=Decimal("0.0001"),
            volatile_fee_pct=Decimal("0.003"),
        )

        # Stable pool swap (USDC/USDT)
        fee = model.calculate_fee(
            Decimal("1000"),
            intent_type=IntentType.SWAP,
            pool_type=AerodromePoolType.STABLE,
        )

        # Volatile pool swap (ETH/USDC)
        fee = model.calculate_fee(
            Decimal("1000"),
            intent_type=IntentType.SWAP,
            pool_type=AerodromePoolType.VOLATILE,
        )
    """

    stable_fee_pct: Decimal = Decimal("0.0001")  # 0.01% for stable pools
    volatile_fee_pct: Decimal = Decimal("0.003")  # 0.3% for volatile pools
    protocol_fee_share: Decimal = Decimal("0")  # Share of fees to protocol (0-1)
    token_pair_pool_types: dict[tuple[str, str], AerodromePoolType] | None = None

    # Well-known stablecoin pairs that should use stable pools
    _default_stable_pairs: frozenset[frozenset[str]] = frozenset(
        {
            frozenset({"USDC", "USDT"}),
            frozenset({"USDC", "DAI"}),
            frozenset({"USDT", "DAI"}),
            frozenset({"USDC", "USDBC"}),  # Base-specific bridged USDC
            frozenset({"USDC", "USDC.E"}),
            frozenset({"WETH", "STETH"}),
            frozenset({"WETH", "WSTETH"}),
            frozenset({"CBETH", "WETH"}),
        }
    )

    def calculate_fee(
        self,
        trade_amount: Decimal,
        **kwargs: Any,
    ) -> Decimal:
        """Calculate Aerodrome fee for an intent.

        Only SWAP intents incur fees. LP_OPEN and LP_CLOSE have no direct
        protocol fees.

        Args:
            trade_amount: Notional amount of the trade in USD
            **kwargs: Additional parameters:
                - intent_type: Type of intent being executed (default: SWAP)
                - market_state: Current market state at execution time
                - protocol: Protocol being used (ignored, always aerodrome)
                - pool_type: AerodromePoolType (STABLE or VOLATILE)
                - token_in: Input token symbol for auto pool type detection
                - token_out: Output token symbol for auto pool type detection
                - fee_pct: Explicit fee percentage override

        Returns:
            Fee amount in USD
        """
        # Get intent type from kwargs, default to SWAP
        intent_type = kwargs.get("intent_type", IntentType.SWAP)

        # LP operations have no direct protocol fee
        if intent_type in (IntentType.LP_OPEN, IntentType.LP_CLOSE):
            return Decimal("0")

        # Only SWAP has fees
        if intent_type != IntentType.SWAP:
            return Decimal("0")

        # Determine the fee rate to use
        fee_rate = self._resolve_fee_rate(**kwargs)

        # Calculate fee
        return trade_amount * fee_rate

    def _resolve_fee_rate(self, **kwargs: Any) -> Decimal:
        """Resolve the fee rate to use based on pool type and kwargs.

        Priority:
        1. Explicit fee_pct parameter
        2. Pool type from kwargs
        3. Token pair lookup in token_pair_pool_types
        4. Auto-detect stable pairs from known stablecoin pairs
        5. Default to volatile pool rate

        Args:
            **kwargs: May contain fee_pct, pool_type, token_in, token_out

        Returns:
            The resolved fee rate as a Decimal
        """
        # Check for explicit fee percentage
        if "fee_pct" in kwargs:
            return Decimal(str(kwargs["fee_pct"]))

        # Determine pool type
        pool_type = self._resolve_pool_type(**kwargs)

        # Return fee rate based on pool type
        if pool_type == AerodromePoolType.STABLE:
            return self.stable_fee_pct
        return self.volatile_fee_pct

    def _resolve_pool_type(self, **kwargs: Any) -> AerodromePoolType:
        """Resolve the pool type based on kwargs and token pair.

        Priority:
        1. Explicit pool_type parameter
        2. Token pair lookup in token_pair_pool_types
        3. Auto-detect from known stablecoin pairs
        4. Default to VOLATILE

        Args:
            **kwargs: May contain pool_type, token_in, token_out

        Returns:
            The resolved AerodromePoolType
        """
        # Check for explicit pool type
        if "pool_type" in kwargs:
            pool_type = kwargs["pool_type"]
            if isinstance(pool_type, AerodromePoolType):
                return pool_type
            # Handle string value
            if isinstance(pool_type, str):
                pool_type_lower = pool_type.lower()
                if pool_type_lower == "stable":
                    return AerodromePoolType.STABLE
                if pool_type_lower == "volatile":
                    return AerodromePoolType.VOLATILE

        # Check for token pair mapping
        token_in = kwargs.get("token_in")
        token_out = kwargs.get("token_out")

        if token_in and token_out:
            token_in_upper = str(token_in).upper()
            token_out_upper = str(token_out).upper()

            # Check explicit mapping
            if self.token_pair_pool_types:
                pair = (token_in_upper, token_out_upper)
                pair_reverse = (token_out_upper, token_in_upper)
                if pair in self.token_pair_pool_types:
                    return self.token_pair_pool_types[pair]
                if pair_reverse in self.token_pair_pool_types:
                    return self.token_pair_pool_types[pair_reverse]

            # Check known stable pairs
            token_pair_set = frozenset({token_in_upper, token_out_upper})
            if token_pair_set in self._default_stable_pairs:
                return AerodromePoolType.STABLE

        # Default to volatile
        return AerodromePoolType.VOLATILE

    @property
    def model_name(self) -> str:
        """Return the unique name of this fee model."""
        return "aerodrome"

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        token_pair_pool_types_dict: dict[str, str] = {}
        if self.token_pair_pool_types:
            for pair, pool_type in self.token_pair_pool_types.items():
                key = f"{pair[0]}/{pair[1]}"
                token_pair_pool_types_dict[key] = pool_type.value

        return {
            "model_name": self.model_name,
            "stable_fee_pct": str(self.stable_fee_pct),
            "volatile_fee_pct": str(self.volatile_fee_pct),
            "protocol_fee_share": str(self.protocol_fee_share),
            "token_pair_pool_types": token_pair_pool_types_dict,
        }


__all__ = [
    "AerodromePoolType",
    "AerodromeFeeModel",
    "AERODROME_DEFAULT_FEES",
]
