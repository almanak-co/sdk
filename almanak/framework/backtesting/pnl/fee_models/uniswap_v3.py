"""Uniswap V3 specific fee and slippage models for PnL backtesting.

This module provides fee and slippage model implementations tailored
to Uniswap V3's unique characteristics:

- Fee tiers: 0.01%, 0.05%, 0.3%, 1%
- No protocol fee on LP operations (only swap fees to LPs)
- Concentrated liquidity affects slippage based on tick range

Key Components:
    - UniswapV3FeeTier: Enum of supported Uniswap V3 fee tiers
    - UniswapV3FeeModel: Fee model using pool fee tiers
    - UniswapV3SlippageModel: Liquidity-aware slippage estimation

Example:
    from almanak.framework.backtesting.pnl.fee_models.uniswap_v3 import (
        UniswapV3FeeModel,
        UniswapV3SlippageModel,
        UniswapV3FeeTier,
    )

    fee_model = UniswapV3FeeModel(default_fee_tier=UniswapV3FeeTier.MEDIUM)
    slippage_model = UniswapV3SlippageModel()

    fee = fee_model.calculate_fee(
        intent_type=IntentType.SWAP,
        amount_usd=Decimal("1000"),
        market_state=market_state,
        fee_tier=UniswapV3FeeTier.LOW,  # Override default
    )
"""

from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from typing import Any

from almanak.framework.backtesting.models import IntentType
from almanak.framework.backtesting.pnl.data_provider import MarketState
from almanak.framework.backtesting.pnl.fee_models.base import FeeModel


class UniswapV3FeeTier(StrEnum):
    """Uniswap V3 fee tiers.

    Each tier corresponds to a different pool fee percentage:
    - LOWEST (0.01%): Designed for stablecoin pairs
    - LOW (0.05%): Best for stable pairs with low volatility
    - MEDIUM (0.3%): Most common, suitable for most pairs
    - HIGH (1%): For exotic pairs with high volatility

    The fee is charged on every swap and distributed to liquidity providers.
    """

    LOWEST = "100"  # 0.01% = 100 bps (1 bp = 0.0001)
    LOW = "500"  # 0.05% = 500 bps
    MEDIUM = "3000"  # 0.3% = 3000 bps
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
FEE_TIER_MAP: dict[int, UniswapV3FeeTier] = {
    100: UniswapV3FeeTier.LOWEST,
    500: UniswapV3FeeTier.LOW,
    3000: UniswapV3FeeTier.MEDIUM,
    10000: UniswapV3FeeTier.HIGH,
}


@dataclass
class UniswapV3FeeModel(FeeModel):
    """Fee model for Uniswap V3 using pool fee tiers.

    Uniswap V3 charges fees only on swaps. The fee is determined by the
    pool's fee tier and goes entirely to liquidity providers. There is
    no protocol fee on LP operations (entering/exiting positions).

    Fee tiers:
        - 0.01% (100 bps): Stablecoin pairs (USDC/USDT)
        - 0.05% (500 bps): Stable pairs (USDC/DAI)
        - 0.3% (3000 bps): Most pairs (ETH/USDC)
        - 1% (10000 bps): Exotic pairs (low liquidity)

    Attributes:
        default_fee_tier: Default fee tier when not specified (default: MEDIUM)
        token_pair_tiers: Optional mapping of token pairs to their fee tiers

    Example:
        model = UniswapV3FeeModel(default_fee_tier=UniswapV3FeeTier.MEDIUM)

        # Using default tier
        fee = model.calculate_fee(IntentType.SWAP, Decimal("1000"), market_state)

        # Specifying fee tier
        fee = model.calculate_fee(
            IntentType.SWAP,
            Decimal("1000"),
            market_state,
            fee_tier=UniswapV3FeeTier.LOW,
        )
    """

    default_fee_tier: UniswapV3FeeTier = UniswapV3FeeTier.MEDIUM
    token_pair_tiers: dict[tuple[str, str], UniswapV3FeeTier] | None = None

    def calculate_fee(
        self,
        trade_amount: Decimal,
        **kwargs: Any,
    ) -> Decimal:
        """Calculate Uniswap V3 fee for an intent.

        Only SWAP intents incur fees. LP_OPEN and LP_CLOSE have no protocol
        fees as Uniswap V3 doesn't charge for providing/removing liquidity.

        Args:
            trade_amount: Notional amount of the trade in USD
            **kwargs: Additional parameters:
                - intent_type: Type of intent being executed (default: SWAP)
                - market_state: Current market state at execution time
                - protocol: Protocol being used (ignored, always uniswap_v3)
                - fee_tier: UniswapV3FeeTier to use (overrides default)
                - fee_tier_bps: Fee tier as integer bps (100, 500, 3000, 10000)
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

        # Only SWAP has fees in Uniswap V3
        if intent_type != IntentType.SWAP:
            return Decimal("0")

        # Determine the fee tier to use
        fee_tier = self._resolve_fee_tier(**kwargs)

        # Calculate fee
        return trade_amount * fee_tier.fee_pct

    def _resolve_fee_tier(self, **kwargs: Any) -> UniswapV3FeeTier:
        """Resolve the fee tier to use based on kwargs and defaults.

        Priority:
        1. Explicit fee_tier parameter
        2. fee_tier_bps parameter converted to tier
        3. Token pair lookup in token_pair_tiers
        4. Default fee tier

        Args:
            **kwargs: May contain fee_tier, fee_tier_bps, token_in, token_out

        Returns:
            The resolved UniswapV3FeeTier
        """
        # Check for explicit fee tier
        if "fee_tier" in kwargs:
            tier = kwargs["fee_tier"]
            if isinstance(tier, UniswapV3FeeTier):
                return tier
            # Handle string enum value
            if isinstance(tier, str):
                for t in UniswapV3FeeTier:
                    if t.value == tier or t.name.lower() == tier.lower():
                        return t

        # Check for fee_tier_bps
        if "fee_tier_bps" in kwargs:
            bps = int(kwargs["fee_tier_bps"])
            if bps in FEE_TIER_MAP:
                return FEE_TIER_MAP[bps]

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
        return "uniswap_v3"

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


@dataclass
class UniswapV3SlippageModel:
    """Liquidity-aware slippage model for Uniswap V3.

    This model estimates slippage based on:
    - Trade size relative to available liquidity
    - Concentrated liquidity characteristics of V3
    - Volatility of the trading pair

    For Uniswap V3's concentrated liquidity, slippage can vary significantly
    based on how close the trade is to the edges of concentrated positions.
    This model uses a simplified approach that scales slippage based on
    trade size and a base slippage estimate.

    The slippage formula is:
        slippage = base_slippage * (1 + sqrt(amount_usd / liquidity_depth))

    Where liquidity_depth is an estimate of available liquidity in the pool.

    Liquidity can be provided in several ways (priority order):
    1. Pass `liquidity` kwarg to calculate_slippage() - actual pool liquidity
    2. Pass `liquidity_depth` kwarg - estimated USD liquidity depth
    3. Use set_liquidity() to set from on-chain query result
    4. Fall back to default liquidity_depth_usd attribute

    Attributes:
        base_slippage_pct: Base slippage for small trades (default 0.05%)
        liquidity_depth_usd: Estimated liquidity depth in USD (default $1M)
        max_slippage_pct: Maximum slippage cap (default 5%)
        volatility_multiplier: Multiplier for volatile pairs (default 1.0)
        _actual_liquidity_usd: Actual pool liquidity from on-chain query (internal)
        _liquidity_source: Source of the liquidity data (internal)

    Example:
        model = UniswapV3SlippageModel(
            base_slippage_pct=Decimal("0.0005"),
            liquidity_depth_usd=Decimal("500000"),
        )

        # Option 1: Pass liquidity directly (from on-chain query)
        slippage = model.calculate_slippage(
            IntentType.SWAP,
            Decimal("10000"),
            market_state,
            liquidity=Decimal("5000000"),  # Actual pool liquidity in USD
        )

        # Option 2: Use set_liquidity() for persistent liquidity
        from almanak.framework.backtesting.pnl.fee_models.liquidity import (
            query_pool_liquidity,
        )
        result = await query_pool_liquidity(pool_address, web3)
        model.set_liquidity(result.liquidity_usd, source="on-chain")

        # Now all calculations use the queried liquidity
        slippage = model.calculate_slippage(
            IntentType.SWAP, Decimal("10000"), market_state
        )
    """

    base_slippage_pct: Decimal = Decimal("0.0005")  # 0.05% base slippage
    liquidity_depth_usd: Decimal = Decimal("1000000")  # $1M default liquidity
    max_slippage_pct: Decimal = Decimal("0.05")  # 5% max slippage
    volatility_multiplier: Decimal = Decimal("1.0")

    # Internal fields for actual liquidity (set via set_liquidity())
    _actual_liquidity_usd: Decimal | None = None
    _liquidity_source: str = "estimated"

    def set_liquidity(
        self,
        liquidity_usd: Decimal,
        source: str = "on-chain",
    ) -> None:
        """Set actual pool liquidity from an external query.

        This allows integrating with query_pool_liquidity() to use
        real on-chain liquidity values for more accurate slippage estimates.

        Args:
            liquidity_usd: Pool liquidity in USD
            source: Source of the liquidity data (e.g., "on-chain", "estimated")

        Example:
            from almanak.framework.backtesting.pnl.fee_models.liquidity import (
                query_pool_liquidity,
            )

            result = await query_pool_liquidity(pool_address, web3)
            model.set_liquidity(result.liquidity_usd, source=result.source)
        """
        self._actual_liquidity_usd = liquidity_usd
        self._liquidity_source = source

    def clear_liquidity(self) -> None:
        """Clear the actual liquidity value, reverting to estimated liquidity."""
        self._actual_liquidity_usd = None
        self._liquidity_source = "estimated"

    def get_effective_liquidity(self) -> tuple[Decimal, str]:
        """Get the effective liquidity being used for calculations.

        Returns:
            Tuple of (liquidity_usd, source)
        """
        if self._actual_liquidity_usd is not None:
            return self._actual_liquidity_usd, self._liquidity_source
        return self.liquidity_depth_usd, "estimated"

    def calculate_slippage(
        self,
        intent_type: IntentType,
        amount_usd: Decimal,
        market_state: MarketState,
        protocol: str = "",
        **kwargs: Any,
    ) -> Decimal:
        """Calculate liquidity-aware slippage for Uniswap V3.

        Slippage is only incurred on SWAP and LP_OPEN intents. LP_CLOSE
        also has slippage as it involves withdrawing from a position.

        Liquidity resolution order:
        1. `liquidity` kwarg - actual pool liquidity in USD
        2. `liquidity_depth` kwarg - estimated liquidity depth in USD
        3. `_actual_liquidity_usd` - set via set_liquidity() method
        4. `liquidity_depth_usd` - default instance attribute

        Args:
            intent_type: Type of intent being executed
            amount_usd: Notional amount of the trade in USD
            market_state: Current market state at execution time
            protocol: Protocol being used (ignored, always uniswap_v3)
            **kwargs: Additional parameters:
                - liquidity: Actual pool liquidity in USD (highest priority)
                - liquidity_depth: Override liquidity depth in USD
                - volatility: Volatility multiplier (1.0 = normal)
                - in_range: Boolean, True if trading within concentrated range
                - tick_distance: Distance from current tick to position edge

        Returns:
            Slippage as a decimal percentage (0.01 = 1%)
        """
        # Zero slippage for non-trading intents
        if intent_type in (
            IntentType.HOLD,
            IntentType.SUPPLY,
            IntentType.WITHDRAW,
            IntentType.REPAY,
            IntentType.BORROW,
            IntentType.PERP_OPEN,
            IntentType.PERP_CLOSE,
            IntentType.BRIDGE,
        ):
            return Decimal("0")

        # SWAP, LP_OPEN, LP_CLOSE all have slippage
        if intent_type not in (IntentType.SWAP, IntentType.LP_OPEN, IntentType.LP_CLOSE):
            return Decimal("0")

        # Resolve liquidity (priority: kwarg liquidity > kwarg liquidity_depth > set_liquidity > default)
        liquidity_depth = self._resolve_liquidity(**kwargs)

        # Get volatility multiplier
        volatility = Decimal(str(kwargs.get("volatility", self.volatility_multiplier)))

        # Calculate base slippage
        if liquidity_depth > 0 and amount_usd > 0:
            # sqrt(amount / liquidity) gives a reasonable slippage scaling
            # For $10k trade on $1M liquidity: sqrt(10000/1000000) = 0.1
            ratio = amount_usd / liquidity_depth
            # Use decimal-compatible square root approximation
            size_factor = self._decimal_sqrt(ratio)
        else:
            size_factor = Decimal("0")

        # Calculate slippage
        slippage = self.base_slippage_pct * (Decimal("1") + size_factor) * volatility

        # Adjust for concentrated liquidity position
        if kwargs.get("in_range") is False:
            # Out of range positions have higher slippage
            slippage = slippage * Decimal("2")

        # Apply cap
        return min(slippage, self.max_slippage_pct)

    def _resolve_liquidity(self, **kwargs: Any) -> Decimal:
        """Resolve the liquidity value to use for slippage calculation.

        Priority:
        1. `liquidity` kwarg - actual pool liquidity in USD
        2. `liquidity_depth` kwarg - estimated liquidity depth
        3. `_actual_liquidity_usd` - set via set_liquidity() method
        4. `liquidity_depth_usd` - default instance attribute

        Args:
            **kwargs: May contain liquidity, liquidity_depth

        Returns:
            Liquidity value in USD to use for calculation
        """
        # Check for explicit liquidity kwarg (highest priority)
        if "liquidity" in kwargs:
            return Decimal(str(kwargs["liquidity"]))

        # Check for liquidity_depth kwarg
        if "liquidity_depth" in kwargs:
            return Decimal(str(kwargs["liquidity_depth"]))

        # Check for actual liquidity set via set_liquidity()
        if self._actual_liquidity_usd is not None:
            return self._actual_liquidity_usd

        # Fall back to default
        return self.liquidity_depth_usd

    @staticmethod
    def _decimal_sqrt(n: Decimal) -> Decimal:
        """Calculate square root of a Decimal using Newton's method.

        Args:
            n: Non-negative Decimal to find square root of

        Returns:
            Square root as Decimal with reasonable precision
        """
        if n < 0:
            raise ValueError("Cannot calculate square root of negative number")
        if n == 0:
            return Decimal("0")

        # Newton's method for square root
        # x_{n+1} = (x_n + n/x_n) / 2
        x = n
        two = Decimal("2")

        # Iterate until convergence (typically 10-15 iterations for good precision)
        for _ in range(50):
            x_next = (x + n / x) / two
            if abs(x_next - x) < Decimal("1e-15"):
                break
            x = x_next

        return x

    @property
    def model_name(self) -> str:
        """Return the unique name of this slippage model."""
        return "uniswap_v3"

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        effective_liquidity, source = self.get_effective_liquidity()
        return {
            "model_name": self.model_name,
            "base_slippage_pct": str(self.base_slippage_pct),
            "liquidity_depth_usd": str(self.liquidity_depth_usd),
            "max_slippage_pct": str(self.max_slippage_pct),
            "volatility_multiplier": str(self.volatility_multiplier),
            "actual_liquidity_usd": str(self._actual_liquidity_usd) if self._actual_liquidity_usd else None,
            "liquidity_source": self._liquidity_source,
            "effective_liquidity_usd": str(effective_liquidity),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "UniswapV3SlippageModel":
        """Deserialize from dictionary."""
        model = cls(
            base_slippage_pct=Decimal(data.get("base_slippage_pct", "0.0005")),
            liquidity_depth_usd=Decimal(data.get("liquidity_depth_usd", "1000000")),
            max_slippage_pct=Decimal(data.get("max_slippage_pct", "0.05")),
            volatility_multiplier=Decimal(data.get("volatility_multiplier", "1.0")),
        )

        # Restore actual liquidity if present
        if data.get("actual_liquidity_usd"):
            model._actual_liquidity_usd = Decimal(data["actual_liquidity_usd"])
            model._liquidity_source = data.get("liquidity_source", "restored")

        return model


__all__ = [
    "UniswapV3FeeTier",
    "UniswapV3FeeModel",
    "UniswapV3SlippageModel",
    "FEE_TIER_MAP",
]
