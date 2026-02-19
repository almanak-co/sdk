"""Fee and slippage models for PnL backtesting.

This module defines protocols and default implementations for calculating
protocol fees and slippage during backtest simulations.

Key Components:
    - FeeModel: Protocol for calculating protocol/exchange fees
    - SlippageModel: Protocol for estimating trade slippage
    - DefaultFeeModel: Simple percentage-based fee model
    - DefaultSlippageModel: Simple percentage-based slippage model

Example:
    from almanak.framework.backtesting.pnl.fee_models import (
        DefaultFeeModel,
        DefaultSlippageModel,
    )

    fee_model = DefaultFeeModel(fee_pct=Decimal("0.003"))  # 0.3%
    slippage_model = DefaultSlippageModel(slippage_pct=Decimal("0.001"))  # 0.1%

    fee = fee_model.calculate_fee(intent_type, amount_usd, market_state)
    slippage = slippage_model.calculate_slippage(intent_type, amount_usd, market_state)
"""

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Protocol, runtime_checkable

from almanak.framework.backtesting.models import IntentType
from almanak.framework.backtesting.pnl.data_provider import MarketState


@runtime_checkable
class FeeModel(Protocol):
    """Protocol for calculating protocol/exchange fees.

    Fee models estimate the fees charged by protocols (DEXs, lending platforms,
    perps exchanges) for executing trades during backtests.

    Implementations should consider:
    - Protocol-specific fee structures (e.g., Uniswap pool fee tiers)
    - Different fees for different action types (swaps vs LPs vs borrows)
    - Market conditions that might affect fees

    Example implementation:
        class MyFeeModel:
            def calculate_fee(
                self,
                intent_type: IntentType,
                amount_usd: Decimal,
                market_state: MarketState,
                protocol: str = "",
                **kwargs: Any,
            ) -> Decimal:
                # Return fee in USD
                return amount_usd * Decimal("0.003")

            @property
            def model_name(self) -> str:
                return "my_fee_model"
    """

    def calculate_fee(
        self,
        intent_type: IntentType,
        amount_usd: Decimal,
        market_state: MarketState,
        protocol: str = "",
        **kwargs: Any,
    ) -> Decimal:
        """Calculate the fee for an action.

        Args:
            intent_type: Type of intent being executed (SWAP, LP_OPEN, BORROW, etc.)
            amount_usd: Notional amount of the trade in USD
            market_state: Current market state at execution time
            protocol: Protocol being used (e.g., "uniswap_v3", "aave_v3", "gmx")
            **kwargs: Additional parameters (e.g., fee_tier for Uniswap)

        Returns:
            Fee amount in USD
        """
        ...

    @property
    def model_name(self) -> str:
        """Return the unique name of this fee model."""
        ...


@runtime_checkable
class SlippageModel(Protocol):
    """Protocol for estimating trade slippage.

    Slippage models estimate the price impact of trades due to market
    conditions, liquidity, and order size during backtests.

    Implementations should consider:
    - Trade size relative to available liquidity
    - Market volatility at execution time
    - Different slippage characteristics for different action types
    - Price impact for large orders

    Example implementation:
        class MySlippageModel:
            def calculate_slippage(
                self,
                intent_type: IntentType,
                amount_usd: Decimal,
                market_state: MarketState,
                protocol: str = "",
                **kwargs: Any,
            ) -> Decimal:
                # Return slippage as a decimal (0.01 = 1%)
                return Decimal("0.001")

            @property
            def model_name(self) -> str:
                return "my_slippage_model"
    """

    def calculate_slippage(
        self,
        intent_type: IntentType,
        amount_usd: Decimal,
        market_state: MarketState,
        protocol: str = "",
        **kwargs: Any,
    ) -> Decimal:
        """Calculate the slippage for an action.

        Args:
            intent_type: Type of intent being executed (SWAP, LP_OPEN, etc.)
            amount_usd: Notional amount of the trade in USD
            market_state: Current market state at execution time
            protocol: Protocol being used (e.g., "uniswap_v3", "gmx")
            **kwargs: Additional parameters (e.g., token pair, liquidity depth)

        Returns:
            Slippage as a decimal percentage (0.01 = 1% slippage)
            This represents the price impact relative to the mid-price.
        """
        ...

    @property
    def model_name(self) -> str:
        """Return the unique name of this slippage model."""
        ...


@dataclass
class DefaultFeeModel:
    """Default fee model with configurable percentage-based fees.

    A simple fee model that applies a fixed percentage fee to all trades.
    Different fee percentages can be configured for different intent types.

    Attributes:
        fee_pct: Default fee percentage as a decimal (0.003 = 0.3%)
        intent_fees: Optional dict mapping IntentType to specific fee percentages
        zero_fee_intents: Set of intent types that have zero fees

    Example:
        # Simple uniform fee
        model = DefaultFeeModel(fee_pct=Decimal("0.003"))

        # Different fees per intent type
        model = DefaultFeeModel(
            fee_pct=Decimal("0.003"),
            intent_fees={
                IntentType.BORROW: Decimal("0.0001"),
                IntentType.LP_OPEN: Decimal("0"),
            },
        )
    """

    fee_pct: Decimal = Decimal("0.003")  # 0.3% default fee
    intent_fees: dict[IntentType, Decimal] | None = None
    zero_fee_intents: frozenset[IntentType] = frozenset(
        {
            IntentType.HOLD,
            IntentType.LP_CLOSE,  # No protocol fee on LP withdrawal
            IntentType.SUPPLY,
            IntentType.WITHDRAW,
            IntentType.REPAY,
        }
    )

    def calculate_fee(
        self,
        intent_type: IntentType,
        amount_usd: Decimal,
        market_state: MarketState,
        protocol: str = "",
        **kwargs: Any,
    ) -> Decimal:
        """Calculate fee using configured percentages.

        Args:
            intent_type: Type of intent being executed
            amount_usd: Notional amount of the trade in USD
            market_state: Current market state (not used in default model)
            protocol: Protocol being used (not used in default model)
            **kwargs: Additional parameters (not used in default model)

        Returns:
            Fee amount in USD
        """
        # Check for zero-fee intents
        if intent_type in self.zero_fee_intents:
            return Decimal("0")

        # Check for intent-specific fees
        if self.intent_fees and intent_type in self.intent_fees:
            fee_rate = self.intent_fees[intent_type]
        else:
            fee_rate = self.fee_pct

        return amount_usd * fee_rate

    @property
    def model_name(self) -> str:
        """Return the unique name of this fee model."""
        return "default"

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "model_name": self.model_name,
            "fee_pct": str(self.fee_pct),
            "intent_fees": {k.value: str(v) for k, v in (self.intent_fees or {}).items()},
            "zero_fee_intents": [i.value for i in self.zero_fee_intents],
        }


@dataclass
class DefaultSlippageModel:
    """Default slippage model with configurable percentage-based slippage.

    A simple slippage model that applies a base slippage percentage scaled
    by trade size. Larger trades have proportionally more slippage.

    The slippage formula is:
        slippage_pct = base_slippage_pct * (1 + amount_usd / size_scaling_factor)

    For small trades, slippage is approximately base_slippage_pct.
    For larger trades, slippage increases based on the size scaling factor.

    Attributes:
        slippage_pct: Base slippage percentage as a decimal (0.001 = 0.1%)
        size_scaling_factor: Factor for scaling slippage by trade size
            (default 100_000 means a $100k trade doubles the base slippage)
        max_slippage_pct: Maximum slippage cap (default 5%)
        zero_slippage_intents: Set of intent types with zero slippage

    Example:
        # Simple uniform slippage
        model = DefaultSlippageModel(slippage_pct=Decimal("0.001"))

        # With size scaling
        model = DefaultSlippageModel(
            slippage_pct=Decimal("0.0005"),
            size_scaling_factor=Decimal("50000"),
            max_slippage_pct=Decimal("0.03"),
        )
    """

    slippage_pct: Decimal = Decimal("0.001")  # 0.1% base slippage
    size_scaling_factor: Decimal = Decimal("100000")  # $100k for doubling
    max_slippage_pct: Decimal = Decimal("0.05")  # 5% max slippage
    zero_slippage_intents: frozenset[IntentType] = frozenset(
        {
            IntentType.HOLD,
            IntentType.SUPPLY,
            IntentType.WITHDRAW,
            IntentType.REPAY,
            IntentType.BORROW,
        }
    )

    def calculate_slippage(
        self,
        intent_type: IntentType,
        amount_usd: Decimal,
        market_state: MarketState,
        protocol: str = "",
        **kwargs: Any,
    ) -> Decimal:
        """Calculate slippage using configured percentages and size scaling.

        Args:
            intent_type: Type of intent being executed
            amount_usd: Notional amount of the trade in USD
            market_state: Current market state (not used in default model)
            protocol: Protocol being used (not used in default model)
            **kwargs: Additional parameters (not used in default model)

        Returns:
            Slippage as a decimal percentage (0.01 = 1%)
        """
        # Check for zero-slippage intents
        if intent_type in self.zero_slippage_intents:
            return Decimal("0")

        # Calculate size-scaled slippage
        if self.size_scaling_factor > 0:
            size_multiplier = Decimal("1") + (amount_usd / self.size_scaling_factor)
        else:
            size_multiplier = Decimal("1")

        slippage = self.slippage_pct * size_multiplier

        # Apply cap
        return min(slippage, self.max_slippage_pct)

    @property
    def model_name(self) -> str:
        """Return the unique name of this slippage model."""
        return "default"

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "model_name": self.model_name,
            "slippage_pct": str(self.slippage_pct),
            "size_scaling_factor": str(self.size_scaling_factor),
            "max_slippage_pct": str(self.max_slippage_pct),
            "zero_slippage_intents": [i.value for i in self.zero_slippage_intents],
        }


__all__ = [
    "FeeModel",
    "SlippageModel",
    "DefaultFeeModel",
    "DefaultSlippageModel",
]
