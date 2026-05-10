"""Prediction market intent classes.

Intent classes for prediction market operations: buy, sell, and redeem outcome shares.
These intents support protocols like Polymarket.
"""

from datetime import datetime
from decimal import Decimal
from typing import Any, Literal

from pydantic import Field, model_validator

from almanak.framework.models.base import (
    AlmanakImmutableModel,  # noqa: F401  -- re-exported for backward compatibility
    OptionalSafeDecimal,
    default_intent_id,
    default_timestamp,
)
from almanak.framework.models.base import (
    ChainedAmount as PydanticChainedAmount,
)
from almanak.framework.services.prediction_monitor import PredictionExitConditions

from .base import BaseIntent
from .vocabulary import IntentType

# Type aliases for prediction markets
PredictionOutcome = Literal["YES", "NO"]
PredictionOrderType = Literal["market", "limit"]
PredictionTimeInForce = Literal["GTC", "IOC", "FOK"]
# PredictionShareAmount uses PydanticChainedAmount for proper string->Decimal coercion
PredictionShareAmount = PydanticChainedAmount


class PredictionBuyIntent(BaseIntent):
    """Intent to buy shares in a prediction market.

    This intent is used to buy outcome tokens (YES or NO) on Polymarket or
    similar prediction market platforms.

    Attributes:
        market_id: Polymarket market ID or slug (e.g., "will-bitcoin-exceed-100000")
        outcome: Which outcome to buy ("YES" or "NO")
        amount_usd: USDC amount to spend (mutually exclusive with shares)
        shares: Number of shares to buy (mutually exclusive with amount_usd)
        max_price: Maximum price per share (0.01-0.99) for limit orders
        order_type: Order type ("market" or "limit")
        time_in_force: How long order remains active ("GTC", "IOC", "FOK")
        expiration_hours: Hours until order expires (None = no expiry)
        protocol: Protocol to use (defaults to "polymarket")
        chain: Target chain (defaults to "polygon" for Polymarket)
        exit_conditions: Optional exit conditions for automatic position monitoring
            (stop-loss, take-profit, trailing stop, pre-resolution exit)
        intent_id: Unique identifier for this intent
        created_at: Timestamp when the intent was created

    Note:
        - Prices represent implied probability (0.65 = 65% chance of YES)
        - Market orders use aggressive pricing for immediate execution
        - Limit orders rest in the orderbook until matched or cancelled
        - GTC (Good Till Cancelled) orders remain until filled or cancelled
        - IOC (Immediate or Cancel) fills what it can immediately, cancels rest
        - FOK (Fill or Kill) must fill entirely or is cancelled

    Example:
        # Buy $100 worth of YES shares at market price
        intent = Intent.prediction_buy(
            market_id="will-bitcoin-exceed-100000",
            outcome="YES",
            amount_usd=Decimal("100"),
        )

        # Buy 50 YES shares with limit order at max price of $0.65
        intent = Intent.prediction_buy(
            market_id="will-bitcoin-exceed-100000",
            outcome="YES",
            shares=Decimal("50"),
            max_price=Decimal("0.65"),
            order_type="limit",
        )
    """

    market_id: str
    outcome: PredictionOutcome
    amount_usd: OptionalSafeDecimal = None
    shares: OptionalSafeDecimal = None
    max_price: OptionalSafeDecimal = None
    order_type: PredictionOrderType = "market"
    time_in_force: PredictionTimeInForce = "GTC"
    expiration_hours: int | None = None
    protocol: str = "polymarket"
    chain: str | None = None
    exit_conditions: PredictionExitConditions | None = None
    intent_id: str = Field(default_factory=default_intent_id)
    created_at: datetime = Field(default_factory=default_timestamp)

    @model_validator(mode="after")
    def validate_prediction_buy_intent(self) -> "PredictionBuyIntent":
        """Validate prediction buy parameters."""
        # Validate amount specification
        if self.amount_usd is None and self.shares is None:
            raise ValueError("Must specify either amount_usd or shares")
        if self.amount_usd is not None and self.shares is not None:
            raise ValueError("Cannot specify both amount_usd and shares")
        if self.amount_usd is not None and self.amount_usd <= 0:
            raise ValueError("amount_usd must be positive")
        if self.shares is not None and self.shares <= 0:
            raise ValueError("shares must be positive")

        # Validate max_price (0.01-0.99 for prediction markets)
        if self.max_price is not None:
            if self.max_price < Decimal("0.01") or self.max_price > Decimal("0.99"):
                raise ValueError("max_price must be between 0.01 and 0.99")

        # Limit orders require max_price
        if self.order_type == "limit" and self.max_price is None:
            raise ValueError("Limit orders require max_price to be specified")

        # Validate expiration_hours
        if self.expiration_hours is not None and self.expiration_hours <= 0:
            raise ValueError("expiration_hours must be positive")

        return self

    @property
    def intent_type(self) -> IntentType:
        """Return the type of this intent."""
        return IntentType.PREDICTION_BUY

    def serialize(self) -> dict[str, Any]:
        """Serialize the intent to a dictionary."""
        data = self.model_dump(mode="json")
        data["type"] = self.intent_type.value
        # Serialize exit_conditions using its to_dict() method
        if self.exit_conditions is not None:
            data["exit_conditions"] = self.exit_conditions.to_dict()
        return data

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> "PredictionBuyIntent":
        """Deserialize a dictionary to a PredictionBuyIntent."""
        from almanak.framework.services.prediction_monitor import PredictionExitConditions

        clean_data = {k: v for k, v in data.items() if k != "type"}
        if "created_at" in clean_data and isinstance(clean_data["created_at"], str):
            clean_data["created_at"] = datetime.fromisoformat(clean_data["created_at"])
        # Deserialize exit_conditions from dict
        if "exit_conditions" in clean_data and clean_data["exit_conditions"] is not None:
            ec_data = clean_data["exit_conditions"]
            clean_data["exit_conditions"] = PredictionExitConditions(
                stop_loss_price=Decimal(ec_data["stop_loss_price"]) if ec_data.get("stop_loss_price") else None,
                take_profit_price=Decimal(ec_data["take_profit_price"]) if ec_data.get("take_profit_price") else None,
                exit_before_resolution_hours=ec_data.get("exit_before_resolution_hours"),
                exit_before_resolution_seconds=ec_data.get("exit_before_resolution_seconds"),
                trailing_stop_pct=Decimal(ec_data["trailing_stop_pct"]) if ec_data.get("trailing_stop_pct") else None,
                max_spread_pct=Decimal(ec_data["max_spread_pct"]) if ec_data.get("max_spread_pct") else None,
                min_liquidity_usd=Decimal(ec_data["min_liquidity_usd"]) if ec_data.get("min_liquidity_usd") else None,
            )
        return cls.model_validate(clean_data)


class PredictionSellIntent(BaseIntent):
    """Intent to sell shares in a prediction market.

    This intent is used to sell outcome tokens (YES or NO) on Polymarket or
    similar prediction market platforms.

    Attributes:
        market_id: Polymarket market ID or slug
        outcome: Which outcome to sell ("YES" or "NO")
        shares: Number of shares to sell, or "all" to sell entire position
        min_price: Minimum price per share (0.01-0.99) for limit orders
        order_type: Order type ("market" or "limit")
        time_in_force: How long order remains active ("GTC", "IOC", "FOK")
        protocol: Protocol to use (defaults to "polymarket")
        chain: Target chain (defaults to "polygon" for Polymarket)
        intent_id: Unique identifier for this intent
        created_at: Timestamp when the intent was created

    Note:
        - Use shares="all" to sell your entire position
        - Market orders execute immediately at best available price
        - Limit orders only execute at min_price or better

    Example:
        # Sell all YES shares at market price
        intent = Intent.prediction_sell(
            market_id="will-bitcoin-exceed-100000",
            outcome="YES",
            shares="all",
        )

        # Sell 25 NO shares with limit order at min $0.40
        intent = Intent.prediction_sell(
            market_id="will-bitcoin-exceed-100000",
            outcome="NO",
            shares=Decimal("25"),
            min_price=Decimal("0.40"),
            order_type="limit",
        )
    """

    market_id: str
    outcome: PredictionOutcome
    shares: PredictionShareAmount
    min_price: OptionalSafeDecimal = None
    order_type: PredictionOrderType = "market"
    time_in_force: PredictionTimeInForce = "GTC"
    protocol: str = "polymarket"
    chain: str | None = None
    intent_id: str = Field(default_factory=default_intent_id)
    created_at: datetime = Field(default_factory=default_timestamp)

    @model_validator(mode="after")
    def validate_prediction_sell_intent(self) -> "PredictionSellIntent":
        """Validate prediction sell parameters."""
        # Validate shares
        if isinstance(self.shares, Decimal) and self.shares <= 0:
            raise ValueError("shares must be positive")
        elif not isinstance(self.shares, Decimal) and self.shares != "all":
            raise ValueError("shares must be a positive Decimal or 'all'")

        # Validate min_price (0.01-0.99 for prediction markets)
        if self.min_price is not None:
            if self.min_price < Decimal("0.01") or self.min_price > Decimal("0.99"):
                raise ValueError("min_price must be between 0.01 and 0.99")

        # Limit orders require min_price
        if self.order_type == "limit" and self.min_price is None:
            raise ValueError("Limit orders require min_price to be specified")

        return self

    @property
    def is_chained_amount(self) -> bool:
        """Check if this intent uses a chained amount from previous step."""
        return self.shares == "all"

    @property
    def intent_type(self) -> IntentType:
        """Return the type of this intent."""
        return IntentType.PREDICTION_SELL

    def serialize(self) -> dict[str, Any]:
        """Serialize the intent to a dictionary."""
        data = self.model_dump(mode="json")
        data["type"] = self.intent_type.value
        # Preserve "all" literal
        if self.shares == "all":
            data["shares"] = "all"
        return data

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> "PredictionSellIntent":
        """Deserialize a dictionary to a PredictionSellIntent."""
        clean_data = {k: v for k, v in data.items() if k != "type"}
        if "created_at" in clean_data and isinstance(clean_data["created_at"], str):
            clean_data["created_at"] = datetime.fromisoformat(clean_data["created_at"])
        return cls.model_validate(clean_data)


class PredictionRedeemIntent(BaseIntent):
    """Intent to redeem winning prediction market positions.

    This intent is used to redeem outcome tokens after a market has resolved.
    Winning tokens can be redeemed for $1 each (in USDC).

    Attributes:
        market_id: Polymarket market ID or slug
        outcome: Which outcome to redeem ("YES", "NO", or None for both)
        shares: Number of shares to redeem, or "all" (default)
        protocol: Protocol to use (defaults to "polymarket")
        chain: Target chain (defaults to "polygon" for Polymarket)
        intent_id: Unique identifier for this intent
        created_at: Timestamp when the intent was created

    Note:
        - Redemption is only possible after the market has resolved
        - Winning positions redeem for $1 per share
        - Losing positions are worthless
        - Use outcome=None to redeem all winning positions

    Example:
        # Redeem all winning positions from a market
        intent = Intent.prediction_redeem(
            market_id="will-bitcoin-exceed-100000",
        )

        # Redeem only YES shares (if YES won)
        intent = Intent.prediction_redeem(
            market_id="will-bitcoin-exceed-100000",
            outcome="YES",
            shares="all",
        )
    """

    market_id: str
    outcome: PredictionOutcome | None = None
    shares: PredictionShareAmount = "all"
    protocol: str = "polymarket"
    chain: str | None = None
    intent_id: str = Field(default_factory=default_intent_id)
    created_at: datetime = Field(default_factory=default_timestamp)

    @model_validator(mode="after")
    def validate_prediction_redeem_intent(self) -> "PredictionRedeemIntent":
        """Validate prediction redeem parameters."""
        # Validate shares
        if isinstance(self.shares, Decimal) and self.shares <= 0:
            raise ValueError("shares must be positive")
        elif not isinstance(self.shares, Decimal) and self.shares != "all":
            raise ValueError("shares must be a positive Decimal or 'all'")

        return self

    @property
    def is_chained_amount(self) -> bool:
        """Check if this intent uses a chained amount from previous step."""
        return self.shares == "all"

    @property
    def intent_type(self) -> IntentType:
        """Return the type of this intent."""
        return IntentType.PREDICTION_REDEEM

    def serialize(self) -> dict[str, Any]:
        """Serialize the intent to a dictionary."""
        data = self.model_dump(mode="json")
        data["type"] = self.intent_type.value
        # Preserve "all" literal
        if self.shares == "all":
            data["shares"] = "all"
        return data

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> "PredictionRedeemIntent":
        """Deserialize a dictionary to a PredictionRedeemIntent."""
        clean_data = {k: v for k, v in data.items() if k != "type"}
        if "created_at" in clean_data and isinstance(clean_data["created_at"], str):
            clean_data["created_at"] = datetime.fromisoformat(clean_data["created_at"])
        return cls.model_validate(clean_data)
