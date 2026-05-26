"""Perpetual futures intent classes.

Intent classes for perpetual futures operations: open and close positions.
These intents support protocols like GMX V2, Hyperliquid, Drift, etc.
"""

from datetime import datetime
from decimal import Decimal
from typing import Any

from pydantic import Field, model_validator

from almanak.framework.models.base import (
    AlmanakImmutableModel,  # noqa: F401  -- re-exported for backward compatibility
    OptionalSafeDecimal,
    SafeDecimal,
    default_intent_id,
    default_timestamp,
)
from almanak.framework.models.base import (
    ChainedAmount as PydanticChainedAmount,
)

from .base import BaseIntent
from .intent_errors import InvalidProtocolParameterError
from .vocabulary import (
    IntentType,
)


def _capabilities_for(protocol_lower: str) -> dict[str, Any]:
    """Return the capability dict for ``protocol_lower`` via the connector registry.

    Function-local import: see ``lending_intents._capabilities_for`` for the
    full rationale. Same cold-boot circular-import constraint applies here.
    """
    from almanak.connectors._strategy_base.capabilities_registry import get_protocol_capabilities

    return get_protocol_capabilities(protocol_lower)


class PerpOpenIntent(BaseIntent):
    """Intent to open a perpetual futures position.

    Attributes:
        market: Market identifier (e.g., "ETH/USD") or market address
        collateral_token: Token symbol or address for collateral
        collateral_amount: Amount of collateral in token terms, or "all" for previous step output
        size_usd: Position size in USD terms
        is_long: True for long position, False for short
        leverage: Target leverage for the position (protocol-specific limits apply)
        max_slippage: Maximum acceptable slippage (e.g., 0.01 = 1%)
        protocol: Perpetuals protocol (default "gmx_v2")
        chain: Optional target chain for execution (defaults to strategy's primary chain)
        intent_id: Unique identifier for this intent
        created_at: Timestamp when the intent was created

    Note:
        When collateral_amount="all", the perp open will use the entire output from
        the previous step in a sequence. This is useful for chaining operations like:
        swap -> perp_open.

        The leverage parameter is validated against protocol-specific limits:
        - GMX V2: Supports leverage from 1.1x to 100x
        - Hyperliquid: Supports leverage from 1x to 50x
    """

    market: str
    collateral_token: str
    collateral_amount: PydanticChainedAmount
    size_usd: SafeDecimal
    is_long: bool = True
    leverage: SafeDecimal = Field(default=Decimal("1"))
    max_slippage: SafeDecimal = Field(default=Decimal("0.01"))
    protocol: str = "gmx_v2"
    chain: str | None = None
    intent_id: str = Field(default_factory=default_intent_id)
    created_at: datetime = Field(default_factory=default_timestamp)

    @model_validator(mode="after")
    def validate_perp_open_intent(self) -> "PerpOpenIntent":
        """Validate perp open parameters."""
        if isinstance(self.collateral_amount, Decimal) and self.collateral_amount <= 0:
            raise ValueError("collateral_amount must be positive")
        elif not isinstance(self.collateral_amount, Decimal) and self.collateral_amount != "all":
            raise ValueError("collateral_amount must be a positive Decimal or 'all'")
        if self.size_usd <= 0:
            raise ValueError("size_usd must be positive")
        if self.max_slippage < 0 or self.max_slippage > 1:
            raise ValueError("max_slippage must be between 0 and 1")
        if self.leverage < 1:
            raise ValueError("leverage must be >= 1")
        # Validate leverage against protocol capabilities
        self._validate_protocol_params()
        return self

    def _validate_protocol_params(self) -> None:
        """Validate protocol-specific parameters."""
        protocol_lower = self.protocol.lower()
        capabilities = _capabilities_for(protocol_lower)

        # Validate leverage if the protocol supports it
        if capabilities.get("supports_leverage", False):
            min_leverage = capabilities.get("min_leverage", Decimal("1"))
            max_leverage = capabilities.get("max_leverage", Decimal("100"))

            if self.leverage < min_leverage:
                raise InvalidProtocolParameterError(
                    protocol=self.protocol,
                    parameter="leverage",
                    value=self.leverage,
                    reason=f"Leverage must be at least {min_leverage}x for {self.protocol}",
                )
            if self.leverage > max_leverage:
                raise InvalidProtocolParameterError(
                    protocol=self.protocol,
                    parameter="leverage",
                    value=self.leverage,
                    reason=f"Leverage cannot exceed {max_leverage}x for {self.protocol}",
                )

    @property
    def is_chained_amount(self) -> bool:
        """Check if this intent uses a chained amount from previous step."""
        return self.collateral_amount == "all"

    @property
    def intent_type(self) -> IntentType:
        """Return the type of this intent."""
        return IntentType.PERP_OPEN

    def serialize(self) -> dict[str, Any]:
        """Serialize the intent to a dictionary."""
        data = self.model_dump(mode="json")
        data["type"] = self.intent_type.value
        if self.collateral_amount == "all":
            data["collateral_amount"] = "all"
        return data

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> "PerpOpenIntent":
        """Deserialize a dictionary to a PerpOpenIntent."""
        clean_data = {k: v for k, v in data.items() if k != "type"}
        if "created_at" in clean_data and isinstance(clean_data["created_at"], str):
            clean_data["created_at"] = datetime.fromisoformat(clean_data["created_at"])
        return cls.model_validate(clean_data)


class PerpCloseIntent(BaseIntent):
    """Intent to close a perpetual futures position.

    Attributes:
        market: Market identifier (e.g., "ETH/USD") or market address
        collateral_token: Token symbol or address for collateral
        is_long: Position direction
        size_usd: Amount to close in USD (None = close full position)
        max_slippage: Maximum acceptable slippage (e.g., 0.01 = 1%)
        protocol: Perpetuals protocol (default "gmx_v2")
        chain: Optional target chain for execution (defaults to strategy's primary chain)
        position_id: Optional venue-specific position identifier. Required for venues
            where (market, is_long, collateral_token) is insufficient to disambiguate
            an open position — in particular **PancakeSwap Perps (ApolloX)** which
            keys positions on a ``bytes32`` ``tradeHash``. Format is venue-specific:
              - ``pancakeswap_perps``: 0x-prefixed 32-byte hex (66 chars)
              - ``gmx_v2`` / ``hyperliquid`` / ``drift``: ignored (market+side suffices)
            Strategies obtain the ``tradeHash`` from the open receipt
            (``MarketPendingTrade`` / ``OpenMarketTrade`` events) via the
            ``ResultEnricher`` and persist it in their state.
        intent_id: Unique identifier for this intent
        created_at: Timestamp when the intent was created
    """

    market: str
    collateral_token: str
    is_long: bool
    size_usd: OptionalSafeDecimal = None
    max_slippage: SafeDecimal = Field(default=Decimal("0.01"))
    protocol: str = "gmx_v2"
    chain: str | None = None
    position_id: str | None = None
    intent_id: str = Field(default_factory=default_intent_id)
    created_at: datetime = Field(default_factory=default_timestamp)

    @model_validator(mode="after")
    def validate_perp_close_intent(self) -> "PerpCloseIntent":
        """Validate perp close parameters."""
        if self.size_usd is not None and self.size_usd <= 0:
            raise ValueError("size_usd must be positive if specified")
        if self.max_slippage < 0 or self.max_slippage > 1:
            raise ValueError("max_slippage must be between 0 and 1")
        if self.position_id is not None:
            pid = self.position_id
            if not isinstance(pid, str) or not pid.startswith("0x"):
                raise ValueError("position_id must be a 0x-prefixed hex string")
            # bytes32 = 32 bytes = 64 hex chars + "0x" prefix = 66 chars total.
            # We accept any positive-length hex past the 0x prefix to keep the
            # field venue-agnostic; protocol-specific compilers do the strict
            # length check (e.g., PCS Perps requires exactly bytes32).
            try:
                int(pid, 16)
            except ValueError as e:
                raise ValueError(f"position_id must be valid hex: {e}") from e
        return self

    @property
    def intent_type(self) -> IntentType:
        """Return the type of this intent."""
        return IntentType.PERP_CLOSE

    @property
    def close_full_position(self) -> bool:
        """Check if this intent is to close the full position."""
        return self.size_usd is None

    def serialize(self) -> dict[str, Any]:
        """Serialize the intent to a dictionary."""
        data = self.model_dump(mode="json")
        data["type"] = self.intent_type.value
        return data

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> "PerpCloseIntent":
        """Deserialize a dictionary to a PerpCloseIntent."""
        clean_data = {k: v for k, v in data.items() if k != "type"}
        if "created_at" in clean_data and isinstance(clean_data["created_at"], str):
            clean_data["created_at"] = datetime.fromisoformat(clean_data["created_at"])
        return cls.model_validate(clean_data)
