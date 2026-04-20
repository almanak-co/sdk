"""Core data models for strategy market data and execution results.

This module contains TokenBalance, PriceData, ExecutionResult, and related
type aliases used throughout the strategy framework.

These were extracted from intent_strategy.py for maintainability. All symbols
remain importable from almanak.framework.strategies.intent_strategy.
"""

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from ..intents import StepResult
from ..intents.vocabulary import AnyIntent
from ..models.reproduction_bundle import ActionBundle
from .indicator_models import RSIData


@dataclass
class TokenBalance:
    """Balance information for a single token.

    Supports numeric comparisons so strategy authors can write:
        if market.balance("ETH") > Decimal("1.0"): ...
        amount = min(trade_size, market.balance("USDC"))

    Comparisons delegate to the ``balance`` field (native units).

    Attributes:
        symbol: Token symbol (e.g., "ETH", "USDC")
        balance: Token balance in native units
        balance_usd: Token balance in USD terms
        address: Token contract address
    """

    symbol: str
    balance: Decimal
    balance_usd: Decimal
    address: str = ""

    # -- numeric protocol --------------------------------------------------

    def _to_decimal(self, other: object) -> Decimal | None:
        """Coerce other to Decimal for comparison, or return None."""
        if isinstance(other, TokenBalance):
            return other.balance
        if isinstance(other, Decimal):
            return other
        if isinstance(other, int | float):
            return Decimal(str(other))
        return None

    def __eq__(self, other: object) -> bool:
        if isinstance(other, TokenBalance):
            return self.symbol == other.symbol and self.balance == other.balance and self.address == other.address
        val = self._to_decimal(other)
        if val is None:
            return NotImplemented
        return self.balance == val

    def __lt__(self, other: object) -> bool:
        val = self._to_decimal(other)
        if val is None:
            return NotImplemented
        return self.balance < val

    def __le__(self, other: object) -> bool:
        val = self._to_decimal(other)
        if val is None:
            return NotImplemented
        return self.balance <= val

    def __gt__(self, other: object) -> bool:
        val = self._to_decimal(other)
        if val is None:
            return NotImplemented
        return self.balance > val

    def __ge__(self, other: object) -> bool:
        val = self._to_decimal(other)
        if val is None:
            return NotImplemented
        return self.balance >= val

    def __hash__(self) -> int:
        # Hash only on balance to satisfy the Python invariant: a == b => hash(a) == hash(b).
        # Since __eq__ returns True for `tb == Decimal(100)`, hash must match hash(Decimal(100)).
        # Different tokens with the same balance will hash-collide but __eq__ distinguishes them.
        return hash(self.balance)

    def __float__(self) -> float:
        return float(self.balance)

    def __int__(self) -> int:
        return int(self.balance)

    def __format__(self, format_spec: str) -> str:
        if format_spec:
            return format(self.balance, format_spec)
        return str(self.balance)

    def __repr__(self) -> str:
        return f"TokenBalance(symbol={self.symbol!r}, balance={self.balance}, balance_usd={self.balance_usd})"


@dataclass
class PriceData:
    """Price data for a token.

    Attributes:
        price: Current price in USD
        price_24h_ago: Price 24 hours ago in USD
        change_24h_pct: 24-hour price change percentage
        high_24h: 24-hour high
        low_24h: 24-hour low
        timestamp: When the price was fetched
    """

    price: Decimal
    price_24h_ago: Decimal = Decimal("0")
    change_24h_pct: Decimal = Decimal("0")
    high_24h: Decimal = Decimal("0")
    low_24h: Decimal = Decimal("0")
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))


# Type for price oracle function. Runtime supports both legacy
# ``(token, quote)`` callables and chain-aware ``(token, quote, chain)``
# callables, so keep the type broad enough for both.
PriceOracle = Callable[..., Decimal]

# Type for RSI provider function (accepts optional timeframe kwarg for backward compat)
RSIProvider = Callable[..., RSIData]

# Type for balance provider function
BalanceProvider = Callable[[str], TokenBalance]


@dataclass
class ExecutionResult:
    """Result of strategy execution.

    Attributes:
        intent: The intent that was executed (or None if HOLD)
        action_bundle: The compiled action bundle (or None)
        state_machine_result: Final state machine step result
        success: Whether execution was successful
        error: Error message if failed
        execution_time_ms: Time taken for execution in milliseconds
    """

    intent: AnyIntent | None
    action_bundle: ActionBundle | None = None
    state_machine_result: StepResult | None = None
    success: bool = False
    error: str | None = None
    execution_time_ms: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "intent_type": self.intent.intent_type.value if self.intent else None,
            "intent_id": self.intent.intent_id if self.intent else None,
            "action_bundle": self.action_bundle.to_dict() if self.action_bundle else None,
            "success": self.success,
            "error": self.error,
            "execution_time_ms": self.execution_time_ms,
        }


__all__ = [
    "TokenBalance",
    "PriceData",
    "PriceOracle",
    "RSIProvider",
    "BalanceProvider",
    "ExecutionResult",
]
