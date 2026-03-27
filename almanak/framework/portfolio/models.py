"""Portfolio tracking models for the Almanak Strategy Framework.

Provides generic data structures for tracking portfolio value and positions
across all strategy types (LP, Lending, Staking, Trading, Perps, CEX).

These models are used by:
- IntentStrategy.get_portfolio_snapshot() - Strategy-level value reporting
- StrategyRunner - Capturing and persisting value snapshots
- Dashboard - Displaying portfolio value and PnL charts
"""

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from almanak.framework.teardown.models import PositionType


class ValueConfidence(StrEnum):
    """Confidence level of portfolio value calculation.

    Used to indicate data quality in the dashboard:
    - HIGH: Direct on-chain queries, accurate values
    - ESTIMATED: API data or estimates (CEX balances, prediction markets)
    - STALE: Data older than acceptable threshold
    - UNAVAILABLE: Value could not be computed (error state)
    """

    HIGH = "HIGH"
    ESTIMATED = "ESTIMATED"
    STALE = "STALE"
    UNAVAILABLE = "UNAVAILABLE"


@dataclass
class TokenBalance:
    """Token balance with USD value.

    Represents wallet holdings not captured by position tracking
    (e.g., uninvested funds, pending swaps).
    """

    symbol: str
    balance: Decimal
    value_usd: Decimal
    address: str = ""
    price_usd: Decimal | None = None  # Per-token price; enables redenomination (DB persistence is Week 2)

    def __post_init__(self) -> None:
        """Normalize numeric fields to Decimal."""
        if isinstance(self.balance, int | float | str):
            self.balance = Decimal(str(self.balance))
        if isinstance(self.value_usd, int | float | str):
            self.value_usd = Decimal(str(self.value_usd))
        if self.price_usd is not None and isinstance(self.price_usd, int | float | str):
            self.price_usd = Decimal(str(self.price_usd))


@dataclass
class PositionValue:
    """Generic position value for any protocol.

    Represents a single position (LP, lending supply/borrow, perp, stake)
    with its current USD value and display metadata.
    """

    position_type: "PositionType"
    protocol: str
    chain: str
    value_usd: Decimal

    # Display info for dashboard
    label: str  # e.g., "WETH/USDC LP", "AAVE WETH Supply"
    tokens: list[str] = field(default_factory=list)

    # Protocol-specific details for drill-down views
    details: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Normalize numeric fields to Decimal."""
        if isinstance(self.value_usd, int | float | str):
            self.value_usd = Decimal(str(self.value_usd))


@dataclass
class PortfolioSnapshot:
    """Point-in-time portfolio state for any strategy type.

    Captured after each strategy iteration and persisted for:
    - Dashboard value display
    - PnL calculation over time
    - Historical charts

    Example:
        snapshot = strategy.get_portfolio_snapshot(market)
        # snapshot.total_value_usd = Decimal("15234.50")
        # snapshot.value_confidence = ValueConfidence.HIGH
        # snapshot.positions = [PositionValue(...), ...]
    """

    timestamp: datetime
    strategy_id: str

    # Core values
    total_value_usd: Decimal
    available_cash_usd: Decimal  # Uninvested wallet funds

    # Value confidence indicator for dashboard display
    value_confidence: ValueConfidence = ValueConfidence.HIGH
    error: str | None = None  # Error message if value could not be computed

    # Positions by type (for dashboard breakdown)
    positions: list[PositionValue] = field(default_factory=list)

    # Wallet balances (uninvested funds)
    wallet_balances: list[TokenBalance] = field(default_factory=list)

    # Metadata
    chain: str = ""
    iteration_number: int = 0

    def __post_init__(self) -> None:
        """Normalize numeric fields to Decimal."""
        if isinstance(self.total_value_usd, int | float | str):
            self.total_value_usd = Decimal(str(self.total_value_usd))
        if isinstance(self.available_cash_usd, int | float | str):
            self.available_cash_usd = Decimal(str(self.available_cash_usd))

    @property
    def position_value_usd(self) -> Decimal:
        """Total value of all positions (excluding wallet balances)."""
        return sum((p.value_usd for p in self.positions), Decimal("0"))

    @property
    def is_valid(self) -> bool:
        """Check if snapshot contains valid data."""
        return self.value_confidence != ValueConfidence.UNAVAILABLE

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary for JSON storage."""
        return {
            "timestamp": self.timestamp.isoformat(),
            "strategy_id": self.strategy_id,
            "total_value_usd": str(self.total_value_usd),
            "available_cash_usd": str(self.available_cash_usd),
            "value_confidence": self.value_confidence.value,
            "error": self.error,
            "positions": [
                {
                    "position_type": p.position_type.value
                    if hasattr(p.position_type, "value")
                    else str(p.position_type),
                    "protocol": p.protocol,
                    "chain": p.chain,
                    "value_usd": str(p.value_usd),
                    "label": p.label,
                    "tokens": p.tokens,
                    "details": p.details,
                }
                for p in self.positions
            ],
            "wallet_balances": [
                {
                    "symbol": b.symbol,
                    "balance": str(b.balance),
                    "value_usd": str(b.value_usd),
                    "price_usd": str(b.price_usd) if b.price_usd is not None else None,
                    "address": b.address,
                }
                for b in self.wallet_balances
            ],
            "chain": self.chain,
            "iteration_number": self.iteration_number,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PortfolioSnapshot":
        """Deserialize from dictionary."""
        from almanak.framework.teardown.models import PositionType

        positions = []
        for p in data.get("positions", []):
            positions.append(
                PositionValue(
                    position_type=PositionType(p["position_type"]),
                    protocol=p["protocol"],
                    chain=p["chain"],
                    value_usd=Decimal(p["value_usd"]),
                    label=p["label"],
                    tokens=p.get("tokens", []),
                    details=p.get("details", {}),
                )
            )

        wallet_balances = []
        for b in data.get("wallet_balances", []):
            price_usd = b.get("price_usd")
            wallet_balances.append(
                TokenBalance(
                    symbol=b["symbol"],
                    balance=Decimal(b["balance"]),
                    value_usd=Decimal(b["value_usd"]),
                    address=b.get("address", ""),
                    price_usd=Decimal(price_usd) if price_usd is not None else None,
                )
            )

        return cls(
            timestamp=datetime.fromisoformat(data["timestamp"]),
            strategy_id=data["strategy_id"],
            total_value_usd=Decimal(data["total_value_usd"]),
            available_cash_usd=Decimal(data["available_cash_usd"]),
            value_confidence=ValueConfidence(data.get("value_confidence", "HIGH")),
            error=data.get("error"),
            positions=positions,
            wallet_balances=wallet_balances,
            chain=data.get("chain", ""),
            iteration_number=data.get("iteration_number", 0),
        )


@dataclass
class PortfolioMetrics:
    """Computed metrics for PnL tracking.

    Stored separately from snapshots to persist baseline values
    that survive strategy restarts.

    The initial_value_usd is set once on first run and preserved
    across restarts to enable accurate cumulative PnL calculation.
    """

    strategy_id: str
    timestamp: datetime

    # Current value from latest snapshot
    total_value_usd: Decimal

    # Baseline tracking (persisted, survives restarts)
    initial_value_usd: Decimal  # Set once on first run

    # Capital flow tracking for accurate PnL
    deposits_usd: Decimal = Decimal("0")  # Cumulative deposits
    withdrawals_usd: Decimal = Decimal("0")  # Cumulative withdrawals
    gas_spent_usd: Decimal = Decimal("0")  # Cumulative gas costs

    def __post_init__(self) -> None:
        """Normalize numeric fields to Decimal."""
        for attr in ["total_value_usd", "initial_value_usd", "deposits_usd", "withdrawals_usd", "gas_spent_usd"]:
            value = getattr(self, attr)
            if isinstance(value, int | float | str):
                setattr(self, attr, Decimal(str(value)))

    @property
    def pnl_before_gas(self) -> Decimal:
        """PnL excluding gas costs, adjusted for capital flows.

        Formula: current_value - initial_value - deposits + withdrawals
        """
        return self.total_value_usd - self.initial_value_usd - self.deposits_usd + self.withdrawals_usd

    @property
    def pnl_after_gas(self) -> Decimal:
        """Net PnL including gas costs."""
        return self.pnl_before_gas - self.gas_spent_usd

    @property
    def roi_percent(self) -> Decimal:
        """Return on investment percentage (before gas)."""
        if self.initial_value_usd == 0:
            return Decimal("0")
        return (self.pnl_before_gas / self.initial_value_usd) * 100

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary for storage."""
        return {
            "strategy_id": self.strategy_id,
            "timestamp": self.timestamp.isoformat(),
            "total_value_usd": str(self.total_value_usd),
            "initial_value_usd": str(self.initial_value_usd),
            "deposits_usd": str(self.deposits_usd),
            "withdrawals_usd": str(self.withdrawals_usd),
            "gas_spent_usd": str(self.gas_spent_usd),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PortfolioMetrics":
        """Deserialize from dictionary."""
        return cls(
            strategy_id=data["strategy_id"],
            timestamp=datetime.fromisoformat(data["timestamp"]),
            total_value_usd=Decimal(data["total_value_usd"]),
            initial_value_usd=Decimal(data["initial_value_usd"]),
            deposits_usd=Decimal(data.get("deposits_usd", "0")),
            withdrawals_usd=Decimal(data.get("withdrawals_usd", "0")),
            gas_spent_usd=Decimal(data.get("gas_spent_usd", "0")),
        )
