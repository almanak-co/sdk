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

    # Per-position economic state (Phase 4, VIB-2833)
    cost_basis_usd: Decimal = Decimal("0")  # Total capital deployed
    unrealized_pnl_usd: Decimal = Decimal("0")  # value - cost_basis
    realized_pnl_usd: Decimal = Decimal("0")  # Accumulated from closes
    entry_timestamp: str = ""  # ISO timestamp when position was opened
    last_update_timestamp: str = ""  # ISO timestamp of last valuation
    ledger_entry_id: str = ""  # FK to transaction_ledger for traceability

    def __post_init__(self) -> None:
        """Normalize numeric fields to Decimal."""
        if isinstance(self.value_usd, int | float | str):
            self.value_usd = Decimal(str(self.value_usd))
        for attr in ["cost_basis_usd", "unrealized_pnl_usd", "realized_pnl_usd"]:
            value = getattr(self, attr)
            if isinstance(value, int | float | str):
                setattr(self, attr, Decimal(str(value)))


def _position_to_dict(p: "PositionValue") -> dict[str, Any]:
    """Serialize a PositionValue to dict, including Phase 4 economic state fields."""
    d: dict[str, Any] = {
        "position_type": p.position_type.value if hasattr(p.position_type, "value") else str(p.position_type),
        "protocol": p.protocol,
        "chain": p.chain,
        "value_usd": str(p.value_usd),
        "label": p.label,
        "tokens": p.tokens,
        "details": p.details,
    }
    # Phase 4 economic state fields — only include when set to keep payload lean
    if p.cost_basis_usd:
        d["cost_basis_usd"] = str(p.cost_basis_usd)
    if p.unrealized_pnl_usd:
        d["unrealized_pnl_usd"] = str(p.unrealized_pnl_usd)
    if p.realized_pnl_usd:
        d["realized_pnl_usd"] = str(p.realized_pnl_usd)
    if p.entry_timestamp:
        d["entry_timestamp"] = p.entry_timestamp
    if p.last_update_timestamp:
        d["last_update_timestamp"] = p.last_update_timestamp
    if p.ledger_entry_id:
        d["ledger_entry_id"] = p.ledger_entry_id
    return d


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

    # Token prices used for valuation (audit trail)
    # key: "chain:address", value: {"price_usd": str, "symbol": str, "decimals": int|None}
    token_prices: dict[str, dict] = field(default_factory=dict)

    # Deployed capital: sum of cost_basis_usd across all positions.
    # Populated by PortfolioValuer after _enrich_position_pnl() enriches each
    # PositionValue.  Distinct from total_value_usd (strategy-scoped) so callers can
    # compute strategy-level PnL without conflating uninvested wallet funds.
    # Defaults to Decimal("0") when no accounting events exist (e.g. dry-run or
    # a strategy with no open positions).
    deployed_capital_usd: Decimal = Decimal("0")

    # Full wallet total (tracked tokens + all positions, including borrows).
    # This is the pre-VIB-3614 behaviour: sum of all token balances for tracked
    # tokens plus all position values.  Kept for operator debugging / alerting;
    # NOT used for PnL calculations.  Stored in DB but not shown in the dashboard.
    wallet_total_value_usd: Decimal = Decimal("0")

    # Metadata
    chain: str = ""
    iteration_number: int = 0
    snapshot_metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Normalize numeric fields to Decimal."""
        if isinstance(self.total_value_usd, int | float | str):
            self.total_value_usd = Decimal(str(self.total_value_usd))
        if isinstance(self.available_cash_usd, int | float | str):
            self.available_cash_usd = Decimal(str(self.available_cash_usd))
        if isinstance(self.deployed_capital_usd, int | float | str):
            self.deployed_capital_usd = Decimal(str(self.deployed_capital_usd))
        if isinstance(self.wallet_total_value_usd, int | float | str):
            self.wallet_total_value_usd = Decimal(str(self.wallet_total_value_usd))

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
        data: dict[str, Any] = {
            "timestamp": self.timestamp.isoformat(),
            "strategy_id": self.strategy_id,
            "total_value_usd": str(self.total_value_usd),
            "available_cash_usd": str(self.available_cash_usd),
            "deployed_capital_usd": str(self.deployed_capital_usd),
            "wallet_total_value_usd": str(self.wallet_total_value_usd),
            "value_confidence": self.value_confidence.value,
            "error": self.error,
            "positions": [_position_to_dict(p) for p in self.positions],
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
            "token_prices": self.token_prices,
            "chain": self.chain,
            "iteration_number": self.iteration_number,
        }
        if self.snapshot_metadata:
            data["snapshot_metadata"] = self.snapshot_metadata
        return data

    # VIB-3923 — schema version of the canonical envelope shape
    # ``{schema_version, positions, metadata, reconciliation}``. Bumped when the
    # envelope contract changes; readers tolerate both legacy bare lists and
    # any prior envelope schema (forward compatibility on read, strict on
    # write).
    SNAPSHOT_ENVELOPE_SCHEMA_VERSION = 1

    def to_positions_payload(self) -> dict[str, Any]:
        """Serialize positions_json payload for persistence.

        VIB-3923 — every NEW write emits the envelope shape
        ``{schema_version, positions, metadata, reconciliation}``. The
        bare-list legacy shape is kept as a *read*-tolerant fallback in
        ``unpack_positions_payload`` but never written. This stops the
        May 3 production class where snapshots silently went out the door
        as legacy lists, dropping the metadata fields downstream readers
        rely on (G6 reconciliation tile, deployed_capital traceability).

        Pre-fix: a snapshot with empty ``snapshot_metadata`` returned a
        bare list — indistinguishable from the legacy persistence shape.
        Operators reading post-fix snapshots saw "envelope honest" cells
        flap to FAIL whenever a tile happened to construct a snapshot
        without populating metadata.
        """
        positions = [_position_to_dict(p) for p in self.positions]
        # ``reconciliation`` is split out of ``metadata`` for the
        # dashboard's G6 tile so it can be addressed at a stable key
        # without parsing the variable-shape metadata bag.
        metadata = dict(self.snapshot_metadata) if self.snapshot_metadata else {}
        reconciliation = metadata.pop("reconciliation", {}) if isinstance(metadata, dict) else {}
        return {
            "schema_version": self.SNAPSHOT_ENVELOPE_SCHEMA_VERSION,
            "positions": positions,
            "metadata": metadata,
            "reconciliation": reconciliation,
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
                    cost_basis_usd=Decimal(p.get("cost_basis_usd", "0")),
                    unrealized_pnl_usd=Decimal(p.get("unrealized_pnl_usd", "0")),
                    realized_pnl_usd=Decimal(p.get("realized_pnl_usd", "0")),
                    entry_timestamp=p.get("entry_timestamp", ""),
                    last_update_timestamp=p.get("last_update_timestamp", ""),
                    ledger_entry_id=p.get("ledger_entry_id", ""),
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
            deployed_capital_usd=Decimal(data.get("deployed_capital_usd", "0")),
            wallet_total_value_usd=Decimal(data.get("wallet_total_value_usd", "0")),
            value_confidence=ValueConfidence(data.get("value_confidence", "HIGH")),
            error=data.get("error"),
            positions=positions,
            wallet_balances=wallet_balances,
            token_prices=data.get("token_prices", {}),
            chain=data.get("chain", ""),
            iteration_number=data.get("iteration_number", 0),
            snapshot_metadata=data.get("snapshot_metadata", {}),
        )

    @staticmethod
    def unpack_positions_payload(payload: Any) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        """Parse either the legacy list payload or the metadata envelope."""
        if isinstance(payload, list):
            return payload, {}
        if isinstance(payload, dict):
            positions = payload.get("positions", [])
            metadata = payload.get("metadata", {})
            if isinstance(positions, list) and isinstance(metadata, dict):
                return positions, metadata
        return [], {}


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

    # Phase 1a: rich accounting fields (persisted in SQLite, round-tripped)
    positions_json: str = "[]"  # JSON-encoded position details
    cycle_id: str | None = None  # Current execution cycle

    # Phase 4: canonical identity and execution mode (VIB-2835/2837)
    deployment_id: str = ""  # Canonical deployment key (wallet+chain hash or --id)
    execution_mode: str = ""  # "live", "paper", "dry_run", "backtest"
    is_complete: bool = True  # Whether all expected records for this cycle were committed

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
            "positions_json": self.positions_json,
            "cycle_id": self.cycle_id,
            "deployment_id": self.deployment_id,
            "execution_mode": self.execution_mode,
            "is_complete": self.is_complete,
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
            positions_json=data.get("positions_json", "[]"),
            cycle_id=data.get("cycle_id"),
            deployment_id=data.get("deployment_id", ""),
            execution_mode=data.get("execution_mode", ""),
            is_complete=data.get("is_complete", True),
        )
