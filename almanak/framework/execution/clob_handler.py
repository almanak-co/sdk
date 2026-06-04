"""Venue-neutral CLOB order-execution dataclasses.

This module owns the framework-side, venue-neutral data models for off-chain CLOB
(Central Limit Order Book) order execution and persistence:

* ``ClobOrderStatus`` — the order-lifecycle enum
  (PENDING → SUBMITTED → LIVE | MATCHED | CANCELLED | FAILED).
* ``ClobFill`` — a single fill event (price / size / fee / timestamp).
* ``ClobOrderState`` — the persisted order record (order_id, market_id, token_id,
  side, status, price, size, filled_size, fills, timestamps). This is the
  serialization contract read/written by ``StateManager`` and the SQLite backend,
  so its shape must stay stable.
* ``ClobExecutionResult`` — the execution result mirroring on-chain
  ``TransactionResult`` semantics (success, order_id, status, fills, error) for
  uniform handling in ``StrategyRunner``.

These types carry **no** connector/venue dependency, so the framework execution
layer holds no connector import.

VIB-4989 (epic VIB-4851 self-containment): the Polymarket CLOB handler
implementation that used to live here — ``ClobActionHandler`` — now lives in the
connector folder (``almanak/connectors/polymarket/clob_handler.py``). The framework
reaches it through the ``PredictionExecuteRegistry`` seam; the runner registers the
connector-built handler on the ``ExecutionHandler`` Protocol. The framework never
signs CLOB orders — the gateway holds the keys and signs server-side.

The ``_parse_decimal`` module helper stays here (venue-neutral metadata coercion)
and is imported back by the connector handler (connector→framework is allowed).
"""

from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from almanak.framework.execution.extracted_data import PredictionFill


# =============================================================================
# Enums
# =============================================================================


class ClobOrderStatus(StrEnum):
    """Status of a CLOB order.

    Lifecycle:
        PENDING → SUBMITTED → LIVE → MATCHED (full)
                     ↓           ↓
                  FAILED    CANCELLED (user or expiry)
                              ↓
                           MATCHED (partial before cancel)
    """

    PENDING = "pending"  # Order built, not submitted
    SUBMITTED = "submitted"  # API accepted, awaiting confirmation
    LIVE = "live"  # Order active on order book
    MATCHED = "matched"  # Order fully filled
    PARTIALLY_FILLED = "partially_filled"  # Order has partial fills
    CANCELLED = "cancelled"  # Order cancelled
    EXPIRED = "expired"  # Order expired
    FAILED = "failed"  # Order rejected


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class ClobFill:
    """A single fill event for a CLOB order.

    Attributes:
        fill_id: Unique fill identifier
        price: Fill price
        size: Fill size (shares)
        fee: Fee charged for this fill
        timestamp: When the fill occurred
        counterparty: Counterparty address (optional)
    """

    fill_id: str
    price: Decimal
    size: Decimal
    fee: Decimal
    timestamp: datetime
    counterparty: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "fill_id": self.fill_id,
            "price": str(self.price),
            "size": str(self.size),
            "fee": str(self.fee),
            "timestamp": self.timestamp.isoformat(),
            "counterparty": self.counterparty,
        }


@dataclass
class ClobOrderState:
    """Persistent state for a CLOB order.

    This model is persisted in StateManager for crash recovery
    and order tracking across strategy restarts.

    Attributes:
        order_id: Unique order identifier from CLOB API
        market_id: Polymarket market ID
        token_id: Outcome token ID (YES/NO token)
        side: BUY or SELL
        status: Current order status
        price: Order price (0.01-0.99)
        size: Order size (number of shares)
        filled_size: Amount filled so far
        average_fill_price: Average price of fills
        fills: List of fill events
        order_type: GTC, IOC, FOK
        intent_id: Associated intent ID (for tracing)
        submitted_at: When order was submitted
        updated_at: Last status update
        error: Error message if failed
        metadata: Additional metadata
    """

    order_id: str
    market_id: str
    token_id: str
    side: str  # "BUY" or "SELL"
    status: ClobOrderStatus
    price: Decimal
    size: Decimal
    filled_size: Decimal = Decimal("0")
    average_fill_price: Decimal | None = None
    fills: list[ClobFill] = field(default_factory=list)
    order_type: str = "GTC"
    intent_id: str | None = None
    submitted_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    error: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    # Canonical deployment identity (blueprint 29 §3). Must be stamped with
    # the runner's resolved deployment_id by the caller before the order is
    # persisted — clob_orders is deployment-scoped and
    # SQLiteStore.save_clob_order rejects a blank id. Defaults to "" for
    # in-memory / pre-persist use. NOTE: the CLOB execution path does not yet
    # persist orders at all; wiring that path (and the stamping) is
    # outstanding follow-up work, not done by VIB-4722.
    deployment_id: str = ""

    @property
    def is_open(self) -> bool:
        """Return True if order is still open (live or pending)."""
        return self.status in (
            ClobOrderStatus.PENDING,
            ClobOrderStatus.SUBMITTED,
            ClobOrderStatus.LIVE,
            ClobOrderStatus.PARTIALLY_FILLED,
        )

    @property
    def is_terminal(self) -> bool:
        """Return True if order is in a terminal state."""
        return self.status in (
            ClobOrderStatus.MATCHED,
            ClobOrderStatus.CANCELLED,
            ClobOrderStatus.EXPIRED,
            ClobOrderStatus.FAILED,
        )

    @property
    def fill_percentage(self) -> float:
        """Return percentage of order filled (0.0 to 100.0)."""
        if self.size <= 0:
            return 0.0
        return float((self.filled_size / self.size) * 100)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "deployment_id": self.deployment_id,
            "order_id": self.order_id,
            "market_id": self.market_id,
            "token_id": self.token_id,
            "side": self.side,
            "status": self.status.value,
            "price": str(self.price),
            "size": str(self.size),
            "filled_size": str(self.filled_size),
            "average_fill_price": str(self.average_fill_price) if self.average_fill_price else None,
            "fills": [f.to_dict() for f in self.fills],
            "order_type": self.order_type,
            "intent_id": self.intent_id,
            "submitted_at": self.submitted_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "error": self.error,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ClobOrderState":
        """Create from dictionary."""
        fills = [
            ClobFill(
                fill_id=f["fill_id"],
                price=Decimal(f["price"]),
                size=Decimal(f["size"]),
                fee=Decimal(f["fee"]),
                timestamp=datetime.fromisoformat(f["timestamp"]),
                counterparty=f.get("counterparty"),
            )
            for f in data.get("fills", [])
        ]

        return cls(
            order_id=data["order_id"],
            market_id=data["market_id"],
            token_id=data["token_id"],
            side=data["side"],
            status=ClobOrderStatus(data["status"]),
            price=Decimal(data["price"]),
            size=Decimal(data["size"]),
            filled_size=Decimal(data.get("filled_size", "0")),
            average_fill_price=Decimal(data["average_fill_price"]) if data.get("average_fill_price") else None,
            fills=fills,
            order_type=data.get("order_type", "GTC"),
            intent_id=data.get("intent_id"),
            submitted_at=datetime.fromisoformat(data["submitted_at"]),
            updated_at=datetime.fromisoformat(data["updated_at"]),
            error=data.get("error"),
            metadata=data.get("metadata", {}),
            deployment_id=data.get("deployment_id", ""),
        )


@dataclass
class ClobExecutionResult:
    """Result of a CLOB order execution.

    This mirrors TransactionResult semantics for uniform handling
    in the execution pipeline.

    VIB-3218: ``success`` reflects the CLOB order's *classified lifecycle*,
    not just HTTP acceptance. The handler computes
    ``success = (status != FAILED)`` after running the submission through
    :meth:`_classify_status`, so an IOC/FOK the API accepted but that
    matched zero liquidity surfaces here as ``success=False`` -- the
    StrategyRunner then routes it through the failure path (no ledger
    entry, no ``on_intent_executed(success=True)`` callback).

    A GTC that goes to the book (LIVE) and an IOC/FOK that at-least-partially
    matched (MATCHED) both return ``success=True`` but have very different
    ``filled_size`` values. Strategies distinguish them by reading
    ``filled_size`` -- or, preferably, the richer :class:`PredictionFill`
    attached to the downstream :class:`ExecutionResult`.

    Exception path: any uncaught error during submission yields
    ``success=False`` with ``status=FAILED`` and ``error`` populated.

    Attributes:
        success: True iff the classified status is NOT FAILED. Combines
            "API accepted the request" AND "the classified lifecycle isn't
            a terminal failure" (unmatched IOC/FOK, rejected, etc.).
        order_id: API-assigned order identifier
        status: Current order lifecycle state (LIVE/MATCHED/PARTIALLY_FILLED/
            FAILED/...). Computed by :meth:`_classify_status` from the raw
            API status, the fill amount, and the order type hint.
        filled_size: Amount filled at response time. 0 for resting GTC or for
            IOC orders that failed to match any liquidity.
        avg_fill_price: Volume-weighted average price of immediate fills.
            None when no portion of the order filled.
        requested_size: The size the intent asked for. Preserved here so
            the downstream :class:`PredictionFill` can expose fill-vs-request
            without re-reading the intent.
        fills: List of fill events
        error: Error message if failed
        submitted_at: When order was submitted
    """

    success: bool
    order_id: str | None = None
    status: ClobOrderStatus = ClobOrderStatus.PENDING
    filled_size: Decimal = Decimal("0")
    avg_fill_price: Decimal | None = None
    requested_size: Decimal | None = None
    fills: list[ClobFill] = field(default_factory=list)
    error: str | None = None
    submitted_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    # VIB-3710: gateway-side setup transactions (approvals + source-asset →
    # pUSD wrap) submitted before this order. Stored as a list of dicts here
    # (the connector dataclass would force a connector import on every result
    # consumer). The clob_handler converts them to typed PredictionSetupTx
    # when projecting onto PredictionFill.
    setup_txs: list[dict[str, Any]] = field(default_factory=list)
    # VIB-3710: pUSD operator fee charged at match time. None when the order
    # did not match or when the response did not carry a fee field.
    fee_pusd: Decimal | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "success": self.success,
            "order_id": self.order_id,
            "status": self.status.value,
            "filled_size": str(self.filled_size),
            "avg_fill_price": str(self.avg_fill_price) if self.avg_fill_price is not None else None,
            "requested_size": str(self.requested_size) if self.requested_size is not None else None,
            "fills": [f.to_dict() for f in self.fills],
            "error": self.error,
            "submitted_at": self.submitted_at.isoformat(),
            "setup_txs": list(self.setup_txs),
            "fee_pusd": str(self.fee_pusd) if self.fee_pusd is not None else None,
        }

    def to_prediction_fill(self) -> "PredictionFill | None":
        """Project this result onto a :class:`PredictionFill` for strategies.

        Returns ``None`` when ``requested_size`` is unknown (e.g. a "SELL all"
        intent where the requested size is the current position balance,
        derived at compile time but not persisted on the bundle). In that
        case strategies should read post-execution wallet balances instead.

        VIB-3710: also propagates setup_txs + fee_pusd onto the PredictionFill
        so the enricher and prediction handler downstream can fold gas + fees
        into the position's loaded cost basis.
        """
        from almanak.framework.execution.extracted_data import PredictionFill, PredictionSetupTx

        if self.requested_size is None:
            return None
        setup_tx_objs = tuple(
            PredictionSetupTx(
                tx_hash=str(entry.get("tx_hash", "")),
                description=str(entry.get("description", "")),
                gas_used=int(entry.get("gas_used", 0) or 0),
                gas_price_wei=str(entry.get("gas_price_wei", "0")),
                total_cost_wei=str(entry.get("total_cost_wei", "0")),
            )
            for entry in self.setup_txs
        )
        return PredictionFill(
            filled_shares=self.filled_size,
            requested_shares=self.requested_size,
            avg_fill_price=self.avg_fill_price,
            order_id=self.order_id,
            status=self.status.value,
            setup_txs=setup_tx_objs,
            fee_pusd=self.fee_pusd,
        )


# =============================================================================
# Module helpers
# =============================================================================


def _parse_decimal(value: Any) -> Decimal | None:
    """Best-effort parse of ActionBundle-metadata values into Decimal.

    Used for the requested-size hint we thread through to ``PredictionFill``.
    ActionBundle metadata values are stringified at compile time, so we
    coerce back to ``Decimal``; non-numeric or missing values simply drop to
    ``None`` rather than exploding.
    """
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (ValueError, ArithmeticError):
        return None


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    # Enums
    "ClobOrderStatus",
    # Data classes
    "ClobFill",
    "ClobOrderState",
    "ClobExecutionResult",
]
