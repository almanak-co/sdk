"""CLOB Action Handler for Off-Chain Order Execution.

This module provides the ClobActionHandler class for executing Polymarket CLOB
(Central Limit Order Book) orders. Unlike on-chain transactions, CLOB orders
are submitted off-chain via signed API requests.

ARCHITECTURE DESIGN
===================

The CLOB execution architecture integrates with the existing execution pipeline
while handling the fundamental differences between on-chain and off-chain execution:

1. DETECTION PATTERN
   ----------------
   The PlanExecutor detects Polymarket bundles by checking:
   - ActionBundle.metadata["protocol"] == "polymarket"
   - ActionBundle.transactions == [] (CLOB orders have no on-chain txs)
   - ActionBundle.metadata["order_payload"] exists

   This allows routing to ClobActionHandler before the standard on-chain path.

2. EXECUTION FLOW
   ----------------
   ```
   IntentCompiler.compile() → ActionBundle (with order_payload in metadata)
                                    ↓
   PlanExecutor._execute_step() → detects protocol: polymarket
                                    ↓
   ClobActionHandler.can_handle() → True
                                    ↓
   ClobActionHandler.execute() → Submit order via CLOB API
                                    ↓
   ClobExecutionResult → Contains order_id, status, fills
                                    ↓
   StateManager.save_clob_order() → Persist order state
   ```

3. INTEGRATION WITH ExecutionOrchestrator
   ---------------------------------------
   The ExecutionOrchestrator handles on-chain transactions exclusively.
   CLOB orders bypass the orchestrator entirely since they don't require:
   - Nonce management
   - Gas estimation
   - Transaction signing (different signing scheme)
   - Mempool submission

   Instead, ClobActionHandler:
   - Uses ClobClient for L2 HMAC authentication
   - Submits via CLOB REST API
   - Returns structured ClobExecutionResult

4. ORDER LIFECYCLE
   ----------------
   ```
   PENDING → SUBMITTED → [LIVE | MATCHED | CANCELLED]
       ↓         ↓            ↓       ↓         ↓
    (error)   (API ok)     (book)  (fill)   (cancel)
   ```

   States:
   - PENDING: Order built, not yet submitted
   - SUBMITTED: API accepted order, waiting for confirmation
   - LIVE: Order is active on the order book
   - MATCHED: Order fully or partially filled
   - CANCELLED: Order cancelled by user or expired
   - FAILED: Order rejected by API

5. IDEMPOTENCY
   ------------
   Safe retries are supported through:
   - Unique order signatures (EIP-712 based)
   - Order ID tracking before submission
   - Duplicate detection via ClobClient
   - State persistence enables crash recovery

   On restart:
   1. Load persisted ClobOrderState from StateManager
   2. Query CLOB API for current order status
   3. Reconcile persisted vs actual state
   4. Resume or mark as failed

6. STATE PERSISTENCE (via StateManager)
   -------------------------------------
   ClobOrderState model captures:
   - order_id: Unique order identifier from CLOB API
   - market_id: Polymarket market ID
   - token_id: Outcome token ID (YES/NO)
   - side: BUY or SELL
   - status: Current order status
   - price: Order price
   - size: Order size (shares)
   - filled_size: Amount filled
   - fills: List of fill events
   - submitted_at: Submission timestamp
   - updated_at: Last status update

   StateManager methods:
   - save_clob_order(state: ClobOrderState) → Persist order
   - get_clob_order(order_id: str) → Retrieve by ID
   - get_open_orders(market_id: str | None) → Query open orders
   - update_clob_order_status(order_id, status, fills) → Update

7. RECEIPT/RESULT TRACKING
   ------------------------
   ClobExecutionResult mirrors TransactionResult semantics:
   - success: Whether order was accepted
   - order_id: API-assigned order ID
   - status: Current order status
   - fills: List of fill events (partial/full)
   - error: Error message if failed

   This enables uniform result handling in StrategyRunner.

8. FILL NOTIFICATIONS
   -------------------
   The handler supports fill tracking via:
   - Polling: get_status(order_id) queries current state
   - WebSocket: Future enhancement for real-time updates

   Fills are stored in ClobOrderState.fills as:
   ```
   Fill(
       fill_id: str,
       price: Decimal,
       size: Decimal,
       fee: Decimal,
       timestamp: datetime,
   )
   ```

9. CANCELLATION
   -------------
   ClobActionHandler.cancel(order_id) → bool
   - Submits cancel request to CLOB API
   - Updates ClobOrderState.status to CANCELLED
   - Returns True if cancellation accepted

10. ERROR HANDLING
    ---------------
    CLOB-specific errors map to recoverable/non-recoverable:
    - Rate limit → Recoverable (retry with backoff)
    - Invalid signature → Non-recoverable (order needs rebuild)
    - Insufficient balance → Non-recoverable (need funds)
    - Market closed → Non-recoverable (market ended)

Example Usage:
    from almanak.framework.execution.clob_handler import ClobActionHandler
    from almanak.framework.connectors.polymarket import ClobClient, PolymarketConfig

    config = PolymarketConfig.from_env()
    clob_client = ClobClient(config)
    handler = ClobActionHandler(clob_client)

    # Check if bundle is a CLOB order
    if handler.can_handle(bundle):
        result = await handler.execute(bundle)
        if result.success:
            print(f"Order submitted: {result.order_id}")
        else:
            print(f"Order failed: {result.error}")

    # Track order status
    status = await handler.get_status(result.order_id)
    print(f"Order status: {status.status}, filled: {status.filled_size}")

    # Cancel order if needed
    cancelled = await handler.cancel(result.order_id)
"""

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from almanak.framework.connectors.polymarket import ClobClient
    from almanak.framework.execution.extracted_data import PredictionFill
    from almanak.framework.models.reproduction_bundle import ActionBundle

logger = logging.getLogger(__name__)


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
        }

    def to_prediction_fill(self) -> "PredictionFill | None":
        """Project this result onto a :class:`PredictionFill` for strategies.

        Returns ``None`` when ``requested_size`` is unknown (e.g. a "SELL all"
        intent where the requested size is the current position balance,
        derived at compile time but not persisted on the bundle). In that
        case strategies should read post-execution wallet balances instead.
        """
        from almanak.framework.execution.extracted_data import PredictionFill

        if self.requested_size is None:
            return None
        return PredictionFill(
            filled_shares=self.filled_size,
            requested_shares=self.requested_size,
            avg_fill_price=self.avg_fill_price,
            order_id=self.order_id,
            status=self.status.value,
        )


# =============================================================================
# CLOB Action Handler
# =============================================================================


class ClobActionHandler:
    """Handler for executing CLOB orders from ActionBundles.

    This handler integrates with PlanExecutor to route Polymarket orders
    to the CLOB API instead of the on-chain execution path.

    The handler implements the ExecutionHandler protocol for use with
    ExecutionHandlerRegistry. See handler_registry.py for details.

    The handler manages:
    - Order detection (can_handle)
    - Order submission (execute)
    - Status tracking (get_status)
    - Order cancellation (cancel)

    Thread Safety:
        The handler is NOT thread-safe. Use separate instances for concurrent
        execution or protect access with locks.

    Example:
        handler = ClobActionHandler(clob_client)

        if handler.can_handle(bundle):
            result = await handler.execute(bundle)
            print(f"Order {result.order_id}: {result.status}")

    Registry Usage:
        registry = ExecutionHandlerRegistry()
        registry.register(ClobActionHandler(clob_client))
    """

    def __init__(
        self,
        clob_client: "ClobClient | None" = None,
    ) -> None:
        """Initialize the CLOB action handler.

        Args:
            clob_client: CLOB API client for order operations.
                        If None, handler will return errors on execute.
        """
        self._clob = clob_client
        logger.info("ClobActionHandler initialized", extra={"has_client": clob_client is not None})

    @property
    def supported_protocols(self) -> list[str]:
        """List of protocol names this handler supports.

        Returns:
            List containing "polymarket" protocol
        """
        return ["polymarket"]

    def can_handle(self, bundle: "ActionBundle") -> bool:
        """Check if this handler can execute the given bundle.

        Detection criteria:
        1. metadata["protocol"] == "polymarket"
        2. transactions list is empty (CLOB orders are off-chain)
        3. metadata["order_payload"] or metadata["order_request"] exists

        This method implements the ExecutionHandler protocol interface.

        Args:
            bundle: ActionBundle to check

        Returns:
            True if this is a CLOB order that can be handled
        """
        # Check protocol matches
        if bundle.metadata.get("protocol") != "polymarket":
            return False

        # CLOB orders have no on-chain transactions
        if bundle.transactions:
            return False

        # Must have order payload for CLOB submission
        if "order_payload" not in bundle.metadata and "order_request" not in bundle.metadata:
            return False

        return True

    async def execute(self, bundle: "ActionBundle") -> ClobExecutionResult:
        """Execute a CLOB order from an ActionBundle.

        The order request is extracted from bundle metadata and submitted
        through the configured Polymarket client.

        Args:
            bundle: ActionBundle containing the order payload

        Returns:
            ClobExecutionResult with order_id, status, and any fills

        Raises:
            ValueError: If bundle cannot be handled or is malformed
        """
        if not self.can_handle(bundle):
            return ClobExecutionResult(
                success=False,
                error="Bundle is not a CLOB order",
            )

        if self._clob is None:
            return ClobExecutionResult(
                success=False,
                error="CLOB client not configured",
            )

        order_payload = bundle.metadata.get("order_payload")
        order_request = bundle.metadata.get("order_request", {})
        intent_id = bundle.metadata.get("intent_id")
        requested_size = _parse_decimal(bundle.metadata.get("size") or order_request.get("size"))
        order_type_hint = str(
            bundle.metadata.get("order_type")
            or order_request.get("order_type")
            or order_request.get("time_in_force", "")
        ).upper()

        try:
            # Submit order to CLOB API
            logger.info(
                "Submitting CLOB order",
                extra={
                    "intent_id": intent_id,
                    "side": bundle.metadata.get("side") or order_request.get("side"),
                    "size": bundle.metadata.get("size") or order_request.get("size"),
                    "price": bundle.metadata.get("price") or order_request.get("price"),
                    "order_type": order_type_hint or None,
                },
            )

            if order_payload is not None:
                order_response = self._clob.submit_order_payload(order_payload)
            else:
                req_price = _parse_decimal(order_request.get("price"))
                req_size = _parse_decimal(order_request.get("size"))
                if req_price is None or req_size is None:
                    raise ValueError(
                        f"order_request is missing required price or size fields: "
                        f"price={order_request.get('price')!r}, size={order_request.get('size')!r}"
                    )
                order_response = self._clob.create_and_post_order(
                    token_id=str(order_request.get("token_id", "")),
                    price=req_price,
                    size=req_size,
                    side=str(order_request.get("side", "")),
                    time_in_force=str(order_request.get("time_in_force", bundle.metadata.get("order_type", "GTC"))),
                    expiration=int(order_request.get("expiration", 0) or 0),
                    fee_rate_bps=str(order_request.get("fee_rate_bps", "0")),
                )

            # VIB-3218: propagate filled_size / avg_fill_price so the runner
            # can build a PredictionFill on the ExecutionResult and strategies
            # don't end up persisting requested-not-filled amounts.
            #
            # CodeRabbit #1611 round 2 (Major): coerce the response numerics
            # through _parse_decimal. The attributes are typed ``Decimal`` on
            # ``OrderResponse`` but may arrive as strings / None from test
            # doubles or a connector that skips validation; a raw comparison
            # in ``_classify_status`` would otherwise raise TypeError and
            # route to the exception path with ``success=False``.
            order_id = order_response.order_id
            filled_size = _parse_decimal(getattr(order_response, "filled_size", None)) or Decimal("0")
            avg_fill_price = _parse_decimal(getattr(order_response, "avg_fill_price", None))
            status = self._classify_status(
                api_status=order_response.status.value,
                filled_size=filled_size,
                requested_size=requested_size,
                order_type_hint=order_type_hint,
            )

            # VIB-3218: ``success`` is what the runner uses to decide whether
            # to call ``on_intent_executed(success=True, ...)``, write a
            # positive ledger entry, and emit a "transaction confirmed"
            # timeline event. An order the classifier demoted to FAILED
            # (REJECTED API status, IOC/FOK that didn't match, etc.) must NOT
            # flow through the happy path -- treat it as a failed execution.
            success = status != ClobOrderStatus.FAILED
            error = None if success else f"CLOB order rejected (status={status.value})"

            logger.info(
                "CLOB order submitted",
                extra={
                    "order_id": order_id,
                    "status": status.value,
                    "filled_size": str(filled_size),
                    "avg_fill_price": str(avg_fill_price) if avg_fill_price is not None else None,
                    "intent_id": intent_id,
                    "success": success,
                },
            )

            return ClobExecutionResult(
                success=success,
                order_id=order_id,
                status=status,
                filled_size=filled_size,
                avg_fill_price=avg_fill_price,
                requested_size=requested_size,
                error=error,
                submitted_at=datetime.now(UTC),
            )

        except Exception as e:
            logger.exception("Failed to submit CLOB order", extra={"intent_id": intent_id})
            return ClobExecutionResult(
                success=False,
                status=ClobOrderStatus.FAILED,
                requested_size=requested_size,
                error=str(e),
            )

    async def get_status(self, order_id: str) -> ClobOrderState | None:
        """Get current status of a CLOB order.

        Queries the CLOB API for the latest order state including fills.

        Args:
            order_id: Order identifier

        Returns:
            ClobOrderState if found, None if not found or error
        """
        if self._clob is None:
            logger.warning("Cannot get order status: CLOB client not configured")
            return None

        try:
            # get_order returns an OpenOrder object or None
            open_order = self._clob.get_order(order_id)

            if open_order is None:
                return None

            # Map OpenOrder to ClobOrderState
            # OpenOrder has: order_id, market, side, price, size, filled_size, created_at
            return ClobOrderState(
                order_id=open_order.order_id,
                market_id=open_order.market,
                token_id=open_order.market,  # market is the token_id
                side=open_order.side,
                status=self._determine_order_status(open_order),
                price=open_order.price,
                size=open_order.size,
                filled_size=open_order.filled_size,
                order_type="GTC",  # Assume GTC, actual type not in OpenOrder
                submitted_at=open_order.created_at or datetime.now(UTC),
                updated_at=datetime.now(UTC),
            )

        except Exception:
            logger.exception("Failed to get order status", extra={"order_id": order_id})
            return None

    def _determine_order_status(self, order: Any) -> ClobOrderStatus:
        """Determine order status based on fill state.

        Args:
            order: OpenOrder with size and filled_size

        Returns:
            Appropriate ClobOrderStatus
        """
        if order.filled_size >= order.size and order.size > 0:
            return ClobOrderStatus.MATCHED
        elif order.filled_size > 0:
            return ClobOrderStatus.PARTIALLY_FILLED
        else:
            return ClobOrderStatus.LIVE

    async def cancel(self, order_id: str) -> bool:
        """Cancel a CLOB order.

        Args:
            order_id: Order identifier to cancel

        Returns:
            True if cancellation was accepted, False otherwise
        """
        if self._clob is None:
            logger.warning("Cannot cancel order: CLOB client not configured")
            return False

        try:
            # cancel_order returns True if cancelled, raises exception otherwise
            success = self._clob.cancel_order(order_id)

            if success:
                logger.info("CLOB order cancelled", extra={"order_id": order_id})
            else:
                logger.warning(
                    "CLOB order cancellation failed",
                    extra={"order_id": order_id},
                )

            return success

        except Exception:
            logger.exception("Failed to cancel order", extra={"order_id": order_id})
            return False

    def _map_api_status(self, api_status: str) -> ClobOrderStatus:
        """Map CLOB API status to ClobOrderStatus enum.

        Args:
            api_status: Status string from CLOB API

        Returns:
            Corresponding ClobOrderStatus
        """
        status_map = {
            "LIVE": ClobOrderStatus.LIVE,
            "OPEN": ClobOrderStatus.LIVE,
            "MATCHED": ClobOrderStatus.MATCHED,
            "FILLED": ClobOrderStatus.MATCHED,
            "CANCELLED": ClobOrderStatus.CANCELLED,
            "CANCELED": ClobOrderStatus.CANCELLED,
            "EXPIRED": ClobOrderStatus.EXPIRED,
            "FAILED": ClobOrderStatus.FAILED,
            "REJECTED": ClobOrderStatus.FAILED,
            "PENDING": ClobOrderStatus.PENDING,
            # VIB-3218: Polymarket emits these in POST /order responses.
            "UNMATCHED": ClobOrderStatus.FAILED,
            "DELAYED": ClobOrderStatus.PENDING,
        }
        return status_map.get(api_status.upper(), ClobOrderStatus.PENDING)

    def _classify_status(
        self,
        api_status: str,
        filled_size: Decimal,
        requested_size: Decimal | None,
        order_type_hint: str,
    ) -> ClobOrderStatus:
        """Classify the true CLOB lifecycle state from the response (VIB-3218).

        The raw API status alone is insufficient -- a "live" response plus a
        non-zero ``filledSize`` is a partial fill, and a "live" response on
        an IOC order with ``filledSize == 0`` is effectively "unmatched".
        This helper combines both signals so downstream sees a status that
        faithfully reflects fill state.
        """
        base = self._map_api_status(api_status)

        # Any response with non-zero fills is at least partially filled.
        # If the API already reports a terminal MATCHED (Polymarket's
        # "matched" status), preserve it even when ``requested_size`` is
        # unknown -- downgrading a completed order to PARTIALLY_FILLED would
        # leave downstream treating it as still open. Noted by CodeRabbit.
        if filled_size > 0:
            if base == ClobOrderStatus.MATCHED:
                return ClobOrderStatus.MATCHED
            if requested_size is not None and filled_size >= requested_size:
                return ClobOrderStatus.MATCHED
            # CodeRabbit #1611 round 1 (Major): IOC / FOK with a partial fill
            # is TERMINAL -- the matcher never fills more. PARTIALLY_FILLED
            # is treated as open by ``ClobOrderState.is_open``, which would
            # keep the order in the live-order set even though no additional
            # fills will ever arrive. Classify IOC/FOK partials as MATCHED
            # (terminal) so reconciliation doesn't chase a ghost order.
            # ``filled_size`` remains on the result for partial-fill detection
            # at the strategy level.
            if order_type_hint in ("IOC", "FOK"):
                return ClobOrderStatus.MATCHED
            return ClobOrderStatus.PARTIALLY_FILLED

        # IOC / FOK never rest on the book. A "live" or "pending" response
        # with zero fills means no liquidity matched -- treat as FAILED so
        # strategies don't mark a position open on a no-fill acknowledgement.
        if (
            filled_size == 0
            and order_type_hint in ("IOC", "FOK")
            and base
            in (
                ClobOrderStatus.LIVE,
                ClobOrderStatus.PENDING,
            )
        ):
            return ClobOrderStatus.FAILED

        return base


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
    # Handler
    "ClobActionHandler",
]
