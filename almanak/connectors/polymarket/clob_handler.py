"""Polymarket CLOB execution handler (VIB-4989).

Relocated verbatim from ``almanak/framework/execution/clob_handler.py`` (the
``ClobActionHandler`` class) so the Polymarket CLOB order-execution implementation
lives in the connector folder. The venue-neutral result/state dataclasses
(``ClobExecutionResult`` / ``ClobOrderState`` / ``ClobFill`` / ``ClobOrderStatus``)
and the ``_parse_decimal`` helper stay in the framework as the persisted-format
contract and are imported below (connector→framework is allowed). The handler
implements the framework ``ExecutionHandler`` Protocol; the runner builds it via
:class:`~almanak.connectors._strategy_base.prediction_execute_registry.PredictionExecuteRegistry`
(VIB-4989, epic VIB-4851).

The handler signs nothing locally — order submission routes through the
gateway-held ``PolymarketService``. Behaviour is byte-identical to the framework
original, pinned method-by-method by
``tests/unit/connectors/polymarket/test_prediction_relocation_vib4989.py`` until
the framework original is removed (PR B).
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from almanak.connectors._strategy_base.prediction_execute_base import PredictionExecuteSpec
from almanak.framework.execution.clob_handler import (
    ClobExecutionResult,
    ClobOrderState,
    ClobOrderStatus,
    _parse_decimal,
)

if TYPE_CHECKING:
    from almanak.connectors.polymarket import ClobClient
    from almanak.framework.models.reproduction_bundle import ActionBundle

logger = logging.getLogger(__name__)


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
        3. metadata["order_request"] exists

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

        # Must have an order_request for the gateway to sign + submit.
        # The legacy V1 ``order_payload`` (pre-signed in the strategy
        # container) is gone -- under V2 the gateway holds the keys and
        # signs server-side.
        if "order_request" not in bundle.metadata:
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

            req_price = _parse_decimal(order_request.get("price"))
            req_size = _parse_decimal(order_request.get("size"))
            if req_price is None or req_size is None:
                raise ValueError(
                    f"order_request is missing required price or size fields: "
                    f"price={order_request.get('price')!r}, size={order_request.get('size')!r}"
                )
            # V2: ``create_and_post_order`` requires a GammaMarket so it can
            # route neg-risk vs binary CTF V2 (the verifyingContract differs)
            # and validate tick / min-size. Look it up from the token_id —
            # the gateway holds the keys and signs server-side, the framework
            # only assembles the request.
            from almanak.connectors.polymarket import MarketFilters

            token_id = str(order_request.get("token_id", ""))
            markets = self._clob.get_markets(MarketFilters(clob_token_ids=[token_id], limit=1))
            if not markets:
                raise ValueError(f"No Polymarket market found for token_id={token_id}")
            # V2: ``fee_rate_bps`` and on-chain ``nonce`` are gone — fees
            # are operator-set at match time, ``timestamp`` replaces nonce
            # for per-address uniqueness. ``expiration`` is API-level GTD.
            order_response = self._clob.create_and_post_order(
                token_id=token_id,
                price=req_price,
                size=req_size,
                side=str(order_request.get("side", "")),
                market=markets[0],
                time_in_force=(order_type_hint or "GTC"),
                expiration=int(order_request.get("expiration", 0) or 0),
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

            # VIB-3710: capture gateway-side setup_txs (approvals + wrap) and
            # operator fee_pusd. We store setup_txs as plain dicts here to
            # keep the result struct connector-agnostic; the typed conversion
            # to PredictionSetupTx happens in to_prediction_fill().
            setup_txs_raw: list[dict[str, Any]] = []
            for tx in getattr(order_response, "setup_txs", None) or []:
                setup_txs_raw.append(
                    {
                        "tx_hash": getattr(tx, "tx_hash", ""),
                        "description": getattr(tx, "description", ""),
                        "gas_used": int(getattr(tx, "gas_used", 0) or 0),
                        "gas_price_wei": str(getattr(tx, "gas_price_wei", "0")),
                        "total_cost_wei": str(getattr(tx, "total_cost_wei", "0")),
                    }
                )
            fee_pusd = _parse_decimal(getattr(order_response, "fee_pusd", None))

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
                setup_txs=setup_txs_raw,
                fee_pusd=fee_pusd,
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
# Connector-published execution spec (VIB-4989)
# =============================================================================


def _build_handler(*, gateway_client, wallet=None):
    # Mirror the legacy strategy_runner wiring: a gateway-routed CLOB client wrapped
    # by the handler. ``wallet`` is part of the registry's factory contract but the
    # handler holds no wallet (the gateway signs server-side).
    from almanak.connectors.polymarket.gateway_client import GatewayPolymarketClient

    return ClobActionHandler(clob_client=GatewayPolymarketClient(gateway_client))  # type: ignore[arg-type]


PREDICTION_EXECUTE_SPEC = PredictionExecuteSpec(build_handler=_build_handler, chains=frozenset({"polygon"}))
