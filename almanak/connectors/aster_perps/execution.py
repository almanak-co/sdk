"""Aster Pro execution handler: submits compiled order requests via the gateway.

Plugs into the runner's off-chain order lane (the same seam Polymarket's CLOB
handler uses): bundles with no transactions and an ``order_request`` are routed
here instead of the on-chain orchestrator. The handler signs nothing.

A submission whose outcome is unknown (the RPC failed, or the venue did not
answer) is reported as ``outcome_unknown`` — never as a plain failure — and
:meth:`AsterOrderHandler.reconcile` later resolves it from the venue by the
order's deterministic client id, or a withdrawal by the venue's history.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any

import grpc

from almanak.connectors._strategy_base.prediction_execute_base import PredictionExecuteSpec
from almanak.connectors.aster_perps.compiler import PROTOCOL, SUPPORTED_CHAINS
from almanak.connectors.aster_perps.gateway_client import GatewayAsterPerpsClient
from almanak.framework.execution.clob_handler import ClobExecutionResult, ClobOrderStatus

if TYPE_CHECKING:
    from almanak.framework.models.reproduction_bundle import ActionBundle

logger = logging.getLogger(__name__)

# Key under which the venue fill is published into ``extracted_data``; the
# connector runner hook reads it to build the perp accounting payload.
ASTER_ORDER_KEY = "aster_order"
ASTER_WITHDRAW_KEY = "aster_withdraw"
# How old a submission must be before the venue not knowing its client id proves it never traded.
_NOT_FOUND_PROOF_AGE = timedelta(seconds=30)


def _decimal(value: str) -> Decimal | None:
    if not value:
        return None
    try:
        return Decimal(value)
    except (InvalidOperation, ValueError):
        return None


class AsterOrderHandler:
    """Executes Aster Pro order bundles through the gateway."""

    def __init__(self, client: GatewayAsterPerpsClient, *, wallet_address: str) -> None:
        self._client = client
        self._wallet = wallet_address

    @property
    def supported_protocols(self) -> list[str]:
        return [PROTOCOL]

    def can_handle(self, bundle: ActionBundle) -> bool:
        return (
            bundle.metadata.get("protocol") == PROTOCOL
            and not bundle.transactions
            and (
                isinstance(bundle.metadata.get("order_request"), dict)
                or isinstance(bundle.metadata.get("withdraw_request"), dict)
            )
        )

    async def execute(self, bundle: ActionBundle) -> ClobExecutionResult:
        if not self.can_handle(bundle):
            return ClobExecutionResult(success=False, status=ClobOrderStatus.FAILED, error="Not an Aster order bundle")
        if isinstance(bundle.metadata.get("withdraw_request"), dict):
            return await self._withdraw(bundle.metadata["withdraw_request"])
        order_request = bundle.metadata["order_request"]
        try:
            response = await asyncio.to_thread(
                self._client.place_market_order, order_request, wallet_address=self._wallet
            )
        except grpc.RpcError as exc:
            # The gateway may have submitted before the RPC failed.
            return _unknown(f"Aster PlaceMarketOrder RPC failed: {exc.details()}")
        return self._to_result(response, order_request)

    async def reconcile(self, metadata: dict[str, Any], *, since: datetime) -> ClobExecutionResult | None:
        """Resolve a submission whose outcome was unknown, or ``None`` while it still is.

        ``since`` is when the submission was dispatched; a withdrawal is matched
        to the one the venue recorded after it.
        """
        withdraw_request = metadata.get("withdraw_request")
        if isinstance(withdraw_request, dict):
            return await self._reconcile_withdrawal(withdraw_request, since)
        order_request = metadata.get("order_request")
        if not isinstance(order_request, dict):
            return None
        try:
            response = await asyncio.to_thread(
                self._client.get_order,
                symbol=str(order_request["symbol"]),
                client_order_id=str(order_request["client_order_id"]),
                close_position=bool(order_request.get("close_position")),
                wallet_address=self._wallet,
            )
        except grpc.RpcError as exc:
            logger.info("Aster order %s still unresolved: %s", order_request.get("client_order_id"), exc.details())
            return None
        if response.outcome_unknown:
            return None
        if not response.success and not response.order_not_found and not response.status:
            # The gateway could not read the venue (credentials, wallet): no answer about the order.
            logger.info("Aster order %s still unresolved: %s", order_request.get("client_order_id"), response.error)
            return None
        if response.order_not_found:
            result = ClobExecutionResult(
                success=False,
                status=ClobOrderStatus.FAILED,
                error=f"order was never placed: {response.error}",
                venue_answered=True,
            )
        else:
            result = self._to_result(response, order_request)
        # Any order (or close leg) sent moments ago may still be queued at the
        # venue: a verdict short of success is not final until it has had time.
        if not result.success and datetime.now(UTC) - since < _NOT_FOUND_PROOF_AGE:
            return None
        return result

    async def _reconcile_withdrawal(self, request: dict[str, Any], since: datetime) -> ClobExecutionResult | None:
        try:
            response = await asyncio.to_thread(
                self._client.find_withdrawal,
                since_ms=int(since.timestamp() * 1000),
                wallet_address=self._wallet,
                client_request_id=str(request.get("client_request_id") or ""),
            )
        except grpc.RpcError as exc:
            logger.info("Aster withdrawal still unresolved: %s", exc.details())
            return None
        if not response.success:
            logger.info("Aster withdrawal still unresolved: %s", response.error)
            return None
        if response.not_found:
            return ClobExecutionResult(
                success=False,
                status=ClobOrderStatus.FAILED,
                error="withdrawal was never accepted by the venue",
                venue_answered=True,
            )
        if not response.found:
            return None
        return self._withdrawal_result(request, response.withdraw_id, response.amount, response.fee, self._wallet)

    async def _withdraw(self, request: dict[str, Any]) -> ClobExecutionResult:
        try:
            response = await asyncio.to_thread(
                self._client.withdraw,
                asset=str(request["asset"]),
                amount=str(request["amount"]),
                wallet_address=self._wallet,
                client_request_id=str(request.get("client_request_id") or ""),
            )
        except grpc.RpcError as exc:
            return _unknown(f"Aster Withdraw RPC failed: {exc.details()}")
        if response.outcome_unknown:
            return _unknown(response.error)
        if not response.success:
            return ClobExecutionResult(
                success=False, status=ClobOrderStatus.FAILED, error=response.error, venue_answered=True
            )
        return self._withdrawal_result(request, response.withdraw_id, response.amount, response.fee, response.receiver)

    @staticmethod
    def _withdrawal_result(
        request: dict[str, Any], withdraw_id: str, amount: str, fee: str, receiver: str
    ) -> ClobExecutionResult:
        logger.info("Aster withdrawal %s: %s %s (fee %s) to %s", withdraw_id, amount, request["asset"], fee, receiver)
        return ClobExecutionResult(
            success=True,
            order_id=withdraw_id,
            status=ClobOrderStatus.MATCHED,
            venue_data={
                ASTER_WITHDRAW_KEY: {
                    "withdraw_id": withdraw_id,
                    "asset": request["asset"],
                    "amount": amount,
                    "fee": fee,
                    "receiver": receiver,
                }
            },
        )

    @staticmethod
    def _to_result(response: Any, order_request: dict[str, Any]) -> ClobExecutionResult:
        if response.outcome_unknown:
            return _unknown(response.error)
        executed = _decimal(response.executed_qty) or Decimal(0)
        venue = {
            "symbol": order_request["symbol"],
            "is_long": bool(order_request["is_long"]),
            "reduce_only": bool(order_request.get("close_position")),
            "leverage_requested": order_request.get("leverage") or None,
            "order_id": str(response.order_id),
            "client_order_id": response.client_order_id,
            "status": response.status,
            "side": response.side,
            "executed_qty": response.executed_qty,
            "requested_qty": response.requested_qty,
            "avg_price": response.avg_price,
            "cum_quote": response.cum_quote,
            "fee": response.fee,
            "fee_asset": response.fee_asset,
            "realized_pnl": response.realized_pnl,
        }
        if not response.success:
            logger.warning("Aster order %s not filled: %s", order_request.get("client_order_id"), response.error)
            return ClobExecutionResult(
                success=False,
                order_id=str(response.order_id) if response.order_id else None,
                status=ClobOrderStatus.FAILED,
                filled_size=executed,
                error=response.error or f"order status {response.status}",
                venue_answered=True,
                venue_data={ASTER_ORDER_KEY: venue} if executed > 0 else {},
            )
        requested = _decimal(response.requested_qty)
        complete = requested is not None and executed >= requested
        logger.info(
            "Aster order %s %s %s qty=%s avg=%s fee=%s %s",
            response.client_order_id,
            response.status,
            response.side,
            response.executed_qty,
            response.avg_price,
            response.fee or "unread",
            response.fee_asset,
        )
        return ClobExecutionResult(
            success=True,
            order_id=str(response.order_id),
            status=ClobOrderStatus.MATCHED if complete else ClobOrderStatus.PARTIALLY_FILLED,
            filled_size=executed,
            avg_fill_price=_decimal(response.avg_price),
            requested_size=requested,
            venue_data={ASTER_ORDER_KEY: venue},
        )


def _unknown(error: str) -> ClobExecutionResult:
    return ClobExecutionResult(success=False, status=ClobOrderStatus.SUBMITTED, error=error, outcome_unknown=True)


def _build_handler(*, gateway_client: Any, wallet: str | None = None) -> AsterOrderHandler:
    return AsterOrderHandler(GatewayAsterPerpsClient(gateway_client), wallet_address=wallet or "")


EXECUTE_SPEC = PredictionExecuteSpec(build_handler=_build_handler, chains=SUPPORTED_CHAINS)

__all__ = ["ASTER_ORDER_KEY", "ASTER_WITHDRAW_KEY", "EXECUTE_SPEC", "AsterOrderHandler"]
