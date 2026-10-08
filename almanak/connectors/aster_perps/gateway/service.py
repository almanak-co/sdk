"""gRPC servicer for Aster Pro (off-chain order book).

Holds the trading identity (the gateway EOA), signs every Aster request, and
enforces the order-safety rules the strategy container cannot be trusted with:
one-way position mode only, venue size rules applied before submission, every
order an immediate-or-cancel LIMIT bounded by the intent's ``max_slippage``,
full reduce-only closes, and reconciliation by ``client_order_id`` whenever the
outcome of a submission is unknown — an order is never re-sent on a guess.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

import grpc
from eth_account import Account
from eth_account.signers.local import LocalAccount
from web3.exceptions import TransactionNotFound

from almanak.connectors.aster_perps.gateway.api_client import (
    WITHDRAW_CHAIN_ID,
    AsterApiError,
    AsterProApiClient,
    AsterUnknownOutcomeError,
    format_price,
    format_quantity,
    protected_price,
    quantity_for_notional,
)
from almanak.connectors.aster_perps.proto import aster_perps_pb2, aster_perps_pb2_grpc
from almanak.gateway.utils.rpc_provider import get_cached_web3

logger = logging.getLogger(__name__)

# An IOC order is normally terminal in the submit response; poll briefly if the
# venue answers NEW so the strategy sees the outcome rather than a pending order.
_FILL_POLL_ATTEMPTS = 5
_FILL_POLL_INTERVAL_SECONDS = 0.5
_TERMINAL_STATUSES = frozenset({"FILLED", "EXPIRED", "CANCELED", "REJECTED"})
_MAX_LEVERAGE = 125
# A BSC payout mines within seconds of the venue marking a withdrawal SUCCESS;
# older SUCCESS records are settled, and a withdrawal whose outcome was unknown
# and that has not appeared in the venue history by then was never accepted.
_PAYOUT_WINDOW_MS = 15 * 60 * 1000
# Venue and gateway clocks may disagree slightly when matching history by time.
_CLOCK_SKEW_MS = 5_000
_SETTLED_STATES = frozenset({"SUCCESS", "FAILED", "CANCELED", "CANCELLED", "REJECTED"})
_EXECUTED_STATES = frozenset({"PROCESSING", "SUCCESS"})
_FAILED_STATES = frozenset({"FAILED", "CANCELED", "CANCELLED", "REJECTED"})
# A close continues with fresh reduce-only IOC legs until the position is flat:
# at most _CLOSE_LEGS new legs per call, at most _MAX_CLOSE_LEGS over all calls
# (a retry or a teardown slippage rung re-sends the same intent and continues).
_CLOSE_LEGS = 3
_MAX_CLOSE_LEGS = 30
# Aster's "Order does not exist": the venue holds no order under the client id.
# GET /order still finds every order with a fill, so this proves nothing traded.
_NO_SUCH_ORDER = -2013


def _decimal(value: Any) -> Decimal | None:
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _order_response(order: dict[str, Any], *, requested_qty: str = "") -> aster_perps_pb2.AsterOrderResponse:
    """Classify the venue's order record.

    A terminal order with a fill is a success for what filled (a partial fill is
    reported as such). A non-terminal order may still fill, so its outcome is
    unknown. Closes are reported per close, over all their legs (``_close``).
    """
    status = str(order.get("status", ""))
    requested = requested_qty or str(order.get("origQty", ""))
    executed = _decimal(order.get("executedQty")) or Decimal(0)
    error = ""
    unknown = False
    if status not in _TERMINAL_STATUSES:
        unknown = True
        error = f"order outcome unknown (status={status or 'missing'})"
    elif executed <= 0:
        error = f"order not filled within the slippage bound (status={status})"
    return aster_perps_pb2.AsterOrderResponse(
        success=not error,
        error=error,
        outcome_unknown=unknown,
        order_id=int(order.get("orderId", 0) or 0),
        client_order_id=str(order.get("clientOrderId", "")),
        status=status,
        side=str(order.get("side", "")),
        executed_qty=str(order.get("executedQty", "")),
        avg_price=str(order.get("avgPrice", "")),
        cum_quote=str(order.get("cumQuote", "")),
        requested_qty=requested,
    )


@dataclass(frozen=True)
class _PreparedOrder:
    side: str
    quantity: str
    price: str
    reduce_only: bool


def _leg_client_order_id(base: str, leg: int) -> str:
    """Client id of a close's ``leg``: the close's own id for the first, a derived one after.

    Aster caps client ids at 36 characters, so later legs replace the tail.
    """
    return base if leg == 0 else f"{base[:33]}.{leg:02d}"


def _already_flat(client_order_id: str) -> aster_perps_pb2.AsterOrderResponse:
    """A close asks for flat and the venue reads flat (closed elsewhere, liquidated,
    or an open that never filled): success with no fill, so a caller or teardown
    can move on instead of failing forever."""
    return aster_perps_pb2.AsterOrderResponse(
        success=True,
        already_flat=True,
        client_order_id=client_order_id,
        executed_qty="0",
        requested_qty="0",
        cum_quote="0",
    )


def _unknown_order(client_order_id: str, error: str, requested_qty: str = "") -> aster_perps_pb2.AsterOrderResponse:
    return aster_perps_pb2.AsterOrderResponse(
        success=False,
        outcome_unknown=True,
        error=error,
        client_order_id=client_order_id,
        requested_qty=requested_qty,
    )


class AsterPerpsServiceServicer(aster_perps_pb2_grpc.AsterPerpsServiceServicer):
    """Aster Pro proxy bound to the gateway's EOA."""

    def __init__(self, settings: Any) -> None:
        self._account: LocalAccount | None = None
        # Withdrawals this gateway submitted, by venue id -> (asset, amount, fee).
        # Aster debits the account about a second before its history shows the
        # withdrawal; until then only this record keeps the money in flight.
        self._submitted_withdrawals: dict[str, tuple[str, str, str]] = {}
        # When a withdrawal's submission outcome was unknown (epoch ms): the
        # account is unmeasured until the venue history shows it or the payout
        # window passes without it.
        self._unknown_withdrawal_since_ms: int | None = None
        self._unknown_withdrawal_fee = ""
        self._unknown_withdrawal_request_id = ""
        self._settled_payouts: set[str] = set()
        # Venue fee of each withdrawal this gateway submitted, by venue id. After a
        # withdraw-all the venue's withdraw-info lists no balance (and no fee) for
        # the asset, so the fee known at submission is the one to value it with.
        self._withdrawal_fees: dict[str, str] = {}
        # Venue ids of withdrawals whose outcome this gateway knows (accepted, or
        # adopted by an idempotent re-send): never attributed to another attempt.
        # Lookups for an unknown outcome do not add to it, so they stay repeatable.
        self._attributed_withdrawals: set[str] = set()
        # Request id of each withdrawal this gateway accepted -> (venue id, amount,
        # fee): reconciliation matches it exactly even when the caller never
        # received the response, and a re-send is refused unless it provably failed.
        self._accepted_requests: dict[str, tuple[str, str, str]] = {}
        self._unavailable_reason = ""
        safe_mode = getattr(settings, "safe_mode", None) in ("direct", "zodiac")
        private_key = getattr(settings, "private_key", None)
        if safe_mode:
            self._unavailable_reason = "Aster Pro requires an EOA trading identity; Safe wallets are not supported"
        elif not isinstance(private_key, str) or not private_key:
            self._unavailable_reason = "Aster Pro requires the gateway private key (ALMANAK_PRIVATE_KEY)"
        else:
            key = private_key if private_key.startswith("0x") else f"0x{private_key}"
            self._account = Account.from_key(key)
        base_url = getattr(settings, "aster_perps_base_url", None)
        self._client = (
            AsterProApiClient(
                self._account,
                **({"base_url": base_url} if base_url else {}),
                withdraw_ip_whitelist=getattr(settings, "aster_perps_withdraw_ip_whitelist", None),
            )
            if self._account is not None
            else None
        )

    async def close(self) -> None:
        if self._client is not None:
            await self._client.close()

    def _client_for(self, wallet_address: str, *, required: bool = True) -> tuple[AsterProApiClient | None, str]:
        """The venue client for ``wallet_address``; account-scoped calls must name this gateway's wallet."""
        if self._client is None:
            return None, self._unavailable_reason
        if required and not wallet_address:
            return None, "wallet_address is required"
        if wallet_address and wallet_address.lower() != self._client.user_address.lower():
            return None, (f"wallet {wallet_address} is not this gateway's Aster account ({self._client.user_address})")
        return self._client, ""

    async def GetMarket(
        self, request: aster_perps_pb2.AsterGetMarketRequest, _context: grpc.aio.ServicerContext
    ) -> aster_perps_pb2.AsterMarketResponse:
        client, error = self._client_for("", required=False)
        if client is None:
            return aster_perps_pb2.AsterMarketResponse(success=False, error=error)
        try:
            rules = await client.symbol_rules(request.symbol)
            mark = await client.mark_price(request.symbol)
        except (AsterApiError, AsterUnknownOutcomeError) as exc:
            return aster_perps_pb2.AsterMarketResponse(success=False, error=str(exc))
        return aster_perps_pb2.AsterMarketResponse(
            success=True,
            symbol=rules.symbol,
            status=rules.status,
            mark_price=str(mark),
            step_size=str(rules.step_size),
            min_qty=str(rules.min_qty),
            min_notional=str(rules.min_notional),
        )

    async def PlaceMarketOrder(
        self, request: aster_perps_pb2.AsterPlaceMarketOrderRequest, _context: grpc.aio.ServicerContext
    ) -> aster_perps_pb2.AsterOrderResponse:
        """Place a slippage-bounded IOC order, or return the order this client id already placed."""
        client, error = self._client_for(request.wallet_address)
        if client is None:
            return aster_perps_pb2.AsterOrderResponse(success=False, error=error)
        if not request.client_order_id:
            return aster_perps_pb2.AsterOrderResponse(success=False, error="client_order_id is required")
        max_slippage = _decimal(request.max_slippage)
        if max_slippage is None or not Decimal(0) < max_slippage < Decimal(1):
            return aster_perps_pb2.AsterOrderResponse(
                success=False, error=f"max_slippage must be a fraction in (0, 1); got {request.max_slippage!r}"
            )
        if request.close_position:
            return await self._close(client, request, max_slippage, place=True) or _unknown_order(
                request.client_order_id, "close produced no venue record"
            )
        # Aster only enforces client-id uniqueness among OPEN orders, so a retry
        # of a filled order would otherwise trade a second time. The lookup runs
        # before any other read: a failing read must never mask an earlier fill.
        try:
            existing = await self._reconcile(client, request.symbol, request.client_order_id)
        except AsterUnknownOutcomeError as exc:
            return _unknown_order(request.client_order_id, f"cannot tell whether this order was placed: {exc}")
        if existing is not None:
            logger.info("Aster order %s already exists; not resubmitting", request.client_order_id)
            return await self._finalize(client, request.symbol, existing)
        try:
            if await client.is_hedge_mode():
                return aster_perps_pb2.AsterOrderResponse(
                    success=False, error="Aster account is in hedge mode; only one-way mode is supported"
                )
            prepared = await self._prepare_open(client, request, max_slippage)
        except (AsterApiError, AsterUnknownOutcomeError, ValueError) as exc:
            return aster_perps_pb2.AsterOrderResponse(success=False, error=str(exc))
        if isinstance(prepared, aster_perps_pb2.AsterOrderResponse):
            return prepared
        return await self._submit(client, request, prepared)

    async def _held_amount(self, client: AsterProApiClient, symbol: str) -> Decimal:
        return sum(await self._position_amounts(client, symbol), Decimal(0))

    async def _is_flat(self, client: AsterProApiClient, symbol: str) -> bool:
        # Every row, not the net: hedge-mode legs that offset are not flat.
        return all(amount == 0 for amount in await self._position_amounts(client, symbol))

    async def _position_amounts(self, client: AsterProApiClient, symbol: str) -> list[Decimal]:
        positions = [p for p in await client.positions(symbol) if p.get("symbol") == symbol]
        amounts: list[Decimal] = []
        for position in positions:
            amount = _decimal(position.get("positionAmt"))
            # An unreadable amount is not a flat one: a close would report
            # already_flat over a live position, so it raises instead of counting as zero.
            if amount is None or not amount.is_finite():
                raise ValueError(f"Aster {symbol} position amount unreadable: {position.get('positionAmt')!r}")
            amounts.append(amount)
        return amounts

    async def _prepare_open(
        self, client: AsterProApiClient, request: aster_perps_pb2.AsterPlaceMarketOrderRequest, max_slippage: Decimal
    ) -> _PreparedOrder | aster_perps_pb2.AsterOrderResponse:
        notional = _decimal(request.notional_usd)
        if notional is None or notional <= 0:
            return aster_perps_pb2.AsterOrderResponse(success=False, error="notional_usd must be a positive decimal")
        if request.leverage > _MAX_LEVERAGE:
            return aster_perps_pb2.AsterOrderResponse(
                success=False, error=f"leverage {request.leverage} exceeds {_MAX_LEVERAGE}"
            )
        rules = await client.symbol_rules(request.symbol)
        if rules.status != "TRADING":
            return aster_perps_pb2.AsterOrderResponse(
                success=False, error=f"{request.symbol} is not trading (status={rules.status})"
            )
        # One-way mode nets an opposite order against the held position instead
        # of opening a new one, which the open's accounting would misbook.
        held = await self._held_amount(client, request.symbol)
        if held != 0 and (held > 0) != request.is_long:
            held_side = "long" if held > 0 else "short"
            return aster_perps_pb2.AsterOrderResponse(
                success=False,
                error=f"{request.symbol} already holds a {held_side}; close it before opening the opposite side",
            )
        if request.leverage:
            await client.set_leverage(request.symbol, int(request.leverage))
        mark = await client.mark_price(request.symbol)
        side = "BUY" if request.is_long else "SELL"
        quantity = quantity_for_notional(notional, mark, rules)
        price = protected_price(mark, side=side, max_slippage=max_slippage, rules=rules)
        return _PreparedOrder(side, format_quantity(quantity, rules), format_price(price, rules), False)

    async def _prepare_close(
        self, client: AsterProApiClient, request: aster_perps_pb2.AsterPlaceMarketOrderRequest, max_slippage: Decimal
    ) -> _PreparedOrder | aster_perps_pb2.AsterOrderResponse:
        amount = await self._held_amount(client, request.symbol)
        if amount == 0:
            return _already_flat(request.client_order_id)
        if (amount > 0) != request.is_long:
            held = "long" if amount > 0 else "short"
            wanted = "long" if request.is_long else "short"
            return aster_perps_pb2.AsterOrderResponse(
                success=False, error=f"close requested for a {wanted} but the {request.symbol} position is {held}"
            )
        rules = await client.symbol_rules(request.symbol)
        side = "SELL" if amount > 0 else "BUY"
        price = protected_price(
            await client.mark_price(request.symbol), side=side, max_slippage=max_slippage, rules=rules
        )
        return _PreparedOrder(side, format_quantity(abs(amount), rules), format_price(price, rules), True)

    async def _close(
        self,
        client: AsterProApiClient,
        request: aster_perps_pb2.AsterPlaceMarketOrderRequest | aster_perps_pb2.AsterGetOrderRequest,
        max_slippage: Decimal | None,
        *,
        place: bool,
    ) -> aster_perps_pb2.AsterOrderResponse | None:
        """Flatten the position with reduce-only IOC legs, reported as one close.

        Each leg has a client id derived from the close's. Every call first finds
        the legs already sent (so a retry or a reconcile never sends one twice),
        then sends up to ``_CLOSE_LEGS`` new ones while the position is open — so
        a re-sent intent at a wider slippage bound really trades again. With
        ``place`` false (reconcile) legs are only looked up; ``None`` means the
        venue holds no leg of this close.
        """
        base = request.client_order_id
        legs: list[dict[str, Any]] = []
        refusal: aster_perps_pb2.AsterOrderResponse | None = None
        sent = 0
        for leg in range(_MAX_CLOSE_LEGS):
            leg_id = _leg_client_order_id(base, leg)
            try:
                order = await self._reconcile(client, request.symbol, leg_id)
            except AsterUnknownOutcomeError as exc:
                return _unknown_order(base, f"cannot tell whether close leg {leg_id} was placed: {exc}")
            if order is None:
                if not place or sent >= _CLOSE_LEGS:
                    break
                sent += 1
                assert max_slippage is not None
                placed = await self._place_close_leg(client, request, leg_id, max_slippage, first=not legs)
                if isinstance(placed, aster_perps_pb2.AsterOrderResponse):
                    if placed.outcome_unknown or not legs:
                        return placed
                    refusal = placed
                    break
                if placed is None:
                    break
                order = placed
            try:
                order = await self._await_terminal(client, request.symbol, order)
            except AsterUnknownOutcomeError as exc:
                return _unknown_order(base, f"close leg {leg_id} status unreadable: {exc}")
            if str(order.get("status", "")) not in _TERMINAL_STATUSES:
                return _unknown_order(base, f"close leg {leg_id} outcome unknown (status={order.get('status')})")
            legs.append(order)
        if not legs:
            return None
        try:
            remaining = await self._held_amount(client, request.symbol)
        except (AsterApiError, AsterUnknownOutcomeError, ValueError) as exc:
            return _unknown_order(base, f"position after close unreadable: {exc}")
        return await self._close_response(client, request.symbol, base, legs, remaining, refusal)

    async def _place_close_leg(
        self,
        client: AsterProApiClient,
        request: Any,
        leg_id: str,
        max_slippage: Decimal,
        *,
        first: bool,
    ) -> dict[str, Any] | aster_perps_pb2.AsterOrderResponse | None:
        """Send one reduce-only IOC leg; ``None`` when the position is already flat."""
        try:
            if first and await client.is_hedge_mode():
                return aster_perps_pb2.AsterOrderResponse(
                    success=False, error="Aster account is in hedge mode; only one-way mode is supported"
                )
            if not first and await self._held_amount(client, request.symbol) == 0:
                return None
            prepared = await self._prepare_close(client, request, max_slippage)
        except (AsterApiError, AsterUnknownOutcomeError, ValueError) as exc:
            return aster_perps_pb2.AsterOrderResponse(success=False, error=str(exc))
        if isinstance(prepared, aster_perps_pb2.AsterOrderResponse):
            return prepared
        try:
            return await client.place_ioc_order(
                symbol=request.symbol,
                side=prepared.side,
                quantity=prepared.quantity,
                price=prepared.price,
                reduce_only=True,
                client_order_id=leg_id,
            )
        except AsterApiError as exc:
            return aster_perps_pb2.AsterOrderResponse(success=False, error=str(exc), requested_qty=prepared.quantity)
        except AsterUnknownOutcomeError as exc:
            reason = str(exc)
        # Straight after a timed-out submit the leg may still be queued at the venue:
        # a lookup that finds nothing proves nothing yet.
        try:
            order = await self._reconcile(client, request.symbol, leg_id)
        except AsterUnknownOutcomeError as exc:
            order, reason = None, f"{reason}; lookup failed: {exc}"
        if order is None:
            return _unknown_order(request.client_order_id, f"close leg {leg_id} outcome unknown: {reason}")
        return order

    async def _close_response(
        self,
        client: AsterProApiClient,
        symbol: str,
        base: str,
        legs: list[dict[str, Any]],
        remaining: Decimal,
        refusal: aster_perps_pb2.AsterOrderResponse | None,
    ) -> aster_perps_pb2.AsterOrderResponse:
        executed = sum((_decimal(o.get("executedQty")) or Decimal(0) for o in legs), Decimal(0))
        quote = sum((_decimal(o.get("cumQuote")) or Decimal(0) for o in legs), Decimal(0))
        requested = str(legs[0].get("origQty", ""))
        # A close succeeds only when the position is flat, so teardown escalates
        # and nothing withdraws margin from under a residual. A partial close is a
        # definitive failure that still carries its aggregated fill, which the
        # runner books once if the intent finally fails (or with the success that
        # a later attempt of the same intent reaches, which re-aggregates every leg).
        if remaining == 0 and executed > 0:
            error = ""
        elif executed <= 0:
            error = f"order not filled within the slippage bound (status={legs[0].get('status')})"
        else:
            error = f"position only partly closed: {executed} closed, {abs(remaining)} still open"
            if refusal is not None:
                error = f"{error}; next leg refused: {refusal.error}"
        response = aster_perps_pb2.AsterOrderResponse(
            success=not error,
            error=error,
            order_id=int(legs[0].get("orderId", 0) or 0),
            client_order_id=base,
            status=str(legs[-1].get("status", "")) if not error else str(legs[0].get("status", "")),
            side=str(legs[0].get("side", "")),
            executed_qty=str(executed),
            avg_price=str(quote / executed) if executed > 0 else "",
            cum_quote=str(quote),
            requested_qty=requested,
        )
        if executed > 0:
            await self._attach_fill_economics_for(client, symbol, response, legs)
        return response

    async def _submit(
        self,
        client: AsterProApiClient,
        request: aster_perps_pb2.AsterPlaceMarketOrderRequest,
        prepared: _PreparedOrder,
    ) -> aster_perps_pb2.AsterOrderResponse:
        try:
            order = await client.place_ioc_order(
                symbol=request.symbol,
                side=prepared.side,
                quantity=prepared.quantity,
                price=prepared.price,
                reduce_only=prepared.reduce_only,
                client_order_id=request.client_order_id,
            )
        except AsterApiError as exc:
            return aster_perps_pb2.AsterOrderResponse(success=False, error=str(exc), requested_qty=prepared.quantity)
        except AsterUnknownOutcomeError as exc:
            logger.warning("Aster order %s outcome unknown, reconciling: %s", request.client_order_id, exc)
            try:
                reconciled = await self._reconcile(client, request.symbol, request.client_order_id)
            except AsterUnknownOutcomeError as lookup_exc:
                return _unknown_order(
                    request.client_order_id,
                    f"order outcome unknown ({exc}) and its lookup failed: {lookup_exc}",
                    prepared.quantity,
                )
            if reconciled is None:
                # Straight after a timed-out submit the order may still be queued
                # at the venue; only a later reconcile may conclude it never traded.
                return _unknown_order(
                    request.client_order_id,
                    f"order outcome unknown ({exc}); not yet recorded by the venue",
                    prepared.quantity,
                )
            order = reconciled
        return await self._finalize(client, request.symbol, order, requested_qty=prepared.quantity)

    async def _finalize(
        self,
        client: AsterProApiClient,
        symbol: str,
        order: dict[str, Any],
        *,
        requested_qty: str = "",
    ) -> aster_perps_pb2.AsterOrderResponse:
        try:
            order = await self._await_terminal(client, symbol, order)
        except AsterUnknownOutcomeError as exc:
            logger.warning("Aster order %s status unreadable: %s", order.get("clientOrderId"), exc)
        response = _order_response(order, requested_qty=requested_qty)
        if response.order_id and (_decimal(response.executed_qty) or Decimal(0)) > 0:
            await self._attach_fill_economics(client, symbol, response)
        return response

    async def _reconcile(self, client: AsterProApiClient, symbol: str, client_order_id: str) -> dict[str, Any] | None:
        """The venue's record of this client id, or ``None`` when the venue has none.

        Raises ``AsterUnknownOutcomeError`` when the lookup itself fails: a failed
        read is never evidence that the order does not exist.
        """
        try:
            return await client.get_order(symbol=symbol, client_order_id=client_order_id)
        except AsterApiError as exc:
            if exc.code == _NO_SUCH_ORDER:
                return None
            raise AsterUnknownOutcomeError(f"order {client_order_id} lookup failed: {exc}") from exc

    async def _await_terminal(self, client: AsterProApiClient, symbol: str, order: dict[str, Any]) -> dict[str, Any]:
        client_order_id = str(order.get("clientOrderId", ""))
        for _ in range(_FILL_POLL_ATTEMPTS):
            if str(order.get("status")) in _TERMINAL_STATUSES or not client_order_id:
                return order
            await asyncio.sleep(_FILL_POLL_INTERVAL_SECONDS)
            refreshed = await self._reconcile(client, symbol, client_order_id)
            if refreshed is not None:
                order = refreshed
        return order

    async def _attach_fill_economics(
        self, client: AsterProApiClient, symbol: str, response: aster_perps_pb2.AsterOrderResponse
    ) -> None:
        await self._attach_fill_economics_for(client, symbol, response, [{"orderId": response.order_id}])

    async def _attach_fill_economics_for(
        self,
        client: AsterProApiClient,
        symbol: str,
        response: aster_perps_pb2.AsterOrderResponse,
        orders: list[dict[str, Any]],
    ) -> None:
        """Sum commission and realized PnL over every fill of ``orders``.

        Leaves the fields empty (unmeasured) when any read fails; never zero-fills.
        """
        trades: list[dict[str, Any]] = []
        for order in orders:
            order_id = int(order.get("orderId", 0) or 0)
            if not order_id or (_decimal(order.get("executedQty", "1")) or Decimal(0)) <= 0:
                continue
            try:
                trades.extend(await client.user_trades(symbol=symbol, order_id=order_id))
            except (AsterApiError, AsterUnknownOutcomeError) as exc:
                logger.warning("Aster fills for order %s unread: %s", order_id, exc)
                return
        if not trades:
            return
        fee = sum((_decimal(t.get("commission")) or Decimal(0) for t in trades), Decimal(0))
        pnl = sum((_decimal(t.get("realizedPnl")) or Decimal(0) for t in trades), Decimal(0))
        assets = {str(t.get("commissionAsset", "")) for t in trades}
        response.fee = str(fee)
        response.fee_asset = assets.pop() if len(assets) == 1 else ""
        response.realized_pnl = str(pnl)

    async def GetOrder(
        self, request: aster_perps_pb2.AsterGetOrderRequest, _context: grpc.aio.ServicerContext
    ) -> aster_perps_pb2.AsterOrderResponse:
        client, error = self._client_for(request.wallet_address)
        if client is None:
            return aster_perps_pb2.AsterOrderResponse(success=False, error=error)
        if request.close_position:
            closed = await self._close(client, request, None, place=False)
            if closed is not None:
                return closed
            # No leg was ever sent. If the venue reads flat, this is the answer the
            # close gave when it returned already_flat, so a lost response
            # reconciles to the same success; a held position means it never ran.
            try:
                if await self._is_flat(client, request.symbol):
                    return _already_flat(request.client_order_id)
            except (AsterApiError, AsterUnknownOutcomeError, ValueError) as exc:
                return _unknown_order(request.client_order_id, f"position unreadable: {exc}")
            return aster_perps_pb2.AsterOrderResponse(
                success=False,
                order_not_found=True,
                error=f"no Aster close leg under client id {request.client_order_id}",
                client_order_id=request.client_order_id,
            )
        try:
            order = await self._reconcile(client, request.symbol, request.client_order_id)
        except AsterUnknownOutcomeError as exc:
            return _unknown_order(request.client_order_id, str(exc))
        if order is None:
            return aster_perps_pb2.AsterOrderResponse(
                success=False,
                order_not_found=True,
                error=f"no Aster order under client id {request.client_order_id}",
                client_order_id=request.client_order_id,
            )
        return await self._finalize(client, request.symbol, order)

    async def GetPositions(
        self, request: aster_perps_pb2.AsterGetPositionsRequest, _context: grpc.aio.ServicerContext
    ) -> aster_perps_pb2.AsterPositionsResponse:
        client, error = self._client_for(request.wallet_address)
        if client is None:
            return aster_perps_pb2.AsterPositionsResponse(success=False, error=error)
        try:
            rows = await client.positions(request.symbol or None)
        except (AsterApiError, AsterUnknownOutcomeError) as exc:
            return aster_perps_pb2.AsterPositionsResponse(success=False, error=str(exc))
        return aster_perps_pb2.AsterPositionsResponse(
            success=True,
            positions=[
                aster_perps_pb2.AsterPosition(
                    symbol=str(row.get("symbol", "")),
                    position_amt=str(row.get("positionAmt", "")),
                    entry_price=str(row.get("entryPrice", "")),
                    mark_price=str(row.get("markPrice", "")),
                    unrealized_pnl=str(row.get("unRealizedProfit", "")),
                    leverage=str(row.get("leverage", "")),
                    notional=str(row.get("notional", "")),
                    liquidation_price=str(row.get("liquidationPrice", "")),
                    position_side=str(row.get("positionSide", "")),
                )
                for row in rows
                if _decimal(row.get("positionAmt")) not in (None, Decimal(0))
            ],
        )

    async def _pending_transfers(self, client: AsterProApiClient) -> list[aster_perps_pb2.AsterPendingTransfer]:
        """Transfers in flight between the wallet and the account, rebuilt from venue history.

        In flight: any PROCESSING deposit or withdrawal, a recent SUCCESS
        withdrawal whose payout is not mined yet, and a withdrawal this gateway
        submitted that the history does not show yet. An unknown state, a
        reverted payout or an unresolved unknown-outcome withdrawal raises: the
        account is unmeasured rather than valued without the money.
        """
        history = await client.transfer_history()
        now_ms = int(time.time() * 1000)
        self._resolve_unknown_withdrawal(history, now_ms)
        in_flight: list[dict[str, Any]] = []
        for record in history:
            if await self._in_flight(record, now_ms):
                in_flight.append(record)
        recorded = {str(r.get("id", "")) for r in history}
        submitted: list[aster_perps_pb2.AsterPendingTransfer] = []
        for withdraw_id, (asset, amount, fee) in list(self._submitted_withdrawals.items()):
            if withdraw_id in recorded:
                self._submitted_withdrawals.pop(withdraw_id, None)
                continue
            submitted.append(
                aster_perps_pb2.AsterPendingTransfer(
                    type="WITHDRAW", asset=asset, amount=amount, fee=fee, transfer_id=withdraw_id
                )
            )
        unpriced = [r for r in in_flight if _is_withdrawal(r) and str(r.get("id", "")) not in self._withdrawal_fees]
        venue_fees = await self._withdraw_fees(client) if unpriced else {}
        return [
            aster_perps_pb2.AsterPendingTransfer(
                type=str(r.get("type", "")).upper(),
                asset=str(r.get("asset", "")),
                amount=str(r.get("amount", "")),
                fee=self._withdrawal_fee(r, venue_fees) if _is_withdrawal(r) else "",
                transfer_id=str(r.get("id", "")),
            )
            for r in in_flight
        ] + submitted

    def _withdrawal_fee(self, record: dict[str, Any], venue_fees: dict[tuple[str, str], str]) -> str:
        """The withdrawal's venue fee; "" (unmeasured) when neither this gateway nor the venue knows it."""
        known = self._withdrawal_fees.get(str(record.get("id", "")))
        if known is not None:
            return known
        return venue_fees.get((str(record.get("asset", "")), str(record.get("chainId", ""))), "")

    async def _in_flight(self, record: dict[str, Any], now_ms: int) -> bool:
        state = str(record.get("state", "")).upper()
        if state == "PROCESSING":
            return True
        if state not in _SETTLED_STATES:
            raise AsterApiError(f"Aster transfer {record.get('id')} has an unknown state {state!r}")
        if state != "SUCCESS" or not _is_withdrawal(record):
            return False
        if str(record.get("chainId", "")) != str(WITHDRAW_CHAIN_ID):
            # Paid out on another chain: it never reaches the wallet valued here.
            return False
        expired = now_ms - int(record.get("time") or 0) > _PAYOUT_WINDOW_MS
        if expired and str(record.get("id", "")) not in self._known_withdrawals():
            # Old history this gateway did not make (e.g. a manual withdrawal long
            # ago): settled, never a permanent reason to leave the account unmeasured.
            return False
        if await self._payout_settled(record):
            return False
        if expired:
            # Past the window a payout the chain cannot show is not counted twice (wallet and in flight).
            raise AsterApiError(f"payout of withdrawal {record.get('id')} not found on chain after the payout window")
        return True

    def _known_withdrawals(self) -> set[str]:
        """Venue ids of withdrawals this gateway made or reconciled."""
        return self._attributed_withdrawals | set(self._submitted_withdrawals) | set(self._withdrawal_fees)

    async def _payout_settled(self, record: dict[str, Any]) -> bool:
        """Whether the withdrawal's payout is mined; an unreadable or reverted payout raises."""
        tx_hash = str(record.get("txHash") or "")
        if not tx_hash:
            return False
        if tx_hash in self._settled_payouts:
            return True
        if str(record.get("chainId", "")) != str(WITHDRAW_CHAIN_ID):
            raise AsterApiError(f"cannot confirm a payout on chain {record.get('chainId')}")
        web3 = get_cached_web3("bsc")
        try:
            receipt = await asyncio.to_thread(web3.eth.get_transaction_receipt, tx_hash)
        except TransactionNotFound:
            return False
        except Exception as exc:  # noqa: BLE001 — unknown payout state must not count the money twice
            raise AsterApiError(f"payout {tx_hash} receipt unreadable: {exc}") from exc
        if int(receipt.get("status", 0)) != 1:
            raise AsterApiError(f"payout {tx_hash} reverted on BSC; the withdrawn funds are unaccounted for")
        self._settled_payouts.add(tx_hash)
        return True

    def _adopt_unknown_withdrawal(self, history: list[dict[str, Any]], now_ms: int) -> bool:
        """Settle the unknown-outcome withdrawal against the venue history; whether it is still unknown.

        The withdrawal the venue recorded after the attempt is the one that was
        accepted; it takes the fee known at submission (after a withdraw-all the
        venue's withdraw-info no longer lists the asset).
        """
        since = self._unknown_withdrawal_since_ms
        if since is None:
            return False
        executed, unresolved = self._withdrawals_since(history, since - _CLOCK_SKEW_MS)
        if len(executed) > 1 or unresolved:
            return True
        if executed:
            record = executed[0]
            withdraw_id = str(record.get("id", ""))
            self._withdrawal_fees.setdefault(withdraw_id, self._unknown_withdrawal_fee)
            # Now known as accepted: a re-send of the same request is refused like any other.
            self._remember_accepted(
                self._unknown_withdrawal_request_id,
                withdraw_id,
                str(record.get("amount", "")),
                self._withdrawal_fees[withdraw_id],
            )
        elif now_ms - since <= _PAYOUT_WINDOW_MS:
            return True
        self._unknown_withdrawal_since_ms = None
        self._unknown_withdrawal_request_id = ""
        return False

    def _withdrawals_since(
        self, history: list[dict[str, Any]], since_ms: int, asset: str = ""
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Withdrawals recorded since ``since_ms`` not yet matched to a submission: (executed, unresolved).

        Executed ones (PROCESSING or SUCCESS) left the account. Unresolved ones
        carry a state that is neither executed nor an explicit failure. Failed
        ones are dropped: uncorrelated, they prove nothing about any attempt.
        """
        recent = [
            r
            for r in history
            if _is_withdrawal(r)
            and int(r.get("time") or 0) >= since_ms
            and str(r.get("id", "")) not in self._attributed_withdrawals
            and (not asset or str(r.get("asset", "")) == asset)
        ]
        states = [str(r.get("state", "")).upper() for r in recent]
        executed = [r for r, state in zip(recent, states, strict=True) if state in _EXECUTED_STATES]
        unresolved = [
            r for r, state in zip(recent, states, strict=True) if state not in _EXECUTED_STATES | _FAILED_STATES
        ]
        return executed, unresolved

    def _resolve_unknown_withdrawal(self, history: list[dict[str, Any]], now_ms: int) -> None:
        if self._adopt_unknown_withdrawal(history, now_ms):
            raise AsterApiError(
                "an Aster withdrawal's outcome is unknown; the account stays unmeasured until it resolves"
            )

    @staticmethod
    async def _withdraw_fees(client: AsterProApiClient) -> dict[tuple[str, str], str]:
        info = await client.withdraw_info()
        return {
            (asset, str(chain_id)): str(chain.get("withdrawFee", ""))
            for asset, entry in (info.get("balances") or {}).items()
            for chain_id, chain in (entry.get("chainBalances") or {}).items()
        }

    async def GetBalances(
        self, request: aster_perps_pb2.AsterGetBalancesRequest, _context: grpc.aio.ServicerContext
    ) -> aster_perps_pb2.AsterBalancesResponse:
        client, error = self._client_for(request.wallet_address)
        if client is None:
            return aster_perps_pb2.AsterBalancesResponse(success=False, error=error)
        try:
            rows = await client.balances()
            pending = await self._pending_transfers(client)
        except (AsterApiError, AsterUnknownOutcomeError) as exc:
            return aster_perps_pb2.AsterBalancesResponse(success=False, error=str(exc))
        return aster_perps_pb2.AsterBalancesResponse(
            success=True,
            pending_transfers=pending,
            balances=[
                aster_perps_pb2.AsterBalance(
                    asset=str(row.get("asset", "")),
                    balance=str(row.get("balance", "")),
                    available_balance=str(row.get("availableBalance", "")),
                    cross_unrealized_pnl=str(row.get("crossUnPnl", "")),
                )
                for row in rows
                if _decimal(row.get("balance")) not in (None, Decimal(0))
            ],
        )

    async def _resend_refusal(
        self, client: AsterProApiClient, request_id: str
    ) -> aster_perps_pb2.AsterWithdrawResponse | None:
        """Refuse a re-send of an accepted request, unless its withdrawal provably failed.

        A re-send is never answered with the earlier success: a resumed teardown
        re-runs intents that already succeeded, and a second success would be
        booked twice. Only an explicit venue failure (nothing moved) lets the
        request go out again; an unreadable history refuses too.
        """
        withdraw_id = self._accepted_requests[request_id][0]
        try:
            history = await client.transfer_history()
        except (AsterApiError, AsterUnknownOutcomeError) as exc:
            return aster_perps_pb2.AsterWithdrawResponse(
                success=False,
                error=f"withdrawal {withdraw_id} already sent for this request; history unreadable: {exc}",
            )
        record = next((r for r in history if _is_withdrawal(r) and str(r.get("id", "")) == withdraw_id), None)
        if record is not None and str(record.get("state", "")).upper() in _FAILED_STATES:
            del self._accepted_requests[request_id]
            return None
        return aster_perps_pb2.AsterWithdrawResponse(
            success=False, error=f"already withdrawn for this request as Aster withdrawal {withdraw_id}"
        )

    def _remember_accepted(self, request_id: str, withdraw_id: str, amount: str, fee: str) -> None:
        self._attributed_withdrawals.add(withdraw_id)
        if request_id:
            self._accepted_requests[request_id] = (withdraw_id, amount, fee)

    async def _withdraw_terms(
        self, client: AsterProApiClient, request: aster_perps_pb2.AsterWithdrawRequest, amount: Decimal | None
    ) -> tuple[Decimal, Decimal] | aster_perps_pb2.AsterWithdrawResponse:
        """The (amount, fee) to withdraw, or the response that ends the request without sending.

        ``amount`` None withdraws everything withdrawable.
        """
        try:
            info = await client.withdraw_info()
            chain = (info.get("balances", {}).get(request.asset, {}).get("chainBalances", {}) or {}).get(
                str(WITHDRAW_CHAIN_ID), {}
            )
            fee = _decimal(chain.get("withdrawFee"))
            withdrawable = _decimal(chain.get("perpMaxWithdrawAmount"))
        except (AsterApiError, AsterUnknownOutcomeError) as exc:
            return aster_perps_pb2.AsterWithdrawResponse(success=False, error=str(exc))
        if fee is None or withdrawable is None:
            return aster_perps_pb2.AsterWithdrawResponse(
                success=False, error=f"no withdrawable {request.asset} on BSC in the Aster account"
            )
        if amount is None:
            amount = withdrawable
        if amount > withdrawable:
            return aster_perps_pb2.AsterWithdrawResponse(
                success=False, error=f"amount {amount} exceeds withdrawable {withdrawable} {request.asset}"
            )
        if amount <= fee:
            return aster_perps_pb2.AsterWithdrawResponse(
                success=False, error=f"amount {amount} does not cover the withdrawal fee {fee}"
            )
        return amount, fee

    async def Withdraw(
        self, request: aster_perps_pb2.AsterWithdrawRequest, _context: grpc.aio.ServicerContext
    ) -> aster_perps_pb2.AsterWithdrawResponse:
        """Withdraw free margin to the main wallet on BSC (the only permitted receiver)."""
        client, error = self._client_for(request.wallet_address)
        if client is None:
            return aster_perps_pb2.AsterWithdrawResponse(success=False, error=error)
        if request.client_request_id in self._accepted_requests:
            refusal = await self._resend_refusal(client, request.client_request_id)
            if refusal is not None:
                return refusal
        if self._unknown_withdrawal_since_ms is not None:
            return aster_perps_pb2.AsterWithdrawResponse(
                success=False, error="an earlier Aster withdrawal's outcome is still unknown; reconcile it first"
            )
        withdraw_all = request.amount == "all"
        amount = None if withdraw_all else _decimal(request.amount)
        if not withdraw_all and (amount is None or amount <= 0):
            return aster_perps_pb2.AsterWithdrawResponse(
                success=False, error="amount must be a positive decimal or 'all'"
            )
        terms = await self._withdraw_terms(client, request, amount)
        if isinstance(terms, aster_perps_pb2.AsterWithdrawResponse):
            return terms
        amount, fee = terms
        attempted_ms = int(time.time() * 1000)
        try:
            result = await client.withdraw(asset=request.asset, amount=str(amount), fee=str(fee))
        except AsterUnknownOutcomeError as exc:
            self._unknown_withdrawal_since_ms = attempted_ms
            self._unknown_withdrawal_fee = str(fee)
            self._unknown_withdrawal_request_id = request.client_request_id
            return aster_perps_pb2.AsterWithdrawResponse(
                success=False, outcome_unknown=True, error=f"withdrawal outcome unknown: {exc}"
            )
        except AsterApiError as exc:
            return aster_perps_pb2.AsterWithdrawResponse(success=False, error=str(exc))
        withdraw_id = str(result.get("withdrawId", ""))
        if withdraw_id:
            self._submitted_withdrawals[withdraw_id] = (request.asset, str(amount), str(fee))
            self._withdrawal_fees[withdraw_id] = str(fee)
            self._remember_accepted(request.client_request_id, withdraw_id, str(amount), str(fee))
        return aster_perps_pb2.AsterWithdrawResponse(
            success=True,
            withdraw_id=withdraw_id,
            amount=str(amount),
            fee=str(fee),
            receiver=client.user_address,
        )

    async def FindWithdrawal(
        self, request: aster_perps_pb2.AsterFindWithdrawalRequest, _context: grpc.aio.ServicerContext
    ) -> aster_perps_pb2.AsterFindWithdrawalResponse:
        """The withdrawal an attempt made, for reconciling an unknown outcome.

        An attempt this gateway accepted is matched by its request id, even if
        the caller never received the answer. Otherwise (e.g. after a gateway
        restart) only an executed withdrawal recorded since ``since_ms`` counts;
        a failed or unknown-state record cannot be tied to the attempt, so it
        proves nothing and the attempt is "never accepted" only once the payout
        window has passed with no executed record.
        """
        client, error = self._client_for(request.wallet_address)
        if client is None:
            return aster_perps_pb2.AsterFindWithdrawalResponse(success=False, error=error)
        try:
            history = await client.transfer_history()
            accepted = self._accepted_requests.get(request.client_request_id)
            if accepted is not None:
                return self._find_accepted(history, accepted)
            executed, unresolved = self._withdrawals_since(history, request.since_ms - _CLOCK_SKEW_MS)
            if unresolved or len(executed) > 1:
                return aster_perps_pb2.AsterFindWithdrawalResponse(
                    success=False, error=f"withdrawals since {request.since_ms} cannot be attributed to one attempt"
                )
            if not executed:
                waited = int(time.time() * 1000) - request.since_ms
                return aster_perps_pb2.AsterFindWithdrawalResponse(success=True, not_found=waited > _PAYOUT_WINDOW_MS)
            record = executed[0]
            self._adopt_unknown_withdrawal(history, int(time.time() * 1000))
            fees = {} if str(record.get("id", "")) in self._withdrawal_fees else await self._withdraw_fees(client)
        except (AsterApiError, AsterUnknownOutcomeError) as exc:
            return aster_perps_pb2.AsterFindWithdrawalResponse(success=False, error=str(exc))
        return aster_perps_pb2.AsterFindWithdrawalResponse(
            success=True,
            found=True,
            withdraw_id=str(record.get("id", "")),
            amount=str(record.get("amount", "")),
            fee=self._withdrawal_fee(record, fees),
        )

    @staticmethod
    def _find_accepted(
        history: list[dict[str, Any]], accepted: tuple[str, str, str]
    ) -> aster_perps_pb2.AsterFindWithdrawalResponse:
        """Reconcile a withdrawal this gateway accepted, by its venue id."""
        withdraw_id, amount, fee = accepted
        record = next((r for r in history if _is_withdrawal(r) and str(r.get("id", "")) == withdraw_id), None)
        if record is None:
            # Accepted moments ago; the history shows it about a second later.
            return aster_perps_pb2.AsterFindWithdrawalResponse(success=True)
        state = str(record.get("state", "")).upper()
        if state in _FAILED_STATES:
            return aster_perps_pb2.AsterFindWithdrawalResponse(success=True, not_found=True)
        if state not in _EXECUTED_STATES:
            return aster_perps_pb2.AsterFindWithdrawalResponse(
                success=False, error=f"withdrawal {withdraw_id} has an unresolved state {state!r}"
            )
        return aster_perps_pb2.AsterFindWithdrawalResponse(
            success=True, found=True, withdraw_id=withdraw_id, amount=amount, fee=fee
        )


def _is_withdrawal(record: dict[str, Any]) -> bool:
    return str(record.get("type", "")).upper() == "WITHDRAW"


__all__ = ["AsterPerpsServiceServicer"]
