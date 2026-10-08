"""Aster Pro gateway servicer, strategy-side handler and runner hook."""

from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import grpc
import pytest
from eth_account import Account

from almanak.connectors.aster_perps.execution import ASTER_ORDER_KEY, ASTER_WITHDRAW_KEY, AsterOrderHandler
from almanak.connectors.aster_perps.gateway.api_client import (
    AsterApiError,
    AsterUnknownOutcomeError,
    SymbolRules,
    protected_price,
)
from almanak.connectors.aster_perps.gateway.service import AsterPerpsServiceServicer
from almanak.connectors.aster_perps.proto import aster_perps_pb2
from almanak.connectors.aster_perps.runner_hooks import AsterPerpsRunnerHookConnector
from almanak.framework.execution.clob_handler import ClobOrderStatus
from almanak.framework.execution.offchain_venue import has_offchain_fill, offchain_execution_result
from almanak.framework.models.reproduction_bundle import ActionBundle

MAIN = Account.create()
RULES = SymbolRules(
    "ETHUSDT", "TRADING", Decimal("0.001"), Decimal("0.001"), Decimal("2000"), Decimal("5"), 3, Decimal("0.01")
)
NOT_FOUND = AsterApiError("Order does not exist.", code=-2013)


class FakeClient:
    """Stands in for ``AsterProApiClient``: orders by client id, and a position that fills move."""

    def __init__(self, *, hedge: bool = False, position_amt: str = "0", existing: dict | None = None) -> None:
        self.user_address = MAIN.address
        self.hedge = hedge
        self.position_amt = position_amt
        self.orders: dict[str, dict] = {existing["clientOrderId"]: existing} if existing else {}
        self.calls: list[tuple[str, Any]] = []
        self.place_error: Exception | None = None
        self.place_response: dict | None = None
        # Executed quantity per successive placement; a full fill when exhausted.
        self.fills: list[str] = []
        self.lookup_error: Exception = NOT_FOUND
        self.after_unknown: dict | None = None

    async def is_hedge_mode(self) -> bool:
        return self.hedge

    async def symbol_rules(self, symbol: str) -> SymbolRules:
        return RULES

    async def mark_price(self, symbol: str) -> Decimal:
        return Decimal("2727.56")

    async def set_leverage(self, symbol: str, leverage: int) -> dict:
        self.calls.append(("leverage", leverage))
        return {}

    async def positions(self, symbol: str | None = None) -> list[dict]:
        return [{"symbol": "ETHUSDT", "positionAmt": self.position_amt}]

    async def get_order(self, *, symbol: str, client_order_id: str) -> dict:
        if client_order_id in self.orders:
            return self.orders[client_order_id]
        if self.after_unknown is not None and any(c[0] == "place" for c in self.calls):
            return self.after_unknown
        raise self.lookup_error

    async def place_ioc_order(self, **kwargs: Any) -> dict:
        self.calls.append(("place", kwargs))
        if self.place_error is not None:
            raise self.place_error
        if self.place_response is not None:
            return self.place_response
        executed = self.fills.pop(0) if self.fills else kwargs["quantity"]
        status = "FILLED" if Decimal(executed) == Decimal(kwargs["quantity"]) else "EXPIRED"
        order = _filled(kwargs["side"], kwargs["quantity"], kwargs["client_order_id"], status=status, executed=executed)
        order["orderId"] = 41 + len(_placed(self))
        order["cumQuote"] = str(Decimal(executed) * Decimal("2727.5"))
        self.orders[kwargs["client_order_id"]] = order
        signed = Decimal(executed) if kwargs["side"] == "BUY" else -Decimal(executed)
        self.position_amt = str(Decimal(self.position_amt) + signed)
        return order

    async def user_trades(self, *, symbol: str, order_id: int) -> list[dict]:
        self.calls.append(("trades", order_id))
        return [{"commission": "0.0022", "commissionAsset": "USDT", "realizedPnl": "0.01"}]


def _filled(side: str, quantity: str, client_order_id: str, *, status: str = "FILLED", executed: str = "") -> dict:
    return {
        "orderId": 42,
        "clientOrderId": client_order_id,
        "status": status,
        "side": side,
        "executedQty": executed or quantity,
        "origQty": quantity,
        "avgPrice": "2727.50",
        "cumQuote": "5.455",
    }


def _servicer(client: FakeClient) -> AsterPerpsServiceServicer:
    servicer = AsterPerpsServiceServicer(SimpleNamespace(private_key=MAIN.key.hex(), safe_mode=None))
    servicer._client = client  # type: ignore[assignment]
    return servicer


def _request(**overrides: Any) -> aster_perps_pb2.AsterPlaceMarketOrderRequest:
    fields: dict[str, Any] = {
        "symbol": "ETHUSDT",
        "is_long": True,
        "notional_usd": "6",
        "leverage": 5,
        "client_order_id": "almo123",
        "wallet_address": MAIN.address,
        "max_slippage": "0.01",
    }
    fields.update(overrides)
    return aster_perps_pb2.AsterPlaceMarketOrderRequest(**fields)


def _placed(client: FakeClient) -> list[dict]:
    return [c[1] for c in client.calls if c[0] == "place"]


@pytest.mark.asyncio
async def test_open_is_an_ioc_limit_bounded_by_max_slippage() -> None:
    client = FakeClient()
    response = await _servicer(client).PlaceMarketOrder(_request(), None)
    assert response.success
    assert client.calls[0] == ("leverage", 5)
    [place] = _placed(client)
    # 2727.56 x 1.01 = 2754.8356, rounded DOWN to the 0.01 tick for a BUY.
    assert (place["side"], place["quantity"], place["price"], place["reduce_only"]) == (
        "BUY",
        "0.002",
        "2754.83",
        False,
    )
    assert (response.fee, response.fee_asset) == ("0.0022", "USDT")


@pytest.mark.asyncio
async def test_close_flattens_the_whole_position_reduce_only_within_the_bound() -> None:
    client = FakeClient(position_amt="0.002")
    response = await _servicer(client).PlaceMarketOrder(_request(close_position=True, notional_usd=""), None)
    assert response.success
    [place] = _placed(client)
    # 2727.56 x 0.99 = 2700.2844, rounded UP to the tick for a SELL (never wider than asked).
    assert (place["side"], place["quantity"], place["price"], place["reduce_only"]) == (
        "SELL",
        "0.002",
        "2700.29",
        True,
    )


@pytest.mark.parametrize("slippage", ["", "0", "1", "-0.01", "abc"])
@pytest.mark.asyncio
async def test_an_order_without_a_usable_slippage_bound_is_refused(slippage: str) -> None:
    client = FakeClient()
    response = await _servicer(client).PlaceMarketOrder(_request(max_slippage=slippage), None)
    assert not response.success and "max_slippage" in response.error
    assert not client.calls


@pytest.mark.parametrize(("side", "expected"), [("BUY", Decimal("2754.83")), ("SELL", Decimal("2700.29"))])
def test_protected_price_never_widens_the_requested_bound(side: str, expected: Decimal) -> None:
    price = protected_price(Decimal("2727.56"), side=side, max_slippage=Decimal("0.01"), rules=RULES)
    assert price == expected
    assert abs(price - Decimal("2727.56")) <= Decimal("2727.56") * Decimal("0.01")


@pytest.mark.asyncio
async def test_close_refuses_when_the_held_direction_differs() -> None:
    client = FakeClient(position_amt="-0.002")
    response = await _servicer(client).PlaceMarketOrder(_request(close_position=True), None)
    assert not response.success
    assert "is short" in response.error
    assert not client.calls


@pytest.mark.asyncio
async def test_a_close_with_no_position_at_the_venue_succeeds_as_already_flat_without_an_order() -> None:
    client = FakeClient(position_amt="0")
    response = await _servicer(client).PlaceMarketOrder(_close_request(), None)
    assert response.success and response.already_flat and not response.error
    assert (response.executed_qty, response.requested_qty, response.cum_quote) == ("0", "0", "0")
    assert response.client_order_id == "almc" + "a" * 32
    assert not _placed(client)


@pytest.mark.parametrize(
    "rows", [[{"symbol": "ETHUSDT", "positionAmt": bad}] for bad in ("", "garbage", "NaN")] + [[{"symbol": "ETHUSDT"}]]
)
@pytest.mark.asyncio
async def test_a_close_over_an_unreadable_position_amount_is_never_already_flat(rows: list[dict]) -> None:
    client = _PositionsClient(rows)
    response = await _servicer(client).PlaceMarketOrder(_close_request(), None)
    assert not response.success and not response.already_flat
    assert not _placed(client)
    reconcile = aster_perps_pb2.AsterGetOrderRequest(
        symbol="ETHUSDT", client_order_id="almc" + "a" * 32, wallet_address=MAIN.address, close_position=True
    )
    reconciled = await _servicer(client).GetOrder(reconcile, None)
    assert reconciled.outcome_unknown and not reconciled.already_flat


@pytest.mark.asyncio
async def test_open_against_an_opposite_one_way_position_is_refused() -> None:
    client = FakeClient(position_amt="-0.002")
    response = await _servicer(client).PlaceMarketOrder(_request(is_long=True), None)
    assert not response.success and "holds a short" in response.error
    assert not client.calls


@pytest.mark.asyncio
async def test_hedge_mode_account_is_refused_before_any_order() -> None:
    client = FakeClient(hedge=True)
    response = await _servicer(client).PlaceMarketOrder(_request(), None)
    assert not response.success
    assert not client.calls


@pytest.mark.asyncio
async def test_existing_client_order_is_returned_not_resubmitted() -> None:
    client = FakeClient(existing=_filled("BUY", "0.002", "almo123"))
    response = await _servicer(client).PlaceMarketOrder(_request(), None)
    assert response.success
    assert response.order_id == 42
    assert not _placed(client)


@pytest.mark.parametrize("lookup_error", [AsterUnknownOutcomeError("timeout"), AsterApiError("HTTP 429", code=-1003)])
@pytest.mark.asyncio
async def test_a_failed_pre_submit_lookup_never_resubmits(lookup_error: Exception) -> None:
    client = FakeClient()
    client.lookup_error = lookup_error
    response = await _servicer(client).PlaceMarketOrder(_request(), None)
    assert not response.success and response.outcome_unknown
    assert not _placed(client)


@pytest.mark.asyncio
async def test_unknown_outcome_reconciles_by_client_order_id() -> None:
    client = FakeClient()
    client.place_error = AsterUnknownOutcomeError("HTTP 503")
    client.after_unknown = _filled("BUY", "0.002", "almo123")
    response = await _servicer(client).PlaceMarketOrder(_request(), None)
    assert response.success
    assert not response.outcome_unknown


@pytest.mark.asyncio
async def test_a_timed_out_submit_the_venue_has_not_recorded_yet_stays_unknown() -> None:
    """Straight after a timeout the order may still be queued: "not found" proves nothing yet."""
    client = FakeClient()
    client.place_error = AsterUnknownOutcomeError("timeout")
    response = await _servicer(client).PlaceMarketOrder(_request(), None)
    assert not response.success and response.outcome_unknown


@pytest.mark.asyncio
async def test_unknown_outcome_without_a_trace_is_flagged() -> None:
    client = FakeClient()
    client.place_error = AsterUnknownOutcomeError("timeout")
    client.lookup_error = AsterUnknownOutcomeError("timeout again")
    response = await _servicer(client).PlaceMarketOrder(_request(), None)
    assert not response.success
    assert response.outcome_unknown


@pytest.mark.asyncio
async def test_an_ioc_that_filled_nothing_is_a_definitive_failure() -> None:
    client = FakeClient()
    client.place_response = _filled("BUY", "0.002", "almo123", status="EXPIRED", executed="0")
    response = await _servicer(client).PlaceMarketOrder(_request(), None)
    assert not response.success and not response.outcome_unknown
    assert "slippage bound" in response.error


@pytest.mark.asyncio
async def test_a_partly_filled_open_is_a_success_for_what_filled() -> None:
    client = FakeClient()
    client.place_response = _filled("BUY", "0.002", "almo123", status="EXPIRED", executed="0.001")
    response = await _servicer(client).PlaceMarketOrder(_request(), None)
    assert response.success and response.executed_qty == "0.001" and response.requested_qty == "0.002"


def _close_request(**overrides: Any) -> aster_perps_pb2.AsterPlaceMarketOrderRequest:
    return _request(close_position=True, notional_usd="", client_order_id="almc" + "a" * 32, **overrides)


@pytest.mark.asyncio
async def test_a_partly_filled_close_is_finished_by_a_second_leg_and_booked_as_one() -> None:
    client = FakeClient(position_amt="0.002")
    client.fills = ["0.001"]
    response = await _servicer(client).PlaceMarketOrder(_close_request(), None)
    assert response.success and client.position_amt == "0.000"
    legs = _placed(client)
    assert [leg["client_order_id"] for leg in legs] == ["almc" + "a" * 32, "almc" + "a" * 29 + ".01"]
    assert all(leg["reduce_only"] for leg in legs) and legs[1]["quantity"] == "0.001"
    assert (response.executed_qty, response.cum_quote) == ("0.002", str(Decimal("0.002") * Decimal("2727.5")))
    assert (response.fee, response.realized_pnl) == ("0.0044", "0.02")


@pytest.mark.asyncio
async def test_a_close_still_open_after_every_leg_is_not_reported_as_closed() -> None:
    """A close succeeds only when flat; the partial fill travels with the failure to be booked."""
    client = FakeClient(position_amt="0.004")
    client.fills = ["0.001", "0.001", "0.001"]
    response = await _servicer(client).PlaceMarketOrder(_close_request(), None)
    assert not response.success and not response.outcome_unknown
    assert "partly closed" in response.error
    assert (response.executed_qty, response.requested_qty, response.fee) == ("0.003", "0.004", "0.0066")
    result = AsterOrderHandler._to_result(response, {**_bundle().metadata["order_request"], "close_position": True})
    assert not result.success and result.filled_size == Decimal("0.003")
    assert result.venue_data[ASTER_ORDER_KEY]["realized_pnl"] == "0.03"


@pytest.mark.asyncio
async def test_a_close_resent_at_a_wider_bound_trades_again() -> None:
    """Teardown's slippage ladder re-sends the same close intent: each rung must send new legs."""
    client = FakeClient(position_amt="0.002")
    client.fills = ["0", "0", "0"]
    servicer = _servicer(client)
    first = await servicer.PlaceMarketOrder(_close_request(max_slippage="0.005"), None)
    assert not first.success and len(_placed(client)) == 3
    wider = await servicer.PlaceMarketOrder(_close_request(max_slippage="0.03"), None)
    assert wider.success and client.position_amt == "0.000"
    assert len(_placed(client)) == 4
    # 2727.56 x 0.97 = 2645.7332, rounded up for the SELL: the new leg carries the wider bound.
    assert _placed(client)[-1]["price"] == "2645.74"
    assert _placed(client)[-1]["client_order_id"] == "almc" + "a" * 29 + ".03"


@pytest.mark.asyncio
async def test_a_retried_close_finds_its_legs_instead_of_sending_them_again() -> None:
    client = FakeClient(position_amt="0.002")
    client.fills = ["0.001"]
    servicer = _servicer(client)
    first = await servicer.PlaceMarketOrder(_close_request(), None)
    again = await servicer.PlaceMarketOrder(_close_request(), None)
    assert first.success and again.success
    assert len(_placed(client)) == 2


@pytest.mark.asyncio
async def test_get_order_aggregates_a_close_and_never_places() -> None:
    client = FakeClient(position_amt="0.002")
    client.fills = ["0.001"]
    servicer = _servicer(client)
    await servicer.PlaceMarketOrder(_close_request(), None)
    request = aster_perps_pb2.AsterGetOrderRequest(
        symbol="ETHUSDT", client_order_id="almc" + "a" * 32, wallet_address=MAIN.address, close_position=True
    )
    response = await servicer.GetOrder(request, None)
    assert response.success and response.executed_qty == "0.002"
    assert len(_placed(client)) == 2
    missing = aster_perps_pb2.AsterGetOrderRequest(
        symbol="ETHUSDT", client_order_id="almc" + "b" * 32, wallet_address=MAIN.address, close_position=True
    )
    client.position_amt = "0.003"
    assert (await servicer.GetOrder(missing, None)).order_not_found


@pytest.mark.asyncio
async def test_a_close_reconcile_with_an_unreadable_position_stays_unknown() -> None:
    client = _PositionsClient(AsterUnknownOutcomeError("positions timed out"))
    request = aster_perps_pb2.AsterGetOrderRequest(
        symbol="ETHUSDT", client_order_id="almc" + "c" * 32, wallet_address=MAIN.address, close_position=True
    )
    response = await _servicer(client).GetOrder(request, None)
    assert response.outcome_unknown and not response.success and not response.order_not_found


@pytest.mark.asyncio
async def test_a_close_reconcile_over_offsetting_hedge_legs_is_never_already_flat() -> None:
    client = _PositionsClient(
        [{"symbol": "ETHUSDT", "positionAmt": "0.002"}, {"symbol": "ETHUSDT", "positionAmt": "-0.002"}]
    )
    request = aster_perps_pb2.AsterGetOrderRequest(
        symbol="ETHUSDT", client_order_id="almc" + "d" * 32, wallet_address=MAIN.address, close_position=True
    )
    response = await _servicer(client).GetOrder(request, None)
    assert not response.success and not response.already_flat and response.order_not_found


@pytest.mark.asyncio
async def test_a_lost_already_flat_answer_reconciles_to_the_same_success() -> None:
    client = FakeClient(position_amt="0")
    servicer = _servicer(client)
    placed = await servicer.PlaceMarketOrder(_close_request(), None)
    request = aster_perps_pb2.AsterGetOrderRequest(
        symbol="ETHUSDT", client_order_id="almc" + "a" * 32, wallet_address=MAIN.address, close_position=True
    )
    reconciled = await servicer.GetOrder(request, None)
    assert placed.already_flat and reconciled.already_flat and reconciled.success
    assert (reconciled.executed_qty, reconciled.client_order_id) == ("0", "almc" + "a" * 32)
    assert not _placed(client)


@pytest.mark.asyncio
async def test_an_order_still_open_after_polling_has_an_unknown_outcome(monkeypatch: pytest.MonkeyPatch) -> None:
    from almanak.connectors.aster_perps.gateway import service

    monkeypatch.setattr(service, "_FILL_POLL_INTERVAL_SECONDS", 0)
    client = FakeClient()
    client.place_response = _filled("BUY", "0.002", "almo123", status="NEW", executed="0")
    client.after_unknown = client.place_response
    response = await _servicer(client).PlaceMarketOrder(_request(), None)
    assert not response.success and response.outcome_unknown


@pytest.mark.asyncio
async def test_get_order_distinguishes_never_placed_from_unreadable() -> None:
    client = FakeClient()
    request = aster_perps_pb2.AsterGetOrderRequest(
        symbol="ETHUSDT", client_order_id="almo123", wallet_address=MAIN.address
    )
    not_found = await _servicer(client).GetOrder(request, None)
    assert not_found.order_not_found and not not_found.outcome_unknown
    client.lookup_error = AsterUnknownOutcomeError("timeout")
    unreadable = await _servicer(client).GetOrder(request, None)
    assert unreadable.outcome_unknown and not unreadable.order_not_found


@pytest.mark.asyncio
async def test_foreign_wallet_is_refused() -> None:
    client = FakeClient()
    response = await _servicer(client).PlaceMarketOrder(_request(wallet_address="0x" + "11" * 20), None)
    assert not response.success
    assert not client.calls


def test_servicer_is_unavailable_for_safe_wallets() -> None:
    servicer = AsterPerpsServiceServicer(SimpleNamespace(private_key=MAIN.key.hex(), safe_mode="zodiac"))
    client, error = servicer._client_for("")
    assert client is None
    assert "Safe" in error


class FakeGatewayClient:
    def __init__(self, response: Any = None, *, order: Any = None, found: Any = None) -> None:
        self.response = response
        self.order = order
        self.found = found
        self.requests: list[Any] = []

    def place_market_order(self, order_request: dict, *, wallet_address: str) -> Any:
        self.requests.append((order_request, wallet_address))
        if isinstance(self.response, Exception):
            raise self.response
        return self.response

    def get_order(self, **kwargs: Any) -> Any:
        self.requests.append(kwargs)
        if isinstance(self.order, Exception):
            raise self.order
        return self.order

    def find_withdrawal(self, **kwargs: Any) -> Any:
        self.requests.append(kwargs)
        return self.found


class _RpcTimeout(grpc.RpcError):
    def details(self) -> str:
        return "Deadline Exceeded"


def _bundle() -> ActionBundle:
    order = {
        "symbol": "ETHUSDT",
        "is_long": True,
        "notional_usd": "6",
        "close_position": False,
        "leverage": 5,
        "client_order_id": "almo123",
        "max_slippage": "0.01",
    }
    return ActionBundle(intent_type="PERP_OPEN", metadata={"protocol": "aster_perps", "order_request": order})


def _fill(**overrides: Any) -> aster_perps_pb2.AsterOrderResponse:
    fields: dict[str, Any] = dict(
        success=True,
        order_id=42,
        client_order_id="almo123",
        status="FILLED",
        side="BUY",
        executed_qty="0.002",
        requested_qty="0.002",
        avg_price="2727.5",
        cum_quote="5.455",
        fee="0.0022",
        fee_asset="USDT",
        realized_pnl="0",
    )
    fields.update(overrides)
    return aster_perps_pb2.AsterOrderResponse(**fields)


@pytest.mark.asyncio
async def test_handler_publishes_the_venue_fill() -> None:
    gateway = FakeGatewayClient(_fill())
    handler = AsterOrderHandler(gateway, wallet_address=MAIN.address)  # type: ignore[arg-type]
    assert handler.can_handle(_bundle())
    result = await handler.execute(_bundle())
    assert result.success and result.status == ClobOrderStatus.MATCHED
    assert result.requested_size == Decimal("0.002")
    assert result.venue_data[ASTER_ORDER_KEY]["avg_price"] == "2727.5"
    assert gateway.requests[0][1] == MAIN.address


@pytest.mark.asyncio
async def test_handler_reports_an_already_flat_close_as_a_success_with_no_fill() -> None:
    bundle = _bundle()
    bundle.metadata["order_request"].update(close_position=True, notional_usd="")
    flat = aster_perps_pb2.AsterOrderResponse(
        success=True, already_flat=True, client_order_id="almo123", executed_qty="0", requested_qty="0", cum_quote="0"
    )
    result = await AsterOrderHandler(FakeGatewayClient(flat), wallet_address="").execute(bundle)  # type: ignore[arg-type]
    assert result.success and result.filled_size == Decimal(0)
    order = result.venue_data[ASTER_ORDER_KEY]
    assert order["already_flat"] is True and order["reduce_only"] is True
    assert result.order_id is None
    assert not has_offchain_fill(offchain_execution_result(result))


@pytest.mark.asyncio
async def test_handler_marks_a_partial_open_as_partially_filled() -> None:
    handler = AsterOrderHandler(FakeGatewayClient(_fill(status="EXPIRED", executed_qty="0.001")), wallet_address="")  # type: ignore[arg-type]
    result = await handler.execute(_bundle())
    assert (
        result.success and result.status == ClobOrderStatus.PARTIALLY_FILLED and result.filled_size == Decimal("0.001")
    )


@pytest.mark.asyncio
async def test_handler_reports_rejection_as_failure() -> None:
    response = aster_perps_pb2.AsterOrderResponse(success=False, error="Margin is insufficient.")
    handler = AsterOrderHandler(FakeGatewayClient(response), wallet_address=MAIN.address)  # type: ignore[arg-type]
    result = await handler.execute(_bundle())
    assert not result.success and not result.outcome_unknown and result.venue_answered
    assert "insufficient" in (result.error or "")


@pytest.mark.parametrize(
    "response",
    [_RpcTimeout(), aster_perps_pb2.AsterOrderResponse(success=False, outcome_unknown=True, error="timeout")],
)
@pytest.mark.asyncio
async def test_handler_never_reports_an_unknown_outcome_as_a_plain_failure(response: Any) -> None:
    handler = AsterOrderHandler(FakeGatewayClient(response), wallet_address=MAIN.address)  # type: ignore[arg-type]
    result = await handler.execute(_bundle())
    assert not result.success and result.outcome_unknown and not result.venue_answered


@pytest.mark.parametrize(
    ("order", "expected"),
    [
        (_fill(), "filled"),
        (aster_perps_pb2.AsterOrderResponse(success=False, order_not_found=True, error="none"), "never"),
        (aster_perps_pb2.AsterOrderResponse(success=False, outcome_unknown=True, error="timeout"), "unknown"),
        (_RpcTimeout(), "unknown"),
    ],
)
@pytest.mark.asyncio
async def test_handler_reconciles_an_order_by_its_client_id(order: Any, expected: str) -> None:
    gateway = FakeGatewayClient(order=order)
    handler = AsterOrderHandler(gateway, wallet_address=MAIN.address)  # type: ignore[arg-type]
    result = await handler.reconcile(_bundle().metadata, since=datetime.now(UTC) - timedelta(minutes=1))
    if expected == "unknown":
        assert result is None
    else:
        assert result is not None and result.success is (expected == "filled") and not result.outcome_unknown
        assert result.venue_answered is (expected == "never")
    assert gateway.requests[0]["client_order_id"] == "almo123"


@pytest.mark.asyncio
async def test_handler_does_not_conclude_a_failure_for_a_young_submission() -> None:
    partial = _fill(success=False, error="position only partly closed", executed_qty="0.001")
    handler = AsterOrderHandler(FakeGatewayClient(order=partial), wallet_address=MAIN.address)  # type: ignore[arg-type]
    assert await handler.reconcile(_bundle().metadata, since=datetime.now(UTC)) is None


@pytest.mark.parametrize(("age", "settled"), [(timedelta(0), False), (timedelta(seconds=31), True)])
@pytest.mark.asyncio
async def test_handler_holds_an_already_flat_reconcile_until_a_sent_leg_would_be_visible(
    age: timedelta, settled: bool
) -> None:
    flat = aster_perps_pb2.AsterOrderResponse(
        success=True, already_flat=True, client_order_id="almo123", executed_qty="0", requested_qty="0", cum_quote="0"
    )
    handler = AsterOrderHandler(FakeGatewayClient(order=flat), wallet_address=MAIN.address)  # type: ignore[arg-type]
    result = await handler.reconcile(_bundle().metadata, since=datetime.now(UTC) - age)
    if settled:
        assert result is not None and result.success and result.filled_size == Decimal(0)
    else:
        assert result is None


@pytest.mark.asyncio
async def test_handler_does_not_conclude_never_placed_for_a_young_submission() -> None:
    not_found = aster_perps_pb2.AsterOrderResponse(success=False, order_not_found=True, error="none")
    handler = AsterOrderHandler(FakeGatewayClient(order=not_found), wallet_address=MAIN.address)  # type: ignore[arg-type]
    assert await handler.reconcile(_bundle().metadata, since=datetime.now(UTC)) is None


@pytest.mark.parametrize(
    ("found", "expected"),
    [
        (
            aster_perps_pb2.AsterFindWithdrawalResponse(
                success=True, found=True, withdraw_id="9", amount="2", fee="0.11"
            ),
            True,
        ),
        (aster_perps_pb2.AsterFindWithdrawalResponse(success=True, not_found=True), False),
        (aster_perps_pb2.AsterFindWithdrawalResponse(success=True), None),
        (aster_perps_pb2.AsterFindWithdrawalResponse(success=False, error="2 withdrawals"), None),
    ],
)
@pytest.mark.asyncio
async def test_handler_reconciles_a_withdrawal_from_the_venue_history(found: Any, expected: bool | None) -> None:
    since = datetime.now(UTC) - timedelta(minutes=1)
    gateway = FakeGatewayClient(found=found)
    handler = AsterOrderHandler(gateway, wallet_address=MAIN.address)  # type: ignore[arg-type]
    metadata = {"protocol": "aster_perps", "withdraw_request": {"asset": "USDT", "amount": "all"}}
    result = await handler.reconcile(metadata, since=since)
    if expected is None:
        assert result is None
    else:
        assert result is not None and result.success is expected and result.venue_answered is not expected
        if expected:
            assert result.venue_data[ASTER_WITHDRAW_KEY]["withdraw_id"] == "9"
    assert gateway.requests[0]["since_ms"] == int(since.timestamp() * 1000)


def test_handler_ignores_other_bundles() -> None:
    handler = AsterOrderHandler(FakeGatewayClient(None), wallet_address="")  # type: ignore[arg-type]
    assert not handler.can_handle(ActionBundle(intent_type="PERP_OPEN", metadata={"protocol": "gmx_v2"}))
    onchain = _bundle()
    onchain.transactions = [{"to": "0x0"}]
    assert not handler.can_handle(onchain)


def _result(order: dict) -> SimpleNamespace:
    return SimpleNamespace(extracted_data={ASTER_ORDER_KEY: order}, protocol_fees=None)


def test_hook_books_entry_price_and_fee_on_open() -> None:
    result = _result(
        {
            "symbol": "ETHUSDT",
            "reduce_only": False,
            "avg_price": "2727.5",
            "fee": "0.0022",
            "fee_asset": "USDT",
            "realized_pnl": "0",
            "leverage_requested": 5,
        }
    )
    AsterPerpsRunnerHookConnector().enrich_result(result, gateway_client=None, chain="bsc")
    perp = result.extracted_data["perp_data"]
    assert perp.entry_price == Decimal("2727.5")
    assert perp.exit_price is None and perp.realized_pnl is None
    assert result.protocol_fees.perp_fee_usd == Decimal("0.0022")


def test_hook_books_the_measured_fill_size_not_the_request() -> None:
    result = _result(
        {
            "symbol": "ETHUSDT",
            "reduce_only": False,
            "avg_price": "2727.5",
            "cum_quote": "5.455",
            "fee": "0.0022",
            "fee_asset": "USDT",
        }
    )
    AsterPerpsRunnerHookConnector().enrich_result(result, gateway_client=None, chain="bsc")
    assert result.extracted_data["perp_data"].size_delta == Decimal("5.455")


@pytest.mark.parametrize("scenario", ["unique", "ambiguous", "unreadable"])
def test_hook_records_observed_leverage_without_substituting_the_request(monkeypatch, scenario) -> None:
    from almanak.connectors.aster_perps.gateway_client import AsterGatewayError

    def positions(**kwargs):
        if scenario == "unreadable":
            raise AsterGatewayError("position read unavailable")
        observed = SimpleNamespace(symbol="ETHUSDT", position_amt=Decimal("0.002"), leverage=Decimal(3))
        opposite = SimpleNamespace(symbol="ETHUSDT", position_amt=Decimal("-0.002"), leverage=Decimal(7))
        return [observed, observed] if scenario == "ambiguous" else [observed, opposite]

    monkeypatch.setattr(
        "almanak.connectors.aster_perps.gateway_client.GatewayAsterPerpsClient",
        lambda gateway: SimpleNamespace(get_positions=positions),
    )
    result = _result({"symbol": "ETHUSDT", "reduce_only": False, "is_long": True, "leverage_requested": 5})
    AsterPerpsRunnerHookConnector().enrich_result(result, gateway_client=object(), chain="bsc")
    perp = result.extracted_data["perp_data"]
    assert perp.leverage_requested == Decimal(5)
    assert perp.leverage == (Decimal(3) if scenario == "unique" else None)
    assert perp.venue_leverage == perp.leverage


def test_hook_keeps_measured_zero_fee_when_result_is_frozen() -> None:
    from dataclasses import dataclass

    @dataclass(frozen=True)
    class Result:
        extracted_data: dict
        protocol_fees: None = None

    result = Result({"aster_order": {"reduce_only": True, "fee_asset": "USDT", "fee": "0"}})
    AsterPerpsRunnerHookConnector().enrich_result(result, gateway_client=None, chain="bsc")
    assert result.protocol_fees is None
    assert result.extracted_data["protocol_fees"].perp_fee_usd == Decimal(0)


def test_hook_books_exit_and_realized_pnl_on_close() -> None:
    result = _result(
        {
            "symbol": "ETHUSDT",
            "reduce_only": True,
            "avg_price": "2730",
            "fee": "0.0022",
            "fee_asset": "USDT",
            "realized_pnl": "0.005",
        }
    )
    AsterPerpsRunnerHookConnector().enrich_result(result, gateway_client=None, chain="bsc")
    perp = result.extracted_data["perp_data"]
    assert (perp.exit_price, perp.realized_pnl) == (Decimal("2730"), Decimal("0.005"))


def test_hook_books_an_already_flat_close_as_a_zero_size_close_with_unmeasured_economics() -> None:
    result = _result(
        {
            "symbol": "ETHUSDT",
            "reduce_only": True,
            "already_flat": True,
            "executed_qty": "0",
            "avg_price": "",
            "cum_quote": "0",
            "fee": "",
            "fee_asset": "",
            "realized_pnl": "",
        }
    )
    AsterPerpsRunnerHookConnector().enrich_result(result, gateway_client=None, chain="bsc")
    perp = result.extracted_data["perp_data"]
    assert perp.size_delta == Decimal(0)
    assert perp.exit_price is None and perp.realized_pnl is None
    assert result.protocol_fees is None


def test_hook_leaves_non_usd_fee_unmeasured() -> None:
    result = _result(
        {
            "symbol": "ETHUSDT",
            "reduce_only": True,
            "avg_price": "2730",
            "fee": "0.00001",
            "fee_asset": "BNB",
            "realized_pnl": "0.005",
        }
    )
    AsterPerpsRunnerHookConnector().enrich_result(result, gateway_client=None, chain="bsc")
    assert result.protocol_fees is None
    assert result.extracted_data["perp_data"].realized_pnl is None


def test_hook_is_inert_for_other_results() -> None:
    result = SimpleNamespace(extracted_data={"clob_status": "matched"}, protocol_fees=None)
    AsterPerpsRunnerHookConnector().enrich_result(result, gateway_client=None, chain="polygon")
    assert "perp_data" not in result.extracted_data


class _HistoryClient(FakeClient):
    async def balances(self) -> list[dict]:
        return [{"asset": "USDT", "balance": "0", "availableBalance": "0", "crossUnPnl": "0"}]

    async def transfer_history(self) -> list[dict]:
        return [
            {"id": "1", "type": "WITHDRAW", "asset": "USDT", "amount": "2.49", "state": "PROCESSING", "chainId": 56},
            {"id": "2", "type": "DEPOSIT", "asset": "USDT", "amount": "2.5", "state": "SUCCESS", "chainId": 56},
        ]

    async def withdraw_info(self) -> dict:
        return {"balances": {"USDT": {"chainBalances": {"56": {"withdrawFee": "0.11"}}}}}


@pytest.mark.asyncio
async def test_balances_report_only_in_flight_transfers_with_the_withdrawal_fee() -> None:
    response = await _servicer(_HistoryClient()).GetBalances(
        aster_perps_pb2.AsterGetBalancesRequest(wallet_address=MAIN.address), None
    )
    assert response.success
    [pending] = response.pending_transfers
    assert (pending.type, pending.amount, pending.fee) == ("WITHDRAW", "2.49", "0.11")


class _TrackedClient(FakeClient):
    def __init__(self, history: list[dict]) -> None:
        super().__init__()
        self.history = history

    async def balances(self) -> list[dict]:
        return []

    async def transfer_history(self) -> list[dict]:
        return self.history

    async def withdraw_info(self) -> dict:
        return {
            "balances": {"USDT": {"chainBalances": {"56": {"withdrawFee": "0.11", "perpMaxWithdrawAmount": "2.49"}}}}
        }

    async def withdraw(self, **kwargs: Any) -> dict:
        self.calls.append(("withdraw", kwargs))
        if self.place_error is not None:
            raise self.place_error
        return {"withdrawId": "78"}


def _now_ms(minutes_ago: float = 0) -> int:
    return int((time.time() - minutes_ago * 60) * 1000)


def _withdrawal(state: str, *, minutes_ago: float = 0, tx: str = "0xab") -> dict:
    return {
        "id": "77",
        "type": "WITHDRAW",
        "asset": "USDT",
        "amount": "2.49",
        "state": state,
        "txHash": tx,
        "time": _now_ms(minutes_ago),
        "chainId": 56,
    }


async def _balances(servicer: AsterPerpsServiceServicer, *, mined: bool | Exception = False) -> Any:
    async def payout_settled(record: dict) -> bool:
        if isinstance(mined, Exception):
            raise mined
        return mined

    servicer._payout_settled = payout_settled  # type: ignore[method-assign]
    return await servicer.GetBalances(aster_perps_pb2.AsterGetBalancesRequest(wallet_address=MAIN.address), None)


async def _pending(history: list[dict], *, mined: bool = False, submitted: bool = True) -> list:
    servicer = _servicer(_TrackedClient(history))
    if submitted:
        servicer._submitted_withdrawals["77"] = ("USDT", "2.49", "0.11")
    response = await _balances(servicer, mined=mined)
    assert response.success, response.error
    return list(response.pending_transfers)


@pytest.mark.asyncio
async def test_submitted_withdrawal_is_in_flight_before_the_venue_records_it() -> None:
    [pending] = await _pending([])
    assert (pending.type, pending.amount, pending.fee, pending.transfer_id) == ("WITHDRAW", "2.49", "0.11", "77")


@pytest.mark.asyncio
async def test_recorded_withdrawal_stays_in_flight_until_the_payout_is_mined() -> None:
    assert len(await _pending([_withdrawal("SUCCESS")], mined=False)) == 1
    assert await _pending([_withdrawal("SUCCESS")], mined=True) == []


@pytest.mark.asyncio
async def test_after_a_gateway_restart_an_unpaid_withdrawal_is_still_in_flight() -> None:
    [pending] = await _pending([_withdrawal("SUCCESS")], mined=False, submitted=False)
    assert (pending.amount, pending.fee) == ("2.49", "0.11")
    assert await _pending([_withdrawal("SUCCESS", minutes_ago=60)], mined=True, submitted=False) == []


class _EmptiedAccountClient(_TrackedClient):
    """After a withdraw-all the venue's withdraw-info lists no balance, hence no fee."""

    async def withdraw_info(self) -> dict:
        return {"balances": {}}


@pytest.mark.asyncio
async def test_a_withdraw_all_in_flight_is_valued_with_the_fee_known_at_submission() -> None:
    servicer = _servicer(_EmptiedAccountClient([_withdrawal("SUCCESS")]))
    servicer._withdrawal_fees["77"] = "0.11"
    response = await _balances(servicer, mined=False)
    assert response.success
    [pending] = response.pending_transfers
    assert (pending.amount, pending.fee) == ("2.49", "0.11")


@pytest.mark.asyncio
async def test_a_withdrawal_whose_fee_nobody_knows_stays_unpriced() -> None:
    servicer = _servicer(_EmptiedAccountClient([_withdrawal("SUCCESS")]))
    [pending] = (await _balances(servicer, mined=False)).pending_transfers
    assert pending.fee == ""


@pytest.mark.asyncio
async def test_failed_withdrawal_leaves_flight_and_processing_is_not_double_counted() -> None:
    assert await _pending([_withdrawal("FAILED")]) == []
    assert len(await _pending([_withdrawal("PROCESSING")])) == 1


@pytest.mark.parametrize("payout", [AsterApiError("receipt unreadable"), AsterApiError("payout reverted")])
@pytest.mark.asyncio
async def test_unreadable_or_reverted_payout_makes_balances_unmeasured(payout: Exception) -> None:
    servicer = _servicer(_TrackedClient([_withdrawal("SUCCESS")]))
    assert not (await _balances(servicer, mined=payout)).success


@pytest.mark.asyncio
async def test_an_unknown_transfer_state_makes_balances_unmeasured() -> None:
    servicer = _servicer(_TrackedClient([_withdrawal("ON_HOLD")]))
    assert not (await _balances(servicer)).success


@pytest.mark.asyncio
async def test_payout_receipt_status_is_checked(monkeypatch: pytest.MonkeyPatch) -> None:
    from almanak.connectors.aster_perps.gateway import service

    servicer = _servicer(_TrackedClient([]))
    for status, outcome in ((1, True), (0, AsterApiError)):
        web3 = SimpleNamespace(eth=SimpleNamespace(get_transaction_receipt=lambda tx, s=status: {"status": s}))
        monkeypatch.setattr(service, "get_cached_web3", lambda chain, w=web3: w)
        record = _withdrawal("SUCCESS", tx=f"0x{status}")
        if outcome is True:
            assert await servicer._payout_settled(record)
        else:
            with pytest.raises(AsterApiError, match="reverted"):
                await servicer._payout_settled(record)


@pytest.mark.asyncio
async def test_an_unknown_withdrawal_keeps_the_account_unmeasured_until_it_appears() -> None:
    client = _TrackedClient([])
    client.place_error = AsterUnknownOutcomeError("timeout")
    servicer = _servicer(client)
    request = aster_perps_pb2.AsterWithdrawRequest(asset="USDT", amount="all", wallet_address=MAIN.address)
    response = await servicer.Withdraw(request, None)
    assert response.outcome_unknown
    assert not (await _balances(servicer)).success
    again = await servicer.Withdraw(request, None)
    assert not again.success and "still unknown" in again.error
    assert len([c for c in client.calls if c[0] == "withdraw"]) == 1
    client.history = [_withdrawal("PROCESSING")]
    assert (await _balances(servicer)).success


@pytest.mark.asyncio
async def test_a_failed_pre_submit_read_is_not_an_unknown_withdrawal() -> None:
    client = _TrackedClient([])

    async def unreadable() -> dict:
        raise AsterUnknownOutcomeError("timeout")

    client.withdraw_info = unreadable  # type: ignore[method-assign]
    servicer = _servicer(client)
    response = await servicer.Withdraw(
        aster_perps_pb2.AsterWithdrawRequest(asset="USDT", amount="all", wallet_address=MAIN.address), None
    )
    assert not response.success and not response.outcome_unknown
    assert servicer._unknown_withdrawal_since_ms is None


@pytest.mark.asyncio
async def test_an_unknown_withdraw_all_keeps_its_submission_fee_once_recorded() -> None:
    client = _EmptiedAccountClient([])
    client.withdraw_info = _TrackedClient.withdraw_info.__get__(client)  # type: ignore[method-assign]
    client.place_error = AsterUnknownOutcomeError("timeout")
    servicer = _servicer(client)
    request = aster_perps_pb2.AsterWithdrawRequest(asset="USDT", amount="all", wallet_address=MAIN.address)
    assert (await servicer.Withdraw(request, None)).outcome_unknown
    client.withdraw_info = _EmptiedAccountClient.withdraw_info.__get__(client)  # type: ignore[method-assign]
    client.history = [_withdrawal("SUCCESS")]
    found = await servicer.FindWithdrawal(
        aster_perps_pb2.AsterFindWithdrawalRequest(wallet_address=MAIN.address, since_ms=_now_ms(1)), None
    )
    assert found.found and found.fee == "0.11"
    [pending] = (await _balances(servicer, mined=False)).pending_transfers
    assert pending.fee == "0.11"


@pytest.mark.parametrize(
    ("withdrawals", "since_minutes_ago", "expected"),
    [(1, 1, "found"), (0, 1, "pending"), (0, 30, "not_found"), (2, 1, "ambiguous")],
)
@pytest.mark.asyncio
async def test_find_withdrawal_attributes_only_an_unambiguous_record(
    withdrawals: int, since_minutes_ago: float, expected: str
) -> None:
    # Records are stamped at run time: the venue history is matched against a run-time ``since``.
    history = [{**_withdrawal("SUCCESS"), "id": str(77 + i)} for i in range(withdrawals)]
    response = await _servicer(_TrackedClient(history)).FindWithdrawal(
        aster_perps_pb2.AsterFindWithdrawalRequest(wallet_address=MAIN.address, since_ms=_now_ms(since_minutes_ago)),
        None,
    )
    outcome = (
        "ambiguous"
        if not response.success
        else "found"
        if response.found
        else "not_found"
        if response.not_found
        else "pending"
    )
    assert outcome == expected
    if expected == "found":
        assert (response.withdraw_id, response.amount, response.fee) == ("77", "2.49", "0.11")


@pytest.mark.asyncio
async def test_handler_keeps_an_order_unresolved_when_the_gateway_cannot_read_the_venue() -> None:
    """A gateway-local lookup failure says nothing about the order, so the replay barrier must hold."""
    local = aster_perps_pb2.AsterOrderResponse(success=False, error="no Aster credentials for this wallet")
    handler = AsterOrderHandler(FakeGatewayClient(order=local), wallet_address=MAIN.address)  # type: ignore[arg-type]
    assert await handler.reconcile(_bundle().metadata, since=datetime.now(UTC) - timedelta(minutes=1)) is None


class _HttpResponse:
    def __init__(self, status: int, body: str) -> None:
        self.status, self._body = status, body

    async def text(self) -> str:
        return self._body

    async def json(self, content_type: Any = None) -> Any:
        import json

        return json.loads(self._body)


@pytest.mark.parametrize(
    ("status", "body"),
    [
        (408, "Request Timeout"),
        (400, '{"code": -1007, "msg": "Timeout waiting for response from backend server."}'),
        (200, '{"code": -1006, "msg": "An unexpected response was received from the message bus."}'),
    ],
)
@pytest.mark.asyncio
async def test_execution_status_unknown_answers_are_unknown_outcomes(status: int, body: str) -> None:
    from almanak.connectors.aster_perps.gateway.api_client import AsterProApiClient

    with pytest.raises(AsterUnknownOutcomeError):
        await AsterProApiClient._decode(_HttpResponse(status, body), "POST", "/fapi/v3/order")  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_a_plain_venue_rejection_stays_definitive() -> None:
    from almanak.connectors.aster_perps.gateway.api_client import AsterProApiClient

    with pytest.raises(AsterApiError):
        await AsterProApiClient._decode(  # type: ignore[arg-type]
            _HttpResponse(400, '{"code": -2019, "msg": "Margin is insufficient."}'), "POST", "/fapi/v3/order"
        )


@pytest.mark.asyncio
async def test_a_close_on_a_hedge_mode_account_is_refused_before_any_leg() -> None:
    client = FakeClient(hedge=True, position_amt="0.002")
    response = await _servicer(client).PlaceMarketOrder(_close_request(), None)
    assert not response.success and not response.outcome_unknown and "hedge mode" in response.error
    assert _placed(client) == []


@pytest.mark.asyncio
async def test_a_close_leg_the_venue_rejects_is_a_definitive_failure() -> None:
    client = FakeClient(position_amt="0.002")
    client.place_error = AsterApiError("ReduceOnly Order is rejected.", code=-2022)
    response = await _servicer(client).PlaceMarketOrder(_close_request(), None)
    assert not response.success and not response.outcome_unknown
    assert "ReduceOnly" in response.error and response.requested_qty == "0.002"


class _FillsThenTimesOut(FakeClient):
    async def place_ioc_order(self, **kwargs: Any) -> dict:
        await super().place_ioc_order(**kwargs)
        raise AsterUnknownOutcomeError("timeout after the venue filled the leg")


@pytest.mark.asyncio
async def test_a_timed_out_close_leg_the_venue_recorded_is_reported_as_filled() -> None:
    client = _FillsThenTimesOut(position_amt="0.002")
    response = await _servicer(client).PlaceMarketOrder(_close_request(), None)
    assert response.success and response.executed_qty == "0.002"
    assert len(_placed(client)) == 1


class _LookupFailsAfterPlacement(FakeClient):
    async def get_order(self, *, symbol: str, client_order_id: str) -> dict:
        if any(c[0] == "place" for c in self.calls):
            raise AsterUnknownOutcomeError("lookup timed out")
        raise NOT_FOUND


@pytest.mark.parametrize("lookup_fails", [False, True])
@pytest.mark.asyncio
async def test_a_timed_out_close_leg_with_no_venue_record_stays_unknown(lookup_fails: bool) -> None:
    client = _LookupFailsAfterPlacement(position_amt="0.002") if lookup_fails else FakeClient(position_amt="0.002")
    client.place_error = AsterUnknownOutcomeError("timeout")
    response = await _servicer(client).PlaceMarketOrder(_close_request(), None)
    assert not response.success and response.outcome_unknown
    assert ("lookup failed" in response.error) is lookup_fails


class _PositionsClient(FakeClient):
    def __init__(self, rows: list[dict] | Exception) -> None:
        super().__init__()
        self.rows = rows

    async def positions(self, symbol: str | None = None) -> list[dict]:
        if isinstance(self.rows, Exception):
            raise self.rows
        return self.rows


@pytest.mark.asyncio
async def test_get_positions_reports_only_held_positions() -> None:
    rows = [
        {"symbol": "ETHUSDT", "positionAmt": "-0.002", "entryPrice": "2700", "unRealizedProfit": "0.01"},
        {"symbol": "SOLUSDT", "positionAmt": "0"},
    ]
    response = await _servicer(_PositionsClient(rows)).GetPositions(
        aster_perps_pb2.AsterGetPositionsRequest(wallet_address=MAIN.address), None
    )
    assert response.success
    assert [(p.symbol, p.position_amt, p.unrealized_pnl) for p in response.positions] == [("ETHUSDT", "-0.002", "0.01")]


@pytest.mark.parametrize("failure", ["foreign_wallet", "venue_error"])
@pytest.mark.asyncio
async def test_get_positions_failures_are_errors_never_an_empty_book(failure: str) -> None:
    client = _PositionsClient(AsterUnknownOutcomeError("HTTP 503") if failure == "venue_error" else [])
    wallet = "0x" + "11" * 20 if failure == "foreign_wallet" else MAIN.address
    response = await _servicer(client).GetPositions(
        aster_perps_pb2.AsterGetPositionsRequest(wallet_address=wallet), None
    )
    assert not response.success and response.error and not response.positions


async def _find(servicer: AsterPerpsServiceServicer, since_minutes_ago: float = 1) -> Any:
    request = aster_perps_pb2.AsterFindWithdrawalRequest(
        wallet_address=MAIN.address, since_ms=_now_ms(since_minutes_ago)
    )
    return await servicer.FindWithdrawal(request, None)


@pytest.mark.asyncio
async def test_a_withdrawal_this_gateway_already_accepted_is_never_attributed_to_a_later_attempt() -> None:
    client = _TrackedClient([])
    servicer = _servicer(client)
    request = aster_perps_pb2.AsterWithdrawRequest(asset="USDT", amount="all", wallet_address=MAIN.address)
    assert (await servicer.Withdraw(request, None)).withdraw_id == "78"
    client.history = [{**_withdrawal("SUCCESS"), "id": "78"}]
    response = await _find(servicer, since_minutes_ago=0)
    assert response.success and not response.found and not response.not_found


@pytest.mark.asyncio
async def test_a_reconciled_withdrawal_is_found_again_on_a_repeated_lookup() -> None:
    """A held runner re-reconciles; the second lookup must not lose the withdrawal it found."""
    servicer = _servicer(_TrackedClient([_withdrawal("PROCESSING")]))
    assert (await _find(servicer)).found
    assert (await _find(servicer)).found


def _sent(client: FakeClient) -> list:
    return [c for c in client.calls if c[0] == "withdraw"]


def _accept(client: _TrackedClient, request_id: str = "almw1") -> AsterPerpsServiceServicer:
    """A servicer that accepted withdrawal 78 for ``request_id`` (whatever became of its response)."""
    servicer = _servicer(client)
    servicer._remember_accepted(request_id, "78", "2.49", "0.11")
    return servicer


async def _find_request(servicer: AsterPerpsServiceServicer, request_id: str, since_minutes_ago: float = 1) -> Any:
    request = aster_perps_pb2.AsterFindWithdrawalRequest(
        wallet_address=MAIN.address, since_ms=_now_ms(since_minutes_ago), client_request_id=request_id
    )
    return await servicer.FindWithdrawal(request, None)


@pytest.mark.asyncio
async def test_an_accepted_withdrawal_whose_answer_was_lost_reconciles_to_itself() -> None:
    """Accepted but the response never reached the runner: matched by request id, never "not found"."""
    servicer = _accept(_TrackedClient([{**_withdrawal("SUCCESS"), "id": "78"}]))
    found = await _find_request(servicer, "almw1", since_minutes_ago=30)
    assert found.found and (found.withdraw_id, found.amount, found.fee) == ("78", "2.49", "0.11")


@pytest.mark.asyncio
async def test_an_accepted_withdrawal_reaches_reconciliation_through_the_real_withdraw_path() -> None:
    client = _TrackedClient([])
    servicer = _servicer(client)
    request = aster_perps_pb2.AsterWithdrawRequest(
        asset="USDT", amount="2.49", wallet_address=MAIN.address, client_request_id="almw9"
    )
    assert (await servicer.Withdraw(request, None)).withdraw_id == "78"
    client.history = [{**_withdrawal("PROCESSING"), "id": "78"}]
    assert (await _find_request(servicer, "almw9", since_minutes_ago=30)).found


@pytest.mark.parametrize(
    ("state", "outcome"),
    [
        ("FAILED", "not_found"),
        ("REJECTED", "not_found"),
        ("CANCELED", "not_found"),
        ("", "unresolved"),
        ("AUDITING", "unresolved"),
    ],
)
@pytest.mark.asyncio
async def test_an_accepted_withdrawal_is_judged_by_its_own_record_state(state: str, outcome: str) -> None:
    servicer = _accept(_TrackedClient([{**_withdrawal(state), "id": "78"}]))
    response = await _find_request(servicer, "almw1")
    assert (response.success and response.not_found) is (outcome == "not_found")
    assert (not response.success) is (outcome == "unresolved")


@pytest.mark.asyncio
async def test_an_accepted_withdrawal_not_yet_in_history_is_pending() -> None:
    response = await _find_request(_accept(_TrackedClient([])), "almw1", since_minutes_ago=30)
    assert response.success and not response.found and not response.not_found


@pytest.mark.parametrize(("since_minutes_ago", "not_found"), [(1, False), (30, True)])
@pytest.mark.asyncio
async def test_an_uncorrelated_failed_record_proves_nothing_until_the_payout_window_passes(
    since_minutes_ago: float, not_found: bool
) -> None:
    response = await _find_request(_servicer(_TrackedClient([_withdrawal("FAILED")])), "", since_minutes_ago)
    assert response.success and not response.found and response.not_found is not_found


@pytest.mark.parametrize("state", ["", "AUDITING"])
@pytest.mark.asyncio
async def test_an_uncorrelated_record_in_an_unknown_state_keeps_the_attempt_unresolved(state: str) -> None:
    response = await _find_request(_servicer(_TrackedClient([_withdrawal(state)])), "")
    assert not response.success


@pytest.mark.parametrize(("minutes_after", "measured"), [(1, False), (30, True)])
@pytest.mark.asyncio
async def test_an_unknown_withdrawal_with_only_a_failed_record_stays_unmeasured_until_the_window_passes(
    minutes_after: float, measured: bool
) -> None:
    client = _TrackedClient([])
    client.place_error = AsterUnknownOutcomeError("timeout")
    servicer = _servicer(client)
    request = aster_perps_pb2.AsterWithdrawRequest(asset="USDT", amount="all", wallet_address=MAIN.address)
    assert (await servicer.Withdraw(request, None)).outcome_unknown
    servicer._unknown_withdrawal_since_ms = _now_ms(minutes_after)
    client.history = [_withdrawal("FAILED")]
    response = await _balances(servicer)
    assert response.success is measured


@pytest.mark.asyncio
async def test_an_unknown_withdrawal_stays_unmeasured_while_a_record_has_an_unknown_state() -> None:
    client = _TrackedClient([])
    client.place_error = AsterUnknownOutcomeError("timeout")
    servicer = _servicer(client)
    request = aster_perps_pb2.AsterWithdrawRequest(asset="USDT", amount="all", wallet_address=MAIN.address)
    assert (await servicer.Withdraw(request, None)).outcome_unknown
    servicer._unknown_withdrawal_since_ms = _now_ms(30)

    async def not_in_flight(record: dict, now_ms: int) -> bool:
        return False

    # Isolate the latch: the valuation itself also refuses an unknown state.
    servicer._in_flight = not_in_flight  # type: ignore[method-assign]
    client.history = [_withdrawal("AUDITING", minutes_ago=29)]
    assert not (await _balances(servicer)).success


def _resend(request_id: str = "almw1") -> aster_perps_pb2.AsterWithdrawRequest:
    return aster_perps_pb2.AsterWithdrawRequest(
        asset="USDT", amount="2.49", wallet_address=MAIN.address, client_request_id=request_id
    )


def _sent(client: FakeClient) -> list:
    return [c for c in client.calls if c[0] == "withdraw"]


@pytest.mark.parametrize("record", [{"state": "SUCCESS"}, {"state": "PROCESSING"}, {"state": "AUDITING"}, None])
@pytest.mark.asyncio
async def test_a_resent_accepted_request_is_refused_never_answered_with_the_old_success(record: dict | None) -> None:
    """A resumed teardown re-runs succeeded intents: a second success would be booked twice."""
    client = _TrackedClient([{**_withdrawal("SUCCESS"), "id": "78", **record}] if record else [])
    response = await _accept(client).Withdraw(_resend(), None)
    assert not response.success and not response.outcome_unknown and "already withdrawn" in response.error
    assert not _sent(client)


@pytest.mark.asyncio
async def test_a_resent_request_whose_withdrawal_failed_is_sent_again() -> None:
    client = _TrackedClient([{**_withdrawal("FAILED"), "id": "78"}])
    response = await _accept(client).Withdraw(_resend(), None)
    assert response.success and response.withdraw_id == "78"
    assert len(_sent(client)) == 1


@pytest.mark.asyncio
async def test_a_resent_request_with_an_unreadable_history_sends_nothing() -> None:
    class _NoHistory(_TrackedClient):
        async def transfer_history(self) -> list[dict]:
            raise AsterUnknownOutcomeError("HTTP 503")

    client = _NoHistory([])
    response = await _accept(client).Withdraw(_resend(), None)
    assert not response.success and not _sent(client)


async def _value(history: list[dict], *, known: bool, mined: bool | Exception = False) -> Any:
    servicer = _servicer(_TrackedClient(history))
    if known:
        servicer._remember_accepted("almw1", "77", "2.49", "0.11")
    return await _balances(servicer, mined=mined)


@pytest.mark.parametrize(
    ("record", "known", "mined", "pending"),
    [
        ({}, True, True, []),
        ({}, True, False, None),
        ({}, True, AsterApiError("payout reverted"), None),
        ({"txHash": ""}, True, False, None),
        ({}, False, False, []),
        ({"txHash": ""}, False, False, []),
        ({"chainId": 42161}, True, False, []),
    ],
    ids=["own-mined", "own-unmined", "own-reverted", "own-hashless", "foreign", "foreign-hashless", "other-chain"],
)
@pytest.mark.asyncio
async def test_an_old_withdrawal_is_valued_by_who_made_it_and_where_it_pays(
    record: dict, known: bool, mined: bool | Exception, pending: list | None
) -> None:
    """Past the payout window: this gateway's own withdrawal must be proven paid on BSC, or the account
    is unmeasured; old history it did not make, or a payout to another chain, never holds NAV hostage."""
    response = await _value([{**_withdrawal("SUCCESS", minutes_ago=60), **record}], known=known, mined=mined)
    assert response.success is (pending is not None)
    if pending is not None:
        assert list(response.pending_transfers) == pending


@pytest.mark.parametrize("known", [True, False])
@pytest.mark.asyncio
async def test_a_recent_payout_without_a_hash_is_in_flight(known: bool) -> None:
    response = await _value([_withdrawal("SUCCESS", minutes_ago=1, tx="")], known=known)
    assert response.success and [t.transfer_id for t in response.pending_transfers] == ["77"]


@pytest.mark.asyncio
async def test_the_handler_reconciles_a_withdrawal_by_its_request_id() -> None:
    gateway = FakeGatewayClient(found=aster_perps_pb2.AsterFindWithdrawalResponse(success=True))
    handler = AsterOrderHandler(gateway, wallet_address=MAIN.address)  # type: ignore[arg-type]
    metadata = {
        "protocol": "aster_perps",
        "withdraw_request": {"asset": "USDT", "amount": "all", "client_request_id": "almw1"},
    }
    await handler.reconcile(metadata, since=datetime.now(UTC) - timedelta(minutes=1))
    assert gateway.requests[0]["client_request_id"] == "almw1"


@pytest.mark.asyncio
async def test_a_failed_then_resent_withdrawal_whose_resend_times_out_stays_unresolved() -> None:
    """The re-send must not be reconciled against the first attempt's FAILED record."""
    client = _TrackedClient([{**_withdrawal("FAILED"), "id": "78"}])
    servicer = _accept(client)
    client.place_error = AsterUnknownOutcomeError("timeout")
    assert (await servicer.Withdraw(_resend(), None)).outcome_unknown
    response = await _find_request(servicer, "almw1")
    assert not response.not_found and not response.found


@pytest.mark.asyncio
async def test_an_unknown_withdrawal_once_resolved_refuses_a_resend_of_its_request() -> None:
    client = _TrackedClient([])
    client.place_error = AsterUnknownOutcomeError("timeout")
    servicer = _servicer(client)
    assert (await servicer.Withdraw(_resend(), None)).outcome_unknown
    client.place_error = None
    client.history = [{**_withdrawal("SUCCESS"), "amount": "2.49"}]
    assert (await _balances(servicer)).success  # adopts the executed record and clears the latch
    response = await servicer.Withdraw(_resend(), None)
    assert not response.success and "already withdrawn" in response.error
    assert len(_sent(client)) == 1


@pytest.mark.parametrize(
    "call",
    [
        lambda s: s.Withdraw(aster_perps_pb2.AsterWithdrawRequest(asset="USDT", amount="all"), None),
        lambda s: s.PlaceMarketOrder(_request(wallet_address=""), None),
        lambda s: s.GetPositions(aster_perps_pb2.AsterGetPositionsRequest(), None),
        lambda s: s.GetBalances(aster_perps_pb2.AsterGetBalancesRequest(), None),
        lambda s: s.FindWithdrawal(aster_perps_pb2.AsterFindWithdrawalRequest(), None),
    ],
    ids=["withdraw", "order", "positions", "balances", "find_withdrawal"],
)
@pytest.mark.asyncio
async def test_account_scoped_calls_without_a_wallet_are_refused(call: Any) -> None:
    client = _TrackedClient([])
    response = await call(_servicer(client))
    assert not response.success and "wallet_address is required" in response.error
    assert not _sent(client) and not _placed(client)


@pytest.mark.parametrize("is_long", [True, False])
@pytest.mark.parametrize("flat_only", [True, False])
def test_hook_excludes_flat_positions_from_observed_leverage(monkeypatch, is_long, flat_only) -> None:
    flat = SimpleNamespace(symbol="ETHUSDT", position_amt=Decimal(0), leverage=Decimal(9))
    active = SimpleNamespace(
        symbol="ETHUSDT", position_amt=Decimal("0.002") if is_long else Decimal("-0.002"), leverage=Decimal(3)
    )
    monkeypatch.setattr(
        "almanak.connectors.aster_perps.gateway_client.GatewayAsterPerpsClient",
        lambda gateway: SimpleNamespace(get_positions=lambda **kwargs: [flat] if flat_only else [flat, active]),
    )
    result = _result({"symbol": "ETHUSDT", "reduce_only": False, "is_long": is_long, "leverage_requested": 5})
    AsterPerpsRunnerHookConnector().enrich_result(result, gateway_client=object(), chain="bsc")
    assert result.extracted_data["perp_data"].venue_leverage == (None if flat_only else Decimal(3))
