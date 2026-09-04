"""Branch-complete tests for core GatewayPolymarketClient read methods."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock

import grpc
import pytest

from almanak.connectors.polymarket.exceptions import PolymarketAPIError
from almanak.connectors.polymarket.gateway_client import GatewayPolymarketClient
from almanak.connectors.polymarket.models import OrderFilters, OrderStatus, PositionFilters
from almanak.connectors.polymarket.proto import polymarket_pb2
from almanak.framework.gateway_client import GatewayClient, GatewayClientConfig


class _FakeRpcError(grpc.RpcError):
    def details(self) -> str:
        return "transport unavailable"


def _make_wrapper(stub: MagicMock) -> GatewayPolymarketClient:
    gateway = GatewayClient(config=GatewayClientConfig(host="localhost", port=50051, timeout=7.5))
    gateway._connected = True
    gateway._channel = MagicMock()
    gateway._connector_stubs = {"polymarket": stub}
    return GatewayPolymarketClient(gateway)


def _position_response() -> polymarket_pb2.PolymarketPositionsResponse:
    return polymarket_pb2.PolymarketPositionsResponse(
        positions=[
            polymarket_pb2.PolymarketPosition(
                asset="legacy-token-a",
                condition_id="condition-a",
                size="10.5",
                avg_price="0.25",
                realized_pnl="1.5",
                cur_price="0.5",
                market_id="market-a",
                token_id="token-a",
                outcome="NO",
                market_question="Question A?",
            ),
            polymarket_pb2.PolymarketPosition(
                asset="legacy-token-b",
                condition_id="condition-b",
            ),
        ],
        success=True,
    )


class TestParseMarket:
    def test_raw_json_is_authoritative(self) -> None:
        payload = {
            "id": "raw-market",
            "conditionId": "raw-condition",
            "question": "Raw question?",
            "slug": "raw-slug",
            "outcomes": json.dumps(["Yes", "No"]),
            "outcomePrices": json.dumps(["0.6", "0.4"]),
            "clobTokenIds": json.dumps(["raw-yes", "raw-no"]),
            "volume": "11",
            "volume24hr": "2",
            "liquidity": "3",
            "active": True,
            "closed": False,
            "enableOrderBook": True,
            "negRisk": True,
        }
        response = polymarket_pb2.PolymarketMarketResponse(
            raw_json=json.dumps(payload),
            market_id="structured-market",
            end_date="not-a-date",
        )

        market = GatewayPolymarketClient._parse_market(response)

        assert market.id == "raw-market"
        assert market.condition_id == "raw-condition"
        assert market.clob_token_ids == ["raw-yes", "raw-no"]
        assert market.neg_risk is True

    def test_structured_response_preserves_all_fields(self) -> None:
        response = polymarket_pb2.PolymarketMarketResponse(
            market_id="market-id",
            condition_id="condition-id",
            question="Will it happen?",
            slug="market-slug",
            outcomes=["Yes", "No"],
            outcome_prices=["0.55", "0.45"],
            clob_token_ids=["yes-token", "no-token"],
            tokens=["legacy-yes", "legacy-no"],
            volume="100.25",
            volume_24hr="20.5",
            liquidity="75",
            end_date="2026-09-04T12:30:00Z",
            active=True,
            closed=False,
            enable_order_book=True,
            minimum_tick_size="0.001",
            minimum_order_size="10",
            maker_base_fee_bps="12",
            taker_base_fee_bps="34",
            best_bid="0.54",
            best_ask="0.56",
            last_trade_price="0.55",
            event_id="event-id",
            event_slug="event-slug",
            group_slug="group-slug",
            tags=["crypto", "weekly"],
        )

        market = GatewayPolymarketClient._parse_market(response)

        assert market.id == "market-id"
        assert market.condition_id == "condition-id"
        assert market.outcomes == ["Yes", "No"]
        assert market.outcome_prices == [Decimal("0.55"), Decimal("0.45")]
        assert market.clob_token_ids == ["yes-token", "no-token"]
        assert market.volume == Decimal("100.25")
        assert market.volume_24hr == Decimal("20.5")
        assert market.liquidity == Decimal("75")
        assert market.end_date == datetime(2026, 9, 4, 12, 30, tzinfo=UTC)
        assert market.order_price_min_tick_size == Decimal("0.001")
        assert market.order_min_size == Decimal("10")
        assert market.maker_base_fee_bps == 12
        assert market.taker_base_fee_bps == 34
        assert market.best_bid == Decimal("0.54")
        assert market.best_ask == Decimal("0.56")
        assert market.last_trade_price == Decimal("0.55")
        assert market.event_id == "event-id"
        assert market.event_slug == "event-slug"
        assert market.group_slug == "group-slug"
        assert market.tags == ["crypto", "weekly"]

    def test_structured_response_uses_legacy_identity_and_defaults(self) -> None:
        response = polymarket_pb2.PolymarketMarketResponse(
            condition_id="condition-only",
            outcomes=["Yes", "No"],
            outcome_prices=["0.5", "0.5"],
            tokens=["legacy-yes", "legacy-no"],
        )

        market = GatewayPolymarketClient._parse_market(response)

        assert market.id == "condition-only"
        assert market.clob_token_ids == ["legacy-yes", "legacy-no"]
        assert market.volume == Decimal("0")
        assert market.volume_24hr == Decimal("0")
        assert market.liquidity == Decimal("0")
        assert market.end_date is None
        assert market.order_price_min_tick_size == Decimal("0.01")
        assert market.order_min_size == Decimal("5")
        assert market.maker_base_fee_bps == 0
        assert market.taker_base_fee_bps == 0
        assert market.best_bid is None
        assert market.best_ask is None
        assert market.last_trade_price is None
        assert market.event_id is None
        assert market.event_slug is None
        assert market.group_slug is None
        assert market.tags == []

    def test_invalid_structured_end_date_still_raises(self) -> None:
        response = polymarket_pb2.PolymarketMarketResponse(
            condition_id="condition-id",
            outcomes=["Yes", "No"],
            outcome_prices=["0.5", "0.5"],
            end_date="not-a-date",
        )

        with pytest.raises(ValueError):
            GatewayPolymarketClient._parse_market(response)


class TestGetPositions:
    def test_request_and_position_normalization(self) -> None:
        stub = MagicMock()
        stub.GetPositions.return_value = _position_response()

        positions = _make_wrapper(stub).get_positions(wallet="0xCallerMustNotCrossBoundary")

        request = stub.GetPositions.call_args.args[0]
        assert isinstance(request, polymarket_pb2.PolymarketGetPositionsRequest)
        assert request.ListFields() == []
        assert stub.GetPositions.call_args.kwargs["timeout"] == 7.5
        assert len(positions) == 2
        assert positions[0].market_id == "market-a"
        assert positions[0].token_id == "token-a"
        assert positions[0].outcome == "NO"
        assert positions[0].size == Decimal("10.5")
        assert positions[0].avg_price == Decimal("0.25")
        assert positions[0].current_price == Decimal("0.5")
        assert positions[0].realized_pnl == Decimal("1.5")
        assert positions[0].market_question == "Question A?"
        assert positions[1].market_id == "condition-b"
        assert positions[1].token_id == "legacy-token-b"
        assert positions[1].outcome == "YES"
        assert positions[1].size == Decimal("0")

    @pytest.mark.parametrize(
        ("filters", "expected_market_ids"),
        [
            (PositionFilters(), ["market-a", "condition-b"]),
            (PositionFilters(market="market-a"), ["market-a"]),
            (PositionFilters(outcome="YES"), ["condition-b"]),
            (PositionFilters(market="market-a", outcome="NO"), ["market-a"]),
            (PositionFilters(market="market-a", outcome="YES"), []),
        ],
    )
    def test_filters_are_applied_locally(
        self,
        filters: PositionFilters,
        expected_market_ids: list[str],
    ) -> None:
        stub = MagicMock()
        stub.GetPositions.return_value = _position_response()

        positions = _make_wrapper(stub).get_positions(filters=filters)

        assert [position.market_id for position in positions] == expected_market_ids

    def test_application_error_is_preserved(self) -> None:
        stub = MagicMock()
        stub.GetPositions.return_value = polymarket_pb2.PolymarketPositionsResponse(
            success=False,
            error="wallet unavailable",
        )

        with pytest.raises(PolymarketAPIError, match="GetPositions failed: wallet unavailable"):
            _make_wrapper(stub).get_positions()

    def test_transport_error_is_preserved(self) -> None:
        stub = MagicMock()
        stub.GetPositions.side_effect = _FakeRpcError()

        with pytest.raises(PolymarketAPIError, match="GetPositions RPC failed: transport unavailable"):
            _make_wrapper(stub).get_positions()


class TestGetOpenOrders:
    def test_request_and_order_normalization(self) -> None:
        stub = MagicMock()
        stub.GetOpenOrders.return_value = polymarket_pb2.PolymarketOpenOrdersResponse(
            orders=[
                polymarket_pb2.PolymarketOpenOrder(
                    order_id="order-1",
                    market="0xAbC",
                    asset_id="ignored-asset-alias",
                    side="SELL",
                    price="0.4",
                    original_size="12",
                    size_matched="2",
                    status="LIVE",
                    expiration="1800000000",
                    created_at="2026-09-04T12:30:00Z",
                ),
                polymarket_pb2.PolymarketOpenOrder(
                    order_id="order-2",
                    market="other-market",
                    created_at="malformed",
                ),
            ],
            success=True,
        )
        filters = OrderFilters(market="0xAbC", status=OrderStatus.LIVE, limit=1)

        orders = _make_wrapper(stub).get_open_orders(filters)

        request = stub.GetOpenOrders.call_args.args[0]
        assert isinstance(request, polymarket_pb2.PolymarketGetOpenOrdersRequest)
        assert request.market_id == "0xAbC"
        assert request.asset_id == ""
        assert stub.GetOpenOrders.call_args.kwargs["timeout"] == 7.5
        assert len(orders) == 2
        assert orders[0].order_id == "order-1"
        assert orders[0].market == "0xAbC"
        assert orders[0].side == "SELL"
        assert orders[0].price == Decimal("0.4")
        assert orders[0].size == Decimal("12")
        assert orders[0].filled_size == Decimal("2")
        assert orders[0].created_at == datetime(2026, 9, 4, 12, 30, tzinfo=UTC)
        assert orders[0].expiration == 1800000000
        assert orders[1].price == Decimal("0")
        assert orders[1].size == Decimal("0")
        assert orders[1].filled_size == Decimal("0")
        assert orders[1].created_at is None
        assert orders[1].expiration is None

    @pytest.mark.parametrize("filters", [None, OrderFilters()])
    def test_missing_market_filter_sends_empty_request(self, filters: OrderFilters | None) -> None:
        stub = MagicMock()
        stub.GetOpenOrders.return_value = polymarket_pb2.PolymarketOpenOrdersResponse(success=True)

        assert _make_wrapper(stub).get_open_orders(filters) == []
        assert stub.GetOpenOrders.call_args.args[0].market_id == ""

    def test_malformed_expiration_still_raises(self) -> None:
        stub = MagicMock()
        stub.GetOpenOrders.return_value = polymarket_pb2.PolymarketOpenOrdersResponse(
            orders=[polymarket_pb2.PolymarketOpenOrder(expiration="not-an-integer")],
            success=True,
        )

        with pytest.raises(ValueError):
            _make_wrapper(stub).get_open_orders()

    def test_application_error_is_preserved(self) -> None:
        stub = MagicMock()
        stub.GetOpenOrders.return_value = polymarket_pb2.PolymarketOpenOrdersResponse(
            success=False,
            error="credentials expired",
        )

        with pytest.raises(PolymarketAPIError, match="GetOpenOrders failed: credentials expired"):
            _make_wrapper(stub).get_open_orders()

    def test_transport_error_is_preserved(self) -> None:
        stub = MagicMock()
        stub.GetOpenOrders.side_effect = _FakeRpcError()

        with pytest.raises(PolymarketAPIError, match="GetOpenOrders RPC failed: transport unavailable"):
            _make_wrapper(stub).get_open_orders()


class TestGetOrder:
    def test_request_and_order_normalization(self) -> None:
        stub = MagicMock()
        stub.GetOrder.return_value = polymarket_pb2.PolymarketOrderInfoResponse(
            order_id="Order-ID-Cased",
            market="0xMarket-ID",
            asset_id="ignored-asset-alias",
            side="BUY",
            price="0.61",
            original_size="9",
            size_matched="4",
            status="MATCHED",
            expiration="1900000000",
            created_at="2026-09-04T12:30:00Z",
            success=True,
        )

        order = _make_wrapper(stub).get_order("Order-ID-Cased")

        request = stub.GetOrder.call_args.args[0]
        assert isinstance(request, polymarket_pb2.PolymarketGetOrderRequest)
        assert request.order_id == "Order-ID-Cased"
        assert stub.GetOrder.call_args.kwargs["timeout"] == 7.5
        assert order is not None
        assert order.order_id == "Order-ID-Cased"
        assert order.market == "0xMarket-ID"
        assert order.side == "BUY"
        assert order.price == Decimal("0.61")
        assert order.size == Decimal("9")
        assert order.filled_size == Decimal("4")
        assert order.created_at == datetime(2026, 9, 4, 12, 30, tzinfo=UTC)
        assert order.expiration == 1900000000

    def test_empty_values_and_malformed_date_keep_defaults(self) -> None:
        stub = MagicMock()
        stub.GetOrder.return_value = polymarket_pb2.PolymarketOrderInfoResponse(
            order_id="order-empty",
            created_at="malformed",
            success=True,
        )

        order = _make_wrapper(stub).get_order("order-empty")

        assert order is not None
        assert order.price == Decimal("0")
        assert order.size == Decimal("0")
        assert order.filled_size == Decimal("0")
        assert order.created_at is None
        assert order.expiration is None

    @pytest.mark.parametrize("error", ["Order not found", "ORDER NOT FOUND in archive"])
    def test_not_found_returns_none(self, error: str) -> None:
        stub = MagicMock()
        stub.GetOrder.return_value = polymarket_pb2.PolymarketOrderInfoResponse(success=False, error=error)

        assert _make_wrapper(stub).get_order("missing-order") is None

    def test_other_application_error_is_preserved(self) -> None:
        stub = MagicMock()
        stub.GetOrder.return_value = polymarket_pb2.PolymarketOrderInfoResponse(
            success=False,
            error="credentials expired",
        )

        with pytest.raises(PolymarketAPIError, match="GetOrder failed: credentials expired"):
            _make_wrapper(stub).get_order("order-id")

    def test_transport_error_is_preserved(self) -> None:
        stub = MagicMock()
        stub.GetOrder.side_effect = _FakeRpcError()

        with pytest.raises(PolymarketAPIError, match="GetOrder RPC failed: transport unavailable"):
            _make_wrapper(stub).get_order("order-id")
