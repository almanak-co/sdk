"""Unit tests for the 10 wrapper methods added in VIB-3698.

Each gateway RPC was already implemented server-side but reachable only via
raw ``gateway_pb2`` stubs. This suite locks in the typed wrapper surface on
``GatewayPolymarketClient`` so callers (prediction_provider, strategies)
can adopt the methods without shape changes.

Pattern (mirroring ``test_gateway_client_create_and_post_order.py``):
    1. Build a real ``GatewayPolymarketClient`` over a fake gateway with a
       mocked Polymarket gRPC stub — no top-level mock of the wrapper, so
       the wrapper code path actually executes.
    2. Inject a populated proto response on the stub.
    3. Call the wrapper, assert (a) request shape on the wire and (b)
       parsed return value.
    4. For each method, add ONE failure-path test asserting
       ``PolymarketAPIError`` is raised when ``response.success=False``.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.framework.connectors.polymarket.exceptions import PolymarketAPIError
from almanak.framework.connectors.polymarket.gateway_client import (
    GatewayPolymarketClient,
)
from almanak.framework.connectors.polymarket.models import (
    BalanceAllowance,
    OrderResponse,
    OrderStatus,
    SimplifiedMarket,
    Trade,
    TradeFilters,
    TradeStatus,
)
from almanak.framework.gateway_client import GatewayClient, GatewayClientConfig
from almanak.connectors.polymarket.proto import polymarket_pb2

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_gateway_client(stub: MagicMock) -> GatewayClient:
    """Real ``GatewayClient`` with a pre-injected polymarket stub.

    Bypasses ``connect()`` (which would open a real gRPC channel) by setting
    the connection-state attributes directly. The wrapper only reads
    ``is_connected`` (during construction) and ``polymarket`` (per call) and
    ``config.timeout`` (per call).
    """
    client = GatewayClient(config=GatewayClientConfig(host="localhost", port=50051, timeout=5.0))
    client._connected = True
    client._channel = MagicMock()
    client._polymarket_stub = stub
    return client


def _make_wrapper(stub: MagicMock) -> GatewayPolymarketClient:
    return GatewayPolymarketClient(_make_gateway_client(stub))


# ---------------------------------------------------------------------------
# get_simplified_markets
# ---------------------------------------------------------------------------


class TestGetSimplifiedMarkets:
    def test_returns_markets_and_cursor(self) -> None:
        stub = MagicMock()
        stub.GetSimplifiedMarkets.return_value = polymarket_pb2.PolymarketSimplifiedMarketsResponse(
            markets=[
                polymarket_pb2.PolymarketSimplifiedMarket(
                    condition_id="0xabc",
                    tokens=["111", "222"],
                    min_incentive_size="10",
                    max_incentive_spread="0.05",
                    active=True,
                    closed=False,
                ),
                polymarket_pb2.PolymarketSimplifiedMarket(
                    condition_id="0xdef",
                    tokens=["333"],
                    min_incentive_size="0",
                    max_incentive_spread="0",
                    active=False,
                    closed=True,
                ),
            ],
            next_cursor="next-page-token",
            success=True,
        )

        wrapper = _make_wrapper(stub)
        markets, cursor = wrapper.get_simplified_markets(next_cursor="prev-token")

        # Wire shape
        request = stub.GetSimplifiedMarkets.call_args.args[0]
        assert isinstance(request, polymarket_pb2.PolymarketGetSimplifiedMarketsRequest)
        assert request.next_cursor == "prev-token"

        # Parsed result
        assert cursor == "next-page-token"
        assert len(markets) == 2
        assert isinstance(markets[0], SimplifiedMarket)
        assert markets[0].condition_id == "0xabc"
        assert markets[0].tokens == ["111", "222"]
        assert markets[0].min_incentive_size == Decimal("10")
        assert markets[0].max_incentive_spread == Decimal("0.05")
        assert markets[0].active is True
        assert markets[0].closed is False
        assert markets[1].active is False
        assert markets[1].closed is True

    def test_default_cursor_is_empty_string(self) -> None:
        """Calling without ``next_cursor`` must send the empty-string default."""
        stub = MagicMock()
        stub.GetSimplifiedMarkets.return_value = polymarket_pb2.PolymarketSimplifiedMarketsResponse(
            markets=[], next_cursor="", success=True
        )
        wrapper = _make_wrapper(stub)
        markets, cursor = wrapper.get_simplified_markets()
        request = stub.GetSimplifiedMarkets.call_args.args[0]
        assert request.next_cursor == ""
        assert markets == []
        assert cursor == ""

    def test_failure_raises_polymarket_api_error(self) -> None:
        stub = MagicMock()
        stub.GetSimplifiedMarkets.return_value = polymarket_pb2.PolymarketSimplifiedMarketsResponse(
            success=False, error="upstream 500"
        )
        wrapper = _make_wrapper(stub)
        with pytest.raises(PolymarketAPIError, match="GetSimplifiedMarkets failed"):
            wrapper.get_simplified_markets()


# ---------------------------------------------------------------------------
# get_midpoint
# ---------------------------------------------------------------------------


class TestGetMidpoint:
    def test_returns_decimal_midpoint(self) -> None:
        stub = MagicMock()
        stub.GetMidpoint.return_value = polymarket_pb2.PolymarketMidpointResponse(
            midpoint="0.5234", success=True
        )
        wrapper = _make_wrapper(stub)
        result = wrapper.get_midpoint("token-xyz")

        request = stub.GetMidpoint.call_args.args[0]
        assert isinstance(request, polymarket_pb2.PolymarketMidpointRequest)
        assert request.token_id == "token-xyz"
        assert result == Decimal("0.5234")
        assert isinstance(result, Decimal)

    def test_failure_raises_polymarket_api_error(self) -> None:
        stub = MagicMock()
        stub.GetMidpoint.return_value = polymarket_pb2.PolymarketMidpointResponse(
            success=False, error="midpoint unavailable"
        )
        wrapper = _make_wrapper(stub)
        with pytest.raises(PolymarketAPIError, match="GetMidpoint failed"):
            wrapper.get_midpoint("token-xyz")


# ---------------------------------------------------------------------------
# get_price
# ---------------------------------------------------------------------------


class TestGetPrice:
    def test_returns_decimal_price_for_buy(self) -> None:
        stub = MagicMock()
        stub.GetPrice.return_value = polymarket_pb2.PolymarketPriceResponse(
            price="0.61", success=True
        )
        wrapper = _make_wrapper(stub)
        result = wrapper.get_price("token-xyz", "BUY")

        request = stub.GetPrice.call_args.args[0]
        assert isinstance(request, polymarket_pb2.PolymarketPriceRequest)
        assert request.token_id == "token-xyz"
        assert request.side == "BUY"
        assert result == Decimal("0.61")

    def test_returns_decimal_price_for_sell(self) -> None:
        stub = MagicMock()
        stub.GetPrice.return_value = polymarket_pb2.PolymarketPriceResponse(
            price="0.39", success=True
        )
        wrapper = _make_wrapper(stub)
        result = wrapper.get_price("token-xyz", "SELL")
        request = stub.GetPrice.call_args.args[0]
        assert request.side == "SELL"
        assert result == Decimal("0.39")

    def test_invalid_side_raises_value_error_before_rpc(self) -> None:
        """Pre-RPC validation: a typo must not consume a network round-trip."""
        stub = MagicMock()
        wrapper = _make_wrapper(stub)
        with pytest.raises(ValueError, match="must be 'BUY' or 'SELL'"):
            wrapper.get_price("token-xyz", "BID")
        # The stub MUST NOT have been called.
        stub.GetPrice.assert_not_called()

    def test_lowercase_side_normalized_to_uppercase(self) -> None:
        """``buy`` is ambiguous on the wire — normalize before sending."""
        stub = MagicMock()
        stub.GetPrice.return_value = polymarket_pb2.PolymarketPriceResponse(
            price="0.5", success=True
        )
        wrapper = _make_wrapper(stub)
        wrapper.get_price("token-xyz", "buy")
        request = stub.GetPrice.call_args.args[0]
        assert request.side == "BUY"

    def test_failure_raises_polymarket_api_error(self) -> None:
        stub = MagicMock()
        stub.GetPrice.return_value = polymarket_pb2.PolymarketPriceResponse(
            success=False, error="no liquidity"
        )
        wrapper = _make_wrapper(stub)
        with pytest.raises(PolymarketAPIError, match="GetPrice failed"):
            wrapper.get_price("token-xyz", "BUY")


# ---------------------------------------------------------------------------
# get_spread
# ---------------------------------------------------------------------------


class TestGetSpread:
    def test_returns_decimal_spread(self) -> None:
        stub = MagicMock()
        stub.GetSpread.return_value = polymarket_pb2.PolymarketSpreadResponse(
            spread="0.02", success=True
        )
        wrapper = _make_wrapper(stub)
        result = wrapper.get_spread("token-xyz")

        request = stub.GetSpread.call_args.args[0]
        assert isinstance(request, polymarket_pb2.PolymarketSpreadRequest)
        assert request.token_id == "token-xyz"
        assert result == Decimal("0.02")

    def test_failure_raises_polymarket_api_error(self) -> None:
        stub = MagicMock()
        stub.GetSpread.return_value = polymarket_pb2.PolymarketSpreadResponse(
            success=False, error="spread unavailable"
        )
        wrapper = _make_wrapper(stub)
        with pytest.raises(PolymarketAPIError, match="GetSpread failed"):
            wrapper.get_spread("token-xyz")


# ---------------------------------------------------------------------------
# get_tick_size
# ---------------------------------------------------------------------------


class TestGetTickSize:
    def test_returns_decimal_tick_size(self) -> None:
        stub = MagicMock()
        stub.GetTickSize.return_value = polymarket_pb2.PolymarketTickSizeResponse(
            tick_size="0.001", success=True
        )
        wrapper = _make_wrapper(stub)
        result = wrapper.get_tick_size("token-xyz")

        request = stub.GetTickSize.call_args.args[0]
        assert isinstance(request, polymarket_pb2.PolymarketTickSizeRequest)
        assert request.token_id == "token-xyz"
        assert result == Decimal("0.001")

    def test_failure_raises_polymarket_api_error(self) -> None:
        stub = MagicMock()
        stub.GetTickSize.return_value = polymarket_pb2.PolymarketTickSizeResponse(
            success=False, error="not found"
        )
        wrapper = _make_wrapper(stub)
        with pytest.raises(PolymarketAPIError, match="GetTickSize failed"):
            wrapper.get_tick_size("token-xyz")


# ---------------------------------------------------------------------------
# get_balance_allowance
# ---------------------------------------------------------------------------


class TestGetBalanceAllowance:
    def test_returns_balance_allowance_for_collateral(self) -> None:
        stub = MagicMock()
        stub.GetBalanceAllowance.return_value = polymarket_pb2.PolymarketBalanceAllowanceResponse(
            balance="100.50", allowance="500.00", success=True
        )
        wrapper = _make_wrapper(stub)
        result = wrapper.get_balance_allowance()

        # Default asset_type should be COLLATERAL with empty token_id.
        request = stub.GetBalanceAllowance.call_args.args[0]
        assert isinstance(request, polymarket_pb2.PolymarketBalanceAllowanceRequest)
        assert request.asset_type == "COLLATERAL"
        assert request.token_id == ""

        assert isinstance(result, BalanceAllowance)
        assert result.balance == Decimal("100.50")
        assert result.allowance == Decimal("500.00")

    def test_returns_balance_allowance_for_conditional_with_token_id(self) -> None:
        stub = MagicMock()
        stub.GetBalanceAllowance.return_value = polymarket_pb2.PolymarketBalanceAllowanceResponse(
            balance="42", allowance="0", success=True
        )
        wrapper = _make_wrapper(stub)
        result = wrapper.get_balance_allowance(asset_type="CONDITIONAL", token_id="token-abc")

        request = stub.GetBalanceAllowance.call_args.args[0]
        assert request.asset_type == "CONDITIONAL"
        assert request.token_id == "token-abc"
        assert result.balance == Decimal("42")
        assert result.allowance == Decimal("0")

    def test_failure_raises_polymarket_api_error(self) -> None:
        stub = MagicMock()
        stub.GetBalanceAllowance.return_value = polymarket_pb2.PolymarketBalanceAllowanceResponse(
            success=False, error="auth failed"
        )
        wrapper = _make_wrapper(stub)
        with pytest.raises(PolymarketAPIError, match="GetBalanceAllowance failed"):
            wrapper.get_balance_allowance()


# ---------------------------------------------------------------------------
# get_trades
# ---------------------------------------------------------------------------


class TestGetTrades:
    def test_returns_trades_with_filters(self) -> None:
        stub = MagicMock()
        stub.GetTradesHistory.return_value = polymarket_pb2.PolymarketTradesResponse(
            trades=[
                polymarket_pb2.PolymarketTrade(
                    trade_id="trade-1",
                    market="0xmarket",
                    asset_id="111",
                    side="BUY",
                    price="0.55",
                    size="20",
                    fee_rate_bps="0",
                    status="MATCHED",
                    match_time="1745930000",
                    transaction_hash="0xtx",
                ),
                polymarket_pb2.PolymarketTrade(
                    trade_id="trade-2",
                    market="0xmarket",
                    asset_id="222",
                    side="SELL",
                    price="0.45",
                    size="10",
                    fee_rate_bps="0",
                    status="CONFIRMED",
                    match_time="2026-04-29T10:00:00Z",
                ),
            ],
            success=True,
        )

        wrapper = _make_wrapper(stub)
        filters = TradeFilters(market="0xmarket", limit=50)
        results = wrapper.get_trades(filters)

        request = stub.GetTradesHistory.call_args.args[0]
        assert isinstance(request, polymarket_pb2.PolymarketGetTradesRequest)
        assert request.market_id == "0xmarket"
        assert request.limit == 50

        assert len(results) == 2
        assert all(isinstance(t, Trade) for t in results)
        assert results[0].id == "trade-1"
        assert results[0].market_id == "0xmarket"
        assert results[0].token_id == "111"
        assert results[0].side == "BUY"
        assert results[0].price == Decimal("0.55")
        assert results[0].size == Decimal("20")
        assert results[0].status == TradeStatus.MATCHED
        # Unix-second match_time gets parsed.
        assert results[0].timestamp.timestamp() == 1745930000.0
        # ISO match_time gets parsed too.
        assert results[1].id == "trade-2"
        assert results[1].status == TradeStatus.CONFIRMED
        assert results[1].timestamp.year == 2026

    def test_no_filters_sends_empty_request(self) -> None:
        stub = MagicMock()
        stub.GetTradesHistory.return_value = polymarket_pb2.PolymarketTradesResponse(success=True)
        wrapper = _make_wrapper(stub)
        results = wrapper.get_trades()
        request = stub.GetTradesHistory.call_args.args[0]
        assert request.market_id == ""
        assert request.limit == 0
        assert results == []

    def test_failure_raises_polymarket_api_error(self) -> None:
        stub = MagicMock()
        stub.GetTradesHistory.return_value = polymarket_pb2.PolymarketTradesResponse(
            success=False, error="auth required"
        )
        wrapper = _make_wrapper(stub)
        with pytest.raises(PolymarketAPIError, match="GetTradesHistory failed"):
            wrapper.get_trades()


# ---------------------------------------------------------------------------
# cancel_orders
# ---------------------------------------------------------------------------


class TestCancelOrders:
    def test_returns_canceled_and_not_canceled_lists(self) -> None:
        """Partial-success response: caller must see both populated lists so
        retries don't double-cancel the successes."""
        stub = MagicMock()
        stub.CancelOrders.return_value = polymarket_pb2.PolymarketCancelResponse(
            canceled=["ord-1", "ord-2"],
            not_canceled=["ord-3"],
            # Partial failure: success=False on the wire, but the wrapper
            # surfaces both lists rather than raising.
            success=False,
        )
        wrapper = _make_wrapper(stub)
        result = wrapper.cancel_orders(["ord-1", "ord-2", "ord-3"])

        request = stub.CancelOrders.call_args.args[0]
        assert isinstance(request, polymarket_pb2.PolymarketCancelOrdersRequest)
        assert list(request.order_ids) == ["ord-1", "ord-2", "ord-3"]

        assert result == {
            "canceled": ["ord-1", "ord-2"],
            "not_canceled": ["ord-3"],
        }

    def test_full_success_returns_empty_not_canceled(self) -> None:
        stub = MagicMock()
        stub.CancelOrders.return_value = polymarket_pb2.PolymarketCancelResponse(
            canceled=["ord-1", "ord-2"],
            not_canceled=[],
            success=True,
        )
        wrapper = _make_wrapper(stub)
        result = wrapper.cancel_orders(["ord-1", "ord-2"])
        assert result == {"canceled": ["ord-1", "ord-2"], "not_canceled": []}

    def test_grpc_error_raises_polymarket_api_error(self) -> None:
        """``cancel_orders`` returns the partial-success shape rather than
        raising on ``success=False`` — but a transport-layer gRPC error is
        still a hard failure that must surface as ``PolymarketAPIError``."""
        import grpc

        stub = MagicMock()
        rpc_error = grpc.RpcError("boom")
        rpc_error.details = lambda: "transport boom"  # type: ignore[method-assign]
        stub.CancelOrders.side_effect = rpc_error
        wrapper = _make_wrapper(stub)
        with pytest.raises(PolymarketAPIError, match="CancelOrders RPC failed"):
            wrapper.cancel_orders(["ord-1"])


# ---------------------------------------------------------------------------
# cancel_all_orders
# ---------------------------------------------------------------------------


class TestCancelAllOrders:
    def test_returns_list_of_canceled_ids(self) -> None:
        stub = MagicMock()
        stub.CancelAll.return_value = polymarket_pb2.PolymarketCancelResponse(
            canceled=["ord-1", "ord-2", "ord-3"],
            not_canceled=[],
            success=True,
        )
        wrapper = _make_wrapper(stub)
        result = wrapper.cancel_all_orders(market_id="0xmkt", asset_id="111")

        request = stub.CancelAll.call_args.args[0]
        assert isinstance(request, polymarket_pb2.PolymarketCancelAllRequest)
        assert request.market_id == "0xmkt"
        assert request.asset_id == "111"
        assert result == ["ord-1", "ord-2", "ord-3"]

    def test_no_filters_sends_empty_strings(self) -> None:
        stub = MagicMock()
        stub.CancelAll.return_value = polymarket_pb2.PolymarketCancelResponse(
            canceled=[], success=True
        )
        wrapper = _make_wrapper(stub)
        result = wrapper.cancel_all_orders()
        request = stub.CancelAll.call_args.args[0]
        assert request.market_id == ""
        assert request.asset_id == ""
        assert result == []

    def test_partial_response_shape_returns_canceled_subset(self) -> None:
        """When CancelAll succeeds for a subset, the wrapper returns the
        canceled subset (not_canceled is informational on the wire)."""
        stub = MagicMock()
        stub.CancelAll.return_value = polymarket_pb2.PolymarketCancelResponse(
            canceled=["ord-1"],
            not_canceled=["ord-2"],
            success=True,
        )
        wrapper = _make_wrapper(stub)
        result = wrapper.cancel_all_orders()
        assert result == ["ord-1"]

    def test_failure_raises_polymarket_api_error(self) -> None:
        stub = MagicMock()
        stub.CancelAll.return_value = polymarket_pb2.PolymarketCancelResponse(
            success=False, error="auth required"
        )
        wrapper = _make_wrapper(stub)
        with pytest.raises(PolymarketAPIError, match="CancelAll failed"):
            wrapper.cancel_all_orders()


# ---------------------------------------------------------------------------
# create_and_post_market_order
# ---------------------------------------------------------------------------


class TestCreateAndPostMarketOrder:
    def test_buy_market_order_sends_amount_as_usdc(self) -> None:
        """For BUY, ``amount`` is USDC notional — wrapper sends it verbatim."""
        stub = MagicMock()
        stub.CreateAndPostMarketOrder.return_value = polymarket_pb2.PolymarketOrderResponse(
            order_id="ord-buy-1",
            status="MATCHED",
            size_matched="20",
            price="0.5",
            size="20",
            avg_fill_price="0.5",
            created_at="2026-04-29T00:00:00Z",
            success=True,
        )
        wrapper = _make_wrapper(stub)
        result = wrapper.create_and_post_market_order(
            token_id="111",
            amount=Decimal("10"),  # $10 USDC
            side="BUY",
            worst_price=Decimal("0.6"),
            expiration=1745930000,
        )

        request = stub.CreateAndPostMarketOrder.call_args.args[0]
        assert isinstance(request, polymarket_pb2.PolymarketMarketOrderRequest)
        assert request.token_id == "111"
        assert request.amount == "10"
        assert request.side == "BUY"
        assert request.worst_price == "0.6"
        assert request.expiration == 1745930000

        assert isinstance(result, OrderResponse)
        assert result.order_id == "ord-buy-1"
        assert result.status == OrderStatus.MATCHED

    def test_sell_market_order_sends_amount_as_token_size(self) -> None:
        """For SELL, ``amount`` is token (share) size — wrapper sends verbatim."""
        stub = MagicMock()
        stub.CreateAndPostMarketOrder.return_value = polymarket_pb2.PolymarketOrderResponse(
            order_id="ord-sell-1",
            status="MATCHED",
            size_matched="5",
            price="0.45",
            size="5",
            avg_fill_price="0.45",
            success=True,
        )
        wrapper = _make_wrapper(stub)
        result = wrapper.create_and_post_market_order(
            token_id="222",
            amount=Decimal("5"),  # 5 shares
            side="SELL",
        )

        request = stub.CreateAndPostMarketOrder.call_args.args[0]
        assert request.token_id == "222"
        assert request.amount == "5"
        assert request.side == "SELL"
        # Defaults
        assert request.worst_price == ""
        assert request.expiration == 0

        assert result.order_id == "ord-sell-1"

    def test_market_kwarg_accepted_and_not_serialized(self) -> None:
        """``market=`` kwarg is accepted for ClobClient signature parity but
        must not be serialized into the wire request — gateway re-fetches."""
        from types import SimpleNamespace

        stub = MagicMock()
        stub.CreateAndPostMarketOrder.return_value = polymarket_pb2.PolymarketOrderResponse(
            order_id="ord-x",
            status="LIVE",
            size_matched="0",
            price="0.5",
            size="1",
            success=True,
        )
        wrapper = _make_wrapper(stub)
        fake_market = SimpleNamespace(id="mkt", _unserializable=object())
        wrapper.create_and_post_market_order(
            token_id="111",
            amount=Decimal("1"),
            side="BUY",
            market=fake_market,
        )
        request = stub.CreateAndPostMarketOrder.call_args.args[0]
        for field, _value in request.ListFields():
            assert field.name != "market"

    def test_failure_raises_polymarket_api_error(self) -> None:
        stub = MagicMock()
        stub.CreateAndPostMarketOrder.return_value = polymarket_pb2.PolymarketOrderResponse(
            success=False, error="insufficient balance"
        )
        wrapper = _make_wrapper(stub)
        with pytest.raises(PolymarketAPIError, match="CreateAndPostMarketOrder failed"):
            wrapper.create_and_post_market_order(
                token_id="111",
                amount=Decimal("1"),
                side="BUY",
            )
