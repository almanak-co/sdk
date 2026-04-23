"""Gateway-backed Polymarket client for strategy/runtime code.

This wrapper presents the subset of ``ClobClient`` functionality used by the
framework while routing all networked operations through the gateway's
``PolymarketService`` gRPC API.
"""

from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING

import grpc

if TYPE_CHECKING:
    from ...data.prediction_provider import PositionFilters

from almanak.framework.gateway_client import GatewayClient
from almanak.gateway.proto import gateway_pb2

from .exceptions import PolymarketAPIError
from .models import GammaMarket, MarketFilters, OpenOrder, OrderBook, OrderFilters, OrderResponse, Position


class GatewayPolymarketClient:
    """Synchronous Polymarket client that talks only to the gateway."""

    def __init__(self, gateway_client: GatewayClient):
        if not gateway_client.is_connected:
            raise RuntimeError("Gateway client must be connected before creating GatewayPolymarketClient")
        self._gateway_client = gateway_client

    def close(self) -> None:
        """Compatibility no-op for callers that close direct CLOB clients."""
        return None

    def _rpc_timeout(self) -> float:
        return self._gateway_client.config.timeout

    @staticmethod
    def _raise_rpc_error(prefix: str, error: str | None, *, status_code: int | None = None) -> None:
        raise PolymarketAPIError(f"{prefix}: {error or 'unknown error'}", status_code=status_code)

    @staticmethod
    def _parse_market(response: gateway_pb2.PolymarketMarketResponse) -> GammaMarket:
        raw_payload = response.raw_json.strip()
        if raw_payload:
            return GammaMarket.from_api_response(json.loads(raw_payload))

        end_date = None
        if response.end_date:
            end_date = datetime.fromisoformat(response.end_date.replace("Z", "+00:00"))

        return GammaMarket(
            id=response.market_id or response.condition_id,
            condition_id=response.condition_id,
            question=response.question,
            slug=response.slug,
            outcomes=list(response.outcomes),
            outcome_prices=[Decimal(value) for value in response.outcome_prices],
            clob_token_ids=list(response.clob_token_ids or response.tokens),
            volume=Decimal(response.volume or "0"),
            volume_24hr=Decimal(response.volume_24hr or "0"),
            liquidity=Decimal(response.liquidity or "0"),
            end_date=end_date,
            active=response.active,
            closed=response.closed,
            enable_order_book=response.enable_order_book,
            order_price_min_tick_size=Decimal(response.minimum_tick_size or "0.01"),
            order_min_size=Decimal(response.minimum_order_size or "5"),
            maker_base_fee_bps=int(response.maker_base_fee_bps or 0),
            taker_base_fee_bps=int(response.taker_base_fee_bps or 0),
            best_bid=Decimal(response.best_bid) if response.best_bid else None,
            best_ask=Decimal(response.best_ask) if response.best_ask else None,
            last_trade_price=Decimal(response.last_trade_price) if response.last_trade_price else None,
            event_id=response.event_id or None,
            event_slug=response.event_slug or None,
            group_slug=response.group_slug or None,
            tags=list(response.tags),
        )

    def get_market(self, market_id: str) -> GammaMarket:
        try:
            response = self._gateway_client.polymarket.GetMarket(
                gateway_pb2.PolymarketGetMarketRequest(condition_id=market_id),
                timeout=self._rpc_timeout(),
            )
        except grpc.RpcError as exc:
            self._raise_rpc_error("GetMarket RPC failed", exc.details(), status_code=None)
        if not response.success:
            self._raise_rpc_error("GetMarket failed", response.error)
        return self._parse_market(response)

    def get_market_by_slug(self, slug: str) -> GammaMarket | None:
        try:
            response = self._gateway_client.polymarket.GetMarket(
                gateway_pb2.PolymarketGetMarketRequest(slug=slug),
                timeout=self._rpc_timeout(),
            )
        except grpc.RpcError as exc:
            self._raise_rpc_error("GetMarketBySlug RPC failed", exc.details(), status_code=None)
        if not response.success:
            if "not found" in (response.error or "").lower():
                return None
            self._raise_rpc_error("GetMarketBySlug failed", response.error)
        return self._parse_market(response)

    def get_markets(self, filters: MarketFilters | None = None) -> list[GammaMarket]:
        filters_json = filters.model_dump_json(exclude_none=True) if filters else ""
        try:
            response = self._gateway_client.polymarket.GetMarkets(
                gateway_pb2.PolymarketGetMarketsRequest(filters_json=filters_json),
                timeout=self._rpc_timeout(),
            )
        except grpc.RpcError as exc:
            self._raise_rpc_error("GetMarkets RPC failed", exc.details(), status_code=None)
        if not response.success:
            self._raise_rpc_error("GetMarkets failed", response.error)
        return [self._parse_market(item) for item in response.markets]

    def get_orderbook(self, token_id: str) -> OrderBook:
        try:
            response = self._gateway_client.polymarket.GetOrderBook(
                gateway_pb2.PolymarketOrderBookRequest(token_id=token_id),
                timeout=self._rpc_timeout(),
            )
        except grpc.RpcError as exc:
            self._raise_rpc_error("GetOrderBook RPC failed", exc.details(), status_code=None)
        if not response.success:
            self._raise_rpc_error("GetOrderBook failed", response.error)
        return OrderBook.from_api_response(
            {
                "market": response.market,
                "asset_id": response.asset_id,
                "hash": response.hash,
                "bids": [{"price": level.price, "size": level.size} for level in response.bids],
                "asks": [{"price": level.price, "size": level.size} for level in response.asks],
            }
        )

    def create_and_post_order(
        self,
        *,
        token_id: str,
        price: Decimal,
        size: Decimal,
        side: str,
        time_in_force: str = "GTC",
        expiration: int = 0,
        fee_rate_bps: str = "0",
    ) -> OrderResponse:
        try:
            response = self._gateway_client.polymarket.CreateAndPostOrder(
                gateway_pb2.PolymarketCreateOrderRequest(
                    token_id=token_id,
                    price=str(price),
                    size=str(size),
                    side=side,
                    fee_rate_bps=fee_rate_bps,
                    expiration=expiration,
                    time_in_force=time_in_force,
                ),
                timeout=self._rpc_timeout(),
            )
        except grpc.RpcError as exc:
            self._raise_rpc_error("CreateAndPostOrder RPC failed", exc.details(), status_code=None)
        if not response.success:
            self._raise_rpc_error("CreateAndPostOrder failed", response.error)
        return OrderResponse.from_api_response(
            {
                "orderID": response.order_id,
                "status": response.status,
                "price": response.price,
                "size": response.size,
                "filledSize": response.size_matched,
                "avgPrice": response.avg_fill_price,
                "createdAt": response.created_at,
            }
        )

    def get_positions(
        self,
        wallet: str | None = None,
        filters: PositionFilters | None = None,
    ) -> list[Position]:
        """Return positions for the gateway-managed wallet.

        The ``wallet`` parameter is accepted for API compatibility with
        ``ClobClient.get_positions`` but is ignored — the gateway derives
        the wallet from its own configuration.
        """
        try:
            response = self._gateway_client.polymarket.GetPositions(
                gateway_pb2.PolymarketGetPositionsRequest(),
                timeout=self._rpc_timeout(),
            )
        except grpc.RpcError as exc:
            self._raise_rpc_error("GetPositions RPC failed", exc.details(), status_code=None)
        if not response.success:
            self._raise_rpc_error("GetPositions failed", response.error)
        positions: list[Position] = []
        for item in response.positions:
            positions.append(
                Position(
                    market_id=item.market_id or item.condition_id,
                    condition_id=item.condition_id,
                    token_id=item.token_id or item.asset,
                    outcome=item.outcome or "YES",
                    size=Decimal(item.size or "0"),
                    avg_price=Decimal(item.avg_price or "0"),
                    current_price=Decimal(item.cur_price or "0"),
                    realized_pnl=Decimal(item.realized_pnl or "0"),
                    market_question=item.market_question,
                )
            )
        if filters is not None:
            if getattr(filters, "market", None):
                positions = [p for p in positions if p.market_id == filters.market]
            if getattr(filters, "outcome", None):
                positions = [p for p in positions if p.outcome == filters.outcome]
        return positions

    def get_open_orders(self, filters: OrderFilters | None = None) -> list[OpenOrder]:
        request = gateway_pb2.PolymarketGetOpenOrdersRequest(
            market_id=filters.market if filters and filters.market else "",
        )
        try:
            response = self._gateway_client.polymarket.GetOpenOrders(request, timeout=self._rpc_timeout())
        except grpc.RpcError as exc:
            self._raise_rpc_error("GetOpenOrders RPC failed", exc.details(), status_code=None)
        if not response.success:
            self._raise_rpc_error("GetOpenOrders failed", response.error)

        orders: list[OpenOrder] = []
        for item in response.orders:
            created_at = None
            if item.created_at:
                try:
                    created_at = datetime.fromisoformat(item.created_at.replace("Z", "+00:00"))
                except ValueError:
                    created_at = None
            orders.append(
                OpenOrder(
                    order_id=item.order_id,
                    market=item.market,
                    side=item.side,
                    price=Decimal(item.price or "0"),
                    size=Decimal(item.original_size or "0"),
                    filled_size=Decimal(item.size_matched or "0"),
                    created_at=created_at,
                    expiration=int(item.expiration) if item.expiration else None,
                )
            )
        return orders

    def get_order(self, order_id: str) -> OpenOrder | None:
        try:
            response = self._gateway_client.polymarket.GetOrder(
                gateway_pb2.PolymarketGetOrderRequest(order_id=order_id),
                timeout=self._rpc_timeout(),
            )
        except grpc.RpcError as exc:
            self._raise_rpc_error("GetOrder RPC failed", exc.details(), status_code=None)
        if not response.success:
            if "not found" in (response.error or "").lower():
                return None
            self._raise_rpc_error("GetOrder failed", response.error)
        created_at = None
        if response.created_at:
            try:
                created_at = datetime.fromisoformat(response.created_at.replace("Z", "+00:00"))
            except ValueError:
                created_at = None
        return OpenOrder(
            order_id=response.order_id,
            market=response.market,
            side=response.side,
            price=Decimal(response.price or "0"),
            size=Decimal(response.original_size or "0"),
            filled_size=Decimal(response.size_matched or "0"),
            created_at=created_at,
            expiration=int(response.expiration) if response.expiration else None,
        )

    def get_price_history(self, *args: object, **kwargs: object) -> object:
        raise NotImplementedError(
            "get_price_history is not yet supported via the gateway. "
            "File a ticket to add GetPriceHistory to PolymarketService in gateway.proto."
        )

    def get_trade_tape(self, *args: object, **kwargs: object) -> object:
        raise NotImplementedError(
            "get_trade_tape is not yet supported via the gateway. "
            "File a ticket to add GetTrades to PolymarketService in gateway.proto."
        )

    def cancel_order(self, order_id: str) -> bool:
        try:
            response = self._gateway_client.polymarket.CancelOrder(
                gateway_pb2.PolymarketCancelOrderRequest(order_id=order_id),
                timeout=self._rpc_timeout(),
            )
        except grpc.RpcError as exc:
            self._raise_rpc_error("CancelOrder RPC failed", exc.details(), status_code=None)
        if not response.success:
            self._raise_rpc_error("CancelOrder failed", response.error)
        return True
