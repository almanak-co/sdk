"""Gateway-backed Polymarket client for strategy/runtime code.

This wrapper presents the subset of ``ClobClient`` functionality used by the
framework while routing all networked operations through the gateway's
``PolymarketService`` gRPC API.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import grpc

if TYPE_CHECKING:
    from ...data.prediction_provider import PositionFilters

from almanak.framework.gateway_client import GatewayClient
from almanak.gateway.proto import gateway_pb2

from .exceptions import PolymarketAPIError
from .models import (
    BalanceAllowance,
    GammaMarket,
    HistoricalPrice,
    HistoricalTrade,
    MarketFilters,
    OpenOrder,
    OrderBook,
    OrderFilters,
    OrderResponse,
    Position,
    PriceHistory,
    PriceHistoryInterval,
    SimplifiedMarket,
    Trade,
    TradeFilters,
    TradeStatus,
)


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
        # ``market`` accepted for ClobClient signature parity (V2 added it for
        # neg-risk routing + tick validation); ignored here because the gateway
        # re-fetches the market server-side from ``token_id`` (see
        # ``polymarket_service.CreateAndPostOrder``).
        market: Any | None = None,  # noqa: ARG002
    ) -> OrderResponse:
        # V2: ``fee_rate_bps`` removed — fees are operator-set at match time
        # and are not part of the signed order. ``expiration`` is now an
        # API-level GTD timestamp (Unix seconds), not the signed-struct field.
        try:
            response = self._gateway_client.polymarket.CreateAndPostOrder(
                gateway_pb2.PolymarketCreateOrderRequest(
                    token_id=token_id,
                    price=str(price),
                    size=str(size),
                    side=side,
                    expiration=expiration,
                    time_in_force=time_in_force,
                ),
                timeout=self._rpc_timeout(),
            )
        except grpc.RpcError as exc:
            self._raise_rpc_error("CreateAndPostOrder RPC failed", exc.details(), status_code=None)
        if not response.success:
            self._raise_rpc_error("CreateAndPostOrder failed", response.error)
        # VIB-3710: surface gateway-side setup transactions (approvals + wrap)
        # and operator fee_pusd through the OrderResponse model. Gateways
        # older than VIB-3710 omit both fields — proto's default values
        # (empty repeated, empty string) flow through cleanly to None / [].
        # CodeRabbit thread 5: project the caller-known ``side`` and
        # ``token_id`` into the dict — ``OrderResponse.from_api_response``
        # otherwise defaults ``side="BUY"`` and ``market=""`` (see
        # ``models.py``), silently deserializing every SELL response as a BUY.
        return OrderResponse.from_api_response(
            {
                "orderID": response.order_id,
                "status": response.status,
                "side": side,
                "market": token_id,
                "price": response.price,
                "size": response.size,
                "filledSize": response.size_matched,
                "avgPrice": response.avg_fill_price,
                "createdAt": response.created_at,
                "setup_txs": [
                    {
                        "tx_hash": tx.tx_hash,
                        "description": tx.description,
                        "gas_used": tx.gas_used,
                        "gas_price_wei": tx.gas_price_wei,
                        "total_cost_wei": tx.total_cost_wei,
                    }
                    for tx in response.setup_txs
                ],
                "fee_pusd": response.fee_pusd or None,
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

    def get_price_history(
        self,
        token_id: str,
        interval: str | PriceHistoryInterval | None = None,
        start_ts: int | None = None,
        end_ts: int | None = None,
        fidelity: int | None = None,
    ) -> PriceHistory:
        """Fetch historical price points for ``token_id`` via the gateway.

        Signature mirrors :meth:`ClobClient.get_price_history` so
        :class:`PredictionMarketDataProvider` (and any other caller still
        using the V1 ClobClient signature) works unchanged. The local
        ``ValueError`` validation lives server-side now (the underlying
        ClobClient enforces ``interval`` vs ``start_ts``/``end_ts`` mutual
        exclusion); the gateway re-raises it as a ``PolymarketAPIError``
        through ``response.error`` for parity.
        """
        # Normalize the enum -> string before sending; the proto field is a
        # plain string, the SDK accepts either.
        interval_str = interval.value if isinstance(interval, PriceHistoryInterval) else (interval or "")
        request = gateway_pb2.PolymarketGetPriceHistoryRequest(
            token_id=token_id,
            interval=interval_str,
            start_ts=int(start_ts) if start_ts else 0,
            end_ts=int(end_ts) if end_ts else 0,
            fidelity=int(fidelity) if fidelity else 0,
        )
        try:
            response = self._gateway_client.polymarket.GetPriceHistory(request, timeout=self._rpc_timeout())
        except grpc.RpcError as exc:
            self._raise_rpc_error("GetPriceHistory RPC failed", exc.details(), status_code=None)
        if not response.success:
            self._raise_rpc_error("GetPriceHistory failed", response.error)

        prices = [
            HistoricalPrice(
                timestamp=datetime.fromtimestamp(p.timestamp, tz=UTC),
                price=Decimal(p.price or "0"),
            )
            for p in response.prices
        ]
        return PriceHistory(
            token_id=response.token_id or token_id,
            interval=response.interval or interval_str or "custom",
            prices=prices,
            start_time=datetime.fromtimestamp(response.start_time, tz=UTC) if response.start_time else None,
            end_time=datetime.fromtimestamp(response.end_time, tz=UTC) if response.end_time else None,
        )

    def get_trade_tape(
        self,
        token_id: str | None = None,
        limit: int = 100,
    ) -> list[HistoricalTrade]:
        """Fetch the recent trade tape via the gateway.

        Signature mirrors :meth:`ClobClient.get_trade_tape` so
        :class:`PredictionMarketDataProvider` works unchanged.
        """
        request = gateway_pb2.PolymarketGetTradeTapeRequest(
            token_id=token_id or "",
            limit=int(limit),
        )
        try:
            response = self._gateway_client.polymarket.GetTradeTape(request, timeout=self._rpc_timeout())
        except grpc.RpcError as exc:
            self._raise_rpc_error("GetTradeTape RPC failed", exc.details(), status_code=None)
        if not response.success:
            self._raise_rpc_error("GetTradeTape failed", response.error)

        return [
            HistoricalTrade(
                id=t.id,
                # Prefer explicit token_id, falling back to asset_id which the
                # proto carries as an alias for upstream compatibility.
                token_id=t.token_id or t.asset_id,
                # The model is typed Literal["BUY", "SELL"]; preserve upstream
                # casing — empty side falls back to "BUY" to match the SDK.
                side=t.side or "BUY",
                price=Decimal(t.price or "0"),
                size=Decimal(t.size or "0"),
                # Sentinel epoch for missing timestamps — fabricating "now"
                # would mislead recency-based consumers (e.g. age-of-tape
                # heuristics) into treating a malformed/missing trade as
                # fresh activity.
                timestamp=(
                    datetime.fromtimestamp(t.timestamp, tz=UTC) if t.timestamp else datetime.fromtimestamp(0, tz=UTC)
                ),
                maker=t.maker or None,
                taker=t.taker or None,
            )
            for t in response.trades
        ]

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

    def get_simplified_markets(self, next_cursor: str = "") -> tuple[list[SimplifiedMarket], str]:
        """Fetch a paginated page of simplified market summaries.

        Mirrors the gateway's ``GetSimplifiedMarkets`` RPC, which proxies the
        public CLOB ``/simplified-markets`` endpoint. Returns ``(markets,
        next_cursor)`` so callers can page through the catalogue without
        baking pagination state into the client. An empty ``next_cursor``
        signals the final page.
        """
        try:
            response = self._gateway_client.polymarket.GetSimplifiedMarkets(
                gateway_pb2.PolymarketGetSimplifiedMarketsRequest(next_cursor=next_cursor),
                timeout=self._rpc_timeout(),
            )
        except grpc.RpcError as exc:
            self._raise_rpc_error("GetSimplifiedMarkets RPC failed", exc.details(), status_code=None)
        if not response.success:
            self._raise_rpc_error("GetSimplifiedMarkets failed", response.error)
        markets = [
            SimplifiedMarket(
                condition_id=item.condition_id,
                tokens=list(item.tokens),
                min_incentive_size=Decimal(item.min_incentive_size or "0"),
                max_incentive_spread=Decimal(item.max_incentive_spread or "0"),
                active=item.active,
                closed=item.closed,
            )
            for item in response.markets
        ]
        return markets, response.next_cursor

    def get_midpoint(self, token_id: str) -> Decimal:
        """Return the midpoint price for ``token_id`` as a Decimal probability."""
        try:
            response = self._gateway_client.polymarket.GetMidpoint(
                gateway_pb2.PolymarketMidpointRequest(token_id=token_id),
                timeout=self._rpc_timeout(),
            )
        except grpc.RpcError as exc:
            self._raise_rpc_error("GetMidpoint RPC failed", exc.details(), status_code=None)
        if not response.success:
            self._raise_rpc_error("GetMidpoint failed", response.error)
        return Decimal(response.midpoint or "0")

    def get_price(self, token_id: str, side: str) -> Decimal:
        """Return the best price for one side of ``token_id``.

        ``side`` must be ``"BUY"`` or ``"SELL"`` (case-sensitive on the wire,
        normalized here). Validation runs locally BEFORE the RPC fires so a
        typo never burns a network round-trip — V2 split the V1 single-call
        ``/price`` into per-side queries, so an invalid side is unambiguously
        a programming error.
        """
        normalized = side.upper() if isinstance(side, str) else ""
        if normalized not in ("BUY", "SELL"):
            raise ValueError(f"Invalid side {side!r}: must be 'BUY' or 'SELL'")
        try:
            response = self._gateway_client.polymarket.GetPrice(
                gateway_pb2.PolymarketPriceRequest(token_id=token_id, side=normalized),
                timeout=self._rpc_timeout(),
            )
        except grpc.RpcError as exc:
            self._raise_rpc_error("GetPrice RPC failed", exc.details(), status_code=None)
        if not response.success:
            self._raise_rpc_error("GetPrice failed", response.error)
        return Decimal(response.price or "0")

    def get_spread(self, token_id: str) -> Decimal:
        """Return the bid-ask spread for ``token_id`` as a Decimal."""
        try:
            response = self._gateway_client.polymarket.GetSpread(
                gateway_pb2.PolymarketSpreadRequest(token_id=token_id),
                timeout=self._rpc_timeout(),
            )
        except grpc.RpcError as exc:
            self._raise_rpc_error("GetSpread RPC failed", exc.details(), status_code=None)
        if not response.success:
            self._raise_rpc_error("GetSpread failed", response.error)
        return Decimal(response.spread or "0")

    def get_tick_size(self, token_id: str) -> Decimal:
        """Return the minimum tick size for ``token_id`` as a Decimal."""
        try:
            response = self._gateway_client.polymarket.GetTickSize(
                gateway_pb2.PolymarketTickSizeRequest(token_id=token_id),
                timeout=self._rpc_timeout(),
            )
        except grpc.RpcError as exc:
            self._raise_rpc_error("GetTickSize RPC failed", exc.details(), status_code=None)
        if not response.success:
            self._raise_rpc_error("GetTickSize failed", response.error)
        return Decimal(response.tick_size or "0.01")

    def get_balance_allowance(
        self,
        asset_type: str = "COLLATERAL",
        token_id: str | None = None,
    ) -> BalanceAllowance:
        """Return current balance and CLOB allowance for the gateway wallet.

        ``asset_type`` is ``"COLLATERAL"`` (USDC/pUSD) or ``"CONDITIONAL"``
        (a position token — ``token_id`` required in that case). Mirrors
        ``ClobClient.get_balance_allowance``; the gateway derives the wallet
        from its own configuration.
        """
        try:
            response = self._gateway_client.polymarket.GetBalanceAllowance(
                gateway_pb2.PolymarketBalanceAllowanceRequest(
                    asset_type=asset_type,
                    token_id=token_id or "",
                ),
                timeout=self._rpc_timeout(),
            )
        except grpc.RpcError as exc:
            self._raise_rpc_error("GetBalanceAllowance RPC failed", exc.details(), status_code=None)
        if not response.success:
            self._raise_rpc_error("GetBalanceAllowance failed", response.error)
        return BalanceAllowance(
            balance=Decimal(response.balance or "0"),
            allowance=Decimal(response.allowance or "0"),
        )

    def get_trades(self, filters: TradeFilters | None = None) -> list[Trade]:
        """Return executed trade history for the gateway wallet.

        Distinct from :meth:`get_trade_tape`, which returns the public
        market-wide price tape. ``GetTradesHistory`` proxies the
        authenticated ``/trades`` endpoint and reports the wallet's own
        executed fills. Mirrors :meth:`ClobClient.get_trades` so callers can
        adopt without shape changes.
        """
        request = gateway_pb2.PolymarketGetTradesRequest(
            market_id=filters.market if filters and filters.market else "",
            limit=filters.limit if filters else 0,
        )
        try:
            response = self._gateway_client.polymarket.GetTradesHistory(
                request,
                timeout=self._rpc_timeout(),
            )
        except grpc.RpcError as exc:
            self._raise_rpc_error("GetTradesHistory RPC failed", exc.details(), status_code=None)
        if not response.success:
            self._raise_rpc_error("GetTradesHistory failed", response.error)

        trades: list[Trade] = []
        for item in response.trades:
            # ``match_time`` is the upstream timestamp on the authenticated
            # ``/trades`` payload; it can be either an ISO-8601 string or a
            # Unix-second integer-as-string. Normalize both forms; on parse
            # failure fall back to the epoch sentinel so a malformed or
            # missing timestamp is plainly identifiable downstream rather
            # than being mistaken for live activity.
            match_time_raw = item.match_time
            timestamp: datetime
            if match_time_raw:
                try:
                    timestamp = datetime.fromtimestamp(int(match_time_raw), tz=UTC)
                except (ValueError, TypeError):
                    try:
                        timestamp = datetime.fromisoformat(match_time_raw.replace("Z", "+00:00"))
                    except ValueError:
                        timestamp = datetime.fromtimestamp(0, tz=UTC)
            else:
                timestamp = datetime.fromtimestamp(0, tz=UTC)

            # Trade.fee documents the *fee paid* (currency units), but the
            # proto carries ``fee_rate_bps`` (a rate). Compute the absolute
            # fee from the rate and the trade's notional so the model stays
            # consistent with its docstring and downstream PnL bookkeeping
            # doesn't silently consume a rate as if it were a fee.
            price = Decimal(item.price or "0")
            size = Decimal(item.size or "0")
            try:
                fee_rate_bps = Decimal(item.fee_rate_bps or "0")
            except (ValueError, ArithmeticError):
                fee_rate_bps = Decimal("0")
            fee = (price * size * fee_rate_bps) / Decimal("10000")

            try:
                status = TradeStatus(item.status) if item.status else TradeStatus.CONFIRMED
            except ValueError:
                # Upstream introduced a status the enum doesn't know yet.
                # CONFIRMED is the safest fallback — these are historical
                # trades, not live submission acks (which use OrderStatus).
                status = TradeStatus.CONFIRMED

            trades.append(
                Trade(
                    id=item.trade_id,
                    market_id=item.market,
                    # Fall back to ``asset_id`` because the gateway uses it
                    # as the trade's token id alias for upstream compat.
                    token_id=item.asset_id,
                    side=item.side or "BUY",
                    price=price,
                    size=size,
                    fee=fee,
                    timestamp=timestamp,
                    status=status,
                )
            )
        return trades

    def cancel_orders(self, order_ids: list[str]) -> dict[str, list[str]]:
        """Cancel multiple orders, returning per-id success/failure lists.

        Returns ``{"canceled": [...], "not_canceled": [...]}``. The gateway's
        ``CancelOrders`` RPC iterates serverside (one CLOB call per id) and
        returns partial-success — we surface that shape verbatim so callers
        can retry the failures without re-attempting the successes.
        """
        try:
            response = self._gateway_client.polymarket.CancelOrders(
                gateway_pb2.PolymarketCancelOrdersRequest(order_ids=list(order_ids)),
                timeout=self._rpc_timeout(),
            )
        except grpc.RpcError as exc:
            self._raise_rpc_error("CancelOrders RPC failed", exc.details(), status_code=None)
        # Partial failures carry success=False but still ship the canceled
        # subset — return the breakdown rather than raising, so callers can
        # process it. A *full* application-level failure (success=False with
        # both lists empty) is an auth / config / server error, not a benign
        # no-op — surface ``response.error`` so the caller doesn't silently
        # treat it as "nothing to cancel".
        if not response.success and not response.canceled and not response.not_canceled:
            self._raise_rpc_error("CancelOrders failed", response.error, status_code=None)
        return {
            "canceled": list(response.canceled),
            "not_canceled": list(response.not_canceled),
        }

    def cancel_all_orders(
        self,
        market_id: str | None = None,
        asset_id: str | None = None,
    ) -> list[str]:
        """Cancel all open orders, optionally scoped by market or asset.

        ``market_id`` filters by market (CTF condition / market id) and
        ``asset_id`` filters by token id. The gateway lists open orders,
        applies the optional filters, then bulk-cancels — returning the list
        of canceled order ids.
        """
        try:
            response = self._gateway_client.polymarket.CancelAll(
                gateway_pb2.PolymarketCancelAllRequest(
                    market_id=market_id or "",
                    asset_id=asset_id or "",
                ),
                timeout=self._rpc_timeout(),
            )
        except grpc.RpcError as exc:
            self._raise_rpc_error("CancelAll RPC failed", exc.details(), status_code=None)
        if not response.success:
            self._raise_rpc_error("CancelAll failed", response.error)
        return list(response.canceled)

    def create_and_post_market_order(
        self,
        *,
        token_id: str,
        amount: Decimal,
        side: str,
        worst_price: Decimal | None = None,
        expiration: int = 0,
        # ``market`` accepted for ClobClient signature parity (V2 added it
        # for neg-risk routing + tick validation); ignored here because the
        # gateway re-fetches the market server-side from ``token_id`` (see
        # ``polymarket_service.CreateAndPostMarketOrder``).
        market: Any | None = None,  # noqa: ARG002
    ) -> OrderResponse:
        """Submit a market order via the gateway-owned signer.

        ``amount`` semantics differ by side, matching the upstream V2
        endpoint: for ``BUY`` it is the USDC notional to spend; for ``SELL``
        it is the token (share) size to sell. The gateway fetches the
        current best price, derives the size for BUY (``amount / price``),
        and submits the resulting order as Fill-or-Kill.

        Args:
            token_id: CLOB token id to trade.
            amount: USDC notional for BUY, share size for SELL.
            side: ``"BUY"`` or ``"SELL"``.
            worst_price: Optional limit price guard — refuses to fill at a
                worse price (BUY: above worst; SELL: below worst).
            expiration: API-level GTD timestamp (Unix seconds). 0 = none.
            market: Accepted for ClobClient signature parity. Ignored — the
                gateway re-fetches the market server-side from ``token_id``.
        """
        try:
            response = self._gateway_client.polymarket.CreateAndPostMarketOrder(
                gateway_pb2.PolymarketMarketOrderRequest(
                    token_id=token_id,
                    amount=str(amount),
                    side=side,
                    expiration=expiration,
                    worst_price=str(worst_price) if worst_price is not None else "",
                ),
                timeout=self._rpc_timeout(),
            )
        except grpc.RpcError as exc:
            self._raise_rpc_error("CreateAndPostMarketOrder RPC failed", exc.details(), status_code=None)
        if not response.success:
            self._raise_rpc_error("CreateAndPostMarketOrder failed", response.error)
        # VIB-3710: same setup_txs / fee_pusd surfacing as CreateAndPostOrder.
        # Market orders go through the same _ensure_wallet_ready path
        # server-side (CreateAndPostMarketOrder delegates to CreateAndPostOrder).
        # CodeRabbit thread 5: project ``side`` and ``token_id`` into the dict
        # so a SELL market order does not deserialize as BUY (the
        # ``from_api_response`` defaults are ``side="BUY"`` / ``market=""``).
        return OrderResponse.from_api_response(
            {
                "orderID": response.order_id,
                "status": response.status,
                "side": side,
                "market": token_id,
                "price": response.price,
                "size": response.size,
                "filledSize": response.size_matched,
                "avgPrice": response.avg_fill_price,
                "createdAt": response.created_at,
                "setup_txs": [
                    {
                        "tx_hash": tx.tx_hash,
                        "description": tx.description,
                        "gas_used": tx.gas_used,
                        "gas_price_wei": tx.gas_price_wei,
                        "total_cost_wei": tx.total_cost_wei,
                    }
                    for tx in response.setup_txs
                ],
                "fee_pusd": response.fee_pusd or None,
            }
        )
