"""Direct branch coverage for Polymarket gateway RPC handlers."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest
from eth_account import Account

from almanak.connectors.polymarket.exceptions import PolymarketAPIError
from almanak.connectors.polymarket.gateway.service import (
    CLOB_BASE_URL,
    GAMMA_BASE_URL,
    PolymarketServiceServicer,
)
from almanak.connectors.polymarket.models import OpenOrder
from almanak.connectors.polymarket.proto import polymarket_pb2
from almanak.gateway.core.settings import GatewaySettings

TEST_PRIVATE_KEY = "0x" + "ab" * 32
TEST_WALLET = Account.from_key(TEST_PRIVATE_KEY).address


@pytest.fixture
def servicer() -> PolymarketServiceServicer:
    settings = MagicMock(spec=GatewaySettings)
    settings.private_key = TEST_PRIVATE_KEY
    settings.polymarket_private_key = None
    settings.eoa_address = TEST_WALLET
    settings.polymarket_wallet_address = None
    settings.safe_address = None
    settings.safe_mode = None
    settings.polymarket_api_key = "key"
    settings.polymarket_secret = "c2VjcmV0"
    settings.polymarket_passphrase = "passphrase"
    return PolymarketServiceServicer(settings=settings)


def _gamma_market(**overrides: object) -> dict[str, object]:
    return {
        "id": "market-1",
        "conditionId": "condition-1",
        "question": "Will it happen?",
        "slug": "will-it-happen",
        "outcomes": ["Yes", "No"],
        "outcomePrices": ["0.6", "0.4"],
        "clobTokenIds": ["yes-token", "no-token"],
        **overrides,
    }


class TestGetMarket:
    @pytest.mark.asyncio
    async def test_slug_returns_first_market(self, servicer: PolymarketServiceServicer) -> None:
        market = _gamma_market()
        with patch.object(servicer, "_request", new=AsyncMock(return_value=(True, [market], None))) as request:
            response = await servicer.GetMarket(
                polymarket_pb2.PolymarketGetMarketRequest(slug="will-it-happen"),
                MagicMock(),
            )

        assert response.success is True
        assert response.market_id == "market-1"
        request.assert_awaited_once_with(
            "GET",
            GAMMA_BASE_URL,
            "/markets",
            params={"slug": "will-it-happen", "limit": "1"},
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("result", "expected_error"),
        [
            ((False, None, "upstream unavailable"), "upstream unavailable"),
            ((False, None, None), "Market not found"),
            ((True, None, None), "Market not found"),
            ((True, {}, None), "Market not found"),
            ((True, [], None), "Market not found"),
        ],
    )
    async def test_slug_failure_shapes(
        self,
        servicer: PolymarketServiceServicer,
        result: tuple[bool, object, str | None],
        expected_error: str,
    ) -> None:
        with patch.object(servicer, "_request", new=AsyncMock(return_value=result)):
            response = await servicer.GetMarket(
                polymarket_pb2.PolymarketGetMarketRequest(slug="missing"),
                MagicMock(),
            )

        assert response.success is False
        assert response.error == expected_error

    @pytest.mark.asyncio
    async def test_condition_id_uses_direct_result_without_fallback(self, servicer: PolymarketServiceServicer) -> None:
        market = _gamma_market()
        with patch.object(servicer, "_request", new=AsyncMock(return_value=(True, market, None))) as request:
            response = await servicer.GetMarket(
                polymarket_pb2.PolymarketGetMarketRequest(condition_id="condition-1"),
                MagicMock(),
            )

        assert response.success is True
        request.assert_awaited_once_with("GET", GAMMA_BASE_URL, "/markets/condition-1")

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("fallback", "expected_success", "expected_error"),
        [
            ((True, [_gamma_market()], None), True, ""),
            ((True, [], None), False, "Market not found"),
            ((True, {}, None), False, "Market not found"),
            ((False, None, "lookup failed"), False, "lookup failed"),
            ((False, None, None), False, "Market not found"),
        ],
    )
    async def test_condition_id_fallback_shapes(
        self,
        servicer: PolymarketServiceServicer,
        fallback: tuple[bool, object, str | None],
        expected_success: bool,
        expected_error: str,
    ) -> None:
        request = AsyncMock(side_effect=[(False, None, "direct failed"), fallback])
        with patch.object(servicer, "_request", new=request):
            response = await servicer.GetMarket(
                polymarket_pb2.PolymarketGetMarketRequest(condition_id="condition-1"),
                MagicMock(),
            )

        assert response.success is expected_success
        assert response.error == expected_error
        assert request.await_args_list == [
            call("GET", GAMMA_BASE_URL, "/markets/condition-1"),
            call(
                "GET",
                GAMMA_BASE_URL,
                "/markets",
                params={"condition_ids": "condition-1", "limit": "1"},
            ),
        ]

    @pytest.mark.asyncio
    async def test_non_dict_direct_success_falls_back(self, servicer: PolymarketServiceServicer) -> None:
        request = AsyncMock(side_effect=[(True, [_gamma_market()], None), (True, [_gamma_market()], None)])
        with patch.object(servicer, "_request", new=request):
            response = await servicer.GetMarket(
                polymarket_pb2.PolymarketGetMarketRequest(condition_id="condition-1"),
                MagicMock(),
            )

        assert response.success is True
        assert request.await_count == 2


class TestGetMarkets:
    @pytest.mark.asyncio
    async def test_rejects_cursor_without_upstream_request(self, servicer: PolymarketServiceServicer) -> None:
        with patch.object(servicer, "_request", new=AsyncMock()) as request:
            response = await servicer.GetMarkets(
                polymarket_pb2.PolymarketGetMarketsRequest(next_cursor="cursor"),
                MagicMock(),
            )

        assert response.success is False
        assert response.error == "Cursor pagination is not yet supported by GetMarkets"
        request.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_rejects_invalid_filters_json(self, servicer: PolymarketServiceServicer) -> None:
        with patch.object(servicer, "_request", new=AsyncMock()) as request:
            response = await servicer.GetMarkets(
                polymarket_pb2.PolymarketGetMarketsRequest(filters_json="{"),
                MagicMock(),
            )

        assert response.success is False
        assert response.error == "Invalid filters_json"
        request.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_normalizes_filters_and_markets(self, servicer: PolymarketServiceServicer) -> None:
        filters = '{"active":true,"closed":false,"tag":["defi",7],"limit":25,"skip":null}'
        with patch.object(
            servicer,
            "_request",
            new=AsyncMock(return_value=(True, [_gamma_market(), _gamma_market(id="market-2")], None)),
        ) as request:
            response = await servicer.GetMarkets(
                polymarket_pb2.PolymarketGetMarketsRequest(filters_json=filters),
                MagicMock(),
            )

        assert response.success is True
        assert [market.market_id for market in response.markets] == ["market-1", "market-2"]
        assert response.next_cursor == ""
        request.assert_awaited_once_with(
            "GET",
            GAMMA_BASE_URL,
            "/markets",
            params={"active": "true", "closed": "false", "tag": "defi,7", "limit": "25"},
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("result", "expected_error"),
        [
            ((False, None, "upstream failed"), "upstream failed"),
            ((False, None, None), ""),
        ],
    )
    async def test_upstream_failures(
        self,
        servicer: PolymarketServiceServicer,
        result: tuple[bool, object, str | None],
        expected_error: str,
    ) -> None:
        with patch.object(servicer, "_request", new=AsyncMock(return_value=result)):
            response = await servicer.GetMarkets(polymarket_pb2.PolymarketGetMarketsRequest(), MagicMock())

        assert response.success is False
        assert response.error == expected_error

    @pytest.mark.asyncio
    async def test_non_list_success_returns_empty_page(self, servicer: PolymarketServiceServicer) -> None:
        with patch.object(servicer, "_request", new=AsyncMock(return_value=(True, {}, None))) as request:
            response = await servicer.GetMarkets(polymarket_pb2.PolymarketGetMarketsRequest(), MagicMock())

        assert response.success is True
        assert list(response.markets) == []
        request.assert_awaited_once_with("GET", GAMMA_BASE_URL, "/markets", params=None)


class TestGetSimplifiedMarkets:
    @pytest.mark.asyncio
    async def test_list_response_and_cursor_request(self, servicer: PolymarketServiceServicer) -> None:
        item = {
            "condition_id": "condition-1",
            "tokens": [123, "456"],
            "min_incentive_size": 10,
            "max_incentive_spread": "0.05",
            "active": True,
            "closed": False,
        }
        with patch.object(servicer, "_request", new=AsyncMock(return_value=(True, [item], None))) as request:
            response = await servicer.GetSimplifiedMarkets(
                polymarket_pb2.PolymarketGetSimplifiedMarketsRequest(next_cursor="cursor-1"),
                MagicMock(),
            )

        assert response.success is True
        assert response.next_cursor == ""
        assert list(response.markets[0].tokens) == ["123", "456"]
        assert response.markets[0].min_incentive_size == "10"
        request.assert_awaited_once_with(
            "GET",
            CLOB_BASE_URL,
            "/simplified-markets",
            params={"next_cursor": "cursor-1"},
        )

    @pytest.mark.asyncio
    async def test_dict_response_uses_defaults_and_next_cursor(self, servicer: PolymarketServiceServicer) -> None:
        with patch.object(
            servicer,
            "_request",
            new=AsyncMock(return_value=(True, {"data": [{}], "next_cursor": "cursor-2"}, None)),
        ):
            response = await servicer.GetSimplifiedMarkets(
                polymarket_pb2.PolymarketGetSimplifiedMarketsRequest(),
                MagicMock(),
            )

        assert response.success is True
        assert response.next_cursor == "cursor-2"
        assert response.markets[0].min_incentive_size == "0"
        assert response.markets[0].max_incentive_spread == "0"

    @pytest.mark.asyncio
    async def test_null_response_returns_empty_page(self, servicer: PolymarketServiceServicer) -> None:
        with patch.object(servicer, "_request", new=AsyncMock(return_value=(True, None, None))):
            response = await servicer.GetSimplifiedMarkets(
                polymarket_pb2.PolymarketGetSimplifiedMarketsRequest(),
                MagicMock(),
            )

        assert response.success is True
        assert list(response.markets) == []
        assert response.next_cursor == ""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("error", ["upstream failed", None])
    async def test_upstream_failure(self, servicer: PolymarketServiceServicer, error: str | None) -> None:
        with patch.object(servicer, "_request", new=AsyncMock(return_value=(False, None, error))):
            response = await servicer.GetSimplifiedMarkets(
                polymarket_pb2.PolymarketGetSimplifiedMarketsRequest(),
                MagicMock(),
            )

        assert response.success is False
        assert response.error == (error or "")


def _order_response(*, with_optional_fields: bool) -> SimpleNamespace:
    return SimpleNamespace(
        order_id="order-1",
        status=SimpleNamespace(value="LIVE"),
        filled_size=Decimal("1.25"),
        price=Decimal("0.42"),
        size=Decimal("3"),
        avg_fill_price=Decimal("0.41") if with_optional_fields else None,
        created_at=datetime(2026, 1, 2, 3, 4, tzinfo=UTC) if with_optional_fields else None,
        fee_pusd=Decimal("0.01") if with_optional_fields else None,
    )


class TestCreateAndPostOrder:
    @pytest.mark.asyncio
    async def test_unavailable_signer_fails_before_auth(self, servicer: PolymarketServiceServicer) -> None:
        servicer._available = False
        with patch.object(servicer, "_build_authenticated_client", new=AsyncMock()) as build_client:
            response = await servicer.CreateAndPostOrder(
                polymarket_pb2.PolymarketCreateOrderRequest(price="0.5", size="2", side="BUY"),
                MagicMock(),
            )

        assert response.success is False
        assert response.error == "Polymarket signer not configured in gateway"
        build_client.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("price", "size", "side", "expected_error"),
        [
            ("invalid", "2", "BUY", "ConversionSyntax"),
            ("0.5", "invalid", "BUY", "ConversionSyntax"),
            ("0.5", "2", "hold", "Invalid side 'hold': must be 'BUY' or 'SELL'"),
        ],
    )
    async def test_invalid_request_fails_before_auth(
        self,
        servicer: PolymarketServiceServicer,
        price: str,
        size: str,
        side: str,
        expected_error: str,
    ) -> None:
        with patch.object(servicer, "_build_authenticated_client", new=AsyncMock()) as build_client:
            response = await servicer.CreateAndPostOrder(
                polymarket_pb2.PolymarketCreateOrderRequest(price=price, size=size, side=side),
                MagicMock(),
            )

        assert response.success is False
        assert expected_error in response.error
        build_client.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("side", "time_in_force", "expiration", "min_pusd", "with_optional_fields"),
        [
            ("buy", "GTD", 100, 1_260_000, True),
            ("SELL", "", 0, 0, False),
        ],
    )
    async def test_success_preserves_order_and_setup_details(
        self,
        servicer: PolymarketServiceServicer,
        side: str,
        time_in_force: str,
        expiration: int,
        min_pusd: int,
        with_optional_fields: bool,
    ) -> None:
        client = MagicMock()
        client.create_and_post_order.return_value = _order_response(with_optional_fields=with_optional_fields)
        market = object()
        setup_txs = [
            {
                "tx_hash": "0xsetup",
                "description": "Approve pUSD",
                "gas_used": "21000",
                "gas_price_wei": "30",
                "total_cost_wei": "630000",
            }
        ]
        servicer._build_authenticated_client = AsyncMock(return_value=client)  # type: ignore[method-assign]
        servicer._fetch_market_for_token = AsyncMock(return_value=market)  # type: ignore[method-assign]
        servicer._ensure_wallet_ready = AsyncMock(return_value=setup_txs)  # type: ignore[method-assign]

        response = await servicer.CreateAndPostOrder(
            polymarket_pb2.PolymarketCreateOrderRequest(
                token_id="yes-token",
                price="0.42",
                size="3",
                side=side,
                time_in_force=time_in_force,
                expiration=expiration,
            ),
            MagicMock(),
        )

        assert response.success is True
        assert response.order_id == "order-1"
        assert response.avg_fill_price == ("0.41" if with_optional_fields else "")
        assert response.created_at == ("2026-01-02T03:04:00+00:00" if with_optional_fields else "")
        assert response.fee_pusd == ("0.01" if with_optional_fields else "")
        assert response.setup_txs[0].gas_used == 21_000
        servicer._fetch_market_for_token.assert_awaited_once_with(client, "yes-token")
        servicer._ensure_wallet_ready.assert_awaited_once_with(min_pusd_units=min_pusd)
        client.create_and_post_order.assert_called_once_with(
            token_id="yes-token",
            price=Decimal("0.42"),
            size=Decimal("3"),
            side=side.upper(),
            market=market,
            time_in_force=time_in_force or "GTC",
            expiration=expiration if expiration > 0 else 0,
        )
        client.close.assert_called_once()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("error", "should_invalidate"),
        [
            (PolymarketAPIError("order breaks minimum tick size rule: 0.01"), True),
            (RuntimeError("submission failed"), False),
        ],
    )
    async def test_order_submission_error_controls_cache_invalidation(
        self,
        servicer: PolymarketServiceServicer,
        error: Exception,
        should_invalidate: bool,
    ) -> None:
        client = MagicMock()
        client.create_and_post_order.side_effect = error
        servicer._build_authenticated_client = AsyncMock(return_value=client)  # type: ignore[method-assign]
        servicer._fetch_market_for_token = AsyncMock(return_value=object())  # type: ignore[method-assign]
        servicer._ensure_wallet_ready = AsyncMock(return_value=[])  # type: ignore[method-assign]
        servicer._invalidate_market_cache = MagicMock()  # type: ignore[method-assign]

        response = await servicer.CreateAndPostOrder(
            polymarket_pb2.PolymarketCreateOrderRequest(
                token_id="yes-token",
                price="0.42",
                size="3",
                side="BUY",
            ),
            MagicMock(),
        )

        assert response.success is False
        assert response.error == str(error)
        if should_invalidate:
            servicer._invalidate_market_cache.assert_called_once_with("yes-token")
        else:
            servicer._invalidate_market_cache.assert_not_called()
        client.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_market_lookup_error_does_not_invalidate_cache(self, servicer: PolymarketServiceServicer) -> None:
        client = MagicMock()
        error = ValueError("order breaks minimum tick size rule: 0.01")
        servicer._build_authenticated_client = AsyncMock(return_value=client)  # type: ignore[method-assign]
        servicer._fetch_market_for_token = AsyncMock(side_effect=error)  # type: ignore[method-assign]
        servicer._ensure_wallet_ready = AsyncMock()  # type: ignore[method-assign]
        servicer._invalidate_market_cache = MagicMock()  # type: ignore[method-assign]

        response = await servicer.CreateAndPostOrder(
            polymarket_pb2.PolymarketCreateOrderRequest(
                token_id="yes-token",
                price="0.42",
                size="3",
                side="BUY",
            ),
            MagicMock(),
        )

        assert response.success is False
        assert response.error == str(error)
        servicer._ensure_wallet_ready.assert_not_awaited()
        servicer._invalidate_market_cache.assert_not_called()
        client.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_authentication_error_is_returned(self, servicer: PolymarketServiceServicer) -> None:
        servicer._build_authenticated_client = AsyncMock(side_effect=ValueError("credentials unavailable"))  # type: ignore[method-assign]

        response = await servicer.CreateAndPostOrder(
            polymarket_pb2.PolymarketCreateOrderRequest(
                token_id="yes-token",
                price="0.42",
                size="3",
                side="BUY",
            ),
            MagicMock(),
        )

        assert response.success is False
        assert response.error == "credentials unavailable"


def _open_order(order_id: str, market: str) -> OpenOrder:
    return OpenOrder(
        order_id=order_id,
        market=market,
        side="BUY",
        price=Decimal("0.4"),
        size=Decimal("5"),
    )


class TestCancelAll:
    @pytest.mark.asyncio
    async def test_no_orders_skips_cancel(self, servicer: PolymarketServiceServicer) -> None:
        client = MagicMock()
        client.get_open_orders.return_value = []
        servicer._build_authenticated_client = AsyncMock(return_value=client)  # type: ignore[method-assign]

        response = await servicer.CancelAll(polymarket_pb2.PolymarketCancelAllRequest(), MagicMock())

        assert response.success is True
        assert list(response.canceled) == []
        client.get_open_orders.assert_called_once()
        assert client.get_open_orders.call_args.args[0].market is None
        client.cancel_orders.assert_not_called()
        client.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_filters_asset_and_cancels_matches(self, servicer: PolymarketServiceServicer) -> None:
        client = MagicMock()
        client.get_open_orders.return_value = [
            _open_order("order-1", "asset-1"),
            _open_order("order-2", "asset-2"),
        ]
        servicer._build_authenticated_client = AsyncMock(return_value=client)  # type: ignore[method-assign]

        response = await servicer.CancelAll(
            polymarket_pb2.PolymarketCancelAllRequest(market_id="condition-1", asset_id="asset-2"),
            MagicMock(),
        )

        assert response.success is True
        assert list(response.canceled) == ["order-2"]
        assert client.get_open_orders.call_args.args[0].market == "condition-1"
        client.cancel_orders.assert_called_once_with(["order-2"])
        client.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_client_error_returns_failure_and_closes(self, servicer: PolymarketServiceServicer) -> None:
        client = MagicMock()
        client.get_open_orders.side_effect = RuntimeError("lookup failed")
        servicer._build_authenticated_client = AsyncMock(return_value=client)  # type: ignore[method-assign]

        response = await servicer.CancelAll(polymarket_pb2.PolymarketCancelAllRequest(), MagicMock())

        assert response.success is False
        assert response.error == "lookup failed"
        client.close.assert_called_once()


class TestGetTradesHistory:
    @pytest.mark.asyncio
    async def test_list_response_with_all_filters(self, servicer: PolymarketServiceServicer) -> None:
        trades = [
            {
                "id": "trade-1",
                "trade_id": "legacy-id",
                "market": "market-1",
                "asset_id": "asset-1",
                "side": "BUY",
                "price": Decimal("0.4"),
                "size": 5,
                "fee_rate_bps": 10,
                "status": "MATCHED",
                "match_time": "2026-01-01T00:00:00Z",
                "transaction_hash": "0xtx",
                "bucket_index": 2,
            },
            {"trade_id": "trade-2"},
        ]
        with patch.object(servicer, "_request", new=AsyncMock(return_value=(True, trades, None))) as request:
            response = await servicer.GetTradesHistory(
                polymarket_pb2.PolymarketGetTradesRequest(
                    market_id="market-1",
                    asset_id="asset-1",
                    limit=20,
                    before="before",
                    after="after",
                ),
                MagicMock(),
            )

        assert response.success is True
        assert [trade.trade_id for trade in response.trades] == ["trade-1", "trade-2"]
        assert response.trades[0].price == "0.4"
        assert response.trades[1].price == "0"
        assert response.next_cursor == ""
        request.assert_awaited_once_with(
            "GET",
            CLOB_BASE_URL,
            "/trades",
            params={
                "market": "market-1",
                "asset_id": "asset-1",
                "limit": "20",
                "before": "before",
                "after": "after",
            },
            authenticated=True,
        )

    @pytest.mark.asyncio
    async def test_dict_response_preserves_cursor(self, servicer: PolymarketServiceServicer) -> None:
        with patch.object(
            servicer,
            "_request",
            new=AsyncMock(return_value=(True, {"data": [{"id": "trade-1"}], "next_cursor": "next"}, None)),
        ) as request:
            response = await servicer.GetTradesHistory(
                polymarket_pb2.PolymarketGetTradesRequest(),
                MagicMock(),
            )

        assert response.success is True
        assert response.next_cursor == "next"
        request.assert_awaited_once_with(
            "GET",
            CLOB_BASE_URL,
            "/trades",
            params=None,
            authenticated=True,
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize("data", [None, {}])
    async def test_empty_success_shapes(self, servicer: PolymarketServiceServicer, data: object) -> None:
        with patch.object(servicer, "_request", new=AsyncMock(return_value=(True, data, None))):
            response = await servicer.GetTradesHistory(
                polymarket_pb2.PolymarketGetTradesRequest(),
                MagicMock(),
            )

        assert response.success is True
        assert list(response.trades) == []

    @pytest.mark.asyncio
    @pytest.mark.parametrize("error", ["upstream failed", None])
    async def test_upstream_failure(self, servicer: PolymarketServiceServicer, error: str | None) -> None:
        with patch.object(servicer, "_request", new=AsyncMock(return_value=(False, None, error))):
            response = await servicer.GetTradesHistory(
                polymarket_pb2.PolymarketGetTradesRequest(),
                MagicMock(),
            )

        assert response.success is False
        assert response.error == (error or "")


class TestGetBalanceAllowance:
    @pytest.mark.asyncio
    async def test_default_asset_type_and_response_defaults(self, servicer: PolymarketServiceServicer) -> None:
        with patch.object(servicer, "_request", new=AsyncMock(return_value=(True, {}, None))) as request:
            response = await servicer.GetBalanceAllowance(
                polymarket_pb2.PolymarketBalanceAllowanceRequest(),
                MagicMock(),
            )

        assert response.success is False
        assert response.error == "Could not get balance"
        request.assert_awaited_once_with(
            "GET",
            CLOB_BASE_URL,
            "/balance-allowance",
            params={"asset_type": "COLLATERAL"},
            authenticated=True,
        )

    @pytest.mark.asyncio
    async def test_token_request_returns_balance(self, servicer: PolymarketServiceServicer) -> None:
        with patch.object(
            servicer,
            "_request",
            new=AsyncMock(return_value=(True, {"balance": Decimal("12.5"), "allowance": 7}, None)),
        ) as request:
            response = await servicer.GetBalanceAllowance(
                polymarket_pb2.PolymarketBalanceAllowanceRequest(
                    asset_type="CONDITIONAL",
                    token_id="yes-token",
                ),
                MagicMock(),
            )

        assert response.success is True
        assert response.balance == "12.5"
        assert response.allowance == "7"
        assert request.await_args.kwargs["params"] == {
            "asset_type": "CONDITIONAL",
            "token_id": "yes-token",
        }

    @pytest.mark.asyncio
    async def test_missing_fields_use_zero_defaults(self, servicer: PolymarketServiceServicer) -> None:
        with patch.object(servicer, "_request", new=AsyncMock(return_value=(True, {"other": "value"}, None))):
            response = await servicer.GetBalanceAllowance(
                polymarket_pb2.PolymarketBalanceAllowanceRequest(),
                MagicMock(),
            )

        assert response.success is True
        assert response.balance == "0"
        assert response.allowance == "0"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("result", "expected_error"),
        [
            ((False, None, "upstream failed"), "upstream failed"),
            ((False, None, None), "Could not get balance"),
            ((True, None, None), "Could not get balance"),
        ],
    )
    async def test_failure_shapes(
        self,
        servicer: PolymarketServiceServicer,
        result: tuple[bool, object, str | None],
        expected_error: str,
    ) -> None:
        with patch.object(servicer, "_request", new=AsyncMock(return_value=result)):
            response = await servicer.GetBalanceAllowance(
                polymarket_pb2.PolymarketBalanceAllowanceRequest(),
                MagicMock(),
            )

        assert response.success is False
        assert response.error == expected_error
