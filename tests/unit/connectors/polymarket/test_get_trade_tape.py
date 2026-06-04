"""Regression tests for GatewayPolymarketClient.get_trade_tape.

VIB-3695: ``get_trade_tape`` was a ``NotImplementedError`` stub in the
gateway-routed wrapper — every strategy that drove
``PredictionMarketDataProvider.get_trade_tape`` (e.g. flow-analysis or
short-horizon momentum signals built off the recent trade tape) crashed
at runtime. This file exercises the real wrapper code path with a mocked
Polymarket gRPC stub so the regression cannot recur silently.

See ``test_get_price_history.py`` for the wrapper-test pattern rationale.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock

import grpc
import pytest

from almanak.connectors.polymarket.exceptions import PolymarketAPIError
from almanak.connectors.polymarket.gateway_client import (
    GatewayPolymarketClient,
)
from almanak.connectors.polymarket.models import HistoricalTrade
from almanak.framework.gateway_client import GatewayClient, GatewayClientConfig
from almanak.connectors.polymarket.proto import polymarket_pb2


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_gateway_client(stub: MagicMock) -> GatewayClient:
    """Build a real GatewayClient with a pre-injected polymarket stub."""
    client = GatewayClient(config=GatewayClientConfig(host="localhost", port=50051, timeout=5.0))
    client._connected = True
    client._channel = MagicMock()
    client._connector_stubs = {"polymarket": stub}
    return client


def _make_trade(
    *,
    trade_id: str = "trade-1",
    token_id: str = "111",
    side: str = "BUY",
    price: str = "0.42",
    size: str = "10",
    timestamp: int = 1700000000,
    maker: str = "0xMaker",
    taker: str = "0xTaker",
) -> polymarket_pb2.PolymarketHistoricalTrade:
    return polymarket_pb2.PolymarketHistoricalTrade(
        id=trade_id,
        token_id=token_id,
        side=side,
        price=price,
        size=size,
        timestamp=timestamp,
        maker=maker,
        taker=taker,
        asset_id=token_id,
    )


def _make_response(
    *,
    trades: list[polymarket_pb2.PolymarketHistoricalTrade] | None = None,
    success: bool = True,
    error: str = "",
) -> polymarket_pb2.PolymarketTradeTapeResponse:
    return polymarket_pb2.PolymarketTradeTapeResponse(
        trades=trades if trades is not None else [_make_trade()],
        success=success,
        error=error,
    )


# ---------------------------------------------------------------------------
# Tests — happy path / parsing
# ---------------------------------------------------------------------------


class TestGetTradeTapeHappyPath:
    """The wrapper must convert proto trades back into ``HistoricalTrade``
    instances so existing callers (PredictionMarketDataProvider.get_trade_tape)
    work unchanged."""

    def test_parses_trades_into_historical_trade_models(self) -> None:
        stub = MagicMock()
        stub.GetTradeTape.return_value = _make_response(
            trades=[
                _make_trade(trade_id="t1", side="BUY", price="0.42", size="10", timestamp=1700000000),
                _make_trade(trade_id="t2", side="SELL", price="0.45", size="5", timestamp=1700001000),
            ]
        )

        client = GatewayPolymarketClient(_make_gateway_client(stub))
        trades = client.get_trade_tape(token_id="111", limit=50)

        assert len(trades) == 2
        for t in trades:
            assert isinstance(t, HistoricalTrade)
        assert trades[0].id == "t1"
        assert trades[0].side == "BUY"
        assert trades[0].price == Decimal("0.42")
        assert trades[0].size == Decimal("10")
        assert trades[0].timestamp == datetime.fromtimestamp(1700000000, tz=UTC)
        assert trades[0].maker == "0xMaker"
        assert trades[0].taker == "0xTaker"
        assert trades[0].token_id == "111"

        assert trades[1].id == "t2"
        assert trades[1].side == "SELL"

    def test_request_carries_token_id_and_limit(self) -> None:
        stub = MagicMock()
        stub.GetTradeTape.return_value = _make_response(trades=[])

        client = GatewayPolymarketClient(_make_gateway_client(stub))
        client.get_trade_tape(token_id="my-token", limit=42)

        request = stub.GetTradeTape.call_args.args[0]
        assert isinstance(request, polymarket_pb2.PolymarketGetTradeTapeRequest)
        assert request.token_id == "my-token"
        assert request.limit == 42

    def test_omitting_token_id_sends_empty_string(self) -> None:
        """``token_id=None`` must produce an empty wire field — the gateway
        treats empty as "market-wide tape" and that semantic must survive
        the wrapper's normalization layer."""
        stub = MagicMock()
        stub.GetTradeTape.return_value = _make_response(trades=[])

        client = GatewayPolymarketClient(_make_gateway_client(stub))
        client.get_trade_tape(limit=10)

        request = stub.GetTradeTape.call_args.args[0]
        assert request.token_id == ""
        assert request.limit == 10

    def test_default_limit_is_100(self) -> None:
        """Mirrors ``ClobClient.get_trade_tape`` default — strategies should
        not have to pass ``limit=100`` explicitly to keep the same behavior
        when migrating from direct ClobClient to the gateway wrapper."""
        stub = MagicMock()
        stub.GetTradeTape.return_value = _make_response(trades=[])

        client = GatewayPolymarketClient(_make_gateway_client(stub))
        client.get_trade_tape(token_id="111")

        request = stub.GetTradeTape.call_args.args[0]
        assert request.limit == 100

    def test_token_id_falls_back_to_asset_id_alias(self) -> None:
        """The proto carries both ``token_id`` and ``asset_id`` (an upstream
        alias). When the upstream populates only ``asset_id`` we must still
        produce a fully-formed ``HistoricalTrade``."""
        trade = polymarket_pb2.PolymarketHistoricalTrade(
            id="t1",
            token_id="",  # explicit empty
            asset_id="alias-token",
            side="BUY",
            price="0.5",
            size="10",
            timestamp=1700000000,
        )
        stub = MagicMock()
        stub.GetTradeTape.return_value = _make_response(trades=[trade])

        client = GatewayPolymarketClient(_make_gateway_client(stub))
        result = client.get_trade_tape(token_id="any")

        assert result[0].token_id == "alias-token"

    def test_empty_trades_list_returns_empty_list(self) -> None:
        stub = MagicMock()
        stub.GetTradeTape.return_value = _make_response(trades=[])

        client = GatewayPolymarketClient(_make_gateway_client(stub))
        result = client.get_trade_tape(token_id="111")

        assert result == []


# ---------------------------------------------------------------------------
# Tests — error propagation
# ---------------------------------------------------------------------------


class TestGetTradeTapeErrors:
    """gRPC failures and server-side failures both surface as
    ``PolymarketAPIError`` so callers see a single exception type."""

    def test_grpc_error_raises_polymarket_api_error(self) -> None:
        stub = MagicMock()
        rpc_error = grpc.RpcError("auth failed")
        rpc_error.details = lambda: "auth failed"  # type: ignore[method-assign]
        stub.GetTradeTape.side_effect = rpc_error

        client = GatewayPolymarketClient(_make_gateway_client(stub))
        with pytest.raises(PolymarketAPIError, match="GetTradeTape RPC failed"):
            client.get_trade_tape(token_id="111")

    def test_server_failure_raises_with_error_text(self) -> None:
        stub = MagicMock()
        stub.GetTradeTape.return_value = _make_response(
            trades=[], success=False, error="rate limited"
        )

        client = GatewayPolymarketClient(_make_gateway_client(stub))
        with pytest.raises(PolymarketAPIError, match="rate limited"):
            client.get_trade_tape(token_id="111")
