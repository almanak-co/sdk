"""Regression tests for GatewayPolymarketClient.get_price_history.

VIB-3695: ``get_price_history`` was a ``NotImplementedError`` stub in the
gateway-routed wrapper — every strategy that drove
``PredictionMarketDataProvider.get_price_history`` (e.g. signal-generation
flows that compute realized vol or simple moving averages from the CLOB
``/prices-history`` endpoint) crashed at runtime. This file exercises the
real wrapper code path with a mocked Polymarket gRPC stub so the regression
cannot recur silently.

Pattern intentionally matches ``test_gateway_client_create_and_post_order.py``
(the previous Polymarket wrapper regression): construct a *real*
``GatewayPolymarketClient`` against a real ``GatewayClient`` whose stub is
swapped for a ``MagicMock``. A top-level mock of the wrapper class would
mask the very bug we're guarding against.
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
from almanak.connectors.polymarket.models import PriceHistory, PriceHistoryInterval
from almanak.framework.gateway_client import GatewayClient, GatewayClientConfig
from almanak.connectors.polymarket.proto import polymarket_pb2


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_gateway_client(stub: MagicMock) -> GatewayClient:
    """Build a real GatewayClient with a pre-injected polymarket stub.

    See ``test_gateway_client_create_and_post_order.py`` for the rationale:
    we exercise the *real* wrapper code without requiring a live gateway.
    """
    client = GatewayClient(config=GatewayClientConfig(host="localhost", port=50051, timeout=5.0))
    client._connected = True
    client._channel = MagicMock()
    client._connector_stubs = {"polymarket": stub}
    return client


def _make_response(
    *,
    token_id: str = "111",
    interval: str = "1h",
    points: list[tuple[int, str]] | None = None,
    success: bool = True,
    error: str = "",
) -> polymarket_pb2.PolymarketPriceHistoryResponse:
    """Construct a populated PolymarketPriceHistoryResponse fixture."""
    points = points if points is not None else [(1700000000, "0.42"), (1700003600, "0.45")]
    prices = [
        polymarket_pb2.PolymarketHistoricalPrice(timestamp=ts, price=p) for ts, p in points
    ]
    return polymarket_pb2.PolymarketPriceHistoryResponse(
        token_id=token_id,
        interval=interval,
        prices=prices,
        start_time=points[0][0] if points else 0,
        end_time=points[-1][0] if points else 0,
        success=success,
        error=error,
    )


# ---------------------------------------------------------------------------
# Tests — happy path / parsing
# ---------------------------------------------------------------------------


class TestGetPriceHistoryHappyPath:
    """The wrapper must convert the proto response back into the original
    ``PriceHistory`` / ``HistoricalPrice`` model so existing callers work
    unchanged. Bug-class: silent shape drift between proto and model would
    produce confusing AttributeErrors deep inside provider code."""

    def test_parses_prices_into_priceHistory_model(self) -> None:
        stub = MagicMock()
        stub.GetPriceHistory.return_value = _make_response(
            token_id="111",
            interval="1h",
            points=[(1700000000, "0.42"), (1700003600, "0.45"), (1700007200, "0.50")],
        )

        client = GatewayPolymarketClient(_make_gateway_client(stub))
        result = client.get_price_history(token_id="111", interval="1h")

        assert isinstance(result, PriceHistory)
        assert result.token_id == "111"
        assert result.interval == "1h"
        assert len(result.prices) == 3

        # Per-point parsing: timestamps are tz-aware UTC; prices are Decimal.
        assert result.prices[0].timestamp == datetime.fromtimestamp(1700000000, tz=UTC)
        assert result.prices[0].price == Decimal("0.42")
        assert result.prices[2].price == Decimal("0.50")

        # Aggregate properties downstream code relies on.
        assert result.open_price == Decimal("0.42")
        assert result.close_price == Decimal("0.50")
        assert result.high_price == Decimal("0.50")
        assert result.low_price == Decimal("0.42")

    def test_request_carries_all_parameters(self) -> None:
        """Every supported kwarg must traverse the gRPC boundary intact."""
        stub = MagicMock()
        stub.GetPriceHistory.return_value = _make_response()

        client = GatewayPolymarketClient(_make_gateway_client(stub))
        client.get_price_history(
            token_id="my-token",
            interval=None,
            start_ts=1700000000,
            end_ts=1700100000,
            fidelity=5,
        )

        call = stub.GetPriceHistory.call_args
        request = call.args[0]
        assert isinstance(request, polymarket_pb2.PolymarketGetPriceHistoryRequest)
        assert request.token_id == "my-token"
        assert request.interval == ""  # None becomes empty string on the wire
        assert request.start_ts == 1700000000
        assert request.end_ts == 1700100000
        assert request.fidelity == 5

    def test_interval_enum_is_normalized_to_string(self) -> None:
        """``PriceHistoryInterval.ONE_HOUR`` and the literal ``'1h'`` must
        both produce the same wire ``interval`` value — callers should be
        able to use either without conditionalizing the call site."""
        stub = MagicMock()
        stub.GetPriceHistory.return_value = _make_response(interval="1h")

        client = GatewayPolymarketClient(_make_gateway_client(stub))
        client.get_price_history(token_id="111", interval=PriceHistoryInterval.ONE_HOUR)

        request = stub.GetPriceHistory.call_args.args[0]
        assert request.interval == "1h"

    def test_empty_prices_yields_empty_PriceHistory(self) -> None:
        """When the upstream returns no points (e.g. brand-new market), the
        wrapper must return a valid empty ``PriceHistory`` — provider code
        depends on iterability, not on at-least-one-point."""
        stub = MagicMock()
        stub.GetPriceHistory.return_value = _make_response(points=[])

        client = GatewayPolymarketClient(_make_gateway_client(stub))
        result = client.get_price_history(token_id="111", interval="1d")

        assert result.prices == []
        assert result.open_price is None
        assert result.close_price is None
        assert result.start_time is None
        assert result.end_time is None


# ---------------------------------------------------------------------------
# Tests — error propagation
# ---------------------------------------------------------------------------


class TestGetPriceHistoryErrors:
    """The wrapper must convert both gRPC failures and server-side
    ``success=False`` responses into ``PolymarketAPIError`` so callers see
    a single exception type regardless of failure mode."""

    def test_grpc_error_raises_polymarket_api_error(self) -> None:
        stub = MagicMock()
        rpc_error = grpc.RpcError("network down")
        # grpc.RpcError surfaces details() on real instances; stub it for the test.
        rpc_error.details = lambda: "network down"  # type: ignore[method-assign]
        stub.GetPriceHistory.side_effect = rpc_error

        client = GatewayPolymarketClient(_make_gateway_client(stub))
        with pytest.raises(PolymarketAPIError, match="GetPriceHistory RPC failed"):
            client.get_price_history(token_id="111", interval="1h")

    def test_server_failure_raises_with_error_text(self) -> None:
        stub = MagicMock()
        stub.GetPriceHistory.return_value = _make_response(
            success=False, error="invalid token id"
        )

        client = GatewayPolymarketClient(_make_gateway_client(stub))
        with pytest.raises(PolymarketAPIError, match="invalid token id"):
            client.get_price_history(token_id="bad", interval="1h")
