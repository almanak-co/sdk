"""GatewayPolymarketClient parses VIB-3710 setup_txs / fee_pusd onto OrderResponse.

The proto wire shape gained two new fields:

  - PolymarketOrderResponse.setup_txs: repeated PolymarketSetupTx
  - PolymarketOrderResponse.fee_pusd: string (Decimal-encoded)

These tests exercise the wrapper's projection of those fields into the
strategy-side ``OrderResponse`` model:

  j. ``GatewayPolymarketClient.create_and_post_order`` parses setup_txs
     from response into ``OrderResponse.setup_txs``.
  k. ``OrderResponse.setup_txs`` is empty when the proto field is empty
     (backwards compatibility — pre-VIB-3710 gateways still work).
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.framework.connectors.polymarket import SetupTxInfo
from almanak.framework.connectors.polymarket.gateway_client import (
    GatewayPolymarketClient,
)
from almanak.framework.connectors.polymarket.models import OrderResponse, OrderStatus
from almanak.framework.gateway_client import GatewayClient, GatewayClientConfig
from almanak.connectors.polymarket.proto import polymarket_pb2


def _make_gateway_client(stub: MagicMock) -> GatewayClient:
    """Build a real GatewayClient with a pre-injected polymarket stub."""
    client = GatewayClient(config=GatewayClientConfig(host="localhost", port=50051, timeout=5.0))
    client._connected = True
    client._channel = MagicMock()
    client._polymarket_stub = stub
    return client


def _response_with_setup_txs(*, fee_pusd: str = "") -> polymarket_pb2.PolymarketOrderResponse:
    return polymarket_pb2.PolymarketOrderResponse(
        order_id="order-vib3710-1",
        status="MATCHED",
        size_matched="10",
        success=True,
        price="0.50",
        size="10",
        avg_fill_price="0.50",
        created_at="2026-04-29T12:00:00Z",
        setup_txs=[
            polymarket_pb2.PolymarketSetupTx(
                tx_hash="0x" + "ab" * 32,
                description="Approve pUSD -> CTF V2 exchange",
                gas_used=60_000,
                gas_price_wei="50000000000",  # 50 gwei
                total_cost_wei=str(60_000 * 50_000_000_000),
            ),
            polymarket_pb2.PolymarketSetupTx(
                tx_hash="0x" + "cd" * 32,
                description="Wrap USDC.e -> pUSD",
                gas_used=150_000,
                gas_price_wei="50000000000",
                total_cost_wei=str(150_000 * 50_000_000_000),
            ),
        ],
        fee_pusd=fee_pusd,
    )


def _response_no_setup_txs() -> polymarket_pb2.PolymarketOrderResponse:
    """A pre-VIB-3710 / no-setup-needed response — empty repeated, empty fee_pusd."""
    return polymarket_pb2.PolymarketOrderResponse(
        order_id="order-empty-1",
        status="LIVE",
        size_matched="0",
        success=True,
        price="0.50",
        size="10",
        avg_fill_price="",
        created_at="2026-04-29T12:00:00Z",
    )


# ---------------------------------------------------------------------------
# (j) setup_txs flow through into OrderResponse.setup_txs
# ---------------------------------------------------------------------------


class TestSetupTxParsing:
    def test_create_and_post_order_parses_setup_txs(self) -> None:
        stub = MagicMock()
        stub.CreateAndPostOrder.return_value = _response_with_setup_txs(fee_pusd="0.012345")
        client = GatewayPolymarketClient(_make_gateway_client(stub))

        response = client.create_and_post_order(
            token_id="111",  # noqa: S106  (public CLOB token id, not a password)
            price=Decimal("0.50"),
            size=Decimal("10"),
            side="BUY",
        )

        assert isinstance(response, OrderResponse)
        assert response.order_id == "order-vib3710-1"
        assert len(response.setup_txs) == 2

        approval_tx = response.setup_txs[0]
        assert isinstance(approval_tx, SetupTxInfo)
        assert approval_tx.tx_hash == "0x" + "ab" * 32
        assert approval_tx.description == "Approve pUSD -> CTF V2 exchange"
        assert approval_tx.gas_used == 60_000
        assert approval_tx.gas_price_wei == "50000000000"
        assert approval_tx.total_cost_wei == str(60_000 * 50_000_000_000)

        wrap_tx = response.setup_txs[1]
        assert wrap_tx.description == "Wrap USDC.e -> pUSD"
        assert wrap_tx.gas_used == 150_000
        assert wrap_tx.total_cost_wei == str(150_000 * 50_000_000_000)

    def test_create_and_post_order_parses_fee_pusd(self) -> None:
        stub = MagicMock()
        stub.CreateAndPostOrder.return_value = _response_with_setup_txs(fee_pusd="0.012345")
        client = GatewayPolymarketClient(_make_gateway_client(stub))

        response = client.create_and_post_order(
            token_id="111",  # noqa: S106  (public CLOB token id, not a password)
            price=Decimal("0.50"),
            size=Decimal("10"),
            side="BUY",
        )

        assert response.fee_pusd == Decimal("0.012345")

    def test_create_and_post_market_order_parses_setup_txs(self) -> None:
        """Market orders go through the same surfacing path."""
        stub = MagicMock()
        stub.CreateAndPostMarketOrder.return_value = _response_with_setup_txs(fee_pusd="0.05")
        client = GatewayPolymarketClient(_make_gateway_client(stub))

        response = client.create_and_post_market_order(
            token_id="111",  # noqa: S106  (public CLOB token id, not a password)
            amount=Decimal("5.00"),
            side="BUY",
        )

        assert len(response.setup_txs) == 2
        assert response.fee_pusd == Decimal("0.05")


# ---------------------------------------------------------------------------
# CodeRabbit thread 5: side + market projected into OrderResponse so a SELL
# response does not silently deserialize as BUY (model defaults are
# ``side="BUY"`` / ``market=""``).
# ---------------------------------------------------------------------------


class TestSideAndMarketProjectedIntoResponse:
    """Pre-fix: ``OrderResponse.from_api_response`` defaulted ``side="BUY"``
    and ``market=""`` because the gateway client never passed them through.
    This silently deserialized every SELL response as a BUY — a critical
    bookkeeping bug for any caller that introspects ``response.side``."""

    def test_create_and_post_order_sell_response_keeps_sell_side(self) -> None:
        stub = MagicMock()
        stub.CreateAndPostOrder.return_value = _response_no_setup_txs()
        client = GatewayPolymarketClient(_make_gateway_client(stub))

        response = client.create_and_post_order(
            token_id="0xabc",  # noqa: S106  (public CLOB token id, not a password)
            price=Decimal("0.50"),
            size=Decimal("10"),
            side="SELL",
        )

        assert isinstance(response, OrderResponse)
        # Pre-fix: response.side would be "BUY" (the from_api_response default).
        assert response.side == "SELL"
        # Pre-fix: response.market would be "" (the from_api_response default).
        assert response.market == "0xabc"

    def test_create_and_post_market_order_sell_response_keeps_sell_side(self) -> None:
        """Same projection bug exists on the market-order path; verify the fix
        carries through there too."""
        stub = MagicMock()
        stub.CreateAndPostMarketOrder.return_value = _response_no_setup_txs()
        client = GatewayPolymarketClient(_make_gateway_client(stub))

        response = client.create_and_post_market_order(
            token_id="0xdef",  # noqa: S106  (public CLOB token id, not a password)
            amount=Decimal("5.00"),
            side="SELL",
        )

        assert isinstance(response, OrderResponse)
        assert response.side == "SELL"
        assert response.market == "0xdef"

    def test_create_and_post_order_buy_response_still_buy(self) -> None:
        """Sanity: BUY isn't accidentally regressed by the projection fix."""
        stub = MagicMock()
        stub.CreateAndPostOrder.return_value = _response_with_setup_txs()
        client = GatewayPolymarketClient(_make_gateway_client(stub))

        response = client.create_and_post_order(
            token_id="0x123",  # noqa: S106  (public CLOB token id, not a password)
            price=Decimal("0.50"),
            size=Decimal("10"),
            side="BUY",
        )

        assert response.side == "BUY"
        assert response.market == "0x123"


# ---------------------------------------------------------------------------
# (k) Empty proto -> empty list (backward compat)
# ---------------------------------------------------------------------------


class TestEmptySetupTxBackwardCompat:
    def test_empty_proto_field_yields_empty_list(self) -> None:
        stub = MagicMock()
        stub.CreateAndPostOrder.return_value = _response_no_setup_txs()
        client = GatewayPolymarketClient(_make_gateway_client(stub))

        response = client.create_and_post_order(
            token_id="222",
            price=Decimal("0.30"),
            size=Decimal("5"),
            side="SELL",
        )

        assert response.setup_txs == []
        # fee_pusd left empty on the wire -> None in the model.
        assert response.fee_pusd is None

    def test_empty_fee_pusd_string_yields_none(self) -> None:
        """The proto field is a string; "" must map to None on the model so
        downstream consumers can distinguish "fee not measured" from
        "fee measured to be zero"."""
        stub = MagicMock()
        stub.CreateAndPostOrder.return_value = _response_no_setup_txs()
        client = GatewayPolymarketClient(_make_gateway_client(stub))

        response = client.create_and_post_order(
            token_id="333",
            price=Decimal("0.40"),
            size=Decimal("3"),
            side="BUY",
        )

        assert response.fee_pusd is None

    def test_zero_fee_pusd_string_yields_zero_decimal(self) -> None:
        """Distinct from "" -> "0" must produce Decimal(0), not None."""
        stub = MagicMock()
        resp = _response_no_setup_txs()
        resp.fee_pusd = "0"
        stub.CreateAndPostOrder.return_value = resp
        client = GatewayPolymarketClient(_make_gateway_client(stub))

        response = client.create_and_post_order(
            token_id="444",
            price=Decimal("0.40"),
            size=Decimal("3"),
            side="BUY",
        )

        assert response.fee_pusd == Decimal("0")
        # Equality with Decimal("0") is True, but None is None — assert the
        # type stayed Decimal so the consumer never confuses the two.
        assert response.fee_pusd is not None


# ---------------------------------------------------------------------------
# Integration with from_api_response (model unit test, no gRPC)
# ---------------------------------------------------------------------------


class TestOrderResponseFromApiResponseHandlesSetupTxs:
    """Direct unit test on OrderResponse.from_api_response — the serializer
    must correctly map a dict that already contains the new keys."""

    def test_from_api_response_parses_setup_txs_dicts(self) -> None:
        data = {
            "orderID": "order-1",
            "status": "MATCHED",
            "side": "BUY",
            "price": "0.5",
            "size": "10",
            "filledSize": "10",
            "avgPrice": "0.5",
            "setup_txs": [
                {
                    "tx_hash": "0xaa",
                    "description": "approve",
                    "gas_used": 60_000,
                    "gas_price_wei": "50000000000",
                    "total_cost_wei": "3000000000000000",
                }
            ],
            "fee_pusd": "0.01",
        }
        response = OrderResponse.from_api_response(data)
        assert response.status == OrderStatus.MATCHED
        assert len(response.setup_txs) == 1
        tx = response.setup_txs[0]
        assert tx.tx_hash == "0xaa"
        assert tx.gas_used == 60_000
        assert response.fee_pusd == Decimal("0.01")

    def test_from_api_response_falls_back_to_legacy_fee_field(self) -> None:
        """Direct CLOB JSON callers may still pass ``fee`` (legacy field) —
        the parser maps it to ``fee_pusd`` when ``fee_pusd`` is absent."""
        data = {
            "orderID": "order-1",
            "status": "MATCHED",
            "side": "BUY",
            "price": "0.5",
            "size": "10",
            "filledSize": "10",
            "fee": "0.02",  # legacy
        }
        response = OrderResponse.from_api_response(data)
        assert response.fee_pusd == Decimal("0.02")

    def test_from_api_response_skips_malformed_setup_tx_entries(self) -> None:
        """Defensive: a malformed entry (e.g. None gas_used) is dropped, the
        rest survive — under-attribution beats losing the whole response."""
        data = {
            "orderID": "order-1",
            "status": "MATCHED",
            "side": "BUY",
            "price": "0.5",
            "size": "10",
            "filledSize": "10",
            "setup_txs": [
                None,  # bogus
                {
                    "tx_hash": "0xaa",
                    "description": "good",
                    "gas_used": 60_000,
                    "gas_price_wei": "50000000000",
                    "total_cost_wei": "3000000000000000",
                },
            ],
        }
        response = OrderResponse.from_api_response(data)
        assert len(response.setup_txs) == 1
        assert response.setup_txs[0].tx_hash == "0xaa"


@pytest.mark.parametrize(
    "fee_pusd_raw,expected",
    [
        ("0.05", Decimal("0.05")),
        ("0", Decimal("0")),
        ("", None),
        (None, None),
        ("not-a-number", None),
        ("-0.01", None),  # negative fees rejected
    ],
)
def test_fee_pusd_parsing(fee_pusd_raw: str | None, expected: Decimal | None) -> None:
    data = {
        "orderID": "x",
        "status": "MATCHED",
        "side": "BUY",
        "price": "0.5",
        "size": "10",
        "filledSize": "10",
        "fee_pusd": fee_pusd_raw,
    }
    response = OrderResponse.from_api_response(data)
    if expected is None:
        assert response.fee_pusd is None
    else:
        assert response.fee_pusd == expected
