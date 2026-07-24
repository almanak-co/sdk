"""Branch coverage for EnsoClient's direct-HTTP transport paths.

Complements test_enso_client_gateway.py (which owns the gateway gRPC
transport) with the requests-based fallback:

- ``_make_request``: GET vs JSON-body dispatch, HTTPError wrapping (with and
  without a decodable error body), transport-error wrapping;
- ``get_route``: param assembly (defaults, receiver, cross-chain
  normalization, same-destination collapse), price-impact threshold gate,
  and the gateway-delegation branch;
- ``get_bundle``: param/action serialization, skip_quote flag, non-200
  error wrapping, and the gateway-delegation branch.

All HTTP is mocked at the session / requests.post seam — no network.
"""

from unittest.mock import MagicMock, patch

import pytest
import requests

from almanak.connectors.enso.client import EnsoClient, EnsoConfig
from almanak.connectors.enso.exceptions import (
    EnsoAPIError,
    PriceImpactExceedsThresholdError,
)
from almanak.connectors.enso.models import BundleAction, RouteTransaction, RoutingStrategy

WALLET = "0x" + "11" * 20
OTHER = "0x" + "22" * 20
REFUND = "0x" + "33" * 20
ARBITRUM_CHAIN_ID = 42161
BASE_CHAIN_ID = 8453


@pytest.fixture
def client():
    config = EnsoConfig(chain="arbitrum", wallet_address=WALLET, api_key="test-key")
    return EnsoClient(config)


def _route_response(**overrides):
    data = {
        "gas": "210000",
        "tx": {"to": "0xROUTER", "data": "0xdead", "from": WALLET, "value": "0"},
        "amountOut": "500000000000000000",
        "priceImpact": 12,
    }
    data.update(overrides)
    return data


# ---------------------------------------------------------------------------
# _make_request
# ---------------------------------------------------------------------------


class TestMakeRequest:
    def test_get_without_body_returns_json(self, client):
        response = MagicMock()
        response.json.return_value = {"ok": True}
        with patch.object(client.session, "request", return_value=response) as request:
            result = client._make_request("GET", "/api/v1/shortcuts/route", params={"a": 1})

        assert result == {"ok": True}
        request.assert_called_once_with(
            "GET",
            "https://api.enso.finance/api/v1/shortcuts/route",
            params={"a": 1},
            timeout=client.config.timeout,
        )

    def test_json_body_is_forwarded(self, client):
        response = MagicMock()
        response.json.return_value = {"ok": True}
        with patch.object(client.session, "request", return_value=response) as request:
            client._make_request("POST", "/api/v1/x", params=None, json_data=[{"p": 1}])

        assert request.call_args.kwargs["json"] == [{"p": 1}]

    def test_http_error_wraps_status_and_error_data(self, client):
        response = MagicMock()
        response.status_code = 400
        response.raise_for_status.side_effect = requests.exceptions.HTTPError("400 Client Error")
        response.json.return_value = {"error": "Insufficient liquidity"}
        with patch.object(client.session, "request", return_value=response):
            with pytest.raises(EnsoAPIError) as exc_info:
                client._make_request("GET", "/api/v1/shortcuts/route")

        err = exc_info.value
        assert err.status_code == 400
        assert err.endpoint == "/api/v1/shortcuts/route"
        assert err.api_error_message == "Insufficient liquidity"

    def test_http_error_with_undecodable_body_keeps_error_data_none(self, client):
        response = MagicMock()
        response.status_code = 502
        response.raise_for_status.side_effect = requests.exceptions.HTTPError("502 Bad Gateway")
        response.json.side_effect = ValueError("not json")
        with patch.object(client.session, "request", return_value=response):
            with pytest.raises(EnsoAPIError) as exc_info:
                client._make_request("GET", "/api/v1/shortcuts/route")

        assert exc_info.value.status_code == 502
        assert exc_info.value.error_data is None

    def test_transport_error_maps_to_status_zero(self, client):
        with patch.object(
            client.session,
            "request",
            side_effect=requests.exceptions.ConnectionError("dns failure"),
        ):
            with pytest.raises(EnsoAPIError) as exc_info:
                client._make_request("GET", "/api/v1/shortcuts/quote")

        assert exc_info.value.status_code == 0
        assert exc_info.value.endpoint == "/api/v1/shortcuts/quote"


# ---------------------------------------------------------------------------
# get_route (direct HTTP)
# ---------------------------------------------------------------------------


class TestGetRouteDirect:
    def test_same_chain_defaults(self, client):
        with patch.object(client, "_make_request", return_value=_route_response()) as make_request:
            route = client.get_route(token_in="0xIN", token_out="0xOUT", amount_in=1000)

        method, endpoint = make_request.call_args.args
        params = make_request.call_args.kwargs["params"]
        assert (method, endpoint) == ("GET", "/api/v1/shortcuts/route")
        assert params["fromAddress"] == WALLET
        assert params["tokenIn"] == ["0xIN"]
        assert params["tokenOut"] == ["0xOUT"]
        assert params["amountIn"] == ["1000"]
        assert params["chainId"] == ARBITRUM_CHAIN_ID
        assert params["slippage"] == "50"
        assert params["routingStrategy"] == RoutingStrategy.ROUTER.value
        assert "receiver" not in params
        assert "destinationChainId" not in params
        assert route.chain_id == ARBITRUM_CHAIN_ID
        assert route.destination_chain_id is None
        assert route.price_impact == 12

    def test_same_chain_receiver_and_overrides(self, client):
        with patch.object(client, "_make_request", return_value=_route_response()) as make_request:
            client.get_route(
                token_in="0xIN",
                token_out="0xOUT",
                amount_in=1000,
                slippage_bps=75,
                from_address=OTHER,
                receiver=REFUND,
                routing_strategy=RoutingStrategy.DELEGATE,
            )

        params = make_request.call_args.kwargs["params"]
        assert params["fromAddress"] == OTHER
        assert params["receiver"] == REFUND
        assert params["slippage"] == "75"
        assert params["routingStrategy"] == RoutingStrategy.DELEGATE.value

    def test_destination_equal_to_source_collapses_to_same_chain(self, client):
        with patch.object(client, "_make_request", return_value=_route_response()) as make_request:
            route = client.get_route(
                token_in="0xIN",
                token_out="0xOUT",
                amount_in=1000,
                destination_chain_id=ARBITRUM_CHAIN_ID,
            )

        params = make_request.call_args.kwargs["params"]
        assert "destinationChainId" not in params
        assert "refundReceiver" not in params
        assert route.destination_chain_id is None

    def test_cross_chain_inherits_from_address_into_receivers(self, client):
        with patch.object(client, "_make_request", return_value=_route_response()) as make_request:
            route = client.get_route(
                token_in="0xIN",
                token_out="0xOUT",
                amount_in=1000,
                destination_chain_id=BASE_CHAIN_ID,
            )

        params = make_request.call_args.kwargs["params"]
        assert params["destinationChainId"] == BASE_CHAIN_ID
        assert params["receiver"] == WALLET
        assert params["refundReceiver"] == WALLET
        assert route.destination_chain_id == BASE_CHAIN_ID
        assert route.is_cross_chain is True

    def test_cross_chain_explicit_receivers_win(self, client):
        with patch.object(client, "_make_request", return_value=_route_response()) as make_request:
            client.get_route(
                token_in="0xIN",
                token_out="0xOUT",
                amount_in=1000,
                destination_chain_id=BASE_CHAIN_ID,
                receiver=OTHER,
                refund_receiver=REFUND,
            )

        params = make_request.call_args.kwargs["params"]
        assert params["receiver"] == OTHER
        assert params["refundReceiver"] == REFUND

    def test_price_impact_over_threshold_raises(self, client):
        with patch.object(client, "_make_request", return_value=_route_response(priceImpact=120)):
            with pytest.raises(PriceImpactExceedsThresholdError) as exc_info:
                client.get_route(
                    token_in="0xIN",
                    token_out="0xOUT",
                    amount_in=1000,
                    max_price_impact_bps=100,
                )

        assert exc_info.value.price_impact_bps == 120
        assert exc_info.value.threshold_bps == 100

    def test_price_impact_at_threshold_passes(self, client):
        with patch.object(client, "_make_request", return_value=_route_response(priceImpact=100)):
            route = client.get_route(
                token_in="0xIN",
                token_out="0xOUT",
                amount_in=1000,
                max_price_impact_bps=100,
            )

        assert route.price_impact == 100

    def test_unknown_price_impact_skips_threshold_check(self, client):
        with patch.object(client, "_make_request", return_value=_route_response(priceImpact=None)):
            route = client.get_route(
                token_in="0xIN",
                token_out="0xOUT",
                amount_in=1000,
                max_price_impact_bps=10,
            )

        assert route.price_impact is None


class TestGetRouteGatewayDelegation:
    def test_gateway_branch_normalizes_cross_chain_args(self):
        config = EnsoConfig(
            chain="arbitrum",
            wallet_address=WALLET,
            gateway_client=MagicMock(),
        )
        client = EnsoClient(config)
        route = RouteTransaction.from_api_response(_route_response())
        with patch.object(client, "_get_route_via_gateway", return_value=route) as via_gateway:
            result = client.get_route(
                token_in="0xIN",
                token_out="0xOUT",
                amount_in=1000,
                destination_chain_id=BASE_CHAIN_ID,
            )

        assert result is route
        kwargs = via_gateway.call_args.kwargs
        assert kwargs["destination_chain_id"] == BASE_CHAIN_ID
        assert kwargs["receiver"] == WALLET
        assert kwargs["refund_receiver"] == WALLET

    def test_gateway_branch_same_chain_leaves_receiver_unset(self):
        config = EnsoConfig(
            chain="arbitrum",
            wallet_address=WALLET,
            gateway_client=MagicMock(),
        )
        client = EnsoClient(config)
        route = RouteTransaction.from_api_response(_route_response())
        with patch.object(client, "_get_route_via_gateway", return_value=route) as via_gateway:
            client.get_route(token_in="0xIN", token_out="0xOUT", amount_in=1000)

        kwargs = via_gateway.call_args.kwargs
        assert kwargs["destination_chain_id"] is None
        assert kwargs["receiver"] is None
        assert kwargs["refund_receiver"] is None


# ---------------------------------------------------------------------------
# get_bundle (direct HTTP)
# ---------------------------------------------------------------------------


def _bundle_action():
    return BundleAction(
        protocol="enso",
        action="route",
        args={"tokenIn": "0xIN", "tokenOut": "0xOUT", "amountIn": "1000"},
    )


class TestGetBundleDirect:
    def test_posts_actions_and_returns_json(self, client):
        response = MagicMock()
        response.status_code = 200
        response.json.return_value = {"tx": {"to": "0xROUTER"}}
        action = _bundle_action()
        with patch("almanak.connectors.enso.client.requests.post", return_value=response) as post:
            result = client.get_bundle([action])

        assert result == {"tx": {"to": "0xROUTER"}}
        assert post.call_args.args == ("https://api.enso.finance/api/v1/shortcuts/bundle",)
        kwargs = post.call_args.kwargs
        assert kwargs["params"] == {
            "chainId": ARBITRUM_CHAIN_ID,
            "fromAddress": WALLET,
            "routingStrategy": RoutingStrategy.ROUTER.value,
        }
        assert kwargs["json"] == [action.to_api_format()]
        assert kwargs["headers"]["Authorization"] == "Bearer test-key"

    def test_skip_quote_and_overrides_are_forwarded(self, client):
        response = MagicMock()
        response.status_code = 200
        response.json.return_value = {}
        with patch("almanak.connectors.enso.client.requests.post", return_value=response) as post:
            client.get_bundle(
                [_bundle_action()],
                from_address=OTHER,
                routing_strategy=RoutingStrategy.DELEGATE,
                skip_quote=True,
            )

        params = post.call_args.kwargs["params"]
        assert params["skipQuote"] is True
        assert params["fromAddress"] == OTHER
        assert params["routingStrategy"] == RoutingStrategy.DELEGATE.value

    def test_non_200_with_json_body_raises_with_error_data(self, client):
        response = MagicMock()
        response.status_code = 400
        response.text = '{"message": "bad bundle"}'
        response.json.return_value = {"message": "bad bundle"}
        with patch("almanak.connectors.enso.client.requests.post", return_value=response):
            with pytest.raises(EnsoAPIError) as exc_info:
                client.get_bundle([_bundle_action()])

        err = exc_info.value
        assert err.status_code == 400
        assert err.endpoint == "/api/v1/shortcuts/bundle"
        assert err.api_error_message == "bad bundle"

    def test_non_200_with_undecodable_body_keeps_error_data_none(self, client):
        response = MagicMock()
        response.status_code = 500
        response.text = "internal error"
        response.json.side_effect = ValueError("not json")
        with patch("almanak.connectors.enso.client.requests.post", return_value=response):
            with pytest.raises(EnsoAPIError) as exc_info:
                client.get_bundle([_bundle_action()])

        assert exc_info.value.status_code == 500
        assert exc_info.value.error_data is None

    def test_gateway_branch_delegates(self):
        config = EnsoConfig(
            chain="arbitrum",
            wallet_address=WALLET,
            gateway_client=MagicMock(),
        )
        client = EnsoClient(config)
        action = _bundle_action()
        with patch.object(client, "_get_bundle_via_gateway", return_value={"ok": True}) as via_gateway:
            result = client.get_bundle([action], skip_quote=True)

        assert result == {"ok": True}
        kwargs = via_gateway.call_args.kwargs
        assert kwargs["bundle_actions"] == [action]
        assert kwargs["from_addr"] == WALLET
        assert kwargs["skip_quote"] is True
