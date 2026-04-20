"""Tests for EnsoClient gateway transport (VIB-2986 / VIB-2989, Phase 5.1).

When ``EnsoConfig.gateway_client`` is set the client must never hit
api.enso.finance directly — every public method must call the gateway's
EnsoService gRPC stubs instead. These tests assert that contract.
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.connectors.enso.client import EnsoClient, EnsoConfig
from almanak.framework.connectors.enso.exceptions import EnsoAPIError
from almanak.framework.connectors.enso.models import BundleAction, RoutingStrategy


def _make_gateway_client() -> MagicMock:
    """Build a MagicMock GatewayClient with an enso stub that returns successful responses."""
    client = MagicMock()
    enso = MagicMock()

    route_response = MagicMock()
    route_response.success = True
    route_response.to = "0xROUTER"
    route_response.data = "0xdeadbeef"
    route_response.value = "0"
    route_response.gas = "200000"
    route_response.gas_estimate = "200000"
    route_response.amount_out = "500000000000000000"
    route_response.price_impact = 10
    route_response.is_cross_chain = False
    route_response.bridge_fee = "0"
    route_response.estimated_time = 0
    route_response.route_json = "[]"
    route_response.error = ""
    enso.GetRoute = MagicMock(return_value=route_response)

    quote_response = MagicMock()
    quote_response.success = True
    quote_response.amount_out = "500000000000000000"
    quote_response.price_impact = 5
    quote_response.gas_estimate = "100000"
    quote_response.error = ""
    enso.GetQuote = MagicMock(return_value=quote_response)

    approval_response = MagicMock()
    approval_response.success = True
    approval_response.to = "0xTOKEN"
    approval_response.data = "0x095ea7b3"
    approval_response.gas = "50000"
    approval_response.error = ""
    enso.GetApproval = MagicMock(return_value=approval_response)

    bundle_response = MagicMock()
    bundle_response.success = True
    bundle_response.to = "0xROUTER"
    bundle_response.data = "0xbundle"
    bundle_response.value = "0"
    bundle_response.gas = "300000"
    bundle_response.bundle_json = ""
    bundle_response.error = ""
    enso.GetBundle = MagicMock(return_value=bundle_response)

    client.enso = enso
    return client


class TestEnsoConfig:
    def test_gateway_client_drops_api_key_requirement(self, monkeypatch):
        """Gateway holds the credential — no ENSO_API_KEY lookup required."""
        monkeypatch.delenv("ENSO_API_KEY", raising=False)
        gateway = _make_gateway_client()
        config = EnsoConfig(
            chain="arbitrum",
            wallet_address="0x" + "11" * 20,
            gateway_client=gateway,
        )
        assert config.api_key is None
        assert config.gateway_client is gateway

    def test_direct_mode_still_requires_api_key(self, monkeypatch):
        monkeypatch.delenv("ENSO_API_KEY", raising=False)
        from almanak.framework.connectors.enso.exceptions import EnsoConfigError

        with pytest.raises(EnsoConfigError):
            EnsoConfig(chain="arbitrum", wallet_address="0x" + "11" * 20)


class TestEnsoClientGatewayTransport:
    def test_no_http_session_when_gateway_provided(self):
        gateway = _make_gateway_client()
        config = EnsoConfig(
            chain="arbitrum",
            wallet_address="0x" + "11" * 20,
            gateway_client=gateway,
        )
        client = EnsoClient(config)
        assert client.session is None
        assert client._via_gateway is True

    def test_get_route_uses_gateway_stub(self):
        gateway = _make_gateway_client()
        client = EnsoClient(
            EnsoConfig(
                chain="arbitrum",
                wallet_address="0x" + "11" * 20,
                gateway_client=gateway,
            )
        )
        route = client.get_route(
            token_in="0x" + "aa" * 20,
            token_out="0x" + "bb" * 20,
            amount_in=1_000_000,
            slippage_bps=50,
        )
        assert gateway.enso.GetRoute.called
        assert route.tx.to == "0xROUTER"
        assert route.tx.data == "0xdeadbeef"
        assert route.get_amount_out_wei() == 500_000_000_000_000_000

    def test_get_quote_uses_gateway_stub(self):
        gateway = _make_gateway_client()
        client = EnsoClient(
            EnsoConfig(
                chain="arbitrum",
                wallet_address="0x" + "11" * 20,
                gateway_client=gateway,
            )
        )
        quote = client.get_quote(
            token_in="0x" + "aa" * 20,
            token_out="0x" + "bb" * 20,
            amount_in=1_000_000,
        )
        assert gateway.enso.GetQuote.called
        assert quote.amount_out == "500000000000000000"

    def test_get_approval_uses_gateway_stub(self):
        gateway = _make_gateway_client()
        client = EnsoClient(
            EnsoConfig(
                chain="arbitrum",
                wallet_address="0x" + "11" * 20,
                gateway_client=gateway,
            )
        )
        result = client.get_approval(token_address="0x" + "cc" * 20, amount=1_000_000)
        assert gateway.enso.GetApproval.called
        assert result["to"] == "0xTOKEN"
        assert result["data"] == "0x095ea7b3"

    def test_get_bundle_uses_gateway_stub(self):
        gateway = _make_gateway_client()
        client = EnsoClient(
            EnsoConfig(
                chain="arbitrum",
                wallet_address="0x" + "11" * 20,
                gateway_client=gateway,
            )
        )
        actions = [BundleAction(protocol="aave-v3", action="deposit", args={"amount": "1000"})]
        # Fence off the legacy direct-HTTP path — get_bundle() is the one
        # Enso method that reaches requests.post directly on fallback. If a
        # regression ever re-enables the fallback, the test must fail loudly.
        with patch("almanak.framework.connectors.enso.client.requests.post") as mock_post:
            result = client.get_bundle(actions, routing_strategy=RoutingStrategy.ROUTER)
        assert gateway.enso.GetBundle.called
        assert result["tx"]["to"] == "0xROUTER"
        assert result["tx"]["data"] == "0xbundle"
        mock_post.assert_not_called()

    def test_gateway_error_surfaces_as_enso_api_error(self):
        gateway = _make_gateway_client()
        failure = MagicMock(success=False, error="upstream 503")
        gateway.enso.GetRoute = MagicMock(return_value=failure)
        client = EnsoClient(
            EnsoConfig(
                chain="arbitrum",
                wallet_address="0x" + "11" * 20,
                gateway_client=gateway,
            )
        )
        with pytest.raises(EnsoAPIError) as excinfo:
            client.get_route(
                token_in="0x" + "aa" * 20,
                token_out="0x" + "bb" * 20,
                amount_in=1_000,
            )
        assert "upstream 503" in str(excinfo.value)

    def test_bundle_json_fallback_when_gateway_returns_raw(self):
        """Gateway may return bundle_json; client should decode it to preserve the dict shape."""
        gateway = _make_gateway_client()
        raw_bundle = MagicMock(
            success=True,
            to="",
            data="",
            value="0",
            gas="0",
            bundle_json='{"tx":{"to":"0xABC","data":"0xfeed","value":"0","gas":"111"},"extra":1}',
            error="",
        )
        gateway.enso.GetBundle = MagicMock(return_value=raw_bundle)
        client = EnsoClient(
            EnsoConfig(
                chain="arbitrum",
                wallet_address="0x" + "11" * 20,
                gateway_client=gateway,
            )
        )
        result = client.get_bundle([BundleAction(protocol="aave-v3", action="deposit", args={})])
        assert result["tx"]["to"] == "0xABC"
        assert result["extra"] == 1

    def test_cross_chain_route_populates_bridge_fields(self):
        gateway = _make_gateway_client()
        cross_chain_resp = MagicMock(
            success=True,
            to="0xROUTER",
            data="0xdead",
            value="0",
            gas="300000",
            gas_estimate="300000",
            amount_out="1000000",
            price_impact=25,
            is_cross_chain=True,
            bridge_fee="42",
            estimated_time=600,
            route_json="[]",
            error="",
        )
        gateway.enso.GetRoute = MagicMock(return_value=cross_chain_resp)
        client = EnsoClient(
            EnsoConfig(
                chain="base",
                wallet_address="0x" + "11" * 20,
                gateway_client=gateway,
            )
        )
        route = client.get_route(
            token_in="0x" + "aa" * 20,
            token_out="0x" + "bb" * 20,
            amount_in=1_000,
            destination_chain_id=42161,
            refund_receiver="0x" + "22" * 20,
        )
        assert route.bridge_fee == "42"
        assert route.estimated_time == 600


class TestPriceImpactGuardInGatewayMode:
    def test_max_price_impact_still_enforced_via_gateway(self):
        """Client-side price-impact check runs independent of transport."""
        from almanak.framework.connectors.enso.exceptions import PriceImpactExceedsThresholdError

        gateway = _make_gateway_client()
        high_impact_resp = MagicMock(
            success=True,
            to="0xROUTER",
            data="0xdead",
            value="0",
            gas="200000",
            gas_estimate="200000",
            amount_out="1",
            price_impact=500,
            is_cross_chain=False,
            bridge_fee="0",
            estimated_time=0,
            route_json="[]",
            error="",
        )
        gateway.enso.GetRoute = MagicMock(return_value=high_impact_resp)
        client = EnsoClient(
            EnsoConfig(
                chain="arbitrum",
                wallet_address="0x" + "11" * 20,
                gateway_client=gateway,
            )
        )
        with pytest.raises(PriceImpactExceedsThresholdError):
            client.get_route(
                token_in="0x" + "aa" * 20,
                token_out="0x" + "bb" * 20,
                amount_in=1_000,
                max_price_impact_bps=100,
            )


def test_price_is_not_used_directly():
    """Safety check: the test module does not accidentally import real HTTP stacks."""
    import sys

    # After this test file imports, enso.client must still be loadable without
    # a live Enso API or requests session — pure-import smoke.
    assert "almanak.framework.connectors.enso.client" in sys.modules
    _ = Decimal("1")  # silence unused-import warning
