"""Tests for EnsoService gateway implementation."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.connectors.enso.gateway.service import (
    CHAIN_MAPPING,
    EnsoServiceServicer,
    _decode_bundle_arg,
)
from almanak.framework.connectors.enso.client import (
    CHAIN_MAPPING as SDK_CHAIN_MAPPING,
)
from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2


@pytest.fixture
def settings():
    """Create test settings."""
    return GatewaySettings()


@pytest.fixture
def enso_service(settings):
    """Create EnsoService instance."""
    return EnsoServiceServicer(settings)


@pytest.fixture
def mock_context():
    """Create mock gRPC context."""
    context = MagicMock()
    context.set_code = MagicMock()
    context.set_details = MagicMock()
    return context


class TestEnsoServiceAmountOutNormalization:
    """Ensure amountOut supports scalar and list response formats."""

    @pytest.mark.asyncio
    async def test_get_route_accepts_scalar_amount_out(self, enso_service, mock_context):
        """GetRoute should accept scalar amountOut payloads."""
        request = gateway_pb2.EnsoRouteRequest(
            chain="arbitrum",
            token_in="0x1111111111111111111111111111111111111111",
            token_out="0x2222222222222222222222222222222222222222",
            amount_in="1000000",
            from_address="0x3333333333333333333333333333333333333333",
        )
        mock_payload = {
            "tx": {"to": "0x4444444444444444444444444444444444444444", "data": "0xabc", "value": "0", "gas": "210000"},
            "amountOut": "123456",
            "priceImpact": 12,
            "gas": "210000",
            "bridgeFee": "0",
            "route": [],
        }

        with patch.object(enso_service, "_request", AsyncMock(return_value=(True, mock_payload, None))):
            response = await enso_service.GetRoute(request, mock_context)

        assert response.success is True
        assert response.amount_out == "123456"

    @pytest.mark.asyncio
    async def test_get_quote_accepts_scalar_amount_out(self, enso_service, mock_context):
        """GetQuote should accept scalar amountOut payloads."""
        request = gateway_pb2.EnsoQuoteRequest(
            chain="arbitrum",
            token_in="0x1111111111111111111111111111111111111111",
            token_out="0x2222222222222222222222222222222222222222",
            amount_in="1000000",
            from_address="0x3333333333333333333333333333333333333333",
        )
        mock_payload = {
            "amountOut": 987654,
            "priceImpact": 8,
            "gas": "190000",
        }

        with patch.object(enso_service, "_request", AsyncMock(return_value=(True, mock_payload, None))):
            response = await enso_service.GetQuote(request, mock_context)

        assert response.success is True
        assert response.amount_out == "987654"

    @pytest.mark.asyncio
    async def test_get_quote_still_supports_list_amount_out(self, enso_service, mock_context):
        """GetQuote should continue to support list amountOut payloads."""
        request = gateway_pb2.EnsoQuoteRequest(
            chain="arbitrum",
            token_in="0x1111111111111111111111111111111111111111",
            token_out="0x2222222222222222222222222222222222222222",
            amount_in="1000000",
            from_address="0x3333333333333333333333333333333333333333",
        )
        mock_payload = {
            "amountOut": ["555"],
            "priceImpact": 5,
            "gas": "170000",
        }

        with patch.object(enso_service, "_request", AsyncMock(return_value=(True, mock_payload, None))):
            response = await enso_service.GetQuote(request, mock_context)

        assert response.success is True
        assert response.amount_out == "555"


class TestBundleArgDecoding:
    """``_decode_bundle_arg`` reverses client-side JSON encoding so native
    types survive the proto map<string,string> round-trip to Enso."""

    @pytest.mark.parametrize(
        "encoded,expected",
        [
            (json.dumps("0x1234"), "0x1234"),
            (json.dumps(1000), 1000),
            (json.dumps(True), True),
            (json.dumps(False), False),
            (json.dumps(None), None),
            (json.dumps([1, 2, 3]), [1, 2, 3]),
            (json.dumps({"nested": "dict"}), {"nested": "dict"}),
        ],
    )
    def test_roundtrip_native_types(self, encoded, expected):
        assert _decode_bundle_arg(encoded) == expected

    def test_invalid_json_falls_back_to_raw_string(self):
        # A malformed payload (e.g., a legacy client that stuffed an unquoted
        # string into the map) must fall back to the raw value so we don't
        # break loose clients with a 500.
        assert _decode_bundle_arg("not-json") == "not-json"

    def test_empty_string_returns_empty(self):
        assert _decode_bundle_arg("") == ""

    @pytest.mark.asyncio
    async def test_get_bundle_decodes_args_before_forwarding(self, enso_service, mock_context):
        """GetBundle should JSON-decode action args before calling the Enso API."""
        action = gateway_pb2.EnsoBundleAction(
            protocol="aave-v3",
            action="deposit",
            args={
                "amount": json.dumps("1000000"),
                "deadline": json.dumps(1_700_000_000),
                "permit": json.dumps(True),
                "path": json.dumps(["0xA", "0xB"]),
            },
        )
        request = gateway_pb2.EnsoBundleRequest(
            chain="arbitrum",
            from_address="0x3333333333333333333333333333333333333333",
            actions=[action],
        )
        mock_payload = {
            "tx": {
                "to": "0x4444444444444444444444444444444444444444",
                "data": "0xabc",
                "value": "0",
                "gas": "210000",
            }
        }

        captured: dict = {}

        async def capture_request(method, path, params=None, json_body=None):
            captured["json_body"] = json_body
            return (True, mock_payload, None)

        with patch.object(enso_service, "_request", side_effect=capture_request):
            response = await enso_service.GetBundle(request, mock_context)

        assert response.success is True
        forwarded_args = captured["json_body"][0]["args"]
        assert forwarded_args["amount"] == "1000000"  # string preserved
        assert forwarded_args["deadline"] == 1_700_000_000  # int decoded
        assert forwarded_args["permit"] is True  # bool decoded
        assert forwarded_args["path"] == ["0xA", "0xB"]  # list decoded


class TestEnsoServiceChainMapping:
    """Guard the gateway chain allowlist against drift from the SDK client."""

    def test_chain_mapping_matches_sdk_client(self):
        """Gateway and SDK client must agree on supported Enso chains.

        Drift caused VIB-3715 / BUG-37: Berachain present on the SDK side but
        missing from the gateway, breaking edge_discovery_berachain at compile
        time. Keep the two maps locked together.
        """
        assert CHAIN_MAPPING == SDK_CHAIN_MAPPING, (
            f"Gateway Enso CHAIN_MAPPING diverges from SDK client. "
            f"Only-in-gateway: {set(CHAIN_MAPPING) - set(SDK_CHAIN_MAPPING)} | "
            f"Only-in-SDK: {set(SDK_CHAIN_MAPPING) - set(CHAIN_MAPPING)}"
        )

    def test_berachain_resolves(self, enso_service):
        """Berachain (chain_id=80094) must resolve through _get_chain_id."""
        assert enso_service._get_chain_id("berachain") == 80094
        assert enso_service._get_chain_id("BERACHAIN") == 80094

    def test_unknown_chain_returns_none(self, enso_service):
        assert enso_service._get_chain_id("does-not-exist") is None
