"""Branch coverage for EnsoServiceServicer.GetRoute.

Complements test_enso_service.py (amountOut normalization) with the full
GetRoute request/response contract: chain gate, slippage default, optional
routing-strategy / receiver params, cross-chain normalization (fallbacks
and same-destination collapse), upstream failure propagation, Pydantic
response validation, the price-impact threshold gate, and the full success
mapping. The Enso HTTP call is mocked at the ``_request`` seam — no
network.
"""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.connectors.enso.gateway.service import EnsoServiceServicer
from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2

FROM = "0x3333333333333333333333333333333333333333"
RECEIVER = "0x5555555555555555555555555555555555555555"
REFUND = "0x6666666666666666666666666666666666666666"
ARBITRUM_CHAIN_ID = 42161
BASE_CHAIN_ID = 8453


@pytest.fixture
def enso_service():
    return EnsoServiceServicer(GatewaySettings())


@pytest.fixture
def mock_context():
    return MagicMock()


def _request_proto(**overrides):
    fields = {
        "chain": "arbitrum",
        "token_in": "0x1111111111111111111111111111111111111111",
        "token_out": "0x2222222222222222222222222222222222222222",
        "amount_in": "1000000",
        "from_address": FROM,
    }
    fields.update(overrides)
    return gateway_pb2.EnsoRouteRequest(**fields)


def _payload(**overrides):
    data = {
        "tx": {"to": "0x4444444444444444444444444444444444444444", "data": "0xabc", "value": "7", "gas": "210000"},
        "amountOut": ["123456"],
        "priceImpact": 12,
        "gas": "250000",
        "bridgeFee": "42",
        "estimatedTime": 90,
        "route": [{"protocol": "uniswap-v3"}],
    }
    data.update(overrides)
    return data


def _patch_request(enso_service, payload=None, result=None):
    result = result if result is not None else (True, payload, None)
    return patch.object(enso_service, "_request", AsyncMock(return_value=result))


class TestChainGate:
    @pytest.mark.asyncio
    async def test_unsupported_chain_fails_without_api_call(self, enso_service, mock_context):
        with _patch_request(enso_service, _payload()) as request_mock:
            response = await enso_service.GetRoute(_request_proto(chain="junkchain"), mock_context)

        assert response.success is False
        assert response.error == "Unsupported chain: junkchain"
        request_mock.assert_not_awaited()


class TestParamAssembly:
    @pytest.mark.asyncio
    async def test_minimal_request_params(self, enso_service, mock_context):
        with _patch_request(enso_service, _payload()) as request_mock:
            response = await enso_service.GetRoute(_request_proto(), mock_context)

        assert response.success is True
        params = request_mock.await_args.kwargs["params"]
        assert params["fromAddress"] == FROM
        assert params["chainId"] == ARBITRUM_CHAIN_ID
        assert params["slippage"] == "0"  # proto default 0 is >= 0 -> passed through
        assert "routingStrategy" not in params
        assert "receiver" not in params
        assert "destinationChainId" not in params

    @pytest.mark.asyncio
    async def test_negative_slippage_falls_back_to_default(self, enso_service, mock_context):
        with _patch_request(enso_service, _payload()) as request_mock:
            await enso_service.GetRoute(_request_proto(slippage_bps=-1), mock_context)

        assert request_mock.await_args.kwargs["params"]["slippage"] == "50"

    @pytest.mark.asyncio
    async def test_routing_strategy_and_receiver_forwarded(self, enso_service, mock_context):
        with _patch_request(enso_service, _payload()) as request_mock:
            await enso_service.GetRoute(
                _request_proto(slippage_bps=75, routing_strategy="delegate", receiver=RECEIVER),
                mock_context,
            )

        params = request_mock.await_args.kwargs["params"]
        assert params["slippage"] == "75"
        assert params["routingStrategy"] == "delegate"
        assert params["receiver"] == RECEIVER


class TestCrossChain:
    @pytest.mark.asyncio
    async def test_cross_chain_falls_back_to_from_address(self, enso_service, mock_context):
        with _patch_request(enso_service, _payload()) as request_mock:
            response = await enso_service.GetRoute(
                _request_proto(destination_chain_id=BASE_CHAIN_ID), mock_context
            )

        params = request_mock.await_args.kwargs["params"]
        assert params["destinationChainId"] == BASE_CHAIN_ID
        assert params["receiver"] == FROM
        assert params["refundReceiver"] == FROM
        assert response.is_cross_chain is True

    @pytest.mark.asyncio
    async def test_cross_chain_explicit_receivers_win(self, enso_service, mock_context):
        with _patch_request(enso_service, _payload()) as request_mock:
            await enso_service.GetRoute(
                _request_proto(
                    destination_chain_id=BASE_CHAIN_ID,
                    receiver=RECEIVER,
                    refund_receiver=REFUND,
                ),
                mock_context,
            )

        params = request_mock.await_args.kwargs["params"]
        assert params["receiver"] == RECEIVER
        assert params["refundReceiver"] == REFUND

    @pytest.mark.asyncio
    async def test_destination_equal_to_source_is_same_chain(self, enso_service, mock_context):
        with _patch_request(enso_service, _payload()) as request_mock:
            response = await enso_service.GetRoute(
                _request_proto(destination_chain_id=ARBITRUM_CHAIN_ID), mock_context
            )

        params = request_mock.await_args.kwargs["params"]
        assert "destinationChainId" not in params
        assert response.is_cross_chain is False


class TestUpstreamFailures:
    @pytest.mark.asyncio
    async def test_request_error_is_propagated(self, enso_service, mock_context):
        with _patch_request(enso_service, result=(False, None, "HTTP 429: rate limited")):
            response = await enso_service.GetRoute(_request_proto(), mock_context)

        assert response.success is False
        assert response.error == "HTTP 429: rate limited"

    @pytest.mark.asyncio
    async def test_empty_data_uses_fallback_error(self, enso_service, mock_context):
        with _patch_request(enso_service, result=(True, {}, None)):
            response = await enso_service.GetRoute(_request_proto(), mock_context)

        assert response.success is False
        assert response.error == "Failed to get route"

    @pytest.mark.asyncio
    async def test_invalid_response_shape_fails_validation(self, enso_service, mock_context):
        with _patch_request(enso_service, _payload(tx="not-a-dict")):
            response = await enso_service.GetRoute(_request_proto(), mock_context)

        assert response.success is False
        assert "Invalid API response" in response.error


class TestPriceImpactGate:
    @pytest.mark.asyncio
    async def test_over_threshold_fails(self, enso_service, mock_context):
        with _patch_request(enso_service, _payload(priceImpact=120)):
            response = await enso_service.GetRoute(
                _request_proto(max_price_impact_bps=100), mock_context
            )

        assert response.success is False
        assert response.error == "Price impact 120bp exceeds threshold 100bp"

    @pytest.mark.asyncio
    async def test_at_threshold_passes(self, enso_service, mock_context):
        with _patch_request(enso_service, _payload(priceImpact=100)):
            response = await enso_service.GetRoute(
                _request_proto(max_price_impact_bps=100), mock_context
            )

        assert response.success is True
        assert response.price_impact == 100

    @pytest.mark.asyncio
    async def test_zero_threshold_disables_gate(self, enso_service, mock_context):
        with _patch_request(enso_service, _payload(priceImpact=9999)):
            response = await enso_service.GetRoute(_request_proto(), mock_context)

        assert response.success is True
        assert response.price_impact == 9999


class TestSuccessMapping:
    @pytest.mark.asyncio
    async def test_full_field_mapping(self, enso_service, mock_context):
        with _patch_request(enso_service, _payload()):
            response = await enso_service.GetRoute(_request_proto(), mock_context)

        assert response.success is True
        assert response.to == "0x4444444444444444444444444444444444444444"
        assert response.data == "0xabc"
        assert response.value == "7"
        assert response.gas == "210000"
        assert response.amount_out == "123456"
        assert response.price_impact == 12
        assert response.gas_estimate == "250000"
        assert response.bridge_fee == "42"
        assert response.estimated_time == 90
        assert json.loads(response.route_json) == [{"protocol": "uniswap-v3"}]

    @pytest.mark.asyncio
    async def test_missing_optionals_default_to_zero(self, enso_service, mock_context):
        with _patch_request(enso_service, _payload(priceImpact=None, estimatedTime=None)):
            response = await enso_service.GetRoute(_request_proto(), mock_context)

        assert response.success is True
        assert response.price_impact == 0
        assert response.estimated_time == 0
