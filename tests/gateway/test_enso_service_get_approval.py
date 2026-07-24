"""Branch-coverage tests for ``EnsoServiceServicer.GetApproval``.

GetApproval proxies the Enso ``/api/v1/wallet/approve`` endpoint. It
multiplexes:

  * chain-name resolution (unsupported chain short-circuit),
  * amount defaulting (empty amount -> unlimited max uint256),
  * optional ``routingStrategy`` query param,
  * upstream failure propagation (error string vs default message, falsy data),
  * Pydantic response validation failure,
  * tx-field extraction with nested-``tx`` vs top-level fallbacks.

The Enso HTTP seam (``_request``) is mocked in every test — no sockets open.
Style follows ``tests/gateway/test_enso_service.py``.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.connectors.enso.gateway.service import EnsoServiceServicer
from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2

MAX_UINT256 = str(2**256 - 1)
FROM_ADDRESS = "0x3333333333333333333333333333333333333333"
TOKEN_ADDRESS = "0x1111111111111111111111111111111111111111"
SPENDER = "0x4444444444444444444444444444444444444444"


@pytest.fixture
def enso_service():
    return EnsoServiceServicer(GatewaySettings())


@pytest.fixture
def mock_context():
    return MagicMock()


def _make_request(chain: str = "arbitrum", **overrides) -> gateway_pb2.EnsoApprovalRequest:
    fields = {
        "chain": chain,
        "token_address": TOKEN_ADDRESS,
        "from_address": FROM_ADDRESS,
    }
    fields.update(overrides)
    return gateway_pb2.EnsoApprovalRequest(**fields)


class TestGetApprovalChainResolution:
    @pytest.mark.asyncio
    async def test_unsupported_chain_short_circuits(self, enso_service, mock_context):
        request = _make_request(chain="not-a-chain")

        with patch.object(enso_service, "_request", AsyncMock()) as mock_request:
            response = await enso_service.GetApproval(request, mock_context)

        assert response.success is False
        assert response.error == "Unsupported chain: not-a-chain"
        mock_request.assert_not_called()

    @pytest.mark.asyncio
    async def test_chain_name_is_case_insensitive(self, enso_service, mock_context):
        request = _make_request(chain="ARBITRUM")

        with patch.object(
            enso_service, "_request", AsyncMock(return_value=(True, {"tx": {"to": SPENDER}}, None))
        ) as mock_request:
            response = await enso_service.GetApproval(request, mock_context)

        assert response.success is True
        assert mock_request.call_args.kwargs["params"]["chainId"] == 42161


class TestGetApprovalRequestConstruction:
    @pytest.mark.asyncio
    async def test_empty_amount_defaults_to_max_uint256(self, enso_service, mock_context):
        request = _make_request()  # amount unset -> proto default ""

        with patch.object(
            enso_service, "_request", AsyncMock(return_value=(True, {"tx": {"to": SPENDER}}, None))
        ) as mock_request:
            response = await enso_service.GetApproval(request, mock_context)

        assert response.success is True
        args, kwargs = mock_request.call_args
        assert args == ("GET", "/api/v1/wallet/approve")
        assert kwargs["params"] == {
            "chainId": 42161,
            "fromAddress": FROM_ADDRESS,
            "tokenAddress": TOKEN_ADDRESS,
            "amount": MAX_UINT256,
        }

    @pytest.mark.asyncio
    async def test_explicit_amount_forwarded_verbatim(self, enso_service, mock_context):
        request = _make_request(amount="1000000")

        with patch.object(
            enso_service, "_request", AsyncMock(return_value=(True, {"tx": {"to": SPENDER}}, None))
        ) as mock_request:
            await enso_service.GetApproval(request, mock_context)

        assert mock_request.call_args.kwargs["params"]["amount"] == "1000000"

    @pytest.mark.asyncio
    async def test_routing_strategy_included_when_set(self, enso_service, mock_context):
        request = _make_request(routing_strategy="delegate")

        with patch.object(
            enso_service, "_request", AsyncMock(return_value=(True, {"tx": {"to": SPENDER}}, None))
        ) as mock_request:
            await enso_service.GetApproval(request, mock_context)

        assert mock_request.call_args.kwargs["params"]["routingStrategy"] == "delegate"

    @pytest.mark.asyncio
    async def test_routing_strategy_omitted_when_unset(self, enso_service, mock_context):
        request = _make_request()

        with patch.object(
            enso_service, "_request", AsyncMock(return_value=(True, {"tx": {"to": SPENDER}}, None))
        ) as mock_request:
            await enso_service.GetApproval(request, mock_context)

        assert "routingStrategy" not in mock_request.call_args.kwargs["params"]


class TestGetApprovalUpstreamFailures:
    @pytest.mark.asyncio
    async def test_request_failure_propagates_error_string(self, enso_service, mock_context):
        with patch.object(
            enso_service, "_request", AsyncMock(return_value=(False, None, "HTTP 500: boom"))
        ):
            response = await enso_service.GetApproval(_make_request(), mock_context)

        assert response.success is False
        assert response.error == "HTTP 500: boom"

    @pytest.mark.asyncio
    async def test_request_failure_without_error_uses_default_message(
        self, enso_service, mock_context
    ):
        with patch.object(enso_service, "_request", AsyncMock(return_value=(False, None, None))):
            response = await enso_service.GetApproval(_make_request(), mock_context)

        assert response.success is False
        assert response.error == "Failed to get approval"

    @pytest.mark.asyncio
    async def test_success_with_falsy_data_uses_default_message(self, enso_service, mock_context):
        """success=True but empty payload still fails with the default error."""
        with patch.object(enso_service, "_request", AsyncMock(return_value=(True, {}, None))):
            response = await enso_service.GetApproval(_make_request(), mock_context)

        assert response.success is False
        assert response.error == "Failed to get approval"

    @pytest.mark.asyncio
    async def test_invalid_payload_shape_returns_validation_error(
        self, enso_service, mock_context
    ):
        """A ``tx`` that is not an object fails Pydantic validation."""
        with patch.object(
            enso_service, "_request", AsyncMock(return_value=(True, {"tx": ["not-a-dict"]}, None))
        ):
            response = await enso_service.GetApproval(_make_request(), mock_context)

        assert response.success is False
        assert response.error.startswith("Invalid API response: ")


class TestGetApprovalTxExtraction:
    @pytest.mark.asyncio
    async def test_nested_tx_fields_win(self, enso_service, mock_context):
        payload = {
            "tx": {"to": SPENDER, "data": "0xabc", "gas": "210000"},
            "to": "0x9999999999999999999999999999999999999999",
            "data": "0xdef",
            "gas": "999",
        }

        with patch.object(enso_service, "_request", AsyncMock(return_value=(True, payload, None))):
            response = await enso_service.GetApproval(_make_request(), mock_context)

        assert response.success is True
        assert response.to == SPENDER
        assert response.data == "0xabc"
        assert response.gas == "210000"

    @pytest.mark.asyncio
    async def test_top_level_fields_used_when_tx_absent(self, enso_service, mock_context):
        payload = {"to": SPENDER, "data": "0xdef", "gas": 123}

        with patch.object(enso_service, "_request", AsyncMock(return_value=(True, payload, None))):
            response = await enso_service.GetApproval(_make_request(), mock_context)

        assert response.success is True
        assert response.to == SPENDER
        assert response.data == "0xdef"
        assert response.gas == "123"

    @pytest.mark.asyncio
    async def test_empty_nested_tx_values_fall_back_per_field(self, enso_service, mock_context):
        """Each field falls back independently when the nested value is falsy."""
        payload = {
            "tx": {"to": SPENDER, "data": "", "gas": 0},
            "data": "0xtop",
            "gas": "555",
        }

        with patch.object(enso_service, "_request", AsyncMock(return_value=(True, payload, None))):
            response = await enso_service.GetApproval(_make_request(), mock_context)

        assert response.success is True
        assert response.to == SPENDER
        assert response.data == "0xtop"
        assert response.gas == "555"
