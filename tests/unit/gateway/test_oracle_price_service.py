"""Provider-neutral RateHistoryService oracle RPC contracts."""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import patch

import grpc
import pytest

from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.rate_history_service import RateHistoryServiceServicer
from almanak.integrations.chainlink.gateway.history import HistoricalPricePage, HistoricalPricePoint


class Context:
    code = None
    details = None

    def set_code(self, code) -> None:
        self.code = code

    def set_details(self, details) -> None:
        self.details = details


class Reader:
    async def get_latest(self, *, token: str) -> HistoricalPricePoint:
        return HistoricalPricePoint(timestamp=100, price=Decimal("12.34"), observation_id=7)

    async def get_history_page(self, *, token: str, start_ts: int, end_ts: int, max_points: int):
        return HistoricalPricePage(
            points=[HistoricalPricePoint(timestamp=start_ts, price=Decimal("10"), observation_id=6)]
        )

    async def close(self) -> None:
        return None


def _service() -> RateHistoryServiceServicer:
    service = RateHistoryServiceServicer.__new__(RateHistoryServiceServicer)
    service._oracle_history_providers = {}
    service._oracle_history_reader = lambda provider, chain: Reader()
    return service


@pytest.mark.asyncio
async def test_current_oracle_response_is_provider_exact() -> None:
    context = Context()
    response = await _service().GetOraclePrice(
        gateway_pb2.GetOraclePriceRequest(provider="chainlink", chain="ethereum", token="eth"),
        context,
    )
    assert response.success
    assert response.provider == "chainlink"
    assert response.point.price == "12.34"
    assert response.point.observation_id == "7"
    assert context.code is None


@pytest.mark.asyncio
async def test_history_response_preserves_provider_observation_id() -> None:
    context = Context()
    response = await _service().GetOraclePriceHistory(
        gateway_pb2.GetOraclePriceHistoryRequest(
            provider="chainlink",
            chain="ethereum",
            token="ETH",
            start_ts=100,
            end_ts=200,
            max_points=10,
        ),
        context,
    )
    assert response.success
    assert [(point.timestamp, point.price, point.observation_id) for point in response.points] == [(100, "10", "6")]


@pytest.mark.asyncio
async def test_unknown_oracle_provider_is_invalid_argument() -> None:
    context = Context()
    response = await _service().GetOraclePrice(
        gateway_pb2.GetOraclePriceRequest(provider="unknown", chain="ethereum", token="ETH"),
        context,
    )
    assert not response.success
    assert context.code is grpc.StatusCode.INVALID_ARGUMENT
    assert "unknown oracle provider" in response.error


@pytest.mark.asyncio
async def test_history_request_bounds_are_enforced() -> None:
    context = Context()
    response = await _service().GetOraclePriceHistory(
        gateway_pb2.GetOraclePriceHistoryRequest(
            provider="chainlink",
            chain="ethereum",
            token="ETH",
            start_ts=1,
            end_ts=367 * 86_400,
            max_points=10,
        ),
        context,
    )
    assert not response.success
    assert context.code is grpc.StatusCode.INVALID_ARGUMENT
    assert "366 days" in response.error


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("start_ts", "end_ts", "max_points", "message"),
    [
        (0, 2, 1, "positive increasing"),
        (2, 1, 1, "positive increasing"),
        (1, 2, 0, "between 1 and 10000"),
        (1, 2, 10_001, "between 1 and 10000"),
    ],
)
async def test_history_request_shape_and_point_bounds_are_enforced(
    start_ts: int,
    end_ts: int,
    max_points: int,
    message: str,
) -> None:
    context = Context()
    response = await _service().GetOraclePriceHistory(
        gateway_pb2.GetOraclePriceHistoryRequest(
            provider="chainlink",
            chain="ethereum",
            token="ETH",
            start_ts=start_ts,
            end_ts=end_ts,
            max_points=max_points,
        ),
        context,
    )
    assert not response.success
    assert context.code is grpc.StatusCode.INVALID_ARGUMENT
    assert message in response.error


@pytest.mark.asyncio
async def test_history_response_exposes_truncation_and_split_cursor() -> None:
    class PageReader(Reader):
        async def get_history_page(self, *, token: str, start_ts: int, end_ts: int, max_points: int):
            return HistoricalPricePage(
                points=[HistoricalPricePoint(timestamp=start_ts, price=Decimal("10"), observation_id=6)],
                truncated=True,
                recommended_split_ts=150,
            )

    service = _service()
    service._oracle_history_reader = lambda provider, chain: PageReader()
    response = await service.GetOraclePriceHistory(
        gateway_pb2.GetOraclePriceHistoryRequest(
            provider="chainlink",
            chain="ethereum",
            token="ETH",
            start_ts=100,
            end_ts=200,
            max_points=10,
        ),
        Context(),
    )
    assert response.success
    assert response.truncated is True
    assert response.recommended_split_ts == 150


@pytest.mark.asyncio
async def test_registry_validation_failure_is_redacted_internal() -> None:
    context = Context()
    with patch(
        "almanak.gateway.services.rate_history_service.INTEGRATION_REGISTRY.gateway_oracle_reader_factory",
        side_effect=RuntimeError("secret upstream URL"),
    ):
        response = await _service().GetOraclePrice(
            gateway_pb2.GetOraclePriceRequest(provider="chainlink", chain="ethereum", token="ETH"),
            context,
        )
    assert not response.success
    assert context.code is grpc.StatusCode.INTERNAL
    assert response.error == "internal server error"
    assert "secret" not in context.details


@pytest.mark.asyncio
async def test_missing_reader_configuration_is_failed_precondition() -> None:
    service = _service()
    service._oracle_history_reader = lambda provider, chain: (_ for _ in ()).throw(ValueError("missing URL"))
    context = Context()
    response = await service.GetOraclePriceHistory(
        gateway_pb2.GetOraclePriceHistoryRequest(
            provider="chainlink",
            chain="ethereum",
            token="ETH",
            start_ts=100,
            end_ts=200,
            max_points=10,
        ),
        context,
    )
    assert not response.success
    assert context.code is grpc.StatusCode.FAILED_PRECONDITION
    assert "not configured" in response.error


@pytest.mark.asyncio
async def test_missing_current_reader_configuration_is_failed_precondition() -> None:
    service = _service()
    service._oracle_history_reader = lambda provider, chain: (_ for _ in ()).throw(ValueError("missing URL"))
    context = Context()

    response = await service.GetOraclePrice(
        gateway_pb2.GetOraclePriceRequest(provider="chainlink", chain="ethereum", token="ETH"),
        context,
    )

    assert not response.success
    assert context.code is grpc.StatusCode.FAILED_PRECONDITION
    assert response.error == "oracle reader is not configured for the requested chain"


@pytest.mark.asyncio
async def test_runtime_history_value_error_is_internal_not_configuration_error() -> None:
    class FailingReader(Reader):
        async def get_history_page(self, *, token: str, start_ts: int, end_ts: int, max_points: int):
            raise ValueError("malformed feed address")

    service = _service()
    service._oracle_history_reader = lambda provider, chain: FailingReader()
    context = Context()

    response = await service.GetOraclePriceHistory(
        gateway_pb2.GetOraclePriceHistoryRequest(
            provider="chainlink",
            chain="ethereum",
            token="ETH",
            start_ts=100,
            end_ts=200,
            max_points=10,
        ),
        context,
    )

    assert not response.success
    assert context.code is grpc.StatusCode.INTERNAL
    assert response.error == "internal server error"


@pytest.mark.asyncio
async def test_runtime_current_value_error_is_internal_not_configuration_error() -> None:
    class FailingReader(Reader):
        async def get_latest(self, *, token: str) -> HistoricalPricePoint:
            raise ValueError("malformed feed address")

    service = _service()
    service._oracle_history_reader = lambda provider, chain: FailingReader()
    context = Context()

    response = await service.GetOraclePrice(
        gateway_pb2.GetOraclePriceRequest(provider="chainlink", chain="ethereum", token="ETH"),
        context,
    )

    assert not response.success
    assert context.code is grpc.StatusCode.INTERNAL
    assert response.error == "internal server error"


def test_built_reader_must_implement_typed_current_history_and_close_contract() -> None:
    class IncompleteReader:
        async def get_latest(self, *, token: str):
            return HistoricalPricePoint(timestamp=100, price=Decimal("12.34"), observation_id=7)

    class Factory:
        def build(self, *, chain: str, settings: object):
            return IncompleteReader()

    service = RateHistoryServiceServicer.__new__(RateHistoryServiceServicer)
    service.settings = object()
    service._oracle_history_providers = {}

    with patch(
        "almanak.gateway.services.rate_history_service.INTEGRATION_REGISTRY.gateway_oracle_reader_factory",
        return_value=Factory(),
    ):
        with pytest.raises(TypeError, match="invalid gateway oracle reader"):
            service._oracle_history_reader("chainlink", "ethereum")

    assert service._oracle_history_providers == {}
