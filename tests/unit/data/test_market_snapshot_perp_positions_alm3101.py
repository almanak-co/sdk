"""ALM-3101 strategy-facing, gateway-routed perp-position read."""

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import grpc

from almanak.framework.market import MarketSnapshot, MarketSnapshotBuilder


def _snapshot(gateway_client: object) -> MarketSnapshot:
    strategy = SimpleNamespace(
        chain="arbitrum",
        wallet_address="0x0000000000000000000000000000000000000001",
    )
    return MarketSnapshotBuilder.for_strategy_runner(strategy=strategy, gateway_client=gateway_client)


def test_perp_positions_routes_through_gateway_reader_with_strategy_identity() -> None:
    gateway_client = object()
    measured = SimpleNamespace(positions=(), ok=True, truncated=False)
    reader = MagicMock()
    reader.read_positions.return_value = measured
    snapshot = _snapshot(gateway_client)

    with patch(
        "almanak.framework.valuation.perps_position_reader.PerpsPositionReader.from_gateway_client",
        return_value=reader,
    ) as build:
        result = snapshot.perp_positions("gmx_v2")

    assert result is measured
    build.assert_called_once_with(gateway_client)
    reader.read_positions.assert_called_once_with(
        "arbitrum",
        "0x0000000000000000000000000000000000000001",
        "gmx_v2",
    )


def test_block_timestamp_is_gateway_routed_and_measured() -> None:
    gateway_client = SimpleNamespace(is_connected=True)
    snapshot = _snapshot(gateway_client)
    provider = MagicMock()
    provider.make_request.return_value = {"result": {"timestamp": "0x64"}}

    with patch(
        "almanak.framework.web3.gateway_provider.GatewayWeb3Provider",
        return_value=provider,
    ):
        measured = snapshot.block_timestamp()

    assert measured == datetime.fromtimestamp(100, tz=UTC)
    provider.make_request.assert_called_once()


def test_block_timestamp_returns_none_when_gateway_result_is_unmeasured() -> None:
    gateway_client = SimpleNamespace(is_connected=True)
    snapshot = _snapshot(gateway_client)
    provider = MagicMock()
    provider.make_request.return_value = {"error": {"message": "unavailable"}}

    with patch(
        "almanak.framework.web3.gateway_provider.GatewayWeb3Provider",
        return_value=provider,
    ):
        assert snapshot.block_timestamp() is None


def test_block_timestamp_returns_none_when_gateway_request_raises() -> None:
    gateway_client = SimpleNamespace(is_connected=True)
    snapshot = _snapshot(gateway_client)
    provider = MagicMock()
    provider.make_request.side_effect = grpc.RpcError()

    with patch(
        "almanak.framework.web3.gateway_provider.GatewayWeb3Provider",
        return_value=provider,
    ):
        assert snapshot.block_timestamp() is None
