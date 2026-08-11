"""Gateway-client regressions for complete exact-pool historical TWAP series."""

from __future__ import annotations

from types import SimpleNamespace

import grpc
import pytest

from almanak.framework.backtesting.pnl.providers import twap
from almanak.framework.data.interfaces import DataSourceUnavailable
from almanak.gateway.proto import gateway_pb2

POOL = "0xc756bba710d45647715079ce50aa16aab36ded42"


def _response(*, point_count: int = 2, pool: str = POOL):
    timestamps = [1_000, 1_000]
    return gateway_pb2.DexTwapHistoryResponse(
        dex="uniswap_v3",
        chain="ethereum",
        pool_address=pool,
        points=[
            gateway_pb2.DexTwapPoint(
                timestamp=timestamps[index],
                price="1.001",
                tick_observation_count=2,
                as_of_block=100,
            )
            for index in range(point_count)
        ],
        source="on_chain_archive",
        success=True,
    )


def _install_client(monkeypatch: pytest.MonkeyPatch, response):
    requests = []

    def get_series(request, *, timeout):
        assert timeout == 12.5
        requests.append(request)
        return response

    client = SimpleNamespace(
        config=SimpleNamespace(timeout=12.5),
        rate_history=SimpleNamespace(GetDexTwapSeries=get_series),
    )
    monkeypatch.setattr(twap, "_twap_get_connected_gateway_client", lambda: (client, gateway_pb2))
    return requests


def test_fetch_preserves_window_cadence_duplicate_timestamps_and_block_anchor(monkeypatch) -> None:
    requests = _install_client(monkeypatch, _response())

    points = twap.fetch_historical_twap_points(
        protocol="uniswap_v3",
        chain="ethereum",
        pool_address=POOL,
        start_ts=1_000,
        end_ts=4_600,
        interval_secs=3_600,
        window_secs=1_800,
    )

    assert len(points) == 2
    assert [point.timestamp for point in points] == [1_000, 1_000]
    assert [point.block_number for point in points] == [100, 100]
    request = requests[0]
    assert request.interval_secs == 3_600
    assert request.window_secs == 1_800
    assert request.pool_address == POOL


def test_fetch_rejects_partial_grid_coverage(monkeypatch) -> None:
    _install_client(monkeypatch, _response(point_count=1))

    with pytest.raises(DataSourceUnavailable, match="incomplete grid coverage.*requested=2, received=1"):
        twap.fetch_historical_twap_points(
            protocol="uniswap_v3",
            chain="ethereum",
            pool_address=POOL,
            start_ts=1_000,
            end_ts=4_600,
            interval_secs=3_600,
            window_secs=1_800,
        )


def test_fetch_rejects_response_identity_drift(monkeypatch) -> None:
    _install_client(monkeypatch, _response(pool="0x0000000000000000000000000000000000000001"))

    with pytest.raises(DataSourceUnavailable, match="response identity drift"):
        twap.fetch_historical_twap_points(
            protocol="uniswap_v3",
            chain="ethereum",
            pool_address=POOL,
            start_ts=1_000,
            end_ts=4_600,
            interval_secs=3_600,
            window_secs=1_800,
        )


def test_fetch_rejects_unanchored_archive_observation(monkeypatch) -> None:
    response = _response()
    response.points[0].as_of_block = 0
    _install_client(monkeypatch, response)

    with pytest.raises(DataSourceUnavailable, match="omitted its archive block anchor"):
        twap.fetch_historical_twap_points(
            protocol="uniswap_v3",
            chain="ethereum",
            pool_address=POOL,
            start_ts=1_000,
            end_ts=4_600,
            interval_secs=3_600,
            window_secs=1_800,
        )


def test_fetch_maps_gateway_deadline_to_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    class DeadlineExceeded(grpc.RpcError):
        def code(self):
            return grpc.StatusCode.DEADLINE_EXCEEDED

        def details(self):
            return "deadline exceeded"

    def get_series(_request, *, timeout):
        assert timeout == 12.5
        raise DeadlineExceeded()

    client = SimpleNamespace(
        config=SimpleNamespace(timeout=12.5),
        rate_history=SimpleNamespace(GetDexTwapSeries=get_series),
    )
    monkeypatch.setattr(twap, "_twap_get_connected_gateway_client", lambda: (client, gateway_pb2))

    with pytest.raises(DataSourceUnavailable, match="GetDexTwapSeries RPC failed"):
        twap.fetch_historical_twap_points(
            protocol="uniswap_v3",
            chain="ethereum",
            pool_address=POOL,
            start_ts=1_000,
            end_ts=4_600,
            interval_secs=3_600,
            window_secs=1_800,
        )
