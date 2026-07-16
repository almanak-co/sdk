"""Unit tests for the shared perp funding-history gateway client.

``_gateway_history`` owns the single ``RateHistoryService.GetFundingRateHistory``
round-trip the per-venue funding providers share (VIB-4851 Phase D). The venue
provider tests (``test_gmx_funding.py`` / ``test_hyperliquid_funding.py``) mock
``fetch_funding_points`` itself, so this module is the only place the RPC
plumbing is exercised: request construction, window chunking, point decoding
(Empty ≠ Zero skip + malformed-rate discard), and the two failure envelopes
(transport error, ``success=False``). Everything is faked at the
``get_connected_gateway_client`` seam — no socket, no real protobuf.
"""

import logging
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.backtesting.pnl.providers.perp._gateway_history import (
    FundingHistoryPoint,
    fetch_funding_points,
    get_connected_gateway_client,
)
from almanak.framework.data.interfaces import DataSourceUnavailable

_CLIENT_SEAM = (
    "almanak.framework.backtesting.pnl.providers.perp._gateway_history.get_connected_gateway_client"
)


class _FakeRequest:
    """Stands in for ``gateway_pb2.GetFundingRateHistoryRequest``."""

    def __init__(self, **fields: object) -> None:
        self.__dict__.update(fields)


_FAKE_PB2 = SimpleNamespace(GetFundingRateHistoryRequest=_FakeRequest)


def _proto_point(timestamp: int, rate_hourly: str) -> SimpleNamespace:
    return SimpleNamespace(timestamp=timestamp, rate_hourly=rate_hourly)


def _response(
    points: list[SimpleNamespace] | None = None,
    *,
    success: bool = True,
    error: str = "",
    source: str = "gateway",
) -> SimpleNamespace:
    return SimpleNamespace(success=success, error=error, source=source, points=points or [])


class _FakeClient:
    """Scripted gateway client: pops one canned response (or exception) per RPC."""

    def __init__(self, responses: list[object]) -> None:
        self._responses = list(responses)
        self.requests: list[_FakeRequest] = []
        self.rate_history = SimpleNamespace(GetFundingRateHistory=self._call)

    def _call(self, request: _FakeRequest) -> SimpleNamespace:
        self.requests.append(request)
        outcome = self._responses.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        assert isinstance(outcome, SimpleNamespace)
        return outcome


def _fetch(client: _FakeClient, **kwargs: object) -> list[FundingHistoryPoint]:
    with patch(_CLIENT_SEAM, return_value=(client, _FAKE_PB2)):
        return fetch_funding_points(**kwargs)  # type: ignore[arg-type]


class TestFetchFundingPoints:
    """Decoding, request construction, and failure envelopes."""

    def test_decodes_points_and_sorts_by_timestamp(self):
        """Points come back as Decimals, ascending by timestamp."""
        client = _FakeClient(
            [_response([_proto_point(7200, "0.0002"), _proto_point(3600, "-0.0001")])]
        )
        points = _fetch(client, venue="gmx_v2", market="ETH-USD", start_ts=3600, end_ts=7200)
        assert points == [
            FundingHistoryPoint(timestamp=3600, rate_hourly=Decimal("-0.0001")),
            FundingHistoryPoint(timestamp=7200, rate_hourly=Decimal("0.0002")),
        ]

    def test_request_carries_venue_market_chain_and_window(self):
        """The proto request mirrors the call arguments exactly."""
        client = _FakeClient([_response()])
        _fetch(client, venue="gmx_v2", market="BTC-USD", chain="arbitrum", start_ts=100, end_ts=200)
        (request,) = client.requests
        assert request.venue == "gmx_v2"
        assert request.market == "BTC-USD"
        assert request.chain == "arbitrum"
        assert request.start_ts == 100
        assert request.end_ts == 200

    def test_empty_rate_is_skipped_never_zeroed(self):
        """Empty ``rate_hourly`` means unmeasured — skipped, not Decimal(0)."""
        client = _FakeClient(
            [_response([_proto_point(3600, ""), _proto_point(7200, "0.0003")])]
        )
        points = _fetch(client, venue="hyperliquid", market="ETH-USD", start_ts=3600, end_ts=7200)
        assert points == [FundingHistoryPoint(timestamp=7200, rate_hourly=Decimal("0.0003"))]

    def test_malformed_rate_is_discarded_with_warning(self, caplog: pytest.LogCaptureFixture):
        """A non-decimal rate drops that point and logs, keeping the rest."""
        client = _FakeClient(
            [_response([_proto_point(3600, "not-a-number"), _proto_point(7200, "0.0001")])]
        )
        with caplog.at_level(
            logging.WARNING,
            logger="almanak.framework.backtesting.pnl.providers.perp._gateway_history",
        ):
            points = _fetch(client, venue="gmx_v2", market="ETH-USD", start_ts=3600, end_ts=7200)
        assert points == [FundingHistoryPoint(timestamp=7200, rate_hourly=Decimal("0.0001"))]
        assert "malformed funding point" in caplog.text

    def test_rpc_failure_raises_data_source_unavailable(self):
        """A transport-level exception surfaces as DataSourceUnavailable."""
        client = _FakeClient([ConnectionError("gateway down")])
        with pytest.raises(DataSourceUnavailable) as exc_info:
            _fetch(client, venue="gmx_v2", market="ETH-USD", start_ts=0, end_ts=3600)
        assert exc_info.value.source == "gateway"
        assert "gateway down" in exc_info.value.reason

    def test_error_envelope_raises_with_gateway_source_and_reason(self):
        """``success=False`` propagates the response's source and error."""
        client = _FakeClient(
            [_response(success=False, error="venue not registered", source="rate_history")]
        )
        with pytest.raises(DataSourceUnavailable) as exc_info:
            _fetch(client, venue="vertex", market="ETH-USD", start_ts=0, end_ts=3600)
        assert exc_info.value.source == "rate_history"
        assert exc_info.value.reason == "venue not registered"

    def test_error_envelope_defaults_when_fields_empty(self):
        """An empty error envelope still raises with usable defaults."""
        client = _FakeClient([_response(success=False, error="", source="")])
        with pytest.raises(DataSourceUnavailable) as exc_info:
            _fetch(client, venue="gmx_v2", market="ETH-USD", start_ts=0, end_ts=3600)
        assert exc_info.value.source == "gateway"
        assert "success=false" in exc_info.value.reason


class TestWindowChunking:
    """Windows wider than the per-RPC cap split into sequential RPCs."""

    def test_wide_window_splits_into_contiguous_chunks(self):
        """A 3x-cap window issues 3 RPCs covering [start, end] with no gaps."""
        client = _FakeClient(
            [
                _response([_proto_point(0, "0.0001")]),
                _response([_proto_point(100, "0.0002")]),
                _response([_proto_point(200, "0.0003")]),
            ]
        )
        points = _fetch(
            client,
            venue="gmx_v2",
            market="ETH-USD",
            start_ts=0,
            end_ts=250,
            max_window_seconds=100,
        )
        windows = [(request.start_ts, request.end_ts) for request in client.requests]
        assert windows == [(0, 99), (100, 199), (200, 250)]
        assert [point.timestamp for point in points] == [0, 100, 200]

    def test_window_within_cap_is_a_single_rpc(self):
        """No chunking when the window fits one RPC."""
        client = _FakeClient([_response()])
        _fetch(client, venue="gmx_v2", market="ETH-USD", start_ts=0, end_ts=3600)
        assert len(client.requests) == 1

    @pytest.mark.parametrize("bad_window", [0, -1])
    def test_non_positive_window_cap_is_rejected(self, bad_window: int):
        """A non-positive cap would stall the chunk loop — fail fast instead."""
        client = _FakeClient([])
        with pytest.raises(ValueError, match="max_window_seconds"):
            _fetch(
                client,
                venue="gmx_v2",
                market="ETH-USD",
                start_ts=0,
                end_ts=3600,
                max_window_seconds=bad_window,
            )
        assert client.requests == []


class TestGetConnectedGatewayClient:
    """Import + connect dance shared with the lending/TWAP/volume peers."""

    def test_returns_already_connected_client_without_reconnecting(self):
        client = MagicMock()
        client.is_connected = True
        with patch("almanak.framework.gateway_client.get_gateway_client", return_value=client):
            returned, pb2 = get_connected_gateway_client()
        assert returned is client
        client.connect.assert_not_called()
        assert hasattr(pb2, "GetFundingRateHistoryRequest")

    def test_connects_disconnected_client(self):
        client = MagicMock()
        client.is_connected = False
        with patch("almanak.framework.gateway_client.get_gateway_client", return_value=client):
            returned, _ = get_connected_gateway_client()
        assert returned is client
        client.connect.assert_called_once_with()

    def test_connect_failure_raises_data_source_unavailable(self):
        client = MagicMock()
        client.is_connected = False
        client.connect.side_effect = ConnectionError("refused")
        with (
            patch("almanak.framework.gateway_client.get_gateway_client", return_value=client),
            pytest.raises(DataSourceUnavailable) as exc_info,
        ):
            get_connected_gateway_client()
        assert exc_info.value.source == "gateway"
        assert "refused" in exc_info.value.reason


class TestTransportClassification:
    """Only connectivity failures memoize; per-request statuses stay retryable."""

    @staticmethod
    def _raise_through(exc: Exception):
        from almanak.framework.backtesting.pnl.providers.perp import _gateway_history as gh
        from almanak.framework.data.interfaces import DataSourceUnavailable

        class Service:
            def GetFundingRateHistory(self, request):
                raise exc

        class Client:
            rate_history = Service()

        class Pb2:
            @staticmethod
            def GetFundingRateHistoryRequest(**kwargs):
                return kwargs

        try:
            gh._fetch_window(Client(), Pb2, venue="gmx", market="ETH-USD", chain="arbitrum", start_ts=0, end_ts=1)
        except DataSourceUnavailable as caught:
            return caught
        raise AssertionError("expected DataSourceUnavailable")

    def test_permanent_status_is_not_transport(self):
        class RpcError(Exception):
            def code(self):
                from types import SimpleNamespace

                return SimpleNamespace(name="INVALID_ARGUMENT")

        assert self._raise_through(RpcError("bad market")).transport is False

    def test_unavailable_status_is_transport(self):
        class RpcError(Exception):
            def code(self):
                from types import SimpleNamespace

                return SimpleNamespace(name="UNAVAILABLE")

        assert self._raise_through(RpcError("channel down")).transport is True

    def test_connectivity_exception_types_are_transport(self):
        assert self._raise_through(ConnectionError("socket closed")).transport is True
        assert self._raise_through(TimeoutError("timed out")).transport is True

    def test_unknown_exceptions_default_to_non_transport(self):
        # A local decoding bug must not memoize the gateway as down for the run.
        assert self._raise_through(ValueError("bad payload")).transport is False
        assert self._raise_through(TypeError("wrong type")).transport is False
        assert self._raise_through(RuntimeError("boom")).transport is False

    def test_local_oserror_subclasses_are_not_transport(self):
        # FileNotFoundError/PermissionError are OSError subclasses but signal
        # local defects (missing cert, bad path), not channel failures.
        assert self._raise_through(FileNotFoundError("cert missing")).transport is False
        assert self._raise_through(PermissionError("denied")).transport is False

    def test_raising_code_accessor_is_non_transport(self):
        class RpcError(Exception):
            def code(self):
                raise RuntimeError("no status")

        assert self._raise_through(RpcError("weird")).transport is False
