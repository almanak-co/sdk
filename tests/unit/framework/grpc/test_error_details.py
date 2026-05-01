"""Tests for the typed-error gRPC codec (VIB-3800)."""

from __future__ import annotations

from typing import Any

import grpc
import pytest

from almanak.framework.grpc.error_details import (
    StatusDetails,
    pack_status_details,
    set_grpc_error,
    unpack_status_details,
)


class _FakeRpcError(Exception):
    """Mimics the surface ``grpc.RpcError`` exposes (just ``trailing_metadata``)."""

    def __init__(self, trailing: Any) -> None:
        self._trailing = trailing

    def trailing_metadata(self):
        return self._trailing


class _FakeContext:
    """Mimics the gRPC servicer context surface used by ``set_grpc_error``."""

    def __init__(self) -> None:
        self.code: grpc.StatusCode | None = None
        self.details: str | None = None
        self.trailing: list[tuple[str, bytes]] | None = None

    def set_code(self, code: grpc.StatusCode) -> None:
        self.code = code

    def set_details(self, details: str) -> None:
        self.details = details

    def set_trailing_metadata(self, trailing) -> None:
        self.trailing = list(trailing)


class TestPackStatusDetails:
    def test_minimal_pack_round_trip(self) -> None:
        code, msg, trailing = pack_status_details(
            code=grpc.StatusCode.UNAVAILABLE,
            message="upstream down",
        )

        assert code == grpc.StatusCode.UNAVAILABLE
        assert msg == "upstream down"
        assert len(trailing) == 1
        assert trailing[0][0] == "grpc-status-details-bin"
        assert isinstance(trailing[0][1], bytes)

        details = unpack_status_details(_FakeRpcError(trailing))
        assert details is not None
        assert details.code == grpc.StatusCode.UNAVAILABLE
        assert details.message == "upstream down"
        assert details.retry_delay_seconds is None
        assert details.reason is None
        assert details.upstream is None

    def test_pack_with_retry_info(self) -> None:
        code, _, trailing = pack_status_details(
            code=grpc.StatusCode.RESOURCE_EXHAUSTED,
            message="rate limited",
            retry_delay_seconds=2.5,
            reason="UPSTREAM_RATE_LIMITED",
            upstream="geckoterminal",
        )

        assert code == grpc.StatusCode.RESOURCE_EXHAUSTED

        details = unpack_status_details(_FakeRpcError(trailing))
        assert details is not None
        assert details.code == grpc.StatusCode.RESOURCE_EXHAUSTED
        assert details.retry_delay_seconds == pytest.approx(2.5)
        assert details.reason == "UPSTREAM_RATE_LIMITED"
        assert details.upstream == "geckoterminal"

    def test_pack_with_arbitrary_metadata(self) -> None:
        _, _, trailing = pack_status_details(
            code=grpc.StatusCode.UNAVAILABLE,
            message="oops",
            reason="UPSTREAM_HTTP_5XX",
            upstream="binance",
            metadata={"integration_code": "HTTP_503", "attempt": "1"},
        )

        details = unpack_status_details(_FakeRpcError(trailing))
        assert details is not None
        assert details.metadata.get("integration_code") == "HTTP_503"
        assert details.metadata.get("attempt") == "1"
        assert details.metadata.get("upstream") == "binance"

    def test_pack_zero_retry_delay_is_preserved(self) -> None:
        _, _, trailing = pack_status_details(
            code=grpc.StatusCode.RESOURCE_EXHAUSTED,
            message="now",
            retry_delay_seconds=0.0,
        )
        details = unpack_status_details(_FakeRpcError(trailing))
        assert details is not None
        assert details.retry_delay_seconds == pytest.approx(0.0)

    def test_negative_retry_delay_skipped(self) -> None:
        # Negative values are nonsensical; the codec drops them rather than
        # surfacing a misleading ``retry_delay_seconds=-1.0``.
        _, _, trailing = pack_status_details(
            code=grpc.StatusCode.UNAVAILABLE,
            message="x",
            retry_delay_seconds=-1.0,
        )
        details = unpack_status_details(_FakeRpcError(trailing))
        assert details is not None
        assert details.retry_delay_seconds is None

    def test_subsecond_retry_delay_round_trip(self) -> None:
        _, _, trailing = pack_status_details(
            code=grpc.StatusCode.RESOURCE_EXHAUSTED,
            message="x",
            retry_delay_seconds=0.25,
        )
        details = unpack_status_details(_FakeRpcError(trailing))
        assert details is not None
        # Allow some float tolerance from nanos conversion.
        assert details.retry_delay_seconds == pytest.approx(0.25, abs=1e-6)


class TestSetGrpcError:
    def test_sets_code_message_and_trailing(self) -> None:
        ctx = _FakeContext()
        set_grpc_error(
            ctx,
            code=grpc.StatusCode.RESOURCE_EXHAUSTED,
            message="rate limited",
            retry_delay_seconds=1.0,
            reason="UPSTREAM_RATE_LIMITED",
            upstream="binance",
        )
        assert ctx.code == grpc.StatusCode.RESOURCE_EXHAUSTED
        assert ctx.details == "rate limited"
        assert ctx.trailing is not None
        assert ctx.trailing[0][0] == "grpc-status-details-bin"

        # The trailer round-trips through unpack_status_details.
        details = unpack_status_details(_FakeRpcError(ctx.trailing))
        assert details is not None
        assert details.retry_delay_seconds == pytest.approx(1.0)
        assert details.upstream == "binance"


class TestUnpackStatusDetails:
    def test_no_metadata_returns_none(self) -> None:
        class _NoMeta:
            pass

        assert unpack_status_details(_NoMeta()) is None

    def test_empty_metadata_returns_none(self) -> None:
        assert unpack_status_details(_FakeRpcError([])) is None

    def test_irrelevant_metadata_returns_none(self) -> None:
        assert unpack_status_details(_FakeRpcError([("other-key", b"abc")])) is None

    def test_corrupt_payload_returns_none(self) -> None:
        # Garbage bytes — should not crash, just return None.
        bad = [("grpc-status-details-bin", b"\x00\x01\x02not-a-proto")]
        assert unpack_status_details(_FakeRpcError(bad)) is None

    def test_empty_payload_returns_none(self) -> None:
        # Regression for CodeRabbit finding: an empty trailer (b"") parses to
        # a default Status (code=OK, no details). Without the empty-check,
        # callers would see StatusDetails(code=OK) instead of None and skip
        # the legacy fallback path.
        empty = [("grpc-status-details-bin", b"")]
        assert unpack_status_details(_FakeRpcError(empty)) is None

    def test_non_latin1_str_trailer_returns_none(self) -> None:
        # Regression for Gemini finding: a `-bin` trailer surfaced as a str
        # via a buggy shim could contain characters outside latin-1. We
        # previously used errors="ignore" which silently dropped bytes and
        # produced corrupt protobuf. Now we fail loud (None → legacy
        # fallback) when the encode round-trip would lose data.
        bad = [("grpc-status-details-bin", "not-latin1-€")]  # euro sign
        assert unpack_status_details(_FakeRpcError(bad)) is None

    def test_latin1_str_trailer_round_trips(self) -> None:
        # Counter-test: a clean latin-1-compatible str trailer must still
        # decode (some shims do hand back str). This guards against
        # over-tightening the previous fix.
        from google.rpc import status_pb2

        status = status_pb2.Status()
        status.code = 8  # RESOURCE_EXHAUSTED
        status.message = "rate limited"
        payload = status.SerializeToString()
        # Re-decode through latin-1 to a str so the input matches what a
        # broken shim might surface.
        good = [("grpc-status-details-bin", payload.decode("latin-1"))]
        details = unpack_status_details(_FakeRpcError(good))
        assert details is not None
        assert details.code == grpc.StatusCode.RESOURCE_EXHAUSTED
        assert details.message == "rate limited"

    def test_unknown_status_code_decodes_to_unknown(self) -> None:
        # Manually craft a Status with a code outside the standard table.
        from google.rpc import status_pb2

        status = status_pb2.Status()
        status.code = 99
        status.message = "weird"
        bad_trailing = [("grpc-status-details-bin", status.SerializeToString())]

        details = unpack_status_details(_FakeRpcError(bad_trailing))
        assert details is not None
        assert details.code == grpc.StatusCode.UNKNOWN
        assert details.message == "weird"


class TestStatusDetailsDataclass:
    def test_default_metadata_is_empty_dict(self) -> None:
        details = StatusDetails(code=grpc.StatusCode.OK, message="ok")
        assert details.metadata == {}
        # Dataclass default factory should not share state across instances.
        details.metadata["x"] = "y"
        details2 = StatusDetails(code=grpc.StatusCode.OK, message="ok")
        assert "x" not in details2.metadata
