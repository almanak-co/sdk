"""Framework PoolHistoryReader (thin gRPC client) tests (VIB-4755 / POOL-7).

Covers UAT card ``docs/internal/uat-cards/VIB-4755.md`` rows D1.S2 and
D3.F1. Mirrors the VIB-4727 PoolAnalyticsReader test pattern:
mock-stub-based, no real gRPC channel, no real gateway boot, no live
HTTP.
"""

from __future__ import annotations

import inspect
import time
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock

import grpc
import pytest

from almanak.framework.data.interfaces import DataSourceUnavailable
from almanak.framework.data.models import DataEnvelope
from almanak.framework.data.pools.history import PoolHistoryReader, PoolSnapshot
from almanak.framework.runner.failure_kind import FailureKind, classify_failure
from almanak.gateway.proto import gateway_pb2

_BASE_UNIV3_POOL = "0xd0b53d9277642d899df5c87a3966a349a798f224"
_T0 = 1_700_000_000  # 2023-11-14 UTC, aligned to seconds


def _make_snapshot_row(
    *,
    timestamp: int,
    tvl: str = "1210000.0",
    volume_24h: str = "850000.0",
    fee_revenue_24h: str = "1200.0",
    token0_reserve: str = "500.0",
    token1_reserve: str = "2500000.0",
    unmeasured_fields: tuple[str, ...] = (),
) -> gateway_pb2.PoolSnapshot:
    return gateway_pb2.PoolSnapshot(
        timestamp=timestamp,
        tvl=tvl,
        volume_24h=volume_24h,
        fee_revenue_24h=fee_revenue_24h,
        token0_reserve=token0_reserve,
        token1_reserve=token1_reserve,
        unmeasured_fields=list(unmeasured_fields),
    )


def _make_response(
    *,
    success: bool = True,
    error: str = "",
    snapshots: list[gateway_pb2.PoolSnapshot] | None = None,
    truncation_reason: int = gateway_pb2.TruncationReason.TRUNCATION_REASON_UNSPECIFIED,
    next_start_ts: int = 0,
    source: str = "the_graph",
    finalized_only: bool = True,
) -> gateway_pb2.PoolHistoryResponse:
    if snapshots is None:
        snapshots = [
            _make_snapshot_row(timestamp=_T0 + 3600 * i)
            for i in range(3)
        ]
    return gateway_pb2.PoolHistoryResponse(
        snapshots=snapshots,
        truncation_reason=truncation_reason,
        next_start_ts=next_start_ts,
        source=source,
        finalized_only=finalized_only,
        success=success,
        error=error,
    )


def _fake_gateway_with_stub(stub: MagicMock) -> MagicMock:
    gateway = MagicMock()
    gateway.pool_history = stub
    return gateway


class _FakeRpcError(grpc.RpcError):
    def __init__(self, code: grpc.StatusCode, details: str) -> None:
        self._code = code
        self._details = details
        super().__init__(details)

    def code(self) -> grpc.StatusCode:
        return self._code

    def details(self) -> str:
        return self._details


# ============================================================================
# D1.S2 — bare-ctor + happy-path + Empty != Zero + end_date=None anchor
# ============================================================================


def test_pool_history_reader_requires_gateway_client():
    """D1.S2: bare ctor raises TypeError; explicit None raises TypeError with VIB-47 in message."""
    with pytest.raises(TypeError):
        PoolHistoryReader()  # type: ignore[call-arg]

    with pytest.raises(TypeError, match=r"VIB-47") as excinfo:
        PoolHistoryReader(gateway_client=None)  # type: ignore[arg-type]
    assert "GatewayClient" in str(excinfo.value)


@pytest.mark.acceptance_pack
def test_get_pool_history_routes_through_gateway():
    """D1.S2: returned envelope is DataEnvelope[list[PoolSnapshot]] with
    string-decimal wire values parsed at the framework boundary."""
    stub = MagicMock()
    stub.GetPoolHistory.return_value = _make_response()
    reader = PoolHistoryReader(gateway_client=_fake_gateway_with_stub(stub))

    envelope = reader.get_pool_history(
        pool_address=_BASE_UNIV3_POOL,
        chain="base",
        start_date=datetime.fromtimestamp(_T0, tz=UTC),
        end_date=datetime.fromtimestamp(_T0 + 3 * 3600, tz=UTC),
        resolution="1h",
        protocol="uniswap_v3",
    )

    assert isinstance(envelope, DataEnvelope)
    assert isinstance(envelope.value, list)
    assert len(envelope.value) == 3
    assert all(isinstance(s, PoolSnapshot) for s in envelope.value)
    assert envelope.value[0].timestamp.tzinfo is UTC
    assert envelope.value[0].tvl == Decimal("1210000.0")
    assert envelope.value[0].unmeasured_fields == frozenset()
    assert envelope.meta.source == "the_graph"


def test_empty_string_decimals_map_to_none_not_zero():
    """D1.S2: a fixture row with proto.fee_revenue_24h = "" produces
    snap.fee_revenue_24h is None (NOT Decimal('0')). The unmeasured_fields
    set on the row contains exactly the names of fields whose value is None."""
    stub = MagicMock()
    # One row with fee_revenue_24h and token1_reserve unmeasured.
    stub.GetPoolHistory.return_value = _make_response(
        snapshots=[
            _make_snapshot_row(
                timestamp=_T0,
                tvl="1000000.0",
                volume_24h="500000.0",
                fee_revenue_24h="",  # unmeasured
                token0_reserve="100.0",
                token1_reserve="",  # unmeasured
                unmeasured_fields=("fee_revenue_24h", "token1_reserve"),
            ),
        ],
    )
    reader = PoolHistoryReader(gateway_client=_fake_gateway_with_stub(stub))

    envelope = reader.get_pool_history(
        pool_address=_BASE_UNIV3_POOL,
        chain="base",
        start_date=datetime.fromtimestamp(_T0, tz=UTC),
        end_date=datetime.fromtimestamp(_T0 + 3600, tz=UTC),
        resolution="1h",
        protocol="uniswap_v3",
    )

    row = envelope.value[0]
    # Anti-coercion: NOT Decimal("0")
    assert row.fee_revenue_24h is None
    assert row.token1_reserve is None
    # Measured rows survive as Decimal
    assert row.tvl == Decimal("1000000.0")
    assert row.token0_reserve == Decimal("100.0")
    # unmeasured_fields invariant — equals the names of fields that are None.
    expected_unmeasured = frozenset(
        name
        for name in ("tvl", "volume_24h", "fee_revenue_24h", "token0_reserve", "token1_reserve")
        if getattr(row, name) is None
    )
    assert row.unmeasured_fields == expected_unmeasured == frozenset({"fee_revenue_24h", "token1_reserve"})


def test_belt_and_braces_empty_wire_value_without_unmeasured_field_name():
    """Empty != Zero defence-in-depth: even if the wire's unmeasured_fields
    list is incomplete (missing a field that is "" on the wire), the
    framework reader still records that field as unmeasured via the
    belt-and-braces ``if decoded is None: unmeasured.add(name)`` check."""
    stub = MagicMock()
    stub.GetPoolHistory.return_value = _make_response(
        snapshots=[
            _make_snapshot_row(
                timestamp=_T0,
                tvl="",  # unmeasured on the wire
                volume_24h="850000.0",
                fee_revenue_24h="1200.0",
                token0_reserve="500.0",
                token1_reserve="2500000.0",
                unmeasured_fields=(),  # wire LIES — tvl is "" but not listed
            ),
        ],
    )
    reader = PoolHistoryReader(gateway_client=_fake_gateway_with_stub(stub))

    envelope = reader.get_pool_history(
        pool_address=_BASE_UNIV3_POOL,
        chain="base",
        start_date=datetime.fromtimestamp(_T0, tz=UTC),
        end_date=datetime.fromtimestamp(_T0 + 3600, tz=UTC),
        resolution="1h",
        protocol="uniswap_v3",
    )

    row = envelope.value[0]
    assert row.tvl is None
    # Framework recovered the truth despite the malformed wire metadata.
    assert "tvl" in row.unmeasured_fields


def test_direct_reader_end_date_none_anchors_to_now():
    """D1.S2: direct PoolHistoryReader callers with end_date=None resolve
    to datetime.now(UTC) at the framework boundary BEFORE the gRPC call.
    The captured request.end_ts is within ±5s of time.time()."""
    stub = MagicMock()
    stub.GetPoolHistory.return_value = _make_response()
    reader = PoolHistoryReader(gateway_client=_fake_gateway_with_stub(stub))

    before = time.time()
    reader.get_pool_history(
        pool_address=_BASE_UNIV3_POOL,
        chain="base",
        start_date=datetime.fromtimestamp(_T0, tz=UTC),
        end_date=None,
        resolution="1h",
        protocol="uniswap_v3",
    )
    after = time.time()

    sent_request = stub.GetPoolHistory.call_args.args[0]
    # The wire end_ts is a concrete int, NOT 0 (stable cache key).
    assert sent_request.end_ts != 0
    # Within ±5s of time.time() — proves the resolution is "now",
    # NOT a snapshot anchor.
    assert int(before) - 5 <= sent_request.end_ts <= int(after) + 5


def test_protocol_is_required_keyword_only():
    """D-2 lock: PoolHistoryReader.get_pool_history requires protocol as
    a keyword-only argument with no default. Closes the silent cross-
    protocol surface flagged by Phase 0b Round-4."""
    sig = inspect.signature(PoolHistoryReader.get_pool_history)
    assert sig.parameters["protocol"].default is inspect.Parameter.empty
    assert sig.parameters["protocol"].kind is inspect.Parameter.KEYWORD_ONLY

    stub = MagicMock()
    stub.GetPoolHistory.return_value = _make_response()
    reader = PoolHistoryReader(gateway_client=_fake_gateway_with_stub(stub))

    # Calling without protocol raises TypeError (Python's missing-required-kwarg signal).
    with pytest.raises(TypeError, match=r"protocol"):
        reader.get_pool_history(  # type: ignore[call-arg]
            pool_address=_BASE_UNIV3_POOL,
            chain="base",
            start_date=datetime.fromtimestamp(_T0, tz=UTC),
        )


# ============================================================================
# D3.F1 — gateway-down, RuntimeError, success=False all map to DataSourceUnavailable
# ============================================================================


def test_grpc_unavailable_raises_datasource_unavailable():
    """D3.F1: gRPC UNAVAILABLE -> DataSourceUnavailable with __cause__
    preserved; classify_failure walks to DATA_UNAVAILABLE."""
    stub = MagicMock()
    stub.GetPoolHistory.side_effect = _FakeRpcError(
        grpc.StatusCode.UNAVAILABLE,
        "channel closed",
    )
    reader = PoolHistoryReader(gateway_client=_fake_gateway_with_stub(stub))

    with pytest.raises(DataSourceUnavailable) as excinfo:
        reader.get_pool_history(
            pool_address=_BASE_UNIV3_POOL,
            chain="base",
            start_date=datetime.fromtimestamp(_T0, tz=UTC),
            end_date=datetime.fromtimestamp(_T0 + 3600, tz=UTC),
            resolution="1h",
            protocol="uniswap_v3",
        )
    assert isinstance(excinfo.value.__cause__, grpc.RpcError)
    assert classify_failure(excinfo.value) == FailureKind.DATA_UNAVAILABLE


def test_runtime_error_not_connected_raises_datasource_unavailable():
    """D3.F1: GatewayClient.pool_history raises RuntimeError when not
    connected. The reader maps that to DataSourceUnavailable (not leak
    the RuntimeError) so the runner's HOLD path fires via the same
    DATA_UNAVAILABLE classification as a real outage."""
    fake_gateway = MagicMock()
    type(fake_gateway).pool_history = property(
        lambda _self: (_ for _ in ()).throw(RuntimeError("Gateway client not connected")),
    )
    reader = PoolHistoryReader(gateway_client=fake_gateway)

    with pytest.raises(DataSourceUnavailable) as excinfo:
        reader.get_pool_history(
            pool_address=_BASE_UNIV3_POOL,
            chain="base",
            start_date=datetime.fromtimestamp(_T0, tz=UTC),
            end_date=datetime.fromtimestamp(_T0 + 3600, tz=UTC),
            resolution="1h",
            protocol="uniswap_v3",
        )
    assert "not connected" in excinfo.value.reason
    assert isinstance(excinfo.value.__cause__, RuntimeError)
    assert classify_failure(excinfo.value) == FailureKind.DATA_UNAVAILABLE


def test_success_false_response_raises_datasource_unavailable():
    """D3.F1: response.success == False from a connected gateway maps to
    DataSourceUnavailable (no fake-success empty envelope)."""
    stub = MagicMock()
    stub.GetPoolHistory.return_value = _make_response(
        success=False,
        error="all providers unavailable",
        snapshots=[],
        source="",
    )
    reader = PoolHistoryReader(gateway_client=_fake_gateway_with_stub(stub))

    with pytest.raises(DataSourceUnavailable) as excinfo:
        reader.get_pool_history(
            pool_address=_BASE_UNIV3_POOL,
            chain="base",
            start_date=datetime.fromtimestamp(_T0, tz=UTC),
            end_date=datetime.fromtimestamp(_T0 + 3600, tz=UTC),
            resolution="1h",
            protocol="uniswap_v3",
        )
    assert "providers unavailable" in excinfo.value.reason
    # No __cause__ for the success=False path (the gRPC call returned OK).
    assert classify_failure(excinfo.value) == FailureKind.DATA_UNAVAILABLE


# ============================================================================
# Wire-correctness regression — request fields are populated
# ============================================================================


def test_request_fields_correctly_populated():
    """Wire-shape regression: the framework reader constructs a
    PoolHistoryRequest with pool_address, chain, protocol, start_ts,
    end_ts, resolution all populated correctly."""
    stub = MagicMock()
    stub.GetPoolHistory.return_value = _make_response()
    reader = PoolHistoryReader(gateway_client=_fake_gateway_with_stub(stub))

    reader.get_pool_history(
        pool_address=_BASE_UNIV3_POOL,
        chain="base",
        start_date=datetime.fromtimestamp(_T0, tz=UTC),
        end_date=datetime.fromtimestamp(_T0 + 7 * 86400, tz=UTC),
        resolution="1h",
        protocol="uniswap_v3",
    )

    req = stub.GetPoolHistory.call_args.args[0]
    assert req.pool_address == _BASE_UNIV3_POOL
    assert req.chain == "base"
    assert req.protocol == "uniswap_v3"
    assert req.start_ts == _T0
    assert req.end_ts == _T0 + 7 * 86400
    assert req.resolution == gateway_pb2.Resolution.RESOLUTION_1H


# ============================================================================
# /pr-audit Round-1 fixes — Codex P2 / pr-auditor #2 cursor loop
# ============================================================================


def test_truncation_cursor_loops_until_exhausted():
    """Codex /pr-audit P2 + Claude pr-auditor #2: gateway's
    truncation_reason + next_start_ts cursor MUST be iterated until
    next_start_ts == 0. A 200d-1h request that the gateway clamps to
    90d-chunks must NOT silently return only the first 90d slice."""
    stub = MagicMock()
    # Two-chunk response: first call returns CAP_EXCEEDED + next cursor;
    # second call returns the remainder with next_start_ts=0 (terminal).
    chunk_1 = _make_response(
        snapshots=[_make_snapshot_row(timestamp=_T0 + 3600 * i) for i in range(3)],
        truncation_reason=gateway_pb2.TruncationReason.CAP_EXCEEDED,
        next_start_ts=_T0 + 3 * 3600,
    )
    chunk_2 = _make_response(
        snapshots=[_make_snapshot_row(timestamp=_T0 + 3600 * (3 + i)) for i in range(2)],
        truncation_reason=gateway_pb2.TruncationReason.TRUNCATION_REASON_UNSPECIFIED,
        next_start_ts=0,
    )
    stub.GetPoolHistory.side_effect = [chunk_1, chunk_2]
    reader = PoolHistoryReader(gateway_client=_fake_gateway_with_stub(stub))

    envelope = reader.get_pool_history(
        pool_address=_BASE_UNIV3_POOL,
        chain="base",
        start_date=datetime.fromtimestamp(_T0, tz=UTC),
        end_date=datetime.fromtimestamp(_T0 + 5 * 3600, tz=UTC),
        resolution="1h",
        protocol="uniswap_v3",
    )

    # All 5 rows stitched together (3 from chunk_1 + 2 from chunk_2).
    assert len(envelope.value) == 5
    # Cursor was followed — the stub got called twice with advancing start_ts.
    assert stub.GetPoolHistory.call_count == 2
    first_req = stub.GetPoolHistory.call_args_list[0].args[0]
    second_req = stub.GetPoolHistory.call_args_list[1].args[0]
    assert first_req.start_ts == _T0
    assert second_req.start_ts == _T0 + 3 * 3600  # advanced to next_start_ts
    # end_ts stays fixed across chunks (the gateway re-clamps each call).
    assert first_req.end_ts == second_req.end_ts == _T0 + 5 * 3600


def test_provider_retention_sentinel_terminates_loop():
    """next_start_ts == 0 with PROVIDER_RETENTION terminates the loop
    (no further chunks attempted — upstream provider has no more data
    backward; looping anyway would burn the request budget)."""
    stub = MagicMock()
    stub.GetPoolHistory.return_value = _make_response(
        snapshots=[_make_snapshot_row(timestamp=_T0)],
        truncation_reason=gateway_pb2.TruncationReason.PROVIDER_RETENTION,
        next_start_ts=0,
    )
    reader = PoolHistoryReader(gateway_client=_fake_gateway_with_stub(stub))

    envelope = reader.get_pool_history(
        pool_address=_BASE_UNIV3_POOL,
        chain="base",
        start_date=datetime.fromtimestamp(_T0, tz=UTC),
        end_date=datetime.fromtimestamp(_T0 + 365 * 86400, tz=UTC),
        resolution="1d",
        protocol="uniswap_v3",
    )
    assert len(envelope.value) == 1
    assert stub.GetPoolHistory.call_count == 1


def test_non_advancing_cursor_breaks_defensively():
    """If the gateway returns next_start_ts <= current cursor_start_ts,
    the loop breaks defensively (gateway bug; don't infinite-loop)."""
    stub = MagicMock()
    stub.GetPoolHistory.return_value = _make_response(
        snapshots=[_make_snapshot_row(timestamp=_T0)],
        truncation_reason=gateway_pb2.TruncationReason.CAP_EXCEEDED,
        next_start_ts=_T0,  # equal to start — non-advancing
    )
    reader = PoolHistoryReader(gateway_client=_fake_gateway_with_stub(stub))

    envelope = reader.get_pool_history(
        pool_address=_BASE_UNIV3_POOL,
        chain="base",
        start_date=datetime.fromtimestamp(_T0, tz=UTC),
        end_date=datetime.fromtimestamp(_T0 + 365 * 86400, tz=UTC),
        resolution="1d",
        protocol="uniswap_v3",
    )
    # Whatever the bug, we got 1 row and stopped — not 50+ rows from
    # iterating the same start_ts.
    assert len(envelope.value) == 1
    assert stub.GetPoolHistory.call_count == 1


# ============================================================================
# /pr-audit Round-1 fix — pr-auditor #3 NaN / Infinity guard
# ============================================================================


def test_decimal_nan_and_infinity_treated_as_unmeasured():
    """Claude pr-auditor #3: Decimal('NaN') / Decimal('Infinity') are
    parseable by ``Decimal(value)`` but if propagated to PoolSnapshot
    money fields would corrupt downstream IL / volume / accounting
    math (NaN * x = NaN, Infinity * 0 = NaN, etc.). The framework
    reader treats non-finite values as unmeasured (None) to maintain
    the Empty != Zero contract cleanly."""
    stub = MagicMock()
    stub.GetPoolHistory.return_value = _make_response(
        snapshots=[
            _make_snapshot_row(
                timestamp=_T0,
                tvl="NaN",
                volume_24h="Infinity",
                fee_revenue_24h="-Infinity",
                token0_reserve="1000.0",  # finite — should survive
                token1_reserve="500.0",
            ),
        ],
    )
    reader = PoolHistoryReader(gateway_client=_fake_gateway_with_stub(stub))

    envelope = reader.get_pool_history(
        pool_address=_BASE_UNIV3_POOL,
        chain="base",
        start_date=datetime.fromtimestamp(_T0, tz=UTC),
        end_date=datetime.fromtimestamp(_T0 + 3600, tz=UTC),
        resolution="1h",
        protocol="uniswap_v3",
    )

    row = envelope.value[0]
    assert row.tvl is None
    assert row.volume_24h is None
    assert row.fee_revenue_24h is None
    assert row.token0_reserve == Decimal("1000.0")
    assert row.token1_reserve == Decimal("500.0")
    # And the unmeasured_fields invariant holds.
    assert row.unmeasured_fields == frozenset({"tvl", "volume_24h", "fee_revenue_24h"})


# ============================================================================
# /pr-audit Round-1 fix — pr-auditor #4 explicit ValueError on bad resolution
# ============================================================================


def test_unsupported_resolution_raises_value_error_before_rpc():
    """Claude pr-auditor #4: an unknown resolution string (e.g. '5m')
    raises ValueError at the framework boundary BEFORE any gRPC
    round-trip — mirrors D-2's TypeError-before-RPC contract for
    ``protocol``. Without this guard the gateway validator returns
    INVALID_ARGUMENT and the framework rethrows as
    ``DataSourceUnavailable('validator rejected request')`` — losing
    the explicit 'you passed a bad resolution string' signal."""
    stub = MagicMock()
    stub.GetPoolHistory.return_value = _make_response()
    reader = PoolHistoryReader(gateway_client=_fake_gateway_with_stub(stub))

    with pytest.raises(ValueError, match=r"Unsupported resolution"):
        reader.get_pool_history(
            pool_address=_BASE_UNIV3_POOL,
            chain="base",
            start_date=datetime.fromtimestamp(_T0, tz=UTC),
            end_date=datetime.fromtimestamp(_T0 + 3600, tz=UTC),
            resolution="5m",  # not in {1h, 4h, 1d}
            protocol="uniswap_v3",
        )

    # And NO gRPC round-trip was attempted.
    assert stub.GetPoolHistory.call_count == 0


# ============================================================================
# /pr-audit Round-1 fix — CodeRabbit unmeasured_fields derivation
# ============================================================================


def test_unmeasured_fields_derived_from_decoded_values_not_wire_list():
    """CodeRabbit /pr-audit: the framework reader DERIVES
    unmeasured_fields from the decoded values (NOT seeds from
    row.unmeasured_fields). The wire's list is informational only;
    the decoded value is authoritative. A wire that LIES (claims
    'tvl' is unmeasured but ALSO sends tvl='1000') previously had
    snap.tvl == Decimal('1000') AND 'tvl' in snap.unmeasured_fields —
    violating the PoolSnapshot invariant. CodeRabbit's fix: derive
    purely from decoded values; ignore the wire's claim."""
    stub = MagicMock()
    # The wire LIES: lists "tvl" as unmeasured but sends a present value.
    stub.GetPoolHistory.return_value = _make_response(
        snapshots=[
            _make_snapshot_row(
                timestamp=_T0,
                tvl="1000.0",  # PRESENT
                volume_24h="500.0",  # PRESENT
                fee_revenue_24h="10.0",  # PRESENT
                token0_reserve="100.0",
                token1_reserve="200.0",
                unmeasured_fields=("tvl",),  # wire says: unmeasured (LIE)
            ),
        ],
    )
    reader = PoolHistoryReader(gateway_client=_fake_gateway_with_stub(stub))

    envelope = reader.get_pool_history(
        pool_address=_BASE_UNIV3_POOL,
        chain="base",
        start_date=datetime.fromtimestamp(_T0, tz=UTC),
        end_date=datetime.fromtimestamp(_T0 + 3600, tz=UTC),
        resolution="1h",
        protocol="uniswap_v3",
    )

    row = envelope.value[0]
    # Decoded value is authoritative.
    assert row.tvl == Decimal("1000.0")
    # The wire's lie does NOT contaminate unmeasured_fields.
    assert "tvl" not in row.unmeasured_fields
    # Invariant: unmeasured_fields == names where the decoded value is None.
    assert row.unmeasured_fields == frozenset()
