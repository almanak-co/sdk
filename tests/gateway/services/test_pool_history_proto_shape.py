"""D1.S0 proto-shape test for PoolHistoryService (POOL-1 / VIB-4749).

This is the proto-only acceptance step from the umbrella UAT card at
``docs/internal/uat-cards/VIB-4728.md``. It is the only D-step POOL-1 must
land green in CI; the rest of D1/D2/D3 require POOL-2..POOL-9 work and
are gated to those tickets per the card's sub-feature evidence map.

The assertions here mirror the inline ``uv run python -c "..."`` block in
the card's D1.S0 section verbatim. A future regression that, for example,
changes ``truncation_reason`` to a bool, removes a ``Resolution`` enum
value, or strips ``unmeasured_fields`` from ``PoolSnapshot`` is caught
here at the proto layer before any servicer / framework code can mask it.

UAT card frozen SHA at the time this test was written:
``c7c5afedd556e8f290f2b79810a11ca612c43845``.
"""

from __future__ import annotations

from google.protobuf.descriptor import FieldDescriptor

from almanak.gateway.proto import gateway_pb2


def test_resolution_enum_values_locked() -> None:
    assert gateway_pb2.Resolution.RESOLUTION_UNSPECIFIED == 0
    assert gateway_pb2.Resolution.RESOLUTION_1H == 1
    assert gateway_pb2.Resolution.RESOLUTION_1D == 2
    assert gateway_pb2.Resolution.RESOLUTION_4H == 3


def test_truncation_reason_enum_values_locked() -> None:
    assert gateway_pb2.TruncationReason.TRUNCATION_REASON_UNSPECIFIED == 0
    assert gateway_pb2.TruncationReason.CAP_EXCEEDED == 1
    assert gateway_pb2.TruncationReason.PROVIDER_PAGE_CAP == 2
    assert gateway_pb2.TruncationReason.PROVIDER_RETENTION == 3


def test_pool_snapshot_has_locked_fields_and_no_speculative_fields() -> None:
    snap_fields = {d.name: d for d in gateway_pb2.PoolSnapshot.DESCRIPTOR.fields}
    for name in (
        "timestamp",
        "tvl",
        "volume_24h",
        "fee_revenue_24h",
        "token0_reserve",
        "token1_reserve",
        "unmeasured_fields",
    ):
        assert name in snap_fields, name
    # Deferred to follow-ups (PoolX.md POOL-1 C2):
    for name in (
        "fee_tier",
        "tick",
        "liquidity",
        "sqrt_price_x96",
        "token0_weight",
        "token1_weight",
        "price",
    ):
        assert name not in snap_fields, f"{name} must be deferred to a follow-up ticket"


def test_pool_snapshot_field_types() -> None:
    snap_fields = {d.name: d for d in gateway_pb2.PoolSnapshot.DESCRIPTOR.fields}
    assert snap_fields["timestamp"].type == FieldDescriptor.TYPE_INT64
    for name in ("tvl", "volume_24h", "fee_revenue_24h", "token0_reserve", "token1_reserve"):
        assert snap_fields[name].type == FieldDescriptor.TYPE_STRING, name
    unmeasured = snap_fields["unmeasured_fields"]
    assert unmeasured.is_repeated
    assert unmeasured.type == FieldDescriptor.TYPE_STRING


def test_pool_history_request_required_fields_and_types() -> None:
    req_fields = {d.name: d for d in gateway_pb2.PoolHistoryRequest.DESCRIPTOR.fields}
    for name in ("pool_address", "chain", "protocol", "start_ts", "end_ts", "resolution"):
        assert name in req_fields, name
    assert req_fields["pool_address"].type == FieldDescriptor.TYPE_STRING
    assert req_fields["chain"].type == FieldDescriptor.TYPE_STRING
    assert req_fields["protocol"].type == FieldDescriptor.TYPE_STRING
    assert req_fields["start_ts"].type == FieldDescriptor.TYPE_INT64
    assert req_fields["end_ts"].type == FieldDescriptor.TYPE_INT64
    resolution = req_fields["resolution"]
    assert resolution.type == FieldDescriptor.TYPE_ENUM
    assert resolution.enum_type is not None
    assert resolution.enum_type.name == "Resolution"


def test_pool_history_response_truncation_reason_is_enum_not_bool() -> None:
    """Codex Round-1 C1: a regression to ``bool truncated`` would silently
    break the CAP_EXCEEDED vs PROVIDER_RETENTION distinction and cause
    callers to infinite-loop on PROVIDER_RETENTION."""
    resp_fields = {d.name: d for d in gateway_pb2.PoolHistoryResponse.DESCRIPTOR.fields}
    tr = resp_fields["truncation_reason"]
    assert tr.type == FieldDescriptor.TYPE_ENUM, (
        f"truncation_reason must be TYPE_ENUM, got {tr.type}; bool is forbidden"
    )
    assert tr.enum_type is not None
    assert tr.enum_type.name == "TruncationReason"
    # Anti-regression: no field named "truncated" anywhere.
    assert "truncated" not in resp_fields, "bool `truncated` field is forbidden (Codex C1)"


def test_pool_history_response_envelope_field_types() -> None:
    resp_fields = {d.name: d for d in gateway_pb2.PoolHistoryResponse.DESCRIPTOR.fields}
    for name in (
        "snapshots",
        "truncation_reason",
        "next_start_ts",
        "source",
        "finalized_only",
        "success",
        "error",
    ):
        assert name in resp_fields, name
    snapshots = resp_fields["snapshots"]
    assert snapshots.type == FieldDescriptor.TYPE_MESSAGE
    assert snapshots.message_type.name == "PoolSnapshot"
    assert snapshots.is_repeated
    assert resp_fields["next_start_ts"].type == FieldDescriptor.TYPE_INT64
    assert resp_fields["source"].type == FieldDescriptor.TYPE_STRING
    assert resp_fields["finalized_only"].type == FieldDescriptor.TYPE_BOOL
    assert resp_fields["success"].type == FieldDescriptor.TYPE_BOOL
    assert resp_fields["error"].type == FieldDescriptor.TYPE_STRING


def test_pool_history_service_rpc_signature() -> None:
    fd = gateway_pb2.DESCRIPTOR
    svc = fd.services_by_name.get("PoolHistoryService")
    assert svc is not None, "service PoolHistoryService missing from proto"
    method = svc.methods_by_name.get("GetPoolHistory")
    assert method is not None, "rpc GetPoolHistory missing from PoolHistoryService"
    assert method.input_type.name == "PoolHistoryRequest"
    assert method.output_type.name == "PoolHistoryResponse"
    # No streaming (UAT card D1: server-streaming and client-streaming forbidden).
    assert method.client_streaming is False
    assert method.server_streaming is False


def test_pool_history_grpc_stubs_generated() -> None:
    """The gateway_pb2_grpc.py file must expose both the client stub and
    the servicer-base class (POOL-2 will subclass the latter)."""
    from almanak.gateway.proto import gateway_pb2_grpc

    assert hasattr(gateway_pb2_grpc, "PoolHistoryServiceStub")
    assert hasattr(gateway_pb2_grpc, "PoolHistoryServiceServicer")
    assert hasattr(gateway_pb2_grpc, "add_PoolHistoryServiceServicer_to_server")
