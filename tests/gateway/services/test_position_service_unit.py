"""Unit tests for the PositionService Reconcile RPC (T24 / VIB-4210).

Pure-function level tests of the four-bucket diff classifier, the cursor
encode/decode roundtrip, and request validation. Tests that need real chain
fanout (D2/D3 in the UAT card) live in
``tests/gateway/services/test_position_service.py`` (integration scope,
later in this PR).
"""

from __future__ import annotations

import grpc

from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.position_service import (
    DEFAULT_RECONCILIATION_PAGE_SIZE,
    MAX_RECONCILIATION_PAGE_SIZE,
    PositionServiceServicer,
    classify_diff,
    decode_cursor,
    encode_cursor,
)


# =============================================================================
# classify_diff — pure-function tests (UAT §D1.S3 + §D1.S4 four-bucket logic)
# =============================================================================


def test_classify_diff_empty_both_sides():
    matched, phantom, stranded = classify_diff(on_chain=[], registry=[])
    assert matched == []
    assert phantom == []
    assert stranded == []


def test_classify_diff_all_matched():
    on_chain = [
        {"physical_identity_hash": "h1", "primitive": "lp", "accounting_category": "lp"},
        {"physical_identity_hash": "h2", "primitive": "lp", "accounting_category": "lp"},
    ]
    registry = [
        {"physical_identity_hash": "h1", "primitive": "lp", "accounting_category": "lp"},
        {"physical_identity_hash": "h2", "primitive": "lp", "accounting_category": "lp"},
    ]
    matched, phantom, stranded = classify_diff(on_chain=on_chain, registry=registry)
    assert {m["physical_identity_hash"] for m in matched} == {"h1", "h2"}
    assert phantom == []
    assert stranded == []


def test_classify_diff_phantom_missing():
    """GH #2131 case: on-chain has a position, registry doesn't."""
    on_chain = [
        {"physical_identity_hash": "h_phantom", "primitive": "lp", "accounting_category": "lp"},
    ]
    registry = []
    matched, phantom, stranded = classify_diff(on_chain=on_chain, registry=registry)
    assert matched == []
    assert len(phantom) == 1
    assert phantom[0]["physical_identity_hash"] == "h_phantom"
    assert stranded == []


def test_classify_diff_stranded():
    """Registry has open row, chain doesn't have the position."""
    on_chain = []
    registry = [
        {"physical_identity_hash": "h_stranded", "primitive": "lp", "accounting_category": "lp"},
    ]
    matched, phantom, stranded = classify_diff(on_chain=on_chain, registry=registry)
    assert matched == []
    assert phantom == []
    assert len(stranded) == 1
    assert stranded[0]["physical_identity_hash"] == "h_stranded"


def test_classify_diff_mixed():
    on_chain = [
        {"physical_identity_hash": "h_match", "primitive": "lp", "accounting_category": "lp"},
        {"physical_identity_hash": "h_phantom", "primitive": "lp", "accounting_category": "lp"},
    ]
    registry = [
        {"physical_identity_hash": "h_match", "primitive": "lp", "accounting_category": "lp"},
        {"physical_identity_hash": "h_stranded", "primitive": "lp", "accounting_category": "lp"},
    ]
    matched, phantom, stranded = classify_diff(on_chain=on_chain, registry=registry)
    assert {m["physical_identity_hash"] for m in matched} == {"h_match"}
    assert {p["physical_identity_hash"] for p in phantom} == {"h_phantom"}
    assert {s["physical_identity_hash"] for s in stranded} == {"h_stranded"}


def test_classify_diff_deterministic_ordering():
    """Output ordering is deterministic (sorted by physical_identity_hash).

    Avoids flaky test snapshots and gives operators a stable diff to scan.
    """
    on_chain = [
        {"physical_identity_hash": "h_z", "primitive": "lp", "accounting_category": "lp"},
        {"physical_identity_hash": "h_a", "primitive": "lp", "accounting_category": "lp"},
        {"physical_identity_hash": "h_m", "primitive": "lp", "accounting_category": "lp"},
    ]
    registry = []
    _, phantom, _ = classify_diff(on_chain=on_chain, registry=registry)
    assert [p["physical_identity_hash"] for p in phantom] == ["h_a", "h_m", "h_z"]


# =============================================================================
# Cursor encode/decode roundtrip (ADR §4.2)
# =============================================================================


def test_cursor_encode_decode_roundtrip():
    cursor = encode_cursor(source_block_number=12345, last_primitive="lp", last_hash="h_abc")
    decoded = decode_cursor(cursor)
    assert decoded is not None
    assert decoded["source_block_number"] == 12345
    assert decoded["last_primitive"] == "lp"
    assert decoded["last_physical_identity_hash"] == "h_abc"
    assert decoded["schema_version"] == 1


def test_decode_cursor_empty_returns_none():
    assert decode_cursor(b"") is None


def test_decode_cursor_malformed_returns_none():
    assert decode_cursor(b"not_base64") is None
    assert decode_cursor(b"aGVsbG8=") is None  # b64-encoded "hello" — not JSON


def test_decode_cursor_schema_version_mismatch_returns_none():
    """A cursor from a future gateway schema must NOT be silently accepted.

    Forces FAILED_PRECONDITION + restart at page 0 — the safe default. UAT
    card §D3.F3 silent-error guard.
    """
    import base64
    import json

    payload = {
        "source_block_number": 100,
        "last_primitive": "lp",
        "last_physical_identity_hash": "h",
        "schema_version": 999,  # future
    }
    cursor = base64.b64encode(json.dumps(payload).encode("utf-8"))
    assert decode_cursor(cursor) is None


# =============================================================================
# Request validation (UAT §D3.F6 silent-error guard for INVALID_ARGUMENT)
# =============================================================================


class _MockContext:
    """Captures grpc status code + details set on a ServicerContext."""

    def __init__(self) -> None:
        self.code: grpc.StatusCode | None = None
        self.details: str = ""

    def set_code(self, code: grpc.StatusCode) -> None:
        self.code = code

    def set_details(self, details: str) -> None:
        self.details = details


def test_validate_request_rejects_empty_deployment_id():
    servicer = PositionServiceServicer(settings=None)  # type: ignore[arg-type]
    ctx = _MockContext()
    req = gateway_pb2.ReconcileRequest(deployment_id="", chain="arbitrum", wallet_address="0xabc")
    result = servicer._validate_request(req, ctx)
    assert result is None
    assert ctx.code == grpc.StatusCode.INVALID_ARGUMENT
    assert "deployment_id" in ctx.details


def test_validate_request_rejects_empty_chain():
    servicer = PositionServiceServicer(settings=None)  # type: ignore[arg-type]
    ctx = _MockContext()
    req = gateway_pb2.ReconcileRequest(
        deployment_id="TestStrat:abc", chain="", wallet_address="0xabc"
    )
    result = servicer._validate_request(req, ctx)
    assert result is None
    assert ctx.code == grpc.StatusCode.INVALID_ARGUMENT
    assert "chain" in ctx.details


def test_validate_request_rejects_empty_wallet():
    servicer = PositionServiceServicer(settings=None)  # type: ignore[arg-type]
    ctx = _MockContext()
    req = gateway_pb2.ReconcileRequest(
        deployment_id="TestStrat:abc", chain="arbitrum", wallet_address=""
    )
    result = servicer._validate_request(req, ctx)
    assert result is None
    assert ctx.code == grpc.StatusCode.INVALID_ARGUMENT
    assert "wallet_address" in ctx.details


def test_validate_request_rejects_unknown_primitive():
    """v1 supports only 'lp'; unknown primitives surface as INVALID_ARGUMENT.

    UAT card §D3.F6 silent-error guard — MUST NOT return empty diffs.
    """
    servicer = PositionServiceServicer(settings=None)  # type: ignore[arg-type]
    ctx = _MockContext()
    req = gateway_pb2.ReconcileRequest(
        deployment_id="TestStrat:abc",
        chain="arbitrum",
        wallet_address="0xabc",
        primitives=["nonsense"],
    )
    result = servicer._validate_request(req, ctx)
    assert result is None
    assert ctx.code == grpc.StatusCode.INVALID_ARGUMENT
    assert "nonsense" in ctx.details


def test_validate_request_defaults_to_lp_when_primitives_empty():
    servicer = PositionServiceServicer(settings=None)  # type: ignore[arg-type]
    ctx = _MockContext()
    req = gateway_pb2.ReconcileRequest(
        deployment_id="TestStrat:abc",
        chain="arbitrum",
        wallet_address="0xabc",
    )
    result = servicer._validate_request(req, ctx)
    assert result is not None
    _, _, _, primitives, _ = result
    assert primitives == ["lp"]


def test_validate_request_page_size_clamping():
    servicer = PositionServiceServicer(settings=None)  # type: ignore[arg-type]
    ctx = _MockContext()
    # 0 → default
    req_default = gateway_pb2.ReconcileRequest(
        deployment_id="TestStrat:abc",
        chain="arbitrum",
        wallet_address="0xabc",
        page_size=0,
    )
    result = servicer._validate_request(req_default, ctx)
    assert result is not None
    _, _, _, _, ps = result
    assert ps == DEFAULT_RECONCILIATION_PAGE_SIZE

    # >MAX → clamp silently per proto comment
    ctx2 = _MockContext()
    req_big = gateway_pb2.ReconcileRequest(
        deployment_id="TestStrat:abc",
        chain="arbitrum",
        wallet_address="0xabc",
        page_size=99999,
    )
    result = servicer._validate_request(req_big, ctx2)
    assert result is not None
    _, _, _, _, ps = result
    assert ps == MAX_RECONCILIATION_PAGE_SIZE


def test_validate_request_operator_note_size_cap():
    servicer = PositionServiceServicer(settings=None)  # type: ignore[arg-type]
    ctx = _MockContext()
    req = gateway_pb2.ReconcileRequest(
        deployment_id="TestStrat:abc",
        chain="arbitrum",
        wallet_address="0xabc",
        operator_note="X" * 300,
    )
    result = servicer._validate_request(req, ctx)
    assert result is None
    assert ctx.code == grpc.StatusCode.INVALID_ARGUMENT
    assert "operator_note" in ctx.details
