"""Higher-level unit tests for ``PositionServiceServicer.Reconcile`` (T24 / VIB-4210).

These tests mock the ``rpc_servicer`` + ``state_servicer`` dependencies and
exercise the Reconcile RPC's full algorithm (validate → sample head →
enumerate on-chain → read registry → diff → optionally apply → build response).

Anvil-fork integration tests live separately (covered by the UAT card §D1/D2/D3
manual run). This file pins the in-RPC contract surface so regressions don't
slip past.
"""

from __future__ import annotations

import json
import os
import tempfile
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import grpc
import pytest
import pytest_asyncio

from almanak.framework.accounting.commit import RegistryRow
from almanak.framework.observability.ledger import LedgerEntry
from almanak.framework.primitives.types import AccountingCategory, Primitive
from almanak.framework.state.state_manager import (
    SQLiteConfigLight,
    StateManager,
    StateManagerConfig,
    WarmBackendType,
)
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.position_service import PositionServiceServicer


class _NoopGrpcContext:
    def __init__(self) -> None:
        self.code: grpc.StatusCode | None = None
        self.details: str = ""

    def set_code(self, code: grpc.StatusCode) -> None:
        self.code = code

    def set_details(self, details: str) -> None:
        self.details = details


def _make_rpc_response_success(result_str: str) -> gateway_pb2.RpcResponse:
    """Helper to build a successful RpcResponse with JSON-encoded result."""
    return gateway_pb2.RpcResponse(success=True, result=json.dumps(result_str))


def _make_rpc_response_failure(error: str) -> gateway_pb2.RpcResponse:
    return gateway_pb2.RpcResponse(success=False, error=error)


@pytest_asyncio.fixture
async def temp_state_manager():
    """A real StateManager backed by a temp SQLite DB."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test_pos.db")
        config = StateManagerConfig(
            warm_backend=WarmBackendType.SQLITE,
            sqlite_config=SQLiteConfigLight(db_path=db_path, wal_mode=False),
            load_state_on_startup=False,
        )
        manager = StateManager(config)
        await manager.initialize()
        yield manager, db_path
        await manager.close()


@pytest.fixture
def mock_state_servicer(temp_state_manager):
    """A mocked state_servicer that wires to the real StateManager."""
    manager, _db_path = temp_state_manager
    mock = MagicMock()
    mock._state_manager = manager
    mock._snapshot_pool = None  # force SQLite path

    async def fake_ensure_init():
        pass

    async def fake_ensure_pool():
        pass

    mock._ensure_initialized = fake_ensure_init
    mock._ensure_snapshot_pool = fake_ensure_pool
    return mock


@pytest.fixture
def mock_rpc_servicer_empty_wallet():
    """RPC servicer that returns balanceOf=0 on every NPM (empty wallet)."""
    rpc = MagicMock()

    async def fake_call(request, context):
        if request.method == "eth_blockNumber":
            return _make_rpc_response_success("0xbc614e")  # 12345678
        # balanceOf returns 0
        return _make_rpc_response_success("0x0000000000000000000000000000000000000000000000000000000000000000")

    rpc.Call = AsyncMock(side_effect=fake_call)
    return rpc


@pytest.mark.asyncio
async def test_reconcile_empty_diff_no_writes(mock_rpc_servicer_empty_wallet, mock_state_servicer):
    """UAT §D1.S2 — empty registry + 0 on-chain LP NFTs => empty diff, no writes."""
    servicer = PositionServiceServicer(settings=None)  # type: ignore[arg-type]
    servicer.rpc_servicer = mock_rpc_servicer_empty_wallet
    servicer.state_servicer = mock_state_servicer

    request = gateway_pb2.ReconcileRequest(
        deployment_id="TestStrat:abc",
        chain="arbitrum",
        wallet_address="0xdeadbeef0000000000000000000000000000abcd",
        primitives=["lp"],
        apply=False,
    )
    ctx = _NoopGrpcContext()
    response = await servicer.Reconcile(request, ctx)

    assert ctx.code is None  # no error
    assert response.matched_count == 0
    assert response.phantom_missing_count == 0
    assert response.stranded_count == 0
    assert response.rebuilt_count == 0
    assert response.reconciliation_id  # UUID is non-empty
    assert response.source_block_number == 12345678


@pytest.mark.asyncio
async def test_reconcile_validation_failures(mock_rpc_servicer_empty_wallet, mock_state_servicer):
    """UAT §D3.F6 — request validation surfaces typed gRPC errors."""
    servicer = PositionServiceServicer(settings=None)  # type: ignore[arg-type]
    servicer.rpc_servicer = mock_rpc_servicer_empty_wallet
    servicer.state_servicer = mock_state_servicer

    # Missing deployment_id
    ctx = _NoopGrpcContext()
    await servicer.Reconcile(
        gateway_pb2.ReconcileRequest(chain="arbitrum", wallet_address="0xabc"), ctx
    )
    assert ctx.code == grpc.StatusCode.INVALID_ARGUMENT
    assert "deployment_id" in ctx.details

    # Unknown primitive
    ctx2 = _NoopGrpcContext()
    await servicer.Reconcile(
        gateway_pb2.ReconcileRequest(
            deployment_id="TestStrat:abc",
            chain="arbitrum",
            wallet_address="0xabc",
            primitives=["bogus"],
        ),
        ctx2,
    )
    assert ctx2.code == grpc.StatusCode.INVALID_ARGUMENT
    assert "bogus" in ctx2.details


@pytest.mark.asyncio
async def test_reconcile_stranded_diff_no_writes(mock_rpc_servicer_empty_wallet, mock_state_servicer, temp_state_manager):
    """UAT §D3.F8 — stranded row reported but NOT auto-closed.

    Setup: registry has an OPEN row, chain has nothing. Reconcile reports
    stranded_count=1 with rebuilt_count=0; the registry row's status is
    STILL 'open' after the call.
    """
    manager, db_path = temp_state_manager
    # Pre-populate registry with one open row.
    ledger = LedgerEntry(
        id="seed-1",
        cycle_id="cycle-1",
        deployment_id="TestStrat:abc",
        execution_mode="live",
        timestamp=datetime.now(UTC),
        intent_type="LP_OPEN",
        token_in="USDC",
        amount_in="100",
        token_out="WETH",
        amount_out="0.04",
        effective_price="2500",
        slippage_bps=10.0,
        gas_used=200000,
        gas_usd="0.50",
        tx_hash="0xseed",
        chain="arbitrum",
        protocol="uniswap_v3",
        success=True,
        error="",
    )
    registry = RegistryRow(
        deployment_id="TestStrat:abc",
        chain="arbitrum",
        primitive=Primitive.LP,
        accounting_category=AccountingCategory.LP,
        physical_identity_hash="hash_seed",
        semantic_grouping_key="arbitrum:seed_pool",
        grouping_policy_version="univ3_lp@v1",
        status="open",
        payload={"source": "intent", "token_id": 999},
        matching_policy_version=1,
        opened_at_block=1000,
        opened_tx="0xseed",
    )
    await manager.save_ledger_and_registry(ledger=ledger, registry=registry, handle=None)

    # Run reconcile — chain reports 0 positions but registry has one open row.
    servicer = PositionServiceServicer(settings=None)  # type: ignore[arg-type]
    servicer.rpc_servicer = mock_rpc_servicer_empty_wallet
    servicer.state_servicer = mock_state_servicer

    ctx = _NoopGrpcContext()
    request = gateway_pb2.ReconcileRequest(
        deployment_id="TestStrat:abc",
        chain="arbitrum",
        wallet_address="0xdeadbeef",
        primitives=["lp"],
        apply=True,  # IMPORTANT: even with apply=true, stranded rows are NOT auto-closed
    )
    response = await servicer.Reconcile(request, ctx)
    assert ctx.code is None
    assert response.stranded_count == 1
    assert response.rebuilt_count == 0
    assert response.matched_count == 0
    assert response.phantom_missing_count == 0

    # Verify the registry row's status is STILL 'open' (NOT auto-closed).
    import sqlite3

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            "SELECT status FROM position_registry WHERE physical_identity_hash = ?",
            ("hash_seed",),
        ).fetchone()
    finally:
        conn.close()
    assert row is not None
    assert row["status"] == "open", "stranded row MUST NOT be auto-closed (ADR §5.4)"


# =============================================================================
# Round 2 — CodeRabbit MAJOR follow-ups (PR #2240)
# =============================================================================


class _FakeWalletRegistryRaises:
    """A wallet registry stub whose ``.resolve()`` always raises."""

    def __init__(self, exc: Exception) -> None:
        self._exc = exc

    def resolve(self, chain: str):  # noqa: ARG002
        raise self._exc


class _FakeWalletRegistryReturnsNone:
    """A wallet registry stub whose ``.resolve()`` returns None."""

    def resolve(self, chain: str):  # noqa: ARG002
        return None


@pytest.mark.asyncio
async def test_reconcile_wallet_registry_keyerror_fails_closed(
    mock_rpc_servicer_empty_wallet, mock_state_servicer
):
    """CodeRabbit round 2 MAJOR — registry KeyError MUST fail closed.

    When ``wallet_registry.resolve()`` raises KeyError (no mapping for the
    chain), the request MUST be rejected with FAILED_PRECONDITION instead
    of being silently allowed through. Swallowing the error and returning
    True would let a misconfigured registry bypass ownership validation
    in the multi-tenant hosted posture.
    """
    servicer = PositionServiceServicer(settings=None)  # type: ignore[arg-type]
    servicer.rpc_servicer = mock_rpc_servicer_empty_wallet
    servicer.state_servicer = mock_state_servicer
    servicer.wallet_registry = _FakeWalletRegistryRaises(KeyError("arbitrum"))

    ctx = _NoopGrpcContext()
    request = gateway_pb2.ReconcileRequest(
        deployment_id="TestStrat:abc",
        chain="arbitrum",
        wallet_address="0xdeadbeef0000000000000000000000000000abcd",
        primitives=["lp"],
        apply=False,
    )
    response = await servicer.Reconcile(request, ctx)

    assert ctx.code == grpc.StatusCode.FAILED_PRECONDITION
    assert "no registered wallet mapping" in ctx.details
    # Response is the empty default proto — no reconciliation performed.
    assert response.reconciliation_id == ""


@pytest.mark.asyncio
async def test_reconcile_wallet_registry_broad_exception_fails_closed(
    mock_rpc_servicer_empty_wallet, mock_state_servicer
):
    """CodeRabbit round 2 MAJOR — registry plugin exception MUST fail closed.

    When ``wallet_registry.resolve()`` raises any unexpected exception
    (registry plugin contract is "anything"), the request MUST be rejected
    with INTERNAL. The previous "log and return True" behaviour
    silently allowed unauthenticated requests through when the registry
    was unavailable.
    """
    servicer = PositionServiceServicer(settings=None)  # type: ignore[arg-type]
    servicer.rpc_servicer = mock_rpc_servicer_empty_wallet
    servicer.state_servicer = mock_state_servicer
    servicer.wallet_registry = _FakeWalletRegistryRaises(RuntimeError("plugin boom"))

    ctx = _NoopGrpcContext()
    request = gateway_pb2.ReconcileRequest(
        deployment_id="TestStrat:abc",
        chain="arbitrum",
        wallet_address="0xdeadbeef0000000000000000000000000000abcd",
        primitives=["lp"],
        apply=False,
    )
    response = await servicer.Reconcile(request, ctx)

    assert ctx.code == grpc.StatusCode.INTERNAL
    assert "wallet registry unavailable" in ctx.details
    assert response.reconciliation_id == ""


@pytest.mark.asyncio
async def test_reconcile_wallet_registry_returns_none_fails_closed(
    mock_rpc_servicer_empty_wallet, mock_state_servicer
):
    """CodeRabbit round 2 MAJOR — registry returning None MUST fail closed.

    When the registry maps the chain but resolves to None (no wallet
    registered for this chain), the request MUST be rejected with
    FAILED_PRECONDITION. Treating None as "no opinion, proceed" would
    bypass ownership validation.
    """
    servicer = PositionServiceServicer(settings=None)  # type: ignore[arg-type]
    servicer.rpc_servicer = mock_rpc_servicer_empty_wallet
    servicer.state_servicer = mock_state_servicer
    servicer.wallet_registry = _FakeWalletRegistryReturnsNone()

    ctx = _NoopGrpcContext()
    request = gateway_pb2.ReconcileRequest(
        deployment_id="TestStrat:abc",
        chain="arbitrum",
        wallet_address="0xdeadbeef0000000000000000000000000000abcd",
        primitives=["lp"],
        apply=False,
    )
    response = await servicer.Reconcile(request, ctx)

    assert ctx.code == grpc.StatusCode.FAILED_PRECONDITION
    assert "no registered wallet mapping" in ctx.details
    assert response.reconciliation_id == ""


@pytest.mark.asyncio
async def test_reconcile_first_page_rejects_nonzero_max_age_blocks(
    mock_rpc_servicer_empty_wallet, mock_state_servicer
):
    """CodeRabbit round 2 MAJOR — first-page max_age_blocks > 0 MUST be rejected.

    In v1 the gateway has a single RPC source for both observed head and
    reference head, so on first-page requests there is no independent
    freshness oracle. Silently accepting non-zero max_age_blocks would
    mislead callers into thinking the guardrail is active. The gateway
    rejects with INVALID_ARGUMENT until reference-head freshness is
    implemented (T24+1).
    """
    servicer = PositionServiceServicer(settings=None)  # type: ignore[arg-type]
    servicer.rpc_servicer = mock_rpc_servicer_empty_wallet
    servicer.state_servicer = mock_state_servicer

    ctx = _NoopGrpcContext()
    request = gateway_pb2.ReconcileRequest(
        deployment_id="TestStrat:abc",
        chain="arbitrum",
        wallet_address="0xdeadbeef0000000000000000000000000000abcd",
        primitives=["lp"],
        apply=False,
        max_age_blocks=32,  # non-zero, first page (no page_cursor)
    )
    response = await servicer.Reconcile(request, ctx)

    assert ctx.code == grpc.StatusCode.INVALID_ARGUMENT
    assert "max_age_blocks" in ctx.details
    assert "first-page" in ctx.details
    # Reconciliation didn't run — no counts.
    assert response.matched_count == 0
    assert response.phantom_missing_count == 0
    assert response.stranded_count == 0
    assert response.rebuilt_count == 0


@pytest.mark.asyncio
async def test_reconcile_first_page_accepts_zero_max_age_blocks(
    mock_rpc_servicer_empty_wallet, mock_state_servicer
):
    """First-page request with max_age_blocks=0 (default) is accepted.

    Companion to the rejection test above — confirms the default CLI
    invocation (which sends max_age_blocks=0) still works.
    """
    servicer = PositionServiceServicer(settings=None)  # type: ignore[arg-type]
    servicer.rpc_servicer = mock_rpc_servicer_empty_wallet
    servicer.state_servicer = mock_state_servicer

    ctx = _NoopGrpcContext()
    request = gateway_pb2.ReconcileRequest(
        deployment_id="TestStrat:abc",
        chain="arbitrum",
        wallet_address="0xdeadbeef0000000000000000000000000000abcd",
        primitives=["lp"],
        apply=False,
        max_age_blocks=0,
    )
    response = await servicer.Reconcile(request, ctx)

    assert ctx.code is None
    assert response.reconciliation_id  # success path
