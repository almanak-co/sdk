"""VIB-4493 Phase 1C/D — tests for the reconciliation triad +
RefreshRegistryFromChain.

Coverage:
  - PreviewTokenStore lifecycle (issue, consume OK, consume EXPIRED,
    consume NOT_FOUND, consume WRONG_STRATEGY, gc_expired)
  - ReconciliationReportCache TTL behavior
  - StateFingerprint equality semantics
  - reconcile_response_to_report bucket → finding mapping
  - categorize_apply_result decision matrix
  - GetReconciliationReport handler: invalid arg, no chain/wallet, happy path
  - PreviewReconcile + ApplyReconcile end-to-end: token issue → consume → apply
  - ApplyReconcile STATE_DRIFT detection (fingerprint mismatch)
  - ApplyReconcile NOT_FOUND / EXPIRED / WRONG_STRATEGY paths
  - RefreshRegistryFromChain happy path + RATE_LIMITED (concurrent lock)

Mocking approach: position_servicer is a MagicMock with an awaitable
Reconcile method returning a hand-built gateway_pb2.ReconcileResponse.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import grpc
import pytest

from almanak.framework.portfolio.models import PortfolioSnapshot, ValueConfidence
from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services._dashboard_phase1 import (
    PreviewTokenStore,
    ReconciliationReportCache,
    StateFingerprint,
    categorize_apply_result,
    compute_state_fingerprint,
    reconcile_response_to_report,
)
from almanak.gateway.services.dashboard_service import DashboardServiceServicer


@pytest.fixture
def settings() -> GatewaySettings:
    return GatewaySettings()


@pytest.fixture
def mock_context() -> MagicMock:
    return MagicMock(spec=grpc.aio.ServicerContext)


@pytest.fixture
def dashboard_service(settings: GatewaySettings) -> DashboardServiceServicer:
    return DashboardServiceServicer(settings)


def _portfolio_snapshot(*, chain: str = "base", wallet: str = "0xwallet") -> PortfolioSnapshot:
    snap = PortfolioSnapshot(
        strategy_id="test_strategy",
        timestamp=datetime(2026, 5, 17, 12, 0, tzinfo=UTC),
        total_value_usd=1000.0,
        available_cash_usd=100.0,
        deployed_capital_usd=900.0,
        wallet_total_value_usd=1000.0,
        value_confidence=ValueConfidence.HIGH,
        positions=[],
        token_prices={},
        wallet_balances=[],
        chain=chain,
    )
    # PortfolioSnapshot doesn't declare wallet_address — attach via setattr
    # to mirror the legacy shape some callers expect.
    object.__setattr__(snap, "wallet_address", wallet) if hasattr(snap, "__dict__") else setattr(snap, "wallet_address", wallet)
    return snap


def _reconcile_response(
    *,
    reconciliation_id: str = "recon-abc",
    source_block_number: int = 1000,
    matched_count: int = 2,
    phantom_count: int = 1,
    stranded_count: int = 0,
    rebuilt_count: int = 0,
    primitive_errors: list[gateway_pb2.PrimitiveError] | None = None,
) -> gateway_pb2.ReconcileResponse:
    response = gateway_pb2.ReconcileResponse(
        reconciliation_id=reconciliation_id,
        source_block_number=source_block_number,
        matched_count=matched_count,
        phantom_missing_count=phantom_count,
        stranded_count=stranded_count,
        rebuilt_count=rebuilt_count,
    )
    for i in range(matched_count):
        response.matched.append(
            gateway_pb2.MatchedPosition(
                physical_identity_hash=f"hash_match_{i}",
                primitive="lp",
                accounting_category="LP_UNIV3",
                confirmed_at_block=source_block_number,
            )
        )
    for i in range(phantom_count):
        response.phantom_missing.append(
            gateway_pb2.PhantomMissingPosition(
                physical_identity_hash=f"hash_phantom_{i}",
                primitive="lp",
                accounting_category="LP_UNIV3",
                semantic_grouping_key="grp",
                opened_at_block=source_block_number - 10,
                opened_tx=f"0xphantom{i}",
            )
        )
    for i in range(stranded_count):
        response.stranded.append(
            gateway_pb2.StrandedRow(
                physical_identity_hash=f"hash_strand_{i}",
                primitive="lp",
                accounting_category="LP_UNIV3",
                handle=f"strand_{i}",
                absent_reason="position no longer on chain",
                confirmed_absent_at_block=source_block_number,
            )
        )
    for i in range(rebuilt_count):
        response.rebuilt.append(
            gateway_pb2.RebuiltRow(
                physical_identity_hash=f"hash_rebuild_{i}",
                primitive="lp",
                accounting_category="LP_UNIV3",
                source="reconciliation_discovery",
                last_reconciled_at_block=source_block_number,
                reconciliation_id=reconciliation_id,
            )
        )
    if primitive_errors:
        for err in primitive_errors:
            response.primitive_errors.append(err)
    return response


def _wire_state_manager(dashboard_service: DashboardServiceServicer, snap: PortfolioSnapshot | None = None) -> MagicMock:
    sm = MagicMock()
    sm.get_latest_snapshot = AsyncMock(return_value=snap)
    sm.get_position_registry_open_rows = AsyncMock(return_value=[])
    sm.get_ledger_entries = AsyncMock(return_value=[])
    dashboard_service._state_manager = sm
    return sm


def _wire_position_servicer(
    dashboard_service: DashboardServiceServicer,
    reconcile_response: gateway_pb2.ReconcileResponse,
) -> MagicMock:
    ps = MagicMock()
    ps.Reconcile = AsyncMock(return_value=reconcile_response)
    dashboard_service.position_servicer = ps
    return ps


# =============================================================================
# PreviewTokenStore unit tests
# =============================================================================


class TestPreviewTokenStore:
    def test_issue_returns_unique_tokens(self) -> None:
        store = PreviewTokenStore()
        fp = StateFingerprint(registry_row_count=1, registry_max_block=100, ledger_max_id="L1", source_block_number=200)
        t1, _ = store.issue(strategy_id="s1", fingerprint=fp, reconcile_response=None, now_unix_seconds=1000)
        t2, _ = store.issue(strategy_id="s1", fingerprint=fp, reconcile_response=None, now_unix_seconds=1000)
        assert t1 != t2
        assert t1.startswith("preview-")

    def test_consume_ok_removes_entry(self) -> None:
        store = PreviewTokenStore()
        fp = StateFingerprint(1, 100, "L1", 200)
        t, _ = store.issue(strategy_id="s1", fingerprint=fp, reconcile_response="rr", now_unix_seconds=1000)
        status, entry = store.consume(token=t, strategy_id="s1", now_unix_seconds=1100)
        assert status == "OK"
        assert entry is not None
        assert entry.reconcile_response == "rr"
        # Second consume → NOT_FOUND.
        status2, _ = store.consume(token=t, strategy_id="s1", now_unix_seconds=1100)
        assert status2 == "NOT_FOUND"

    def test_consume_expired(self) -> None:
        store = PreviewTokenStore(default_ttl_seconds=10)
        fp = StateFingerprint(1, 100, "L1", 200)
        t, _ = store.issue(strategy_id="s1", fingerprint=fp, reconcile_response="rr", now_unix_seconds=1000)
        # 20 seconds later, well past TTL.
        status, entry = store.consume(token=t, strategy_id="s1", now_unix_seconds=1020)
        assert status == "EXPIRED"
        assert entry is None

    def test_consume_wrong_strategy(self) -> None:
        store = PreviewTokenStore()
        fp = StateFingerprint(1, 100, "L1", 200)
        t, _ = store.issue(strategy_id="s1", fingerprint=fp, reconcile_response="rr", now_unix_seconds=1000)
        status, entry = store.consume(token=t, strategy_id="other", now_unix_seconds=1000)
        assert status == "WRONG_STRATEGY"
        assert entry is None
        # Token NOT consumed — original strategy can still use it.
        status2, entry2 = store.consume(token=t, strategy_id="s1", now_unix_seconds=1000)
        assert status2 == "OK"
        assert entry2 is not None

    def test_gc_expired(self) -> None:
        store = PreviewTokenStore(default_ttl_seconds=5)
        fp = StateFingerprint(1, 100, "L1", 200)
        for i in range(3):
            store.issue(strategy_id=f"s{i}", fingerprint=fp, reconcile_response=None, now_unix_seconds=1000)
        purged = store.gc_expired(now_unix_seconds=2000)
        assert purged == 3
        purged_again = store.gc_expired(now_unix_seconds=2000)
        assert purged_again == 0


# =============================================================================
# ReconciliationReportCache unit tests
# =============================================================================


class TestReconciliationReportCache:
    def test_miss_then_put_then_hit(self) -> None:
        cache = ReconciliationReportCache(ttl_seconds=5)
        assert cache.get("s1", now_unix_seconds=1000) is None
        cache.put("s1", "report", now_unix_seconds=1000)
        assert cache.get("s1", now_unix_seconds=1003) == "report"

    def test_expires_after_ttl(self) -> None:
        cache = ReconciliationReportCache(ttl_seconds=5)
        cache.put("s1", "report", now_unix_seconds=1000)
        assert cache.get("s1", now_unix_seconds=1010) is None

    def test_isolation_per_strategy(self) -> None:
        cache = ReconciliationReportCache(ttl_seconds=5)
        cache.put("s1", "report1", now_unix_seconds=1000)
        assert cache.get("s2", now_unix_seconds=1000) is None


# =============================================================================
# StateFingerprint + compute_state_fingerprint unit tests
# =============================================================================


class TestStateFingerprint:
    def test_equality(self) -> None:
        a = StateFingerprint(1, 100, "L1", 200)
        b = StateFingerprint(1, 100, "L1", 200)
        c = StateFingerprint(1, 100, "L1", 201)
        assert a.equals(b)
        assert not a.equals(c)

    def test_compute_from_rows(self) -> None:
        rows = [
            {"last_reconciled_at_block": 100, "opened_at_block": 90},
            {"last_reconciled_at_block": 200, "opened_at_block": 150},
            {"last_reconciled_at_block": None, "opened_at_block": 50},
        ]
        fp = compute_state_fingerprint(registry_rows=rows, ledger_max_id="L42", source_block_number=300)
        assert fp.registry_row_count == 3
        assert fp.registry_max_block == 200  # max of valid last_reconciled values
        assert fp.ledger_max_id == "L42"
        assert fp.source_block_number == 300


# =============================================================================
# reconcile_response_to_report mapping unit tests
# =============================================================================


class TestReconcileResponseToReport:
    def test_matched_becomes_info_finding(self) -> None:
        rr = _reconcile_response(matched_count=2, phantom_count=0, stranded_count=0)
        report = reconcile_response_to_report(reconcile_response=rr, now_unix_seconds=1000)
        info_findings = [f for f in report.findings if f.severity == gateway_pb2.RECONCILIATION_SEVERITY_INFO]
        assert len(info_findings) == 2

    def test_phantom_becomes_diverged_finding(self) -> None:
        rr = _reconcile_response(matched_count=0, phantom_count=2, stranded_count=0)
        report = reconcile_response_to_report(reconcile_response=rr, now_unix_seconds=1000)
        diverged = [f for f in report.findings if f.severity == gateway_pb2.RECONCILIATION_SEVERITY_DIVERGED]
        assert len(diverged) == 2
        assert "PreviewReconcile" in diverged[0].suggested_action

    def test_stranded_becomes_warn_finding(self) -> None:
        rr = _reconcile_response(matched_count=0, phantom_count=0, stranded_count=1)
        report = reconcile_response_to_report(reconcile_response=rr, now_unix_seconds=1000)
        warns = [f for f in report.findings if f.severity == gateway_pb2.RECONCILIATION_SEVERITY_WARN]
        assert len(warns) == 1

    def test_parser_unsupported_surfaces_stub(self) -> None:
        err = gateway_pb2.PrimitiveError(
            primitive="lending",
            chain="base",
            code="PARSER_UNSUPPORTED",
            message="lending not in v1",
            recoverable=False,
        )
        rr = _reconcile_response(matched_count=0, phantom_count=0, primitive_errors=[err])
        report = reconcile_response_to_report(reconcile_response=rr, now_unix_seconds=1000)
        assert len(report.primitive_stubs) == 1
        assert report.primitive_stubs[0].primitive == "lending"
        assert report.primitive_stubs[0].ticket == "VIB-4501"


# =============================================================================
# categorize_apply_result decision matrix
# =============================================================================


class TestCategorizeApplyResult:
    def test_fingerprint_mismatch_returns_state_drift(self) -> None:
        rr = _reconcile_response()
        result, detail = categorize_apply_result(reconcile_response=rr, fingerprint_matched=False)
        assert result == "STATE_DRIFT"
        assert "re-issue PreviewReconcile" in detail

    def test_partial_when_primitive_errors_present(self) -> None:
        err = gateway_pb2.PrimitiveError(primitive="lp", chain="base", code="BACKEND_TIMEOUT", message="timed out", recoverable=True)
        rr = _reconcile_response(primitive_errors=[err])
        result, _ = categorize_apply_result(reconcile_response=rr, fingerprint_matched=True)
        assert result == "PARTIAL_SUCCESS"

    def test_success_when_clean(self) -> None:
        rr = _reconcile_response()
        result, _ = categorize_apply_result(reconcile_response=rr, fingerprint_matched=True)
        assert result == "SUCCESS"


# =============================================================================
# GetReconciliationReport handler tests
# =============================================================================


@pytest.mark.asyncio
class TestGetReconciliationReport:
    async def test_invalid_strategy_id(self, dashboard_service: DashboardServiceServicer, mock_context: MagicMock) -> None:
        dashboard_service._initialized = True
        req = gateway_pb2.GetReconciliationReportRequest(strategy_id="")
        resp = await dashboard_service.GetReconciliationReport(req, mock_context)
        assert isinstance(resp, gateway_pb2.GetReconciliationReportResponse)
        mock_context.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)

    async def test_no_chain_wallet_returns_empty_report(self, dashboard_service: DashboardServiceServicer, mock_context: MagicMock) -> None:
        dashboard_service._initialized = True
        _wire_state_manager(dashboard_service, snap=None)
        req = gateway_pb2.GetReconciliationReportRequest(strategy_id="test_strategy")
        resp = await dashboard_service.GetReconciliationReport(req, mock_context)
        assert len(resp.findings) == 0
        assert resp.as_of != ""  # timestamp set even when degraded

    async def test_happy_path_returns_findings(self, dashboard_service: DashboardServiceServicer, mock_context: MagicMock) -> None:
        dashboard_service._initialized = True
        _wire_state_manager(dashboard_service, snap=_portfolio_snapshot())
        rr = _reconcile_response(matched_count=2, phantom_count=1)
        ps = _wire_position_servicer(dashboard_service, rr)

        req = gateway_pb2.GetReconciliationReportRequest(strategy_id="test_strategy")
        resp = await dashboard_service.GetReconciliationReport(req, mock_context)

        assert len(resp.findings) == 3  # 2 matched + 1 phantom
        assert resp.reconciliation_id == "recon-abc"
        ps.Reconcile.assert_awaited_once()
        # Called with apply=False (preview only).
        call_args = ps.Reconcile.await_args
        assert call_args.args[0].apply is False

    async def test_second_call_within_ttl_uses_cache(self, dashboard_service: DashboardServiceServicer, mock_context: MagicMock) -> None:
        dashboard_service._initialized = True
        _wire_state_manager(dashboard_service, snap=_portfolio_snapshot())
        rr = _reconcile_response(matched_count=1)
        ps = _wire_position_servicer(dashboard_service, rr)

        req = gateway_pb2.GetReconciliationReportRequest(strategy_id="test_strategy")
        await dashboard_service.GetReconciliationReport(req, mock_context)
        await dashboard_service.GetReconciliationReport(req, mock_context)
        # Only one upstream call despite two requests.
        assert ps.Reconcile.await_count == 1


# =============================================================================
# PreviewReconcile + ApplyReconcile end-to-end
# =============================================================================


@pytest.mark.asyncio
class TestPreviewApplyReconcile:
    async def test_preview_returns_token_and_diff(self, dashboard_service: DashboardServiceServicer, mock_context: MagicMock) -> None:
        dashboard_service._initialized = True
        _wire_state_manager(dashboard_service, snap=_portfolio_snapshot())
        rr = _reconcile_response(matched_count=1, phantom_count=2)
        _wire_position_servicer(dashboard_service, rr)

        req = gateway_pb2.PreviewReconcileRequest(strategy_id="test_strategy")
        resp = await dashboard_service.PreviewReconcile(req, mock_context)

        assert resp.preview_token.startswith("preview-")
        assert resp.expires_at_unix_seconds > 0
        assert len(resp.matched) == 1
        assert len(resp.phantom_missing) == 2
        assert resp.source_block_number == 1000

    async def test_apply_with_matching_fingerprint_succeeds(self, dashboard_service: DashboardServiceServicer, mock_context: MagicMock) -> None:
        dashboard_service._initialized = True
        _wire_state_manager(dashboard_service, snap=_portfolio_snapshot())
        rr_preview = _reconcile_response(matched_count=1, phantom_count=1)
        ps = _wire_position_servicer(dashboard_service, rr_preview)

        # 1) Preview.
        preview_resp = await dashboard_service.PreviewReconcile(
            gateway_pb2.PreviewReconcileRequest(strategy_id="test_strategy"), mock_context
        )
        # 2) Switch the position_servicer mock to return the apply-shaped response
        #    with the SAME source_block_number (no drift) and a rebuilt row.
        rr_apply = _reconcile_response(matched_count=1, phantom_count=0, rebuilt_count=1, source_block_number=1000)
        ps.Reconcile = AsyncMock(return_value=rr_apply)

        apply_resp = await dashboard_service.ApplyReconcile(
            gateway_pb2.ApplyReconcileRequest(strategy_id="test_strategy", preview_token=preview_resp.preview_token),
            mock_context,
        )
        assert apply_resp.result == "SUCCESS"
        assert len(apply_resp.rebuilt) == 1

    async def test_apply_with_drift_returns_state_drift(self, dashboard_service: DashboardServiceServicer, mock_context: MagicMock) -> None:
        dashboard_service._initialized = True
        _wire_state_manager(dashboard_service, snap=_portfolio_snapshot())
        rr_preview = _reconcile_response(source_block_number=1000)
        ps = _wire_position_servicer(dashboard_service, rr_preview)

        preview_resp = await dashboard_service.PreviewReconcile(
            gateway_pb2.PreviewReconcileRequest(strategy_id="test_strategy"), mock_context
        )
        # Apply returns a DIFFERENT source_block_number → fingerprint mismatch.
        rr_apply = _reconcile_response(source_block_number=1100)
        ps.Reconcile = AsyncMock(return_value=rr_apply)

        apply_resp = await dashboard_service.ApplyReconcile(
            gateway_pb2.ApplyReconcileRequest(strategy_id="test_strategy", preview_token=preview_resp.preview_token),
            mock_context,
        )
        assert apply_resp.result == "STATE_DRIFT"
        assert len(apply_resp.rebuilt) == 0

    async def test_apply_drift_check_runs_BEFORE_mutation(
        self, dashboard_service: DashboardServiceServicer, mock_context: MagicMock
    ) -> None:
        """Regression for Codex finding: ApplyReconcile previously called
        Reconcile(apply=True) and only compared the fingerprint AFTER —
        which meant STATE_DRIFT responses could lie about whether writes
        had already happened. The handler now does a dry-run
        (apply=False) first, compares fingerprints, and only fires the
        mutating call when the fingerprint matches.
        """
        dashboard_service._initialized = True
        _wire_state_manager(dashboard_service, snap=_portfolio_snapshot())
        rr_preview = _reconcile_response(source_block_number=1000)
        ps = _wire_position_servicer(dashboard_service, rr_preview)

        preview_resp = await dashboard_service.PreviewReconcile(
            gateway_pb2.PreviewReconcileRequest(strategy_id="test_strategy"), mock_context
        )

        # After preview, every Reconcile invocation (dry-run AND any apply)
        # returns a drifted block number. The handler MUST detect the drift
        # via the dry-run and skip the apply call entirely.
        rr_drifted = _reconcile_response(source_block_number=1100)
        ps.Reconcile = AsyncMock(return_value=rr_drifted)

        apply_resp = await dashboard_service.ApplyReconcile(
            gateway_pb2.ApplyReconcileRequest(
                strategy_id="test_strategy", preview_token=preview_resp.preview_token
            ),
            mock_context,
        )

        assert apply_resp.result == "STATE_DRIFT"
        assert len(apply_resp.rebuilt) == 0

        # Single Reconcile invocation = dry-run only. NO apply=True call.
        assert ps.Reconcile.call_count == 1
        sole_call = ps.Reconcile.call_args
        assert sole_call is not None
        req = sole_call.args[0]
        assert req.apply is False, (
            "On drift, only the dry-run (apply=False) should fire; the "
            "mutating call (apply=True) must NOT be invoked."
        )

    async def test_apply_no_drift_invokes_both_dry_run_and_apply(
        self, dashboard_service: DashboardServiceServicer, mock_context: MagicMock
    ) -> None:
        """No drift path: Reconcile is called twice — once apply=False for
        the drift check, then apply=True for the actual mutation."""
        dashboard_service._initialized = True
        _wire_state_manager(dashboard_service, snap=_portfolio_snapshot())
        rr = _reconcile_response(matched_count=1, phantom_count=0, rebuilt_count=1, source_block_number=1000)
        ps = _wire_position_servicer(dashboard_service, rr)

        preview_resp = await dashboard_service.PreviewReconcile(
            gateway_pb2.PreviewReconcileRequest(strategy_id="test_strategy"), mock_context
        )
        # Reconcile always returns block=1000 → no drift between preview and apply.
        ps.Reconcile = AsyncMock(return_value=rr)

        apply_resp = await dashboard_service.ApplyReconcile(
            gateway_pb2.ApplyReconcileRequest(
                strategy_id="test_strategy", preview_token=preview_resp.preview_token
            ),
            mock_context,
        )

        assert apply_resp.result == "SUCCESS"
        assert ps.Reconcile.call_count == 2
        # First call = dry-run, second = mutating apply.
        first_req = ps.Reconcile.call_args_list[0].args[0]
        second_req = ps.Reconcile.call_args_list[1].args[0]
        assert first_req.apply is False
        assert second_req.apply is True

    async def test_apply_with_unknown_token_returns_not_found(self, dashboard_service: DashboardServiceServicer, mock_context: MagicMock) -> None:
        dashboard_service._initialized = True
        _wire_state_manager(dashboard_service, snap=_portfolio_snapshot())
        apply_resp = await dashboard_service.ApplyReconcile(
            gateway_pb2.ApplyReconcileRequest(strategy_id="test_strategy", preview_token="preview-bogus"),
            mock_context,
        )
        assert apply_resp.result == "NOT_FOUND"

    async def test_apply_missing_token_returns_invalid_argument(self, dashboard_service: DashboardServiceServicer, mock_context: MagicMock) -> None:
        dashboard_service._initialized = True
        apply_resp = await dashboard_service.ApplyReconcile(
            gateway_pb2.ApplyReconcileRequest(strategy_id="test_strategy", preview_token=""),
            mock_context,
        )
        assert apply_resp.result == "INVALID_ARGUMENT"
        mock_context.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)


# =============================================================================
# RefreshRegistryFromChain
# =============================================================================


@pytest.mark.asyncio
class TestRefreshRegistryFromChain:
    async def test_invalid_strategy_id(self, dashboard_service: DashboardServiceServicer, mock_context: MagicMock) -> None:
        dashboard_service._initialized = True
        resp = await dashboard_service.RefreshRegistryFromChain(
            gateway_pb2.RefreshRegistryFromChainRequest(strategy_id=""), mock_context
        )
        assert resp.result == "INVALID_ARGUMENT"

    async def test_happy_path_returns_counts(self, dashboard_service: DashboardServiceServicer, mock_context: MagicMock) -> None:
        dashboard_service._initialized = True
        _wire_state_manager(dashboard_service, snap=_portfolio_snapshot())
        rr = _reconcile_response(matched_count=3, phantom_count=0, rebuilt_count=2, source_block_number=5000)
        _wire_position_servicer(dashboard_service, rr)

        resp = await dashboard_service.RefreshRegistryFromChain(
            gateway_pb2.RefreshRegistryFromChainRequest(strategy_id="test_strategy"), mock_context
        )
        assert resp.result == "SUCCESS"
        assert resp.positions_refreshed == 5  # matched + rebuilt
        assert resp.events_emitted == 2
        assert resp.source_block_number == 5000

    async def test_no_chain_wallet_returns_failed(self, dashboard_service: DashboardServiceServicer, mock_context: MagicMock) -> None:
        dashboard_service._initialized = True
        _wire_state_manager(dashboard_service, snap=None)
        resp = await dashboard_service.RefreshRegistryFromChain(
            gateway_pb2.RefreshRegistryFromChainRequest(strategy_id="test_strategy"), mock_context
        )
        assert resp.result == "FAILED"

    async def test_concurrent_call_returns_rate_limited(self, dashboard_service: DashboardServiceServicer, mock_context: MagicMock) -> None:
        dashboard_service._initialized = True
        _wire_state_manager(dashboard_service, snap=_portfolio_snapshot())

        # Make Reconcile take a measurable amount of time so the second call
        # arrives while the first holds the lock.
        async def slow_reconcile(*args: object, **kwargs: object) -> gateway_pb2.ReconcileResponse:
            await asyncio.sleep(0.05)
            return _reconcile_response(matched_count=1)

        ps = MagicMock()
        ps.Reconcile = AsyncMock(side_effect=slow_reconcile)
        dashboard_service.position_servicer = ps

        req = gateway_pb2.RefreshRegistryFromChainRequest(strategy_id="test_strategy")
        first_task = asyncio.create_task(dashboard_service.RefreshRegistryFromChain(req, mock_context))
        # Yield once so the first task acquires the lock before we call again.
        await asyncio.sleep(0.01)
        second = await dashboard_service.RefreshRegistryFromChain(req, mock_context)
        first = await first_task

        # First call: SUCCESS. Second call (concurrent): RATE_LIMITED.
        assert first.result == "SUCCESS"
        assert second.result == "RATE_LIMITED"


# =============================================================================
# Operator authorization gate (Codex review fix — opt-in operator token)
# =============================================================================


@pytest.mark.asyncio
class TestOperatorAuthorizationGate:
    """When `GatewaySettings.operator_token` (env
    `ALMANAK_GATEWAY_OPERATOR_TOKEN`) is set, the three mutation RPCs
    must require a matching `x-operator-token` metadata header. Without
    the setting, current behaviour is preserved (single-token auth gate
    is the only check). The handler reads via `self.settings.operator_token`
    rather than `os.environ` directly to respect the project's
    config-boundary lint."""

    def _ctx_with_metadata(self, metadata: dict) -> MagicMock:
        ctx = MagicMock(spec=grpc.aio.ServicerContext)
        ctx.invocation_metadata.return_value = list(metadata.items())
        # Abort needs to be awaitable + raise so the test simulates real gRPC.
        ctx.abort = AsyncMock(side_effect=grpc.RpcError("aborted"))
        return ctx

    async def test_unset_passes_through(
        self,
        dashboard_service: DashboardServiceServicer,
    ) -> None:
        dashboard_service.settings.operator_token = None
        ctx = self._ctx_with_metadata({})
        ok = await dashboard_service._require_operator_authorization(ctx)
        assert ok is True
        ctx.abort.assert_not_awaited()

    async def test_set_metadata_match_passes(
        self,
        dashboard_service: DashboardServiceServicer,
    ) -> None:
        dashboard_service.settings.operator_token = "secret-op"
        ctx = self._ctx_with_metadata({"x-operator-token": "secret-op"})
        ok = await dashboard_service._require_operator_authorization(ctx)
        assert ok is True
        ctx.abort.assert_not_awaited()

    async def test_set_metadata_missing_aborts(
        self,
        dashboard_service: DashboardServiceServicer,
    ) -> None:
        dashboard_service.settings.operator_token = "secret-op"
        ctx = self._ctx_with_metadata({})  # no header
        with pytest.raises(grpc.RpcError):
            await dashboard_service._require_operator_authorization(ctx)
        ctx.abort.assert_awaited_once()
        code, _ = ctx.abort.await_args.args
        assert code == grpc.StatusCode.PERMISSION_DENIED

    async def test_set_metadata_wrong_aborts(
        self,
        dashboard_service: DashboardServiceServicer,
    ) -> None:
        dashboard_service.settings.operator_token = "secret-op"
        ctx = self._ctx_with_metadata({"x-operator-token": "wrong"})
        with pytest.raises(grpc.RpcError):
            await dashboard_service._require_operator_authorization(ctx)
        ctx.abort.assert_awaited_once()

    async def test_set_bytes_metadata_decoded(
        self,
        dashboard_service: DashboardServiceServicer,
    ) -> None:
        """gRPC metadata values can arrive as bytes on some transports;
        the gate must decode before comparing."""
        dashboard_service.settings.operator_token = "secret-op"
        ctx = self._ctx_with_metadata({"x-operator-token": b"secret-op"})
        ok = await dashboard_service._require_operator_authorization(ctx)
        assert ok is True
