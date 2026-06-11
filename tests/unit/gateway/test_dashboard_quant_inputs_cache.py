"""VIB-5059 Phase 1 — one dashboard render must load the quant inputs ONCE.

A single page render fans out to ``GetPnLSummary`` + ``GetCostStack`` +
``GetAuditPosture``. Each previously called ``_load_quant_inputs``
independently, re-fetching up to ``_QUANT_HEADER_LEDGER_CAP`` (100k)
full-width ledger rows, every accounting event, and the recent snapshot
window — three times per render. The inputs change at snapshot/iteration
cadence (minutes), so a short-TTL single-flight cache coalesces the burst
into one load without making any tile observably stale.

These tests pin the contract: the three tile RPCs share one load, truly
concurrent calls coalesce (single-flight), distinct deployments never share
entries, and a zero TTL disables the cache (the kill-switch semantic).
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.dashboard_service import DashboardServiceServicer

_DEPLOYMENT = "33a657c4-d473-452b-8b47-a36e55dd820a"


class _CountingStateManager:
    """State-manager stub that counts every quant-input fetch."""

    def __init__(self) -> None:
        self.calls: dict[str, int] = {
            "get_portfolio_metrics": 0,
            "get_recent_snapshots": 0,
            "get_ledger_entries": 0,
            "get_accounting_events_for_dashboard": 0,
        }

    async def get_portfolio_metrics(self, deployment_id):
        self.calls["get_portfolio_metrics"] += 1
        return None

    async def get_recent_snapshots(self, deployment_id, limit=168):
        self.calls["get_recent_snapshots"] += 1
        return []

    async def get_ledger_entries(self, deployment_id, **kwargs):
        self.calls["get_ledger_entries"] += 1
        return []

    async def get_accounting_events_for_dashboard(self, deployment_id):
        self.calls["get_accounting_events_for_dashboard"] += 1
        return []

    async def get_latest_snapshot(self, deployment_id):
        return None


def _make_servicer(sm: _CountingStateManager) -> DashboardServiceServicer:
    svc = DashboardServiceServicer.__new__(DashboardServiceServicer)
    svc.settings = SimpleNamespace()
    svc._state_manager = sm
    svc._initialized = True
    svc._strategies_root = None
    svc._cached_positions = {}
    return svc


def _ctx() -> MagicMock:
    return MagicMock()


async def _fire_all_three(svc: DashboardServiceServicer) -> None:
    await svc.GetPnLSummary(gateway_pb2.GetPnLSummaryRequest(deployment_id=_DEPLOYMENT), _ctx())
    await svc.GetCostStack(gateway_pb2.GetCostStackRequest(deployment_id=_DEPLOYMENT), _ctx())
    await svc.GetAuditPosture(gateway_pb2.GetAuditPostureRequest(deployment_id=_DEPLOYMENT), _ctx())


@pytest.mark.asyncio
async def test_one_render_burst_loads_quant_inputs_once() -> None:
    """GetPnLSummary + GetCostStack + GetAuditPosture share ONE input load."""
    sm = _CountingStateManager()
    svc = _make_servicer(sm)

    await _fire_all_three(svc)

    assert sm.calls["get_ledger_entries"] == 1, (
        f"a single render burst must fetch the ledger once, got {sm.calls['get_ledger_entries']}"
    )
    assert sm.calls["get_accounting_events_for_dashboard"] == 1
    assert sm.calls["get_recent_snapshots"] == 1
    assert sm.calls["get_portfolio_metrics"] == 1


@pytest.mark.asyncio
async def test_concurrent_rpcs_single_flight() -> None:
    """Truly concurrent tile RPCs coalesce into one load (no thundering herd)."""
    sm = _CountingStateManager()
    svc = _make_servicer(sm)

    await asyncio.gather(
        svc.GetPnLSummary(gateway_pb2.GetPnLSummaryRequest(deployment_id=_DEPLOYMENT), _ctx()),
        svc.GetCostStack(gateway_pb2.GetCostStackRequest(deployment_id=_DEPLOYMENT), _ctx()),
        svc.GetAuditPosture(gateway_pb2.GetAuditPostureRequest(deployment_id=_DEPLOYMENT), _ctx()),
    )

    assert sm.calls["get_ledger_entries"] == 1


@pytest.mark.asyncio
async def test_distinct_deployments_do_not_share_cache_entries() -> None:
    sm = _CountingStateManager()
    svc = _make_servicer(sm)

    await svc.GetPnLSummary(gateway_pb2.GetPnLSummaryRequest(deployment_id="dep-aaa"), _ctx())
    await svc.GetPnLSummary(gateway_pb2.GetPnLSummaryRequest(deployment_id="dep-bbb"), _ctx())

    assert sm.calls["get_ledger_entries"] == 2


@pytest.mark.asyncio
async def test_zero_ttl_disables_caching() -> None:
    """TTL = 0 is the kill switch: every sequential RPC reloads fresh inputs."""
    sm = _CountingStateManager()
    svc = _make_servicer(sm)
    svc._quant_inputs_ttl_seconds = 0.0

    await svc.GetPnLSummary(gateway_pb2.GetPnLSummaryRequest(deployment_id=_DEPLOYMENT), _ctx())
    await svc.GetPnLSummary(gateway_pb2.GetPnLSummaryRequest(deployment_id=_DEPLOYMENT), _ctx())

    assert sm.calls["get_ledger_entries"] == 2


@pytest.mark.asyncio
async def test_positive_ttl_expires_and_reloads() -> None:
    """A positive TTL is a STALENESS BOUND, not a forever-cache.

    Phase 1 spec critique (Codex): a cache that never expires when TTL > 0 but
    disables at TTL = 0 would pass every other test here while serving stale
    tiles indefinitely. Pin the expiry: within the TTL the entry is served
    from cache; after the TTL elapses the next RPC reloads fresh inputs.
    """
    sm = _CountingStateManager()
    svc = _make_servicer(sm)
    svc._quant_inputs_ttl_seconds = 0.05

    await svc.GetPnLSummary(gateway_pb2.GetPnLSummaryRequest(deployment_id=_DEPLOYMENT), _ctx())
    await svc.GetPnLSummary(gateway_pb2.GetPnLSummaryRequest(deployment_id=_DEPLOYMENT), _ctx())
    assert sm.calls["get_ledger_entries"] == 1, "within the TTL the burst shares one load"

    # Generous margin over the 0.05 TTL — a tight 0.01 gap flakes on loaded
    # CI runners (CodeRabbit review note on PR #2731).
    await asyncio.sleep(0.2)

    await svc.GetPnLSummary(gateway_pb2.GetPnLSummaryRequest(deployment_id=_DEPLOYMENT), _ctx())
    assert sm.calls["get_ledger_entries"] == 2, "after the TTL elapses the cache must reload"
