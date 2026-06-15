"""Gateway-boundary coverage for the VIB-5134 lifetime-drawdown checkpoint.

Per the ``almanak/gateway/**`` coding guideline, the new checkpoint helpers
(``_get_lifetime_drawdown`` / ``_full_scan_lifetime_drawdown`` /
``_fold_lifetime_drawdown``, the TTL-gated full scan vs. incremental fold, the
abandoned-entry pruning, and the failure-preservation path) are exercised here
through the ``GetPnLSummary`` gRPC handler — the service boundary — over a real
SQLite backend, not only via the unit suite under ``tests/unit/dashboard/``.

Pins:
1. TTL expiry re-triggers the expensive full-history scan; within the TTL the
   render advances via the cheap incremental ``since`` fold instead.
2. A new high-water-mark-then-fall *between* full scans is reflected in the
   wire ``current_drawdown_pct`` on the next render (the Codex finding-1 path,
   end-to-end through gRPC).
3. A transient backend failure preserves the last-known-good lifetime on the
   wire rather than blanking the tile to the recent-window value.
4. Abandoned per-deployment checkpoints are pruned while the active one survives.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import grpc
import pytest
import pytest_asyncio

from almanak.framework.dashboard.quant_aggregations import DrawdownState
from almanak.framework.portfolio.models import PortfolioSnapshot, ValueConfidence
from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore
from almanak.framework.state.state_manager import StateManager
from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.dashboard_service import (
    DashboardServiceServicer,
    _LifetimeDrawdownCheckpoint,
)

_DEP = "deployment:vib5134gw"
_BASE_TS = datetime(2026, 6, 1, 0, 0, 0, tzinfo=UTC)


def _snap(i: int, total: Decimal, ts: datetime) -> PortfolioSnapshot:
    return PortfolioSnapshot(
        timestamp=ts,
        deployment_id=_DEP,
        total_value_usd=total,
        available_cash_usd=Decimal("0"),
        deployed_capital_usd=total,
        wallet_total_value_usd=total,
        value_confidence=ValueConfidence.HIGH,
        chain="arbitrum",
        iteration_number=i,
        cycle_id=f"iter-{i}",
    )


@pytest_asyncio.fixture
async def store(tmp_path: Path) -> SQLiteStore:
    s = SQLiteStore(SQLiteConfig(db_path=str(tmp_path / "vib5134gw.db")))
    await s.initialize()
    yield s
    await s.close()


def _servicer(store: SQLiteStore) -> DashboardServiceServicer:
    sm = StateManager.__new__(StateManager)
    sm._initialized = True
    sm._warm = store
    sm._record_metrics = MagicMock()

    svc = DashboardServiceServicer(GatewaySettings())
    svc._state_manager = sm
    svc._initialized = True  # short-circuit _ensure_initialized
    return svc


def _ctx() -> MagicMock:
    return MagicMock(spec=grpc.aio.ServicerContext)


async def _pnl(svc: DashboardServiceServicer) -> gateway_pb2.PnLSummary:
    return await svc.GetPnLSummary(gateway_pb2.GetPnLSummaryRequest(deployment_id=_DEP), _ctx())


def _full_scan_spy(svc: DashboardServiceServicer) -> dict[str, int]:
    counts = {"full": 0, "incremental": 0}
    real = svc._state_manager.get_nav_series

    async def _spy(*a, **k):
        if k.get("since") is None:
            counts["full"] += 1
        else:
            counts["incremental"] += 1
        return await real(*a, **k)

    svc._state_manager.get_nav_series = _spy  # type: ignore[method-assign]
    return counts


@pytest.mark.asyncio
async def test_ttl_expiry_retriggers_full_scan_else_incremental(store: SQLiteStore) -> None:
    await store.save_portfolio_snapshot(_snap(0, Decimal("100"), _BASE_TS))
    await store.save_portfolio_snapshot(_snap(1, Decimal("80"), _BASE_TS + timedelta(minutes=1)))

    # TTL=0 → every render re-runs the expensive full-history scan.
    svc = _servicer(store)
    svc._lifetime_dd_ttl_seconds = 0.0
    counts = _full_scan_spy(svc)
    await _pnl(svc)
    await _pnl(svc)
    assert counts["full"] == 2, "an elapsed full-scan TTL re-triggers the full scan"

    # Large TTL → one full scan, then the cheap incremental fold between renders.
    svc2 = _servicer(store)
    svc2._lifetime_dd_ttl_seconds = 1000.0
    counts2 = _full_scan_spy(svc2)
    await _pnl(svc2)
    await _pnl(svc2)
    assert counts2["full"] == 1, "within the TTL the full scan fires once"
    assert counts2["incremental"] == 1, "subsequent renders advance via the since fold"


@pytest.mark.asyncio
async def test_new_high_then_fall_is_live_on_the_wire(store: SQLiteStore) -> None:
    await store.save_portfolio_snapshot(_snap(0, Decimal("100"), _BASE_TS))
    await store.save_portfolio_snapshot(_snap(1, Decimal("100"), _BASE_TS + timedelta(minutes=1)))

    svc = _servicer(store)
    svc._lifetime_dd_ttl_seconds = 1000.0  # keep the 2nd render inside the full-scan TTL
    counts = _full_scan_spy(svc)

    seeded = await _pnl(svc)
    assert seeded.current_drawdown_pct == "0.00"

    # New high ($120) then fall ($90) AFTER the cached full scan.
    await store.save_portfolio_snapshot(_snap(2, Decimal("120"), _BASE_TS + timedelta(minutes=2)))
    await store.save_portfolio_snapshot(_snap(3, Decimal("90"), _BASE_TS + timedelta(minutes=3)))

    live = await _pnl(svc)
    # (120-90)/120 = 25% — the incremental fold raised the peak before computing the
    # drawdown; a plain "latest vs cached peak($100)" would have shown only 10%.
    assert live.current_drawdown_pct == "25.00"
    assert counts["full"] == 1, "the new high was captured without a second full scan"


@pytest.mark.asyncio
async def test_transient_failure_preserves_last_known_good_on_the_wire(store: SQLiteStore) -> None:
    await store.save_portfolio_snapshot(_snap(0, Decimal("100"), _BASE_TS))
    await store.save_portfolio_snapshot(_snap(1, Decimal("70"), _BASE_TS + timedelta(minutes=1)))

    svc = _servicer(store)
    svc._lifetime_dd_ttl_seconds = 0.0  # force the full-scan branch each render

    good = await _pnl(svc)
    assert good.max_drawdown_pct == "30.00"  # (100-70)/100

    # Backend now fails — the lifetime tile must keep the last-known-good value, not
    # blank to the recent-window, and the handler must not error.
    svc._state_manager.get_nav_series = AsyncMock(side_effect=RuntimeError("transient blip"))
    ctx = _ctx()
    resp = await svc.GetPnLSummary(gateway_pb2.GetPnLSummaryRequest(deployment_id=_DEP), ctx)
    assert resp.max_drawdown_pct == "30.00"
    ctx.set_code.assert_not_called()


@pytest.mark.asyncio
async def test_abandoned_checkpoints_are_pruned(store: SQLiteStore) -> None:
    await store.save_portfolio_snapshot(_snap(0, Decimal("100"), _BASE_TS))
    await store.save_portfolio_snapshot(_snap(1, Decimal("90"), _BASE_TS + timedelta(minutes=1)))

    svc = _servicer(store)
    # An abandoned deployment's checkpoint, last scanned well beyond the TTL.
    svc._lifetime_dd_ckpt["deployment:ghost"] = _LifetimeDrawdownCheckpoint(
        state=DrawdownState(running_peak=Decimal("1")),
        cursor=None,
        full_scan_at=time.monotonic() - 10_000.0,
        truncated=False,
    )

    await _pnl(svc)  # a render for _DEP triggers the prune

    assert "deployment:ghost" not in svc._lifetime_dd_ckpt, "abandoned entry pruned past the TTL"
    assert _DEP in svc._lifetime_dd_ckpt, "the active deployment's checkpoint survives"
