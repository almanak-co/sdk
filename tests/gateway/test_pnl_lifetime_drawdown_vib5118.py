"""Gateway-boundary coverage for the VIB-5118 lifetime-drawdown PnL path.

Per the `almanak/gateway/**` coding guideline (gateway is the security boundary),
this drives the `GetPnLSummary` gRPC handler end-to-end over a real SQLite backend
and pins three contracts the unit suite under `tests/unit/dashboard/` cannot:

1. `GetPnLSummary` reports the **lifetime** drawdown on the wire (over a full
   history whose peak/crash predate the recent 168-row window, where the windowed
   path would report 0).
2. The expensive full-history `get_nav_series` scan fires **only** on the PnL
   path — `GetCostStack` (which shares the quant-input load and surfaces no
   drawdown) never triggers it.
3. A backend read failure **degrades gracefully** to the windowed drawdown and
   does **not** leak the backend error detail into the gRPC response.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import grpc
import pytest
import pytest_asyncio

from almanak.framework.portfolio.models import PortfolioSnapshot, ValueConfidence
from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore
from almanak.framework.state.state_manager import StateManager
from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.dashboard_service import DashboardServiceServicer

_DEP = "deployment:vib5118gw"
_BASE_TS = datetime(2026, 6, 1, 0, 0, 0, tzinfo=UTC)
_TOTAL_COUNT = 250  # > 168, so the recent window misses the early peak/crash


def _total_for(i: int) -> Decimal:
    # snap 0 = $100 peak; snaps 1..81 = $60 trough (40% DD); snaps 82..249 = flat $95.
    if i == 0:
        return Decimal("100")
    if i < 82:
        return Decimal("60")
    return Decimal("95")


def _snap(i: int) -> PortfolioSnapshot:
    total = _total_for(i)
    return PortfolioSnapshot(
        timestamp=_BASE_TS + timedelta(minutes=i),
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
    s = SQLiteStore(SQLiteConfig(db_path=str(tmp_path / "vib5118gw.db")))
    await s.initialize()
    for i in range(_TOTAL_COUNT):
        await s.save_portfolio_snapshot(_snap(i))
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


@pytest.mark.asyncio
async def test_get_pnl_summary_reports_lifetime_drawdown(store: SQLiteStore) -> None:
    svc = _servicer(store)
    resp = await svc.GetPnLSummary(gateway_pb2.GetPnLSummaryRequest(deployment_id=_DEP), _ctx())

    # Lifetime over the WHOLE history ($100 peak → $60 trough → $95 last), not the
    # flat-$95 recent window that would have reported 0.00.
    assert resp.max_drawdown_pct == "40.00"
    assert resp.current_drawdown_pct == "5.00"


@pytest.mark.asyncio
async def test_only_pnl_path_scans_nav_series(store: SQLiteStore) -> None:
    svc = _servicer(store)

    calls: list[str] = []
    real = svc._state_manager.get_nav_series

    async def _spy(*a: object, **k: object) -> object:
        calls.append("get_nav_series")
        return await real(*a, **k)

    svc._state_manager.get_nav_series = _spy  # type: ignore[method-assign]

    # The cost-stack surface shares the quant-input load but surfaces no drawdown —
    # it must NOT pay for the full-history scan.
    await svc.GetCostStack(gateway_pb2.GetCostStackRequest(deployment_id=_DEP), _ctx())
    assert calls == [], "GetCostStack must not scan the full NAV series"

    # The PnL surface is the sole caller.
    await svc.GetPnLSummary(gateway_pb2.GetPnLSummaryRequest(deployment_id=_DEP), _ctx())
    assert calls == ["get_nav_series"], "GetPnLSummary is the sole trigger of the NAV scan"


@pytest.mark.asyncio
async def test_pnl_backend_failure_degrades_without_leaking(store: SQLiteStore) -> None:
    svc = _servicer(store)
    secret = "db-down-internal-dsn-secret"
    svc._state_manager.get_nav_series = AsyncMock(side_effect=RuntimeError(secret))

    ctx = _ctx()
    resp = await svc.GetPnLSummary(gateway_pb2.GetPnLSummaryRequest(deployment_id=_DEP), ctx)

    # Degrades to the recent-window drawdown (flat $95 → 0.00) instead of raising…
    assert resp.max_drawdown_pct == "0.00"
    # …the handler does not set an error status…
    ctx.set_code.assert_not_called()
    # …and the backend error detail never leaks into any response field.
    assert secret not in str(resp)
