"""Gateway unit coverage for the windowed-NAV truncation metric (VIB-5059 P2).

The integration suite exercises the full store→gateway path; this focused
gateway unit test pins the metric contract directly: the
``dashboard_nav_history_truncated_total`` counter increments (and a WARNING is
logged) exactly when the windowed read is truncated, and not otherwise. Per the
repo guideline, ``almanak/gateway/**`` changes carry a ``tests/gateway/`` test.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.metrics import DASHBOARD_NAV_HISTORY_TRUNCATED
from almanak.gateway.services.dashboard_service import DashboardServiceServicer

_DEPLOYMENT_ID = "WindowTruncMetricStrategy:abc123"


def _rows(n: int) -> list[tuple[datetime, str, str]]:
    base = datetime(2026, 1, 1, tzinfo=UTC)
    return [(base + timedelta(minutes=5 * i), str(Decimal(1000 + i)), "HIGH") for i in range(n)]


def _servicer_with_window(rows: list[tuple[datetime, str, str]], *, truncated: bool) -> DashboardServiceServicer:
    svc = DashboardServiceServicer(GatewaySettings())
    sm = AsyncMock()
    sm.get_snapshots_in_window = AsyncMock(return_value=(rows, truncated))
    sm.get_portfolio_metrics = AsyncMock(return_value=None)
    svc._state_manager = sm
    return svc


@pytest.mark.asyncio
async def test_truncation_metric_increments_and_warns_when_truncated(caplog) -> None:
    svc = _servicer_with_window(_rows(5), truncated=True)
    metric = DASHBOARD_NAV_HISTORY_TRUNCATED.labels(deployment_id=_DEPLOYMENT_ID)
    before = metric._value.get()

    with caplog.at_level(logging.WARNING):
        out = await svc._build_pnl_history(_DEPLOYMENT_ID, from_dt=None, to_dt=None, max_points=1500)

    assert metric._value.get() == before + 1, "truncation must increment the labeled metric"
    assert any("truncat" in r.getMessage().lower() for r in caplog.records), "operator-visible WARNING required"
    assert len(out) >= 1, "a non-empty truncated window still returns a (decimated) series"


@pytest.mark.asyncio
async def test_truncation_metric_unchanged_when_not_truncated() -> None:
    svc = _servicer_with_window(_rows(5), truncated=False)
    metric = DASHBOARD_NAV_HISTORY_TRUNCATED.labels(deployment_id=_DEPLOYMENT_ID)
    before = metric._value.get()

    await svc._build_pnl_history(_DEPLOYMENT_ID, from_dt=None, to_dt=None, max_points=1500)

    assert metric._value.get() == before, "a complete (non-truncated) window must not increment the metric"
