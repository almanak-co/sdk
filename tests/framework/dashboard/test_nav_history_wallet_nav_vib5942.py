"""VIB-5942 — NAV-history chart plots WALLET NAV (total − debt + cash), not
position value alone, and honours Empty≠Zero both directions.

Root cause fixed here: the gateway PnL-history builders computed net position
equity (``total_value_usd − debt``) and dropped ``available_cash_usd``. On a
post-close snapshot the position value goes to 0 while the funds sit in the
wallet as cash, so the series read 0 → the ``_fetch_windowed_nav`` filter dropped
it (its own Empty≠Zero bug: a MEASURED zero must plot) → a 2-point series
collapsed to 1 point and Plotly auto-padded a degenerate ~2ms x-axis.

Two layers are pinned:

* **Gateway builder** (``_build_pnl_history_windowed`` / ``_recent``): reproduce
  the exact avax/arb mainnet shape (open snapshot with a live perp + cash, then a
  post-close snapshot with position value 0 + returned cash) on a real SQLite
  store and assert the series is 2 points, both ≈ wallet NAV, spanning the real
  minutes — not a collapsed single point.
* **Client filter** (``sections._fetch_windowed_nav``): a measured zero plots; an
  unmeasured (None) sample is skipped, never fabricated as $0.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest
import pytest_asyncio

from almanak.framework.dashboard import sections
from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore
from almanak.framework.state.state_manager import StateManager, StateManagerConfig
from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.services.dashboard_service import DashboardServiceServicer

_DEPLOYMENT_ID = "deployment:vib5942test"


def _perp_position(value_usd: str) -> dict:
    """A GMX-style PERP position (no BORROW leg → nets Decimal("0") debt)."""
    return {
        "position_type": "PERP",
        "protocol": "gmx_v2",
        "chain": "avalanche",
        "value_usd": value_usd,
        "cost_basis_usd": value_usd,
        "label": "gmx_v2 PERP",
        "tokens": [],
        "details": {"is_long": True, "leverage": "2.0"},
    }


async def _seed(store: SQLiteStore, *, ts: datetime, total: str, cash: str, positions: list[dict]) -> None:
    from almanak.framework.portfolio.models import PortfolioSnapshot

    snap = PortfolioSnapshot.from_dict(
        {
            "timestamp": ts.isoformat(),
            "deployment_id": _DEPLOYMENT_ID,
            "total_value_usd": total,
            "available_cash_usd": cash,
            "value_confidence": "HIGH",
            "positions": positions,
        }
    )
    await store.save_portfolio_snapshot(snap)


@pytest_asyncio.fixture
async def store_with_open_then_close(tmp_path):
    """Two snapshots mirroring a real GMX perp round-trip:

    * t0 — position open: position value 8.00, idle cash 13.41  → wallet NAV 21.41
    * t1 — post-close:    position value 0.00, cash 21.11         → wallet NAV 21.11

    The pre-VIB-5942 builder plotted [8.00, 0.00] (position only) → the 0 point was
    dropped and the chart collapsed. The fix plots [21.41, 21.11].
    """
    store = SQLiteStore(SQLiteConfig(db_path=str(tmp_path / "vib5942.sqlite")))
    await store.initialize()
    await _seed(
        store,
        ts=datetime(2026, 7, 21, 4, 12, 12, tzinfo=UTC),
        total="8.00",
        cash="13.41",
        positions=[_perp_position("8.00")],
    )
    await _seed(
        store,
        ts=datetime(2026, 7, 21, 4, 18, 54, tzinfo=UTC),
        total="0",
        cash="21.11",
        positions=[],  # position closed
    )
    return store


def _servicer(store: SQLiteStore) -> DashboardServiceServicer:
    svc = DashboardServiceServicer(GatewaySettings())
    svc._state_manager = StateManager(StateManagerConfig(), warm_backend=store)
    return svc


@pytest.mark.asyncio
async def test_windowed_series_is_wallet_nav_not_collapsed(store_with_open_then_close) -> None:
    svc = _servicer(store_with_open_then_close)
    points = await svc._build_pnl_history_windowed(_DEPLOYMENT_ID, None, None, 1500)

    # BOTH snapshots survive as MEASURED wallet-NAV points — not a collapsed single
    # point (the pre-fix bug dropped the post-close 0 and left one point).
    assert len(points) == 2, [(p.timestamp, p.value_usd) for p in points]

    values = [Decimal(p.value_usd) for p in points]
    assert values[0] == Decimal("21.41"), "open snapshot = 8.00 position + 13.41 cash"
    assert values[1] == Decimal("21.11"), "post-close = 0 position + 21.11 cash (NOT 0)"

    # The x-span is the real 6m42s between snapshots (int epoch seconds), not a
    # ~2ms degenerate axis. 04:18:54 − 04:12:12 = 402s.
    span_s = points[-1].timestamp - points[0].timestamp
    assert span_s == 402, f"expected 402s real span, got {span_s}s"


@pytest.mark.asyncio
async def test_recent_series_is_wallet_nav(store_with_open_then_close) -> None:
    svc = _servicer(store_with_open_then_close)
    points = await svc._build_pnl_history_recent(_DEPLOYMENT_ID)

    assert len(points) == 2
    assert {Decimal(p.value_usd) for p in points} == {Decimal("21.41"), Decimal("21.11")}


# ---------------------------------------------------------------------------
# Client-side filter: Empty≠Zero BOTH directions (guardrail #2).
# ---------------------------------------------------------------------------


def _details_with(pnl_history: list[dict]) -> MagicMock:
    details = MagicMock()
    details.pnl_history = pnl_history
    return details


def _run_fetch(pnl_history: list[dict]) -> list[dict]:
    client = MagicMock()
    client.get_strategy_details.return_value = _details_with(pnl_history)
    fetch = getattr(sections._fetch_windowed_nav, "__wrapped__", sections._fetch_windowed_nav)
    with patch("almanak.framework.dashboard.data_source.get_dashboard_client", return_value=client):
        return fetch("deployment:x", "All")


def test_measured_zero_nav_plots() -> None:
    """A MEASURED zero NAV (Decimal("0")) is a real data point and MUST plot —
    dropping it was the bug that collapsed the post-close chart."""
    ts = datetime(2026, 7, 21, 4, 18, 54, tzinfo=UTC)
    points = _run_fetch([{"timestamp": ts, "value_usd": Decimal("0"), "pnl_usd": Decimal("-5")}])
    assert len(points) == 1
    assert points[0]["value"] == 0.0


def test_unmeasured_nav_is_skipped_not_fabricated() -> None:
    """An UNMEASURED sample (value_usd None) is skipped — never plotted as $0."""
    ts = datetime(2026, 7, 21, 4, 18, 54, tzinfo=UTC)
    points = _run_fetch([{"timestamp": ts, "value_usd": None, "pnl_usd": None}])
    assert points == []


def test_mixed_measured_and_unmeasured() -> None:
    """A measured non-zero, a measured zero, and an unmeasured sample: the first two
    plot, the last is dropped."""
    t0 = datetime(2026, 7, 21, 4, 12, 12, tzinfo=UTC)
    t1 = datetime(2026, 7, 21, 4, 15, 0, tzinfo=UTC)
    t2 = datetime(2026, 7, 21, 4, 18, 54, tzinfo=UTC)
    points = _run_fetch(
        [
            {"timestamp": t0, "value_usd": Decimal("21.41"), "pnl_usd": Decimal("0")},
            {"timestamp": t1, "value_usd": None, "pnl_usd": None},
            {"timestamp": t2, "value_usd": Decimal("0"), "pnl_usd": Decimal("-21.41")},
        ]
    )
    assert [p["value"] for p in points] == [21.41, 0.0]


def test_measured_value_with_unmeasured_pnl_gaps_pnl_not_zero() -> None:
    """VIB-5942 CodeRabbit #1: a point whose NAV is measured but pnl is UNMEASURED
    (None) plots the value but leaves pnl as None (a chart GAP) — never a fabricated
    float(0.0) that plants a measured $0 on the PnL tab."""
    ts = datetime(2026, 7, 21, 4, 12, 12, tzinfo=UTC)
    points = _run_fetch([{"timestamp": ts, "value_usd": Decimal("21.41"), "pnl_usd": None}])
    assert len(points) == 1
    assert points[0]["value"] == 21.41
    assert points[0]["pnl"] is None  # gap, NOT 0.0


def test_measured_pnl_plots_including_measured_zero() -> None:
    """A measured pnl (incl. a measured Decimal('0')) plots as its float value."""
    ts = datetime(2026, 7, 21, 4, 12, 12, tzinfo=UTC)
    pts = _run_fetch([{"timestamp": ts, "value_usd": Decimal("21.41"), "pnl_usd": Decimal("0")}])
    assert pts[0]["pnl"] == 0.0  # measured zero — a real point, not a gap
