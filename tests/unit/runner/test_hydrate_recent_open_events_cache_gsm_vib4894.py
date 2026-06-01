"""VIB-4894 — hosted GatewayStateManager bulk-hydrate for the recent-open cache.

Pre-fix, ``hydrate_recent_open_events_cache`` early-returned ``0`` whenever the
runner's state manager lacked ``get_position_events_sync`` — which is exactly
the hosted ``GatewayStateManager`` shape (GSM deliberately does not expose the
local-SQLite sync wedge). The result: every hosted runner booted with an empty
``_recent_open_events`` cache, so a teardown loop over N open positions did N
separate ``get_position_history`` gRPC round-trips (via the VIB-4839 per-CLOSE
durable fallback) instead of one bulk pre-warm at boot.

The fix branches on the available *async* surface:

* ``get_position_events_for_dashboard`` — local StateManager warm-tier getter.
* ``get_position_events_filtered`` — present on BOTH StateManager and GSM.

Both reduce through the SAME ``_collect_open_positions`` OPEN/CLOSE fold, so
there is no per-backend reducer divergence (the VIB-4839 ``upper()`` vs
``lower()`` anti-pattern).

These tests are honest about the pre-fix failure mode: the GSM-shaped fakes
expose ONLY ``get_position_events_filtered`` (no sync getter, no dashboard
getter), so the pre-fix early-return leaves the cache ``{}`` and
``test_hosted_gsm_hydrates_cache_via_filtered`` FAILS without the fix. None of
the asserted cache values are monkey-patched in — they flow through the real
reducer from the seeded rows.
"""

from __future__ import annotations

import logging
from types import SimpleNamespace

import pytest

from almanak.framework.observability.position_events import PositionType
from almanak.framework.runner._run_loop_helpers import (
    _hydration_position_types,
    hydrate_recent_open_events_cache,
)


def _lp_open_row(position_id: str = "5500290") -> dict:
    """A position_events OPEN row carrying the full immutable LP bracket."""
    return {
        "position_id": position_id,
        "position_type": "LP",
        "event_type": "OPEN",
        "value_usd": "3.965",
        "ledger_entry_id": "led-open-1",
        "timestamp": "2026-05-22T00:00:00Z",
        "tick_lower": -200490,
        "tick_upper": -199490,
        "liquidity": "123456789",
        "token0": "WETH",
        "token1": "USDC",
    }


def _lp_close_row(position_id: str = "5500290") -> dict:
    return {
        "position_id": position_id,
        "position_type": "LP",
        "event_type": "CLOSE",
        "value_usd": "",
        "ledger_entry_id": "led-close-1",
        "timestamp": "2026-05-26T00:00:00Z",
        "tick_lower": None,
        "tick_upper": None,
        "liquidity": "",
        "token0": "",
        "token1": "",
    }


def _lending_open_row() -> dict:
    return {
        "position_id": "lending:arb:aave_v3:0xabc:usdc",
        "position_type": "LENDING_COLLATERAL",
        "event_type": "OPEN",
        "value_usd": "100",
        "ledger_entry_id": "led-lend-1",
        "timestamp": "2026-05-22T00:00:00Z",
        "tick_lower": None,
        "tick_upper": None,
        "liquidity": None,
        "token0": "",
        "token1": "",
    }


class _GsmShapedStateManager:
    """A GatewayStateManager-shaped fake.

    Exposes ONLY the async ``get_position_events_filtered`` surface — no
    ``get_position_events_sync`` (the local-SQLite wedge GSM omits) and no
    ``get_position_events_for_dashboard`` (warm-tier wrapper, also absent on
    GSM). This is the exact shape the pre-fix early-return tripped on.
    """

    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows
        self.filtered_calls: list[tuple[str, frozenset[str]]] = []

    async def get_position_events_filtered(
        self,
        *,
        deployment_id: str,
        position_types: frozenset[str],
    ) -> list[dict]:
        self.filtered_calls.append((deployment_id, position_types))
        # Mirror the real backend: filter by position_type, ASC order.
        return [r for r in self._rows if r.get("position_type") in position_types]


class _GsmRaisingStateManager:
    """GSM-shaped fake whose filtered getter raises (gRPC error)."""

    async def get_position_events_filtered(
        self,
        *,
        deployment_id: str,
        position_types: frozenset[str],
    ) -> list[dict]:
        raise RuntimeError("gateway unreachable")


class _LocalDashboardStateManager:
    """Local StateManager-shaped fake exposing the warm-tier dashboard getter."""

    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows
        self.dashboard_calls: list[str] = []

    async def get_position_events_for_dashboard(
        self,
        deployment_id: str,
        position_id: str | None = None,
        position_type: str | None = None,
        event_type: str | None = None,
    ) -> list[dict]:
        self.dashboard_calls.append(deployment_id)
        return list(self._rows)


def _make_runner(state_manager: object) -> SimpleNamespace:
    return SimpleNamespace(state_manager=state_manager, _recent_open_events={})


def _make_strategy(deployment_id: str = "AccountingQuantLPStrategy:0f6cfd82") -> SimpleNamespace:
    return SimpleNamespace(deployment_id=deployment_id)


def test_hydration_position_types_covers_full_observability_enum():
    """The filter set is derived from the canonical observability enum so a
    future PositionType addition is picked up automatically."""
    assert _hydration_position_types() == frozenset(pt.value for pt in PositionType)
    assert {"LP", "PERP", "LENDING_COLLATERAL", "LENDING_DEBT"} <= _hydration_position_types()


@pytest.mark.asyncio
async def test_hosted_gsm_hydrates_cache_via_filtered():
    """Canonical VIB-4894 bug scenario: GSM-shaped manager with a seeded LP
    OPEN. Pre-fix this FAILS (early-return on missing sync getter → cache {}).
    Post-fix the cache is non-empty and carries the full immutable bracket."""
    sm = _GsmShapedStateManager([_lp_open_row()])
    runner = _make_runner(sm)
    strategy = _make_strategy()

    populated = await hydrate_recent_open_events_cache(runner, strategy)

    assert populated == 1
    key = ("5500290", "LP")
    assert key in runner._recent_open_events, "GSM bulk-hydrate must populate the cache"
    cached = runner._recent_open_events[key]
    # Bracket fields flow through the real reducer — NOT monkey-patched.
    assert cached["token0"] == "WETH"
    assert cached["token1"] == "USDC"
    assert cached["tick_lower"] == -200490
    assert cached["tick_upper"] == -199490
    assert cached["liquidity"] == "123456789"
    assert cached["value_usd"] == "3.965"
    # The filtered getter was called with the full position-type vocabulary.
    assert sm.filtered_calls == [(strategy.deployment_id, _hydration_position_types())]


@pytest.mark.asyncio
async def test_local_state_manager_hydrates_via_dashboard_getter():
    """Regression guard: the local warm-tier path (dashboard getter present)
    must still populate the cache. The dashboard getter is preferred over the
    filtered getter when both are available."""
    sm = _LocalDashboardStateManager([_lp_open_row("5471740")])
    runner = _make_runner(sm)
    strategy = _make_strategy()

    populated = await hydrate_recent_open_events_cache(runner, strategy)

    assert populated == 1
    key = ("5471740", "LP")
    assert key in runner._recent_open_events
    assert runner._recent_open_events[key]["token0"] == "WETH"
    assert sm.dashboard_calls == [strategy.deployment_id]


@pytest.mark.asyncio
async def test_dashboard_getter_preferred_over_filtered():
    """When a backend exposes BOTH getters (StateManager does), the dashboard
    getter wins and the filtered getter is never consulted."""

    class _BothGetters(_LocalDashboardStateManager):
        def __init__(self, rows: list[dict]) -> None:
            super().__init__(rows)
            self.filtered_calls: list[object] = []

        async def get_position_events_filtered(self, **kwargs):  # noqa: ANN003
            self.filtered_calls.append(kwargs)
            return []

    sm = _BothGetters([_lp_open_row()])
    runner = _make_runner(sm)

    populated = await hydrate_recent_open_events_cache(runner, _make_strategy())

    assert populated == 1
    assert sm.dashboard_calls, "dashboard getter must be used"
    assert sm.filtered_calls == [], "filtered getter must NOT be consulted when dashboard exists"


@pytest.mark.asyncio
async def test_gsm_open_then_close_leaves_cache_empty():
    """OPEN followed by CLOSE for the same position → key ABSENT after
    hydration (the OPEN/CLOSE fold runs identically on the GSM path)."""
    sm = _GsmShapedStateManager([_lp_open_row(), _lp_close_row()])
    runner = _make_runner(sm)

    populated = await hydrate_recent_open_events_cache(runner, _make_strategy())

    assert populated == 0
    assert ("5500290", "LP") not in runner._recent_open_events


@pytest.mark.asyncio
async def test_gsm_hydrates_lending_open_not_just_lp():
    """The filter set covers all position-event types — a lending OPEN
    hydrates too, not only LP (criterion 3)."""
    sm = _GsmShapedStateManager([_lp_open_row(), _lending_open_row()])
    runner = _make_runner(sm)

    populated = await hydrate_recent_open_events_cache(runner, _make_strategy())

    assert populated == 2
    assert ("5500290", "LP") in runner._recent_open_events
    assert ("lending:arb:aave_v3:0xabc:usdc", "LENDING_COLLATERAL") in runner._recent_open_events


@pytest.mark.asyncio
async def test_gsm_empty_history_is_noop():
    """Fresh deployment (no rows) → cache empty, returns 0, no raise."""
    sm = _GsmShapedStateManager([])
    runner = _make_runner(sm)

    populated = await hydrate_recent_open_events_cache(runner, _make_strategy())

    assert populated == 0
    assert runner._recent_open_events == {}


@pytest.mark.asyncio
async def test_gsm_getter_raises_is_noop():
    """A raising getter is caught, returns 0, leaves the cache empty — boot
    is never blocked by a transient gateway error (criterion 5)."""
    sm = _GsmRaisingStateManager()
    runner = _make_runner(sm)

    populated = await hydrate_recent_open_events_cache(runner, _make_strategy())

    assert populated == 0
    assert runner._recent_open_events == {}


@pytest.mark.asyncio
async def test_no_state_manager_is_noop():
    """Persistence disabled / init failed → state_manager is None → no-op."""
    runner = _make_runner(None)
    populated = await hydrate_recent_open_events_cache(runner, _make_strategy())
    assert populated == 0


@pytest.mark.asyncio
async def test_empty_deployment_id_is_noop():
    """Empty deployment_id short-circuits before any backend read."""
    sm = _GsmShapedStateManager([_lp_open_row()])
    runner = _make_runner(sm)
    populated = await hydrate_recent_open_events_cache(runner, _make_strategy(deployment_id=""))
    assert populated == 0
    assert sm.filtered_calls == []


@pytest.mark.asyncio
async def test_backend_without_any_getter_warns_and_noops(caplog):
    """A backend exposing NEITHER getter returns 0 (we never grow the sync
    wedge onto GSM); the gap is surfaced via WARN, not silently succeeded."""
    sm = SimpleNamespace()  # no getters at all
    runner = _make_runner(sm)
    with caplog.at_level(logging.WARNING, logger="almanak.framework.runner.strategy_runner"):
        populated = await hydrate_recent_open_events_cache(runner, _make_strategy())
    assert populated == 0
    # The no-surface gap MUST be loud, not a silent no-op: if the WARN is
    # removed and the helper silently returns 0, this assertion fails.
    assert any("exposes no" in r.getMessage() and "skipping bulk pre-warm" in r.getMessage() for r in caplog.records), (
        f"expected a WARN surfacing the missing async read surface; got {[r.getMessage() for r in caplog.records]}"
    )
