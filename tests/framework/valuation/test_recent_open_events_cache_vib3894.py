"""VIB-3894 — recent-open cache enriches cost_basis_usd without sync RPC.

Pre-fix: ``PortfolioValuer._enrich_from_open_event`` early-returned when
the state_manager lacked ``get_position_events_sync``. The production
gateway-backed state manager doesn't expose that method, so for the
same-iteration snapshot fired right after LP_OPEN, ``cost_basis_usd``
stayed 0 and ``deployed_capital_usd`` therefore also 0 — even though the
position was physically on disk in ``position_events``.

The fix introduces a runner-side ``_recent_open_events`` cache populated
when ``save_position_event`` succeeds. The valuer reads from this cache
first, falling through to the disk path when it's missing or the cache
doesn't carry the event.
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from almanak.framework.teardown.models import PositionInfo, PositionType
from almanak.framework.valuation.portfolio_valuer import PortfolioValuer


def _make_position_info(position_id: str = "5464283") -> PositionInfo:
    return PositionInfo(
        position_type=PositionType.LP,
        position_id=position_id,
        chain="arbitrum",
        protocol="uniswap_v3",
        value_usd=Decimal("0"),
        details={},
    )


def _make_position_value(value_usd: Decimal = Decimal("5.84")) -> SimpleNamespace:
    """Mock-shape replacement for PositionValue (only the fields the helper writes)."""
    return SimpleNamespace(
        cost_basis_usd=Decimal("0"),
        unrealized_pnl_usd=Decimal("0"),
        entry_timestamp="",
        ledger_entry_id="",
        value_usd=value_usd,
    )


@pytest.fixture
def valuer():
    """A PortfolioValuer instance with a stub state_manager that has NO
    ``get_position_events_sync`` (matches the production gateway shape)."""
    v = PortfolioValuer.__new__(PortfolioValuer)
    v._accounting_store = MagicMock(spec=[])  # no get_position_events_sync
    v._deployment_id = "AccountingQuantLPStrategy:0f6cfd82b9fd"
    v._snapshot_event_cache = None
    return v


def test_cache_hit_populates_cost_basis_when_state_manager_lacks_sync_getter(valuer):
    """Canonical bug scenario: gateway state_manager has no
    ``get_position_events_sync``. The cache lookup must succeed and
    populate cost_basis_usd from the in-memory entry."""
    valuer._recent_open_events = {
        ("5464283", "LP"): {
            "value_usd": "5.836857017890536",
            "ledger_entry_id": "ledger-uuid-1",
            "timestamp": "2026-05-02T15:48:03.917Z",
        }
    }
    pv = _make_position_value(Decimal("5.85"))  # current value approximately matches
    pi = _make_position_info("5464283")

    valuer._enrich_from_open_event(pv, pi, position_type="LP")

    assert pv.cost_basis_usd == Decimal("5.836857017890536")
    assert pv.unrealized_pnl_usd == pv.value_usd - pv.cost_basis_usd
    assert pv.entry_timestamp == "2026-05-02T15:48:03.917Z"
    assert pv.ledger_entry_id == "ledger-uuid-1"


def test_cache_miss_falls_through_when_no_sync_getter(valuer):
    """Cache miss + no get_position_events_sync = pre-fix behaviour
    (cost_basis stays 0) — matches the production fallback when the
    runner is restarted between LP_OPEN and the next snapshot."""
    valuer._recent_open_events = {}
    pv = _make_position_value()
    pi = _make_position_info()

    valuer._enrich_from_open_event(pv, pi, position_type="LP")

    assert pv.cost_basis_usd == Decimal("0")


def test_cache_skipped_when_value_usd_is_zero(valuer):
    """Defensive: a cached entry with value_usd=0 should not write a
    misleading cost_basis. The early-return `if cost_basis <= 0` guard
    matches the disk-path semantics."""
    valuer._recent_open_events = {
        ("5464283", "LP"): {"value_usd": "0", "ledger_entry_id": "x", "timestamp": ""}
    }
    pv = _make_position_value()
    pi = _make_position_info()

    valuer._enrich_from_open_event(pv, pi, position_type="LP")

    assert pv.cost_basis_usd == Decimal("0")  # untouched


def test_cache_keyed_by_position_type():
    """Two positions with the same ID but different types
    (LP vs PERP) must not collide in the cache."""
    v = PortfolioValuer.__new__(PortfolioValuer)
    v._accounting_store = MagicMock(spec=[])
    v._deployment_id = "dep-1"
    v._recent_open_events = {
        ("123", "LP"): {"value_usd": "10.0", "ledger_entry_id": "lp-x", "timestamp": ""},
        ("123", "PERP"): {"value_usd": "20.0", "ledger_entry_id": "perp-x", "timestamp": ""},
    }
    pv_lp = _make_position_value()
    pv_perp = _make_position_value()
    pi = _make_position_info("123")

    v._enrich_from_open_event(pv_lp, pi, position_type="LP")
    v._enrich_from_open_event(pv_perp, pi, position_type="PERP")

    assert pv_lp.cost_basis_usd == Decimal("10.0")
    assert pv_perp.cost_basis_usd == Decimal("20.0")


def test_disk_fallback_still_works_when_state_manager_has_sync_getter():
    """Backwards compat: SQLite-backed state_manager exposes
    ``get_position_events_sync``; that path keeps working when the cache
    is empty (covers warm-start / runner restart / snapshot far after
    LP_OPEN)."""
    v = PortfolioValuer.__new__(PortfolioValuer)
    store = MagicMock()
    store.get_position_events_sync.return_value = [
        {
            "value_usd": "4.20",
            "ledger_entry_id": "from-disk",
            "timestamp": "2026-05-02T10:00:00Z",
        }
    ]
    v._accounting_store = store
    v._deployment_id = "dep-1"
    v._recent_open_events = {}
    pv = _make_position_value(Decimal("5.00"))
    pi = _make_position_info("789")

    v._enrich_from_open_event(pv, pi, position_type="LP")

    store.get_position_events_sync.assert_called_once_with(
        "dep-1", position_id="789", position_type="LP", event_type="OPEN"
    )
    assert pv.cost_basis_usd == Decimal("4.20")
    assert pv.ledger_entry_id == "from-disk"


def test_cache_takes_precedence_over_disk(valuer):
    """Cache hit short-circuits before the disk path. Otherwise a stale
    state_manager (or a pre-write race) could shadow the just-written
    OPEN event."""
    valuer._accounting_store = MagicMock()
    valuer._accounting_store.get_position_events_sync.return_value = [
        {"value_usd": "999.0", "ledger_entry_id": "stale-disk", "timestamp": ""}
    ]
    valuer._recent_open_events = {
        ("1", "LP"): {"value_usd": "4.00", "ledger_entry_id": "fresh-cache", "timestamp": ""}
    }
    pv = _make_position_value()
    pi = _make_position_info("1")

    valuer._enrich_from_open_event(pv, pi, position_type="LP")

    # Cache wins — disk path NEVER consulted.
    valuer._accounting_store.get_position_events_sync.assert_not_called()
    assert pv.cost_basis_usd == Decimal("4.00")
    assert pv.ledger_entry_id == "fresh-cache"


# ──────────────────────────────────────────────────────────────────────────
# Runner-side cache update — _update_recent_open_events_cache
# ──────────────────────────────────────────────────────────────────────────


def test_runner_cache_updates_on_open_and_close():
    """The runner's cache update helper records OPEN events and removes
    them on CLOSE — so a post-teardown snapshot doesn't keep reporting
    deployed capital after the position has been closed."""
    from almanak.framework.runner.strategy_runner import StrategyRunner

    runner = StrategyRunner.__new__(StrategyRunner)
    runner._recent_open_events = {}

    open_event = SimpleNamespace(
        event_type="OPEN",
        position_id=5464283,
        position_type="LP",
        value_usd="5.84",
        ledger_entry_id="ledger-1",
        timestamp="2026-05-02T15:48:03Z",
    )
    runner._update_recent_open_events_cache(open_event)
    assert ("5464283", "LP") in runner._recent_open_events
    assert runner._recent_open_events[("5464283", "LP")]["value_usd"] == "5.84"

    close_event = SimpleNamespace(
        event_type="CLOSE",
        position_id=5464283,
        position_type="LP",
        value_usd="0",
        ledger_entry_id="ledger-2",
        timestamp="2026-05-02T16:00:00Z",
    )
    runner._update_recent_open_events_cache(close_event)
    assert ("5464283", "LP") not in runner._recent_open_events


# ──────────────────────────────────────────────────────────────────────────
# VIB-4086 — token0/token1 carry-forward across the LP lifecycle
# ──────────────────────────────────────────────────────────────────────────


def test_runner_cache_stamps_tokens_on_open():
    """OPEN events stamp ``token0`` / ``token1`` into the cache so the
    matching LP_CLOSE row can carry them forward when the close-receipt
    parser doesn't re-emit the pair."""
    from almanak.framework.runner.strategy_runner import StrategyRunner

    runner = StrategyRunner.__new__(StrategyRunner)
    runner._recent_open_events = {}

    open_event = SimpleNamespace(
        event_type="OPEN",
        position_id="5471740",
        position_type="LP",
        value_usd="1873.66",
        ledger_entry_id="ledger-1",
        timestamp="2026-05-04T00:00:00Z",
        tick_lower=-204000,
        tick_upper=-203000,
        liquidity="123456789",
        token0="WETH",
        token1="USDC",
    )
    runner._update_recent_open_events_cache(open_event)

    cached = runner._recent_open_events[("5471740", "LP")]
    assert cached["token0"] == "WETH"
    assert cached["token1"] == "USDC"


def test_lp_close_columns_carry_forward_tokens_from_cache():
    """``_apply_lp_close_columns`` populates ``event.token0/token1`` from
    the ``recent_open_events`` cache when the CLOSE receipt parser
    doesn't carry them. Pre-VIB-4086 the CLOSE row landed with token
    columns empty even though the OPEN had them — breaking
    ``_apply_lp_close_value_usd`` (which gates on
    ``token0 and token1``) and the dashboard's lifecycle render.
    """
    from almanak.framework.observability.position_events import (
        IntentEventContext,
        PositionEvent,
        _apply_lp_close_columns,
    )

    event = PositionEvent(
        deployment_id="dep-1",
        position_id="5471740",
        position_type="LP",
        event_type="CLOSE",
        chain="arbitrum",
        protocol="uniswap_v3",
        amount0="373299496677784068",
        amount1="988019899",
    )
    ctx = IntentEventContext(
        intent=SimpleNamespace(),
        result=None,
        extracted={},
        deployment_id="dep-1",
        chain="arbitrum",
        ledger_entry_id="ledger-2",
        price_oracle=None,
    )
    cache = {
        ("5471740", "LP"): {
            "value_usd": "1873.66",
            "ledger_entry_id": "ledger-1",
            "timestamp": "2026-05-04T00:00:00Z",
            "tick_lower": -204000,
            "tick_upper": -203000,
            "liquidity": "1000",
            "token0": "WETH",
            "token1": "USDC",
        }
    }

    _apply_lp_close_columns(event, ctx, cache, price_oracle=None)

    assert event.token0 == "WETH", "CLOSE event must carry token0 forward from OPEN cache"
    assert event.token1 == "USDC", "CLOSE event must carry token1 forward from OPEN cache"
    # bracket carry-forward (VIB-3919) still works
    assert event.tick_lower == -204000
    assert event.tick_upper == -203000


def test_lp_close_does_not_overwrite_existing_tokens():
    """If the CLOSE receipt parser DOES emit token0/token1, the carry-
    forward must not clobber them. The existing-value guard (``not
    event.token0``) lets the receipt-parser-supplied values win."""
    from almanak.framework.observability.position_events import (
        IntentEventContext,
        PositionEvent,
        _apply_lp_close_columns,
    )

    event = PositionEvent(
        deployment_id="dep-1",
        position_id="5471740",
        position_type="LP",
        event_type="CLOSE",
        chain="arbitrum",
        protocol="uniswap_v3",
        token0="RECEIPT_TOKEN0",
        token1="RECEIPT_TOKEN1",
    )
    ctx = IntentEventContext(
        intent=SimpleNamespace(),
        result=None,
        extracted={},
        deployment_id="dep-1",
        chain="arbitrum",
        ledger_entry_id="ledger-2",
        price_oracle=None,
    )
    cache = {("5471740", "LP"): {"token0": "CACHE_TOKEN0", "token1": "CACHE_TOKEN1"}}

    _apply_lp_close_columns(event, ctx, cache, price_oracle=None)

    assert event.token0 == "RECEIPT_TOKEN0"
    assert event.token1 == "RECEIPT_TOKEN1"
