"""VIB-5463 / TD-05 — morpho_looping teardown enumeration re-derives from chain.

Blueprint 14:811: ``get_open_positions()`` MUST query on-chain state. These tests
pin the migration from cached counters to a live ``position_health`` re-derivation:

* a WIPED restart (in-memory counters zero) still surfaces the live on-chain
  position from chain — the stranding bug this ticket closes;
* a successful all-zero chain read reports NO position (genuinely closed);
* an UNAVAILABLE chain read falls back to the cached counters (a gateway blip
  must never DROP a known position).
"""

from __future__ import annotations

from decimal import Decimal

from almanak.demo_strategies.morpho_looping.strategy import MorphoLoopingStrategy
from almanak.framework.teardown.models import PositionType


class _Health:
    def __init__(self, collateral_value_usd, debt_value_usd, health_factor):
        self.collateral_value_usd = collateral_value_usd
        self.debt_value_usd = debt_value_usd
        self.health_factor = health_factor


class _Snapshot:
    def __init__(self, *, health=None, raise_health=False, prices=None):
        self._health = health
        self._raise_health = raise_health
        self._prices = prices or {"wstETH": Decimal("3400"), "USDC": Decimal("1")}

    def position_health(self, protocol, market_id, *, collateral_price_usd=None, debt_price_usd=None):
        if self._raise_health:
            raise RuntimeError("gateway unavailable")
        return self._health

    def price(self, token):
        return self._prices[token]


def _strategy(*, total_collateral=Decimal("0"), total_borrowed=Decimal("0")) -> MorphoLoopingStrategy:
    s = MorphoLoopingStrategy.__new__(MorphoLoopingStrategy)
    s._chain = "ethereum"
    s.market_id = "0xb323495f7e4148be5643a4ea4a8221eef163e4bccfdedc2a6f4696baacbc86cc"
    s.collateral_token = "wstETH"
    s.borrow_token = "USDC"
    s._total_collateral = total_collateral
    s._total_borrowed = total_borrowed
    s._current_health_factor = Decimal("0")
    return s


def test_wiped_state_rederives_live_position_from_chain() -> None:
    """In-memory counters zero (wiped restart) but a live position on-chain ⇒
    teardown still enumerates it. This is the stranding bug TD-05 closes."""
    strategy = _strategy(total_collateral=Decimal("0"), total_borrowed=Decimal("0"))
    strategy.create_market_snapshot = lambda: _Snapshot(  # type: ignore[method-assign]
        health=_Health(Decimal("3400"), Decimal("1700"), Decimal("1.72"))
    )
    summary = strategy.get_open_positions()
    by_type = {p.position_type: p for p in summary.positions}
    assert PositionType.SUPPLY in by_type
    assert PositionType.BORROW in by_type
    assert by_type[PositionType.SUPPLY].value_usd == Decimal("3400")
    assert by_type[PositionType.BORROW].value_usd == Decimal("1700")
    assert by_type[PositionType.SUPPLY].details["source"] == "chain"
    assert by_type[PositionType.BORROW].health_factor == Decimal("1.72")


def test_chain_authoritative_closed_market_reports_nothing() -> None:
    """A clean all-zero chain read is a CLOSED market — even if the stale cache
    still believes it holds collateral (chain is authoritative, 14:811)."""
    strategy = _strategy(total_collateral=Decimal("5"), total_borrowed=Decimal("9000"))
    strategy.create_market_snapshot = lambda: _Snapshot(  # type: ignore[method-assign]
        health=_Health(Decimal("0"), Decimal("0"), None)
    )
    summary = strategy.get_open_positions()
    assert summary.positions == []


def test_unavailable_chain_read_falls_back_to_cache() -> None:
    """A gateway blip (position_health raises) must NOT drop a known position —
    fall back to the cached counters (Empty != Zero)."""
    strategy = _strategy(total_collateral=Decimal("2"), total_borrowed=Decimal("3000"))
    strategy.create_market_snapshot = lambda: _Snapshot(raise_health=True)  # type: ignore[method-assign]
    summary = strategy.get_open_positions()
    by_type = {p.position_type: p for p in summary.positions}
    assert PositionType.SUPPLY in by_type
    assert PositionType.BORROW in by_type
    assert by_type[PositionType.SUPPLY].details["source"] == "cache"
    assert by_type[PositionType.BORROW].value_usd == Decimal("3000")


def test_no_snapshot_and_empty_cache_reports_nothing() -> None:
    """Snapshot build fails AND cache is empty ⇒ no positions (nothing to strand)."""

    def _boom():
        raise RuntimeError("no gateway")

    strategy = _strategy()
    strategy.create_market_snapshot = _boom  # type: ignore[method-assign]
    summary = strategy.get_open_positions()
    assert summary.positions == []
