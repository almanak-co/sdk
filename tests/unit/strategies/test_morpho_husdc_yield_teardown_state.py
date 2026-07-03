"""Teardown-state persistence regression for morpho_husdc_yield (VIB-5486 / TD-06c).

The shipped config ships ``market_id=""`` → ``self.market_id is None``; the Morpho
Blue market is resolved at runtime into the in-memory ``_resolved_market_id``. Before
this fix that seed was NOT persisted, so after a restart both
``get_open_positions()`` and ``generate_teardown_intents()`` computed
``market_id = self.market_id or self._resolved_market_id = None`` and a live HUSDC
supply on Morpho Blue was stranded — teardown reported "nothing to do".

These tests prove the round-trip: with the market identity restored via
``load_persistent_state()`` teardown sees the position again; without it, teardown is
blind (the bug the PERSISTED posture closes). The supply AMOUNT stays chain-derived —
``_get_supply_assets`` is stubbed to stand in for the live on-chain read.
"""

from __future__ import annotations

import json
from decimal import Decimal

import pytest

_MARKET = "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"


def _make_strategy(*, resolved_market_id: str | None):
    from strategies.incubating.morpho_husdc_yield.strategy import MorphoHusdcYieldStrategy

    strat = MorphoHusdcYieldStrategy.__new__(MorphoHusdcYieldStrategy)
    strat.config = {}
    strat._chain = "base"
    strat._deployment_id = "test-morpho-husdc"
    strat.STRATEGY_NAME = "morpho_husdc_yield"
    strat.protocol = "morpho_blue"
    strat.token_symbol = "HUSDC"
    # Config omits market_id (ships ""), so the only identity source is the runtime seed.
    strat.market_id = None
    strat._resolved_market_id = resolved_market_id
    strat._state = {"last_action": None, "last_apy_pct": None, "last_market_id": None}
    # Stand in for the live on-chain supply read (chain-derived AMOUNT). Returns a
    # positive balance whenever a market id is known — the gate is the identity, not
    # the amount.
    strat._get_supply_assets = lambda market_id: Decimal("100") if market_id else Decimal("0")
    return strat


def _teardown_mode():
    from almanak.framework.teardown import TeardownMode

    return TeardownMode.SOFT


def test_teardown_blind_after_restart_without_persistence() -> None:
    """A restarted instance with no restored market id sees NOTHING — the bug."""
    strat = _make_strategy(resolved_market_id=None)

    summary = strat.get_open_positions()
    assert summary.positions == [], "no market id → get_open_positions must report empty"

    intents = strat.generate_teardown_intents(_teardown_mode())
    assert intents == [], "no market id → generate_teardown_intents must emit nothing"


def test_persistent_state_round_trip_restores_market_identity() -> None:
    """Persist the resolved market id, restore it on a fresh instance, and confirm
    teardown is NOT blind after the restart."""
    # Pre-restart instance that resolved the market at runtime.
    pre = _make_strategy(resolved_market_id=_MARKET)
    snapshot = pre.get_persistent_state()
    assert snapshot == {"resolved_market_id": _MARKET}
    # Persistence is stored as JSON (save_state -> json.dumps(default=str)).
    assert json.loads(json.dumps(snapshot, default=str)) == snapshot

    # Fresh post-restart instance: market id wiped, as it would be on boot.
    post = _make_strategy(resolved_market_id=None)
    assert post.get_open_positions().positions == [], "sanity: blind before restore"

    post.load_persistent_state(snapshot)
    assert post._resolved_market_id == _MARKET

    # get_open_positions now sees the live supply again.
    positions = post.get_open_positions().positions
    assert len(positions) == 1
    pos = positions[0]
    assert pos.details["market_id"] == _MARKET
    assert pos.value_usd == Decimal("100")
    assert pos.position_id.endswith(_MARKET)

    # generate_teardown_intents now emits the unwind against the restored market.
    intents = post.generate_teardown_intents(_teardown_mode())
    assert len(intents) == 1
    intent = intents[0]
    assert intent.market_id == _MARKET
    assert intent.withdraw_all is True


def test_load_persistent_state_tolerates_empty_seed() -> None:
    """An empty / missing seed normalises to None (never a falsey empty string)."""
    strat = _make_strategy(resolved_market_id=_MARKET)
    strat.load_persistent_state({})
    assert strat._resolved_market_id is None
    strat.load_persistent_state({"resolved_market_id": ""})
    assert strat._resolved_market_id is None


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-v"]))
