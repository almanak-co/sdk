"""VIB-5406 — the post-execution/teardown snapshot awaits the current unit's
outbox drains before reading accounting_events, closing the held-PT / swap NAV
race.

Root cause (proven on a frozen fixture + live runner trace): the disposal's
accounting event is written by a fire-and-forget ``drain_one`` task. The snapshot
derives held-PT / open-swap inventory by REPLAYING accounting_events into a fresh
FIFO store. With no barrier the snapshot can prefetch BEFORE the disposal drains,
replaying an event stream that still shows the just-sold lot as held → phantom
inventory / NAV overstatement.

These tests pin:
  1. ``await_drain_barrier`` semantics (empty / completes / times-out-no-cancel).
  2. The race itself: skipping the barrier surfaces a phantom held PT; running it
     first applies the disposal drain so the replay shows remaining=0.
  3. Degraded contract (Empty≠Zero): a timed-out barrier stamps inventory
     ``unmeasured``/``drain_incomplete`` and NEVER halts the loop.
  4. Both lanes (iteration ``capture_snapshot_with_accounting`` + teardown
     ``capture_teardown_snapshot_with_accounting``) run the barrier / reset.
"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

from almanak.framework.market.models import PtPriceData
from almanak.framework.portfolio.models import ValueConfidence
from almanak.framework.runner import _run_loop_helpers as H
from almanak.framework.teardown.models import PositionType, TeardownPositionSummary
from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

DEP = "deployment:vib5406"
WALLET = "0x00000000000000000000000000000000000000dd"
PT = "PT-sUSDe-26DEC2024"
CHAIN = "ethereum"


# --------------------------------------------------------------------------- #
# Harness (mirrors tests/unit/valuation/test_pt_inventory_classification_vib5316)
# --------------------------------------------------------------------------- #
def _strategy() -> MagicMock:
    s = MagicMock()
    s.deployment_id = DEP
    s.chain = CHAIN
    s.wallet_address = WALLET
    s._get_tracked_tokens.return_value = ["USDC"]
    s.get_open_positions.return_value = TeardownPositionSummary(
        deployment_id=DEP, timestamp=datetime.now(UTC), positions=[]
    )
    return s


def _market() -> MagicMock:
    m = MagicMock()

    def _price(t, quote="USD"):
        if t in ("ETH", "WETH"):
            return Decimal("2000")
        if t == "USDC":
            return Decimal("1")
        raise ValueError(t)

    def _bal(t):
        r = MagicMock()
        r.balance = Decimal("100") if t == "USDC" else Decimal("0")
        return r

    m.price = _price
    m.balance = _bal
    m.pt_price = MagicMock(
        return_value=PtPriceData(
            symbol=PT,
            chain=CHAIN,
            price=Decimal("0.97"),
            confidence=ValueConfidence.HIGH,
            underlying_price=Decimal("1.0"),
            pt_to_asset_rate=Decimal("0.97"),
            days_to_maturity=180,
            source="test",
        )
    )
    return m


def _pt_event(event_type: str, pt_amount: str, sy_amount: str, *, ts: str) -> dict:
    return {
        "event_type": event_type,
        "deployment_id": DEP,
        "position_key": "pt:ethereum",
        "chain": CHAIN,
        "wallet_address": WALLET,
        "timestamp": ts,
        "payload_json": json.dumps({"pt_token": PT, "pt_amount": pt_amount, "sy_amount": sy_amount}),
    }


PT_BUY = _pt_event("PT_BUY", "100", "95", ts="2026-06-01T00:00:00+00:00")
PT_SELL = _pt_event("PT_SELL", "100", "95", ts="2026-06-02T00:00:00+00:00")  # full disposal → remaining 0


def _valuer_reading(events_list: list) -> PortfolioValuer:
    """A valuer whose accounting store reads ``events_list`` LIVE (so a drain that
    appends mid-test is reflected at the next prefetch)."""
    store = MagicMock()
    store.get_accounting_events_sync = lambda dep, position_key=None: list(events_list)
    v = PortfolioValuer()
    v.set_accounting_context(store, DEP)
    return v


def _pt_rows(snap):
    return [
        p
        for p in snap.positions
        if p.position_type == PositionType.TOKEN and (p.details or {}).get("source") == "pt_inventory_lots"
    ]


# --------------------------------------------------------------------------- #
# 1. await_drain_barrier semantics
# --------------------------------------------------------------------------- #
def test_await_drain_barrier_empty_batch_is_noop_true():
    async def scenario():
        runner = SimpleNamespace(deployment_id=DEP)
        batch: list[asyncio.Task] = []
        ok = await H.await_drain_barrier(runner, batch)
        assert ok is True
        assert batch == []

    asyncio.run(scenario())


def test_await_drain_barrier_awaits_completion_and_clears():
    async def scenario():
        runner = SimpleNamespace(deployment_id=DEP)
        done = {"ran": False}

        async def _drain():
            await asyncio.sleep(0)
            done["ran"] = True

        batch = [asyncio.create_task(_drain())]
        ok = await H.await_drain_barrier(runner, batch)
        assert ok is True
        assert done["ran"] is True  # barrier actually waited for the drain
        assert batch == []  # cleared after awaiting

    asyncio.run(scenario())


def test_await_drain_barrier_timeout_returns_false_without_cancelling():
    async def scenario():
        runner = SimpleNamespace(deployment_id=DEP)
        gate = asyncio.Event()

        async def _slow_drain():
            await gate.wait()  # never set within the timeout
            return True

        task = asyncio.create_task(_slow_drain())
        batch = [task]
        ok = await H.await_drain_barrier(runner, batch, timeout=0.05)
        assert ok is False  # degraded
        assert task.cancelled() is False  # straggler NOT cancelled — must still drain
        assert task.done() is False  # still running in the background
        assert batch == []  # batch cleared regardless
        # cleanup: let the straggler finish so the loop closes cleanly
        gate.set()
        await task

    asyncio.run(scenario())


def test_await_drain_barrier_false_result_is_incomplete():
    """A drain that COMPLETES but returns False (drain_one persistent-write
    failure) must mark the barrier incomplete — completion alone is not success
    (VIB-5406, CodeRabbit). Otherwise the snapshot replays a stale stream."""

    async def scenario():
        runner = SimpleNamespace(deployment_id=DEP)

        async def _failed_drain():
            await asyncio.sleep(0)
            return False  # drain_one returns False on persistent write failure

        batch = [asyncio.create_task(_failed_drain())]
        ok = await H.await_drain_barrier(runner, batch)
        assert ok is False  # failed drain ⇒ degraded, not success
        assert batch == []  # batch still cleared

    asyncio.run(scenario())


def test_await_drain_barrier_raised_result_is_incomplete():
    """A drain task that RAISED must read as failure, not silent success — and
    retrieving the result must consume the exception (no 'never retrieved')."""

    async def scenario():
        runner = SimpleNamespace(deployment_id=DEP)

        async def _raising_drain():
            await asyncio.sleep(0)
            raise RuntimeError("backend write blew up")

        batch = [asyncio.create_task(_raising_drain())]
        ok = await H.await_drain_barrier(runner, batch)
        assert ok is False  # raised drain ⇒ degraded

    asyncio.run(scenario())


def test_await_drain_barrier_false_among_already_done_tasks_is_incomplete():
    """A task that finished (False) BEFORE the await is still inspected — the
    `pending` filter skips already-done tasks, so their result must be checked
    separately (VIB-5406, CodeRabbit)."""

    async def scenario():
        runner = SimpleNamespace(deployment_id=DEP)

        async def _ok_drain():
            return True

        async def _failed_drain():
            return False

        ok_task = asyncio.create_task(_ok_drain())
        bad_task = asyncio.create_task(_failed_drain())
        await asyncio.sleep(0)  # let both finish so they're already-done at barrier entry
        assert ok_task.done() and bad_task.done()
        batch = [ok_task, bad_task]
        ok = await H.await_drain_barrier(runner, batch)
        assert ok is False  # the already-done False is detected

    asyncio.run(scenario())


# --------------------------------------------------------------------------- #
# 2. The race — barrier closes it (PT lane)
# --------------------------------------------------------------------------- #
def test_barrier_closes_pt_disposal_race():
    """Skipping the barrier replays a stream missing the PT_SELL → phantom held
    PT. Running the barrier first applies the disposal drain → remaining 0 → no
    phantom."""

    async def scenario():
        runner = SimpleNamespace(deployment_id=DEP)
        # The event stream starts with only the BUY drained; the SELL drain is
        # still in flight (the production race window).
        events = [dict(PT_BUY)]
        valuer = _valuer_reading(events)

        async def _sell_drain():
            await asyncio.sleep(0)
            events.append(dict(PT_SELL))  # disposal lands → remaining 0

        # --- pre-fix behaviour: snapshot WITHOUT awaiting the drain → phantom ---
        snap_racy = valuer.value(_strategy(), _market())
        assert len(_pt_rows(snap_racy)) == 1, "expected the pre-fix phantom held-PT row"

        # --- post-fix behaviour: barrier awaits the disposal drain first ---
        batch = [asyncio.create_task(_sell_drain())]
        ok = await H.await_drain_barrier(runner, batch)
        assert ok is True
        valuer.set_drain_barrier_incomplete(not ok)
        snap_fixed = valuer.value(_strategy(), _market())
        assert _pt_rows(snap_fixed) == [], "disposal drained → no phantom held PT after the barrier"

    asyncio.run(scenario())


# --------------------------------------------------------------------------- #
# 3. Degraded contract — timed-out barrier stamps unmeasured, never halts
# --------------------------------------------------------------------------- #
def test_drain_incomplete_stamps_pt_inventory_unmeasured_and_does_not_halt():
    async def scenario():
        runner = SimpleNamespace(deployment_id=DEP)
        events = [dict(PT_BUY)]  # SELL never drains within the timeout
        valuer = _valuer_reading(events)
        gate = asyncio.Event()

        async def _never():
            await gate.wait()

        task = asyncio.create_task(_never())
        batch = [task]
        ok = await H.await_drain_barrier(runner, batch, timeout=0.05)
        assert ok is False
        valuer.set_drain_barrier_incomplete(not ok)

        # value() must NOT raise (no halt) and must degrade the PT inventory to a
        # confidence-downgrading marker — NOT a phantom lot, NOT empty-but-HIGH.
        snap = valuer.value(_strategy(), _market())
        rows = _pt_rows(snap)
        assert len(rows) == 1, "expected a single drain_incomplete marker row"
        assert rows[0].value_usd == Decimal("0")  # no phantom NAV
        assert rows[0].details["valuation_status"] == "no_path"
        assert rows[0].details["unavailable_reason"] == "drain_incomplete"
        meta = (snap.snapshot_metadata or {}).get("pt_inventory")
        assert isinstance(meta, dict) and meta.get("reason") == "drain_incomplete", meta
        # AUDIT FIX (VIB-5406): a degraded NAV must read UNAVAILABLE, never a
        # silent HIGH that drops the held PT from total_value_usd.
        assert snap.value_confidence == ValueConfidence.UNAVAILABLE

        gate.set()
        await task

    asyncio.run(scenario())


def test_value_consumes_drain_flag_once():
    """value()'s finally resets the flag so the next snapshot starts clean."""
    valuer = _valuer_reading([dict(PT_BUY)])
    valuer.set_drain_barrier_incomplete(True)
    assert valuer._drain_barrier_incomplete is True
    valuer.value(_strategy(), _market())
    assert valuer._drain_barrier_incomplete is False  # consumed once


def test_swap_inventory_degrades_when_drain_incomplete():
    """The swap-lot twin (VIB-5057) honours the same drain_incomplete contract:
    a confidence-downgrading marker (not empty rows), so the snapshot reads
    UNAVAILABLE rather than a silent HIGH (audit fix VIB-5406)."""
    valuer = _valuer_reading([])
    valuer.set_drain_barrier_incomplete(True)
    result = valuer._swap_inventory_for_snapshot(CHAIN, {}, {})
    assert len(result.rows) == 1
    assert result.rows[0].value_usd == Decimal("0")  # no phantom NAV
    assert result.rows[0].details["valuation_status"] == "no_path"
    assert result.rows[0].details["unavailable_reason"] == "drain_incomplete"
    assert result.metadata == {"status": "unmeasured", "reason": "drain_incomplete"}
    # The marker forces the snapshot-level confidence to UNAVAILABLE.
    conf = PortfolioValuer._determine_value_confidence(
        positions=result.rows,
        wallet_balances=[],
        positions_unavailable=False,
        wallet_data_incomplete=False,
    )
    assert conf == ValueConfidence.UNAVAILABLE


# --------------------------------------------------------------------------- #
# 4. Iteration lane wiring — capture_snapshot_with_accounting runs the barrier
# --------------------------------------------------------------------------- #
def _fake_runner_for_capture(valuer: PortfolioValuer, batch: list) -> SimpleNamespace:
    captured = {"flag_at_capture": None, "capture_called": False}

    async def _capture_portfolio_snapshot(*, strategy, iteration_number):
        captured["capture_called"] = True
        captured["flag_at_capture"] = valuer._drain_barrier_incomplete

    runner = SimpleNamespace(
        deployment_id=DEP,
        config=SimpleNamespace(enable_state_persistence=True),
        _drain_batch=batch,
        _portfolio_valuer=valuer,
        _total_iterations=0,
        _iteration_had_trade=False,
        _capture_portfolio_snapshot=_capture_portfolio_snapshot,
        _is_live_mode=lambda: False,
        _captured=captured,
    )
    return runner


def test_iteration_capture_runs_barrier_and_threads_flag_on_timeout():
    async def scenario():
        valuer = _valuer_reading([dict(PT_BUY)])
        gate = asyncio.Event()

        async def _never():
            await gate.wait()

        task = asyncio.create_task(_never())
        runner = _fake_runner_for_capture(valuer, [task])
        result_in = SimpleNamespace(status="x")
        # Tighten the barrier timeout via the module constant for a fast test.
        orig = H._DRAIN_BARRIER_TIMEOUT_S
        H._DRAIN_BARRIER_TIMEOUT_S = 0.05
        try:
            out = await H.capture_snapshot_with_accounting(runner, _strategy(), DEP, result_in)
        finally:
            H._DRAIN_BARRIER_TIMEOUT_S = orig
        assert out is result_in  # non-live happy path returns the input result
        assert runner._captured["capture_called"] is True
        # The barrier timed out → the valuer saw the incomplete flag at capture time.
        assert runner._captured["flag_at_capture"] is True
        assert runner._drain_batch == []  # barrier cleared the batch
        gate.set()
        await task

    asyncio.run(scenario())


def test_iteration_capture_flag_false_when_drain_completes():
    async def scenario():
        valuer = _valuer_reading([dict(PT_BUY)])

        async def _drain():
            await asyncio.sleep(0)

        runner = _fake_runner_for_capture(valuer, [asyncio.create_task(_drain())])
        await H.capture_snapshot_with_accounting(runner, _strategy(), DEP, SimpleNamespace(status="x"))
        assert runner._captured["flag_at_capture"] is False  # drained in time → not degraded

    asyncio.run(scenario())


# --------------------------------------------------------------------------- #
# 5. Teardown lane wiring — PRE resets the batch, POST runs the barrier
# --------------------------------------------------------------------------- #
def _fake_runner_for_teardown(valuer: PortfolioValuer, batch: list, monkeypatch) -> SimpleNamespace:
    captured = {"flag_at_capture": None}

    async def _fake_capture_portfolio_snapshot(runner, strategy, *, iteration_number, force_snapshot):
        captured["flag_at_capture"] = valuer._drain_barrier_incomplete
        return SimpleNamespace(total_value_usd=Decimal("0"))

    # capture_teardown_snapshot_with_accounting imports this name locally.
    monkeypatch.setattr(
        "almanak.framework.runner.runner_state.capture_portfolio_snapshot",
        _fake_capture_portfolio_snapshot,
    )
    runner = SimpleNamespace(
        deployment_id=DEP,
        config=SimpleNamespace(enable_state_persistence=True),
        _drain_batch=batch,
        _portfolio_valuer=valuer,
        _total_iterations=0,
        _last_cycle_id="",
        _teardown_price_oracle=None,
        _begin_market_snapshot_iteration=lambda *a, **k: None,
        _captured=captured,
    )
    return runner


def test_teardown_pre_bracket_resets_drain_batch(monkeypatch):
    async def scenario():
        valuer = _valuer_reading([dict(PT_BUY)])
        # A stale task left over from a prior unit must be dropped by the PRE reset.
        stale = asyncio.create_task(asyncio.sleep(0))
        runner = _fake_runner_for_teardown(valuer, [stale], monkeypatch)
        await H.capture_teardown_snapshot_with_accounting(
            runner, _strategy(), teardown_cycle_id="teardown-1", pre_teardown=True
        )
        assert runner._drain_batch == []  # PRE bracket opened a fresh batch
        await stale

    asyncio.run(scenario())


def test_teardown_post_bracket_runs_barrier_and_threads_flag(monkeypatch):
    async def scenario():
        valuer = _valuer_reading([dict(PT_BUY)])
        gate = asyncio.Event()

        async def _never():
            await gate.wait()

        task = asyncio.create_task(_never())
        runner = _fake_runner_for_teardown(valuer, [task], monkeypatch)
        orig = H._DRAIN_BARRIER_TIMEOUT_S
        H._DRAIN_BARRIER_TIMEOUT_S = 0.05
        try:
            outcome = await H.capture_teardown_snapshot_with_accounting(
                runner, _strategy(), teardown_cycle_id="teardown-1", pre_teardown=False
            )
        finally:
            H._DRAIN_BARRIER_TIMEOUT_S = orig
        # POST bracket: barrier timed out → valuer saw the incomplete flag, and
        # teardown did NOT halt (outcome returned, snapshot captured).
        assert runner._captured["flag_at_capture"] is True
        assert runner._drain_batch == []
        assert outcome.snapshot_captured is True
        gate.set()
        await task

    asyncio.run(scenario())
